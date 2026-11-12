"""RPA form-entry queue: generation, stepwise execution, conflicts and reconciliation."""

from app.db import connect
from app.repositories import rpa as rpa_repo
from app.services import rpa_service

DELTA_SOURCE_ID = 4  # Delta RPA Clicks seed source


def test_create_queue_one_step_per_row(isolated_db):
    batch = rpa_service.create_queue(DELTA_SOURCE_ID)
    assert batch["status"] == "ready"
    steps = batch["steps"]
    assert [s["seq"] for s in steps] == [1, 2, 3, 4]
    assert all(s["status"] == "pending" for s in steps)
    # The malformed 4th row still gets a queue step, but no business key.
    assert steps[3]["business_key"] is None
    assert "__form_error__" in steps[3]["payload"]


def test_stepwise_execution_writes_canonical_and_isolates_conflict(isolated_db):
    # Seed canonical via Alpha first so Delta's SKU-1001 row becomes a conflict later.
    alpha = rpa_service.create_queue(1)
    rpa_service.execute_queue(alpha["id"])

    batch = rpa_service.create_queue(DELTA_SOURCE_ID)
    batch_id = batch["id"]
    steps = batch["steps"]

    # Step 1: new key -> insert into canonical.
    rpa_service.execute_queue(batch_id, step_id=steps[0]["id"])
    step1 = rpa_service.get_batch(batch_id)["steps"][0]
    assert step1["status"] == "success"
    assert step1["result"] == "insert"
    assert step1["staging_id"] is not None

    # Step 2: same key as Alpha canonical with a different price -> conflict quarantine.
    rpa_service.execute_queue(batch_id, step_id=steps[1]["id"])
    refreshed = rpa_service.get_batch(batch_id)
    step2 = refreshed["steps"][1]
    assert step2["status"] == "success"
    assert step2["result"] == "conflict"
    assert step2["conflict_id"] is not None

    # Step 4: malformed row fails closed and must not appear anywhere as ingested data.
    rpa_service.execute_queue(batch_id, step_id=steps[3]["id"])
    refreshed = rpa_service.get_batch(batch_id)
    step4 = refreshed["steps"][3]
    assert step4["status"] == "failed"
    assert step4["error"]
    assert step4["staging_id"] is None
    assert step4["conflict_id"] is None

    conn = connect()
    try:
        bad_rows = conn.execute(
            "SELECT COUNT(*) c FROM staging_rows WHERE business_key = 'SKU-4003'"
        ).fetchone()["c"]
        assert bad_rows == 0
    finally:
        conn.close()


def test_batch_execution_report_reconciles(isolated_db):
    # Put Alpha into canonical so Delta's SKU-1001 row becomes a conflict.
    rpa_service.execute_queue(rpa_service.create_queue(1)["id"])

    batch = rpa_service.create_queue(DELTA_SOURCE_ID)
    result = rpa_service.execute_queue(batch["id"])  # all pending steps at once

    report = result["report"]
    assert report["step_total"] == 4
    assert report["pending"] == 0
    assert report["success"] == 3
    assert report["failed"] == 1
    assert report["canonical_writes"] == 2          # SKU-4001, SKU-4002
    assert report["conflict_steps"] == 1            # SKU-1001 price mismatch
    assert report["staging_rows"] == 3              # success steps only, failed step excluded
    assert report["conflict_rows"] == 1
    assert report["reconciles"] is True
    assert all(report["cross_check"].values())

    conn = connect()
    try:
        # Step list and report are the same numbers (single source of truth).
        steps = rpa_repo.list_steps(conn, batch["id"])
        assert sum(1 for s in steps if s["status"] == "success") == report["success"]
        assert sum(1 for s in steps if s["status"] == "failed") == report["failed"]
        canonical_keys = {
            r["business_key"]
            for r in conn.execute("SELECT business_key FROM canonical_items").fetchall()
        }
        assert {"SKU-1001", "SKU-1002", "SKU-1003", "SKU-4001", "SKU-4002"} <= canonical_keys
        # Failed row never reached canonical.
        assert "SKU-4003" not in canonical_keys
    finally:
        conn.close()


def test_queue_persists_across_connections(isolated_db):
    batch = rpa_service.create_queue(DELTA_SOURCE_ID)
    rpa_service.execute_queue(batch["id"], limit=2)

    # A brand new connection (simulating a page refresh) still sees the queue and state.
    reloaded = rpa_service.get_batch(batch["id"])
    assert reloaded["status"] == "ready"  # pending steps remain
    statuses = [s["status"] for s in reloaded["steps"]]
    assert statuses == ["success", "success", "pending", "pending"]
    assert reloaded["report"]["step_total"] == 4


def test_intake_target_holds_then_promotes(isolated_db):
    rpa_service.execute_queue(rpa_service.create_queue(1)["id"])  # Alpha canonical base

    batch = rpa_service.create_queue(DELTA_SOURCE_ID, target="intake")
    after = rpa_service.execute_queue(batch["id"])
    report = after["report"]
    assert report["intake_held"] == 3
    assert report["canonical_writes"] == 0
    assert report["staging_rows"] == 0  # nothing ingested yet

    intake = rpa_service.list_intake()
    assert len(intake) == 3
    assert all(r["status"] == "held" for r in intake)

    promotion = rpa_service.promote_intake(batch_id=batch["id"])
    assert promotion["promoted"] == 2
    assert promotion["conflicts"] == 1
    assert promotion["failed"] == 0

    final = rpa_service.get_batch(batch["id"])
    assert final["report"]["intake_promoted_rows"] == 3
    assert final["report"]["canonical_writes_total"] == 2
    assert final["report"]["conflict_rows"] == 1
    assert final["report"]["reconciles"] is True


def test_promotion_failures_still_reconcile(isolated_db, monkeypatch):
    rpa_service.execute_queue(rpa_service.create_queue(1)["id"])  # Alpha canonical base

    batch = rpa_service.create_queue(DELTA_SOURCE_ID, target="intake")
    rpa_service.execute_queue(batch["id"])  # 3 held + 1 fill failure

    def boom(*args, **kwargs):
        raise RuntimeError("simulated canonical write outage")

    # Insert-path rows (SKU-4001/4002) fail; the conflict row never calls upsert_item.
    monkeypatch.setattr("app.services.rpa_service.canon_repo.upsert_item", boom)
    result = rpa_service.promote_intake(batch_id=batch["id"])

    assert result["failed"] == 2
    assert result["conflicts"] == 1
    report = rpa_service.get_batch(batch["id"])["report"]
    assert report["success"] == 1            # only the conflict step succeeded
    assert report["failed"] == 3             # 2 promote failures + 1 fill failure
    assert report["intake_failed_rows"] == 2
    assert report["staging_rows"] == 1       # the conflict step still wrote staging
    assert report["reconciles"] is True      # no double subtraction of failed steps
