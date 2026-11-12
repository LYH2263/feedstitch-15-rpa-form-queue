"""RPA-style form-entry queue service.

Simulates a local robot typing fixture rows into an entry form one step at a time:

1. ``create_queue`` turns a fixture source into an ordered step queue (one row = one
   step, keyed by business key). Unparseable rows still get a step, which fails closed.
2. ``execute_queue`` fills pending steps in order, one step or a batch at a time.
   Successful steps either merge straight into canonical data or land in the pre-ingest
   intake queue; same-key / non-key divergences still go to conflict quarantine.
   A failed step is never written anywhere as ingested data.
3. Every merge execution is also recorded as a normal ``runs`` row, so Staging /
   Conflicts / Canonical pages and the RPA report explain the same numbers.

No real browser or live network is involved — only local fixtures and SQLite.
"""

from __future__ import annotations

from typing import Any

from app.config import FIXTURES_DIR
from app.db import connect
from app.engines.merge_rules import decide_merge, eligible_for_incremental
from app.engines.parse_feed import iter_fixture_records
from app.repositories import canonical as canon_repo
from app.repositories import rpa as rpa_repo
from app.repositories import runs as runs_repo
from app.repositories import sources as sources_repo

TARGETS = ("canonical", "intake")
MODES = ("incremental", "full")


def create_queue(source_id: int, target: str = "canonical") -> dict[str, Any]:
    if target not in TARGETS:
        raise ValueError("target must be canonical or intake")
    conn = connect()
    try:
        source = sources_repo.get_source(conn, source_id)
        if not source:
            raise ValueError("source not found")
        if not source["enabled"]:
            raise ValueError("source disabled")

        path = FIXTURES_DIR / source["fixture_path"]
        records = iter_fixture_records(path, source["format"])

        batch_id = rpa_repo.create_batch(conn, source_id, target)
        for seq, (ok, value) in enumerate(records, start=1):
            if ok:
                business_key = value["business_key"]
                payload = value["payload"]
            else:
                # Keep the row in the queue; its step must fail when the robot "fills" it.
                business_key = None
                payload = {"__form_error__": value["error"], "raw": value.get("raw")}
            rpa_repo.insert_step(
                conn,
                batch_id=batch_id,
                seq=seq,
                source_id=source_id,
                business_key=business_key,
                payload=payload,
            )
        conn.commit()
        return get_batch(batch_id)
    finally:
        conn.close()


def list_batches() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return rpa_repo.list_batches(conn)
    finally:
        conn.close()


def get_batch(batch_id: int) -> dict[str, Any]:
    conn = connect()
    try:
        batch = rpa_repo.get_batch(conn, batch_id)
        if not batch:
            raise ValueError("batch not found")
        batch["steps"] = rpa_repo.list_steps(conn, batch_id)
        batch["report"] = _build_report(conn, batch_id)
        return batch
    finally:
        conn.close()


def execute_queue(
    batch_id: int,
    *,
    step_id: int | None = None,
    limit: int | None = None,
    mode: str = "incremental",
) -> dict[str, Any]:
    if mode not in MODES:
        raise ValueError("mode must be incremental or full")
    if step_id is not None and limit is not None:
        raise ValueError("choose a single step or a batch limit, not both")
    conn = connect()
    try:
        batch = rpa_repo.get_batch(conn, batch_id)
        if not batch:
            raise ValueError("batch not found")
        source = sources_repo.get_source(conn, batch["source_id"])
        if not source:
            raise ValueError("source not found")

        if mode == "full":
            sources_repo.set_watermark(conn, source["id"], None)
            source["watermark_ts"] = None

        if step_id is not None:
            step = rpa_repo.get_step(conn, step_id)
            if not step or step["batch_id"] != batch_id:
                raise ValueError("step not found in batch")
            if step["status"] != rpa_repo.STEP_PENDING:
                raise ValueError("step is not pending")
            steps = [step]
        else:
            steps = rpa_repo.next_pending_steps(conn, batch_id, limit)
        if not steps:
            return get_batch(batch_id)

        rpa_repo.set_batch_status(conn, batch_id, rpa_repo.BATCH_RUNNING)
        run_id = runs_repo.create_run(conn, source["id"], mode)

        parsed = 0
        upserted = 0
        conflicts = 0
        skipped = 0
        failed = 0
        max_event_ts: str | None = source.get("watermark_ts")

        for step in steps:
            parsed += 1
            payload = step["payload"]
            if "__form_error__" in payload:
                # Robot could not fill the form: fail closed, nothing is ingested.
                rpa_repo.mark_step_outcome(
                    conn,
                    step["id"],
                    status=rpa_repo.STEP_FAILED,
                    error=payload["__form_error__"],
                    run_id=run_id,
                )
                failed += 1
                continue

            item = _item_from_payload(payload)
            try:
                if batch["target"] == "intake":
                    intake_id = rpa_repo.insert_intake_row(
                        conn,
                        batch_id=batch_id,
                        step_id=step["id"],
                        source_id=source["id"],
                        item=item,
                    )
                    rpa_repo.mark_step_outcome(
                        conn,
                        step["id"],
                        status=rpa_repo.STEP_SUCCESS,
                        result="intake_held",
                        run_id=run_id,
                        intake_id=intake_id,
                    )
                    continue

                result, staging_id, conflict_id = _merge_one(
                    conn,
                    run_id=run_id,
                    source_id=source["id"],
                    source_watermark=source.get("watermark_ts"),
                    item=item,
                    mode=mode,
                )
            except Exception as exc:  # noqa: BLE001 — a fill failure must never fake success
                rpa_repo.mark_step_outcome(
                    conn,
                    step["id"],
                    status=rpa_repo.STEP_FAILED,
                    error=str(exc),
                    run_id=run_id,
                )
                failed += 1
                continue

            rpa_repo.mark_step_outcome(
                conn,
                step["id"],
                status=rpa_repo.STEP_SUCCESS,
                result=result,
                run_id=run_id,
                staging_id=staging_id,
                conflict_id=conflict_id,
            )
            if result in ("insert", "upsert"):
                upserted += 1
            elif result == "conflict":
                conflicts += 1
            else:
                skipped += 1
            if max_event_ts is None or item["event_ts"] > max_event_ts:
                max_event_ts = item["event_ts"]

        if batch["target"] == "canonical" and max_event_ts:
            sources_repo.set_watermark(conn, source["id"], max_event_ts)

        remaining = rpa_repo.summarize_batch(conn, batch_id)["pending"]
        rpa_repo.set_batch_status(
            conn,
            batch_id,
            rpa_repo.BATCH_DONE if remaining == 0 else rpa_repo.BATCH_READY,
        )

        report = {
            "rpa_batch_id": batch_id,
            "rpa_failed_count": failed,
            "parsed_count": parsed,
            "upserted_count": upserted,
            "conflict_count": conflicts,
            "skipped_count": skipped,
            "target": batch["target"],
            "watermark_ts": max_event_ts if batch["target"] == "canonical" else source.get("watermark_ts"),
        }
        runs_repo.finish_run(
            conn,
            run_id,
            status="ok",
            parsed=parsed,
            upserted=upserted,
            conflicts=conflicts,
            skipped=skipped,
            report=report,
        )
        conn.commit()
        return get_batch(batch_id)
    finally:
        conn.close()


def list_intake() -> list[dict[str, Any]]:
    conn = connect()
    try:
        return rpa_repo.list_intake_rows(conn)
    finally:
        conn.close()


def promote_intake(
    *,
    batch_id: int | None = None,
    intake_id: int | None = None,
    limit: int | None = None,
    mode: str = "incremental",
) -> dict[str, Any]:
    """Merge held intake rows into canonical data, honoring the same conflict rules."""
    if mode not in MODES:
        raise ValueError("mode must be incremental or full")
    conn = connect()
    try:
        held = [
            r
            for r in rpa_repo.list_intake_rows(conn)
            if r["status"] == rpa_repo.INTAKE_HELD
            and (batch_id is None or r["batch_id"] == batch_id)
            and (intake_id is None or r["id"] == intake_id)
        ]
        held.sort(key=lambda r: r["id"])
        if limit is not None:
            held = held[:limit]
        if not held:
            return {"promoted": 0, "conflicts": 0, "skipped": 0, "failed": 0}

        source_ids = {r["source_id"] for r in held}
        if len(source_ids) != 1:
            raise ValueError("promote one source at a time")
        source_id = source_ids.pop()
        source = sources_repo.get_source(conn, source_id)
        if mode == "full":
            sources_repo.set_watermark(conn, source_id, None)
            source["watermark_ts"] = None

        run_id = runs_repo.create_run(conn, source_id, mode)
        promoted = 0
        conflicts = 0
        skipped = 0
        failed = 0
        max_event_ts: str | None = source.get("watermark_ts")

        for row in held:
            item = _item_from_payload(row["payload"])
            try:
                result, staging_id, conflict_id = _merge_one(
                    conn,
                    run_id=run_id,
                    source_id=source_id,
                    source_watermark=source.get("watermark_ts"),
                    item=item,
                    mode=mode,
                )
            except Exception as exc:  # noqa: BLE001
                rpa_repo.mark_intake_outcome(
                    conn, row["id"], status=rpa_repo.INTAKE_FAILED, run_id=run_id
                )
                step = rpa_repo.get_step(conn, row["step_id"])
                if step:
                    rpa_repo.mark_step_outcome(
                        conn,
                        step["id"],
                        status=rpa_repo.STEP_FAILED,
                        error=f"promote failed: {exc}",
                        run_id=run_id,
                    )
                failed += 1
                continue

            rpa_repo.mark_intake_outcome(
                conn,
                row["id"],
                status=rpa_repo.INTAKE_PROMOTED,
                run_id=run_id,
                staging_id=staging_id,
                conflict_id=conflict_id,
            )
            step = rpa_repo.get_step(conn, row["step_id"])
            if step:
                step_result = (
                    "promoted_conflict"
                    if result == "conflict"
                    else "promoted_insert"
                    if result in ("insert", "upsert")
                    else result
                )
                rpa_repo.mark_step_outcome(
                    conn,
                    step["id"],
                    status=rpa_repo.STEP_SUCCESS,
                    result=step_result,
                    run_id=run_id,
                    staging_id=staging_id,
                    conflict_id=conflict_id,
                )
            if result in ("insert", "upsert"):
                promoted += 1
            elif result == "conflict":
                conflicts += 1
            else:
                skipped += 1
            if max_event_ts is None or item["event_ts"] > max_event_ts:
                max_event_ts = item["event_ts"]

        if max_event_ts:
            sources_repo.set_watermark(conn, source_id, max_event_ts)

        runs_repo.finish_run(
            conn,
            run_id,
            status="ok",
            parsed=len(held),
            upserted=promoted,
            conflicts=conflicts,
            skipped=skipped,
            report={
                "rpa_promotion": True,
                "rpa_batch_id": batch_id,
                "rpa_failed_count": failed,
                "parsed_count": len(held),
                "upserted_count": promoted,
                "conflict_count": conflicts,
                "skipped_count": skipped,
                "watermark_ts": max_event_ts,
            },
        )
        conn.commit()
        return {
            "promoted": promoted,
            "conflicts": conflicts,
            "skipped": skipped,
            "failed": failed,
            "run_id": run_id,
        }
    finally:
        conn.close()


def _merge_one(
    conn,
    *,
    run_id: int,
    source_id: int,
    source_watermark: str | None,
    item: dict[str, Any],
    mode: str,
) -> tuple[str, int | None, int | None]:
    """Apply the base merge rules to one item. Returns (result, staging_id, conflict_id)."""
    if mode == "incremental" and not eligible_for_incremental(
        item["event_ts"], source_watermark, 0
    ):
        staging_id = runs_repo.insert_staging(
            conn,
            run_id=run_id,
            source_id=source_id,
            item=item,
            disposition="skipped_watermark",
        )
        return "skipped_watermark", staging_id, None

    existing = canon_repo.get_by_key(conn, item["business_key"])
    existing_view = None
    if existing:
        existing_view = {
            "business_key": existing["business_key"],
            "title": existing["title"],
            "price": existing["price"],
            "category": existing["category"],
            "event_ts": existing["event_ts"],
            "source_revision": existing["source_revision"],
            "payload": existing["payload"],
        }

    decision = decide_merge(existing_view, item)
    if decision in ("insert", "upsert"):
        canon_repo.upsert_item(conn, item, source_id)
        staging_id = runs_repo.insert_staging(
            conn, run_id=run_id, source_id=source_id, item=item, disposition=decision
        )
        return decision, staging_id, None
    if decision == "conflict":
        conflict_id = runs_repo.insert_conflict(
            conn,
            run_id=run_id,
            source_id=source_id,
            business_key=item["business_key"],
            existing_payload=existing_view["payload"] if existing_view else {},
            incoming_payload=item["payload"],
            reason="non_key_field_mismatch",
        )
        staging_id = runs_repo.insert_staging(
            conn,
            run_id=run_id,
            source_id=source_id,
            item=item,
            disposition="quarantine",
        )
        return "conflict", staging_id, conflict_id

    staging_id = runs_repo.insert_staging(
        conn, run_id=run_id, source_id=source_id, item=item, disposition="skip_stale"
    )
    return "skip_stale", staging_id, None


def _item_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "business_key": str(payload["business_key"]).strip(),
        "title": str(payload["title"]).strip(),
        "price": float(payload["price"]),
        "category": str(payload["category"]).strip(),
        "event_ts": str(payload["event_ts"]).strip(),
        "source_revision": int(payload["source_revision"]),
        "payload": dict(payload),
    }


def _build_report(conn, batch_id: int) -> dict[str, Any]:
    """Derive every number from rpa_steps, then cross-check against the other tables."""
    summary = rpa_repo.summarize_batch(conn, batch_id)
    run_rows = conn.execute(
        "SELECT DISTINCT run_id FROM rpa_steps WHERE batch_id = ? AND run_id IS NOT NULL",
        (batch_id,),
    ).fetchall()
    run_ids = [r["run_id"] for r in run_rows]
    if run_ids:
        placeholders = ",".join("?" for _ in run_ids)
        staging_actual = conn.execute(
            f"SELECT COUNT(*) c FROM staging_rows WHERE run_id IN ({placeholders})",
            run_ids,
        ).fetchone()["c"]
        conflict_actual = conn.execute(
            f"SELECT COUNT(*) c FROM conflicts WHERE run_id IN ({placeholders})",
            run_ids,
        ).fetchone()["c"]
    else:
        staging_actual = 0
        conflict_actual = 0

    intake_rows = conn.execute(
        "SELECT status, COUNT(*) c FROM intake_rows WHERE batch_id = ? GROUP BY status",
        (batch_id,),
    ).fetchall()
    intake_by_status = {r["status"]: r["c"] for r in intake_rows}
    promote_failed = intake_by_status.get(rpa_repo.INTAKE_FAILED, 0)

    # Every successful merge step writes exactly one staging row; intake-held and
    # failed steps do not. Promote-failed steps are already counted under `failed`,
    # so they are not subtracted again here.
    staging_expected = summary["success"] - summary["intake_held"]
    conflict_expected = summary["conflict_steps"] + summary["promoted_conflicts"]
    canonical_writes = summary["canonical_writes"] + summary["promoted_writes"]

    report = {
        **summary,
        "canonical_writes_total": canonical_writes,
        "staging_rows": staging_actual,
        "conflict_rows": conflict_actual,
        "intake_held_rows": intake_by_status.get(rpa_repo.INTAKE_HELD, 0),
        "intake_promoted_rows": intake_by_status.get(rpa_repo.INTAKE_PROMOTED, 0),
        "intake_failed_rows": promote_failed,
        "canonical_count_now": canon_repo.count_items(conn),
        "cross_check": {
            "staging_matches": staging_actual == staging_expected,
            "conflict_matches": conflict_actual == conflict_expected,
            "step_total_matches": summary["step_total"]
            == summary["pending"] + summary["success"] + summary["failed"],
            "outcome_matches": summary["success"]
            == canonical_writes
            + conflict_expected
            + summary["intake_held"]
            + summary["skipped"],
        },
    }
    report["reconciles"] = all(report["cross_check"].values())
    return report
