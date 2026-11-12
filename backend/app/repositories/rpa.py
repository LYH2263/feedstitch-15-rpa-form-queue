"""Persistence for the local RPA form-entry queue.

Two durable tables back the RPA link:
- ``rpa_batches``: one generated queue per fixture source, with an execution pointer.
- ``rpa_steps``: one form-fill step per fixture row (status pending/success/failed).

A third table, ``intake_rows``, is the optional pre-ingest holding area used when a
batch targets ``intake`` instead of merging straight into canonical data.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any

STEP_PENDING = "pending"
STEP_SUCCESS = "success"
STEP_FAILED = "failed"

BATCH_READY = "ready"
BATCH_RUNNING = "running"
BATCH_DONE = "done"

INTAKE_HELD = "held"
INTAKE_PROMOTED = "promoted"
INTAKE_FAILED = "promote_failed"


def create_batch(conn: sqlite3.Connection, source_id: int, target: str) -> int:
    cur = conn.execute(
        """
        INSERT INTO rpa_batches(source_id, status, target, created_at, updated_at)
        VALUES (?, 'ready', ?, datetime('now'), datetime('now'))
        """,
        (source_id, target),
    )
    return int(cur.lastrowid)


def get_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT b.id, b.source_id, s.name AS source_name, b.status, b.target,
               b.created_at, b.updated_at
        FROM rpa_batches b JOIN sources s ON s.id = b.source_id
        WHERE b.id = ?
        """,
        (batch_id,),
    ).fetchone()
    return dict(row) if row else None


def list_batches(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT b.id, b.source_id, s.name AS source_name, b.status, b.target,
               b.created_at, b.updated_at,
               COUNT(st.id) AS step_total,
               SUM(CASE WHEN st.status = 'pending' THEN 1 ELSE 0 END) AS step_pending,
               SUM(CASE WHEN st.status = 'success' THEN 1 ELSE 0 END) AS step_success,
               SUM(CASE WHEN st.status = 'failed' THEN 1 ELSE 0 END) AS step_failed
        FROM rpa_batches b
        JOIN sources s ON s.id = b.source_id
        LEFT JOIN rpa_steps st ON st.batch_id = b.id
        GROUP BY b.id
        ORDER BY b.id DESC
        """
    ).fetchall()
    return [dict(r) for r in rows]


def set_batch_status(conn: sqlite3.Connection, batch_id: int, status: str) -> None:
    conn.execute(
        "UPDATE rpa_batches SET status = ?, updated_at = datetime('now') WHERE id = ?",
        (status, batch_id),
    )


def insert_step(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    seq: int,
    source_id: int,
    business_key: str | None,
    payload: dict[str, Any],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO rpa_steps(
            batch_id, seq, source_id, business_key, payload_json, status
        ) VALUES (?,?,?,?,?, 'pending')
        """,
        (batch_id, seq, source_id, business_key, json.dumps(payload, ensure_ascii=False)),
    )
    return int(cur.lastrowid)


def list_steps(conn: sqlite3.Connection, batch_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM rpa_steps WHERE batch_id = ? ORDER BY seq",
        (batch_id,),
    ).fetchall()
    return [_hydrate_step(r) for r in rows]


def get_step(conn: sqlite3.Connection, step_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM rpa_steps WHERE id = ?", (step_id,)).fetchone()
    return _hydrate_step(row) if row else None


def next_pending_steps(conn: sqlite3.Connection, batch_id: int, limit: int | None) -> list[dict[str, Any]]:
    sql = "SELECT * FROM rpa_steps WHERE batch_id = ? AND status = 'pending' ORDER BY seq"
    params: tuple[Any, ...] = (batch_id,)
    if limit is not None:
        sql += " LIMIT ?"
        params = (batch_id, limit)
    rows = conn.execute(sql, params).fetchall()
    return [_hydrate_step(r) for r in rows]


def mark_step_outcome(
    conn: sqlite3.Connection,
    step_id: int,
    *,
    status: str,
    result: str | None = None,
    error: str | None = None,
    run_id: int | None = None,
    staging_id: int | None = None,
    conflict_id: int | None = None,
    intake_id: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE rpa_steps SET
            status = ?, result = ?, error = ?, run_id = ?, staging_id = ?,
            conflict_id = ?, intake_id = ?, attempted_at = COALESCE(attempted_at, datetime('now')),
            finished_at = datetime('now')
        WHERE id = ?
        """,
        (status, result, error, run_id, staging_id, conflict_id, intake_id, step_id),
    )


def insert_intake_row(
    conn: sqlite3.Connection,
    *,
    batch_id: int,
    step_id: int,
    source_id: int,
    item: dict[str, Any],
) -> int:
    cur = conn.execute(
        """
        INSERT INTO intake_rows(
            batch_id, step_id, source_id, business_key, event_ts, source_revision,
            payload_json, status, created_at
        ) VALUES (?,?,?,?,?,?,?, 'held', datetime('now'))
        """,
        (
            batch_id,
            step_id,
            source_id,
            item["business_key"],
            item["event_ts"],
            item["source_revision"],
            json.dumps(item["payload"], ensure_ascii=False),
        ),
    )
    return int(cur.lastrowid)


def list_intake_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute("SELECT * FROM intake_rows ORDER BY id DESC LIMIT 200").fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json"))
        out.append(d)
    return out


def get_intake_row(conn: sqlite3.Connection, intake_id: int) -> dict[str, Any] | None:
    row = conn.execute("SELECT * FROM intake_rows WHERE id = ?", (intake_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    d["payload"] = json.loads(d.pop("payload_json"))
    return d


def mark_intake_outcome(
    conn: sqlite3.Connection,
    intake_id: int,
    *,
    status: str,
    run_id: int | None = None,
    staging_id: int | None = None,
    conflict_id: int | None = None,
) -> None:
    conn.execute(
        """
        UPDATE intake_rows SET status = ?, run_id = ?, staging_id = ?, conflict_id = ?,
            promoted_at = datetime('now')
        WHERE id = ?
        """,
        (status, run_id, staging_id, conflict_id, intake_id),
    )


def summarize_batch(conn: sqlite3.Connection, batch_id: int) -> dict[str, Any]:
    """Single source of truth for the batch report: every number derives from rpa_steps."""
    row = conn.execute(
        """
        SELECT
            COUNT(*) AS step_total,
            SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
            SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS success,
            SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status = 'success' AND result IN ('insert', 'upsert') THEN 1 ELSE 0 END) AS canonical_writes,
            SUM(CASE WHEN status = 'success' AND result = 'conflict' THEN 1 ELSE 0 END) AS conflict_steps,
            SUM(CASE WHEN status = 'success' AND result = 'intake_held' THEN 1 ELSE 0 END) AS intake_held,
            SUM(CASE WHEN status = 'success' AND result = 'promoted_insert' THEN 1 ELSE 0 END) AS promoted_writes,
            SUM(CASE WHEN status = 'success' AND result = 'promoted_conflict' THEN 1 ELSE 0 END) AS promoted_conflicts,
            SUM(CASE WHEN status = 'success' AND result IN ('skip_stale', 'skipped_watermark') THEN 1 ELSE 0 END) AS skipped
        FROM rpa_steps WHERE batch_id = ?
        """,
        (batch_id,),
    ).fetchone()
    return {k: (int(v) if v is not None else 0) for k, v in dict(row).items()}


def _hydrate_step(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    d["payload"] = json.loads(d.pop("payload_json") or "{}")
    return d
