from app.db import connect


def init_db():
    conn = connect()
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS settings(
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS sources(
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            format TEXT NOT NULL,
            fixture_path TEXT NOT NULL,
            watermark_ts TEXT,
            lookback_seconds INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1,
            note TEXT
        );
        CREATE TABLE IF NOT EXISTS runs(
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            mode TEXT NOT NULL,
            status TEXT NOT NULL,
            parsed_count INTEGER NOT NULL,
            upserted_count INTEGER NOT NULL,
            conflict_count INTEGER NOT NULL,
            skipped_count INTEGER NOT NULL,
            report_json TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS staging_rows(
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            business_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            source_revision INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            disposition TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS canonical_items(
            id INTEGER PRIMARY KEY,
            business_key TEXT NOT NULL UNIQUE,
            title TEXT NOT NULL,
            price REAL NOT NULL,
            category TEXT NOT NULL,
            source_id INTEGER NOT NULL,
            event_ts TEXT NOT NULL,
            source_revision INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS conflicts(
            id INTEGER PRIMARY KEY,
            run_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            business_key TEXT NOT NULL,
            existing_payload TEXT NOT NULL,
            incoming_payload TEXT NOT NULL,
            reason TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rpa_batches(
            id INTEGER PRIMARY KEY,
            source_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            target TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS rpa_steps(
            id INTEGER PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            seq INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            business_key TEXT,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            result TEXT,
            error TEXT,
            run_id INTEGER,
            staging_id INTEGER,
            conflict_id INTEGER,
            intake_id INTEGER,
            attempted_at TEXT,
            finished_at TEXT,
            UNIQUE(batch_id, seq)
        );
        CREATE TABLE IF NOT EXISTS intake_rows(
            id INTEGER PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            step_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            business_key TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            source_revision INTEGER NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL,
            run_id INTEGER,
            staging_id INTEGER,
            conflict_id INTEGER,
            created_at TEXT NOT NULL,
            promoted_at TEXT
        );
        """
    )
    if conn.execute("SELECT COUNT(*) c FROM sources").fetchone()["c"] == 0:
        conn.executemany(
            """
            INSERT INTO sources(name, format, fixture_path, watermark_ts, lookback_seconds, enabled, note)
            VALUES (?,?,?,?,?,?,?)
            """,
            [
                (
                    "Alpha Catalog",
                    "json",
                    "alpha_catalog.json",
                    None,
                    0,
                    1,
                    "clean：三件独立货号，首跑应全部 upsert",
                ),
                (
                    "Beta Offers",
                    "jsonl",
                    "beta_offers.jsonl",
                    None,
                    0,
                    1,
                    "dirty：SKU-1001 与 Alpha 同键异价→冲突；另含独立货号",
                ),
                (
                    "Gamma Shelf",
                    "xml",
                    "gamma_shelf.xml",
                    None,
                    0,
                    1,
                    "xml 源：两件独立货号",
                ),
                (
                    "Delta RPA Clicks",
                    "jsonl",
                    "delta_rpa_clicks.jsonl",
                    None,
                    0,
                    1,
                    "RPA 演示：含新货号、同键异价冲突行与一条坏行（逐步失败）",
                ),
            ],
        )
        conn.execute("INSERT INTO settings(key, value) VALUES ('project_label', 'Feedstitch')")
        conn.execute("INSERT INTO settings(key, value) VALUES ('default_mode', 'incremental')")
        conn.commit()
    elif conn.execute(
        "SELECT COUNT(*) c FROM sources WHERE fixture_path = 'delta_rpa_clicks.jsonl'"
    ).fetchone()["c"] == 0:
        # Idempotent seed for databases created before the RPA queue existed.
        conn.execute(
            """
            INSERT INTO sources(name, format, fixture_path, watermark_ts, lookback_seconds, enabled, note)
            VALUES ('Delta RPA Clicks', 'jsonl', 'delta_rpa_clicks.jsonl', NULL, 0, 1,
                    'RPA 演示：含新货号、同键异价冲突行与一条坏行（逐步失败）')
            """
        )
        conn.commit()
    conn.close()
