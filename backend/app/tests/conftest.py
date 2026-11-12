import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    """Point every app.db client at a fresh SQLite file under tmp_path."""
    db_file = tmp_path / "rpa_test.db"

    def fake_connect():
        import sqlite3

        conn = sqlite3.connect(db_file)
        conn.row_factory = sqlite3.Row
        return conn

    import app.db as db_module
    import app.seed as seed_module
    from app.services import rpa_service

    monkeypatch.setattr(db_module, "DB_PATH", db_file)
    # Only modules that call connect() themselves need patching; repositories take
    # the connection as an argument and the services import connect by name.
    monkeypatch.setattr(seed_module, "connect", fake_connect)
    monkeypatch.setattr(rpa_service, "connect", fake_connect)
    monkeypatch.setattr("app.services.ingest_service.connect", fake_connect)

    seed_module.init_db()
    return db_file
