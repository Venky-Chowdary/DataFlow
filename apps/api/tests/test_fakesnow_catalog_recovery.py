"""fakesnow DuckDB catalog must recover from version skew / corruption."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_fakesnow_catalog_error_detection():
    from connectors.snowflake_conn import _is_fakesnow_catalog_error

    assert _is_fakesnow_catalog_error(
        RuntimeError("Serialization Error: Failed to deserialize: expected end of object, but found field id: 202")
    )
    assert _is_fakesnow_catalog_error(
        RuntimeError('IO Error: The file "x.db" exists, but it is not a valid DuckDB database file!')
    )
    assert not _is_fakesnow_catalog_error(RuntimeError("authentication failed"))


def test_get_connection_recovers_corrupt_fakesnow_catalog(tmp_path, monkeypatch):
    from connectors import snowflake_conn

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path))
    # Ensure no leaked product patch from other tests.
    with snowflake_conn._fakesnow_lock:
        snowflake_conn._fakesnow_refcount = 0
        if snowflake_conn._fakesnow_patch_cm is not None:
            try:
                snowflake_conn._fakesnow_patch_cm.__exit__(None, None, None)
            except Exception:
                pass
            snowflake_conn._fakesnow_patch_cm = None

    corrupt = tmp_path / "DATAFLOW.db"
    corrupt.write_bytes(b"CORRUPT_NOT_DUCKDB")

    conn = snowflake_conn.get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1")
        assert cur.fetchone()[0] == 1
    finally:
        conn.close()
