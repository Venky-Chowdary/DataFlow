"""With FAKESNOW_KEEP_PATCH (CI) a new FAKESNOW_DB_PATH still gets its own catalog.

CI keeps the product's fakesnow patch for the whole process
(DATAFLOW_FAKESNOW_KEEP_PATCH=1): the last close leaves the patch active with
the refcount at 0. The re-install for a different catalog path only ran
while the refcount was above 0, so every later test that set its own
FAKESNOW_DB_PATH (per-test isolation) wrote into the first test's catalog.
In api-tests 5/6 a PEOPLE__DF_BAK left there made
test_overwrite_all_quarantined_keeps_set_aside_destination
test_multi_stream_overwrite_settles_each_streams_backup refuse its first run
(reproduced locally after test_nf1_overwrite_keeps_security_metadata with the
flag on).
"""

from __future__ import annotations

import pytest


def _tables(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'PUBLIC'")
        return {str(r[0]).upper() for r in cur.fetchall()}


def _connect():
    from connectors.snowflake_conn import get_connection

    return get_connection(account="localhost", username="test", password="test",
                          database="dataflow", schema="PUBLIC", warehouse="", connection_string="")


def test_a_second_catalog_path_is_a_second_catalog(tmp_path, monkeypatch) -> None:
    pytest.importorskip("fakesnow")
    monkeypatch.setenv("DATAFLOW_FAKESNOW_KEEP_PATCH", "1")
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "first"))
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE LEFT_BEHIND (id INT)")
        assert "LEFT_BEHIND" in _tables(conn)
    finally:
        conn.close()

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "second"))
    conn = _connect()
    try:
        assert "LEFT_BEHIND" not in _tables(conn)
    finally:
        conn.close()
