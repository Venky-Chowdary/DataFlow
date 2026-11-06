"""E2-023 review — one seam reports the images an upsert collapsed, on every writer.

Gate-8 expects one row per key from an upsert; the writer must say how many
older images it dropped (``collapsed_duplicate_rows`` in ``WriteResult.meta``)
or a correct 3-versions-per-key load fails "expected 30 vs 10". The SQL
writers stamp it through ``SqlWriteAccumulator.gate8_meta``; the Mongo writer
counts what its per-batch dedupe dropped. The ladder and the source re-read
pick the same survivor as the writer (``duplicate_policy.latest_version_per_key``).
"""

from __future__ import annotations

import socket
import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def _up(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1):
            return True
    except OSError:
        return False


_ROWS = [["1", "first"], ["2", "two"], ["1", "latest"]]
_COMMON = {
    "headers": ["id", "name"],
    "mappings": [_mapping("id", "id"), _mapping("name", "name")],
    "column_types": {"id": "INTEGER", "name": "VARCHAR"},
    "create_table": True,
    "write_mode": "upsert",
    "conflict_columns": ["id"],
}


def _assert_collapsed(result, rows_expected: int = 2):
    assert result.ok, result.error
    assert result.meta.get("collapsed_duplicate_rows") == 1, result.meta
    assert result.rows_written == rows_expected


def test_sqlite_writer_reports_the_collapse(tmp_path):
    from connectors.sqlite_writer import write_mapped_rows

    res = write_mapped_rows(host="", port=0, database=str(tmp_path / "d.db"), username="", password="",
                            schema="main", connection_string="", ssl=False,
                            table_name="t_" + uuid.uuid4().hex[:8], data_rows=list(_ROWS), **_COMMON)
    _assert_collapsed(res)


def test_postgresql_writer_reports_the_collapse():
    import psycopg2

    try:
        psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow",
                         password="dataflow", connect_timeout=2).close()
    except Exception as exc:  # noqa: BLE001 — a foreign PG on :5432 refuses these credentials
        pytest.skip(f"project PostgreSQL not reachable: {exc}")
    from connectors.postgresql_writer import write_mapped_rows

    table = "col_pg_" + uuid.uuid4().hex[:8]
    res = write_mapped_rows(host="127.0.0.1", port=5432, database="dataflow", username="dataflow",
                            password="dataflow", schema="public", connection_string="", ssl=False,
                            table_name=table, data_rows=list(_ROWS), **_COMMON)
    try:
        _assert_collapsed(res)
    finally:
        import psycopg2

        c = psycopg2.connect(host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow", password="dataflow")
        c.autocommit = True
        c.cursor().execute(f'DROP TABLE IF EXISTS "{table}"')
        c.close()


def test_mysql_writer_reports_the_collapse():
    if not _up("127.0.0.1", 3306):
        pytest.skip("MySQL not reachable")
    from connectors.mysql_writer import write_mapped_rows

    table = "col_my_" + uuid.uuid4().hex[:8]
    res = write_mapped_rows(host="127.0.0.1", port=3306, database="dataflow", username="dataflow",
                            password="dataflow", schema="dataflow", connection_string="", ssl=False,
                            table_name=table, data_rows=list(_ROWS), **_COMMON)
    try:
        _assert_collapsed(res)
    finally:
        import pymysql

        c = pymysql.connect(host="127.0.0.1", port=3306, user="dataflow", password="dataflow", database="dataflow", autocommit=True)
        c.cursor().execute(f"DROP TABLE IF EXISTS `{table}`")
        c.close()


def test_mongodb_writer_reports_the_collapse():
    if not _up("127.0.0.1", 27017):
        pytest.skip("MongoDB not reachable")
    from connectors.mongodb_writer import write_mapped_rows

    coll = "col_mg_" + uuid.uuid4().hex[:8]
    res = write_mapped_rows(host="127.0.0.1", port=27017, database="dataflow_r13", username="", password="",
                            schema="", connection_string="", ssl=False, table_name=coll,
                            data_rows=list(_ROWS), **_COMMON)
    try:
        _assert_collapsed(res)
        from pymongo import MongoClient

        docs = {d["id"]: d["name"] for d in MongoClient("mongodb://127.0.0.1:27017")["dataflow_r13"][coll].find({}, {"_id": 0})}
        assert docs[1] == "latest" and len(docs) == 2
    finally:
        from pymongo import MongoClient

        MongoClient("mongodb://127.0.0.1:27017")["dataflow_r13"][coll].drop()


def test_snowflake_writer_reports_the_collapse(tmp_path, monkeypatch):
    pytest.importorskip("fakesnow")
    (tmp_path / "fakesnow").mkdir()
    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    from connectors.snowflake_writer import write_mapped_rows

    res = write_mapped_rows(host="localhost", port=443, database="DATAFLOW", username="test", password="test",
                            schema="PUBLIC", connection_string="", ssl=False, warehouse="WH",
                            table_name="COL_SF_" + uuid.uuid4().hex[:8].upper(), data_rows=list(_ROWS), **_COMMON)
    _assert_collapsed(res)


# --- the survivor the ladder and the re-read pick (item 3) -----------------

def test_latest_version_per_key_uses_the_typed_cursor_order():
    from services.duplicate_policy import latest_version_per_key

    rows = [
        {"k": "a", "cur": "1000", "v": "late-by-value"},
        {"k": "a", "cur": "999", "v": "early-but-lexically-larger"},
        {"k": "b", "cur": SQL_NULL_SENTINEL, "v": "null-cursor"},
        {"k": "b", "cur": "5", "v": "real"},
        {"k": "c", "cur": "7", "v": "tie-first"},
        {"k": "c", "cur": "7", "v": "tie-last"},
        {"k": "d", "cur": None, "v": "only-null"},
    ]
    kept = {r["k"]: r["v"] for r in latest_version_per_key(rows, cursor_column="cur", key_columns=["k"])}
    assert kept["a"] == "late-by-value", "999 < 1000 by value, not by text"
    assert kept["b"] == "real", "a NULL cursor is the lowest, never the winner"
    assert kept["c"] == "tie-last", "equal (cursor, key) keep arrival order, as the writer does"
    assert kept["d"] == "only-null"
    ts = [{"k": 1, "cur": "2026-01-02T10:00:00+00:00", "v": "utc"},
          {"k": 1, "cur": "2026-01-02T05:00:00-06:00", "v": "later-instant"}]
    assert latest_version_per_key(ts, cursor_column="cur", key_columns=["k"])[0]["v"] == "later-instant"
    # No cursor: plain keep-last in arrival order.
    assert [r["v"] for r in latest_version_per_key(rows[:2], cursor_column="", key_columns=["k"])] == ["early-but-lexically-larger"]


# --- per-stream decision and the ordered-log assumption (item 5) ----------

def test_latest_wins_is_decided_per_stream():
    from services.primary_key import latest_wins_dedupe, latest_wins_note

    with_cursor = {"name": "a", "selected": True, "cursor_field": "updated_at"}
    without = {"name": "b", "selected": True}
    both = [with_cursor, without]
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=both) == "", "a cursorless stream still refuses"
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=both, stream_name="a") == "keep_last"
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=both, stream_name="b") == ""
    assert latest_wins_dedupe("", "incremental_deduped", stream_contracts=[with_cursor]) == "keep_last"
    assert "partition" in latest_wins_note("kafka") and latest_wins_note("postgresql") == ""


def test_the_integrity_audit_passes_the_stream_name_through():
    """F10: the per-stream rule only runs if the audit names its stream."""
    import inspect

    import services.data_integrity as di
    import services.preflight_context as pc

    assert "stream_name=stream_name" in inspect.getsource(di.run_integrity_audit)
    assert "stream_name=str(getattr(self.plan.source" in inspect.getsource(pc)
