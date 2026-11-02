"""D177 — MySQL ``cdc_incremental`` ignored the declared ``source_timezone``.

Preflight accepted ``source_timezone=UTC``; the run then failed with
"TIMESTAMPTZ refuses naive wall-clock" and left the empty destination table it
had created. Three independent causes, each pinned here:

* a MySQL ``TIMESTAMP`` reached the writer as zoneless wall-clock on the binlog
  and keyset paths (the catalog type is not in a binlog row and only the reader
  scan marked the instant), so a ``TIMESTAMPTZ`` column refused it;
* a stream that borrowed the job's column map (``mappings_inherited``) fell back
  to an identity map when its column set differed, which dropped the
  ``assume_timezone`` transform the declaration had stamped;
* a first CDC run that failed before landing a row kept the table it created.
"""

from __future__ import annotations

import socket
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.mysql_change_stream as mcs  # noqa: E402
from services.multi_stream_plan import adopt_inherited_mappings  # noqa: E402
from src.transfer.cdc_transfer import _align_inherited_batch  # noqa: E402

ZONE = "assume_timezone:UTC"
HEADERS = ["id", "ts", "tstz"]


# ---------------------------------------------------------------------------
# Unit: the declared zone survives an identity fallback
# ---------------------------------------------------------------------------


def _inherited() -> list[dict[str, Any]]:
    return [
        {"source": "id", "target": "id", "transform": "integer", "target_type": "BIGINT"},
        {"source": "ts", "target": "ts", "transform": ZONE, "target_type": "TIMESTAMPTZ(0)"},
        {"source": "tstz", "target": "tstz", "transform": "datetime"},
    ]


def test_identity_fallback_keeps_the_declared_zone_transform():
    mapped, note = adopt_inherited_mappings(_inherited(), ["id", "ts", "tstz", "extra"])
    by_source = {m["source"]: m for m in mapped}
    assert set(by_source) == {"id", "ts", "tstz", "extra"}
    assert by_source["ts"]["transform"] == ZONE
    # Only the declaration's own transform is carried: another table's cast
    # (``integer``) is not applied to a column it was not declared for.
    assert not by_source["id"].get("transform")
    assert not by_source["extra"].get("transform")
    assert "extra" in note or "omits" in note


def test_an_empty_inherited_map_is_still_a_plain_identity_map():
    mapped, note = adopt_inherited_mappings([], ["id", "ts"])
    assert [m["source"] for m in mapped] == ["id", "ts"]
    assert not any(m.get("transform") for m in mapped)
    assert note == ""


def test_a_matching_column_set_keeps_the_map_untouched():
    mapped, note = adopt_inherited_mappings(_inherited(), ["id", "ts", "tstz"])
    assert mapped == _inherited()
    assert note == ""


def test_align_inherited_batch_keeps_the_zone_when_it_rebuilds_the_map():
    from services.cdc_engine import ChangeBatch

    change = ChangeBatch(inserts=[{"id": "1", "ts": "2024-01-01T00:00:00", "extra": "x"}])
    mappings, headers, _pk, _note = _align_inherited_batch(
        _inherited(), [], change, "id", ["id"], {}
    )
    assert headers == ["id", "ts", "extra"]
    assert {m["source"]: m.get("transform") for m in mappings}["ts"] == ZONE


# ---------------------------------------------------------------------------
# Unit: a MySQL TIMESTAMP is a UTC instant on every CDC path
# ---------------------------------------------------------------------------


class _Cur:
    """Catalog answers for the type lookup; page rows for the keyset SELECT."""

    TYPES = [("id", "int"), ("ts", "datetime"), ("tstz", "timestamp")]

    def __init__(self, page: list[tuple] | None = None) -> None:
        self.page = page or []
        self._last = ""
        self.description = [("id",), ("ts",), ("tstz",)]

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False

    def execute(self, sql: str, params: Any = None) -> None:
        self._last = sql

    def fetchall(self):
        if "information_schema" in self._last:
            return list(self.TYPES)
        return list(self.page)


class _Conn:
    def __init__(self, page: list[tuple] | None = None) -> None:
        self.page = page

    def cursor(self) -> _Cur:
        return _Cur(self.page)

    def close(self) -> None:
        return None


def _cdc() -> mcs.MySqlChangeStreamCdc:
    cdc = mcs.MySqlChangeStreamCdc(
        {"host": "h", "database": "d", "username": "u", "password": "x"},
        "t",
        "id",
        columns=HEADERS,
    )
    cdc._conn = lambda: _Conn()  # type: ignore[method-assign]
    return cdc


NAIVE = datetime(2024, 1, 2, 3, 4, 5)


def test_binlog_timestamp_is_utc_and_datetime_stays_wall_clock():
    record = _cdc()._row_to_record({"id": 1, "ts": NAIVE, "tstz": NAIVE})
    assert record["ts"] == "2024-01-02T03:04:05"  # DATETIME: zoneless wall clock
    assert record["tstz"] == "2024-01-02T03:04:05+00:00"  # TIMESTAMP: the instant


def test_binlog_timestamp_null_stays_null():
    from services.value_serializer import SQL_NULL_SENTINEL

    record = _cdc()._row_to_record({"id": 1, "ts": None, "tstz": None})
    assert record["ts"] == SQL_NULL_SENTINEL
    assert record["tstz"] == SQL_NULL_SENTINEL


def test_keyset_page_timestamp_is_utc():
    cdc = _cdc()
    page = cdc._mysql_keyset_page(
        "t", ["id"], "0", _Conn([(1, NAIVE, NAIVE), (2, None, None)])
    )
    assert page[0]["ts"] == NAIVE
    assert page[0]["tstz"] == datetime(2024, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    assert page[1]["tstz"] is None


# ---------------------------------------------------------------------------
# Live: MySQL binlog → PostgreSQL through the CDC executor
# ---------------------------------------------------------------------------

MYSQL = {
    "host": "localhost",
    "port": 3306,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
}
PG = {
    "host": "localhost",
    "port": 5432,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "connection_string": "",
    "ssl": False,
    "schema": "public",
}


def _mysql():
    from connectors.mysql_conn import get_connection

    return get_connection(**MYSQL)


def _pg():
    import psycopg2

    return psycopg2.connect(
        host="localhost", port=5432, dbname="dataflow", user="dataflow",
        password="dataflow", connect_timeout=5,
    )


def _live_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        for port in (3306, 5432):
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        conn = _mysql()
        try:
            with conn.cursor() as cur:
                cur.execute("SHOW VARIABLES LIKE 'binlog_format'")
                row = cur.fetchone()
                if not (row and str(row[1]).upper() == "ROW"):
                    return False
        finally:
            conn.close()
        _pg().close()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(
    not _live_ready(), reason="MySQL ROW binlog or PostgreSQL dataflow/dataflow not reachable"
)


def _mysql_exec(*statements: str) -> None:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            for sql in statements:
                cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _pg_exec(sql: str) -> None:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
    finally:
        conn.close()


def _pg_query(sql: str) -> list[tuple]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return list(cur.fetchall())
    finally:
        conn.close()


def _pg_table_exists(table: str) -> bool:
    return bool(_pg_query(f"SELECT to_regclass('public.{table}') IS NOT NULL")[0][0])


def _run(table: str, mappings: list[dict], schema: dict[str, str], job_id: str, **kw):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    src = EndpointConfig(kind="database", format="mysql", table=table, **MYSQL)
    dst = EndpointConfig(kind="database", format="postgresql", table=table, **PG)
    stream = [{
        "name": table, "selected": True, "snapshot_mode": "initial",
        "primary_key": "id", "sync_mode": "cdc",
    }]
    return run_cdc_database_transfer(
        src, dst, mappings, schema, sync_mode="cdc", stream_contracts=stream,
        job_id=job_id, **kw,
    )


#: What the engine hands the CDC executor for ``source_timezone=UTC``.
TZ_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INT", "target_type": "BIGINT"},
    {"source": "ts", "target": "ts", "source_type": "DATETIME",
     "target_type": "TIMESTAMPTZ(0)", "transform": ZONE},
    {"source": "tstz", "target": "tstz", "source_type": "TIMESTAMP",
     "target_type": "TIMESTAMPTZ", "transform": "datetime"},
    {"source": "upd", "target": "upd", "source_type": "DATETIME",
     "target_type": "TIMESTAMPTZ(6)", "transform": ZONE},
]
TZ_SCHEMA = {"id": "INTEGER", "ts": "TIMESTAMP", "tstz": "TIMESTAMP", "upd": "TIMESTAMP"}


@live
def test_live_cdc_with_a_declared_zone_lands_datetime_and_timestamp_and_nulls():
    table = "d177_" + uuid.uuid4().hex[:8]
    job_id = "d177-" + uuid.uuid4().hex[:8]
    _mysql_exec(
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id INT PRIMARY KEY, ts DATETIME, tstz TIMESTAMP NULL, "
        "upd DATETIME(6))",
        f"INSERT INTO {table} VALUES "
        "(1,'2024-01-02 03:04:05','2024-01-02 03:04:05','2024-01-02 03:04:05.123456'),"
        "(2,NULL,NULL,NULL)",
    )
    _pg_exec(f"DROP TABLE IF EXISTS {table}")
    try:
        rows1, _ddl, summary1, _h = _run(table, TZ_MAPPINGS, TZ_SCHEMA, job_id)
        assert rows1 == 2, summary1
        _mysql_exec(
            f"INSERT INTO {table} VALUES (3,'2025-05-05 05:05:05','2025-05-05 05:05:05',"
            "'2025-05-05 05:05:05.5'),(4,NULL,NULL,NULL)",
            f"UPDATE {table} SET ts='2026-06-06 06:06:06' WHERE id=1",
        )
        _run(table, TZ_MAPPINGS, TZ_SCHEMA, job_id)  # binlog stream pass
        got = {
            r[0]: r[1:]
            for r in _pg_query(
                f"SELECT id, ts AT TIME ZONE 'UTC', tstz AT TIME ZONE 'UTC', "
                f"upd AT TIME ZONE 'UTC' FROM {table} ORDER BY id"
            )
        }
        assert got[1][0] == datetime(2026, 6, 6, 6, 6, 6)  # streamed UPDATE of a DATETIME
        assert got[1][1] == datetime(2024, 1, 2, 3, 4, 5)
        assert got[1][2] == datetime(2024, 1, 2, 3, 4, 5, 123456)
        assert got[2] == (None, None, None)  # snapshot all-NULL row
        assert got[3] == (
            datetime(2025, 5, 5, 5, 5, 5),
            datetime(2025, 5, 5, 5, 5, 5),
            datetime(2025, 5, 5, 5, 5, 5, 500000),
        )  # streamed INSERT: TIMESTAMP is the instant, DATETIME the declared zone
        assert got[4] == (None, None, None)  # streamed all-NULL row
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS {table}")
        _pg_exec(f"DROP TABLE IF EXISTS {table}")


# ---------------------------------------------------------------------------
# Live: a failed first run does not keep the table it created
# ---------------------------------------------------------------------------

BAD_MAPPINGS = [
    {"source": "id", "target": "id", "source_type": "INT", "target_type": "BIGINT"},
    {"source": "note", "target": "note", "source_type": "VARCHAR", "target_type": "INTEGER"},
]
BAD_SCHEMA = {"id": "INTEGER", "note": "INTEGER"}


def _fail_first_run(table: str, job_id: str) -> None:
    with pytest.raises(Exception):
        _run(table, BAD_MAPPINGS, BAD_SCHEMA, job_id, validation_mode="strict")


@live
def test_live_failed_first_run_drops_the_empty_table_it_created():
    table = "d177f_" + uuid.uuid4().hex[:8]
    _mysql_exec(
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20))",
        f"INSERT INTO {table} VALUES (1,'abc'),(2,'def')",
    )
    _pg_exec(f"DROP TABLE IF EXISTS {table}")
    try:
        _fail_first_run(table, "d177f-" + uuid.uuid4().hex[:8])
        assert not _pg_table_exists(table), "the run created this table and landed no row"
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS {table}")
        _pg_exec(f"DROP TABLE IF EXISTS {table}")


@live
def test_live_failed_run_keeps_a_table_that_already_existed():
    table = "d177k_" + uuid.uuid4().hex[:8]
    _mysql_exec(
        f"DROP TABLE IF EXISTS {table}",
        f"CREATE TABLE {table} (id INT PRIMARY KEY, note VARCHAR(20))",
        f"INSERT INTO {table} VALUES (1,'abc')",
    )
    _pg_exec(f"DROP TABLE IF EXISTS {table}")
    _pg_exec(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, note INTEGER, _df_lsn TEXT)")
    try:
        _fail_first_run(table, "d177k-" + uuid.uuid4().hex[:8])
        assert _pg_table_exists(table), "a table the run did not create is never dropped"
    finally:
        _mysql_exec(f"DROP TABLE IF EXISTS {table}")
        _pg_exec(f"DROP TABLE IF EXISTS {table}")
