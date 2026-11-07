"""D174 — a CDC snapshot wrote SQL NULL as the text ``__DF_SQL_NULL__``.

The MySQL snapshot has three page modes. ``keyset`` returns raw driver values
(``None`` for NULL); ``scan`` and ``offset`` return the *wire* strings of
``mysql_reader`` (NULL already spelled ``__DF_SQL_NULL__``), and the binlog
stream spells every event the same way. ``cdc_transfer._records_to_matrix``
then ran ``cell_to_string(..., preserve_sql_null=True)`` over each cell, so a
wire NULL was escaped into the *text* ``__DF_ESC____DF_SQL_NULL__`` (a text
column stored ``__DF_SQL_NULL__``, a typed column refused the run) and a user
literal was escaped twice.

Whatever the page mode, the matrix handed to the writer must carry a NULL as the
NULL sentinel exactly once and a user text ``__DF_SQL_NULL__`` as the escaped
literal exactly once.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import connectors.mysql_change_stream as mcs  # noqa: E402
from connectors.mysql_reader import _cell  # noqa: E402
from services.value_serializer import (  # noqa: E402
    ESCAPED_WIRE_PREFIX,
    SQL_NULL_SENTINEL,
    cell_to_string,
    is_reader_null_cell,
    unescape_wire_text,
)
from src.transfer.cdc_transfer import _records_to_matrix  # noqa: E402

LITERAL = "__DF_SQL_NULL__"
HEADERS = ["id", "n", "note"]
#: What the writer must receive for the two rows below, whatever the page mode.
EXPECTED = [
    ["1", SQL_NULL_SENTINEL, SQL_NULL_SENTINEL],
    ["2", "5", f"{ESCAPED_WIRE_PREFIX}{LITERAL}"],
]
#: The same two source rows as raw driver values (what ``keyset`` yields).
RAW_ROWS = [(1, None, None), (2, 5, LITERAL)]


def _cdc(**kw: Any) -> mcs.MySqlChangeStreamCdc:
    return mcs.MySqlChangeStreamCdc(
        {"host": "h", "database": "d", "username": "u", "password": "x"},
        "t",
        "id",
        columns=HEADERS,
        **kw,
    )


def _reader_batch() -> SimpleNamespace:
    """The shape ``read_table_scan_batch`` / ``read_table_batch`` return."""
    return SimpleNamespace(
        headers=list(HEADERS),
        rows=[[_cell(v) for v in row] for row in RAW_ROWS],
    )


def _pages(cdc: mcs.MySqlChangeStreamCdc, **kw: Any) -> list[dict[str, Any]]:
    batches = list(
        cdc._snapshot_table_pages(
            kw.pop("table", "t"), start_pos={}, table_offset=kw.pop("offset", 0),
            table_last_pk=kw.pop("last_pk", ""), lock_conn=None,
        )
    )
    return [rec for b in batches for rec in b.inserts]


def _scan(monkeypatch) -> list[list[str]]:
    monkeypatch.setattr(mcs, "read_table_scan_batch", lambda **_k: _reader_batch())
    return _records_to_matrix(_pages(_cdc()), HEADERS)


def _offset(monkeypatch) -> list[list[str]]:
    monkeypatch.setattr(mcs, "read_table_batch", lambda **_k: _reader_batch())
    return _records_to_matrix(_pages(_cdc(), offset=3), HEADERS)


def _keyset(monkeypatch) -> list[list[str]]:
    monkeypatch.setattr(
        mcs.MySqlChangeStreamCdc,
        "_mysql_keyset_page",
        lambda self, *a, **k: [dict(zip(HEADERS, row)) for row in RAW_ROWS],
    )
    return _records_to_matrix(_pages(_cdc(), last_pk="0"), HEADERS)


def _binlog(monkeypatch) -> list[list[str]]:
    cdc = _cdc()
    records = [cdc._row_to_record(dict(zip(HEADERS, row))) for row in RAW_ROWS]
    return _records_to_matrix(records, HEADERS)


@pytest.mark.parametrize("mode", [_scan, _offset, _keyset, _binlog], ids=lambda f: f.__name__[1:])
def test_every_mysql_cdc_page_mode_hands_the_writer_one_encoding(monkeypatch, mode):
    assert mode(monkeypatch) == EXPECTED


def test_the_writer_decodes_null_as_null_and_the_literal_as_text(monkeypatch):
    matrix = _scan(monkeypatch)
    assert matrix[0][1] == SQL_NULL_SENTINEL  # absence, not text
    assert unescape_wire_text(matrix[1][2]) == LITERAL
    assert not is_reader_null_cell(unescape_wire_text(matrix[1][2]))
    assert is_reader_null_cell(matrix[0][1])


def test_page_modes_agree_cell_for_cell(monkeypatch):
    scan, offset, keyset = _scan(monkeypatch), _offset(monkeypatch), _keyset(monkeypatch)
    assert scan == offset == keyset


def test_a_raw_value_is_still_encoded_once():
    """A reader that yields raw values keeps the single encode in the matrix."""
    assert _records_to_matrix([{"a": None, "b": LITERAL, "c": 7}], ["a", "b", "c"]) == [
        [SQL_NULL_SENTINEL, f"{ESCAPED_WIRE_PREFIX}{LITERAL}", "7"]
    ]


def test_cell_to_string_does_not_re_encode_a_marked_wire_cell():
    from services.value_serializer import wire_cell

    null_wire = wire_cell(None)
    literal_wire = wire_cell(LITERAL)
    assert cell_to_string(null_wire, preserve_sql_null=True) == SQL_NULL_SENTINEL
    assert cell_to_string(literal_wire, preserve_sql_null=True) == f"{ESCAPED_WIRE_PREFIX}{LITERAL}"
    # An unmarked string that merely spells the sentinel is user text: escaped.
    assert cell_to_string(LITERAL, preserve_sql_null=True) == f"{ESCAPED_WIRE_PREFIX}{LITERAL}"


# ---------------------------------------------------------------------------
# The other CDC connectors hand the engine the same wire cells
# ---------------------------------------------------------------------------


def _wire_matrix(records: list[dict[str, Any]]) -> list[list[str]]:
    return _records_to_matrix(records, HEADERS)


def test_sqlserver_change_tracking_snapshot_rows_are_encoded_once():
    from connectors.sqlserver_change_stream import SqlServerChangeTrackingCdc

    cdc = SqlServerChangeTrackingCdc({"database": "d"}, table="t", primary_key="id")
    records = [cdc._row_to_record(HEADERS, row) for row in RAW_ROWS]
    assert _wire_matrix(records) == EXPECTED


def test_oracle_flashback_snapshot_rows_are_encoded_once():
    from connectors.oracle_change_stream import OracleFlashbackCdc

    cdc = OracleFlashbackCdc({"username": "u"}, table="t", primary_key="id")
    upper = [h.upper() for h in HEADERS]
    records = [cdc._row_to_record(HEADERS, row) for row in RAW_ROWS]
    assert _records_to_matrix(records, upper) == EXPECTED


def test_postgres_pgoutput_null_is_encoded_once():
    from connectors.pgoutput_decoder import PgOutputDecoder, _ByteReader

    payload = (
        (3).to_bytes(2, "big")
        + b"t" + (1).to_bytes(4, "big") + b"1"
        + b"n"
        + b"t" + (len(LITERAL)).to_bytes(4, "big") + LITERAL.encode()
    )
    record = PgOutputDecoder()._read_tuple(_ByteReader(payload), HEADERS)
    assert _wire_matrix([record]) == [["1", SQL_NULL_SENTINEL, f"{ESCAPED_WIRE_PREFIX}{LITERAL}"]]


def test_postgres_test_decoding_null_is_encoded_once():
    from connectors.postgresql_change_stream import _parse_value

    record = {"id": _parse_value("1"), "n": _parse_value("null"), "note": _parse_value(f"'{LITERAL}'")}
    assert _wire_matrix([record]) == [["1", SQL_NULL_SENTINEL, f"{ESCAPED_WIRE_PREFIX}{LITERAL}"]]


def test_postgres_snapshot_pages_are_encoded_once(monkeypatch):
    import connectors.postgresql_change_stream as pcs
    import connectors.postgresql_reader as pr

    class Cur:
        description = [("id",), ("n",), ("note",)]

        def __init__(self) -> None:
            self.pages = [list(RAW_ROWS)]

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def execute(self, *_a, **_k):
            return None

        def fetchone(self):
            return ("0/1",)

        def fetchmany(self, _n):
            return self.pages.pop(0) if self.pages else []

        def fetchall(self):
            return []

    class Conn:
        autocommit = True

        def __enter__(self):
            return self

        def __exit__(self, *_a):
            return False

        def cursor(self):
            return Cur()

        def commit(self):
            return None

        def rollback(self):
            return None

    cdc = pcs.PostgreSqlChangeStreamCdc(
        {"host": "h", "database": "d"}, "t", "id", "k", columns=HEADERS
    )
    for name in ("_ensure_publication", "_ensure_replica_identity", "_ensure_slot",
                 "_ensure_decode_schema", "heartbeat"):
        monkeypatch.setattr(cdc, name, lambda *a, **k: None)
    monkeypatch.setattr(cdc, "_conn", lambda: Conn())
    monkeypatch.setattr(pr, "_order_by_clause", lambda *a, **k: '"id"')
    records = [rec for b in cdc.snapshot() for rec in b.inserts]
    assert _wire_matrix(records) == EXPECTED


# ---------------------------------------------------------------------------
# Live: real MySQL snapshot pages (scan / offset / keyset) and MySQL -> Postgres
# ---------------------------------------------------------------------------

import socket  # noqa: E402
import uuid  # noqa: E402

LIVE_MYSQL = {
    "host": "localhost", "port": 3306, "database": "dataflow", "username": "dataflow",
    "password": "dataflow", "connection_string": "", "ssl": False,
}
LIVE_PG = {**LIVE_MYSQL, "port": 5432, "schema": "public"}
LIVE_COLUMNS = ["id", "i", "d", "b", "dt", "ts", "j", "note"]


def _live_ready() -> bool:
    try:
        import psycopg2
        import pymysqlreplication  # noqa: F401

        for port in (3306, 5432):
            with socket.create_connection(("localhost", port), timeout=1):
                pass
        _mysql_conn().close()
        psycopg2.connect(host="localhost", dbname="dataflow", user="dataflow",
                         password="dataflow", connect_timeout=3).close()
        return True
    except Exception:
        return False


def _mysql_conn():
    from connectors.mysql_conn import get_connection

    return get_connection(**LIVE_MYSQL)


live = pytest.mark.skipif(not _live_ready(), reason="MySQL or PostgreSQL not reachable")


def _make_source() -> str:
    table = "d174_" + uuid.uuid4().hex[:8]
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table} (id INT PRIMARY KEY, i INT, d DECIMAL(10,2), b TINYINT(1), "
                "dt DATE, ts DATETIME, j JSON, note VARCHAR(40))"
            )
            cur.execute(
                f"INSERT INTO {table} VALUES "
                "(1,NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
                "(2,5,1.50,1,'2024-01-02','2024-01-02 03:04:05','{\"a\": 1}',NULL),"
                f"(3,6,2.50,0,'2024-01-03','2024-01-03 03:04:05','[1]','{LITERAL}')"
            )
        conn.commit()
    finally:
        conn.close()
    return table


def _drop_source(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.commit()
    finally:
        conn.close()


def _live_cdc(table: str) -> mcs.MySqlChangeStreamCdc:
    return mcs.MySqlChangeStreamCdc(LIVE_MYSQL, table, "id", columns=LIVE_COLUMNS)


@live
def test_live_every_snapshot_page_mode_yields_the_same_matrix():
    table = _make_source()
    try:
        cdc = _live_cdc(table)
        scan = _records_to_matrix(_pages(cdc, table=table), LIVE_COLUMNS)
        offset = _records_to_matrix(_pages(_live_cdc(table), table=table, offset=1), LIVE_COLUMNS)
        keyset = _records_to_matrix(
            _pages(_live_cdc(table), table=table, offset=1, last_pk="1"), LIVE_COLUMNS
        )
        assert scan[0] == ["1"] + [SQL_NULL_SENTINEL] * 7  # all-NULL row: NULL in every column
        assert scan[2][7] == f"{ESCAPED_WIRE_PREFIX}{LITERAL}"  # a user literal stays text
        assert offset == scan[1:]  # offset 1 skips row 1, so rows 2-3 follow
        assert keyset == scan[1:]  # keyset after PK 1 yields the same rows 2-3
    finally:
        _drop_source(table)


@live
def test_live_cdc_snapshot_to_postgres_writes_null_as_null_and_the_literal_as_text():
    import psycopg2

    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    table = _make_source()
    mappings = [
        {"source": "id", "target": "id", "source_type": "INT", "target_type": "INTEGER"},
        {"source": "i", "target": "i", "source_type": "INT", "target_type": "INTEGER"},
        {"source": "d", "target": "d", "source_type": "DECIMAL", "target_type": "NUMERIC(10,2)"},
        {"source": "b", "target": "b", "source_type": "TINYINT", "target_type": "BOOLEAN"},
        {"source": "dt", "target": "dt", "source_type": "DATE", "target_type": "DATE"},
        {"source": "ts", "target": "ts", "source_type": "DATETIME", "target_type": "TIMESTAMP"},
        {"source": "j", "target": "j", "source_type": "JSON", "target_type": "JSONB"},
        {"source": "note", "target": "note", "source_type": "VARCHAR", "target_type": "TEXT"},
    ]
    schema = {m["source"]: m["target_type"] for m in mappings}
    pg = psycopg2.connect(host="localhost", dbname="dataflow", user="dataflow", password="dataflow")
    pg.autocommit = True
    try:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        src = EndpointConfig(kind="database", format="mysql", table=table, **LIVE_MYSQL)
        dst = EndpointConfig(kind="database", format="postgresql", table=table, **LIVE_PG)
        stream = [{"name": table, "selected": True, "snapshot_mode": "initial",
                   "primary_key": "id", "sync_mode": "cdc"}]
        rows, _ddl, summary, _h = run_cdc_database_transfer(
            src, dst, mappings, schema, sync_mode="cdc", stream_contracts=stream,
            job_id="d174-" + uuid.uuid4().hex[:8], validation_mode="strict",
        )
        assert rows == 3, summary
        with pg.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM {table} WHERE note = '{LITERAL}'")
            assert cur.fetchone()[0] == 1  # only the row whose source text is the literal
            cur.execute(f"SELECT i, d, b, dt, ts, j, note FROM {table} WHERE id = 1")
            assert cur.fetchone() == (None,) * 7
            cur.execute(f"SELECT note FROM {table} WHERE id IN (2, 3) ORDER BY id")
            assert [r[0] for r in cur.fetchall()] == [None, LITERAL]
    finally:
        with pg.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        pg.close()
        _drop_source(table)
