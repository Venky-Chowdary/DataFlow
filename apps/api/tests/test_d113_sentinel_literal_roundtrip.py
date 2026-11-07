"""D113/D109 sentinel-layer regression: NULL numerics must not be re-escaped
into literal text, and a user literal spelling ``__DF_SQL_NULL__`` must land
as literal text — never NULL — on every engine and mode.

Wire protocol: NULL travels as ``__DF_SQL_NULL__``; a colliding literal travels
``__DF_ESC__``-escaped; decode happens exactly once, at the leaf. Defects:
mid-wire re-serialize re-escaped the encoded NULL into ``__DF_ESC__`` text that
typed casts refused ("Invalid decimal: '__DF_ESC____DF_SQL_NULL__'" — D113),
and leaf decode produced a bare str the next sentinel check re-read as NULL
(D109 literal -> NULL). The fix: ``WireLiteral`` marks decoded sentinel-spelling
literals as real values, and mid-wire serializers use ``wire_cell_text``.
"""
from __future__ import annotations

import csv
import uuid

import psycopg2
import pytest

from connectors.writer_common import (
    bind_sql_mapped_rows_with_quarantine,
    quarantine_cell_wire,
)
from services.transform_engine import apply_transform
from services.value_serializer import (
    SQL_NULL_SENTINEL,
    escape_wire_text,
    is_null_evidence,
    is_reader_null_cell,
    present_cell_text,
    unescape_wire_text,
)

try:
    # Post-fix symbols — absent on the pre-fix tree; lazy imports below turn
    # those tests into failures there instead of a collection error.
    from services.value_serializer import WireLiteral, wire_cell_text
except ImportError:  # pragma: no cover - pre-fix tree
    WireLiteral = None  # type: ignore[assignment]
    wire_cell_text = None  # type: ignore[assignment]
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG_CONN = dict(
    host="localhost", port=5432, database="dataflow",
    user="dataflow", password="dataflow",
)
PG_CFG = dict(
    host="localhost", port=5432, database="dataflow",
    username="dataflow", password="dataflow",
)

ROWS = [
    # literal colliding with the NULL sentinel + real NULLs + a plain 'NULL' text.
    # The wide column mix matters: the pre-fix literal->NULL decode only fired on
    # the richer mapped-bundle path (reproduced with text/numeric/int/bool/jsonb/ts).
    (1, "ok", 10.50, 5, True, '{"a":1}', "2026-01-01 00:00:00+00"),
    (2, "__DF_SQL_NULL__", 20.00, 6, False, '{"b":2}', "2026-01-02 00:00:00+00"),
    (3, None, None, None, None, None, None),
    (4, "NULL", None, 8, None, None, "2026-01-04 00:00:00+00"),
]


def _seed_pg(src_table: str) -> None:
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            f'CREATE TABLE "{src_table}" ('
            "id int PRIMARY KEY, name text, amt numeric(12,2), "
            "score int, flag bool, meta jsonb, updated_at timestamptz)"
        )
        cur.executemany(
            f'INSERT INTO "{src_table}" VALUES (%s,%s,%s,%s,%s,%s,%s)', ROWS
        )
    conn.close()


def _drop(*tables: str) -> None:
    conn = psycopg2.connect(**PG_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f'DROP TABLE IF EXISTS "{t}"')
    conn.close()


def _dst_rows(table: str) -> dict:
    conn = psycopg2.connect(**PG_CONN)
    with conn.cursor() as cur:
        cur.execute(
            f'SELECT id, name, amt, score, flag, meta, updated_at FROM "{table}" ORDER BY id'
        )
        # (name, amt, score, flag, meta, updated_at) per id
        out = {r[0]: tuple(r[1:]) for r in cur.fetchall()}
    conn.close()
    return out


def _pg_to_pg(sync_mode: str, validation_mode: str):
    sfx = uuid.uuid4().hex[:8]
    src, dst = f"d113_t{sync_mode[:2]}_s_{sfx}", f"d113_t{sync_mode[:2]}_d_{sfx}"
    _seed_pg(src)
    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=src, **PG_CFG,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql", schema="public",
            table=dst, **PG_CFG,
        ),
        sync_mode=sync_mode,
        validation_mode=validation_mode,
        stream_contracts=[{
            "name": "t", "sync_mode": "upsert",
            "primary_key": "id", "selected": True,
        }],
        skip_preflight=True,
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    return result, src, dst


def _assert_roundtrip(dst: str) -> None:
    got = _dst_rows(dst)
    assert got[1][0] == "ok"
    assert float(got[1][1]) == 10.50
    # literal must land as literal text — D109 regression was NULL here
    assert got[2][0] == "__DF_SQL_NULL__", f"literal landed as {got[2][0]!r}"
    assert float(got[2][1]) == 20.00
    # real NULLs must land as SQL NULL on every type — D113 failed the job
    assert all(v is None for v in got[3]), f"id3 NULLs: {got[3]!r}"
    assert got[4][0] == "NULL"
    assert got[4][1] is None
    # typed columns: int NULL stays NULL, bool/jsonb/ts round-trip
    assert got[1][3] is True and got[2][3] is False
    assert got[4][2] == 8


@pytest.mark.parametrize("sync_mode", ["upsert", "incremental", "full_refresh_overwrite"])
@pytest.mark.parametrize("validation_mode", ["strict", "balanced"])
def test_d113_pg_null_numeric_and_literal_roundtrip(sync_mode, validation_mode):
    """PG->PG: NULL numeric + NULL text + colliding literal round-trip exactly."""
    result, src, dst = _pg_to_pg(sync_mode, validation_mode)
    try:
        assert getattr(result, "success", False), getattr(result, "error", "")
        _assert_roundtrip(dst)
    finally:
        _drop(src, dst)


def test_d113_s3_csv_encoder_roundtrip():
    """S3/GCS CSV encode path (``ObjectStoreEncoder._append_delimited``): wire
    NULL -> empty unquoted cell; escaped literal -> literal text; never the
    ``__DF_ESC__`` wire noise that D113 wrote and casts refused."""
    from connectors.object_store_materialize import ObjectStoreEncoder

    enc = ObjectStoreEncoder(
        key="exports/t.csv",
        target_cols=["id", "name", "amt"],
        dest_types={"id": "INTEGER", "name": "VARCHAR", "amt": "NUMBER(12,2)"},
    )
    enc._append_delimited([
        ("1", "ok", "10.50"),
        ("2", "__DF_ESC____DF_SQL_NULL__", "20.00"),
        ("3", SQL_NULL_SENTINEL, SQL_NULL_SENTINEL),
        ("4", "NULL", SQL_NULL_SENTINEL),
    ])
    export = enc.finish()
    body = export.read_all().decode("utf-8")
    rows = list(csv.reader(body.splitlines()))
    body_rows = rows[1:]
    by_id = {r[0]: r for r in body_rows}
    assert by_id["2"][1] == "__DF_SQL_NULL__", f"literal: {by_id['2'][1]!r}"
    # NULL numeric + NULL text -> empty cells; never __DF_ESC__ text
    assert by_id["3"] == ["3", "", ""]
    assert by_id["4"] == ["4", "NULL", ""]
    assert "__DF_ESC__" not in body


def test_d113_snowflake_bind_literal_and_null():
    """Snowflake-shaped bind: wire NULL binds NULL; escaped literal binds text."""
    # numeric carrier: bare sentinel -> NULL
    rejected: list = []
    bound = bind_sql_mapped_rows_with_quarantine(
        [("1", SQL_NULL_SENTINEL, "10.50"),
         ("2", "__DF_ESC____DF_SQL_NULL__", "20.00")],
        ["id", "name", "amt"],
        ["INTEGER", "VARCHAR", "NUMBER(12,2)"],
        rejected, "fail",
        engine="snowflake", dialect_label="Snowflake",
        mappings=[{"source": "id", "target": "id"},
                  {"source": "name", "target": "name"},
                  {"source": "amt", "target": "amt"}],
    )
    assert len(bound) == 2 and not rejected
    # wire NULL stays the sentinel for the writer leaf to emit SQL NULL
    assert bound[0][1] == SQL_NULL_SENTINEL
    assert is_reader_null_cell(bound[0][1])
    # the escaped literal binds as WireLiteral: text that is immune to
    # re-classification as NULL by every downstream sentinel check
    assert bound[1][1] == "__DF_SQL_NULL__"
    assert WireLiteral is not None and isinstance(bound[1][1], WireLiteral)
    assert not is_reader_null_cell(bound[1][1])
    # D120b: the real connector picks its bind method by exact class name and
    # refused the subclass ("Binding data in type (wireliteral)"); the product
    # converter hands every bind leaf an exact str, client- and server-side.
    from snowflake.connector.converter import SnowflakeConverter

    from connectors.snowflake_conn import snowflake_converter_class

    with pytest.raises(Exception, match="wireliteral"):
        SnowflakeConverter().to_snowflake(bound[1][1])
    converter = snowflake_converter_class()()
    assert issubclass(snowflake_converter_class(), SnowflakeConverter)
    client_side = converter.to_snowflake(bound[1][1])
    assert type(client_side) is str and client_side == "__DF_SQL_NULL__"
    sf_type = converter.snowflake_type(bound[1][1])
    assert sf_type == "TEXT"
    server_side = converter.to_snowflake_bindings(sf_type, bound[1][1])
    assert type(server_side) is str and server_side == "__DF_SQL_NULL__"
    assert converter.to_csv_bindings(bound[1][1]) == "__DF_SQL_NULL__"
    # numeric sentinel decodes to NULL — not the escaped-text error of D113
    converted, err = apply_transform(SQL_NULL_SENTINEL, "decimal")
    assert converted is None and err is None
    converted, err = apply_transform("__DF_ESC____DF_SQL_NULL__", "text")
    assert converted == "__DF_ESC____DF_SQL_NULL__"  # identity keeps the wire


def test_d120b_every_snowflake_connection_uses_the_wire_converter(monkeypatch):
    """The writer, SQLAlchemy (SCD2/mirror) and verification reads all connect
    through ``get_connection``, so it must install the converter (D120b)."""
    from connectors import snowflake_conn

    seen: dict = {}

    class _Connector:
        @staticmethod
        def connect(**kwargs):
            seen.update(kwargs)
            return object()

    monkeypatch.setattr(snowflake_conn, "_snowflake_connector_module", lambda: _Connector)
    snowflake_conn.get_connection(
        account="acme-prod", username="u", password="p",
        database="DB", schema="PUBLIC", warehouse="WH", connection_string="",
    )
    assert seen["converter_class"] is snowflake_conn.snowflake_converter_class()


def test_d120b_local_snowflake_binds_wire_literal(tmp_path, monkeypatch):
    """fakesnow (the local Snowflake target) binds client-side with its own
    stock converter, so it refused the literal exactly like the real one."""
    pytest.importorskip("fakesnow")
    from connectors.snowflake_conn import get_connection

    monkeypatch.setenv("FAKESNOW_DB_PATH", str(tmp_path / "fakesnow"))
    literal = unescape_wire_text("__DF_ESC____DF_SQL_NULL__")
    assert WireLiteral is not None and isinstance(literal, WireLiteral)
    conn = get_connection(
        account="localhost", username="test", password="test",
        database="dataflow", schema="PUBLIC", warehouse="", connection_string="",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT %s", (literal,))
            assert cur.fetchone()[0] == "__DF_SQL_NULL__"
    finally:
        conn.close()


def test_d113_sentinel_layer_unit():
    """Leaf protocol: escape once, decode once, WireLiteral marks literals."""
    assert WireLiteral is not None and wire_cell_text is not None, \
        "sentinel-layer fix symbols missing"
    # escape: colliding literal escapes once; plain text untouched
    assert escape_wire_text("__DF_SQL_NULL__") == "__DF_ESC____DF_SQL_NULL__"
    assert escape_wire_text("ok") == "ok"
    # decode: one layer; sentinel-spelling decode is marked literal
    lit = unescape_wire_text("__DF_ESC____DF_SQL_NULL__")
    assert isinstance(lit, WireLiteral)
    assert lit == "__DF_SQL_NULL__"
    # classifiers must not re-read a decoded literal as absence
    assert not is_reader_null_cell(lit)
    assert not is_null_evidence(lit)
    # real absence still classifies
    assert is_reader_null_cell(SQL_NULL_SENTINEL)
    # mid-wire re-serialize keeps wire strings verbatim — no double-escape
    assert wire_cell_text(SQL_NULL_SENTINEL) == SQL_NULL_SENTINEL
    assert wire_cell_text("__DF_ESC____DF_SQL_NULL__") == "__DF_ESC____DF_SQL_NULL__"
    # quarantine_cell_wire escapes once unconditionally; _unwire strips one —
    # the pair is a lossless round-trip for wire NULLs AND wire literals.
    from services.quarantine_dlq import _unwire_quarantine_detail

    for wire in (SQL_NULL_SENTINEL, "__DF_ESC__abc", "__DF_ESC____DF_SQL_NULL__"):
        stored = quarantine_cell_wire(wire)
        assert stored != SQL_NULL_SENTINEL or wire == SQL_NULL_SENTINEL
    detail = _unwire_quarantine_detail({"row": {"c": quarantine_cell_wire("__DF_ESC__abc")}})
    assert detail["row"]["c"] == "abc"
    # a real NULL unwires to None
    assert _unwire_quarantine_detail({"row": {"c": quarantine_cell_wire(SQL_NULL_SENTINEL)}})["row"]["c"] is None
    # present_cell_text returns the decoded literal, not None (D109 root)
    assert present_cell_text("__DF_ESC____DF_SQL_NULL__") == "__DF_SQL_NULL__"
    # transform decode: bare sentinel -> NULL; WireLiteral -> literal text
    conv, err = apply_transform(SQL_NULL_SENTINEL, "decimal")
    assert conv is None and err is None
    conv, err = apply_transform(WireLiteral("__DF_SQL_NULL__"), "none")
    assert conv == "__DF_SQL_NULL__" and err is None


def test_d113_apply_transform_escaped_literal_is_text_not_crash():
    """A malformed double-encoded value is literal text, not a crash: the cast
    names the decoded literal, never '__DF_ESC__...' wire noise."""
    conv, err = apply_transform("__DF_ESC__abc", "decimal")
    assert conv is None
    assert err and "__DF_ESC__" not in err
    # identity transform keeps the escaped wire for the leaf to decode
    conv, err = apply_transform("__DF_ESC__abc", "none")
    assert conv == "__DF_ESC__abc" and err is None
