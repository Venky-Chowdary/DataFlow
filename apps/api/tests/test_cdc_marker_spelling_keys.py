"""A real key whose text spells a wire marker is a key, not a NULL (review F1).

The NULL-key fix classified NULL by spelling: ``_pk_value`` and the apply
backstop read any text equal to ``__DF_SQL_NULL__`` / ``__DF_MISSING__`` /
``__df_ddb_null__`` as NULL. A source row whose key really is that text then had
its CDC delete parked and never applied (data loss; it was applied before), and
the DLQ image rewrote the key to ``None``.

NULL is now classified by the reader's own encoding: ``None``, the ``Missing``
singleton, or a marked reader wire cell (``WireCell``) holding the marker. A
plain string is a value whatever it spells. Proven live on PostgreSQL logical
decoding and MySQL binlog: the literal-key delete is applied; a true NULL-key
delete is still parked.
"""

from __future__ import annotations

import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL, Missing, WireCell
from tests.test_cdc_null_key_sentinel import (  # noqa: F401
    _jsonl,
    _mysql_pg_ready,
    _pg_cfg,
    _pg_exec,
    _pg_logical_ready,
    _run_mysql,
    _run_pg,
    stores,
)

SPELLINGS = [SQL_NULL_SENTINEL, "__df_ddb_null__"]


# --------------------------------------------------------------------------
# Unit: classify by encoding, never by spelling
# --------------------------------------------------------------------------


@pytest.mark.parametrize("text", SPELLINGS + ["__DF_MISSING__"])
def test_plain_text_spelling_a_marker_is_a_key(text):
    from services.cdc_snapshot_window import _pk_value
    from services.cdc_unkeyed_delete import null_key_part

    assert _pk_value({"sku": text}, "sku") == text
    assert null_key_part(text) is False
    assert null_key_part(["1", text], 2) is False


@pytest.mark.parametrize("null", [None, Missing, WireCell(SQL_NULL_SENTINEL)])
def test_a_reader_encoded_null_is_no_key(null):
    from services.cdc_snapshot_window import _pk_value
    from services.cdc_unkeyed_delete import null_key_part

    assert _pk_value({"sku": null}, "sku") is None
    assert null_key_part(null) is True
    assert null_key_part(["1", null], 2) is True


def test_the_apply_backstop_leaves_a_literal_key_delete_alone():
    from services.cdc_engine import ChangeBatch
    from services.cdc_unkeyed_delete import park_null_key_deletes

    change = ChangeBatch(deletes=[SQL_NULL_SENTINEL, "a", None])
    assert park_null_key_deletes(change, ["sku"], table="t") == 1
    assert change.deletes == [SQL_NULL_SENTINEL, "a"]
    assert len(change.unkeyed_deletes) == 1


def test_sqlserver_classifier_deletes_a_literal_key_and_parks_a_null_one():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    parked: list[dict] = []
    _i, _u, deletes = classify_mssql_cdc_rows(
        [{"__$operation": 1, "sku": "__df_ddb_null__", "v": "x"},
         {"__$operation": 1, "sku": None, "v": "y"}],
        primary_key="sku", parked=parked, table="dbo.t",
    )
    assert len(deletes) == 1 and len(parked) == 1, (deletes, parked)


# --------------------------------------------------------------------------
# Live: PostgreSQL logical decoding and MySQL binlog into PostgreSQL
# --------------------------------------------------------------------------


def _seed_rows() -> str:
    return "('a', 1), ('__DF_SQL_NULL__', 2), ('__df_ddb_null__', 3), (NULL, 9)"


def _dest(pg_cfg: dict, table: str) -> list[tuple]:
    return _pg_exec(pg_cfg, f'SELECT sku, qty FROM public."{table}" ORDER BY qty', fetch=True)


@pytest.fixture()
def pg_route(stores):
    if not _pg_logical_ready():
        pytest.skip("PostgreSQL with wal_level=logical not reachable")
    pg_cfg = _pg_cfg()
    src, dst = f"mks_{uuid.uuid4().hex[:8]}", f"mkd_{uuid.uuid4().hex[:8]}"
    _pg_exec(pg_cfg, f"CREATE TABLE public.{src} (sku TEXT NULL, qty INT)")
    _pg_exec(pg_cfg, f"ALTER TABLE public.{src} REPLICA IDENTITY FULL")
    _pg_exec(pg_cfg, f"INSERT INTO public.{src} VALUES {_seed_rows()}")
    try:
        yield src, dst, pg_cfg
    finally:
        _pg_exec(pg_cfg, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
                 f"WHERE NOT active AND slot_name LIKE '%{src}%'")
        for (pub,) in _pg_exec(
            pg_cfg, f"SELECT pubname FROM pg_publication WHERE pubname LIKE '%{src}%'", fetch=True
        ) or []:
            _pg_exec(pg_cfg, f'DROP PUBLICATION IF EXISTS "{pub}"')
        for t in (src, dst, f"{dst}_df_quarantine"):
            _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{t}"')


def test_live_pg_literal_key_delete_is_applied_and_a_null_key_delete_parked(pg_route, stores):
    src, dst, pg_cfg = pg_route
    job = "mkp" + uuid.uuid4().hex[:10]
    _run_pg(src, dst, pg_cfg, job)
    assert _dest(pg_cfg, dst) == [("a", 1), (SQL_NULL_SENTINEL, 2), ("__df_ddb_null__", 3)]
    _pg_exec(pg_cfg, f"DELETE FROM public.{src} WHERE sku IN ('__DF_SQL_NULL__', '__df_ddb_null__')")
    _pg_exec(pg_cfg, f"DELETE FROM public.{src} WHERE sku IS NULL")
    _r, _d, summary, _h = _run_pg(src, dst, pg_cfg, job)
    assert _dest(pg_cfg, dst) == [("a", 1)]
    cdc = summary.get("cdc") or {}
    assert cdc.get("delete_events") == 2 and cdc.get("unkeyed_deletes_parked") == 1, cdc


@pytest.fixture()
def mysql_route(stores):
    if not _mysql_pg_ready():
        pytest.skip("live MySQL (ROW binlog) and PostgreSQL not reachable")
    from connectors.mysql_conn import get_connection
    from tests.helpers.live_env import mysql_creds

    m = mysql_creds()
    my_cfg = {"host": m["host"], "port": int(m["port"]), "database": m["database"],
              "username": m["username"], "password": m["password"], "connection_string": "", "ssl": False}
    pg_cfg = _pg_cfg()
    table = f"mkm_{uuid.uuid4().hex[:8]}"

    def my(sql: str) -> None:
        conn = get_connection(**my_cfg)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
        finally:
            conn.close()

    try:
        my(f"CREATE TABLE {table} (sku VARCHAR(20) NULL, qty INT)")
        my(f"INSERT INTO {table} VALUES {_seed_rows()}")
        yield table, my, my_cfg, pg_cfg
    finally:
        my(f"DROP TABLE IF EXISTS {table}")
        _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{table}"')
        _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def test_live_mysql_literal_key_delete_is_applied_and_a_null_key_delete_parked(mysql_route, stores):
    from services.cdc_unkeyed_delete import DLQ_ACTION

    table, my, my_cfg, pg_cfg = mysql_route
    job = "mkm" + uuid.uuid4().hex[:10]
    _run_mysql(table, my_cfg, pg_cfg, job)
    assert _dest(pg_cfg, table) == [("a", 1), (SQL_NULL_SENTINEL, 2), ("__df_ddb_null__", 3)]
    my(f"DELETE FROM {table} WHERE sku IN ('__DF_SQL_NULL__', '__df_ddb_null__')")
    my(f"DELETE FROM {table} WHERE sku IS NULL")
    _r, _d, summary, _h = _run_mysql(table, my_cfg, pg_cfg, job)
    assert _dest(pg_cfg, table) == [("a", 1)]
    cdc = summary.get("cdc") or {}
    assert cdc.get("delete_events") == 2 and cdc.get("unkeyed_deletes_parked") == 1, cdc
    dlq = [e for e in _jsonl(stores / "dlq.jsonl") if e.get("action") == DLQ_ACTION]
    images = [d.get("row") for e in dlq for d in e["details"]["deletes"]]
    # Only the NULL-key row was parked; neither literal key was.
    assert [str(i.get("qty")) for i in images] == ["9"], images


# --------------------------------------------------------------------------
# A literal ``__DF_MISSING__`` key: its delete is applied, not a crash
# --------------------------------------------------------------------------


def test_the_delete_key_census_keeps_a_marker_spelling_key():
    """``coerce_pk_part`` read ``__DF_MISSING__`` as an integer and raised."""
    from services.row_conservation import coerce_pk_part, parse_delete_keys

    assert coerce_pk_part("__DF_MISSING__") == "__DF_MISSING__"
    assert parse_delete_keys(["__DF_MISSING__", "7"], 1) == [("__DF_MISSING__",), (7,)]


def test_live_pg_delete_of_a_missing_marker_key_does_not_stop_the_stream(pg_route, stores):
    src, dst, pg_cfg = pg_route
    _pg_exec(pg_cfg, f"INSERT INTO public.{src} VALUES ('__DF_MISSING__', 4)")
    job = "mkx" + uuid.uuid4().hex[:10]
    _run_pg(src, dst, pg_cfg, job)
    _pg_exec(pg_cfg, f"DELETE FROM public.{src} WHERE sku = '__DF_MISSING__'")
    _r, _d, summary, _h = _run_pg(src, dst, pg_cfg, job)
    cdc = summary.get("cdc") or {}
    # Applied as a keyed delete, never parked as a NULL key. (The row itself
    # landed as '' on the first run: a pre-existing writer issue, filed apart.)
    assert cdc.get("delete_events") == 1 and not cdc.get("unkeyed_deletes_parked"), cdc
