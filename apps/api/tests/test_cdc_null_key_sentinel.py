"""A NULL in a declared CDC key is never a key value (follow-up to C2).

The read wire carries SQL NULL as the ``__DF_SQL_NULL__`` sentinel
(``services.value_serializer.SQL_NULL_SENTINEL``). Three places took that
sentinel for a real key:

* the post-run image repair re-read the source, found the NULL-key row the
  writer had held out "missing" at the destination, and upserted it again as
  plain text cells. Encoded a second time, the sentinel landed as the literal
  key ``'__DF_SQL_NULL__'``, while the run still reported the row as rejected;
* the composite key builder (``cdc_snapshot_window._pk_value``) treated only a
  Python ``None`` as "no key", so a wire NULL became the key ``__DF_SQL_NULL__``.
  The SQL Server native classifier and the PostgreSQL REPLICA IDENTITY FULL
  reader then sent ``DELETE ... WHERE sku = '__DF_SQL_NULL__'`` and counted it
  as an applied delete;
* nothing at the apply step stopped a delete key holding the sentinel.

A row with a NULL key is held out (quarantined) and a delete with a NULL key is
parked as unkeyed (``services.cdc_unkeyed_delete``). The destination never holds
the sentinel text, and the rejected count matches what did not land.
"""

from __future__ import annotations

import json
import uuid

import pytest

from services.value_serializer import SQL_NULL_SENTINEL, WireCell, WireLiteral


@pytest.fixture()
def stores(tmp_path, monkeypatch):
    from services import audit_log, quarantine_dlq

    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: None)
    monkeypatch.setattr(quarantine_dlq, "DLQ_PATH", tmp_path / "dlq.jsonl")
    monkeypatch.setattr(quarantine_dlq, "_dlq_coll", lambda: None)
    return tmp_path


def _jsonl(path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


# --------------------------------------------------------------------------
# Unit: the key builder, the SQL Server classifier, the apply backstop
# --------------------------------------------------------------------------


def test_a_wire_null_is_no_key():
    from services.cdc_snapshot_window import _pk_value

    assert _pk_value({"sku": WireCell(SQL_NULL_SENTINEL)}, "sku") is None
    assert _pk_value({"sku": SQL_NULL_SENTINEL}, "sku") is None
    assert _pk_value({"a": "1", "b": WireCell(SQL_NULL_SENTINEL)}, ["a", "b"]) is None
    # The user's own text that spells the sentinel is a value, not NULL.
    assert _pk_value({"sku": WireLiteral(SQL_NULL_SENTINEL)}, "sku") == SQL_NULL_SENTINEL
    # '' is a value (D53), and an ordinary key is unchanged.
    assert _pk_value({"sku": ""}, "sku") == ""
    assert _pk_value({"sku": "a"}, "sku") == "a"


def test_sqlserver_delete_of_a_null_key_row_is_parked_not_deleted():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows

    parked: list[dict] = []
    inserts, updates, deletes = classify_mssql_cdc_rows(
        [
            {"__$operation": 1, "sku": None, "qty": 9},
            {"__$operation": 1, "sku": "b", "qty": 2},
        ],
        primary_key="sku",
        parked=parked,
        table="dbo.t",
    )
    assert deletes == ["b"], deletes
    assert SQL_NULL_SENTINEL not in deletes
    assert len(parked) == 1 and parked[0]["row"]["qty"] == "9"
    assert parked[0]["row"]["sku"] == SQL_NULL_SENTINEL  # the image says NULL


def test_sqlserver_null_key_delete_without_a_park_sink_still_refuses():
    from connectors.sqlserver_cdc_native import classify_mssql_cdc_rows
    from services.cdc_transaction_buffer import CdcUnkeyedDelete

    with pytest.raises(CdcUnkeyedDelete):
        classify_mssql_cdc_rows([{"__$operation": 1, "sku": None}], primary_key="sku")


def test_apply_backstop_parks_a_delete_whose_key_is_the_sentinel():
    from services.cdc_engine import ChangeBatch
    from services.cdc_unkeyed_delete import park_null_key_deletes
    from services.keyset_pagination import KEYSET_SEP

    change = ChangeBatch(
        deletes=["b", SQL_NULL_SENTINEL, f"x{KEYSET_SEP}{SQL_NULL_SENTINEL}", ""],
        resume_token="0/10",
    )
    moved = park_null_key_deletes(change, ["sku"], table="orders")
    assert moved == 1 and change.deletes == ["b", f"x{KEYSET_SEP}{SQL_NULL_SENTINEL}", ""]
    assert change.unkeyed_deletes[0]["table"] == "orders"
    assert change.unkeyed_deletes[0]["row"] == {"sku": SQL_NULL_SENTINEL}

    composite = ChangeBatch(deletes=[f"x{KEYSET_SEP}{SQL_NULL_SENTINEL}", f"x{KEYSET_SEP}y"])
    assert park_null_key_deletes(composite, ["a", "b"], table="t") == 1
    assert composite.deletes == [f"x{KEYSET_SEP}y"]
    assert composite.unkeyed_deletes[0]["row"] == {"a": "x", "b": SQL_NULL_SENTINEL}
    # Idempotent: a second pass finds nothing more.
    assert park_null_key_deletes(composite, ["a", "b"], table="t") == 0


def test_image_repair_never_rewrites_a_null_key_row_and_keeps_null_as_null():
    from src.transfer.cdc_transfer import _repairable_image

    rows = [
        {"sku": SQL_NULL_SENTINEL, "qty": "9"},
        {"sku": "a", "qty": SQL_NULL_SENTINEL},
    ]
    kept, held = _repairable_image(rows, ["sku"])
    assert held == 1 and len(kept) == 1
    assert kept[0]["sku"] == "a"
    # The reader's NULL stays the wire NULL: it is not text to encode again.
    assert isinstance(kept[0]["qty"], WireCell) and kept[0]["qty"] == SQL_NULL_SENTINEL


# --------------------------------------------------------------------------
# Live legs
# --------------------------------------------------------------------------


def _mysql_pg_ready() -> bool:
    try:
        import pymysqlreplication  # noqa: F401

        from tests.helpers.live_env import mysql_up, pg_up

        return mysql_up() and pg_up()
    except Exception:  # noqa: BLE001 — no live engines: the live leg skips
        return False


def _pg_cfg() -> dict:
    from tests.helpers.live_env import pg_creds

    p = pg_creds()
    return {
        "host": p["host"], "port": int(p["port"]), "database": p["database"],
        "username": p["username"], "password": p["password"],
        "connection_string": "", "ssl": False,
    }


def _pg_exec(pg_cfg: dict, sql: str, fetch: bool = False):
    import psycopg2

    conn = psycopg2.connect(
        host=pg_cfg["host"], port=pg_cfg["port"], dbname=pg_cfg["database"],
        user=pg_cfg["username"], password=pg_cfg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall() if fetch else None
    finally:
        conn.close()


def _no_sentinel_anywhere(rows: list[tuple]) -> None:
    flat = [str(c) for r in rows for c in r if c is not None]
    assert not any("__DF_SQL_NULL__" in c or "__DF_ESC__" in c for c in flat), rows


@pytest.fixture()
def mysql_route(stores):
    if not _mysql_pg_ready():
        pytest.skip("live MySQL (ROW binlog) and PostgreSQL not reachable")
    from connectors.mysql_conn import get_connection
    from tests.helpers.live_env import mysql_creds

    m = mysql_creds()
    my_cfg = {
        "host": m["host"], "port": int(m["port"]), "database": m["database"],
        "username": m["username"], "password": m["password"],
        "connection_string": "", "ssl": False,
    }
    pg_cfg = _pg_cfg()
    table = f"nk_{uuid.uuid4().hex[:8]}"

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
        my(f"INSERT INTO {table} VALUES ('a', 1), ('b', 2), (NULL, 9)")
        yield table, my, my_cfg, pg_cfg
    finally:
        my(f"DROP TABLE IF EXISTS {table}")
        _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{table}"')
        _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{table}_df_quarantine"')


def _run_mysql(table: str, my_cfg: dict, pg_cfg: dict, job_id: str):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    return run_cdc_database_transfer(
        EndpointConfig(kind="database", format="mysql", **my_cfg, table=table),
        EndpointConfig(kind="database", format="postgresql", **pg_cfg, schema="public", table=table),
        [
            {"source": "sku", "target": "sku", "source_type": "VARCHAR(20)", "target_type": "VARCHAR(20)"},
            {"source": "qty", "target": "qty", "source_type": "INT", "target_type": "INTEGER"},
        ],
        {"sku": "VARCHAR(20)", "qty": "INT"},
        sync_mode="cdc",
        stream_contracts=[{
            "name": table, "selected": True, "sync_mode": "cdc",
            "snapshot_mode": "initial", "primary_key": "sku",
        }],
        job_id=job_id,
        limit=0,
        validation_mode="balanced",
    )


def test_live_mysql_null_key_row_is_held_out_and_never_lands_as_the_sentinel(mysql_route, stores):
    from services.cdc_unkeyed_delete import DLQ_ACTION

    table, my, my_cfg, pg_cfg = mysql_route
    job_id = "nk" + uuid.uuid4().hex[:10]

    def dest() -> list[tuple]:
        return _pg_exec(pg_cfg, f'SELECT sku, qty FROM public."{table}" ORDER BY sku NULLS FIRST', fetch=True)

    # Run 1 — snapshot, then the post-run image repair.
    _rows, _ddl, s1, _h = _run_mysql(table, my_cfg, pg_cfg, job_id)
    landed = dest()
    _no_sentinel_anywhere(landed)
    assert landed == [("a", 1), ("b", 2)], landed
    assert int(s1.get("rejected_rows") or 0) == 1, s1.get("rejected_rows")
    assert len(landed) == 3 - int(s1.get("rejected_rows") or 0)

    # Run 2 — a streamed NULL-key insert beside a keyed one. The NULL-key rows
    # still sit at the source, so the image repair runs over them again.
    my(f"INSERT INTO {table} VALUES (NULL, 7)")
    my(f"INSERT INTO {table} VALUES ('c', 3)")
    _rows, _ddl, s2, _h = _run_mysql(table, my_cfg, pg_cfg, job_id)
    landed = dest()
    _no_sentinel_anywhere(landed)
    assert landed == [("a", 1), ("b", 2), ("c", 3)], landed
    assert int(s2.get("rejected_rows") or 0) == 1, s2.get("rejected_rows")

    # Run 3 — the NULL-key rows are deleted at the source: parked, never a
    # ``DELETE ... WHERE sku = '__DF_SQL_NULL__'``.
    my(f"DELETE FROM {table} WHERE sku IS NULL")
    my(f"DELETE FROM {table} WHERE sku = 'b'")
    _rows, _ddl, s3, _h = _run_mysql(table, my_cfg, pg_cfg, job_id)
    landed = dest()
    _no_sentinel_anywhere(landed)
    assert landed == [("a", 1), ("c", 3)], landed
    cdc = s3.get("cdc") or {}
    assert cdc.get("delete_events") == 1 and cdc.get("unkeyed_deletes_parked") == 2, cdc
    dlq = [e for e in _jsonl(stores / "dlq.jsonl") if e.get("action") == DLQ_ACTION]
    assert sum(int(e.get("rows") or 0) for e in dlq) == 2, dlq


def _pg_logical_ready() -> bool:
    try:
        from tests.helpers.live_env import pg_up

        if not pg_up():
            return False
        return _pg_exec(_pg_cfg(), "SHOW wal_level", fetch=True)[0][0] == "logical"
    except Exception:  # noqa: BLE001
        return False


@pytest.fixture()
def pg_logical_route(stores):
    if not _pg_logical_ready():
        pytest.skip("PostgreSQL with wal_level=logical not reachable")
    pg_cfg = _pg_cfg()
    src = f"nks_{uuid.uuid4().hex[:8]}"
    dst = f"nkd_{uuid.uuid4().hex[:8]}"
    _pg_exec(pg_cfg, f"CREATE TABLE public.{src} (sku TEXT NULL, qty INT)")
    _pg_exec(pg_cfg, f"ALTER TABLE public.{src} REPLICA IDENTITY FULL")
    _pg_exec(pg_cfg, f"INSERT INTO public.{src} VALUES ('a', 1), ('b', 2), (NULL, 9)")
    try:
        yield src, dst, pg_cfg
    finally:
        _pg_exec(
            pg_cfg,
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots "
            f"WHERE NOT active AND slot_name LIKE '%{src}%'",
        )
        # The route's publication too: it outlives the slot otherwise.
        for (pub,) in _pg_exec(
            pg_cfg, f"SELECT pubname FROM pg_publication WHERE pubname LIKE '%{src}%'", fetch=True
        ) or []:
            _pg_exec(pg_cfg, f'DROP PUBLICATION IF EXISTS "{pub}"')
        for t in (src, dst, f"{dst}_df_quarantine"):
            _pg_exec(pg_cfg, f'DROP TABLE IF EXISTS public."{t}"')


def _run_pg(src: str, dst: str, pg_cfg: dict, job_id: str):
    from src.transfer.cdc_transfer import run_cdc_database_transfer
    from src.transfer.models import EndpointConfig

    return run_cdc_database_transfer(
        EndpointConfig(kind="database", format="postgresql", **pg_cfg, schema="public", table=src),
        EndpointConfig(kind="database", format="postgresql", **pg_cfg, schema="public", table=dst),
        [
            {"source": "sku", "target": "sku", "source_type": "TEXT", "target_type": "TEXT"},
            {"source": "qty", "target": "qty", "source_type": "INTEGER", "target_type": "INTEGER"},
        ],
        {"sku": "TEXT", "qty": "INTEGER"},
        sync_mode="cdc",
        stream_contracts=[{
            "name": src, "selected": True, "sync_mode": "cdc",
            "snapshot_mode": "initial", "primary_key": "sku",
        }],
        job_id=job_id,
        limit=0,
        validation_mode="balanced",
    )


def test_live_pg_replica_identity_full_null_key_delete_is_parked(pg_logical_route, stores):
    src, dst, pg_cfg = pg_logical_route
    job_id = "nkp" + uuid.uuid4().hex[:10]

    def dest() -> list[tuple]:
        return _pg_exec(pg_cfg, f'SELECT sku, qty FROM public."{dst}" ORDER BY sku NULLS FIRST', fetch=True)

    _rows, _ddl, s1, _h = _run_pg(src, dst, pg_cfg, job_id)
    landed = dest()
    _no_sentinel_anywhere(landed)
    assert landed == [("a", 1), ("b", 2)], landed
    assert int(s1.get("rejected_rows") or 0) == 1, s1.get("rejected_rows")

    _pg_exec(pg_cfg, f"DELETE FROM public.{src} WHERE sku IS NULL")
    _pg_exec(pg_cfg, f"DELETE FROM public.{src} WHERE sku = 'b'")
    _rows, _ddl, s2, _h = _run_pg(src, dst, pg_cfg, job_id)
    landed = dest()
    _no_sentinel_anywhere(landed)
    assert landed == [("a", 1)], landed
    cdc = s2.get("cdc") or {}
    # The NULL-key delete is not an applied delete: it is parked.
    assert cdc.get("delete_events") == 1 and cdc.get("unkeyed_deletes_parked") == 1, cdc
