"""D244: one plan reads an existing SQL Server destination's catalog once.

Three plan steps (destination read, endpoint sample, FK/CHECK probe) each
spelled the same destination request differently, so the plan memo missed
and the catalog walk (~7 queries) ran three times: 52 queries and 28 s
behind a 300 ms round trip.
"""
from __future__ import annotations

import os
import uuid

import pytest

from services.introspect_memo import introspect_memo_scope


def test_empty_arguments_do_not_split_one_catalog_request(monkeypatch):
    import services.schema_introspect as si

    calls = []
    monkeypatch.setattr(
        si, "_introspect_schema", lambda db, **kw: calls.append(kw) or {"ok": True}
    )
    base = {"host": "h", "port": 1433, "database": "d", "table": "t"}
    with introspect_memo_scope():
        si.introspect_schema("sqlserver", **base)
        si.introspect_schema("sqlserver", **base, api_key="", role="", reader_cfg=None)
        si.introspect_schema("sqlserver", **base, warehouse="", auth_source="")
        si.introspect_schema("sqlserver", **dict(base, table="other"))
        si.introspect_schema("sqlserver", **base, strict_namespace=True)
    assert len(calls) == 3


def _mssql_ready() -> bool:
    if not os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD"):
        return False
    try:
        import pymssql  # noqa: F401
    except ImportError:
        return False
    return True


@pytest.mark.skipif(not _mssql_ready(), reason="needs local SQL Server")
def test_plan_reads_an_existing_sqlserver_destination_once(monkeypatch):
    import psycopg2
    import pymssql

    import services.schema_introspect as si
    from services.connector_store import create_connector, delete_connector
    from src.ai.copilot.transfer_tools import plan_transfer

    pg = {
        "host": os.environ.get("PGHOST", "127.0.0.1"),
        "port": int(os.environ.get("PGPORT", "5432")),
        "database": os.environ.get("PGDATABASE", "dataflow"),
        "username": os.environ.get("PGUSER", "dataflow"),
        "password": os.environ.get("PGPASSWORD", "dataflow"),
    }
    ms = {
        "host": "127.0.0.1",
        "port": int(os.environ.get("DATAFLOW_TEST_MSSQL_PORT", "1433")),
        "database": "master",
        "trust_server_certificate": True,
        "username": os.environ.get("DATAFLOW_TEST_MSSQL_USER", "sa"),
        "password": os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"],
    }
    try:
        pc = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
    except Exception as exc:  # noqa: BLE001 - environment, not product
        pytest.skip(f"PostgreSQL unavailable: {type(exc).__name__}")
    pc.autocommit = True
    src_t = "d244_src_" + uuid.uuid4().hex[:6]
    dst_t = "d244_dst_" + uuid.uuid4().hex[:6]
    cur = pc.cursor()
    cur.execute(
        f"CREATE TABLE {src_t} (id int primary key, name text, "
        "updated_at timestamp(6))"
    )
    cur.execute(
        f"INSERT INTO {src_t} SELECT g, 'n'||g, timestamp '2026-11-02 10:00' "
        "+ g * interval '1 minute' FROM generate_series(1, 5) g"
    )
    mc = pymssql.connect(
        server=ms["host"], port=ms["port"], user=ms["username"],
        password=ms["password"], database="master", autocommit=True,
    ).cursor()
    mc.execute(
        f"CREATE TABLE {dst_t} (id INT NOT NULL, name NVARCHAR(MAX), "
        "updated_at DATETIME2(6))"
    )
    reads = []
    real = si._introspect_schema

    def counting(db_type, **kw):
        if db_type == "sqlserver":
            reads.append(kw.get("table"))
        return real(db_type, **kw)

    monkeypatch.setattr(si, "_introspect_schema", counting)
    made = []
    try:
        s = create_connector({"name": "d244-pg-" + uuid.uuid4().hex[:4],
                              "type": "postgresql", "role": "source", **pg})
        made.append(s.id)
        d = create_connector({"name": "d244-ms-" + uuid.uuid4().hex[:4],
                              "type": "sqlserver", "role": "destination", **ms})
        made.append(d.id)
        result = plan_transfer(
            source_connector_id=s.id, source_table=src_t,
            dest_connector_id=d.id, dest_table=dst_t,
            sync_mode="incremental_append", cursor_column="updated_at",
        )
        assert result.success, result.error
    finally:
        for cid in made:
            delete_connector(cid)
        cur.execute(f"DROP TABLE IF EXISTS {src_t}")
        mc.execute(f"IF OBJECT_ID('{dst_t}') IS NOT NULL DROP TABLE {dst_t}")
    assert reads.count(dst_t) == 1, reads
