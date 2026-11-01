"""QA r12 D10 - additive schema drift was refused on a MongoDB destination.

QA (production 8f7d246d): add ``loyalty_tier`` to the PostgreSQL source, run
the PG -> MongoDB upsert again with propagate_columns. It failed: "MongoDB
physical DDL missing for mapped column(s) loyalty_tier - refuse silent Map
stamp bind", and no document got the field.

Root cause: the Mongo writer (``connectors/mongodb_writer.write_mapped_rows``)
samples the existing collection's BSON types and sends them through
``writer_common.require_physical_types_for_existing_table``, the SQL rule that
every mapped column must already have a physical type. A field no document
holds yet has no physical type to contradict anything. With the Studio probe of
the collection present, the same gap was also read as a "partial Studio"
(``resolve_studio_or_map_dest_types``) and refused as "live BSON incomplete".
The check is now skipped for schemaless destinations, decided by the round-11
engine-family resolver (``engine_family.is_schemaless_family``), and a field
neither the probe nor the sample holds takes Map's stamp.

Real engines: PostgreSQL :5432 -> MongoDB :27017 through MCP start_transfer
(incremental upsert). Values cover NULL, '', whitespace and unicode; runs 2
and 3 write into the collection run 1 created.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timedelta, timezone

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

_DB = "dataflow_test"
_TIERS = [None, "", "   ", "gold ✓", "Ünïcødé 山田", "silver"]
_T0 = datetime(2026, 10, 1, tzinfo=timezone.utc)


def _mongo():
    try:
        socket.create_connection(("127.0.0.1", 27017), timeout=1).close()
    except OSError:
        pytest.skip("MongoDB not reachable on :27017")
    pymongo = pytest.importorskip("pymongo")
    return pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c


def _connector(client, name: str, kind: str) -> str:
    if kind == "mongodb":
        body = {"name": name, "type": "mongodb", "host": "127.0.0.1", "port": 27017,
                "connection_string": "mongodb://127.0.0.1:27017", "database": _DB}
    else:
        cfg = h.dest_config("postgresql")
        body = {"name": name, "type": "postgresql", "host": cfg["host"], "port": cfg["port"],
                "database": cfg["database"], "username": cfg["username"],
                "password": cfg["password"], "schema": "public"}
    resp = client.post("/api/v1/connectors", json=body)
    assert resp.status_code == 200, resp.text
    return resp.json()["id"]


def _run(client, src: str, dst: str, table: str) -> dict:
    return h.stage_and_run(client, "start_transfer", {
        "source_connector_id": src, "source_table": table,
        "dest_connector_id": dst, "dest_table": table,
        "sync_mode": "incremental_upsert", "primary_key": "id",
        "cursor_column": "updated_at", "cursor_semantics": "modification_timestamp",
        "schema_policy": "propagate_columns",
    })


def _docs(table: str) -> dict[int, dict]:
    return {d["id"]: d for d in _mongo()[_DB][table].find({}, {"_id": 0})}


def _pg(table: str) -> dict[int, tuple]:
    cols = [r[0] for r in h.query("postgresql", (
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"))]
    rows = h.query("postgresql", f'SELECT {", ".join(cols)} FROM "{table}"')  # nosec B608
    return {r[0]: dict(zip(cols, r)) for r in rows}


def _assert_same(table: str, *, with_tier: bool) -> None:
    src, docs = _pg(table), _docs(table)
    assert sorted(docs) == sorted(src)
    for key, row in src.items():
        doc = docs[key]
        assert doc["name"] == row["name"], (key, doc)
        if with_tier:
            assert "loyalty_tier" in doc, (key, doc)
            assert doc["loyalty_tier"] == row["loyalty_tier"], (key, doc, row)


def test_pg_to_mongo_upsert_writes_a_new_source_column(client):
    tag = uuid.uuid4().hex[:6]
    table = f"r12d10_{tag}"
    h.drop_table("postgresql", table)
    h.execute("postgresql", f'CREATE TABLE "{table}" (id int PRIMARY KEY, name text, '
                            "updated_at timestamptz(3))")
    conn = h.pg_conn()
    try:
        conn.cursor().executemany(
            f'INSERT INTO "{table}" VALUES (%s, %s, %s)',  # nosec B608
            [(i, ["", "   ", "Łukasz 🎉", None, f"n{i}"][i % 5], _T0 + timedelta(seconds=i))
             for i in range(1, 31)],
        )
    finally:
        conn.close()
    try:
        src = _connector(client, f"p{tag}", "postgresql")
        dst = _connector(client, f"m{tag}", "mongodb")
        _run(client, src, dst, table)
        _assert_same(table, with_tier=False)

        # Additive drift on the source; every row changes so run 2 carries it.
        h.execute("postgresql", f'ALTER TABLE "{table}" ADD COLUMN loyalty_tier text')
        conn = h.pg_conn()
        try:
            conn.cursor().executemany(
                f'UPDATE "{table}" SET loyalty_tier = %s, updated_at = %s WHERE id = %s',  # nosec B608
                [(_TIERS[i % len(_TIERS)], _T0 + timedelta(days=1, seconds=i), i)
                 for i in range(1, 31)],
            )
        finally:
            conn.close()
        _run(client, src, dst, table)
        _assert_same(table, with_tier=True)

        # Run 3: the field now exists in the collection; new rows and edits.
        h.execute("postgresql", (
            f"INSERT INTO \"{table}\" VALUES (31, 'new', '{(_T0 + timedelta(days=2)).isoformat()}', "
            "'   ')"))
        h.execute("postgresql", (
            f"UPDATE \"{table}\" SET loyalty_tier = NULL, updated_at = "
            f"'{(_T0 + timedelta(days=2, seconds=1)).isoformat()}' WHERE id = 4"))
        _run(client, src, dst, table)
        _assert_same(table, with_tier=True)
    finally:
        h.drop_table("postgresql", table)
        _mongo()[_DB][table].drop()


def test_schemaless_destinations_skip_the_physical_ddl_check_by_engine_family():
    from connectors.writer_common import require_physical_types_for_existing_table
    from src.transfer.engine_family import is_schemaless_family

    for kind in ("mongodb", "mongodb_atlas", "amazon_documentdb", "dynamodb", "redis"):
        assert is_schemaless_family(kind), kind
    for kind in ("postgresql", "alloydb", "mysql", "sqlserver", "snowflake", "milvus", ""):
        assert not is_schemaless_family(kind), kind

    partial = {"id": "INTEGER"}
    cols = ["id", "loyalty_tier"]
    assert require_physical_types_for_existing_table(
        table_existed=True, physical=partial, dialect_label="MongoDB", target_cols=cols,
        destination_kind="mongodb_atlas",
    ) is None
    # An empty sample is refused unless it was read successfully.
    assert require_physical_types_for_existing_table(
        table_existed=True, physical={}, dialect_label="MongoDB", target_cols=cols,
        destination_kind="mongodb",
    )
    assert require_physical_types_for_existing_table(
        table_existed=True, physical={}, dialect_label="MongoDB", target_cols=cols,
        destination_kind="mongodb", physical_sampled=True,
    ) is None
    # A typed destination keeps refusing a missing carrier.
    assert "physical DDL missing" in require_physical_types_for_existing_table(
        table_existed=True, physical=partial, dialect_label="PostgreSQL", target_cols=cols,
        destination_kind="postgresql",
    )
