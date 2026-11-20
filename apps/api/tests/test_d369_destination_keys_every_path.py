"""D369 on every destination key and every mapping path (rule 4).

The first D369 fix protected MongoDB ``_id`` by name. A SQL primary key, an
identity column or a DynamoDB key attribute was protected only when the target
row carried a per-column flag, and no caller built such a row: introspection
reports keys at table level, and the MCP plan, Execute auto-map, the Pilot map
tool and Studio's /transfer/map rebuilt their target rows as
``{name, inferred_type, samples}``. So ``customer_id`` was still proposed onto a
destination ``id`` primary key, which turns an upsert into a merge of
unrelated rows.
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.key_column_guard import destination_key_names, with_key_flags  # noqa: E402
from services.semantic_mapper import map_columns  # noqa: E402


def _src(*names: str) -> list[dict]:
    return [
        {
            "name": n,
            "inferred_type": "VARCHAR" if n == "name" else "INTEGER",
            "samples": ["a", "b"] if n == "name" else ["1", "2"],
        }
        for n in names
    ]


def _tgt(*names: str) -> list[dict]:
    return [
        {"name": n, "inferred_type": "VARCHAR" if n == "name" else "INTEGER"} for n in names
    ]


def _pairs(mappings: list[dict]) -> set[tuple[str, str]]:
    return {(m["source"], m["target"]) for m in mappings}


@pytest.mark.parametrize(
    "dest_db, source, key, meta",
    [
        ("postgresql", "customer_id", "id", {"primary_key_columns": ["id"]}),
        ("mysql", "order_no", "order_id", {"primary_key_columns": ["order_id"]}),
        ("sqlserver", "uid", "user_id", {"identity_columns": ["user_id"]}),
        ("dynamodb", "account", "pk", {"dynamo_key_schema": [{"name": "pk", "key_type": "HASH"}]}),
        # The Pilot's introspection keeps the catalog result under ``raw``.
        ("oracle", "customer_id", "id", {"raw": {"primary_key_columns": ["ID"]}}),
    ],
)
def test_an_unrelated_source_is_never_proposed_onto_a_destination_key(
    dest_db, source, key, meta
) -> None:
    unguarded = map_columns(
        [source, "name"],
        [key, "name"],
        source_schemas=_src(source, "name"),
        target_schemas=_tgt(key, "name"),
        destination_db_type=dest_db,
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    # Without the key flag the scorer does pair them: this is the defect.
    assert (source, key) in _pairs(unguarded)

    guarded = map_columns(
        [source, "name"],
        [key, "name"],
        source_schemas=_src(source, "name"),
        target_schemas=with_key_flags(_tgt(key, "name"), meta),
        destination_db_type=dest_db,
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    assert all(m["target"] != key for m in guarded), guarded
    assert ("name", "name") in _pairs(guarded)


@pytest.mark.parametrize("dest_db", ["mongodb", "elasticsearch"])
def test_document_id_takes_only_a_source_id(dest_db) -> None:
    mappings = map_columns(
        ["sku", "d", "name"],
        ["_id", "name"],
        source_schemas=_src("sku", "d", "name"),
        target_schemas=_tgt("_id", "name"),
        destination_db_type=dest_db,
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    assert all(m["target"] != "_id" for m in mappings)


def test_the_key_keeps_its_own_name_in_any_case_or_separator() -> None:
    mappings = map_columns(
        ["ID", "Order_ID", "name"],
        ["id", "orderid", "name"],
        source_schemas=_src("ID", "Order_ID", "name"),
        target_schemas=with_key_flags(_tgt("id", "orderid", "name"), {"primary_key_columns": ["id", "orderid"]}),
        destination_db_type="postgresql",
        destination_table_exists=True,
        source_db_type="mysql",
    )
    assert {("ID", "id"), ("Order_ID", "orderid")} <= _pairs(mappings)


def test_operator_locked_mapping_onto_a_key_survives_remap() -> None:
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        ["customer_id", "name"],
        ["id", "name"],
        source_schemas=_src("customer_id", "name"),
        target_schemas=with_key_flags(_tgt("id", "name"), {"primary_key_columns": ["id"]}),
        destination_db_type="postgresql",
        source_db_type="postgresql",
        destination_table_exists=True,
        use_llm=False,
        prior_mappings=[
            {
                "source": "customer_id",
                "target": "id",
                "confidence": 0.99,
                "reasoning": "operator mapping",
                "user_override": True,
            }
        ],
    )
    assert ("customer_id", "id") in _pairs(result["mappings"])


def test_an_unlocked_prior_onto_a_key_is_dropped_on_remap() -> None:
    from services.mapping_pipeline import run_mapping_pipeline

    result = run_mapping_pipeline(
        ["customer_id", "name"],
        ["id", "name"],
        source_schemas=_src("customer_id", "name"),
        target_schemas=with_key_flags(_tgt("id", "name"), {"primary_key_columns": ["id"]}),
        destination_db_type="postgresql",
        source_db_type="postgresql",
        destination_table_exists=True,
        use_llm=False,
        prior_mappings=[
            {"source": "customer_id", "target": "id", "confidence": 0.9, "reasoning": "earlier map"}
        ],
    )
    assert ("customer_id", "id") not in _pairs(result["mappings"])


def test_key_names_are_read_from_every_introspection_shape() -> None:
    assert destination_key_names(
        {
            "primary_key_columns": ["Id"],
            "identity_columns": ["seq"],
            "dynamo_key_schema": [{"name": "pk", "key_type": "HASH"}, {"name": "sk"}],
            "columns": [{"name": "Hash", "is_primary_key": True}, {"name": "x"}],
            "raw": {"primary_key_columns": ["tenant"]},
        }
    ) == {"id", "seq", "pk", "sk", "hash", "tenant"}
    assert with_key_flags(None, {"primary_key_columns": ["id"]}) is None
    rows = [{"name": "id"}, {"name": "x"}]
    assert with_key_flags(rows, {}) is rows


def test_studio_map_route_protects_the_keys_it_is_sent(tmp_path, monkeypatch) -> None:
    """Studio's /transfer/map (map and remap) sends the probe's key columns."""
    from fastapi.testclient import TestClient

    import services.config  # noqa: F401 - settings parse before the harness blanks env

    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    from src.main import app

    body = {
        "source_columns": ["customer_id", "name"],
        "source_schema": {"customer_id": "INTEGER", "name": "VARCHAR"},
        "source_samples": {"customer_id": ["7", "8"], "name": ["ada", "beau"]},
        "target_columns": ["id", "name"],
        "target_schema": {"id": "INTEGER", "name": "VARCHAR"},
        "destination_db_type": "postgresql",
        "source_db_type": "postgresql",
        "destination_table_exists": True,
        "use_llm": False,
    }
    with TestClient(app) as client:
        before = client.post("/api/v1/transfer/map", json=body)
        after = client.post("/api/v1/transfer/map", json={**body, "target_key_columns": ["id"]})
    assert before.status_code == 200, before.text
    assert after.status_code == 200, after.text
    # Without the key list the scorer pairs them (the defect the field closes).
    assert ("customer_id", "id") in _pairs(before.json()["mappings"])
    assert ("customer_id", "id") not in _pairs(after.json()["mappings"])
    assert ("name", "name") in _pairs(after.json()["mappings"])


def test_execute_auto_map_reads_the_keys_the_probe_stamped(monkeypatch) -> None:
    from src.transfer import engine as engine_mod
    from src.transfer import engine_automap

    def _probe(destination, sync_mode=""):
        destination.extra = {**(destination.extra or {}), "primary_key_columns": ["id"]}
        return {"id": "INTEGER", "name": "VARCHAR"}, True

    monkeypatch.setattr(engine_mod, "_destination_schema_probe", _probe)
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="database", format="postgresql", table="s"),
        destination=EndpointConfig(kind="database", format="postgresql", table="t"),
        sync_mode="incremental_append",
        validation_mode="balanced",
    )
    mappings = engine_automap._auto_map(
        request,
        ["customer_id", "name"],
        {"customer_id": "INTEGER", "name": "VARCHAR"},
        [{"customer_id": 1, "name": "a"}, {"customer_id": 2, "name": "b"}],
    )
    assert ("customer_id", "id") not in _pairs(mappings)
    assert ("name", "name") in _pairs(mappings)


def _up(port: int) -> bool:
    try:
        socket.create_connection(("127.0.0.1", port), timeout=1).close()
        return True
    except OSError:
        return False


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from services import connector_store
    from src.ai.copilot import tools as tools_mod

    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_PILOT_ENGINE", "local")
    monkeypatch.setattr(connector_store, "_backend_choice", None)
    monkeypatch.setattr(tools_mod, "_tools", None)
    return connector_store


def test_mcp_plan_never_proposes_a_foreign_key_onto_the_destination_primary_key(store) -> None:
    """Live: PostgreSQL ``(customer_id, name)`` into an existing ``(id PK, name)``."""
    if not _up(5432) or not os.environ.get("PGPASSWORD"):
        pytest.skip("PostgreSQL not configured")
    import psycopg2

    from src.ai.copilot import transfer_tools

    src_t = "d369s_" + uuid.uuid4().hex[:8]
    dst_t = "d369d_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="127.0.0.1", port=5432, dbname="dataflow", user="dataflow",
        password=os.environ["PGPASSWORD"],
    )
    conn.autocommit = True
    cur = conn.cursor()
    try:
        cur.execute(f'CREATE TABLE "{src_t}" (customer_id integer, name text)')
        cur.execute(f'INSERT INTO "{src_t}" VALUES (7, %s), (8, %s)', ("ada", "beau"))
        cur.execute(f'CREATE TABLE "{dst_t}" (id integer PRIMARY KEY, name text)')
        common = dict(
            type="postgresql", host="127.0.0.1", port=5432, database="dataflow",
            username="dataflow", password=os.environ["PGPASSWORD"], ssl=False, workspace_id="",
        )
        source = store.create_connector({"name": "d369-src-" + src_t, "role": "source", **common})
        dest = store.create_connector({"name": "d369-dst-" + dst_t, "role": "destination", **common})
        result = transfer_tools.plan_transfer(
            source_connector_id=str(source.id),
            source_table=src_t,
            dest_connector_id=str(dest.id),
            dest_table=dst_t,
            sync_mode="full_refresh_overwrite",
        )
        assert result.success, result.error
        mappings = result.output.get("engine_mappings") or result.output.get("mappings") or []
        assert mappings, result.output
        assert ("customer_id", "id") not in _pairs(mappings), mappings
        assert ("name", "name") in _pairs(mappings)
    finally:
        cur.execute(f'DROP TABLE IF EXISTS "{src_t}"')
        cur.execute(f'DROP TABLE IF EXISTS "{dst_t}"')
        conn.close()


def test_map_keys_come_from_the_catalog_not_the_client() -> None:
    """Review r2 minor 2: the client's list was the only key source."""
    from services.map_key_columns import resolve_map_key_columns

    catalog = {"columns": ["id", "Code", "name"], "primary_key_columns": ["code"], "identity_columns": []}
    # A stale list (the previous table's key) is ignored when the catalog was read.
    assert resolve_map_key_columns(["id"], target_columns=["id", "Code", "name"], catalog=catalog) == (
        ["Code"], "catalog",
    )
    # A table confirmed missing has no key, whatever the client says.
    assert resolve_map_key_columns(["id"], target_columns=[], catalog={"table_exists": False}) == ([], "catalog")
    # No catalog: only names that are columns of the mapped table survive.
    assert resolve_map_key_columns(
        ["ID", "ghost"], target_columns=["id", "name"], catalog=None
    ) == (["id"], "client_checked")
    assert resolve_map_key_columns(["ghost"], target_columns=["id"], catalog=None) == ([], "none")


def test_studio_map_route_reads_the_keys_of_the_destination_it_names(tmp_path, monkeypatch) -> None:
    """With the destination named, a stale key list cannot protect the wrong
    column: the route reads the table's own primary key (``code``), so ``id``
    is free again and ``code`` is protected."""
    import psycopg2
    from fastapi.testclient import TestClient

    import services.config  # noqa: F401 - settings parse before the harness blanks env
    from tests.helpers.live_env import pg_creds, pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not configured")
    from tests import r4_route_harness as h

    p = pg_creds()
    table = f"d369m_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=p["host"], port=int(p["port"]), user=p["username"], password=p["password"], dbname=p["database"]
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {table} (id INTEGER, code VARCHAR(20) PRIMARY KEY, name VARCHAR(40))")
    h.isolate(tmp_path, monkeypatch)
    from src.main import app

    body = {
        "source_columns": ["customer_id", "name"],
        "source_schema": {"customer_id": "INTEGER", "name": "VARCHAR"},
        "source_samples": {"customer_id": ["7", "8"], "name": ["ada", "beau"]},
        "target_columns": ["id", "code", "name"],
        "target_schema": {"id": "INTEGER", "code": "VARCHAR(20)", "name": "VARCHAR(40)"},
        "target_key_columns": ["id"],  # the previous table's key
        "destination_db_type": "postgresql",
        "source_db_type": "postgresql",
        "destination_table_exists": True,
        "use_llm": False,
        "destination": {
            "kind": "database", "format": "postgresql", "host": str(p["host"]), "port": int(p["port"]),
            "database": str(p["database"]), "username": str(p["username"]),
            "password": str(p["password"]), "schema": "public", "table": table,
        },
    }
    try:
        with TestClient(app) as client:
            named = client.post("/api/v1/transfer/map", json=body)
            unnamed = client.post(
                "/api/v1/transfer/map",
                json={**{k: v for k, v in body.items() if k != "destination"}, "target_key_columns": ["id", "ghost"]},
            )
        assert named.status_code == 200, named.text
        assert named.json()["target_key_columns"] == ["code"]
        assert named.json()["target_key_columns_source"] == "catalog"
        assert ("customer_id", "id") in _pairs(named.json()["mappings"])
        assert unnamed.status_code == 200, unnamed.text
        assert unnamed.json()["target_key_columns"] == ["id"]
        assert unnamed.json()["target_key_columns_source"] == "client_checked"
        assert ("customer_id", "id") not in _pairs(unnamed.json()["mappings"])
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
        conn.close()
