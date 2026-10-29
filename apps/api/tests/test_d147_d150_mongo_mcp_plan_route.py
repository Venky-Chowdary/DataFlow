"""D147 / D150 on the route QA drives: saved connector -> ``plan_transfer``.

The engine route (``execute_tracked``) loaded MongoDB documents with dates, a
nested sub-document that is empty in one document and sparse keys. The MCP
route did not, because it differs from the engine in three places:

* it mapped without the source engine identity, so a BSON date read as a
  zoneless TIMESTAMP and VARCHAR as a code-page type (every mapping
  "lossy_cast", a risk contract demanded for ``TIMESTAMP -> TIMESTAMP``);
* its gate sample was the query preview, whose JSON drops the UTC the reader
  stamps on a BSON date ("TIMESTAMPTZ refuses naive wall-clock");
* its destination introspection dropped the connector's saved handshake keys
  (SQL Server trust flag), so the destination read as unreachable and every
  mapping sat at "pending Studio/Map stamp".
"""

from __future__ import annotations

import datetime as dt
import os
import socket
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

pymongo = pytest.importorskip("pymongo")

T0 = dt.datetime(2024, 1, 2, 3, 4, 5, 123000)


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


@pytest.fixture()
def collection():
    if not _up(27017):
        pytest.skip("MongoDB not reachable on :27017")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    name = "mcp147_" + uuid.uuid4().hex[:8]
    coll = client["dataflow_test"][name]
    try:
        yield coll
    finally:
        coll.drop()
        client.close()


def _docs():
    return [
        {"_id": 1, "name": "a", "created_at": T0, "meta": {"tier": "x", "since": T0}, "payload_k": "k1"},
        {"_id": 2, "name": "b", "created_at": T0, "meta": {}},
        {"_id": 3, "name": "c", "created_at": T0, "meta": {"tier": "z", "since": T0}, "payload_k": "k3"},
    ]


def _destination(store, kind: str):
    if kind == "postgresql":
        if not _up(5432) or not os.environ.get("PGPASSWORD"):
            pytest.skip("PostgreSQL not configured")
        data = dict(type="postgresql", host="127.0.0.1", port=5432, database="dataflow",
                    username="dataflow", password=os.environ["PGPASSWORD"])
    elif kind == "mysql":
        if not _up(3306) or not os.environ.get("MYSQL_PASSWORD"):
            pytest.skip("MySQL not configured")
        data = dict(type="mysql", host="127.0.0.1", port=3306, database="dataflow",
                    username="dataflow", password=os.environ["MYSQL_PASSWORD"])
    else:
        if not _up(1433) or not os.environ.get("DATAFLOW_TEST_MSSQL_PASSWORD"):
            pytest.skip("SQL Server not configured")
        data = dict(type="sqlserver", host="127.0.0.1", port=1433, database="dataflow",
                    username="sa", password=os.environ["DATAFLOW_TEST_MSSQL_PASSWORD"],
                    extra={"trust_server_certificate": True})
    return store.create_connector(
        {"name": f"d147-{kind}-{uuid.uuid4().hex[:6]}", "role": "destination",
         "workspace_id": "", "ssl": False, **data}
    )


@pytest.mark.parametrize("kind", ["postgresql", "mysql", "sqlserver"])
@pytest.mark.parametrize("sparse_first", [False, True], ids=["dense_first", "sparse_first"])
def test_plan_of_ordinary_documents_into_a_new_table_is_safe_to_start(
    store, collection, kind, sparse_first
):
    from src.ai.copilot import transfer_tools

    docs = _docs()
    collection.insert_many([docs[1], docs[0], docs[2]] if sparse_first else docs)
    source = store.create_connector(
        {"name": "d147-mongo-" + uuid.uuid4().hex[:6], "type": "mongodb", "role": "source",
         "host": "127.0.0.1", "port": 27017, "database": "dataflow_test", "ssl": False,
         "workspace_id": ""}
    )
    destination = _destination(store, kind)

    result = transfer_tools.plan_transfer(
        source_connector_id=str(source.id),
        source_table=collection.name,
        dest_connector_id=str(destination.id),
        dest_table=collection.name + "_out",
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )

    assert result.success, result.error
    plan = result.output
    blockers = (plan.get("preflight") or {}).get("blockers") or []
    assert plan["safe_to_start"] is True, blockers
    assert not blockers
    by_source = {m["source"]: m for m in plan["engine_mappings"]}
    # The BSON date is an instant and stays one, beside the nested one.
    assert by_source["created_at"]["fidelity"] == "preserve"
    assert by_source["meta_since"]["fidelity"] == "preserve"
    assert not any(m.get("requires_risk_contract") for m in by_source.values())
    assert set(by_source) >= {"payload_k", "meta_tier", "meta_since"}


def test_destination_introspection_carries_the_connectors_handshake_keys(store, monkeypatch):
    import services.schema_introspect as si
    from src.ai.copilot.schema_tools import introspect_connector_table

    seen: dict = {}

    def fake(db_type, **kwargs):
        seen.update(kwargs)
        return {"ok": True, "columns": [], "tables": []}

    monkeypatch.setattr(si, "introspect_schema", fake)
    conn = store.create_connector(
        {"name": "d147-trust", "type": "sqlserver", "role": "destination", "host": "h",
         "database": "d", "username": "u", "password": "p", "workspace_id": "",
         "extra": {"trust_server_certificate": True}}
    )
    from src.ai.copilot.schema_tools import _connector_dict

    introspect_connector_table(_connector_dict(str(conn.id), ""), "t", purpose="destination")

    assert seen.get("trust_server_certificate") is True


def test_a_document_store_is_sampled_through_the_execute_reader(monkeypatch):
    from services import preflight_sample
    from src.ai.copilot import query_tools, transfer_tools

    reader = preflight_sample.EngineSample(rows=[{"created_at": "2024-01-02T03:04:05.123000+00:00"}])
    monkeypatch.setattr(preflight_sample, "engine_sample_rows", lambda **_k: reader)

    naive = SimpleNamespace(
        success=True, output={"rows": [{"created_at": "2024-01-02T03:04:05.123000"}]}
    )
    monkeypatch.setattr(query_tools, "sample_connector_object", lambda **_k: naive)

    rows = transfer_tools._sample_rows({"id": "c1", "type": "mongodb"}, "orders")

    assert rows == [{"created_at": "2024-01-02T03:04:05.123000+00:00"}]
