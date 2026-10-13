"""MX2-11 — Mongo nested object: the transfer plan must see the reader's columns.

QA: ``start_transfer`` previewed 13 mapped columns and no unmapped ones, then
the job failed G13 at runtime with "3 source column(s) are neither mapped nor
declared omitted: attributes_color, attributes_dims, attributes_label". The
Pilot plan read the source through ``introspect_schema`` (raw top-level keys)
while the Execute reader expands nested objects into ``parent_child`` columns.
Live against local MongoDB.
"""

from __future__ import annotations

import os
import socket
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
        return True
    except Exception:
        return False


live = pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")

ROWS = 12


@pytest.fixture()
def products():
    import pymongo

    db_name = f"df_mx211_{uuid.uuid4().hex[:8]}"
    client = pymongo.MongoClient(MONGO_URI)
    client[db_name]["products"].insert_many(
        [
            {
                "sku": f"SKU-{i:03d}",
                "name": f"Product {i}",
                "price": 10.25 + i,
                "attributes": {"color": "red" if i % 2 else "blue", "dims": f"{i}x2", "label": f"L{i}"},
            }
            for i in range(ROWS)
        ]
    )
    try:
        yield {"database": db_name, "collection": "products"}
    finally:
        client.drop_database(db_name)
        client.close()


def _conn(products) -> dict:
    return {
        "id": "",
        "name": "QA Mongo",
        "type": "mongodb",
        "connection_string": MONGO_URI,
        "database": products["database"],
    }


def _reader_headers(products) -> list[str]:
    from connectors.mongodb_reader import read_collection_batch

    return read_collection_batch(
        cfg={"connection_string": MONGO_URI},
        database=products["database"],
        collection=products["collection"],
    ).headers


@live
def test_plan_introspect_lists_every_column_the_reader_emits(products):
    from src.ai.copilot.schema_tools import introspect_connector_table

    info = introspect_connector_table(
        _conn(products), "products", purpose="source", execute_shape=True
    )
    planned = [c["name"] for c in info["columns"]]
    reader = _reader_headers(products)
    assert set(planned) == set(reader), {"plan": planned, "reader": reader}
    assert {"attributes_color", "attributes_dims", "attributes_label"} <= set(planned)


@live
def test_query_introspect_keeps_raw_document_keys(products):
    """$group / $match pipelines address ``attributes.color``, not a flattened name."""
    from src.ai.copilot.schema_tools import introspect_connector_table

    info = introspect_connector_table(_conn(products), "products", purpose="source")
    names = {c["name"] for c in info["columns"]}
    assert "attributes" in names
    assert not any(n.startswith("attributes_") for n in names), names


@live
def test_flatten_nested_off_keeps_plan_and_reader_aligned(products):
    from services.schema_introspect import introspect_schema

    cfg = {"connection_string": MONGO_URI, "flatten_nested": False}
    info = introspect_schema(
        "mongodb",
        connection_string=MONGO_URI,
        database=products["database"],
        table="products",
        reader_cfg=cfg,
    )
    from connectors.mongodb_reader import read_collection_batch

    reader = read_collection_batch(
        cfg=cfg, database=products["database"], collection=products["collection"]
    ).headers
    assert {c["name"] for c in info["columns"]} == set(reader)


def test_plan_transfer_asks_for_the_execute_shape(monkeypatch):
    """plan_transfer must request the reader's shape for a table source."""
    seen: dict[str, object] = {}

    def _fake_introspect(conn, table, purpose="", execute_shape=False):
        if purpose == "source":
            seen["execute_shape"] = execute_shape
            raise RuntimeError("stop after source introspect")
        return {"ok": True, "columns": [], "table_exists": False, "db_type": "sqlite", "error": ""}

    def _fake_connector(cid, name, tool):
        role = "source" if "Mongo" in (name or "") else "destination"
        return {"id": f"{role}-1", "name": name or role, "type": "mongodb" if role == "source" else "sqlite"}, None

    import src.ai.copilot.transfer_tools as tt

    monkeypatch.setattr(tt, "_safe_connector", _fake_connector)
    monkeypatch.setattr(tt, "_introspect", _fake_introspect)
    tt.plan_transfer(
        source_connector_name="QA Mongo",
        source_table="products",
        dest_connector_name="QA SQLite",
        dest_table="QA_E2E_RT_mongo_products",
        sync_mode="full_refresh_overwrite",
    )
    assert seen.get("execute_shape") is True


@live
def test_mongo_nested_to_sqlite_runs_with_plan_mappings(products, tmp_path: Path, monkeypatch):
    """Map from the plan's columns, then Execute with preflight on: no G13 surprise."""
    from src.ai.copilot.schema_tools import introspect_connector_table
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            return True

    fake = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    import inspect

    # Exactly the call plan_transfer makes for a table source (pre-fix it had
    # no execute_shape and the plan only saw the raw top-level keys).
    kwargs = {"purpose": "source"}
    if "execute_shape" in inspect.signature(introspect_connector_table).parameters:
        kwargs["execute_shape"] = True
    info = introspect_connector_table(_conn(products), "products", **kwargs)
    mappings = [
        {"source": c["name"], "target": c["name"], "confidence": 1.0}
        for c in info["columns"]
    ]
    dest = tmp_path / "dest.sqlite"
    request = TransferRequest(
        mappings=mappings,
        source=EndpointConfig(
            kind="database",
            format="mongodb",
            connection_string=MONGO_URI,
            database=products["database"],
            collection="products",
            table="products",
        ),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(dest), table="QA_E2E_RT_mongo_products"
        ),
        sync_mode="full_refresh_overwrite",
        validation_mode="balanced",
    )
    job_id = "mx211" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert "neither mapped nor declared omitted" not in str(result.error or ""), result.error
    assert result.success, result.error

    import sqlite3

    conn = sqlite3.connect(str(dest))
    try:
        cols = [r[1] for r in conn.execute('PRAGMA table_info("QA_E2E_RT_mongo_products")')]
        assert {"attributes_color", "attributes_dims", "attributes_label"} <= set(cols), cols
        n = conn.execute('SELECT COUNT(*) FROM "QA_E2E_RT_mongo_products"').fetchone()[0]
        assert n == ROWS
    finally:
        conn.close()
