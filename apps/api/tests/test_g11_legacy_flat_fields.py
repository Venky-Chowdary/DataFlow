"""G11 compatibility: MongoDB / Elasticsearch targets whose old spelling exists.

Before G11 a MongoDB target ``address.city`` was written as the top-level
field ``address_city`` and an Elasticsearch ``@timestamp`` as ``_timestamp``
(``host.name`` as ``host_name``). Those targets now keep their names, so a run
into a collection or index an earlier run filled would silently mix both
shapes and leave the old field stale. Validate (``g23e_target_legacy_fields``,
code ``target_path_legacy_flat_field``) names both spellings and the remedies
(rename the target, one overwrite run, or ``$rename``). It blocks a run that
would mix shapes (append / upsert) unless the operator set
``legacy_field_policy=write_new``; an overwrite only warns. Nothing is reported
for a new or empty collection or one already holding the nested shape.
(Review #3 turned the Validate warning into the same block Execute applies.)
"""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def _written_before_g11(monkeypatch):
    """These collections / indexes / prefixes / tables were written by runs
    before the G11 upgrade, so the old-field check applies to them
    (``naming_history.object_history``; final review M-d)."""
    import services.naming_history as naming_history

    monkeypatch.setattr(naming_history, "object_history",
                        lambda *a, **k: naming_history.LEGACY)

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _maps(*targets: str) -> list[dict]:
    return [{"source": f"c{i}", "target": t, "confidence": 1.0} for i, t in enumerate(targets)]


def _gates(dest_type: str, targets, sync_mode: str = "full_refresh_append", **kw) -> dict[str, dict]:
    from services.preflight_transfer_policy import run_transfer_policy_gates

    gates = run_transfer_policy_gates(
        sync_mode=sync_mode, dest_type=dest_type, source_type="postgresql",
        source_kind="database", mappings=_maps(*targets),
        destination_table=kw.pop("destination_table", "t"), **kw,
    )
    return {g["id"]: g for g in gates if str(g.get("id", "")).startswith("g23")}


MONGO_URI = os.environ.get("DATAFLOW_TEST_MONGO_URI", "mongodb://localhost:27017")


def _mongo_up() -> bool:
    try:
        socket.create_connection(("localhost", 27017), timeout=1).close()
        import pymongo

        pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=1500).server_info()
    except Exception:  # noqa: BLE001 - any failure means "not available"
        return False
    return True


@pytest.fixture
def mongo_db():
    import pymongo

    name = f"g11m_{uuid.uuid4().hex[:10]}"
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        yield client, name
    finally:
        client.drop_database(name)
        client.close()


@pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")
def test_mongo_existing_flat_field_warns_with_remedies(mongo_db):
    client, db = mongo_db
    client[db]["legacy"].insert_one({"id": 1, "address_city": "Paris"})
    client[db]["nested"].insert_one({"id": 1, "address": {"city": "Paris"}})
    client[db].create_collection("empty")
    cfg = {"connection_string": MONGO_URI, "database": db}

    gates = _gates("mongodb", ["id", "address.city"], destination_config=cfg,
                   destination_table="legacy")
    warn = gates["g23e_target_legacy_fields"]
    assert warn["status"] == "block"
    assert "legacy_field_policy='write_new'" in warn["message"]
    issue = warn["details"]["issues"][0]
    assert issue["code"] == "target_path_legacy_flat_field"
    assert (issue["target"], issue["other"]) == ("address.city", "address_city")
    for needle in ("'address_city'", "'address.city'", "overwrite", "$rename"):
        assert needle in warn["message"], warn["message"]
    acked = _gates("mongodb", ["id", "address.city"], destination_config=cfg,
                   destination_table="legacy",
                   destination_endpoint={"format": "mongodb", "legacy_field_policy": "write_new"})
    assert acked["g23e_target_legacy_fields"]["status"] == "warn"
    replaced = _gates("mongodb", ["id", "address.city"], "full_refresh_overwrite",
                      destination_config=cfg, destination_table="legacy")
    assert replaced["g23e_target_legacy_fields"]["status"] == "warn"
    for table in ("nested", "empty", "missing"):
        assert _gates("mongodb", ["id", "address.city"], destination_config=cfg,
                      destination_table=table) == {}, table


def test_elasticsearch_existing_renamed_field_warns():
    mapping = {"logs": {"mappings": {"properties": {
        "_timestamp": {"type": "date"}, "host_name": {"type": "keyword"},
        "level": {"type": "keyword"},
    }}}}
    client = MagicMock()
    client.indices.get_mapping.return_value = mapping
    with patch("connectors.elasticsearch_reader._client", return_value=client):
        gates = _gates("elasticsearch", ["@timestamp", "host.name", "level"],
                       destination_config={"host": "localhost", "port": 9200},
                       destination_table="logs")
    issues = gates["g23e_target_legacy_fields"]["details"]["issues"]
    assert [(i["target"], i["other"]) for i in issues] == [
        ("@timestamp", "_timestamp"), ("host.name", "host_name")]

    client.indices.get_mapping.return_value = {"logs": {"mappings": {"properties": {
        "@timestamp": {"type": "date"},
        "host": {"properties": {"name": {"type": "keyword"}}},
    }}}}
    with patch("connectors.elasticsearch_reader._client", return_value=client):
        assert _gates("elasticsearch", ["@timestamp", "host.name"],
                      destination_config={"host": "localhost"},
                      destination_table="logs") == {}




@pytest.fixture
def api(tmp_path, monkeypatch):
    from tests import r4_route_harness as h

    h.isolate(tmp_path, monkeypatch)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "false")
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.mark.skipif(not _mongo_up(), reason="no MongoDB on :27017")
def test_validate_route_shows_the_legacy_field_warning(api, mongo_db):
    client, db = mongo_db
    client[db]["customers"].insert_one({"id": 1, "address_city": "Paris"})
    resp = api.post(
        "/api/v1/preflight/run",
        json={
            "columns": ["id", "city"],
            "column_types": {"id": "INTEGER", "city": "TEXT"},
            "row_count": 1,
            "sample_rows": [{"id": 1, "city": "Paris"}],
            "source_kind": "database",
            "source_type": "postgresql",
            "dest_kind": "database",
            "dest_type": "mongodb",
            "dest_connection_string": MONGO_URI,
            "dest_database": db,
            "dest_collection": "customers",
            "sync_mode": "upsert",
            "mappings": [
                {"source": "id", "target": "id", "confidence": 1.0},
                {"source": "city", "target": "address.city", "confidence": 1.0},
            ],
        },
    )
    assert resp.status_code == 200, resp.text
    gates = {g.get("id"): g for g in resp.json().get("gates") or []}
    warn = gates.get("g23e_target_legacy_fields")
    assert warn, sorted(gates)
    assert warn["details"]["issues"][0]["code"] == "target_path_legacy_flat_field"
