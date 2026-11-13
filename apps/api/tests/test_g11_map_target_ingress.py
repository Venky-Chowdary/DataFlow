"""G11 security re-check F1(a) / F4: no inbound ``map_target`` reaches Execute.

``map_target`` is the server's record of a legacy-column adoption. 6b6a32cd1
stripped it in ``TransferRequest.__post_init__`` only, but the multipart routes
assign ``request_obj.mappings`` after construction (``/api/v1/transfer/run``,
the plan merge, ``POST /plans/{id}/mappings``, connectors ``/transfer``), and
``stream_contracts[*].mappings`` were never stripped. A reverse-crafted row
``{target: v_ok, map_target: v_evil}`` matched an approval for ``v -> v_ok``
and the multi-stream loop wrote ``v_evil`` (live: success, Gate-8 passed).

Now one choke point every Execute passes (the top of ``request_refusal``, before
adoption) strips it from the Map and from every stream contract; stored plans
and schedules lose it on write and on read.
"""

from __future__ import annotations

import json
import uuid
from unittest.mock import patch

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)

CRAFTED = [{"source": "id", "target": "id"},
           {"source": "v", "target": "v_ok", "map_target": "v_evil"}]


def _request():
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database="/x.db", table="t"),
        destination=EndpointConfig(kind="database", format="postgresql", table="d"),
        sync_mode="append")


def test_request_refusal_strips_rows_assigned_after_construction():
    from services.source_filter_gate import request_refusal

    req = _request()
    req.mappings = [dict(m) for m in CRAFTED]  # what the multipart routes do
    req.stream_contracts = [
        {"name": "a", "selected": True, "mappings": [dict(m) for m in CRAFTED]},
        {"name": "b", "selected": True},
    ]
    with patch("services.target_name_legacy.adopt_for_request", return_value=[]):
        request_refusal(req)
    assert all("map_target" not in m for m in req.mappings)
    assert [m["target"] for m in req.mappings] == ["id", "v_ok"]
    assert all("map_target" not in m for m in req.stream_contracts[0]["mappings"])


def test_a_client_map_target_never_stamps_legacy():
    """F4: the stamp is the server's decision, never a client key."""
    from services import mongodb_service
    from services.naming_history import naming_job_scope
    from services.source_filter_gate import request_refusal
    from src.transfer.models import EndpointConfig, TransferRequest

    svc = mongodb_service.MemoryMongoDBService()
    svc.connect()
    with patch.object(mongodb_service, "_mongodb_service", svc):
        req = TransferRequest(
            source=EndpointConfig(kind="database", format="postgresql", database="d",
                                  table="t"),
            destination=EndpointConfig(kind="database", format="kafka", host="broker",
                                       port=9092, table="topic"),
            sync_mode="append")
        req.mappings = [{"source": "fn", "target": "First Name", "map_target": "x"}]
        job = svc.create_transfer_job({"workspace_id": "", "destination_type": "kafka"})
        with naming_job_scope(job, ""):
            assert request_refusal(req) == ""
        stamps = svc._jobs[job].get("naming") or []
    assert all(s.get("mode") != "legacy" for s in stamps), stamps


def test_stored_plans_lose_map_target_on_write_and_read():
    from services.transfer_plan_store import PlanRevision

    rev = PlanRevision.from_dict({"version": 1, "mappings": [dict(m) for m in CRAFTED]})
    assert all("map_target" not in m for m in rev.mappings)


def test_stored_schedules_lose_map_target_on_read():
    from services.schedule_store import PipelineSchedule

    sched = PipelineSchedule.from_dict({
        "id": "s", "name": "s", "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": "d", "interval": "daily",
        "mappings": [dict(m) for m in CRAFTED],
        "stream_contracts": [{"name": "t", "mappings": [dict(m) for m in CRAFTED]}],
    })
    assert all("map_target" not in m for m in sched.mappings)
    assert all("map_target" not in m for m in sched.stream_contracts[0]["mappings"])


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _form(source, destination, plan_id, pf, mappings, contracts=None):
    proof = pf.get("proof_bundle") or {}
    artifact = proof.get("decision_artifact") or {}
    return {
        "source_kind": "database", "source_format": source["format"],
        "source_connector_id": source.get("connector_id") or "",
        "source_database": source.get("database") or "",
        "source_table": source.get("table") or "",
        "dest_kind": "database", "dest_format": destination["format"],
        "dest_connector_id": destination.get("connector_id") or "",
        "dest_database": destination.get("database") or "",
        "dest_table": destination["table"], "dest_collection": destination["table"],
        "skip_preflight": "false", "async_mode": "false", "plan_id": plan_id,
        "mappings_json": json.dumps(mappings), "sync_mode": "full_refresh_append",
        "validation_mode": "strict", "schema_policy": "propagate_all",
        "approved_decision_artifact_hash":
            proof.get("decision_artifact_hash") or artifact["content_hash"],
        "approved_ddl_identity_hash":
            (proof.get("ddl_identity") or {}).get("ddl_identity_hash", ""),
        "decision_artifact_json": json.dumps(artifact),
        **({"stream_contracts_json": json.dumps(contracts)} if contracts else {}),
    }


def multi_stream_world(token):
    """Two PostgreSQL source tables and two destination tables with v_ok and v_evil."""
    s1, s2, schema = f"g11i1_{token}", f"g11i2_{token}", f"g11i_{token}"
    for t in (s1, s2):
        h.execute("postgresql", f"CREATE TABLE {t} (id int PRIMARY KEY, v int)")  # nosec B608
        h.execute("postgresql", f"INSERT INTO {t} VALUES (1, 10), (2, 20)")  # nosec B608
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    for t in (s1, s2):
        h.execute("postgresql",
                  f"CREATE TABLE {schema}.{t} (id bigint, v_ok bigint, v_evil bigint)")
    from services.connector_store import create_connector

    src = create_connector({"name": f"g11i-src-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"g11i-dst-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False,
                            **{**h.dest_config("postgresql"), "schema": schema}})
    source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
              "database": h.PG["database"], "table": s1}
    destination = {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                   "database": h.PG["database"], "schema": schema, "table": s1}
    maps = [{"source": "id", "target": "id", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"},
            {"source": "v", "target": "v_ok", "confidence": 1.0, "source_type": "INTEGER",
             "target_type": "BIGINT"}]
    return s1, s2, schema, source, destination, maps


def drop_world(s1, s2, schema):
    for t in (s1, s2):
        h.drop_table("postgresql", t)
    h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _column(schema, tables, col):
    return [r for t in tables for r in h.query(
        "postgresql", f"SELECT {col} FROM {schema}.{t} WHERE {col} IS NOT NULL")]  # nosec B608


def test_live_multistream_reverse_crafted_writes_no_unapproved_column(client):
    """The reviewer's probe: approved v -> v_ok, Execute sends
    {target: v_ok, map_target: v_evil} on a two-stream route."""
    token = uuid.uuid4().hex[:8]
    s1, s2, schema, source, destination, maps = multi_stream_world(token)
    try:
        contracts = [{"name": s1, "selected": True, "sync_mode": "full_refresh_append"},
                     {"name": s2, "selected": True, "sync_mode": "full_refresh_append"}]
        plan_id, pf = flow.validate(
            client, source=source, destination=destination, columns=["id", "v"],
            types={"id": "INTEGER", "v": "INTEGER"}, mappings=maps,
            sync_mode="full_refresh_append", stream_contracts=contracts,
            schema_policy="propagate_all")
        crafted = [dict(r) for r in (pf.get("mappings") or maps)]
        for r in crafted:
            if r.get("target") == "v_ok":
                r["map_target"] = "v_evil"
        client.post("/api/v1/transfer/run",
                    data=_form(source, destination, plan_id, pf, crafted, contracts))
        assert _column(schema, (s1, s2), "v_evil") == []
    finally:
        drop_world(s1, s2, schema)
