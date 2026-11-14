"""SEC-APPROVAL-1: Execute and schedule saves run only under a real Validate approval.

P1: Execute of a validated plan with the Validate stamps left out stamped its
    own preflight and wrote a column nobody approved.
P2: the approval did not bind the destination: Validate of table s1 ran into s2.
P3: a schedule save re-stamped whatever rows were saved.

The Studio flow (Validate, then Run with Validate's stamps) and a schedule saved
from a real Validate keep working.
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import g11_studio_flow as flow
from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(600)


@pytest.fixture
def client(tmp_path, monkeypatch):
    orig = monkeypatch.setenv

    def _setenv(name, value, prepend=None):
        if name == "DATAFLOW_SEED_DEMO" and value == "":
            value = "false"
        return orig(name, value, prepend)

    monkeypatch.setenv = _setenv
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient

    from src.main import app

    with TestClient(app) as c:
        yield c


def _world(token):
    h.pg_conn()
    s1, s2, schema = f"w1sec_s1_{token}", f"w1sec_s2_{token}", f"w1sec_{token}"
    for t in (s1, s2):
        h.execute("postgresql", f"CREATE TABLE {t} (id int PRIMARY KEY, v int)")  # nosec B608
        h.execute("postgresql", f"INSERT INTO {t} VALUES (1, 10), (2, 20)")  # nosec B608
    h.execute("postgresql", f"CREATE SCHEMA {schema}")
    for t in (s1, s2):
        h.execute("postgresql", f"CREATE TABLE {schema}.{t} (id bigint, v_ok bigint, v_evil bigint)")
    from services.connector_store import create_connector

    src = create_connector({"name": f"w1sec-src-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"w1sec-dst-{token}", "role": "destination", "workspace_id": "",
                            "ssl": False, **{**h.dest_config("postgresql"), "schema": schema}})
    source = {"kind": "database", "format": "postgresql", "connector_id": src.id,
              "database": h.PG["database"], "table": s1}
    destination = {"kind": "database", "format": "postgresql", "connector_id": dst.id,
                   "database": h.PG["database"], "schema": schema, "table": s1}
    maps = [{"source": "id", "target": "id", "confidence": 1.0, "source_type": "INTEGER", "target_type": "BIGINT"},
            {"source": "v", "target": "v_ok", "confidence": 1.0, "source_type": "INTEGER", "target_type": "BIGINT"}]
    return s1, s2, schema, source, destination, maps


def _drop(s1, s2, schema):
    for t in (s1, s2):
        h.drop_table("postgresql", t)
    h.execute("postgresql", f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _rows(schema, table, col):
    return h.query("postgresql", f"SELECT {col} FROM {schema}.{table} WHERE {col} IS NOT NULL")  # nosec B608


def _form(source, destination, plan_id, pf, mappings, *, stamps=True):
    proof = pf.get("proof_bundle") or {}
    artifact = proof.get("decision_artifact") or {}
    form = {
        "source_kind": "database", "source_format": source["format"],
        "source_connector_id": source.get("connector_id") or "", "source_database": source.get("database") or "",
        "source_table": source.get("table") or "",
        "dest_kind": "database", "dest_format": destination["format"],
        "dest_connector_id": destination.get("connector_id") or "",
        "dest_database": destination.get("database") or "",
        "dest_table": destination["table"], "dest_collection": destination["table"],
        "skip_preflight": "false", "async_mode": "false", "plan_id": plan_id,
        "mappings_json": json.dumps(mappings), "sync_mode": "full_refresh_append",
        "validation_mode": "strict", "schema_policy": "propagate_all",
    }
    if stamps:
        form.update({
            "approved_decision_artifact_hash": proof.get("decision_artifact_hash") or artifact["content_hash"],
            "approved_ddl_identity_hash": (proof.get("ddl_identity") or {}).get("ddl_identity_hash", ""),
            "decision_artifact_json": json.dumps(artifact),
        })
    return form


def _validate(client, source, destination, maps):
    plan_id, pf = flow.validate(
        client, source=source, destination=destination, columns=["id", "v"],
        types={"id": "INTEGER", "v": "INTEGER"}, mappings=maps,
        sync_mode="full_refresh_append", schema_policy="propagate_all")
    assert pf.get("passed") is True, flow.blockers(pf)
    return plan_id, pf


def _detail(resp):
    body = resp.json()
    return body.get("detail") if isinstance(body.get("detail"), dict) else body


def test_p1_a_validated_plan_without_its_stamps_is_refused_not_self_stamped(client):
    token = uuid.uuid4().hex[:8]
    s1, s2, schema, source, destination, maps = _world(token)
    try:
        plan_id, pf = _validate(client, source, destination, maps)
        evil = [dict(maps[0]), {**maps[1], "target": "v_evil"}]
        resp = client.post("/api/v1/transfer/run", data=_form(source, destination, plan_id, pf, evil, stamps=False))
        assert resp.status_code == 409, resp.text[:300]
        assert _detail(resp).get("code") == "validate_approval_required"
        assert _rows(schema, s1, "v_evil") == [] and _rows(schema, s1, "v_ok") == []
        # Control: the Studio flow, Validate's own stamps on the Map it approved, runs.
        ok = client.post("/api/v1/transfer/run",
                         data=_form(source, destination, plan_id, pf, pf.get("mappings") or maps))
        assert ok.status_code == 200 and _detail(ok).get("success") is True, ok.text[:400]
        assert len(_rows(schema, s1, "v_ok")) == 2
    finally:
        _drop(s1, s2, schema)


def test_p2_an_approval_for_table_s1_does_not_run_into_s2(client):
    token = uuid.uuid4().hex[:8]
    s1, s2, schema, source, destination, maps = _world(token)
    try:
        plan_id, pf = _validate(client, source, destination, maps)
        other = dict(destination, table=s2)
        resp = client.post("/api/v1/transfer/run", data=_form(source, other, plan_id, pf, pf.get("mappings") or maps))
        assert resp.status_code == 409, resp.text[:300]
        assert _detail(resp).get("code") == "validate_approval_destination"
        assert _rows(schema, s2, "v_ok") == []
    finally:
        _drop(s1, s2, schema)


def test_p2_binding_rules_unit():
    from services.validate_approvals import _binding_matches, binding_of

    rec = binding_of({"table": "s1", "connector_id": "c1"}, [{"name": "s1"}, {"name": "s2"}])
    assert _binding_matches(rec, binding_of({"table": "s1", "connector_id": "c1"},
                                            [{"name": "s2"}, {"name": "s1"}, {"name": "x", "selected": False}]))
    assert not _binding_matches(rec, binding_of({"table": "s2", "connector_id": "c1"}, [{"name": "s1"}, {"name": "s2"}]))
    assert not _binding_matches(rec, binding_of({"table": "s1", "connector_id": "c1"}, [{"name": "s1"}]))
    assert not _binding_matches(rec, binding_of({"table": "s1", "connector_id": "c2"}, [{"name": "s1"}, {"name": "s2"}]))
    single = binding_of({"table": "s1"}, [])
    assert _binding_matches(single, binding_of({"table": "s1"}, [{"name": "s1"}]))
    assert not _binding_matches(single, binding_of({"table": "s1"}, [{"name": "s2"}]))


def _schedule_body(source, destination, maps, name, decision="", ddl=""):
    return {"name": name, "interval": "daily", "enabled": False,
            "source_connector_id": source["connector_id"], "source_table": source["table"],
            "dest_connector_id": destination["connector_id"], "dest_table": destination["table"],
            "sync_mode": "full_refresh_append", "mappings": maps,
            "approved_decision_artifact_hash": decision, "approved_ddl_identity_hash": ddl}


def test_p3_a_schedule_keeps_only_a_stamp_a_real_validate_gave_these_rows(client):
    from services.decision_kernel import build_artifact_from_mappings

    token = uuid.uuid4().hex[:8]
    s1, s2, schema, source, destination, maps = _world(token)
    try:
        # A self-computed hash for rows nobody validated is not an approval.
        evil = [dict(maps[0]), {**maps[1], "target": "v_evil"}]
        forged = build_artifact_from_mappings(
            evil, dest_db="postgresql", source_db="postgresql", source_fingerprint="", dest_fingerprint="",
            sync_mode="full_refresh_append", route_id="validate:postgresql", tenant_id="anonymous",
            artifact_id="da_inline", created_at="1970-01-01T00:00:00+00:00").content_hash
        r = client.post("/api/v1/schedules/", json=_schedule_body(source, destination, evil, f"w1sec-f-{token}", forged))
        assert r.status_code == 201, r.text[:300]
        assert r.json()["approved_decision_artifact_hash"] == ""
        assert any("not saved" in w for w in r.json().get("warnings") or []), r.json().get("warnings")
        # A Validate's own stamp on the rows it approved is kept.
        _plan_id, pf = _validate(client, source, destination, maps)
        proof = pf.get("proof_bundle") or {}
        decision = proof.get("decision_artifact_hash") or proof["decision_artifact"]["content_hash"]
        ddl = (proof.get("ddl_identity") or {}).get("ddl_identity_hash", "")
        rows = pf.get("mappings") or maps
        r = client.post("/api/v1/schedules/", json=_schedule_body(source, destination, rows, f"w1sec-v-{token}",
                                                                 decision, ddl))
        assert r.status_code == 201, r.text[:300]
        assert r.json()["approved_decision_artifact_hash"], r.json().get("warnings")
        sid = r.json()["id"]
        # Editing the rows does not carry the approval onto rows Validate never saw.
        p = client.patch(f"/api/v1/schedules/{sid}", json={"mappings": evil})
        assert p.status_code == 200, p.text[:300]
        assert p.json()["approved_decision_artifact_hash"] == "", p.json()
        # Nor does sending Validate's stamp for other rows.
        r = client.post("/api/v1/schedules/", json=_schedule_body(source, destination, evil, f"w1sec-x-{token}",
                                                                 decision, ddl))
        assert r.status_code == 201 and r.json()["approved_decision_artifact_hash"] == "", r.text[:300]
    finally:
        _drop(s1, s2, schema)
