"""QA round 12, bugs 6 and 7: an unknown ``validation_mode`` / ``schema_policy`` was queued.

QA (8f7d246d, still true on 2eb5e3c0): ``POST /api/v1/transfer/execute`` with
``validation_mode: "bogus"`` answered 200 and started a job (the engine then ran
it as ``strict``); an unknown ``schema_policy`` was accepted the same way, and
so were schedules, MCP ``plan_transfer`` / ``start_transfer`` /
``create_schedule`` and GitOps manifests.

Contract pinned here, at every entry point, on real PostgreSQL:

* ``"bogus"`` -> 422 (MCP: a tool error) naming the field and listing every
  allowed value, taken from the canonical sets
  (``validation_mode_contract.VALIDATION_MODES``, ``schema_drift.SCHEMA_POLICIES``);
  nothing is queued or stored;
* ``""`` and whitespace mean "not given": the entry point's own default;
* a mixed-case / padded valid value is accepted as its canonical id;
* a documented legacy spelling is accepted as its canonical id
  (``block`` -> ``block_all``, ``approve`` -> ``manual_review``; ``maximum`` is
  the legacy Studio validation mode and stays itself).
"""

from __future__ import annotations

import json
import uuid

import pytest

from tests import r4_route_harness as h

pytestmark = pytest.mark.timeout(900)

VALIDATION_MODES = (
    "audit", "balanced", "discovery", "lenient", "maximum", "migration", "strict",
)
SCHEMA_POLICIES = (
    "block_all", "manual_review", "pause_on_change", "propagate_all",
    "propagate_columns", "type_locked",
)
ALLOWED = {"validation_mode": VALIDATION_MODES, "schema_policy": SCHEMA_POLICIES}

#: (sent, canonical) — "" canonical means "the entry point's default".
ACCEPTED = {
    "validation_mode": [
        ("", ""), ("   ", ""), ("  BaLanced ", "balanced"), ("Maximum", "maximum"),
    ],
    "schema_policy": [
        ("", ""), (" \t ", ""), ("Propagate_Columns", "propagate_columns"),
        ("BLOCK", "block_all"), ("approve", "manual_review"),
    ],
}
FIELDS = ("validation_mode", "schema_policy")


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def pg_pair(client):
    """A real PG source table (NULL, '', whitespace, unicode) and source/dest connectors."""
    from services.connector_store import create_connector

    token = uuid.uuid4().hex[:8]
    table = f"r12e_{token}"
    h.execute("postgresql", f'CREATE TABLE "{table}" (id INT PRIMARY KEY, note TEXT)')
    h.execute(
        "postgresql",
        f"INSERT INTO \"{table}\" VALUES (1, 'a'), (2, NULL), (3, ''), (4, '   '), "  # nosec B608
        "(5, 'Zoë ✓ 中文')",
    )
    src = create_connector({"name": f"r12e-src-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    dst = create_connector({"name": f"r12e-dst-{token}", "role": "destination", "workspace_id": "",
                            "ssl": False, **h.dest_config("postgresql")})
    created: list[str] = []
    yield {"src": src, "dst": dst, "table": table, "token": token, "created": created}
    for name in [table, *created]:
        h.drop_table("postgresql", name)


def _assert_422_names(resp, field: str, sent) -> None:
    assert resp.status_code == 422, (field, sent, resp.status_code, resp.text[:400])
    text = json.dumps(resp.json())
    assert field in text, text
    missing = [v for v in ALLOWED[field] if v not in text]
    assert not missing, (field, missing, text[:600])


def _assert_tool_error_names(ok: bool, payload, field: str) -> None:
    assert not ok, (field, str(payload)[:300])
    text = str(payload)
    assert field in text and "bogus" in text, text
    missing = [v for v in ALLOWED[field] if v not in text]
    assert not missing, (field, missing, text)


def _jobs() -> list[dict]:
    from services.mongodb_service import get_mongodb_service

    svc = get_mongodb_service()
    return list(svc.list_jobs(limit=500) if hasattr(svc, "list_jobs") else [])


def _job_request(job_id: str) -> dict:
    from services.mongodb_service import get_mongodb_service

    job = get_mongodb_service().get_job(job_id) or {}
    return job.get("transfer_request") or {}


# ---------------------------------------------------------------- REST execute

def _execute_body(pair, dest_table: str, **policy) -> dict:
    return {
        "source": {"kind": "database", "format": "postgresql",
                   "connector_id": pair["src"].id, "table": pair["table"]},
        "destination": {"kind": "database", "format": "postgresql",
                        "connector_id": pair["dst"].id, "table": dest_table},
        "mappings": [{"source": "id", "target": "id", "confidence": 1.0},
                     {"source": "note", "target": "note", "confidence": 1.0}],
        "async_mode": True, "skip_preflight": True, **policy,
    }


def test_rest_execute_refuses_unknown_and_canonicalizes_known(client, pg_pair):
    before = len(_jobs())
    for field in FIELDS:
        resp = client.post("/api/v1/transfer/execute",
                           json=_execute_body(pg_pair, f"{pg_pair['table']}_x", **{field: "bogus"}))
        _assert_422_names(resp, field, "bogus")
    assert len(_jobs()) == before, "a refused request queued a job"

    defaults = {"validation_mode": "strict", "schema_policy": "manual_review"}
    job_ids = []
    for field in FIELDS:
        for i, (sent, canonical) in enumerate(ACCEPTED[field]):
            dest = f"{pg_pair['table']}_{field[0]}{i}"
            pg_pair["created"].append(dest)
            resp = client.post("/api/v1/transfer/execute",
                               json=_execute_body(pg_pair, dest, **{field: sent}))
            assert resp.status_code == 200, (field, sent, resp.text[:400])
            job_id = resp.json()["job_id"]
            job_ids.append(job_id)
            assert _job_request(job_id).get(field) == (canonical or defaults[field]), (
                field, sent, _job_request(job_id).get(field))
    for job_id in job_ids:
        h.wait_job(job_id)
    # Second run into the table DataFlow created, with a canonicalized alias.
    dest = f"{pg_pair['table']}_s0"
    resp = client.post("/api/v1/transfer/execute", json={
        **_execute_body(pg_pair, dest, schema_policy="Propagate_Columns"), "async_mode": False})
    assert resp.status_code == 200, resp.text[:400]
    rows = h.query("postgresql", f'SELECT id, note FROM "{dest}" ORDER BY id')  # nosec B608
    assert rows == [(1, "a"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓ 中文")], rows


# ------------------------------------------------------------ Studio /run form

def _run_form(pair, dest_table: str, **policy) -> dict:
    return {
        "source_kind": "database", "source_format": "postgresql",
        "source_connector_id": pair["src"].id, "source_table": pair["table"],
        "dest_kind": "database", "dest_format": "postgresql",
        "dest_connector_id": pair["dst"].id, "dest_table": dest_table,
        "dest_collection": dest_table, "skip_preflight": "true", "async_mode": "true",
        "mappings_json": json.dumps([{"source": "id", "target": "id", "confidence": 1.0},
                                     {"source": "note", "target": "note", "confidence": 1.0}]),
        "sync_mode": "full_refresh_overwrite", **policy,
    }


def test_studio_run_form_refuses_unknown_and_canonicalizes_known(client, pg_pair):
    before = len(_jobs())
    for field in FIELDS:
        resp = client.post("/api/v1/transfer/run",
                           data=_run_form(pg_pair, f"{pg_pair['table']}_x", **{field: "bogus"}))
        _assert_422_names(resp, field, "bogus")
    assert len(_jobs()) == before, "a refused form queued a job"

    job_ids = []
    for field in FIELDS:
        for i, (sent, canonical) in enumerate(ACCEPTED[field]):
            if not canonical:
                continue  # blank on /run = inherit the plan; covered by the plan test below
            dest = f"{pg_pair['table']}_r{field[0]}{i}"
            pg_pair["created"].append(dest)
            resp = client.post("/api/v1/transfer/run", data=_run_form(pg_pair, dest, **{field: sent}))
            assert resp.status_code == 200, (field, sent, resp.text[:400])
            job_id = str(resp.json().get("job_id") or "")
            job_ids.append(job_id)
            assert _job_request(job_id).get(field) == canonical, (field, sent)
    for job_id in job_ids:
        h.wait_job(job_id)


def test_studio_plan_policies_refuse_unknown(client, pg_pair):
    """Blank on /run inherits the plan's stored policies, so the plan must be checked too."""
    base = {"name": "r12e", "source": {"kind": "database", "format": "postgresql"},
            "destination": {"kind": "database", "format": "postgresql"}}
    for field in FIELDS:
        resp = client.post("/api/v1/transfer/plans", json={**base, "policies": {field: "bogus"}})
        _assert_422_names(resp, field, "bogus")
    resp = client.post("/api/v1/transfer/plans", json={
        **base, "policies": {"validation_mode": " STRICT", "schema_policy": "block"}})
    assert resp.status_code == 200, resp.text[:300]
    plan = resp.json()["plan"]
    policies = plan.get("policies") or {}
    assert policies.get("validation_mode") == "strict", policies
    assert policies.get("schema_policy") == "block_all", policies
    resp = client.patch(f"/api/v1/transfer/plans/{plan['id']}",
                        json={"policies": {"schema_policy": "bogus"}})
    _assert_422_names(resp, "schema_policy", "bogus")
    for field in FIELDS:
        resp = client.post("/api/v1/transfer/map", json={
            "source_columns": ["id"], "use_llm": False, field: "bogus"})
        _assert_422_names(resp, field, "bogus")
        resp = client.post(f"/api/v1/transfer/plans/{plan['id']}/map",
                           json={"validation_mode": "bogus", "use_llm": False})
        _assert_422_names(resp, "validation_mode", "bogus")


# ------------------------------------------------------------------ preflight

def test_preflight_routes_refuse_unknown_and_canonicalize_known(client, pg_pair):
    def body(**policy):
        return {
            "dataset_name": pg_pair["table"], "columns": ["id", "note"],
            "column_types": {"id": "integer", "note": "text"},
            "mappings": [{"source": "id", "target": "id", "confidence": 1.0},
                         {"source": "note", "target": "note", "confidence": 1.0}],
            "source_connector_id": pg_pair["src"].id, "source_kind": "database",
            "source_type": "postgresql", "source_table": pg_pair["table"],
            "connector_id": pg_pair["dst"].id, "dest_kind": "database",
            "dest_type": "postgresql", "dest_table": f"{pg_pair['table']}_pf", **policy,
        }

    for field in FIELDS:
        _assert_422_names(client.post("/api/v1/preflight/run", json=body(**{field: "bogus"})),
                          field, "bogus")
    _assert_422_names(client.post("/api/v1/preflight/schema-drift",
                                  json={"schema_policy": "bogus"}), "schema_policy", "bogus")
    _assert_422_names(client.post("/api/v1/preflight/explain",
                                  json={"validation_mode": "bogus", "use_llm": False}),
                      "validation_mode", "bogus")
    for field in FIELDS:
        for sent, canonical in ACCEPTED[field]:
            resp = client.post("/api/v1/preflight/run", json=body(**{field: sent}))
            assert resp.status_code == 200, (field, sent, resp.text[:300])
            if field == "validation_mode":
                # The run record keeps the mode the request carried.
                from services.preflight_run_store import get_preflight_run

                record = get_preflight_run(resp.json()["run_id"]) or {}
                stamped = str(record.get("validation_mode") or "")
                assert stamped == (canonical or "strict"), (sent, stamped)
            else:
                gate = [g for g in resp.json().get("gates") or []
                        if g.get("id") == "g10_schema_policy"]
                assert not gate or gate[0].get("status") != "block", (sent, gate)


# ------------------------------------------------------------------------ MCP

def _mcp_args(pair, dest_table: str, **policy) -> dict:
    return {"source_connector_id": pair["src"].id, "source_table": pair["table"],
            "dest_connector_id": pair["dst"].id, "dest_table": dest_table,
            "sync_mode": "full_refresh_overwrite", **policy}


def test_mcp_tools_refuse_unknown_and_canonicalize_known(client, pg_pair):
    dest = f"{pg_pair['table']}_m"
    for tool in ("plan_transfer", "start_transfer", "create_schedule"):
        for field in FIELDS:
            ok, payload = h.mcp_call(client, tool, _mcp_args(pg_pair, dest, **{field: "bogus"}))
            _assert_tool_error_names(ok, payload, field)
    from services.schedule_store import list_schedules

    assert not list_schedules(), "a refused create_schedule stored a schedule"

    defaults = {"validation_mode": "balanced", "schema_policy": "manual_review"}
    for field in FIELDS:
        for sent, canonical in ACCEPTED[field]:
            ok, payload = h.mcp_call(client, "plan_transfer",
                                     _mcp_args(pg_pair, dest, **{field: sent}))
            assert ok, (field, sent, payload)
            plan = payload.get("plan") if isinstance(payload.get("plan"), dict) else payload
            assert plan.get(field) == (canonical or defaults[field]), (field, sent, plan.get(field))

    ok, staged = h.mcp_call(client, "start_transfer", _mcp_args(
        pg_pair, dest, validation_mode="Maximum", schema_policy="BLOCK"))
    assert ok, staged
    plan = staged.get("plan") if isinstance(staged.get("plan"), dict) else staged
    assert (plan.get("validation_mode"), plan.get("schema_policy")) == ("maximum", "block_all"), plan

    ok, created = h.mcp_call(client, "create_schedule", {
        **_mcp_args(pg_pair, f"{dest}_s", validation_mode=" Strict ", schema_policy="approve"),
        "cadence": "daily", "name": f"r12e-mcp-{pg_pair['token']}"})
    assert ok, created
    assert created.get("ack_id"), created
    resp = client.post("/api/v1/copilot/confirm", json={"ack_id": created["ack_id"], "reason": "r12"})
    assert resp.status_code == 200 and resp.json().get("ok") is True, resp.text[:400]
    stored = [s for s in list_schedules() if s.name == f"r12e-mcp-{pg_pair['token']}"]
    assert stored, created
    assert (stored[0].validation_mode, stored[0].schema_policy) == ("strict", "manual_review")


# ------------------------------------------------------------------ schedules

def _schedule_body(pair, name: str, **policy) -> dict:
    return {"name": name, "source_connector_id": pair["src"].id, "source_table": pair["table"],
            "dest_connector_id": pair["dst"].id, "dest_table": f"{pair['table']}_sch", **policy}


def test_schedule_create_and_update_refuse_unknown_and_canonicalize_known(client, pg_pair):
    from services.schedule_store import list_schedules

    for field in FIELDS:
        resp = client.post("/api/v1/schedules", json=_schedule_body(pg_pair, "bad", **{field: "bogus"}))
        _assert_422_names(resp, field, "bogus")
    assert not list_schedules()

    defaults = {"validation_mode": "strict", "schema_policy": "manual_review"}
    for field in FIELDS:
        for i, (sent, canonical) in enumerate(ACCEPTED[field]):
            resp = client.post("/api/v1/schedules",
                               json=_schedule_body(pg_pair, f"ok-{field}-{i}", **{field: sent}))
            assert resp.status_code == 201, (field, sent, resp.text[:300])
            assert resp.json()[field] == (canonical or defaults[field]), (field, sent)

    sid = client.post("/api/v1/schedules", json=_schedule_body(
        pg_pair, "patch-me", validation_mode="balanced", schema_policy="type_locked")).json()["id"]
    for field in FIELDS:
        _assert_422_names(client.patch(f"/api/v1/schedules/{sid}", json={field: "bogus"}),
                          field, "bogus")
        # Blank on a PATCH is "no change".
        for blank in ("", "   "):
            resp = client.patch(f"/api/v1/schedules/{sid}", json={field: blank})
            assert resp.status_code == 200, resp.text[:300]
    got = client.get(f"/api/v1/schedules/{sid}").json()
    assert (got["validation_mode"], got["schema_policy"]) == ("balanced", "type_locked"), got
    resp = client.patch(f"/api/v1/schedules/{sid}",
                        json={"validation_mode": "MIGRATION", "schema_policy": "Block"})
    assert resp.status_code == 200, resp.text[:300]
    assert (resp.json()["validation_mode"], resp.json()["schema_policy"]) == ("migration", "block_all")


# --------------------------------------------------------------------- GitOps

def test_gitops_plan_and_apply_refuse_unknown_and_canonicalize_known(client, pg_pair):
    from services.schedule_store import list_schedules

    def manifest(name: str, **policy) -> dict:
        return {"apiVersion": "dataflow.space/v1", "kind": "PipelineSchedule",
                "metadata": {"name": name},
                "spec": _schedule_body(pg_pair, name, **policy)}

    for field in FIELDS:
        for route in ("/api/v1/schedules/gitops/plan", "/api/v1/schedules/gitops/apply"):
            _assert_422_names(client.post(route, json=manifest("bad", **{field: "bogus"})),
                              field, "bogus")
    assert not list_schedules(), "a refused manifest created a schedule"

    resp = client.post("/api/v1/schedules/gitops/apply",
                       json=manifest("gitops-ok", validation_mode="  Lenient", schema_policy="BLOCK"))
    assert resp.status_code == 200, resp.text[:300]
    stored = [s for s in list_schedules() if s.name == "gitops-ok"]
    assert stored, resp.json()
    assert (stored[0].validation_mode, stored[0].schema_policy) == ("lenient", "block_all")
    resp = client.post("/api/v1/schedules/gitops/apply",
                       json=manifest("gitops-blank", validation_mode="", schema_policy="  "))
    assert resp.status_code == 200, resp.text[:300]
    stored = [s for s in list_schedules() if s.name == "gitops-blank"]
    assert (stored[0].validation_mode, stored[0].schema_policy) == ("strict", "manual_review")


# ------------------------------------------------------- legacy upload route

def test_legacy_connector_upload_transfer_refuses_unknown(client):
    before = len(_jobs())
    for field in FIELDS:
        resp = client.post(
            "/api/v1/connectors/transfer",
            data={"destination_database": "db", "destination_collection": "c", field: "bogus"},
            files={"file": ("x.csv", b"id,note\n1,a\n", "text/csv")},
        )
        _assert_422_names(resp, field, "bogus")
    assert len(_jobs()) == before


def test_shared_vocabulary_is_the_engine_canonical_set():
    """The validator lists exactly what the engine honours; no second copy."""
    from services import schema_drift, validation_mode_contract
    from services.schedule_store import SCHEMA_POLICIES as schedule_set
    from services.transfer_policy_values import allowed_values

    assert tuple(allowed_values("validation_mode")) == VALIDATION_MODES
    assert set(VALIDATION_MODES) == validation_mode_contract.VALIDATION_MODES
    assert tuple(allowed_values("schema_policy")) == SCHEMA_POLICIES
    assert set(SCHEMA_POLICIES) == set(schema_drift.SCHEMA_POLICY_HONESTY)
    assert schedule_set is schema_drift.SCHEMA_POLICIES
