"""QA C10: a schedule whose mappings every run would refuse is refused when saved.

QA: a schedule with low-confidence mappings and no ``user_override`` was
created (201) and only failed at run time. The save now judges the stored rows
with the owners every run meets — G4's floor and clearances
(``preflight.gates.mapping_confidence_layers``) and Execute's review gate
(``mapping_pipeline.assert_mappings_executable``) — and answers 422 naming the
mappings and the override route. Live PostgreSQL; REST create, PATCH and
GitOps import.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


_LOW = [{"source": "id", "target": "id", "confidence": 0.3},
        {"source": "name", "target": "name", "confidence": 0.2}]
_NO_CONFIDENCE = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]
_REVIEW = [{"source": "id", "target": "id", "confidence": 0.99},
           {"source": "name", "target": "name", "confidence": 0.9, "requires_review": True}]


def _refusal(resp, *names: str) -> dict:
    assert resp.status_code == 422, resp.text
    err = resp.json()["error"]
    assert err["code"] == "SCHEDULE_MAPPING_REVIEW_REQUIRED", err
    assert "user_override" in err["message"] and "user_override" in err["override"], err
    for name in names:
        assert name in err["message"], (name, err["message"])
    return err


def test_save_refuses_what_every_run_would_refuse_and_override_runs(client):
    from services.connector_store import create_connector

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"c10_src_{token}", f"c10_dst_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        cur.executemany(f"INSERT INTO {src_table} VALUES (%s, %s)",  # nosec B608
                        [(1, "Ada"), (2, None), (3, ""), (4, "   "), (5, "Zoë ✓")])
        src = create_connector({"name": f"c10-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"c10-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        base = {"source_connector_id": src.id, "source_table": src_table,
                "dest_connector_id": dst.id, "dest_table": dest_table, "interval": "hourly",
                "sync_mode": "full_refresh_overwrite", "enabled": True}

        err = _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"low {token}",
                                                               "mappings": _LOW}),
                       "id→id (0.30)", "name→name (0.20)")
        assert set(err["mappings"]) == {"id→id (0.30)", "name→name (0.20)"}, err
        _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"none {token}",
                                                         "mappings": _NO_CONFIDENCE}), "id→id")
        _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"rev {token}",
                                                         "mappings": _REVIEW}), "name")
        # A draft is refused too: enabling it could only fail.
        _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"draft {token}",
                                                         "enabled": False, "mappings": _LOW}))
        from services.schedule_store import list_schedules

        assert not [s for s in list_schedules() if s.dest_table == dest_table]

        # The override route the refusal names: reviewed rows save and run.
        reviewed = [{**m, "user_override": True} for m in _LOW]
        resp = client.post("/api/v1/schedules/", json={**base, "name": f"ok {token}",
                                                       "mappings": reviewed})
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        run = _beat(sid)
        assert run["job"].get("status") == "completed", run["job"].get("error")

        # A PATCH back to unreviewed rows is refused and changes nothing.
        _refusal(client.patch(f"/api/v1/schedules/{sid}", json={"mappings": _LOW}))
        assert client.get(f"/api/v1/schedules/{sid}").json()["mappings"][0].get("user_override")
        # Raising validation_mode re-judges the stored rows against the new floor.
        ok_balanced = [{"source": c, "target": c, "confidence": 0.8} for c in ("id", "name")]
        assert client.patch(f"/api/v1/schedules/{sid}", json={
            "mappings": ok_balanced, "validation_mode": "balanced"}).status_code == 200
        _refusal(client.patch(f"/api/v1/schedules/{sid}", json={"validation_mode": "strict"}))

        # GitOps import goes through the same check.
        from services.brand import GITOPS_API_VERSION

        imported = client.post("/api/v1/schedules/import", json={
            "apiVersion": GITOPS_API_VERSION, "kind": "PipelineSchedule",
            "metadata": {"name": f"gitops {token}"},
            "spec": {**base, "name": f"gitops {token}", "dest_table": f"{dest_table}_g",
                     "mappings": _LOW},
        })
        assert imported.status_code in (400, 422), imported.text
        assert "user_override" in imported.text, imported.text
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest_table)


def test_findings_agree_with_the_g4_gate_on_the_same_rows():
    """One owner: the save-time check and Validate's G4 read the same layers."""
    from preflight.gates import mapping_confidence_layers

    from services.schedule_mapping_contract import schedule_mapping_findings

    assert schedule_mapping_findings([{"source": "a", "target": "a", "confidence": 0.9}], "strict") == []
    found = schedule_mapping_findings(_LOW, "strict")
    assert [f["layer"] for f in found] == ["low_confidence"]
    assert found[0]["message"] == mapping_confidence_layers(_LOW, 0.85)[0][1]
    # balanced floor is 0.75: 0.8 passes there and fails strict.
    rows = [{"source": "a", "target": "a", "confidence": 0.8}]
    assert schedule_mapping_findings(rows, "balanced") == []
    assert schedule_mapping_findings(rows, "strict")
    # Intentional omissions are not judged.
    assert schedule_mapping_findings(
        [{"source": "x", "target": "", "intentional_omit": True, "confidence": 0.0},
         {"source": "a", "target": "a", "confidence": 0.99}], "strict") == []


def test_a_create_that_persists_onto_an_existing_draft_is_judged_too(client):
    """Review of 52de05a44: the Studio replay branch (an existing draft on the
    same route, or an explicit replay_schedule_id) returned before the check."""
    from services.connector_store import create_connector
    from services.schedule_store import get_schedule

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"c10r_src_{token}", f"c10r_dst_{token}"
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, name TEXT)")  # nosec B608
        src = create_connector({"name": f"c10r-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"c10r-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        base = {"source_connector_id": src.id, "source_table": src_table,
                "dest_connector_id": dst.id, "dest_table": dest_table, "interval": "hourly",
                "sync_mode": "full_refresh_overwrite"}
        draft = client.post("/api/v1/schedules/", json={**base, "name": f"draft {token}",
                                                        "mappings": [], "enabled": False})
        assert draft.status_code == 201, draft.text
        draft_id = draft.json()["id"]

        _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"adopt {token}",
                                                         "mappings": _LOW, "enabled": True}),
                 "id→id (0.30)")
        _refusal(client.post("/api/v1/schedules/", json={**base, "name": f"replay {token}",
                                                         "mappings": _LOW, "enabled": True,
                                                         "replay_schedule_id": draft_id}),
                 "id→id (0.30)")
        stored = get_schedule(draft_id)
        assert stored is not None and not stored.mappings and not stored.enabled, stored

        reviewed = [{**m, "user_override": True} for m in _LOW]
        ok = client.post("/api/v1/schedules/", json={**base, "name": f"replay ok {token}",
                                                     "mappings": reviewed, "enabled": True,
                                                     "replay_schedule_id": draft_id})
        assert ok.status_code == 201 and ok.json()["id"] == draft_id, ok.text
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608


def test_an_unsigned_continue_policy_contract_saves_and_runs_as_execute_reads_it(client):
    """Review of 52de05a44: the save judged raw rows while Validate/Execute sign
    Map drafts first, so a lossy cast at 0.95 with an unsigned CAST_AND_CONTINUE
    contract was a 422 at save yet a clean run."""
    from services.connector_store import create_connector
    from services.schedule_mapping_contract import schedule_mapping_findings

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    src_table, dest_table = f"c10h_src_{token}", f"c10h_dst_{token}"
    draft = {"source_column": "flag", "target_column": "flag", "source_type": "TEXT",
             "destination_type": "INTEGER", "execution_policy": "CAST_AND_CONTINUE",
             "approved_by": "qa@example.com", "reason": "Cast TEXT flags; quarantine non-numeric",
             "expected_precision_loss": True, "quarantine_policy": "QUARANTINE_ROW_on_cast_failure",
             "retry_policy": "none", "rollback_strategy": "DOCUMENT_ONLY"}
    rows = [{"source": "id", "target": "id", "confidence": 0.99},
            {"source": "flag", "target": "flag", "confidence": 0.95, "source_type": "TEXT",
             "target_type": "INTEGER", "fidelity": "lossy_cast", "type_narrowing": True,
             "risk_contract": dict(draft)}]
    assert schedule_mapping_findings(rows, "strict") == []
    try:
        cur.execute(f"CREATE TABLE {src_table} (id BIGINT PRIMARY KEY, flag TEXT)")  # nosec B608
        cur.executemany(f"INSERT INTO {src_table} VALUES (%s, %s)",  # nosec B608
                        [(1, "0"), (2, "1"), (3, "1")])
        src = create_connector({"name": f"c10h-src-{token}", "type": "postgresql", "role": "source",
                                "workspace_id": "", "ssl": False, **h.PG, "schema": "public"})
        dst = create_connector({"name": f"c10h-dst-{token}", "role": "destination",
                                "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
        resp = client.post("/api/v1/schedules/", json={
            "name": f"c10h {token}", "source_connector_id": src.id, "source_table": src_table,
            "dest_connector_id": dst.id, "dest_table": dest_table, "interval": "hourly",
            "sync_mode": "full_refresh_overwrite", "mappings": rows, "enabled": True})
        assert resp.status_code == 201, resp.text
        run = _beat(resp.json()["id"])
        assert run["job"].get("status") in ("completed", "completed_with_quarantine"), run["job"].get("error")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src_table}")  # nosec B608
        h.drop_table("postgresql", dest_table)


def test_the_save_floor_is_g4s_floor():
    from preflight.gates import g4_confidence_floor

    from services.preflight_service import confidence_threshold_for_mode

    for mode in ("strict", "balanced", "maximum", "lenient"):
        floor = g4_confidence_floor(confidence_threshold_for_mode(mode))
        from services.schedule_mapping_contract import schedule_mapping_findings

        just_below = [{"source": "a", "target": "a", "confidence": round(floor - 0.01, 2)}]
        at_floor = [{"source": "a", "target": "a", "confidence": floor}]
        assert schedule_mapping_findings(just_below, mode), mode
        assert schedule_mapping_findings(at_floor, mode) == [], mode
