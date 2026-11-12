"""Exit rehearsal PII review (web integration gap 1): the transfer PII gate, acknowledged per system.

A rehearsal of a table the PII gate holds is never let through silently: it
reads ``needs_pii_review`` with the action, a person acknowledges PII for the
system (who / when / why, audited, the transfer acknowledgement contract), and
the next rehearsal's extract jobs pass the gate the way an acknowledged
transfer does. The acknowledgement is in the rehearsal and its signed pack,
and it lapses when the system's definition changes.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import os
import sqlite3

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from r8_ent_harness import ent  # noqa: E402,F401
from tests.test_exit_rehearsal_e2e import _run, _system, world  # noqa: E402,F401

_SSN = "123-45-6001"


@pytest.fixture()
def people(world):  # noqa: F811
    conn = sqlite3.connect(str(world["src"]))
    with conn:
        conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, name TEXT, ssn TEXT, email TEXT)")
        conn.executemany("INSERT INTO people VALUES (?,?,?,?)",
                         [(i, f"Person {i}", f"123-45-{6000 + i}", f"p{i}@example.com") for i in range(1, 30)])
    conn.close()
    return world


@pytest.fixture()
def audit_events(monkeypatch):
    from services import audit_log

    events: list[dict] = []
    real = audit_log.append_audit_event

    def spy(**kw):
        events.append(kw)
        return real(**kw)

    monkeypatch.setattr(audit_log, "append_audit_event", spy)
    return events


def _ack(system_id, actor="dpo@example.com", reason="DPA clause 9 allows the exit extract"):
    from services.exit_pii import acknowledge

    return acknowledge(system_id, actor=actor, reason=reason, expected_version=None)


def test_gap1_a_table_the_pii_gate_holds_reads_needs_pii_review_with_the_action(people):
    from services.exit_inventory import board, get_system, readiness

    system = _system(name="People", tables=[{"name": "people"}])
    out = _run(system)
    table = out["tables"][0]
    assert out["verdict"] == "needs_pii_review", out["verdict_reason"]
    assert table["status"] == "needs_pii_review"
    assert "ssn" in table["reason"] and "email" in table["reason"] and "acknowledge" in table["reason"].lower()
    assert _SSN not in str(out) and "p1@example.com" not in str(out)
    assert out["completeness"]["tables_needs_pii_review"] == 1
    assert out["duration"]["fits_window"] is None and out["signoff"]["status"] == "blocked"
    ready = readiness(get_system(system["id"]))
    assert ready["state"] == "needs_pii_review"
    row = board([get_system(system["id"])])["systems"][0]
    assert row["state"] == "needs_pii_review" and row["completeness_label"] == "Needs PII review"


def test_gap1_an_acknowledged_system_s_extracts_pass_the_gate_and_the_pack_carries_it(people, audit_events):
    from services.exit_evidence import render_markdown
    from services.exit_inventory import get_system, public_system
    from services.exit_store import REHEARSALS, get
    from services.parity_state import job_store

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"
    acked = _ack(system["id"])
    view = public_system(acked)["pii_acknowledgement"]
    assert view["actor"] == "dpo@example.com" and view["current"] is True and view["at"]
    actions = [e["action"] for e in audit_events]
    assert "preflight.acknowledge_compliance" in actions and "exit.system.pii_acknowledged" in actions

    out = _run(get_system(system["id"]))
    assert out["verdict"] == "proven", (out["verdict_reason"], out["tables"][0]["reason"])
    assert out["pii_acknowledgement"]["actor"] == "dpo@example.com"
    job = job_store().get_job(out["tables"][0]["job_id"])
    req = job["transfer_request"]
    assert req["compliance_acknowledged"] is True and req["acknowledgment_actor"] == "dpo@example.com"
    assert "DPA clause 9" in req["acknowledgment_reason"]
    assert "PII acknowledgement: dpo@example.com" in render_markdown(get(REHEARSALS, out["id"]))
    # The masked control totals still never show a value.
    assert _SSN not in str(out)


def test_gap1_the_acknowledgement_needs_a_named_person_and_a_reason(people):
    from services.exit_inventory import ExitForbidden, ExitInvalid

    system = _system(name="People", tables=[{"name": "people"}])
    with pytest.raises(ExitInvalid):
        _ack(system["id"], reason="ok")
    with pytest.raises(ExitForbidden):
        _ack(system["id"], actor="anonymous")


def test_gap1_a_changed_definition_needs_a_new_acknowledgement_and_it_can_be_withdrawn(people):
    from services.exit_inventory import get_system, public_system, update
    from services.exit_pii import withdraw

    system = _system(name="People", tables=[{"name": "people"}])
    assert _run(system)["verdict"] == "needs_pii_review"  # the hold the acknowledgement is of (review 3, M2)
    _ack(system["id"])
    update(system["id"], {"tables": [{"name": "people"}, {"name": "events"}]}, actor="owner@example.com",
           expected_version=None)
    assert public_system(get_system(system["id"]))["pii_acknowledgement"]["current"] is False
    assert _run(get_system(system["id"]))["verdict"] == "needs_pii_review"
    _ack(system["id"])
    after = withdraw(system["id"], actor="dpo@example.com")
    assert public_system(after)["pii_acknowledgement"] is None


def test_gap1_the_acknowledgement_routes_need_job_manage_and_workspace_write(ent):  # noqa: F811
    from services.embedded_volume import volume_workspace_dir

    ws, other = ent.workspace("Exit PII"), ent.workspace("Exit PII elsewhere")
    # The workspace's own area of the SQLite volume (a flat file has no provable owner).
    area = volume_workspace_dir(ent.tmp / "sqlite", ws)
    area.mkdir(parents=True, exist_ok=True)
    db = area / "pii.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE people (id INTEGER PRIMARY KEY, ssn TEXT)")
    conn.execute("INSERT INTO people VALUES (1, '078-05-1120')")
    conn.commit()
    conn.close()
    editor = ent.member(email="exit-pii-editor@example.com", workspace_id=ws, role="editor")
    viewer = ent.member(email="exit-pii-viewer@example.com", workspace_id=ws, role="viewer")
    stranger = ent.member(email="exit-pii-stranger@example.com", workspace_id=other, role="admin")
    connector = editor.post("/api/v1/connectors/saved", json={"name": "people", "type": "sqlite", "role": "source",
                                                               "database": str(db)}).json()["id"]
    sid = editor.post("/api/v1/exit/systems", json={"name": "PII", "source": {"connector_id": connector},
                                                     "tables": [{"name": "people"}], "exit_window_hours": 24}).json()["id"]
    path = f"/api/v1/exit/systems/{sid}/pii-acknowledgement"
    body = {"reason": "DPA clause 9 allows the exit extract"}
    assert viewer.post(path, json=body).status_code == 403
    assert stranger.post(path, json=body).status_code == 404
    assert editor.post(path, json={"reason": "ok"}).status_code == 422
    # Review 3, M2: nothing is held yet, so there is nothing to acknowledge (never a blank cheque).
    early = editor.post(path, json=body)
    assert early.status_code == 422 and "nothing to acknowledge" in early.json()["detail"]
    started = editor.post(f"/api/v1/exit/systems/{sid}/rehearsals").json()
    from services import exit_rehearsal

    assert exit_rehearsal.join_background(120)
    held = editor.get(f"/api/v1/exit/rehearsals/{started['id']}").json()
    assert held["verdict"] == "needs_pii_review", held["verdict_reason"]
    assert held["tables"][0]["pii_held"] and held["tables"][0]["pii_held"][0]["column"] == "ssn"
    hold = editor.get(f"/api/v1/exit/systems/{sid}").json()["pii_hold"]
    assert hold["rehearsal_id"] == started["id"] and hold["tables"][0]["table"] == "people"
    res = editor.post(path, json=body)
    assert res.status_code == 200, res.text
    ack = res.json()["pii_acknowledgement"]
    assert ack["actor"] == "exit-pii-editor@example.com" and ack["current"] is True
    assert [c["column"] for c in ack["covers"][0]["columns"]] == ["ssn"] and ack["covers"][0]["table"] == "people"
    assert editor.delete(path).json()["pii_acknowledgement"] is None
    actions = {e["action"] for e in ent.audit(ws)}
    assert {"exit.system.pii_acknowledged", "exit.system.pii_acknowledgement_withdrawn"} <= actions
