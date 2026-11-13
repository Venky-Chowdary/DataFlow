"""Exit inventory: declare systems, readiness, overdue, the board, and the REST surface.

Unit tests run on the file store; the API tests run the real app on a live
MongoDB (r8 enterprise harness): RBAC (viewer reads, cannot declare), workspace
isolation (404 for another workspace's system on every route), 422 / 409, and
one audit event per write.
"""

from __future__ import annotations

from tests.exit_background_guard import _no_exit_background_work_outlives_a_test  # noqa: F401

import sqlite3
from datetime import datetime, timedelta, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401


@pytest.fixture()
def file_store(tmp_path, monkeypatch):
    from services import exit_store

    monkeypatch.setattr(exit_store, "STORE_PATH", tmp_path / "exit_inventory.json")
    monkeypatch.setattr(exit_store, "_db", lambda: None)
    return exit_store


def _system(**over):
    base = {
        "id": "exs_1", "workspace_id": "ws", "name": "Core ledger", "criticality": "critical",
        "source": {"connector_id": "c1", "connector_name": "ledger", "engine": "postgresql"},
        "tables": [{"name": "orders", "schema": None, "key_columns": []}],
        "exit_window_hours": 720, "rehearsal_every_days": 90, "regulatory_dates": [],
        "created_at": "2026-10-01T00:00:00Z",
    }
    base.update(over)
    return base


def _rehearsal(n, verdict, started, measured=None, **over):
    from services.exit_inventory import definition_sha256

    doc = {
        "id": f"exr_{n}", "system_id": "exs_1", "workspace_id": "ws", "n": n, "verdict": verdict,
        "started_at": started, "finished_at": started if verdict != "running" else None,
        "completeness": {"tables_declared": 1, "tables_proven": 1 if verdict == "proven" else 0},
        "duration": {"measured_seconds": measured, "window_seconds": 720 * 3600,
                     "fits_window": None if measured is None else measured <= 720 * 3600},
        "seal": {"total_bytes": 100}, "signoff": {"status": "open"},
        "definition_sha256": definition_sha256(_system()),
    }
    doc.update(over)
    return doc


NOW = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


def test_a_never_rehearsed_system_is_overdue_after_its_period():
    from services.exit_inventory import readiness

    fresh = readiness(_system(created_at="2026-11-01T00:00:00Z"), [], now=NOW)
    assert fresh == {"state": "never_rehearsed", "overdue": False, "next_due_at": "2027-01-30T00:00:00Z",
                     "running_rehearsal_id": None, "stale": False, "stale_reason": None, "last_rehearsal": None,
                     "last_proven_rehearsal": None}
    stale = readiness(_system(created_at="2026-01-01T00:00:00Z"), [], now=NOW)
    assert stale["overdue"] is True


def test_readiness_follows_the_latest_rehearsal_and_keeps_the_last_proven_one():
    from services.exit_inventory import readiness

    rows = [_rehearsal(3, "incomplete", "2026-11-10T00:00:00Z", 100),
            _rehearsal(2, "proven", "2026-08-01T00:00:00Z", 90)]
    out = readiness(_system(), rows, now=NOW)
    assert out["state"] == "incomplete" and out["overdue"] is False
    assert out["last_rehearsal"]["id"] == "exr_3"
    assert out["last_proven_rehearsal"]["id"] == "exr_2"
    running = readiness(_system(), [_rehearsal(4, "running", "2026-11-11T00:00:00Z"), *rows], now=NOW)
    assert running["state"] == "running" and running["last_rehearsal"]["id"] == "exr_3"
    overdue = readiness(_system(), [_rehearsal(2, "proven", "2026-07-01T00:00:00Z", 90)], now=NOW)
    assert overdue["state"] == "proven" and overdue["overdue"] is True


def test_board_kpis_are_measured_only(file_store):
    from services import exit_store
    from services.exit_inventory import board

    a = _system(regulatory_dates=[{"date": "2027-01-12", "label": "EU Data Act: switching charges end"},
                                  {"date": "2026-01-01", "label": "past"}])
    b = _system(id="exs_2", name="CRM", created_at="2026-01-01T00:00:00Z")
    for doc in (_rehearsal(1, "proven", "2026-11-01T00:00:00Z", 219600),):
        exit_store.insert(exit_store.REHEARSALS, doc)
    out = board([a, b], now=NOW)
    kpis = out["kpis"]
    assert kpis["systems"] == 2 and kpis["systems_proven"] == 1 and kpis["overdue"] == 1
    assert kpis["longest_measured_seconds"] == 219600 and kpis["longest_measured_system_id"] == "exs_1"
    assert kpis["next_regulatory_date"] == {"date": "2027-01-12", "label": "EU Data Act: switching charges end",
                                            "system_id": "exs_1"}
    rows = {r["id"]: r for r in out["systems"]}
    assert rows["exs_1"]["completeness_label"] == "Proven, 1 table"
    assert rows["exs_2"]["completeness_label"] == "Never rehearsed"
    assert rows["exs_2"]["measured_seconds"] is None  # never an estimate


@pytest.mark.parametrize("body,needle", [
    ({"name": "", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 1}, "name"),
    ({"name": "x", "source": {}, "tables": ["t"], "exit_window_hours": 1}, "connector_id"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": [], "exit_window_hours": 1}, "at least one"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t", "T"], "exit_window_hours": 1}, "twice"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 0}, "between"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 5,
      "criticality": "vital"}, "criticality"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 5,
      "rehearsal_schedule": {"cron": "99 * * * *"}}, "rehearsal_schedule"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 5,
      "required_roles": ["auditor"]}, "auditor"),
    ({"name": "x", "source": {"connector_id": "c"}, "tables": ["t"], "exit_window_hours": 5,
      "regulatory_dates": [{"date": "12/01/2027", "label": "x"}]}, "YYYY-MM-DD"),
])
def test_bad_input_is_refused_with_the_reason(body, needle):
    from services.exit_inventory import ExitInvalid, validate

    with pytest.raises(ExitInvalid) as err:
        validate(body)
    assert needle in str(err.value)


def test_store_update_is_compare_and_swap(file_store):
    from services import exit_store

    exit_store.insert(exit_store.SYSTEMS, _system())
    doc, _ = exit_store.update(exit_store.SYSTEMS, "exs_1", lambda d: d.update(name="A"), expected_version=1)
    assert doc["version"] == 2 and exit_store.get(exit_store.SYSTEMS, "exs_1")["name"] == "A"
    with pytest.raises(exit_store.StoreConflict):
        exit_store.update(exit_store.SYSTEMS, "exs_1", lambda d: d.update(name="B"), expected_version=1)
    assert exit_store.get(exit_store.SYSTEMS, "exs_1")["name"] == "A"


# ----------------------------------------------------------------- REST (live Mongo)


@pytest.fixture()
def world(ent, tmp_path):
    from services.embedded_volume import volume_workspace_dir

    ws, other = ent.workspace("Exit"), ent.workspace("Exit elsewhere")
    # The workspace's own area of the SQLite volume (a flat file has no provable owner).
    area = volume_workspace_dir(tmp_path / "sqlite", ws)
    area.mkdir(parents=True)
    db = area / "ledger.sqlite"
    conn = sqlite3.connect(str(db))
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, amount NUMERIC)")
    conn.commit()
    conn.close()
    editor = ent.member(email="exit-editor@example.com", workspace_id=ws, role="editor")
    res = editor.post("/api/v1/connectors/saved", json={"name": "ledger", "type": "sqlite", "role": "source",
                                                         "database": str(db)})
    assert res.status_code == 200, res.text
    return {
        "ws": ws, "editor": editor, "connector": res.json()["id"], "db": db,
        "viewer": ent.member(email="exit-viewer@example.com", workspace_id=ws, role="viewer"),
        "stranger": ent.member(email="exit-stranger@example.com", workspace_id=other, role="admin"),
    }


def _declare(client, connector, **over):
    body = {"name": "Core ledger", "criticality": "critical", "source": {"connector_id": connector},
            "tables": [{"name": "orders", "key_columns": ["id"]}], "exit_window_hours": 720,
            "window_basis": "EU Data Act switching window",
            "regulatory_dates": [{"date": "2027-01-12", "label": "EU Data Act: switching charges end"}]}
    body.update(over)
    return client.post("/api/v1/exit/systems", json=body)


def test_declare_read_update_delete_with_rbac_isolation_and_audit(ent, world):
    editor, viewer, stranger = world["editor"], world["viewer"], world["stranger"]
    assert _declare(viewer, world["connector"]).status_code == 403
    res = _declare(editor, world["connector"])
    assert res.status_code == 201, res.text
    system = res.json()
    sid = system["id"]
    assert system["workspace_id"] == world["ws"] and system["version"] == 1
    assert system["source"] == {"connector_id": world["connector"], "connector_name": "ledger", "engine": "sqlite"}
    assert system["required_roles"] == ["engineering", "data_owner", "compliance"]
    assert system["readiness"]["state"] == "never_rehearsed"

    assert viewer.get(f"/api/v1/exit/systems/{sid}").status_code == 200
    assert [s["id"] for s in viewer.get("/api/v1/exit/systems").json()["systems"]] == [sid]
    board = viewer.get("/api/v1/exit/readiness").json()
    assert board["kpis"]["systems"] == 1 and board["kpis"]["next_regulatory_date"]["date"] == "2027-01-12"
    for method, path in (("get", f"/api/v1/exit/systems/{sid}"), ("patch", f"/api/v1/exit/systems/{sid}"),
                         ("delete", f"/api/v1/exit/systems/{sid}")):
        kwargs = {"json": {"name": "x"}} if method == "patch" else {}
        assert getattr(stranger, method)(path, **kwargs).status_code == 404, (method, path)
    assert stranger.get("/api/v1/exit/readiness").json()["kpis"]["systems"] == 0
    assert viewer.patch(f"/api/v1/exit/systems/{sid}", json={"name": "x"}).status_code == 403

    bad = editor.patch(f"/api/v1/exit/systems/{sid}", json={"exit_window_hours": 0})
    assert bad.status_code == 422
    assert editor.patch(f"/api/v1/exit/systems/{sid}", json={"nonsense": 1}).status_code == 422
    ok = editor.patch(f"/api/v1/exit/systems/{sid}", json={"exit_window_hours": 48, "expected_version": 1})
    assert ok.status_code == 200 and ok.json()["exit_window_hours"] == 48 and ok.json()["version"] == 2
    stale = editor.patch(f"/api/v1/exit/systems/{sid}", json={"name": "y", "expected_version": 1})
    assert stale.status_code == 409

    assert editor.delete(f"/api/v1/exit/systems/{sid}").status_code == 204
    assert viewer.get(f"/api/v1/exit/systems/{sid}").status_code == 404
    actions = [e["action"] for e in ent.audit(world["ws"]) if str(e.get("action", "")).startswith("exit.")]
    assert {"exit.system.declared", "exit.system.updated", "exit.system.deleted"} <= set(actions)


def test_a_connection_the_caller_cannot_see_is_refused(ent, world):
    res = _declare(world["stranger"], world["connector"])
    assert res.status_code == 404
    missing = _declare(world["editor"], "no-such-connector")
    assert missing.status_code == 422


def test_rehearsal_due_dates_track_the_period(file_store):
    from services.exit_inventory import readiness

    started = (NOW - timedelta(days=10)).isoformat()
    out = readiness(_system(rehearsal_every_days=7), [_rehearsal(1, "proven", started, 5)], now=NOW)
    assert out["overdue"] is True


def test_rehearsal_signoff_and_evidence_over_rest(ent, world, monkeypatch, tmp_path):
    import time

    monkeypatch.setenv("DATAWRAP_EXPORTS_DIR", str(tmp_path / "exports"))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    conn = sqlite3.connect(str(world["db"]))
    with conn:
        conn.executemany("INSERT INTO orders VALUES (?, ?)", [(i, i * 2) for i in range(1, 26)])
    conn.close()
    editor, viewer, stranger = world["editor"], world["viewer"], world["stranger"]
    sid = _declare(editor, world["connector"]).json()["id"]
    assert viewer.post(f"/api/v1/exit/systems/{sid}/rehearsals").status_code == 403
    assert stranger.post(f"/api/v1/exit/systems/{sid}/rehearsals").status_code == 404
    started = editor.post(f"/api/v1/exit/systems/{sid}/rehearsals")
    assert started.status_code == 202, started.text
    rid = started.json()["id"]
    assert editor.post(f"/api/v1/exit/systems/{sid}/rehearsals").status_code in {202, 409}
    deadline = time.monotonic() + 120
    body = started.json()
    while body["verdict"] == "running" and time.monotonic() < deadline:
        time.sleep(0.5)
        body = viewer.get(f"/api/v1/exit/rehearsals/{rid}").json()
    assert body["verdict"] == "proven", (body["verdict_reason"], [t["reason"] for t in body["tables"]])
    assert stranger.get(f"/api/v1/exit/rehearsals/{rid}").status_code == 404
    listed = viewer.get(f"/api/v1/exit/systems/{sid}/rehearsals").json()
    assert listed["rehearsals"][0]["id"] == rid and listed["trend"]["points"][-1]["rehearsal_id"] == rid
    assert viewer.get(f"/api/v1/exit/rehearsals/{rid}/evidence").status_code == 409

    ws = world["ws"]
    signers = [
        ("engineering", ent.member(email="exit-eng@example.com", workspace_id=ws, role="editor")),
        ("data_owner", editor),
        ("compliance", ent.member(email="exit-admin@example.com", workspace_id=ws, role="admin")),
    ]
    assert viewer.post(f"/api/v1/exit/rehearsals/{rid}/signoff", json={"role": "engineering"}).status_code == 403
    assert editor.post(f"/api/v1/exit/rehearsals/{rid}/signoff", json={"role": "nobody"}).status_code == 422
    assert signers[0][1].post(f"/api/v1/exit/rehearsals/{rid}/signoff",
                              json={"role": "compliance"}).status_code == 403
    for role, client in signers:
        res = client.post(f"/api/v1/exit/rehearsals/{rid}/signoff", json={"role": role, "note": "ok"})
        assert res.status_code == 200, (role, res.text)
    signed = res.json()
    assert signed["signoff"]["status"] == "signed" and signed["evidence"]["signed"] is True
    assert editor.post(f"/api/v1/exit/rehearsals/{rid}/signoff", json={"role": "data_owner"}).status_code == 409

    pack = viewer.get(f"/api/v1/exit/rehearsals/{rid}/evidence").json()
    assert pack["kind"] == "exit_evidence" and pack["rehearsal_id"] == rid
    checked = viewer.post("/api/v1/exit/evidence/verify", json={"pack": pack}).json()
    assert checked["ok"] is True and checked["anchor"]["status"] == "not_anchored"
    shared = editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": pack}).json()
    assert shared["ok"] is True and shared["kind"] == "exit_evidence"
    markdown = viewer.get(f"/api/v1/exit/rehearsals/{rid}/evidence?format=markdown")
    assert markdown.status_code == 200 and "Verdict: **proven**" in markdown.text
    actions = {e["action"] for e in ent.audit(ws)}
    assert {"exit.rehearsal.started", "exit.rehearsal.completed", "exit.rehearsal.signed_off",
            "exit.rehearsal.signoff_refused", "exit.rehearsal.signed", "exit.evidence.exported"} <= actions
