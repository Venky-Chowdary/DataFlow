"""Parity Studio API on the real app and a live MongoDB (r8 enterprise harness).

Covers workspace isolation (404 for another workspace's job on every route),
RBAC (viewer reads and signs, cannot approve; editor approves), the 409s
(second approve, not-applicable fix, sign-off while unexplained, a role signing
twice), the 422s, the audit events, the explanation fallback, the signed export
verifying through the existing proof-pack verify route, and the workspace
summary.
"""

from __future__ import annotations

import re

import pytest

from r8_ent_harness import ent  # noqa: F401


def _job(workspace_id: str, name: str, **over) -> str:
    from services.mongodb_service import get_mongodb_service

    doc = {
        "name": name,
        "workspace_id": workspace_id,
        "status": "completed",
        "source": {"type": "postgresql"},
        "destination": {"type": "postgresql"},
        "reconciliation": {
            "passed": False,
            "source_rows": 100,
            "target_rows": 99,
            "assurance_level": "none",
            "sample_compare": {
                "rows_compared": 100,
                "mismatches": [{"key": "5", "source": "amount", "target": "amount",
                                "source_value": "10.1049", "target_value": "10.10"}],
            },
            "culprit_keys": {"missing_in_dest": ["42"]},
        },
        "destination_summary": {
            "table": "payments", "primary_key_columns": ["id"], "sync_mode": "full_refresh_overwrite",
        },
    }
    doc.update(over)
    store = get_mongodb_service()
    job_id = store.create_transfer_job(dict(doc))
    store.update_job_status(job_id, doc["status"])  # creation always starts a job as pending
    return job_id


@pytest.fixture()
def world(ent, monkeypatch):
    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    ws = ent.workspace("Parity")
    other = ent.workspace("Parity elsewhere")
    return {
        "ws": ws,
        "other": other,
        "job": _job(ws, "payments nightly"),
        "foreign": _job(other, "theirs"),
        "editor": ent.member(email="parity-editor@example.com", workspace_id=ws, role="editor"),
        "viewer": ent.member(email="parity-viewer@example.com", workspace_id=ws, role="viewer"),
        "owner": ent.member(email="parity-owner@example.com", workspace_id=ws, role="viewer"),
    }


def _fix(report: dict, action: str, *, status: str = "proposed") -> dict:
    return next(f for f in report["fixes"] if f["action"] == action and f["status"] == status)


def test_report_shape_and_workspace_isolation(ent, world):
    viewer, job, foreign = world["viewer"], world["job"], world["foreign"]
    res = viewer.get(f"/api/v1/jobs/{job}/parity-report")
    assert res.status_code == 200, res.text
    report = res.json()
    assert report["job_id"] == job
    assert report["verdict"] == "unexplained"
    assert report["parity"]["unexplained"] == 2
    assert {d["cause"]["code"] for d in report["differences"]} == {"numeric_scale", "unexplained"}
    assert report["tables"][0]["check"] == "key_diff"
    assert report["signoff"]["status"] == "blocked"
    assert not any(k.startswith("_") for f in report["fixes"] for k in f)
    assert not any(k.startswith("_") for k in report)
    # No saved route: the cells could not be re-read, so the stored evidence stands.
    assert report["evidence"]["reread"]["payments"]["status"] == "failed"

    fix_id = report["fixes"][0]["id"]
    for method, path, body in (
        ("get", f"/api/v1/jobs/{foreign}/parity-report", None),
        ("post", f"/api/v1/jobs/{foreign}/parity-report/explain", None),
        ("post", f"/api/v1/jobs/{foreign}/parity-report/fixes/{fix_id}/approve", {}),
        ("post", f"/api/v1/jobs/{foreign}/parity-report/fixes/{fix_id}/dismiss", {}),
        ("post", f"/api/v1/jobs/{foreign}/parity-report/signoff", {"role": "engineering"}),
        ("get", f"/api/v1/jobs/{foreign}/parity-report/export", None),
        ("get", "/api/v1/jobs/does-not-exist/parity-report", None),
    ):
        client = world["editor"]
        res = getattr(client, method)(path, json=body) if body is not None else getattr(client, method)(path)
        assert res.status_code == 404, (path, res.status_code, res.text)


def test_rbac_conflicts_audit_signature_and_export(ent, world):
    editor, viewer, owner, job, ws = world["editor"], world["viewer"], world["owner"], world["job"], world["ws"]
    report = viewer.get(f"/api/v1/jobs/{job}/parity-report").json()
    accept = _fix(report, "accept")

    assert viewer.post(f"/api/v1/jobs/{job}/parity-report/fixes/{accept['id']}/approve", json={}).status_code == 403
    resync = _fix(report, "resync_keys")
    assert resync["applicable"] is False  # no saved route on this job
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{resync['id']}/approve", json={})
    assert res.status_code == 409, res.text
    assert editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/f_unknown/approve", json={}).status_code == 404

    res = viewer.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "engineering"})
    assert res.status_code == 409 and "unexplained" in res.text
    assert viewer.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "cfo"}).status_code == 422
    assert viewer.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={}).status_code == 422

    version = report["version"]
    for fix in [f for f in report["fixes"] if f["action"] == "accept"]:
        res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{fix['id']}/approve",
                          json={"note": "known source rounding"})
        assert res.status_code == 200, res.text
        assert res.json()["version"] > version
        version = res.json()["version"]
    again = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{accept['id']}/approve", json={})
    assert again.status_code == 409  # idempotent: an applied fix is not applied twice
    report = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert report["parity"]["unexplained"] == 0
    assert report["verdict"] == "explained"
    assert report["signoff"]["status"] == "open"

    assert editor.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "engineering"}).status_code == 200
    dup = viewer.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "engineering"})
    assert dup.status_code == 409
    assert viewer.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "data_owner"}).status_code == 200
    last = owner.post(f"/api/v1/jobs/{job}/parity-report/signoff", json={"role": "compliance", "note": "ok"})
    assert last.status_code == 200, last.text
    signed = last.json()
    assert signed["signoff"]["status"] == "signed"
    assert signed["signoff"]["signature"].startswith("hmac-sha256:")
    assert [a["user"] for a in signed["signoff"]["approvals"]] == [
        "parity-editor@example.com", "parity-viewer@example.com", "parity-owner@example.com",
    ]

    pack = viewer.get(f"/api/v1/jobs/{job}/parity-report/export", params={"format": "json"})
    assert pack.status_code == 200, pack.text
    # The existing verify route (its own permission: connector.write) checks the parity pack.
    verified = editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": pack.json()})
    assert verified.status_code == 200, verified.text
    assert verified.json()["ok"] is True
    forged = {**pack.json(), "parity": {**pack.json()["parity"], "verdict": "proven"}}
    assert editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": forged}).json()["ok"] is False

    md = viewer.get(f"/api/v1/jobs/{job}/parity-report/export")
    assert md.status_code == 200 and md.headers["content-type"].startswith("text/markdown")
    for text in ("**Verdict:** explained", "## Tables", "## Differences", "Accepted",
                 "- compliance: parity-owner@example.com", "hmac-sha256:", "proof-pack/verify"):
        assert text in md.text, text

    proof = ent.admin.get(f"/api/v1/transfer/{job}/proof-pack", headers={"X-Workspace-Id": ws})
    assert proof.status_code == 200, proof.text
    assert proof.json()["parity"]["signature"] == signed["signoff"]["signature"]

    actions = [e.get("action") for e in ent.audit(ws)]
    for name in ("job.parity.fix_approved", "job.parity.fix_applied", "job.parity.signed_off",
                 "job.parity.exported"):
        assert name in actions, name


def test_dismiss_explain_and_summary(ent, world):
    editor, viewer, job, ws = world["editor"], world["viewer"], world["job"], world["ws"]
    report = viewer.get(f"/api/v1/jobs/{job}/parity-report").json()
    accept = _fix(report, "accept")
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{accept['id']}/dismiss",
                      json={"note": "not ours to accept"})
    assert res.status_code == 200, res.text
    dismissed = next(f for f in res.json()["fixes"] if f["id"] == accept["id"])
    assert dismissed["status"] == "dismissed" and dismissed["approved_by"] == "parity-editor@example.com"
    assert editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{accept['id']}/dismiss", json={}).status_code == 409
    assert "job.parity.fix_dismissed" in [e.get("action") for e in ent.audit(ws)]

    explained = viewer.post(f"/api/v1/jobs/{job}/parity-report/explain")
    assert explained.status_code == 200, explained.text
    explanation = explained.json()["explanation"]
    assert explanation["source"] == "template"
    assert "no rule explains" in explanation["text"]
    assert "10.1049" not in explanation["text"]  # metadata only: no row values in the words
    again = viewer.get(f"/api/v1/jobs/{job}/parity-report").json()
    assert again["explanation"]["text"] == explanation["text"]

    _job(ws, "clean run", reconciliation={
        "passed": True, "source_rows": 10, "target_rows": 10,
        "sample_compare": {"rows_compared": 10, "mismatches": []},
    })
    summary = viewer.get("/api/v1/parity/summary", params={"limit": 10})
    assert summary.status_code == 200, summary.text
    body = summary.json()
    assert body["kpis"]["runs"] == 2
    assert body["kpis"]["runs_with_unexplained"] == 1
    assert body["kpis"]["most_drifting_table"] == "payments"
    assert [r["name"] for r in body["runs"]] == ["payments nightly", "clean run"]  # newest last
    # ISO-8601 UTC, not str() of the naive datetime Mongo returns ("2026-11-11 14:00:00.123000").
    assert all(re.fullmatch(r"\d{4}-\d{2}-\d{2}T[\d:.]+Z", r["at"]) for r in body["runs"]), body["runs"]
    assert world["foreign"] not in body["heatmap"]["job_ids"]
    row = body["heatmap"]["cells"][body["heatmap"]["tables"].index("payments")]
    assert row[0] == "unexplained_large"  # 2 of 100 rows is over the 1% "small" line
    assert body["runs"][1]["verdict"] == "no_difference_found"  # a clean sample is not proof
    assert viewer.get("/api/v1/parity/summary", params={"limit": 0}).status_code == 422


def test_rerun_fix_starts_the_existing_rerun(ent, world, monkeypatch):
    import importlib

    connectors_router = importlib.import_module("src.routers.connectors_router")
    started = []

    async def fake_rerun(job_id, background_tasks, request, force=False, body=None):
        started.append((job_id, request.url.path))
        return {"success": True, "job_id": "rerun-child-1"}

    monkeypatch.setattr(connectors_router, "rerun_transfer_job", fake_rerun)
    ws, editor = world["ws"], world["editor"]
    job = _job(ws, "short load", reconciliation={
        "passed": False, "source_rows": 100, "target_rows": 97, "culprit_census_clean": True,
    }, transfer_request={"source": {"kind": "database", "table": "o"},
                         "destination": {"kind": "database", "table": "o"}})
    report = editor.get(f"/api/v1/jobs/{job}/parity-report").json()
    rerun = _fix(report, "rerun")
    assert rerun["applicable"] and rerun["preview"] == f"POST /api/v1/connectors/jobs/{job}/rerun"
    res = editor.post(f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve", json={"note": "go"})
    assert res.status_code == 200, res.text
    done = next(f for f in res.json()["fixes"] if f["id"] == rerun["id"])
    assert done["status"] == "applied" and "rerun-child-1" in done["result"]
    assert started == [(job, f"/api/v1/jobs/{job}/parity-report/fixes/{rerun['id']}/approve")]
    assert res.json()["rounds"] == []  # a re-run is checked by its own report, not a round here
