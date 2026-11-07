"""QA D222: another workspace's resource answers 404, exactly like a missing one.

Seven id-addressed routes answered 403 for a resource in a workspace the caller
cannot read and 404 for an id that does not exist, so the status code told a
stranger which ids are real: the three job-quarantine routes, the three
notification-channel routes and the job-scoped CDC snapshot cancel. They now
go through the shared ownership check (``services.workspace_access``), which
404s anything the caller's tenant scope does not cover.

Through the real REST routes on the live MongoDB (job store, CDC signals):
Bravo, a fresh self-serve signup and admin of its own workspace, probes Acme's
resources and a made-up id of the same kind; the two answers must be the same
404. Acme's owner still reaches every one of them. A viewer in Acme — who can
see the resource — still gets 403 on a write, because that refusal reveals
nothing they could not already read.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401

MISSING_JOB = "0123456789abcdef01234567"


def _cdc_job(workspace_id: str, host: str) -> str:
    from services.mongodb_service import get_mongodb_service

    return get_mongodb_service().create_transfer_job({
        "status": "running",
        "workspace_id": workspace_id,
        "sync_mode": "cdc",
        "name": f"cdc-{host}",
        "transfer_request": {
            "source": {
                "kind": "database", "format": "postgresql", "host": host, "port": 5432,
                "database": "app", "table": "orders",
            },
            "destination": {
                "kind": "database", "format": "postgresql", "host": f"{host}-dw", "port": 5432,
                "database": "dw", "table": "orders",
            },
            "sync_mode": "cdc",
        },
    })


def _same_404(foreign, missing, detail: str) -> None:
    assert foreign.status_code == 404, foreign.text
    assert missing.status_code == 404, missing.text
    assert foreign.json() == missing.json() == {"detail": detail}


def test_quarantine_routes_404_another_workspaces_job(r9):
    acme_job = _cdc_job(r9.acme, "acme-db")
    bravo = r9.bravo()
    for method, suffix, body in (
        ("GET", "quarantine", None),
        ("POST", "quarantine/export", {}),
        ("POST", "quarantine/replay", {}),
    ):
        foreign = bravo.request(method, f"/api/v1/connectors/jobs/{acme_job}/{suffix}", json=body)
        missing = bravo.request(method, f"/api/v1/connectors/jobs/{MISSING_JOB}/{suffix}", json=body)
        _same_404(foreign, missing, "Job not found")

    # The owner still reads it, and a viewer of Acme (who can see the job) is
    # refused the write with 403 — that refusal leaks nothing.
    assert r9.owner.get(f"/api/v1/connectors/jobs/{acme_job}/quarantine").status_code == 200
    r9.world.member("viewer@acme.test", "viewer")
    viewer = r9.login("viewer@acme.test", "Member-Password-2026!", r9.acme)
    assert viewer.get(f"/api/v1/connectors/jobs/{acme_job}/quarantine").status_code == 200
    export = viewer.post(f"/api/v1/connectors/jobs/{acme_job}/quarantine/export", json={})
    assert export.status_code == 403, export.text


def test_notification_channel_routes_404_another_workspaces_channel(r9):
    made = r9.owner.post("/api/v1/workspace/notifications", json={
        "workspace_id": r9.acme, "kind": "webhook", "label": "acme-alerts",
        "config": {"url": "https://hooks.example.com/acme"},
    })
    assert made.status_code == 200, made.text
    channel = made.json()["id"]
    # A deployment-wide (unscoped) channel is the platform's, not Bravo's.
    from services.notification_store import create_channel

    platform_channel = create_channel(
        workspace_id="", kind="webhook", label="platform-alerts",
        config={"url": "https://hooks.example.com/platform"}, created_by="root@example.test",
    ).id

    bravo = r9.bravo()
    for target in (channel, platform_channel):
        for method, suffix, body in (
            ("PATCH", "", {"label": "pwned"}),
            ("POST", "/test", None),
            ("DELETE", "", None),
        ):
            foreign = bravo.request(method, f"/api/v1/workspace/notifications/{target}{suffix}", json=body)
            missing = bravo.request(method, f"/api/v1/workspace/notifications/nope-{target}{suffix}", json=body)
            _same_404(foreign, missing, "Channel not found")

    from services.notification_store import get_channel

    assert get_channel(channel).label == "acme-alerts"
    assert get_channel(platform_channel).label == "platform-alerts"
    renamed = r9.owner.patch(f"/api/v1/workspace/notifications/{channel}", json={"label": "acme-2"})
    assert renamed.status_code == 200, renamed.text


def test_cdc_snapshot_cancel_404s_another_workspaces_signal(r9):
    acme_job = _cdc_job(r9.acme, "acme-db")
    bravo_job = _cdc_job(r9.bravo_workspace_id, "bravo-db")
    queued = r9.owner.post(
        f"/api/v1/transfer/{acme_job}/cdc/snapshots", json={"table": "orders", "primary_key": "id"}
    )
    assert queued.status_code == 200, queued.text
    signal = queued.json()["signal"]["id"] if "signal" in queued.json() else queued.json()["id"]

    bravo = r9.bravo()
    foreign = bravo.post(f"/api/v1/transfer/{bravo_job}/cdc/snapshots/{signal}/cancel")
    missing = bravo.post(f"/api/v1/transfer/{bravo_job}/cdc/snapshots/snap_000000000000/cancel")
    _same_404(foreign, missing, "Snapshot signal not found")

    # The standalone signal routes are scoped the same way: Bravo can neither
    # read nor cancel Acme's backfill by id, nor see it listed.
    foreign_get = bravo.get(f"/api/v1/cdc/snapshots/{signal}")
    missing_get = bravo.get("/api/v1/cdc/snapshots/snap_000000000000")
    _same_404(foreign_get, missing_get, "Snapshot signal not found")
    foreign_cancel = bravo.post(f"/api/v1/cdc/snapshots/{signal}/cancel")
    missing_cancel = bravo.post("/api/v1/cdc/snapshots/snap_000000000000/cancel")
    _same_404(foreign_cancel, missing_cancel, "Snapshot signal not found")
    listed = bravo.get("/api/v1/cdc/snapshots")
    assert listed.status_code == 200, listed.text
    assert signal not in listed.text

    from services.cdc_incremental_snapshot import get_signal

    assert get_signal(signal).status == "pending"
    owned = r9.owner.post(f"/api/v1/transfer/{acme_job}/cdc/snapshots/{signal}/cancel")
    assert owned.status_code == 200, owned.text
    assert owned.json()["status"] == "cancelled"
