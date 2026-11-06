"""QA MySQL D7 / Snowflake D10: a scheduled table of integer ids is not PII, and
a stored PII acknowledgement and the approve route carry a real finding through.

QA (build 8f7d246d): a schedule on a MySQL table whose BIGINT ids are multiples
of 100000000007 was flagged phone/card, every run went approval_pending, the
approve call answered 409 naming ``replay_compliance_ack``, and
``compliance_acknowledged`` sent at creation changed nothing.

Real MySQL 8 source -> real PostgreSQL destination, through the routes a user
drives: ``POST /api/v1/schedules/`` (REST) and MCP ``create_schedule`` ->
``/api/v1/copilot/confirm``, then scheduler beats and
``POST /schedules/{id}/approvals/{id}/approve``.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat, _view

pytestmark = pytest.mark.timeout(400)

#: 13-16 digit ids; a tenth of them pass Luhn (the card check).
_IDS = [100000000007 * k for k in range(1, 41)]
_ROWS = [
    (i, k % 7, ("Ada", None, "", "   ", "Zoë ✓", "日本語")[k % 6])
    for k, i in enumerate(_IDS)
]


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def _connectors(token: str):
    from services.connector_store import create_connector

    src = create_connector({"name": f"d7-my-{token}", "role": "source", "workspace_id": "",
                            "ssl": False, **h.dest_config("mysql")})
    dst = create_connector({"name": f"d7-pg-{token}", "role": "destination",
                            "workspace_id": "", "ssl": False, **h.dest_config("postgresql")})
    return src, dst


def _maps(cols):
    return [{"source": c, "target": c, "confidence": 0.99} for c in cols]


def _completed(run, label):
    sched = run["schedule"]
    assert run["started"] == 1, (label, sched.last_status, sched.approval_request)
    job = run["job"]
    assert job.get("status") == "completed", (label, job.get("status"), job.get("error"),
                                              sched.approval_request)
    return job


def test_integer_ids_are_not_cards_or_phones_rest_and_mcp(client):
    my = h.mysql_conn()
    cur = my.cursor()
    token = uuid.uuid4().hex[:8]
    table = f"d7_ids_{token}"
    dests = [f"d7_rest_{token}", f"d7_mcp_{token}"]
    try:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, qty INT, label VARCHAR(40)) "
                    "CHARSET=utf8mb4")
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s)", _ROWS)
        src, dst = _connectors(token)
        resp = client.post("/api/v1/schedules/", json={
            "name": f"d7 rest {token}", "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": dests[0], "interval": "hourly",
            "sync_mode": "full_refresh_overwrite", "primary_key": "id",
            "mappings": _maps(["id", "qty", "label"]), "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        rest_id = resp.json()["id"]
        for n in (1, 2):
            _completed(_beat(rest_id), f"rest run {n}")
            view = _view(client, rest_id)
            assert view["state"] == "scheduled" and view["next_run_at"], view
        got = h.query("postgresql", f'SELECT id, qty, label FROM public."{dests[0]}" ORDER BY id')  # nosec B608
        assert [tuple(r) for r in got] == sorted(_ROWS)

        ok, staged = h.mcp_call(client, "create_schedule", {
            "source_connector_id": src.id, "source_table": table, "dest_connector_id": dst.id,
            "dest_table": dests[1], "sync_mode": "full_refresh_overwrite", "cadence": "hourly",
            "validation_mode": "strict",
        })
        assert ok, staged
        confirmed = client.post("/api/v1/copilot/confirm",
                                json={"ack_id": staged["ack_id"], "reason": "d7 mcp"}).json()
        assert confirmed.get("ok") is True, confirmed
        _completed(_beat(confirmed["schedule_id"]), "mcp run")
    finally:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        for d in dests:
            h.drop_table("postgresql", d)


def test_stored_acknowledgement_applies_and_the_approve_route_resumes(client):
    my = h.mysql_conn()
    cur = my.cursor()
    token = uuid.uuid4().hex[:8]
    table = f"d7_pii_{token}"
    dests = [f"d7_ack_{token}", f"d7_appr_{token}"]
    rows = [(i, f"user{k}@example.com", f"19{70 + k}-05-20", ("Ada", "", "Zoë ✓")[k % 3])
            for k, i in enumerate(_IDS[:12])]
    try:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, email VARCHAR(80), "
                    "birth_date DATE, first_name VARCHAR(40)) CHARSET=utf8mb4")
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s, %s)", rows)
        src, dst = _connectors(token)
        base = {"source_connector_id": src.id, "source_table": table, "dest_connector_id": dst.id,
                "interval": "hourly", "sync_mode": "full_refresh_overwrite", "primary_key": "id",
                "mappings": _maps(["id", "email", "birth_date", "first_name"]), "enabled": True}
        # An acknowledgement given at creation is carried by every run.
        resp = client.post("/api/v1/schedules/", json={
            **base, "name": f"d7 ack {token}", "dest_table": dests[0],
            "compliance_acknowledged": True, "acknowledgment_actor": "qa@example.com",
            "acknowledgment_reason": "Reviewed: test e-mail addresses only",
        })
        assert resp.status_code == 201, resp.text
        _completed(_beat(resp.json()["id"]), "acknowledged at creation")

        # Without one, the real finding parks once; approving it resumes.
        resp = client.post("/api/v1/schedules/", json={
            **base, "name": f"d7 appr {token}", "dest_table": dests[1]})
        assert resp.status_code == 201, resp.text
        sid = resp.json()["id"]
        run = _beat(sid)
        assert run["job"].get("status") != "completed", run["job"].get("status")
        import time

        # The claim is released a moment before the finding is written.
        deadline = time.monotonic() + 15
        while True:
            view = _view(client, sid)
            request = view.get("approval_request") or {}
            if request.get("status") == "open" or time.monotonic() > deadline:
                break
            time.sleep(0.2)
        assert request.get("status") == "open", view
        assert view["state"] == "stalled" and view["stalled_reason"], view

        refused = client.post(f"/api/v1/schedules/{sid}/approvals/{request['id']}/approve",
                              json={"reason": "approve without the attestation"},
                              headers={"X-Actor": "qa@example.com"})
        # Nothing was attested: refused, naming the field to send rather than an
        # internal scope id alone.
        assert refused.status_code == 409, refused.text
        assert '"compliance": true' in refused.json()["detail"], refused.text
        approved = client.post(f"/api/v1/schedules/{sid}/approvals/{request['id']}/approve",
                               json={"reason": "Reviewed: test e-mail addresses only",
                                     "compliance": True},
                               headers={"X-Actor": "qa@example.com"})
        assert approved.status_code == 200, approved.text
        _completed(_beat(sid), "after approval")
        got = h.query("postgresql", f'SELECT COUNT(*) FROM public."{dests[1]}"')  # nosec B608
        assert int(got[0][0]) == len(rows)
    finally:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        for d in dests:
            h.drop_table("postgresql", d)


def test_real_card_numbers_in_a_bigint_column_are_still_held(client):
    """Review of 04f5423fd: a neutral-named BIGINT of real PANs is PCI, not a key."""
    my = h.mysql_conn()
    cur = my.cursor()
    token = uuid.uuid4().hex[:8]
    table, dest = f"d7_pan_{token}", f"d7_pan_dst_{token}"
    pans = [4111111111111111, 5555555555554444, 378282246310005, 6011111111111117]
    try:
        cur.execute(f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, ref BIGINT, label VARCHAR(20)) "
                    "CHARSET=utf8mb4")
        cur.executemany(f"INSERT INTO `{table}` VALUES (%s, %s, %s)",
                        [(i + 1, p, ("Ada", "", "   ", "Zoë ✓")[i % 4]) for i, p in enumerate(pans * 3)])
        src, dst = _connectors(token)
        resp = client.post("/api/v1/schedules/", json={
            "name": f"d7 pan {token}", "source_connector_id": src.id, "source_table": table,
            "dest_connector_id": dst.id, "dest_table": dest, "interval": "hourly",
            "sync_mode": "full_refresh_overwrite", "primary_key": "id",
            "mappings": _maps(["id", "ref", "label"]), "enabled": True,
        })
        assert resp.status_code == 201, resp.text
        run = _beat(resp.json()["id"])
        error = str(run["job"].get("error") or "")
        assert run["job"].get("status") != "completed", run["job"].get("status")
        assert "ref (pci_pan by sample value)" in error, error
    finally:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        h.drop_table("postgresql", dest)
