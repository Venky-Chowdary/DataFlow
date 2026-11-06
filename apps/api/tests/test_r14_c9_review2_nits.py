"""QA C9 / D7 review 2 nits.

* A job refused by the Validate-time g24_append_continuity gate (not the
  Execute-time check) carries job-level error_code DESTINATION_CONTINUITY and
  parks under it — before, error_code was null and the first beat retried.
* A schedule parked before the code existed (RUN_REFUSED, continuity wording)
  keeps its /recover path.
* A replacing rebuild is staged with risk "destructive", not "mutate".
* The PAN name rule is card-context only: pan_id / pan_african are not cards.
"""

from __future__ import annotations

import uuid

import pytest

from tests import r4_route_harness as h
from tests.test_r12_mongo_incremental_schedule_e2e import _beat
from tests.test_r14_c9_rebuild_disclosure_and_failures_e2e import _parked, _schedule

pytestmark = pytest.mark.timeout(300)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    with TestClient(app) as c:
        yield c


def test_a_g24_refused_job_carries_the_code_and_parks_under_it(client, monkeypatch):
    import services.append_continuity as ac
    from services.schedule_store import get_schedule

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src, dest = _schedule(client, cur, token, keyed=True)
    try:
        # Bypass the Execute-time check so the Validate-time gate refuses.
        monkeypatch.setattr(ac, "check_before_write", lambda *a, **k: None)
        cur.execute(f'DROP TABLE public."{dest}"')  # nosec B608
        job = _beat(sid)["job"]
        assert job.get("status") == "failed"
        gates = [b.get("id") for b in ((job.get("preflight") or {}).get("blockers") or [])]
        assert ac.CONTINUITY_GATE_ID in gates, gates
        assert job.get("error_code") == ac.CONTINUITY_ERROR_CODE, job.get("error_code")
        assert _parked(sid)
        assert (get_schedule(sid).approval_request or {}).get("code") == ac.CONTINUITY_ERROR_CODE
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        h.drop_table("postgresql", dest)


def test_a_schedule_parked_before_the_code_keeps_its_recover_path(client):
    from services.schedule_store import get_schedule, update_schedule

    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src, dest = _schedule(client, cur, token, keyed=True)
    try:
        legacy = {
            "id": "legacy1", "status": "open", "kind": "run_refused", "code": "RUN_REFUSED",
            "finding": f"Verification failed for table '{dest}': the table no longer exists, "
                       "but the last verified run (j) left 4 rows in it.",
            "corrective_action": "Open Validate for this job and resolve the blocking check, "
                                 "then run the schedule again.",
        }
        update_schedule(sid, {"approval_request": legacy})
        dry = client.post(f"/api/v1/schedules/{sid}/recover",
                          json={"action": "rebaseline", "dry_run": True})
        assert dry.status_code == 200, dry.text
        other = {**legacy, "id": "legacy2", "finding": "Mapping confidence below floor: id"}
        update_schedule(sid, {"approval_request": other})
        refused = client.post(f"/api/v1/schedules/{sid}/recover",
                              json={"action": "rebaseline", "dry_run": True})
        assert refused.status_code == 409, refused.text
        assert get_schedule(sid).approval_request["id"] == "legacy2"
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        h.drop_table("postgresql", dest)


def test_a_replacing_rebuild_is_staged_with_a_destructive_risk_class(client):
    pg = h.pg_conn()
    cur = pg.cursor()
    token = uuid.uuid4().hex[:8]
    sid, src, dest = _schedule(client, cur, token, keyed=False)
    try:
        ok, staged = h.mcp_call(client, "recover_schedule_destination",
                                {"schedule_id": sid, "action": "rebuild", "reason": "rebuild"})
        assert ok, staged
        assert staged["risk"] == "destructive" and staged["destructive"] is True, staged
        ok, keep = h.mcp_call(client, "recover_schedule_destination",
                              {"schedule_id": sid, "action": "rebaseline", "reason": "accept"})
        assert ok and keep["risk"] == "mutate" and keep["destructive"] is False, keep
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {src}")  # nosec B608
        h.drop_table("postgresql", dest)


@pytest.mark.parametrize("col,card", [
    ("pan", True), ("card_pan", True), ("pan_value", True), ("pan_number", True),
    ("customer_card_pan", True), ("pan_id", False), ("pan_african", False),
    ("pan_region", False), ("japan", False), ("span", False),
])
def test_the_pan_name_rule_is_card_context_only(col, card):
    from services.compliance_guard import detect_pii_fields

    risk = detect_pii_fields([col], [{col: "x"}]).get("field_risk", {})
    assert ("pci_pan" in risk.get(col, [])) is card, (col, risk)
