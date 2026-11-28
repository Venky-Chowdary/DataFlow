"""D435: "Waiting on a decision" names routes by connection and gives a fix that fits the finding.

QA (round 72-75, Schedules page): parked schedules were listed as
"<connector-uuid>:<table> -> <connector-uuid>:<table>", and a DNS failure
("Host not found ... Check the spelling of the host name") carried the
remedy "Open Validate for this job and resolve the blocking check": Validate
cannot fix a connection's host. A parked schedule whose connector had been
deleted gave no hint of that either.
"""

from __future__ import annotations

import pytest

from tests import r4_route_harness as h

DNS_FINDING = (
    "Host not found: the connection's host name didn't resolve (DNS). Check the spelling "
    "of the host name on the connection."
)


@pytest.fixture
def client(tmp_path, monkeypatch):
    h.isolate(tmp_path, monkeypatch)
    from fastapi.testclient import TestClient
    from src.main import app

    return TestClient(app)


def _route():
    from services.connector_store import create_connector
    from services.schedule_store import create_schedule

    src = create_connector({"name": "QAR72-MY", "type": "mysql", "role": "source", "host": "127.0.0.1",
                            "port": 3306, "database": "d435", "username": "d435", "password": "x-not-real"})
    dst = create_connector({"name": "QAR72-ORA", "type": "postgresql", "role": "destination",
                            "host": "127.0.0.1", "port": 5432, "database": "d435", "username": "d435",
                            "password": "x-not-real"})
    sched = create_schedule({
        "name": "QAR72-S4b-my-ora-iu", "source_connector_id": src.id, "source_table": "r72_up_src2",
        "dest_connector_id": dst.id, "dest_table": "R72_UP_ORA2", "interval": "daily", "timezone": "UTC",
        "sync_mode": "full_refresh_overwrite", "mappings": [{"source": "id", "target": "id"}],
    })
    return src, dst, sched


def _park_from_job(sched, *, error: str, error_code: str):
    """Park the way ``_finalize_run`` does for a failed job."""
    from services.schedule_runner import _finding_patch
    from services.schedule_store import get_schedule, update_schedule

    patch = _finding_patch(
        get_schedule(sched.id), error, attempt=0, job_id="job-1",
        evidence={"error_code": error_code, "park_reason": "deterministic_refusal"},
    )
    assert patch is not None
    update_schedule(sched.id, patch)
    return get_schedule(sched.id).approval_request


def _inbox(client) -> list[dict]:
    resp = client.get("/api/v1/schedules/approvals/open")
    assert resp.status_code == 200, resp.text
    return resp.json()["approvals"]


def test_a_dns_finding_is_parked_with_a_connection_remedy(client):
    _src, _dst, sched = _route()
    request = _park_from_job(sched, error=DNS_FINDING, error_code="network.dns_not_found")
    corrective = request["corrective_action"]
    assert "Validate" not in corrective, corrective
    assert "host name" in corrective and "connection" in corrective, corrective


def test_the_inbox_names_connections_and_tables_and_keeps_ids_off_the_label(client):
    src, dst, sched = _route()
    _park_from_job(sched, error=DNS_FINDING, error_code="network.dns_not_found")
    item = next(i for i in _inbox(client) if i["schedule_id"] == sched.id)
    assert item["source_connector"] == {"id": src.id, "name": "QAR72-MY", "type": "mysql", "deleted": False}
    assert item["destination_connector"]["name"] == "QAR72-ORA"
    assert item["source_table"] == "r72_up_src2" and item["dest_table"] == "R72_UP_ORA2"
    assert item["remedy"]["target"] == "connection", item["remedy"]


def test_a_finding_parked_before_the_fix_still_shows_the_connection_remedy(client):
    from services.schedule_store import get_schedule, update_schedule

    _src, _dst, sched = _route()
    request = dict(_park_from_job(sched, error=DNS_FINDING, error_code="network.dns_not_found"))
    request["corrective_action"] = (
        "Open Validate for this job and resolve the blocking check, then run the schedule again."
    )
    update_schedule(sched.id, {"approval_request": request})
    assert "Validate" in get_schedule(sched.id).approval_request["corrective_action"]

    item = next(i for i in _inbox(client) if i["schedule_id"] == sched.id)
    assert "Validate" not in item["approval"]["corrective_action"], item["approval"]
    assert "host name" in item["approval"]["corrective_action"]


def test_a_sign_in_failure_points_at_the_connection_too(client):
    _src, _dst, sched = _route()
    request = _park_from_job(
        sched, error="MySQL rejected the sign-in for user d435: the username or password is wrong.",
        error_code="auth.password_rejected",
    )
    assert "Validate" not in request["corrective_action"], request
    assert "username and password" in request["corrective_action"]


def test_a_gate_finding_keeps_its_own_remedy(client):
    _src, _dst, sched = _route()
    _park_from_job(sched, error="Gate G6 refused the run: target DDL unproven.", error_code="")
    item = next(i for i in _inbox(client) if i["schedule_id"] == sched.id)
    assert item["remedy"] == {}, item["remedy"]


def test_a_deleted_connection_says_so_with_reselect_guidance(client):
    from services.connector_store import delete_connector

    src, _dst, sched = _route()
    _park_from_job(sched, error=DNS_FINDING, error_code="network.dns_not_found")
    delete_connector(src.id)
    item = next(i for i in _inbox(client) if i["schedule_id"] == sched.id)
    assert item["source_connector"]["deleted"] is True
    assert item["remedy"]["target"] == "connection_deleted", item["remedy"]
    assert item["remedy"]["sides"] == "source"
    corrective = item["remedy"]["corrective_action"]
    assert corrective.startswith("Connection deleted") and "Re-select" in corrective, corrective
