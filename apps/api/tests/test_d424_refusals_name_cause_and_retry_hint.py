"""D424: a refusal's envelope names its real cause and whether retrying helps.

Build 70cb: every refusal from plan_transfer (REST ``/mcp/tools/call``) and from
``POST /connectors/jobs/{id}/resume`` came back as ``platform.request_refused``,
message "Request refused", action "Check the values on the form, then try
again.", ``retriable: false``, with the cause only in ``detail.error``: a
preflight timeout was marked non-retriable, and a Resume refused because the
checkpoint is not safe pointed at the form instead of Retry.

Driven through the real app (TestClient) on the routes QA used.
"""

from __future__ import annotations

import sqlite3
import time
import uuid

import pytest

from tests.test_u1_d425_connector_save_refusals_http import client  # noqa: F401  (fixture)

FORM = "Check the values on the form"


def _sqlite_pair(tmp_path):
    from services.connector_store import create_connector

    path = tmp_path / "src.db"
    conn = sqlite3.connect(str(path))
    with conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, name TEXT)")
        conn.execute("INSERT INTO orders VALUES (1, 'a')")
    conn.close()
    src = create_connector({"name": "d424-src", "type": "sqlite", "role": "source",
                            "database": str(path), "workspace_id": ""})
    dst = create_connector({"name": "d424-dst", "type": "sqlite", "role": "destination",
                            "database": str(tmp_path / "dst.db"), "workspace_id": ""})
    return src, dst


def _envelope(response) -> dict:
    body = response.json()
    assert isinstance(body.get("error"), dict), body
    return body["error"]


def test_a_plan_that_times_out_in_preflight_names_the_step_and_is_retriable(client, monkeypatch, tmp_path):  # noqa: F811
    import src.ai.copilot.transfer_tools as transfer_tools

    src, dst = _sqlite_pair(tmp_path)

    def slow_preflight(**_kwargs):
        time.sleep(6)

    monkeypatch.setenv("DATAFLOW_TOOL_DEADLINE_SECONDS", "2")
    monkeypatch.setattr(transfer_tools, "_run_preflight", slow_preflight)
    r = client.post("/api/v1/mcp/tools/call", json={"name": "plan_transfer", "arguments": {
        "source_connector_id": src.id, "source_table": "orders",
        "dest_connector_id": dst.id, "dest_table": "orders_copy", "sync_mode": "full_refresh_overwrite",
    }})
    assert r.status_code == 422, r.text
    err = _envelope(r)
    assert "within 2s" in err["message"] and "preflight" in err["message"], err
    assert err["message"] != "Request refused"
    assert err["retriable"] is True, err
    assert FORM not in err["action"]["label"], err
    assert err["reference"]


def test_an_mcp_tool_error_carries_its_cause_in_the_envelope_message(client, tmp_path):  # noqa: F811
    src, _dst = _sqlite_pair(tmp_path)
    r = client.post("/api/v1/mcp/tools/call", json={"name": "plan_transfer", "arguments": {
        "source_connector_id": src.id, "source_table": "orders",
        "dest_connector_id": "no-such-connector", "dest_table": "t", "sync_mode": "full_refresh_overwrite",
    }})
    assert r.status_code == 422, r.text
    err = _envelope(r)
    detail_error = r.json()["detail"]["error"]
    assert err["message"] == detail_error and err["message"] != "Request refused", (err, detail_error)


def _failed_job_without_checkpoint(tmp_path) -> str:
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest, transfer_request_to_dict

    src = tmp_path / "s.db"
    sqlite3.connect(str(src)).close()  # no table: the run fails before any row
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="missing"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(tmp_path / "d.db"),
                                   table="t"),
        sync_mode="full_refresh_append",
        mappings=[{"source": "id", "target": "id", "confidence": 1.0}],
    )
    job_id = uuid.uuid4().hex[:24]
    result = UniversalTransferEngine().execute_tracked(request, job_id)
    assert result.success is False
    job_id = result.job_id or job_id
    mongo = get_mongodb_service()
    # The submit routes store the request on the job; execute_tracked alone does not.
    mongo.update_job_fields(job_id, {"transfer_request": transfer_request_to_dict(request)})
    job = mongo.get_job(job_id)
    assert job and job.get("transfer_request") and not job.get("checkpoint"), job
    return job_id


def test_a_resume_refused_for_an_unsafe_checkpoint_points_at_retry(client, tmp_path):  # noqa: F811
    job_id = _failed_job_without_checkpoint(tmp_path)
    r = client.post(f"/api/v1/connectors/jobs/{job_id}/resume")
    assert r.status_code == 409, r.text
    err = _envelope(r)
    assert "Use Retry" in err["message"], err
    assert err["message"] != "Request refused"
    assert err["action"]["label"].startswith("Use Retry"), err
    assert err["action"]["ui"] == "retry", err
    assert FORM not in err["action"]["label"]


def test_an_unresolvable_host_names_the_connection_not_the_form(client, monkeypatch):  # noqa: F811
    """AC-3: a DNS refusal's action opens the connection."""
    import socket

    def no_dns(*_a, **_k):
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    monkeypatch.setattr(socket, "getaddrinfo", no_dns)
    from services.connector_store import create_connector

    pg = create_connector({"name": "d424-pg", "type": "postgresql", "role": "both",
                           "host": "db.does-not-resolve.example", "port": 5432, "database": "app",
                           "username": "app", "password": "dummy-password", "workspace_id": ""})
    r = client.post("/api/v1/mcp/tools/call", json={"name": "plan_transfer", "arguments": {
        "source_connector_id": pg.id, "source_table": "orders",
        "dest_connector_id": pg.id, "dest_table": "orders_copy", "sync_mode": "full_refresh_overwrite",
    }})
    assert r.status_code == 422, r.text
    err = _envelope(r)
    assert err["message"] != "Request refused", err
    assert err["code"] == "network.dns_not_found", err
    assert err["action"]["ui"] == "open_connection", err
    assert FORM not in err["action"]["label"]


@pytest.mark.parametrize("status,message,code,retriable", [
    (422, "plan_transfer did not finish within 45s (step: running the preflight checks).",
     "platform.step_timed_out", True),
    (409, "Resume refused: No durable checkpoint - use Retry from start.", "platform.use_retry", False),
    (409, "This schedule run is already in progress.", "platform.state_conflict", False),
    (400, "Name a source.", "platform.request_refused", False),
])
def test_the_envelope_code_follows_the_cause(status, message, code, retriable):
    from services.safe_errors import _platform_envelope

    err = _platform_envelope(message, "ref", message, status)["error"]
    assert (err["code"], err["retriable"]) == (code, retriable), err
    assert err["message"] == message
    assert FORM not in err["action"]["label"]
