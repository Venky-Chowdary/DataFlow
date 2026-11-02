from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def studio_confirm(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_PILOT_ACK_PATH", str(tmp_path / "acks.json"))

    from src.ai.copilot import ack_ledger
    from src.main import app
    from src.routers import copilot_router

    monkeypatch.setattr(ack_ledger, "_ledger", None)
    yield SimpleNamespace(
        client=TestClient(app),
        ledger=ack_ledger.get_ack_ledger(),
        router=copilot_router,
    )


def _set_role(monkeypatch, router, role: str) -> None:
    monkeypatch.setattr(router, "_caller", lambda _request: (role, f"{role}@example.test"))


def _put_studio_ack(ledger, **overrides) -> str:
    payload = {
        "remediation_kind": "open_bad_data_fix",
        "run_id": None,
        "destructive": False,
        "workspace_id": None,
    }
    payload.update(overrides)
    return ledger.put(
        kind="studio_remediation",
        payload=payload,
        preview={
            "label": "Fix bad data…",
            "remediation_kind": payload["remediation_kind"],
            "run_id": payload["run_id"],
            "destructive": payload["destructive"],
        },
    )


def test_role_without_job_run_cannot_consume_studio_ack(studio_confirm, monkeypatch):
    _set_role(monkeypatch, studio_confirm.router, "viewer")
    ack_id = _put_studio_ack(studio_confirm.ledger)

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": ack_id},
    )

    assert response.status_code == 403
    peek = studio_confirm.ledger.peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
    assert studio_confirm.ledger.unusable_reason(ack_id) == ""


def test_staged_workspace_mismatch_refuses_before_consuming_ack(studio_confirm, monkeypatch):
    _set_role(monkeypatch, studio_confirm.router, "operator")
    ack_id = _put_studio_ack(studio_confirm.ledger, workspace_id="workspace-a")

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        headers={"X-Workspace-Id": "workspace-b"},
        json={"ack_id": ack_id},
    )

    assert response.status_code == 403
    peek = studio_confirm.ledger.peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
    assert studio_confirm.ledger.unusable_reason(ack_id) == ""


def test_allowed_confirm_uses_staged_values_not_client_fields(
    studio_confirm, monkeypatch
):
    _set_role(monkeypatch, studio_confirm.router, "operator")
    from services import preflight_run_store

    monkeypatch.setattr(
        preflight_run_store,
        "get_preflight_run",
        lambda run_id: {"run_id": run_id, "workspace_id": "workspace-a"},
    )
    ack_id = _put_studio_ack(
        studio_confirm.ledger,
        remediation_kind="open_bad_data_fix",
        run_id="pf_server",
        workspace_id="workspace-a",
    )

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        headers={"X-Workspace-Id": "workspace-a"},
        json={
            "ack_id": ack_id,
            "remediation_kind": "quarantine_and_rerun",
            "run_id": "pf_client",
            "label": "Client label",
            "destructive": True,
        },
    )

    assert response.status_code == 200, response.text
    body = response.json()
    assert body == {
        "ok": True,
        "idempotent": False,
        "kind": "studio_remediation",
        "remediation_kind": "open_bad_data_fix",
        "run_id": "pf_server",
        "label": "Fix bad data…",
        "destructive": False,
    }
    assert studio_confirm.ledger.peek(ack_id)["consumed"] is True


def test_unmapped_ack_kind_is_refused(studio_confirm, monkeypatch):
    _set_role(monkeypatch, studio_confirm.router, "admin")
    ack_id = studio_confirm.ledger.put(
        kind="future_unmapped_action",
        payload={},
    )

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        json={"ack_id": ack_id},
    )

    assert response.status_code == 403
    peek = studio_confirm.ledger.peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False


def test_missing_preflight_run_refuses_and_releases_studio_ack(
    studio_confirm, monkeypatch
):
    _set_role(monkeypatch, studio_confirm.router, "operator")
    from services import preflight_run_store

    monkeypatch.setattr(preflight_run_store, "get_preflight_run", lambda _run_id: None)
    ack_id = _put_studio_ack(
        studio_confirm.ledger,
        run_id="pf_missing",
        workspace_id="workspace-a",
    )

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        headers={"X-Workspace-Id": "workspace-a"},
        json={"ack_id": ack_id},
    )

    assert response.status_code == 404
    peek = studio_confirm.ledger.peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
    assert studio_confirm.ledger.unusable_reason(ack_id) == ""


def test_preflight_run_workspace_mismatch_refuses_and_releases_studio_ack(
    studio_confirm, monkeypatch
):
    _set_role(monkeypatch, studio_confirm.router, "operator")
    from services import preflight_run_store

    monkeypatch.setattr(
        preflight_run_store,
        "get_preflight_run",
        lambda run_id: {"run_id": run_id, "workspace_id": "workspace-b"},
    )
    ack_id = _put_studio_ack(
        studio_confirm.ledger,
        run_id="pf_other_workspace",
        workspace_id="workspace-a",
    )

    response = studio_confirm.client.post(
        "/api/v1/copilot/confirm",
        headers={"X-Workspace-Id": "workspace-a"},
        json={"ack_id": ack_id},
    )

    assert response.status_code == 403
    peek = studio_confirm.ledger.peek(ack_id)
    assert peek is not None
    assert peek["consumed"] is False
    assert studio_confirm.ledger.unusable_reason(ack_id) == ""


def test_local_recovery_studio_pending_action_has_server_ack_id(studio_confirm):
    from src.ai.copilot.pilot_agent import DataPilotAgent

    response = DataPilotAgent().chat("fix bad data")

    pending = next(
        action for action in response.pending_actions if action.get("type") == "studio"
    )
    assert pending["payload"]["ack_id"]


def test_studio_ack_uses_data_context_workspace_not_tool_argument(studio_confirm):
    from src.ai.copilot.pilot_agent import DataPilotAgent
    from src.ai.copilot.tools import DataPilotTools

    agent = DataPilotAgent()
    args = agent._with_result_context(
        "remediate_validation",
        {"kind": "open_bad_data_fix", "workspace_id": "client-workspace"},
        {"workspace_id": "staging-workspace"},
    )
    staged = DataPilotTools().execute("remediate_validation", args)

    assert staged.success is True, staged.error
    ack_id = staged.output["ack_id"]
    payload, error = studio_confirm.ledger.get_pending_payload(ack_id)
    assert error == ""
    assert payload == {
        "remediation_kind": "open_bad_data_fix",
        "run_id": None,
        "destructive": False,
        "workspace_id": "staging-workspace",
    }


def test_normalize_control_characters_remains_navigation_only():
    from src.ai.copilot.tools import DataPilotTools

    result = DataPilotTools().execute(
        "remediate_validation",
        {"kind": "normalize_control_chars"},
    )

    assert result.success is True
    assert result.output["ui_only"] is True
    assert "ack_id" not in result.output
