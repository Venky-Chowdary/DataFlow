"""Validate's PII/drift/FK approval must reach the persisted-plan preflight.

The plan transport used to POST no body, so an operator who clicked "Approve PII
for this transfer" re-ran preflight with `compliance_acknowledged=False` and
Execute stayed locked forever. These tests pin the whole path: the attestation
contract, the plan service, and the route.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.acknowledgment_contract import (
    AcknowledgmentRefused,
    Acknowledgments,
    acknowledgments_from_policies,
    resolve_acknowledgments,
)
from services.transfer_plan_service import run_plan_preflight, sync_plan_mappings
from services.transfer_plan_store import create_plan, get_plan


@pytest.fixture(autouse=True)
def isolated_stores(tmp_path, monkeypatch):
    monkeypatch.setattr(
        "services.transfer_plan_store.STORE_PATH", tmp_path / "plans.json"
    )
    monkeypatch.setattr("services.audit_log.STORE_PATH", tmp_path / "audit.jsonl")
    yield


def _make_plan() -> str:
    plan = create_plan(
        {
            "name": "excel-pg",
            "source": {"kind": "file", "format": "xlsx"},
            "destination": {
                "kind": "database",
                "format": "postgresql",
                "connector_id": "dst",
                "table": "people",
            },
            "source_columns": ["username", "email"],
            "source_schema": {"username": "VARCHAR", "email": "VARCHAR"},
            "target_columns": ["username", "email"],
            "target_schema": {"username": "VARCHAR", "email": "VARCHAR"},
            "sample_rows": [{"username": "a", "email": "a@b.co"}],
            "policies": {"validation_mode": "strict"},
        }
    )
    sync_plan_mappings(
        plan.id,
        [
            {"source": "username", "target": "username", "confidence": 0.99},
            {"source": "email", "target": "email", "confidence": 0.99},
        ],
    )
    return plan.id


def _run(plan_id: str, **kwargs) -> dict:
    captured: dict = {}

    def fake_run_file_preflight(**kw):
        captured.update(kw)
        return {
            "passed": False,
            "passed_count": 16,
            "total_gates": 16,
            "gates": [],
            "blockers": [],
        }

    with patch("services.transfer_plan_service._preflight") as mock_pf, patch(
        "services.transfer_plan_service.read_source_database",
        side_effect=Exception("skip"),
    ):
        mock_pf.return_value = (
            lambda pf, *_a, **_k: pf,
            lambda mode: 0.85,
            lambda **_k: {
                "connected": True,
                "table_exists": True,
                "can_create_table": True,
                "db_type": "postgresql",
                "column_types": {"username": "VARCHAR", "email": "VARCHAR"},
                "message": "ok",
            },
            fake_run_file_preflight,
            lambda **_k: [],
        )
        run_plan_preflight(plan_id, **kwargs)
    return captured


def test_no_acknowledgment_leaves_the_compliance_gate_standing():
    captured = _run(_make_plan())
    assert captured["compliance_acknowledged"] is False
    assert captured["acknowledgment_actor"] == ""


def test_acknowledgment_reaches_the_preflight_engine():
    plan_id = _make_plan()
    captured = _run(
        plan_id,
        acknowledgments=resolve_acknowledgments(
            compliance=True,
            actor="operator@dataflow.test",
            reason="Governance policy allows moving detected PII for this transfer",
        ),
    )
    assert captured["compliance_acknowledged"] is True
    assert captured["acknowledgment_actor"] == "operator@dataflow.test"
    assert "Governance policy" in captured["acknowledgment_reason"]
    # Unrelated attestations are never inferred from a compliance approval.
    assert captured["schema_drift_acknowledged"] is False
    assert captured["fk_risk_acknowledged"] is False


def test_acknowledgment_is_recorded_on_the_plan_and_audited():
    plan_id = _make_plan()
    _run(
        plan_id,
        acknowledgments=resolve_acknowledgments(
            compliance=True,
            actor="operator@dataflow.test",
            reason="Governance approved for this migration window",
        ),
    )
    plan = get_plan(plan_id)
    assert plan is not None
    assert plan.policies["compliance_acknowledged"] is True
    assert plan.policies["acknowledgment_actor"] == "operator@dataflow.test"
    assert plan.policies["acknowledgment_version"] == plan.active_version

    from services.audit_log import list_audit_events

    actions = [e.get("action") for e in list_audit_events(limit=50)]
    assert "preflight.acknowledge_compliance" in actions


def test_recorded_acknowledgment_survives_a_plain_revalidate():
    plan_id = _make_plan()
    _run(
        plan_id,
        acknowledgments=resolve_acknowledgments(
            compliance=True,
            actor="operator@dataflow.test",
            reason="Governance approved for this migration window",
        ),
    )
    captured = _run(plan_id)
    assert captured["compliance_acknowledged"] is True


def test_remap_invalidates_a_recorded_acknowledgment():
    plan_id = _make_plan()
    _run(
        plan_id,
        acknowledgments=resolve_acknowledgments(
            compliance=True,
            actor="operator@dataflow.test",
            reason="Governance approved for this migration window",
        ),
    )
    # A new mapping revision is a different transfer shape — the operator has to
    # attest again rather than inherit the previous green.
    sync_plan_mappings(
        plan_id,
        [
            {"source": "username", "target": "username", "confidence": 0.99},
            {"source": "email", "target": "work_email", "confidence": 0.99},
        ],
    )
    captured = _run(plan_id)
    assert captured["compliance_acknowledged"] is False


@pytest.mark.parametrize(
    "actor,reason",
    [
        ("", "Governance approved for this migration window"),
        ("o", "Governance approved for this migration window"),
        ("operator@dataflow.test", ""),
        ("operator@dataflow.test", "ok"),
    ],
)
def test_unattributed_acknowledgment_is_refused(actor, reason):
    with pytest.raises(AcknowledgmentRefused):
        resolve_acknowledgments(compliance=True, actor=actor, reason=reason)


def test_nothing_claimed_needs_no_actor_or_reason():
    ack = resolve_acknowledgments()
    assert ack == Acknowledgments()
    assert ack.any_claimed is False


def test_stale_policy_record_is_ignored_by_version():
    policies = {
        "compliance_acknowledged": True,
        "acknowledgment_actor": "operator@dataflow.test",
        "acknowledgment_reason": "Approved for revision 1",
        "acknowledgment_version": 1,
    }
    assert acknowledgments_from_policies(policies, mapping_version=1).compliance is True
    assert acknowledgments_from_policies(policies, mapping_version=2).compliance is False


def test_route_forwards_acknowledgment_body():
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    seen: dict = {}

    def fake_run_plan_preflight(plan_id, *, acknowledgments=None):
        seen["plan_id"] = plan_id
        seen["ack"] = acknowledgments
        return {"passed": True}

    from fastapi import FastAPI

    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    client = TestClient(app)

    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=fake_run_plan_preflight,
    ):
        res = client.post(
            "/api/v1/transfer/plans/p1/preflight",
            json={
                "compliance_acknowledged": True,
                "acknowledgment_actor": "operator@dataflow.test",
                "acknowledgment_reason": "Governance approved for this window",
            },
        )
    assert res.status_code == 200, res.text
    assert seen["ack"].compliance is True
    assert seen["ack"].actor == "operator@dataflow.test"

    # A claimed acknowledgment without an actor is refused before preflight runs.
    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        side_effect=fake_run_plan_preflight,
    ):
        bad = client.post(
            "/api/v1/transfer/plans/p1/preflight",
            json={"compliance_acknowledged": True, "acknowledgment_reason": "x"},
        )
    assert bad.status_code == 400
    assert "acknowledgment_actor" in bad.text


def test_body_less_call_still_runs_preflight():
    """Other callers (schedules, API clients) post no body — that must not 422."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from src.routers import transfer_router

    app = FastAPI()
    app.include_router(transfer_router.router, prefix="/api/v1")
    client = TestClient(app)

    with patch(
        "services.transfer_plan_service.run_plan_preflight",
        return_value={"passed": True},
    ):
        res = client.post("/api/v1/transfer/plans/p1/preflight")
    assert res.status_code == 200, res.text
