"""D329: the "Mapping confidence below floor" block names the remedy an MCP or
REST operator can reach (risk_acceptance), says validation_mode=lenient does
not lower the floor, and create_schedule takes the same risk_acceptance.

QA: SFTP ``mx3_sales.csv`` -> MSSQL, ``column_transforms={"amount_s": "currency"}``
blocked at 0.70 < 0.75, identically for balanced and lenient. The remedy said
"Open Map -> Approve with evidence" (Studio only). risk_acceptance on
start_transfer cleared it, but create_schedule had no such parameter, so the
route could not be scheduled at all.

Decision on lenient (read from services/validation_mode_contract.py): lenient
is documented as "a named policy with the balanced confidence floor (0.75)" —
it holds out cells that fail to cast, it does not accept a low-confidence
mapping. The floor is kept; the message now says so.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from src.ai.copilot import schedule_tools

ACCEPTANCE = {
    "approved_by": "qa@example.com",
    "reason": "currency parse reviewed on the sample",
    "execution_policy": "TRANSFORM_AND_CONTINUE",
}

CURRENCY_ROW = {
    "source": "amount_s",
    "target": "amount_s",
    "source_type": "VARCHAR",
    "target_type": "DECIMAL(18,2)",
    "transform": "currency",
    "confidence": 0.7,
}

G4_BLOCK = {
    "id": "g4_mapping_confidence",
    "status": "block",
    "message": "1 mapping(s) below floor 0.75: amount_s→amount_s (0.70)",
    "details": {"low_confidence": ["amount_s→amount_s (0.70)"]},
}


def _confidence_root_blocker() -> dict[str, Any]:
    from services.root_cause_engine import apply_root_causes_to_preflight

    out = apply_root_causes_to_preflight(
        {"passed": False, "gates": [dict(G4_BLOCK)], "blockers": [dict(G4_BLOCK)]}
    )
    roots = [b for b in out["blockers"] if (b.get("details") or {}).get("kind") == "mapping_confidence"]
    assert len(roots) == 1, out["blockers"]
    return roots[0]


def test_confidence_block_names_risk_acceptance_and_its_fields():
    fix = _confidence_root_blocker()["details"]["recommended_fix"]
    assert "risk_acceptance" in fix
    for field in ("approved_by", "reason", "execution_policy", "TRANSFORM_AND_CONTINUE"):
        assert field in fix, field
    assert "columns: [amount_s]" in fix
    # Surfaces that exist, including the schedule tool.
    assert "start_transfer" in fix and "create_schedule" in fix


def test_confidence_block_says_lenient_does_not_lower_the_floor():
    details = _confidence_root_blocker()["details"]
    text = details["recommended_fix"] + " ".join(details["alternative_fixes"])
    assert "lenient" in text and "does not lower this floor" in text
    assert "Discovery/Migration" not in text


def test_lenient_keeps_the_balanced_floor_by_contract():
    from preflight.gates import g4_confidence_floor

    from services.preflight_service import confidence_threshold_for_mode
    from services.schedule_mapping_contract import schedule_mapping_findings

    floors = {m: g4_confidence_floor(confidence_threshold_for_mode(m)) for m in ("balanced", "lenient")}
    assert floors == {"balanced": 0.75, "lenient": 0.75}
    for mode in ("balanced", "lenient"):
        findings = schedule_mapping_findings([dict(CURRENCY_ROW)], mode)
        assert [f["layer"] for f in findings] == ["low_confidence"], findings


def test_signed_acceptance_clears_the_stored_schedule_mapping():
    """Schedules already store mappings with their risk_contract; a signed row
    passes the save check every unattended run applies, after a JSON round trip."""
    from services.schedule_mapping_contract import schedule_mapping_findings
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    signed = _sign_required_risk_contracts(
        [dict(CURRENCY_ROW)], ACCEPTANCE, table="mx3_sales", force_columns={"amount_s"}
    )
    assert signed[0]["risk_contract"]["execution_policy"] == "TRANSFORM_AND_CONTINUE"
    stored = json.loads(json.dumps(signed))
    assert schedule_mapping_findings(stored, "balanced") == []


class _Plan:
    def __init__(self, output: dict[str, Any]):
        self.success = True
        self.error = ""
        self.output = output


def _plan_output(mappings: list[dict[str, Any]], preflight: dict[str, Any]) -> dict[str, Any]:
    return {
        "source": {"connector_id": "c-src", "connector_name": "SFTP", "table": "mx3_sales.csv"},
        "destination": {"connector_id": "c-dst", "connector_name": "MSSQL", "table": "mx3_sales"},
        "sync_mode": "full_refresh_overwrite",
        "schema_policy": "manual_review",
        "validation_mode": "balanced",
        "engine_mappings": mappings,
        "stream_contracts": [],
        "mapped_count": len(mappings),
        "unmapped_source_columns": [],
        "preflight": preflight,
        "data_rules": {},
    }


def test_create_schedule_signs_risk_acceptance_onto_the_stored_mappings(monkeypatch):
    from src.ai.copilot import ack_ledger
    from src.ai.copilot.transfer_tools import _sign_required_risk_contracts

    seen: dict[str, Any] = {}

    def _plan_transfer(**kwargs: Any):
        seen.update(kwargs)
        mappings = [dict(CURRENCY_ROW)]
        if kwargs.get("risk_acceptance"):
            mappings = _sign_required_risk_contracts(
                mappings, kwargs["risk_acceptance"], table="mx3_sales",
                force_columns={"amount_s"},
            )
        return _Plan(_plan_output(mappings, {
            "passed": True, "run_id": "pf_d329", "blockers": [],
            "transfer_decision": {"decision": "approve"},
        }))

    staged: dict[str, Any] = {}

    class _Ledger:
        def put(self, *, kind: str, payload: dict[str, Any], preview=None, **_: Any) -> str:
            staged.update(kind=kind, payload=payload)
            return "ack-d329"

    monkeypatch.setattr(schedule_tools, "plan_transfer", _plan_transfer)
    monkeypatch.setattr(schedule_tools, "_is_execute_cleared", lambda pf: True)
    monkeypatch.setattr(ack_ledger, "get_ack_ledger", lambda: _Ledger())
    monkeypatch.setattr(
        "services.schedule_store.assert_unique_schedule_name", lambda name: None
    )

    result = schedule_tools.create_schedule(
        cadence="daily at 02:00 UTC",
        source_connector_name="SFTP",
        source_table="mx3_sales.csv",
        dest_connector_name="MSSQL",
        column_transforms={"amount_s": "currency"},
        risk_acceptance=dict(ACCEPTANCE),
    )
    assert result.success is True, result.error
    assert seen["risk_acceptance"] == ACCEPTANCE
    stored = staged["payload"]["mappings"]
    assert stored[0]["risk_contract"]["approved_by"] == "qa@example.com"

    from services.schedule_mapping_contract import schedule_mapping_findings

    assert schedule_mapping_findings(stored, staged["payload"]["validation_mode"]) == []


def test_create_schedule_refusal_carries_the_remedy(monkeypatch):
    blocker = _confidence_root_blocker()
    blocked = _plan_output([dict(CURRENCY_ROW)], {
        "passed": False, "run_id": "pf_d329b", "blockers": [blocker],
        "transfer_decision": {"decision": "block"},
    })
    monkeypatch.setattr(schedule_tools, "plan_transfer", lambda **kw: _Plan(blocked))
    result = schedule_tools.create_schedule(cadence="daily at 02:00 UTC")
    assert result.success is False
    assert "To proceed:" in result.error and "risk_acceptance" in result.error


@pytest.mark.parametrize("tool_name", ["create_schedule", "start_transfer", "plan_transfer"])
def test_tool_schemas_expose_risk_acceptance(tool_name):
    from src.ai.copilot.tools import TOOL_DEFINITIONS

    tool = next(t for t in TOOL_DEFINITIONS if t["name"] == tool_name)
    assert "risk_acceptance" in tool["input_schema"]["properties"]
