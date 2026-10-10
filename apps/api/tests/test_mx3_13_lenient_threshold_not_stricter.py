"""QA MX3-13 — validation_mode=lenient must not be stricter than balanced.

Pilot/MCP advertise ``lenient`` (``strict|balanced|lenient``) but the mode
contract had no such mode, so it fell through to the unknown-mode rule and ran
as Strict: confidence floor 0.85 vs balanced 0.75, and g11 printed
"Validation posture lenient uses confidence threshold 0.85".
"""

from __future__ import annotations

from services.preflight_service import confidence_threshold_for_mode, run_transfer_policy_gates
from services.validation_mode_contract import mode_contract


def _g11(mode: str) -> dict:
    gates = run_transfer_policy_gates(validation_mode=mode)
    return next(g for g in gates if g["id"] == "g11_validation_posture")


def test_lenient_floor_is_not_stricter_than_balanced():
    lenient = confidence_threshold_for_mode("lenient")
    balanced = confidence_threshold_for_mode("balanced")
    strict = confidence_threshold_for_mode("strict")
    assert lenient <= balanced <= strict, (lenient, balanced, strict)
    assert mode_contract("lenient")["allows_write"] is True


def test_g11_reports_the_applied_mode_and_its_threshold():
    gate = _g11("lenient")
    assert gate["details"]["validation_mode"] == "balanced"
    assert gate["details"]["requested_validation_mode"] == "lenient"
    assert gate["details"]["confidence_threshold"] == 0.75
    assert "runs as balanced" in gate["message"]


def test_unknown_mode_still_fails_closed_to_strict_and_says_so():
    gate = _g11("yolo")
    assert gate["details"]["validation_mode"] == "strict"
    assert gate["details"]["confidence_threshold"] == 0.85
    assert "not a validation mode" in gate["message"]
    assert _g11("strict")["message"] == "Validation posture strict uses confidence threshold 0.85"
