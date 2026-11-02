"""Numeric samples must beat weak header lexicon (FSI State Legitimacy ≠ state_code)."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.semantic_analyzer import analyze_column  # noqa: E402


def test_state_legitimacy_score_not_state_code():
    out = analyze_column(
        "P1: State Legitimacy",
        "DECIMAL",
        samples=["9.6", "8.7", "9.1", "10", "9.5"],
    )
    assert out["semantic_role"] == "numeric_value"
    assert "province" not in out["description"].lower()
    assert "state or province" not in out["description"].lower()


def test_idps_metric_not_identifier_substring():
    out = analyze_column(
        "S2: Refugees and IDPs",
        "DECIMAL",
        samples=["9", "8.5", "7.2"],
    )
    assert out["semantic_role"] == "numeric_value"
    assert out["semantic_role"] != "identifier"


def test_exact_state_code_column_still_lexicon():
    out = analyze_column("state_code", "VARCHAR", samples=["CA", "NY", "TX"])
    assert out["semantic_role"] == "state_code"


def test_short_id_alias_not_substring_of_idps():
    from services.semantic_analyzer import _role_from_name

    role, conf = _role_from_name("s2_refugees_and_idps")
    assert role != "identifier" or conf < 0.8
