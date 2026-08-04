"""Module 7 — Validation Mode Contract tests."""

from __future__ import annotations

import pytest

from services.validation_mode_contract import (
    VALIDATION_MODES,
    WRITE_REFUSED_MODES,
    ValidationModeWriteRefused,
    assert_mode_allows_write,
    confidence_floor_for_mode,
    mode_allows_write,
    mode_contract,
    normalize_validation_mode,
    stamp_validation_mode,
)


def test_charter_modes_present():
    for m in ("strict", "balanced", "migration", "discovery", "audit"):
        assert m in VALIDATION_MODES
    assert "maximum" in VALIDATION_MODES  # legacy Studio stricter variant


def test_every_mode_declares_guarantees_non_guarantees_coverage():
    for m in sorted(VALIDATION_MODES):
        c = mode_contract(m)
        assert c["id"] == m
        assert c["guarantees"], m
        assert c["non_guarantees"], m
        assert c["coverage"], m
        assert c["population_proof"] is False
        assert "confidence_floor" in c
        assert "allows_write" in c


def test_discovery_and_audit_never_write():
    assert WRITE_REFUSED_MODES == frozenset({"discovery", "audit"})
    assert mode_allows_write("discovery") is False
    assert mode_allows_write("audit") is False
    assert mode_allows_write("strict") is True
    assert mode_allows_write("migration") is True
    with pytest.raises(ValidationModeWriteRefused):
        assert_mode_allows_write("audit")
    with pytest.raises(ValidationModeWriteRefused):
        assert_mode_allows_write("discovery")


def test_unknown_mode_fails_closed_to_strict():
    assert normalize_validation_mode("nope") == "strict"
    assert confidence_floor_for_mode("nope") == 0.85


def test_confidence_floors():
    assert confidence_floor_for_mode("maximum") == 0.95
    assert confidence_floor_for_mode("strict") == 0.85
    assert confidence_floor_for_mode("balanced") == 0.75
    assert confidence_floor_for_mode("migration") == 0.75
    assert confidence_floor_for_mode("discovery") == 0.0
    assert confidence_floor_for_mode("audit") == 0.85


def test_stamp_attaches_contract():
    out = stamp_validation_mode({}, "migration")
    assert out["validation_mode"] == "migration"
    assert out["validation_mode_contract"]["allows_write"] is True
    assert out["validation_mode_contract"]["warn_recoverable"] is True


def test_discovery_is_report_only():
    c = mode_contract("discovery")
    assert c["report_only"] is True
    assert c["hard_block_fidelity"] is False
    assert c["allows_write"] is False
