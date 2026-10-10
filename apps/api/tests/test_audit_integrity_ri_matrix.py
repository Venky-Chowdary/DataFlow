"""AUDIT-INTEGRITY — Gate-8 G22 source/destination RI matrix (no live database).

Measured orphans block; a probe that raised blocks; an unproven scan warns; the
anomaly origin is named; a G22-only failure keeps the rows as evidence; strict
routes land orphans for G22 instead of quarantining them under a green checksum.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.destination_ri_probe import apply_dest_ri_to_reconcile, build_dest_ri_gate
from src.transfer.engine import _note_failed_batch_undo, _ri_is_the_only_failure
from src.transfer.stream import _fk_orphans_quarantine

_REL = {
    "columns": ["customer_id"],
    "referred_table": "customers",
    "referred_columns": ["id"],
    "status": "orphans",
    "available": True,
    "orphan_count": 1,
}


def _orphans(origin: str | None = None) -> dict[str, Any]:
    ev: dict[str, Any] = {
        "verified": False,
        "asked": True,
        "orphan_rows": 1,
        "orphan_relations": ["customer_id->customers"],
        "relations": [dict(_REL)],
    }
    if origin:
        ev["anomaly_origin"] = origin
    return ev


@pytest.mark.parametrize(
    ("origin", "phrase"),
    [("source", "Source anomaly"), ("destination", "Destination anomaly"), (None, "")],
)
def test_orphans_block_and_name_the_anomaly_origin(origin: str | None, phrase: str) -> None:
    gate = build_dest_ri_gate(_orphans(origin), has_relationships=True)
    assert gate["status"] == "block"
    assert gate["details"]["rule_id"].endswith(".orphans")
    assert gate["details"]["anomaly_origin"] == (origin or "undetermined")
    rel = gate["details"]["relations"][0]
    assert (rel["columns"], rel["referred_table"], rel["referred_columns"]) == (
        ["customer_id"],
        "customers",
        ["id"],
    )
    if phrase:
        assert phrase in gate["message"]
    else:
        assert "anomaly" not in gate["message"].lower()


def test_checksum_match_cannot_green_measured_orphans() -> None:
    out = apply_dest_ri_to_reconcile(
        {"passed": True, "message": "checksums match", "source_checksum": "a", "target_checksum": "a"},
        evidence=_orphans("destination"),
        has_relationships=True,
    )
    assert out["passed"] is False
    assert out["passed_before_dest_ri"] is True
    assert "referential integrity failed" in out["message"].lower()


def test_probe_error_fails_closed() -> None:
    evidence = {"verified": False, "asked": True, "probe_error": True, "reason": "probe failed: boom"}
    gate = build_dest_ri_gate(evidence, has_relationships=True)
    assert gate["status"] == "block"
    assert gate["details"]["rule_id"].endswith(".probe_error")
    out = apply_dest_ri_to_reconcile({"passed": True, "message": "ok"}, evidence=evidence)
    assert out["passed"] is False


def test_clean_scan_passes_and_undeclared_skips() -> None:
    clean = {
        "verified": True,
        "asked": True,
        "orphan_rows": 0,
        "relations": [{**_REL, "status": "scanned", "orphan_count": 0}],
    }
    assert build_dest_ri_gate(clean, has_relationships=True)["status"] == "pass"
    assert build_dest_ri_gate({"asked": False, "relations": []})["status"] == "skip"


def _recon(passed_before: bool, rule: str = ".orphans") -> dict[str, Any]:
    return {
        "passed": False,
        "passed_before_dest_ri": passed_before,
        "g22_dest_referential_integrity": {
            "status": "block",
            "details": {"rule_id": f"g22_dest_referential_integrity{rule}"},
        },
    }


def test_only_an_ri_only_failure_retains_rows() -> None:
    assert _ri_is_the_only_failure(_recon(True)) is True
    assert _ri_is_the_only_failure(_recon(False)) is False
    assert _ri_is_the_only_failure(_recon(True, ".probe_error")) is False
    assert _ri_is_the_only_failure({"passed": False}) is False
    assert _ri_is_the_only_failure(None) is False


def test_ri_only_failure_note_keeps_batch() -> None:
    summary: dict[str, Any] = {"table": "orders"}
    msg = _note_failed_batch_undo(None, summary, "RI failed", recon=_recon(True))
    assert summary["partial_batch_undo"] == "retained"
    assert "orphan rows are the evidence" in msg


@pytest.mark.parametrize(
    ("mode", "quarantines"),
    [("strict", False), ("maximum", False), ("balanced", True), ("warn", True)],
)
def test_strict_routes_land_orphans_for_g22(mode: str, quarantines: bool) -> None:
    assert _fk_orphans_quarantine(mode) is quarantines
