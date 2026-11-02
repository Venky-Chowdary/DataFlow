"""AUDIT-INTEGRITY — Gate-8 G22 source/destination RI matrix (no live database).

Measured orphans block; a probe that raised blocks; an unproven scan warns; the
anomaly origin is named; a G22-only failure keeps the rows as evidence; strict
routes land orphans for G22 instead of quarantining them under a green checksum.
"""

from __future__ import annotations

from typing import Any

import pytest

from services.destination_ri_probe import apply_dest_ri_to_reconcile, build_dest_ri_gate
from src.transfer.stream import _fk_orphan_violation_message, _fk_orphans_fail_closed

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


def test_quarantined_orphans_warn_and_never_certify() -> None:
    clean = {
        "verified": True,
        "asked": True,
        "orphan_rows": 0,
        "relations": [{**_REL, "status": "scanned", "orphan_count": 0}],
    }
    out = apply_dest_ri_to_reconcile(
        {"passed": True, "message": "checksums match", "migration_proven": True},
        evidence=clean,
        has_relationships=True,
        quarantined_orphans=2,
    )
    gate = out["g22_dest_referential_integrity"]
    assert gate["status"] == "warn"
    assert gate["details"]["rule_id"].endswith(".quarantined")
    assert gate["details"]["quarantined_orphan_rows"] == 2
    assert out["fk_orphans_quarantined"] == 2
    assert out["migration_proven"] is False
    assert "2 orphan child row(s) were quarantined" in out["message"]


def test_quarantined_orphans_do_not_soften_measured_orphans() -> None:
    out = apply_dest_ri_to_reconcile(
        {"passed": True, "message": "ok"},
        evidence=_orphans("destination"),
        has_relationships=True,
        quarantined_orphans=1,
    )
    assert out["g22_dest_referential_integrity"]["status"] == "block"
    assert out["passed"] is False


@pytest.mark.parametrize(
    ("mode", "fails_closed"),
    [("strict", True), ("maximum", True), ("balanced", False), ("warn", False)],
)
def test_strict_routes_fail_closed_on_orphans(mode: str, fails_closed: bool) -> None:
    assert _fk_orphans_fail_closed(mode) is fails_closed


def test_strict_orphan_error_names_relationship_and_keys() -> None:
    msg = _fk_orphan_violation_message(
        [
            {
                "reason": "Orphan foreign key (no parent row): parent_id->parent = ('2',)",
                "error_class": "orphan_foreign_key",
            }
        ]
    )
    assert "Referential integrity failed before write" in msg
    assert "parent_id->parent" in msg and "('2',)" in msg
