"""Orphan FK blockers must not get map-only CTAs (Module 4 residual)."""

from __future__ import annotations

from services.preflight_rules import explain_gate, explain_issue


def test_sample_orphan_cta_is_fix_parents_not_map_only():
    g = explain_gate(
        "constraint_fk",
        "Sample orphan probe: 2/10 customer_id values missing from customers.id. "
        "Coverage=sample_orphan_probe — population RI not proven.",
        details={"coverage": "sample_orphan_probe"},
    )
    kinds = {a.get("kind") for a in (g.get("suggested_actions") or [])}
    assert "fix_orphans" in kinds or "run_population_orphan_scan" in kinds
    assert "map_column" not in kinds or len(kinds) > 1
    assert "map" not in (g.get("fix") or "").lower() or "orphan" in (g.get("fix") or "").lower()


def test_population_orphan_issue_catalog():
    issue = explain_issue(
        "Population orphan scan: 12 row(s) in orders.customer_id missing from customers.id. "
        "Coverage=population_orphan_probe — RI not proven."
    )
    kinds = {a.get("kind") for a in (issue.get("suggested_actions") or [])}
    assert "fix_orphans" in kinds
    assert issue.get("gate") == "constraint_fk"


def test_unmapped_fk_still_suggests_map():
    g = explain_gate(
        "constraint_fk",
        "Destination FK columns unmapped — transfer blocked",
        details={"coverage": "destination_fk_metadata"},
    )
    kinds = {a.get("kind") for a in (g.get("suggested_actions") or [])}
    assert "map_column" in kinds
