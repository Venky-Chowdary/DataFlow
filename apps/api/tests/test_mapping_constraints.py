"""Tests for destination-aware mapping constraints."""

from services.mapping_constraints import (
    detect_duplicate_targets,
    enforce_destination_constraints,
    mapping_plan_summary,
    unmapped_sources,
)


def test_enforce_drops_invented_targets():
    mappings = [
        {"source": "cust_id", "target": "customer_id", "confidence": 0.9},
        {"source": "amt", "target": "made_up_col", "confidence": 0.88},
    ]
    kept, dropped, invented = enforce_destination_constraints(
        mappings, ["customer_id", "amount"]
    )
    assert len(kept) == 1
    assert kept[0]["target"] == "customer_id"
    assert "amt" in dropped
    assert "amt" in invented


def test_enforce_resolves_target_spelling():
    mappings = [{"source": "Email", "target": "email_address", "confidence": 0.91}]
    kept, _, _ = enforce_destination_constraints(mappings, ["EMAIL_ADDRESS"])
    assert kept[0]["target"] == "EMAIL_ADDRESS"


def test_enforce_preserves_leading_underscore():
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.95},
        {"source": "_id", "target": "_id", "confidence": 0.95},
    ]
    kept, _, _ = enforce_destination_constraints(mappings, ["id", "_id"])
    targets = [m["target"] for m in kept]
    assert targets == ["id", "_id"]


def test_duplicate_target_detection():
    dupes = detect_duplicate_targets([
        {"source": "a", "target": "id"},
        {"source": "b", "target": "ID"},
    ])
    assert dupes == ["ID"]


def test_id_and_underscore_id_are_distinct():
    dupes = detect_duplicate_targets([
        {"source": "a", "target": "id"},
        {"source": "b", "target": "_id"},
    ])
    assert dupes == []


def test_plan_summary_coverage():
    summary = mapping_plan_summary(
        source_columns=["a", "b", "c"],
        target_columns=["x", "y"],
        mappings=[{"source": "a", "target": "x", "confidence": 0.9}],
    )
    assert summary["mapped_count"] == 1
    assert "b" in summary["unmapped_sources"]
    assert summary["coverage_pct"] == 33.3


def test_unmapped_sources():
    assert unmapped_sources(["a", "b"], [{"source": "a", "target": "x"}]) == ["b"]
