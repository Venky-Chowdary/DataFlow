"""Tests for sample-driven mapping validation."""

from services.sample_validator import refine_mapping_confidence, refine_mappings_with_samples


def test_high_parse_rate_boosts_confidence():
    m = refine_mapping_confidence(
        {"source": "AMT", "target": "amount", "confidence": 0.82, "reasoning": "BM25"},
        samples=["10.50", "20.00", "5.25"],
        source_type="VARCHAR",
        target_type="DECIMAL",
    )
    assert m["confidence"] >= 0.86
    assert m.get("sample_parse_rate", 0) >= 0.9


def test_low_parse_rate_flags_review():
    m = refine_mapping_confidence(
        {"source": "AMT", "target": "amount", "confidence": 0.88, "reasoning": "match"},
        samples=["not-a-number", "also-bad", "nope"],
        source_type="VARCHAR",
        target_type="DECIMAL",
    )
    assert m["requires_review"] is True
    assert m["confidence"] < 0.88


def test_refine_mappings_batch():
    mappings = [{"source": "id", "target": "id", "confidence": 0.9}]
    out = refine_mappings_with_samples(
        mappings,
        source_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2", "3"]}],
        target_schemas=[{"name": "id", "inferred_type": "INTEGER", "samples": []}],
    )
    assert len(out) == 1
    assert "transform" in out[0]
