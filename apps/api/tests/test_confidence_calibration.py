"""Confidence calibration summary tests."""

from services.confidence_calibration import summarize_mapping_confidence


def test_summarize_with_sample_parse_rates():
    mappings = [
        {"source": "a", "target": "a", "confidence": 0.9, "sample_parse_rate": 1.0},
        {"source": "b", "target": "b", "confidence": 0.8, "sample_parse_rate": 0.5, "requires_review": True},
    ]
    summary = summarize_mapping_confidence(mappings)
    assert summary["mapping_count"] == 2
    assert summary["sample_validated_count"] == 2
    assert summary["review_count"] == 1
    assert summary["calibrated_score"] <= summary["average_confidence"]


def test_empty_mappings():
    summary = summarize_mapping_confidence([])
    assert summary["average_confidence"] == 0.0
