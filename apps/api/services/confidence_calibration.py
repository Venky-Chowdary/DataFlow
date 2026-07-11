"""Mapping confidence calibration — aggregate sample-validated scores."""

from __future__ import annotations

from typing import Any


def summarize_mapping_confidence(mappings: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize calibrated confidence from sample parse rates on mappings."""
    if not mappings:
        return {"average_confidence": 0.0, "sample_validated_count": 0, "review_count": 0}

    confidences = [float(m.get("confidence", 0)) for m in mappings]
    parse_rates = [float(m["sample_parse_rate"]) for m in mappings if m.get("sample_parse_rate") is not None]
    review = sum(1 for m in mappings if m.get("requires_review"))
    low_parse = sum(
        1 for m in mappings
        if m.get("sample_parse_rate") is not None and float(m["sample_parse_rate"]) < 0.5
    )

    avg_conf = sum(confidences) / len(confidences)
    avg_parse = sum(parse_rates) / len(parse_rates) if parse_rates else None

    calibrated = avg_conf
    if avg_parse is not None:
        calibrated = round(avg_conf * 0.6 + avg_parse * 0.4, 3)

    return {
        "average_confidence": round(avg_conf, 3),
        "calibrated_score": calibrated,
        "sample_validated_count": len(parse_rates),
        "average_parse_rate": round(avg_parse, 3) if avg_parse is not None else None,
        "review_count": review,
        "low_parse_mappings": low_parse,
        "mapping_count": len(mappings),
    }
