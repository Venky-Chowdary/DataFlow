"""Calibrated confidence classes + PII preview masking."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.mapping_quality import (  # noqa: E402
    apply_confidence_class,
    classify_mapping_confidence,
    refine_mappings_with_quality,
)
from services.mapping_proof import _sample_preview  # noqa: E402
from services.pii_guard import mask, mask_preview_value  # noqa: E402


def test_exact_name_type_confidence_near_certain() -> None:
    cls = classify_mapping_confidence(
        {
            "source": "id",
            "target": "id",
            "source_type": "INTEGER",
            "target_type": "INTEGER",
            "confidence": 0.92,
        },
        source_profile={"samples": ["1", "2", "3"], "semantic_pattern_score": 0.9, "null_rate": 0.0},
    )
    assert cls["confidence_class"] == "exact_name_type"
    assert apply_confidence_class(0.92, cls) >= 0.97


def test_create_new_stays_capped() -> None:
    cls = classify_mapping_confidence(
        {
            "source": "email",
            "target": "email",
            "source_type": "VARCHAR",
            "target_type": "VARCHAR",
            "confidence": 0.99,
            "assignment_strategy": "identity_passthrough",
            "create_new": True,
        }
    )
    assert cls["confidence_class"] == "create_new_projected"
    assert apply_confidence_class(0.99, cls) <= 0.93


def test_semantic_inference_band_lower() -> None:
    cls = classify_mapping_confidence(
        {
            "source": "cust_nm",
            "target": "customer_name",
            "source_type": "VARCHAR",
            "target_type": "VARCHAR",
            "confidence": 0.93,
        },
        source_profile={"samples": ["a", "b"], "semantic_pattern_score": 0.2, "null_rate": 0.1},
    )
    assert cls["confidence_class"] == "semantic_inference"
    assert apply_confidence_class(0.93, cls) <= 0.82


def test_refine_spreads_confidence_classes() -> None:
    refined = refine_mappings_with_quality(
        [
            {
                "source": "id",
                "target": "id",
                "source_type": "INTEGER",
                "target_type": "INTEGER",
                "confidence": 0.9,
            },
            {
                "source": "tags",
                "target": "tags",
                "source_type": "ARRAY",
                "target_type": "JSONB",
                "confidence": 0.9,
            },
            {
                "source": "cust_nm",
                "target": "full_name",
                "source_type": "VARCHAR",
                "target_type": "VARCHAR",
                "confidence": 0.9,
            },
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "INTEGER", "samples": ["1", "2", "3", "4"]},
            {"name": "tags", "inferred_type": "ARRAY", "samples": ['["a"]', '["b"]']},
            {"name": "cust_nm", "inferred_type": "VARCHAR", "samples": ["x", "y"]},
        ],
    )
    classes = {m["confidence_class"] for m in refined}
    assert "exact_name_type" in classes or "safe_type_promotion" in classes
    assert "structural_json" in classes or "semantic_inference" in classes
    confs = [m["confidence"] for m in refined]
    assert max(confs) - min(confs) > 0.05  # not all ~0.93


def test_email_mask_shape_preserving() -> None:
    assert mask("alice@example.com").startswith("a")
    assert "@example.com" in mask("alice@example.com")
    assert "alice" not in mask("alice@example.com")


def test_sample_preview_masks_pii_columns() -> None:
    preview = _sample_preview(
        {
            "source": "email",
            "target": "email",
            "is_pii": True,
            "samples": ["bob@gmail.com", "carol@gmail.com"],
        }
    )
    assert preview
    assert all("@" in p for p in preview)
    assert all("bob@" not in p and "carol@" not in p for p in preview)


def test_mask_preview_value_force() -> None:
    assert "***" in mask_preview_value("secret-token-value", column="token", force=True) or "…" in mask_preview_value(
        "secret-token-value", column="token", force=True
    )
