"""Safe promotions must be expressed in the logical vocabulary Map compares.

``classify_mapping_confidence`` normalizes both sides with ``_logical_type``
(TIMESTAMP → ``datetime``), so a table entry spelled in physical types can
never match. ``("date", "timestamp")`` was such an entry: every DATE →
TIMESTAMP widening — the shape of a second sync into a destination DataFlow
itself created — was classed "weak or conflicted" and held for review.
"""

import pytest

from services.mapping_quality import (
    _SAFE_PROMOTIONS,
    _logical_type,
    classify_mapping_confidence,
)


def _classify(source_type: str, target_type: str) -> str:
    mapping = {
        "source": "hire_date",
        "target": "hire_date",
        "source_type": source_type,
        "target_type": target_type,
        "transform": "cast",
        "confidence": 0.99,
    }
    result = classify_mapping_confidence(
        mapping,
        source_profile={"samples": ["2020-01-05", "2021-06-30", "2022-03-09"]},
        destination_db_type="mongodb",
    )
    return str(result["confidence_class"])


def test_every_safe_promotion_is_reachable() -> None:
    """No entry may be spelled in a vocabulary ``_logical_type`` never emits."""
    for source_logical, target_logical in _SAFE_PROMOTIONS:
        assert _logical_type(source_logical) == source_logical
        assert _logical_type(target_logical) == target_logical


def test_date_widened_to_timestamp_is_a_promotion_not_a_conflict() -> None:
    assert _classify("DATE", "TIMESTAMP") == "safe_type_promotion"


def test_timestamp_narrowed_to_date_stays_conflicted() -> None:
    """The reverse direction drops time of day — it must keep demanding review."""
    assert _classify("TIMESTAMP", "DATE") == "weak_or_conflicted"


@pytest.mark.parametrize(
    ("source_type", "target_type"),
    [("INTEGER", "DECIMAL(12,2)"), ("VARCHAR(64)", "TEXT")],
)
def test_other_widenings_stay_promotions(source_type: str, target_type: str) -> None:
    assert _classify(source_type, target_type) == "safe_type_promotion"
