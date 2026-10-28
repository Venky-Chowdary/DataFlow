"""D52 — the DST-gap refusal's headline failure class was UNKNOWN.

``Wall clock '...' does not exist in timezone '...'`` (a spring-forward
gap) matched none of the classifier's timestamp phrases, so Validate's
headline said UNKNOWN beside a perfectly specific cause.
"""

from __future__ import annotations

from services.decision_kernel.findings import FailureClass, classify_transform_failure
from services.transform_engine import apply_transform


def test_dst_gap_refusal_is_an_invalid_timestamp():
    value, error = apply_transform(
        "2024-03-10 02:30:00", "assume_timezone:America/New_York"
    )
    assert error and "does not exist in timezone" in error, (value, error)
    assert classify_transform_failure(
        error, source_type="VARCHAR", target_type="TIMESTAMP"
    ) is FailureClass.INVALID_TIMESTAMP
