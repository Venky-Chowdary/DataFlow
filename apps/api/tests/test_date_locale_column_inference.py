"""A column's date ordering is settled by the column, not by each cell alone.

``12/31/2024`` can only be MDY, but ``5/8/1967`` beside it could be either, and a
cell judged on its own becomes VARCHAR. One such cell made the column mixed, so
the destination got a text column even though the write path went on to parse
every value as MDY correctly — the stored type and the stored values disagreed.

The ordering must still come from evidence: a column whose slash-dates are all
ambiguous has nothing to resolve and stays text rather than guessing.
"""

from __future__ import annotations

import pytest

from services.schema_inference import infer_column


def _logical(samples: list[str]) -> str:
    return str(infer_column(samples, field_name="birth_date").get("logical_type") or "")


@pytest.mark.parametrize(
    "samples",
    [
        # One unambiguous MDY member settles the ambiguous ones.
        ["12/31/2024", "5/8/1967", "7/9/1982"],
        # …and the same holds for DMY: 31 cannot be a month.
        ["31/12/2024", "8/5/1967"],
        # Unambiguous alone, and ISO, were already fine.
        ["12/31/2024"],
        ["2024-12-31", "1967-05-08"],
    ],
)
def test_columns_with_resolvable_ordering_are_dates(samples):
    assert _logical(samples) == "DATE", samples


@pytest.mark.parametrize(
    "samples",
    [
        # Every member is ambiguous — no evidence picks MDY over DMY.
        ["5/8/1967", "7/9/1982"],
        # A genuine non-date member keeps the column text.
        ["12/31/2024", "not a date", "7/9/1982"],
        ["5/8", "abc"],
    ],
)
def test_columns_without_evidence_stay_text(samples):
    assert _logical(samples) != "DATE", samples


def test_explicit_transfer_locale_still_wins():
    """An operator-declared ordering is not overridden by sample evidence."""
    from services.transform_engine import reset_active_date_locale, set_active_date_locale

    # Samples read as DMY on their own (31 cannot be a month).
    samples = ["31/12/2024", "8/5/1967"]
    token = set_active_date_locale("MDY")
    try:
        assert _logical(samples) == "DATE"
    finally:
        reset_active_date_locale(token)


def test_resolution_does_not_leak_between_columns():
    """The ordering is scoped to the column that proved it."""
    from services.transform_engine import _active_date_locale

    assert _logical(["12/31/2024", "5/8/1967"]) == "DATE"
    assert _active_date_locale() == ""
    # A later ambiguous-only column must not inherit the previous column's MDY.
    assert _logical(["5/8/1967", "7/9/1982"]) != "DATE"
