"""A verifier that cannot align rows must say so, never call the data corrupt.

This pins the *class* behind two field reports from one keyless 1M-row CSV load,
not the two instances. The product told the operator their data was corrupt
because its own comparison was invalid, and then offered an action that could
not work. Either half destroys trust faster than the underlying bug.

The invariant: a read-back sample may only report a mismatch when it can prove
which destination row corresponds to which source row. Anything else is
declined, with a reason, and does not fail the transfer.
"""

from __future__ import annotations

import pytest

from services.reconciliation import reconcile, sample_compare_rows

MAPPINGS = [
    {"source": "col1", "target": "col1"},
    {"source": "col2", "target": "col2"},
]


def _rows(pairs):
    return [{"col1": a, "col2": b} for a, b in pairs]


def test_repeating_key_is_declined_not_reported_as_corruption():
    """The reported shape: a region column used as identity."""
    source = _rows([("Asia", "2"), ("Asia", "6"), ("Asia", "10")])
    target = _rows([("Asia", "162"), ("Asia", "2"), ("Asia", "6")])
    result = sample_compare_rows(source, target, MAPPINGS, sort_key="col1")
    assert result["passed"] is True
    assert result["alignment"] == "declined"
    assert result["compared"] == 0
    assert "col1" in result["reason"]


def test_no_key_at_all_is_declined():
    source = _rows([("Asia", "2")])
    target = _rows([("Asia", "999")])
    result = sample_compare_rows(source, target, MAPPINGS)
    assert result["passed"] is True
    assert result["alignment"] == "declined"


def test_declined_sample_does_not_fail_reconciliation():
    """The end the operator sees: a correct transfer must not read as failed."""
    declined = sample_compare_rows(
        _rows([("Asia", "2"), ("Asia", "6")]),
        _rows([("Asia", "6"), ("Asia", "2")]),
        MAPPINGS,
        sort_key="col1",
    )
    report = reconcile(
        source_rows=2,
        target_rows=2,
        source_checksum="abc",
        target_checksum="abc",
        sample_compare=declined,
    )
    assert report.passed is True
    assert "verification failed" not in (report.message or "").lower()


def test_a_unique_key_still_detects_a_real_mismatch():
    """Declining must not cost real detection where alignment is sound."""
    source = _rows([("a", "1"), ("b", "2")])
    target = _rows([("a", "1"), ("b", "999")])
    result = sample_compare_rows(source, target, MAPPINGS, sort_key="col1")
    assert result["alignment"] == "keyed"
    assert result["passed"] is False
    assert result["mismatches"]


def test_a_unique_key_still_passes_matching_data():
    source = _rows([("a", "1"), ("b", "2")])
    target = _rows([("b", "2"), ("a", "1")])
    result = sample_compare_rows(source, target, MAPPINGS, sort_key="col1")
    assert result["alignment"] == "keyed"
    assert result["passed"] is True
    # Counted per compared cell: two rows across two mapped columns.
    assert result["compared"] == 4


def test_key_missing_from_the_readback_window_is_skipped_not_mismatched():
    """A key outside the sampled window is a scope miss, not a wrong value."""
    source = _rows([("a", "1"), ("z", "26")])
    target = _rows([("a", "1")])
    result = sample_compare_rows(source, target, MAPPINGS, sort_key="col1")
    assert result["passed"] is True
    # Only the row whose key was in the window: one row, two mapped columns.
    assert result["compared"] == 2
    assert result["mismatches"] == []


def test_index_pairing_requires_the_caller_to_vouch_for_it():
    """Positional pairing is opt-in, because Gate-8 cannot honestly claim it."""
    source = _rows([("a", "1")])
    target = _rows([("b", "2")])

    assert sample_compare_rows(source, target, MAPPINGS)["alignment"] == "declined"

    paired = sample_compare_rows(source, target, MAPPINGS, rows_are_paired=True)
    assert paired["alignment"] == "paired_by_caller"
    assert paired["passed"] is False


@pytest.mark.parametrize(
    "sync_mode,expected_restart",
    [
        ("full_refresh_overwrite", True),
        ("full_refresh_append", False),
        ("incremental_append", False),
        ("upsert", False),
    ],
)
def test_resume_offers_an_action_the_operator_can_take(
    sync_mode: str, expected_restart: bool
):
    """A full refresh reloads; only append-class modes need a key to continue."""
    from src.routers.connectors_router import _resume_restarts_from_scratch
    from src.transfer.models import EndpointConfig, TransferRequest

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(kind="database", format="postgresql"),
        sync_mode=sync_mode,
    )
    assert _resume_restarts_from_scratch(request) is expected_restart
