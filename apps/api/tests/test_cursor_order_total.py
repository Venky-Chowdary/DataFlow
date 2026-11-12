"""Audit M4 — the cursor comparator is a total order.

It used to infer a type from the two values in hand, so ``2 < 10`` (integers),
``10 < 1a`` (text) and ``1a < 2`` (text) all held at once: a batch's maximum
depended on the order its rows arrived in, and a 10-digit integer compared
above a 13-digit one because both were read as epochs.
"""

from __future__ import annotations

import itertools

from hypothesis import given, settings
from hypothesis import strategies as st

from services.cursor_order import compare_values, max_value, order_key
from services.sync_cursor import (
    KEYSET_SEP,
    advance_stored_cursor,
    compare_cursor_values,
    max_cursor_value,
)

_VALUE = st.one_of(
    st.integers(min_value=-10**15, max_value=10**15).map(str),
    st.decimals(allow_nan=False, allow_infinity=False, places=3,
                min_value=-10**6, max_value=10**6).map(str),
    st.datetimes().map(lambda d: d.isoformat()),
    st.datetimes().map(lambda d: d.replace(microsecond=0).isoformat() + "Z"),
    st.datetimes().map(lambda d: d.replace(microsecond=0).isoformat() + "-04:00"),
    st.dates().map(lambda d: d.isoformat()),
    st.text(alphabet="019aAzZ-_/. ", min_size=1, max_size=8).filter(lambda s: s.strip()),
    st.sampled_from(["2", "10", "1a", "9999999999", "1000000000000", "01", "1.0", "1e3"]),
)


def test_the_audit_cycle_is_gone():
    assert compare_cursor_values("2", "10") == -1
    assert compare_cursor_values("10", "1a") == -1
    assert compare_cursor_values("2", "1a") == -1  # was +1: 2 < 10 < 1a < 2
    for a, b in itertools.permutations(["2", "10", "1a"], 2):
        assert compare_cursor_values(a, b) == -compare_cursor_values(b, a)


def test_a_batch_maximum_does_not_depend_on_row_order():
    marks = {
        max_cursor_value([[v] for v in perm], ["c"], "c")
        for perm in itertools.permutations(["2", "10", "1a"])
    }
    assert marks == {"1a"}


def test_digits_are_integers_not_epochs():
    assert compare_cursor_values("9999999999", "1000000000000") == -1
    assert max_value(["9999999999", "1000000000000"]) == "1000000000000"


def test_instants_still_compare_by_instant_whatever_the_offset():
    assert compare_cursor_values("2024-01-01T05:30:00-04:00", "2024-01-01T09:00:00Z") == 1
    assert compare_cursor_values("2026-01-01T02:45:00-08:00", "2026-01-01T16:15:00+05:30") == 0


def test_a_missing_tiebreak_ranks_below_every_key():
    for key in ("2", "true", "abc", "2024-01-01"):
        assert compare_cursor_values("5" + KEYSET_SEP, "5" + KEYSET_SEP + key) == -1
    rows = [["2024-01-01", ""], ["2024-01-01", "2"], ["2024-01-01", "1"]]
    assert max_cursor_value(rows, ["c", "id"], "c", "id") == "2024-01-01" + KEYSET_SEP + "2"


def test_composite_watermarks_order_by_cursor_then_typed_tiebreak():
    low = "5" + KEYSET_SEP + "999"
    high = "5" + KEYSET_SEP + "2000"
    assert compare_cursor_values(low, high) == -1
    assert advance_stored_cursor(low, high) == (high, True)
    assert advance_stored_cursor(high, low) == (high, False)


@settings(max_examples=400, deadline=None)
@given(_VALUE, _VALUE, _VALUE)
def test_cursor_compare_is_transitive(a, b, c):
    ab, bc, ac = compare_values(a, b), compare_values(b, c), compare_values(a, c)
    if ab <= 0 and bc <= 0:
        assert ac <= 0, (a, b, c, order_key(a), order_key(b), order_key(c))
    if ab >= 0 and bc >= 0:
        assert ac >= 0, (a, b, c)


@settings(max_examples=400, deadline=None)
@given(_VALUE, _VALUE)
def test_cursor_compare_is_antisymmetric_and_matches_the_stored_cursor(a, b):
    assert compare_values(a, b) == -compare_values(b, a)
    assert compare_values(a, a) == 0
    assert compare_cursor_values(a, b) == compare_values(a, b)


@settings(max_examples=200, deadline=None)
@given(st.lists(_VALUE, min_size=1, max_size=6))
def test_max_is_the_same_for_every_arrival_order(values):
    expected = order_key(max_value(values))
    for perm in itertools.islice(itertools.permutations(values), 24):
        got = max_cursor_value([[v] for v in perm], ["c"], "c")
        assert order_key(got) == expected, (perm, got)
