"""Audit M4 — the cursor comparator is a total order.

It used to infer a type from the two values in hand, so ``2 < 10`` (integers),
``10 < 1a`` (text) and ``1a < 2`` (text) all held at once: a batch's maximum
depended on the order its rows arrived in, and a 10-digit integer compared
above a 13-digit one because both were read as epochs.
"""

from __future__ import annotations

import itertools

from services.cursor_order import compare_values, max_value, order_key
from services.sync_cursor import (
    KEYSET_SEP,
    advance_stored_cursor,
    compare_cursor_values,
    max_cursor_value,
)

# One value of every class the comparator distinguishes, plus the audit's
# cycle and the epoch-width pair. The exhaustive checks below run in every
# environment; ``test_cursor_order_total_property.py`` widens the inputs
# with hypothesis where it is installed.
_CORPUS = [
    "", "2", "10", "1a", "01", "-7", "0", "9999999999", "1000000000000",
    "1.0", "1.5", "-0.25", "1e3",
    "2024-01-01", "2024-01-01T09:00:00Z", "2024-01-01T05:30:00-04:00",
    "2024-01-01T09:00:00", "2024-01-01T09:00:00.123456", "2026-01-01T16:15:00+05:30",
    "abc", "ABC", "a-b", "z", "Z_9", "1/2", ". ",
]


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


def test_cursor_compare_is_a_total_order_on_the_corpus():
    for a, b in itertools.product(_CORPUS, repeat=2):
        assert compare_values(a, b) == -compare_values(b, a), (a, b)
        assert compare_cursor_values(a, b) == compare_values(a, b), (a, b)
    for a, b, c in itertools.product(_CORPUS, repeat=3):
        ab, bc, ac = compare_values(a, b), compare_values(b, c), compare_values(a, c)
        if ab <= 0 and bc <= 0:
            assert ac <= 0, (a, b, c, order_key(a), order_key(b), order_key(c))
        if ab >= 0 and bc >= 0:
            assert ac >= 0, (a, b, c)


def test_max_is_the_same_for_every_arrival_order_on_the_corpus():
    values = [v for v in _CORPUS if v]
    for window in range(0, len(values) - 3):
        chunk = values[window:window + 4]
        expected = order_key(max_value(chunk))
        for perm in itertools.permutations(chunk):
            got = max_cursor_value([[v] for v in perm], ["c"], "c")
            assert order_key(got) == expected, (perm, got)
