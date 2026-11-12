"""Audit M4 — the cursor comparator is a total order (hypothesis).

The deterministic checks live in ``test_cursor_order_total.py`` and run
without hypothesis; this module only widens the inputs.
"""

from __future__ import annotations

import itertools

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from services.cursor_order import compare_values, max_value, order_key  # noqa: E402
from services.sync_cursor import compare_cursor_values, max_cursor_value  # noqa: E402

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
