"""Hypothesis sweep of the canonical carrier properties.

The deterministic cases live in ``test_temporal_audit_canonical_carriers.py``
and run without hypothesis; this module only widens the inputs.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import pytest

hypothesis = pytest.importorskip("hypothesis")
pytest.importorskip("sqlalchemy")
from hypothesis import given, settings  # noqa: E402
from hypothesis import strategies as st  # noqa: E402

from tests.test_temporal_audit_canonical_carriers import (  # noqa: E402
    check_date_only_column_stays_a_date,
    check_es_decimal_binds_fixed_point_text,
    check_one_real_time_of_day_makes_a_timestamp,
    check_sqlserver_aware_bind_is_the_same_instant,
    check_typed_decimal_fit_is_exact,
)

_STAMPS = st.datetimes(
    min_value=datetime(1900, 1, 1), max_value=datetime(2200, 12, 31)
).map(lambda d: d.replace(microsecond=0))


@settings(max_examples=60, deadline=None)
@given(
    midnights=st.lists(st.dates(min_value=date(1900, 1, 1)), min_size=1, max_size=6),
    stamp=_STAMPS.filter(lambda d: d.time() != datetime.min.time()),
)
def test_one_real_time_of_day_makes_the_column_a_timestamp(midnights, stamp):
    check_one_real_time_of_day_makes_a_timestamp(midnights, stamp)


@settings(max_examples=30, deadline=None)
@given(days=st.lists(st.dates(min_value=date(1900, 1, 1)), min_size=1, max_size=6))
def test_a_date_only_column_stays_a_date(days):
    check_date_only_column_stays_a_date(days)


@settings(max_examples=60, deadline=None)
@given(stamp=_STAMPS, minutes=st.integers(min_value=-14 * 60, max_value=14 * 60))
def test_sqlserver_aware_bind_is_the_same_instant(stamp, minutes):
    check_sqlserver_aware_bind_is_the_same_instant(stamp, minutes)


@settings(max_examples=200, deadline=None)
@given(
    unscaled=st.integers(min_value=-(10**17), max_value=10**17),
    scale=st.integers(min_value=0, max_value=10),
)
def test_es_decimal_binds_fixed_point_text_with_scale(unscaled, scale):
    check_es_decimal_binds_fixed_point_text(Decimal(unscaled).scaleb(-scale))


@settings(max_examples=200, deadline=None)
@given(
    unscaled=st.integers(min_value=-(10**9), max_value=10**9),
    scale=st.integers(min_value=0, max_value=6),
)
def test_typed_decimal_fit_is_exact_never_a_locale_reparse(unscaled, scale):
    check_typed_decimal_fit_is_exact(Decimal(unscaled).scaleb(-scale))
