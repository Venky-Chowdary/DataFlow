"""Create-new / invent must not invent extra dest scale on any engine.

Named flights-clock fixture (JURTY DEP_TIME / ARR_TIME). Trailing zeros
after the decimal do not change the value, but NUMBER(15,11) /
9.083333000000 is still a critical invent bug: dest scale must equal the
significant observed scale. One algorithm — Snowflake, Postgres, MySQL,
BigQuery. Not a 45-connector certification.
"""

from __future__ import annotations

import pytest

from connectors.snowflake_writer import _snowflake_decimal_type
from services.decimal_observe import (
    CREATE_NEW_NUMERIC_SAFETY_MARGIN,
    create_new_decimal_carrier,
    exact_create_decimal_ps,
    fractional_trailing_zeros_same_value,
    observe_numeric_samples,
    observe_source_numeric_samples,
)
from services.type_system import (
    create_new_mapping_target_type,
    parse_numeric_precision_scale,
)

# Production flights-1m.csv clock cells that printed 9.083333000000
# when invent added +2 scale (and the Snowflake writer added another +2).
FLIGHTS_CLOCK = ("9.083333", "12.483334", "7.9166665", "8.95", "12")
# 7.9166665 needs scale 7. Invent must be 7, not 9 / 11 / 12.
FLIGHTS_CLOCK_SCALE = 7
FLIGHTS_CLOCK_INT = 2  # 12.483334

DEST_ENGINES = (
    ("snowflake", "NUMBER"),
    ("postgresql", "NUMERIC"),
    ("mysql", "DECIMAL"),
    ("bigquery", "BIGNUMERIC"),
)


def test_safety_margin_constant_is_zero():
    assert CREATE_NEW_NUMERIC_SAFETY_MARGIN == 0


def test_exact_ps_is_observed_digits_only():
    p, s = exact_create_decimal_ps(2, 6)
    assert (p, s) == (8, 6)
    p2, s2 = exact_create_decimal_ps(2, 7)
    assert (p2, s2) == (9, 7)
    # Explicit margin is opt-in only — product default never uses it.
    p3, s3 = exact_create_decimal_ps(2, 6, safety_margin=2)
    assert s3 == 8


def test_flights_clock_observe_matches_source_and_invent():
    source = observe_source_numeric_samples(list(FLIGHTS_CLOCK))
    invent = observe_numeric_samples(list(FLIGHTS_CLOCK))
    assert source["kind"] == invent["kind"] == "fixed_decimal"
    assert source["scale"] == invent["scale"] == FLIGHTS_CLOCK_SCALE
    assert invent["carrier"] == f"DECIMAL({FLIGHTS_CLOCK_INT + FLIGHTS_CLOCK_SCALE},{FLIGHTS_CLOCK_SCALE})"
    assert create_new_decimal_carrier(list(FLIGHTS_CLOCK)) == invent["carrier"]


@pytest.mark.parametrize("dest,token", DEST_ENGINES)
def test_flights_clock_create_new_scale_is_exact_on_every_engine(dest: str, token: str):
    stamped = create_new_mapping_target_type(
        "DECIMAL", dest, samples=list(FLIGHTS_CLOCK)
    )
    p, s = parse_numeric_precision_scale(stamped)
    assert s == FLIGHTS_CLOCK_SCALE, (dest, stamped)
    assert p is not None and p - s >= FLIGHTS_CLOCK_INT, (dest, stamped)
    assert token in stamped.upper(), (dest, stamped)
    # The production lie — extra dest zeros — must not be re-invented.
    assert s != 11
    assert s != 12


def test_snowflake_writer_batch_invent_is_exact_envelope():
    rows = [(v,) for v in FLIGHTS_CLOCK]
    typ = _snowflake_decimal_type(0, rows)
    p, s = parse_numeric_precision_scale(typ)
    assert typ.startswith("NUMBER(")
    assert s == FLIGHTS_CLOCK_SCALE, typ
    assert p is not None and p - s >= FLIGHTS_CLOCK_INT


def test_padding_zeros_are_the_same_value_but_must_not_be_invented():
    assert fractional_trailing_zeros_same_value("9.083333", "9.083333000000")
    assert fractional_trailing_zeros_same_value("12.483334", "12.483334000000")
    stamped = create_new_mapping_target_type(
        "DECIMAL", "snowflake", samples=["9.083333"]
    )
    _p, s = parse_numeric_precision_scale(stamped)
    assert s == 6, stamped
