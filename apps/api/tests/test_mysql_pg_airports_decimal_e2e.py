"""Client regression: MySQL→Postgres airports lat must not invent-quarantine.

Real failure: 30/30 rows held out with
``decimal does not fit PostgreSQL NUMERIC(38,9)`` for values like
``52.310500000000000``. Trailing wire zeros + PG scale-round must pass
quarantine AND bind (quarantine≡bind). Integer overflow still fail-closed.
"""

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.sql_bind import normalize_sql_bind_value  # noqa: E402
from connectors.writer_common import (  # noqa: E402
    bind_sql_mapped_rows_with_quarantine,
    quarantine_unfit_decimals,
)


# Sample of the client paste (MySQL DECIMAL/DOUBLE wire padding).
_AIRPORT_LATS = [
    "52.310500000000000",
    "33.640700000000000",
    "13.690000000000000",
    "41.297400000000000",
    "40.080100000000000",
    "41.974200000000000",
    "32.899800000000000",
    "39.856100000000000",
    "25.252800000000000",
    "50.037900000000000",
    "23.392400000000000",
    "22.308000000000000",
    "41.275300000000000",
    "2.745600000000000",
    "51.470000000000000",
    "33.942500000000000",
    "40.498300000000000",
    "19.436100000000000",
    "25.795900000000000",
    "48.353700000000000",
    "40.641300000000000",
    "49.009700000000000",
    "37.621300000000000",
    "37.460200000000000",
    "31.144300000000000",
    "1.364400000000000",
    "-33.939900000000000",
    "35.549400000000000",
    "35.772000000000000",
    "43.677700000000000",
]


def test_airports_lat_survives_pg_quarantine_and_bind():
    rows = [(lat,) for lat in _AIRPORT_LATS]
    details: list[dict] = []
    after_q = quarantine_unfit_decimals(
        rows,
        ["lat"],
        ["NUMERIC(38,9)"],
        details,
        policy="quarantine",
        dialect_label="PostgreSQL NUMERIC",
        dest_db="postgresql",
    )
    assert len(after_q) == 30, details
    assert details == []

    after_bind = bind_sql_mapped_rows_with_quarantine(
        after_q,
        ["lat"],
        ["NUMERIC(38,9)"],
        details,
        "quarantine",
        engine="postgresql",
        dialect_label="PostgreSQL",
    )
    assert len(after_bind) == 30, details
    assert details == []
    assert all(isinstance(r[0], Decimal) for r in after_bind)


def test_airports_lat_normalize_bind_direct():
    for lat in _AIRPORT_LATS[:5]:
        bound = normalize_sql_bind_value(lat, "NUMERIC(38,9)", engine="postgresql")
        assert isinstance(bound, Decimal)
        # Trailing zeros must not invent overflow.
        assert bound == Decimal(lat)


def test_pg_integer_overflow_still_blocks():
    details: list[dict] = []
    huge = "9" * 40  # exceeds NUMERIC(38,9) integer capacity
    out = quarantine_unfit_decimals(
        [(huge,)],
        ["lat"],
        ["NUMERIC(38,9)"],
        details,
        policy="quarantine",
        dialect_label="PostgreSQL NUMERIC",
        dest_db="postgresql",
    )
    assert out == []
    assert details
