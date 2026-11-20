"""D403 edge cases for the CDC proof of a DATETIMEOFFSET written to a MySQL
``VARCHAR(64)`` carrier (PR fix cf2b30f1b, ``stamp_cdc_instant_carriers``).

MySQL has no type that keeps an offset: ``DATETIME`` has no zone and
``TIMESTAMP`` converts to UTC and ends in 2038 (MySQL 8 reference, "The DATE,
DATETIME, and TIMESTAMP Types"), so an offset-pinned instant lands as ISO text
(``type_system.MYSQL_OFFSET_TEXT_DDL``). The proof must compare instants on
that carrier, and must still fail a real difference: another instant, a lost
seventh fractional digit, or an aware instant written as a naive wall clock.
SQL Server is not available locally: the source cells are the values the
driver hands over (aware ``datetime`` or ISO text).
"""

from __future__ import annotations

import datetime as dt

import pytest

from services.cdc_value_digest import compare_row_sets, stamp_cdc_instant_carriers

UTC = dt.timezone.utc
PLUS2 = dt.timezone(dt.timedelta(hours=2))
AWARE = dt.datetime(2024, 11, 3, 5, 37, tzinfo=UTC)


def _types():
    return stamp_cdc_instant_carriers(
        {"tstz": "VARCHAR(64)", "id": "INT"},
        [{"source": "tstz", "target": "tstz", "source_type": "datetimeoffset(7)",
          "target_type": "VARCHAR(64)"}],
    )


def _missing(source, dest) -> int:
    return compare_row_sets(
        [{"id": 1, "tstz": source}], [{"id": 1, "tstz": dest}], ["id", "tstz"],
        engine="mysql", dest_types=_types(),
    ).missing


@pytest.mark.parametrize(
    "source,dest",
    [
        (AWARE, "2024-11-03T05:37:00Z"),
        (AWARE, "2024-11-03 05:37:00+00:00"),
        (AWARE, "2024-11-03T07:37:00+02:00"),
        (dt.datetime(2024, 11, 3, 7, 37, tzinfo=PLUS2), "2024-11-03T05:37:00Z"),
        ("2024-11-03 05:37:00.1234567 +00:00", "2024-11-03T05:37:00.1234567Z"),
        (None, None),
    ],
)
def test_the_same_instant_matches(source, dest):
    assert _missing(source, dest) == 0


@pytest.mark.parametrize(
    "source,dest",
    [
        (AWARE, "2024-11-03T06:37:00Z"),  # another instant
        (AWARE, "2024-11-03 05:37:00"),  # aware written as a naive wall clock
        ("2024-11-03 05:37:00.1234567 +00:00", "2024-11-03T05:37:00Z"),  # digits lost
        (AWARE, None),
    ],
)
def test_a_real_difference_still_fails(source, dest):
    assert _missing(source, dest) == 1


def test_a_text_source_is_not_folded_as_an_instant():
    types = stamp_cdc_instant_carriers(
        {"tstz": "VARCHAR(64)"},
        [{"source": "tstz", "target": "tstz", "source_type": "varchar(30)",
          "target_type": "VARCHAR(64)"}],
    )
    assert types == {"tstz": "VARCHAR(64)"}
