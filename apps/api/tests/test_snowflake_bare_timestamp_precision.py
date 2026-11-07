"""Snowflake's undeclared TIMESTAMP precision is nanoseconds, not "unknown".

Snowflake reports TIMESTAMP_NTZ/LTZ/TZ with no typmod but stores scale 9. The
narrowing check read the absent typmod as unknown and passed the path, so
Snowflake→MySQL into a bare ``DATETIME`` (FSP 0) was green while every
fractional second was dropped on write — silent loss on the most common
warehouse-to-OLTP column there is.

It is a declared ceiling rather than observed nanoseconds, so it accuses only
loss an operator can act on. Microsecond destinations are every mainstream
engine's ceiling, so narrowing to them is unavoidable and reporting it would put
a Risk Contract on every Snowflake timestamp column; landing at millisecond or
whole-second precision is the fixable case.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest  # noqa: E402

from services.type_system import (  # noqa: E402
    is_lossy_coercion,
    temporal_precision_would_narrow,
)


@pytest.mark.parametrize(
    "source_type,target_type,dest_db",
    [
        ("TIMESTAMP_NTZ", "DATETIME", "mysql"),
        ("TIMESTAMP_LTZ", "DATETIME", "mysql"),
        ("TIMESTAMP_TZ", "TIMESTAMP", "mysql"),
        ("TIMESTAMP_NTZ", "DATETIME(3)", "mysql"),
        ("TIMESTAMP_NTZ", "TIMESTAMP(0)", "postgresql"),
        ("TIMESTAMP_NTZ", "DATETIME2(3)", "sqlserver"),
    ],
)
def test_sub_microsecond_target_reports_the_truncation(
    source_type, target_type, dest_db
):
    assert temporal_precision_would_narrow(source_type, target_type, dest_db=dest_db)
    assert is_lossy_coercion(
        source_type, target_type, dest_db=dest_db, dest_table_exists=True
    )


@pytest.mark.parametrize(
    "source_type,target_type,dest_db",
    [
        ("TIMESTAMP_NTZ", "DATETIME(6)", "mysql"),
        ("TIMESTAMP_TZ", "TIMESTAMP(6)", "mysql"),
        ("TIMESTAMP_NTZ", "TIMESTAMP", "postgresql"),
        ("TIMESTAMP_NTZ", "DATETIME2", "sqlserver"),
        ("TIMESTAMP_TZ", "TIMESTAMP WITH TIME ZONE", "oracle"),
        ("TIMESTAMP_NTZ", "DATETIME", "bigquery"),
        ("TIMESTAMP_NTZ", "TIMESTAMP", "databricks"),
        ("TIMESTAMP_NTZ", "TIMESTAMP_NTZ", "snowflake"),
    ],
)
def test_microsecond_or_better_target_is_not_accused(source_type, target_type, dest_db):
    assert not temporal_precision_would_narrow(
        source_type, target_type, dest_db=dest_db
    )


def test_declared_nanoseconds_still_block_below_the_ceiling():
    """A source that *declares* (9) is evidence, not a dialect default."""
    assert temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(9)", "DATETIME(6)", dest_db="mysql"
    )
    assert not temporal_precision_would_narrow(
        "TIMESTAMP_NTZ(6)", "DATETIME(6)", dest_db="mysql"
    )


def test_postgres_timestamptz_keeps_its_own_default():
    """``TIMESTAMPTZ`` is PostgreSQL's spelling — microseconds, not Snowflake ns."""
    assert not temporal_precision_would_narrow(
        "TIMESTAMPTZ", "TIMESTAMP(0)", dest_db="postgresql"
    )
