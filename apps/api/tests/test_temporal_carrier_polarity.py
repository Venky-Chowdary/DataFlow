"""The DDL carrier must keep TZ polarity *and* fractional-second precision.

``ddl_carrier_type`` used to fold every aware datetime onto one ``TIMESTAMPTZ``
token and drop ``(p)``. Two live SQL Server → Oracle defects came out of that:

* ``DATETIMEOFFSET(6)`` (offset-pinned) invented Oracle ``TIMESTAMP WITH LOCAL
  TIME ZONE``, which normalises to the database timezone and stores no offset;
* ``TIMESTAMP_NTZ(6)`` invented a bare ``TIMESTAMP``, which the coercion
  validator then read back against the declaration as a precision collapse and
  blocked the route at Validate.
"""

from __future__ import annotations

import pytest

from services.decision_kernel import (
    create_new_mapping_target_type,
    is_lossy_coercion,
)
from services.type_system import datetime_timezone_polarity, ddl_carrier_type


@pytest.mark.parametrize(
    "declared,carrier,polarity",
    [
        ("DATETIMEOFFSET(6)", "TIMESTAMP_TZ(6)", "tz"),
        ("DATETIMEOFFSET", "TIMESTAMP_TZ", "tz"),
        ("TIMESTAMP(6) WITH TIME ZONE", "TIMESTAMP_TZ(6)", "tz"),
        ("TIMESTAMP_TZ(6)", "TIMESTAMP_TZ(6)", "tz"),
        # Session-relative wires stay session-relative.
        ("TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMPTZ", "ltz"),
        ("TIMESTAMP_LTZ", "TIMESTAMPTZ", "ltz"),
        ("TIMESTAMPTZ", "TIMESTAMPTZ", "ltz"),
        ("TIMESTAMPTZ(3)", "TIMESTAMPTZ(3)", "ltz"),
        # Naive wires keep their declared precision.
        ("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(6)", "ntz"),
        ("DATETIME2(7)", "TIMESTAMP_NTZ(7)", "ntz"),
    ],
)
def test_carrier_keeps_polarity_and_precision(
    declared: str, carrier: str, polarity: str
) -> None:
    assert ddl_carrier_type(declared) == carrier
    assert datetime_timezone_polarity(carrier) == polarity


@pytest.mark.parametrize(
    "dest,physical",
    [
        ("oracle", "TIMESTAMP(6) WITH TIME ZONE"),
        ("postgresql", "TIMESTAMPTZ(6)"),
        ("mssql", "DATETIMEOFFSET(6)"),
        ("snowflake", "TIMESTAMP_TZ(6)"),
    ],
)
def test_offset_pinned_source_never_invents_session_relative(
    dest: str, physical: str
) -> None:
    carrier = ddl_carrier_type("DATETIMEOFFSET(6)")
    target = create_new_mapping_target_type(carrier, dest)
    assert target == physical
    assert is_lossy_coercion("DATETIMEOFFSET(6)", target, dest_db=dest) is False


@pytest.mark.parametrize(
    "dest,physical",
    [
        ("oracle", "TIMESTAMP(6)"),
        ("postgresql", "TIMESTAMP(6)"),
        ("mssql", "DATETIME2(6)"),
    ],
)
def test_naive_microsecond_source_keeps_precision(dest: str, physical: str) -> None:
    carrier = ddl_carrier_type("TIMESTAMP_NTZ(6)")
    target = create_new_mapping_target_type(carrier, dest)
    assert target == physical
    assert is_lossy_coercion("TIMESTAMP_NTZ(6)", target, dest_db=dest) is False
