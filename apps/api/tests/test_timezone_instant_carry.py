"""Session-independent instant: MySQL TIMESTAMP polarity, not session wall-clock.

AWS DMS copies TIMESTAMP digits under whatever session time_zone the server
happens to run. Checksums of those digits stay green while the instant moved
by hours (GMT vs BST, Asia/Calcutta). DataFlow pins both ends to UTC and
stamps TIMESTAMP cells as instants on the wire so dest TIMESTAMPTZ does not
treat naive UTC digits as wall-clock. DATETIME stays zoneless.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from connectors.generic_sql import _to_sa_value
from connectors.sql_temporal import coerce_sql_temporal
from services.timezone_policy import (
    MYSQL_UTC_PIN_SQL,
    mysql_timestamp_instant_wire,
    pin_mysql_session_utc,
)
from services.value_serializer import cell_to_string

IST = timezone(timedelta(hours=5, minutes=30))
WIRE_IST = "2024-03-01T12:00:00+05:30"
UTC_NAIVE = datetime(2024, 3, 1, 6, 30)


def test_pin_mysql_session_utc_sets_plus_zero():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value = cur

    pin_mysql_session_utc(conn)

    cur.execute.assert_called_once_with(MYSQL_UTC_PIN_SQL)
    cur.close.assert_called_once()


def test_mysql_timestamp_instant_wire_attaches_utc_without_shifting():
    wired = mysql_timestamp_instant_wire(UTC_NAIVE)
    assert wired.tzinfo is not None
    assert wired.utcoffset() == timedelta(0)
    assert wired.replace(tzinfo=None) == UTC_NAIVE
    text = cell_to_string(wired, preserve_sql_null=True)
    assert "+00:00" in text or text.endswith("Z")
    assert "06:30" in text


def test_datetime_wall_clock_is_not_an_instant_wire():
    # DATETIME callers must not go through mysql_timestamp_instant_wire.
    wall = datetime(2024, 3, 1, 12, 0, 0)
    text = cell_to_string(wall, preserve_sql_null=True)
    assert "+00:00" not in text
    assert not text.endswith("Z")
    assert "12:00" in text


def test_already_offset_wire_is_left_as_instant():
    assert mysql_timestamp_instant_wire(WIRE_IST) == WIRE_IST


def test_generic_sql_mysql_timestamp_converts_offset_to_utc():
    import sqlalchemy as sa

    out = _to_sa_value(
        WIRE_IST,
        "TIMESTAMP(6)",
        sa_type=sa.TIMESTAMP(),
        dialect_name="mysql",
        db_type="mysql",
    )
    assert out == UTC_NAIVE
    assert out.tzinfo is None


def test_generic_sql_collapsed_datetime_logical_still_uses_physical_timestamp():
    """Map logical 'datetime' must not strip a MySQL TIMESTAMP instant."""
    import sqlalchemy as sa

    out = _to_sa_value(
        WIRE_IST,
        "datetime",
        sa_type=sa.TIMESTAMP(),
        dialect_name="mysql",
        db_type="mysql",
    )
    assert out == UTC_NAIVE
    assert out.tzinfo is None


def test_naive_pg_timestamp_is_not_utc_shifted():
    out = coerce_sql_temporal(WIRE_IST, "TIMESTAMP", engine="postgresql")
    assert out.hour == 12
    assert out.tzinfo is None
