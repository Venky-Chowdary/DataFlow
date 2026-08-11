"""PostgreSQL → MySQL timezone policy: one named contract, no blanket block.

The rule under test: MySQL ``TIMESTAMP`` is a UTC instant carrier (epoch-bounded),
MySQL ``DATETIME`` is a zoneless wall clock that can only carry an instant under an
explicit UTC-normalize contract. Validate and Execute must read the same policy, and
an out-of-range instant must be held out rather than zeroed by MySQL.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from connectors.sql_temporal import coerce_sql_temporal, wire_check_temporal
from services.timezone_policy import (
    MYSQL_TIMESTAMP_MAX,
    MYSQL_TIMESTAMP_MIN,
    POLICY_NATIVE_INSTANT,
    POLICY_OFFSET_PRESERVED,
    POLICY_UTC_INVENT,
    POLICY_UTC_NORMALIZED,
    POLICY_WALL_CLOCK_LOCAL,
    mysql_timestamp_out_of_range,
    resolve_timezone_policy,
)


# --- policy resolution ------------------------------------------------------


def test_pg_timestamptz_to_mysql_timestamp_is_a_native_instant_carrier() -> None:
    policy = resolve_timezone_policy("TIMESTAMPTZ", "TIMESTAMP(6)", dest_db="mysql")
    assert policy is not None
    assert policy.policy == POLICY_NATIVE_INSTANT
    assert policy.instant_preserved is True
    # PostgreSQL TIMESTAMPTZ never stored the label, so nothing is lost here.
    assert policy.offset_label_preserved is False
    assert policy.requires_contract is False
    assert "2038" in policy.range_limit


def test_pg_timestamptz_to_mysql_datetime_needs_a_utc_normalize_contract() -> None:
    policy = resolve_timezone_policy("TIMESTAMPTZ", "DATETIME(6)", dest_db="mysql")
    assert policy is not None
    assert policy.policy == POLICY_UTC_NORMALIZED
    assert policy.instant_preserved is True
    assert policy.requires_contract is True
    assert "TIMESTAMP(6)" in policy.remediation


def test_naive_source_into_aware_mysql_carrier_is_a_utc_invent() -> None:
    policy = resolve_timezone_policy("TIMESTAMP", "TIMESTAMP(6)", dest_db="mysql")
    assert policy is not None
    assert policy.policy == POLICY_UTC_INVENT
    assert policy.instant_preserved is False
    assert policy.requires_contract is True


def test_naive_to_naive_keeps_wall_clock_without_a_contract() -> None:
    policy = resolve_timezone_policy("TIMESTAMP", "DATETIME(6)", dest_db="mysql")
    assert policy is not None
    assert policy.policy == POLICY_WALL_CLOCK_LOCAL
    assert policy.requires_contract is False


def test_offset_pinned_pair_preserves_the_label() -> None:
    policy = resolve_timezone_policy(
        "DATETIMEOFFSET", "DATETIMEOFFSET", dest_db="sqlserver"
    )
    assert policy is not None
    assert policy.policy == POLICY_OFFSET_PRESERVED
    assert policy.offset_label_preserved is True


def test_non_temporal_pairs_raise_no_timezone_question() -> None:
    assert resolve_timezone_policy("VARCHAR(10)", "TEXT", dest_db="mysql") is None


def test_policy_is_identical_for_validate_and_execute_call_shapes() -> None:
    # Same inputs must resolve identically regardless of dialect alias spelling.
    a = resolve_timezone_policy("TIMESTAMPTZ", "TIMESTAMP(6)", dest_db="mysql")
    b = resolve_timezone_policy("TIMESTAMPTZ", "TIMESTAMP(6)", dest_db="mariadb")
    assert a is not None and b is not None
    assert a.as_dict() == b.as_dict()


# --- bind semantics ---------------------------------------------------------


@pytest.mark.parametrize(
    ("wire", "expected"),
    [
        ("2024-03-01T12:00:00+05:30", datetime(2024, 3, 1, 6, 30)),
        ("2024-03-01T12:00:00-04:00", datetime(2024, 3, 1, 16, 0)),
        ("2024-03-01T12:00:00Z", datetime(2024, 3, 1, 12, 0)),
    ],
)
def test_mysql_timestamp_bind_converts_the_offset_to_utc(wire: str, expected) -> None:
    # Session time_zone is pinned to +00:00 by the writer, so a naive UTC bind
    # stores exactly the instant the offset wire carried.
    out = coerce_sql_temporal(wire, "TIMESTAMP(6)", engine="mysql")
    assert out == expected
    assert out.tzinfo is None


def test_mysql_timestamp_bind_survives_a_dst_spring_forward_boundary() -> None:
    # 2024-03-10 02:30 America/New_York does not exist; the aware wire is the
    # authority, and both sides of the transition map to distinct instants.
    before = coerce_sql_temporal(
        "2024-03-10T01:59:59-05:00", "TIMESTAMP(6)", engine="mysql"
    )
    after = coerce_sql_temporal(
        "2024-03-10T03:00:00-04:00", "TIMESTAMP(6)", engine="mysql"
    )
    assert after - before == timedelta(seconds=1)


def test_non_mysql_timestamp_bind_stays_wall_clock() -> None:
    # Bare TIMESTAMP on Postgres is TIMESTAMP WITHOUT TIME ZONE — an offset wire
    # must not be silently UTC-shifted there.
    out = coerce_sql_temporal("2024-03-01T12:00:00+05:30", "TIMESTAMP", engine="postgresql")
    assert out.hour == 12


def test_nulls_pass_through_unchanged() -> None:
    assert coerce_sql_temporal(None, "TIMESTAMP(6)", engine="mysql") is None
    check = wire_check_temporal(None, "TIMESTAMP(6)", engine="mysql")
    assert check["ok"] is True


# --- epoch range ------------------------------------------------------------


@pytest.mark.parametrize(
    "wire",
    [
        "1969-12-31T23:59:59Z",
        "2039-01-01T00:00:00Z",
        "1900-05-04T10:00:00+00:00",
    ],
)
def test_out_of_range_instants_are_detected(wire: str) -> None:
    assert mysql_timestamp_out_of_range(wire) is True


@pytest.mark.parametrize(
    "value",
    [
        MYSQL_TIMESTAMP_MIN,
        MYSQL_TIMESTAMP_MAX,
        datetime(2024, 1, 1, tzinfo=timezone.utc),
    ],
)
def test_in_range_instants_are_accepted(value: datetime) -> None:
    assert mysql_timestamp_out_of_range(value) is False


def test_out_of_range_instant_blocks_at_validate_not_at_write() -> None:
    check = wire_check_temporal("2039-01-01T00:00:00Z", "TIMESTAMP(6)", engine="mysql")
    assert check["ok"] is False
    assert "epoch range" in check["reason"]
    assert "DATETIME(6)" in check["reason"]


def test_out_of_range_instant_is_quarantined_at_write() -> None:
    from connectors.writer_common import quarantine_unfit_temporals

    rejected: list[dict] = []
    rows = quarantine_unfit_temporals(
        [("2039-01-01T00:00:00Z",), ("2024-01-01T00:00:00Z",)],
        ["created_at"],
        ["TIMESTAMP(6)"],
        rejected,
        "quarantine",
        dest_db="mysql",
    )
    assert len(rows) == 1
    assert len(rejected) == 1
    assert "epoch range" in str(rejected[0])


def test_aware_wire_into_mysql_timestamp_is_not_an_offset_strip_quarantine() -> None:
    # TIMESTAMP is an instant carrier on MySQL, so the NTZ offset-strip rule
    # must not fire — that would quarantine every correct row.
    from connectors.writer_common import quarantine_unfit_temporals

    rejected: list[dict] = []
    rows = quarantine_unfit_temporals(
        [("2024-03-01T12:00:00+05:30",)],
        ["created_at"],
        ["TIMESTAMP(6)"],
        rejected,
        "quarantine",
        dest_db="mysql",
    )
    assert len(rows) == 1
    assert rejected == []


def test_aware_wire_into_mysql_datetime_still_quarantines_the_offset_strip() -> None:
    from connectors.writer_common import quarantine_unfit_temporals

    rejected: list[dict] = []
    rows = quarantine_unfit_temporals(
        [("2024-03-01T12:00:00+05:30",)],
        ["created_at"],
        ["DATETIME(6)"],
        rejected,
        "quarantine",
        dest_db="mysql",
    )
    assert rows == []
    assert len(rejected) == 1
