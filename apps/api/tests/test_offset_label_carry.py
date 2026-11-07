"""Originating offset is stored data, not WITH TIME ZONE spelling.

PostgreSQL TIMESTAMPTZ / MySQL TIMESTAMP keep UTC only. SQL Server
DATETIMEOFFSET keeps +05:30. AWS SCT maps the second onto the first and
the load looks green. These tests pin the classifier, the bind, and the
fidelity aspect so the certificate cannot say carried for a dest that
dropped the label.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from connectors.sql_temporal import coerce_sql_temporal, logical_to_temporal_ddl
from services.offset_label import (
    attach_offset_label,
    decide_offset_label,
    extract_offset_label,
    stores_originating_offset,
)
from services.schema_fidelity import SourceSchemaCatalog, plan_create_new_fidelity
from services.timezone_policy import POLICY_NATIVE_INSTANT, resolve_timezone_policy

IST = timezone(timedelta(hours=5, minutes=30))
WIRE = "2024-03-01T12:00:00+05:30"
UTC_INSTANT = datetime(2024, 3, 1, 6, 30, tzinfo=timezone.utc)


def test_extract_offset_label_from_iso_and_datetime():
    assert extract_offset_label(WIRE).minutes == 330
    assert extract_offset_label(WIRE).iso_suffix == "+05:30"
    assert extract_offset_label("2024-03-01T12:00:00Z").minutes == 0
    assert extract_offset_label(datetime(2024, 3, 1, 12, 0, tzinfo=IST)).minutes == 330
    assert extract_offset_label(datetime(2024, 3, 1, 12, 0)) is None


def test_pg_and_mysql_do_not_store_originating_offset():
    assert stores_originating_offset("postgresql", "TIMESTAMP WITH TIME ZONE") is False
    assert stores_originating_offset("postgresql", "TIMESTAMPTZ") is False
    assert stores_originating_offset("mysql", "TIMESTAMP(6)") is False
    assert stores_originating_offset("mysql", "DATETIMEOFFSET") is False


def test_sqlserver_datetimeoffset_stores_originating_offset():
    assert stores_originating_offset("sqlserver", "DATETIMEOFFSET") is True
    assert stores_originating_offset("sqlserver", "DATETIMEOFFSET(7)") is True
    assert stores_originating_offset("sqlserver", "DATETIME2") is False


def test_oracle_with_time_zone_stores_offset_local_does_not():
    assert stores_originating_offset("oracle", "TIMESTAMP WITH TIME ZONE") is True
    assert stores_originating_offset("oracle", "TIMESTAMP WITH LOCAL TIME ZONE") is False


def test_ambiguous_with_time_zone_without_engine_is_not_claimed():
    """SCT lie: WITH TIME ZONE without an engine is PostgreSQL as often as Oracle."""
    assert stores_originating_offset("", "TIMESTAMP WITH TIME ZONE") is False
    assert stores_originating_offset("", "DATETIMEOFFSET") is True


def test_datetimeoffset_to_pg_timestamptz_is_unsupported_not_carried():
    decision = decide_offset_label(
        source_engine="sqlserver",
        source_type="DATETIMEOFFSET",
        dest_engine="postgresql",
        dest_type="TIMESTAMP WITH TIME ZONE",
        source_column="ts",
        dest_column="ts",
    )
    assert decision is not None
    assert decision.status == "unsupported"
    assert decision.source_stores is True
    assert decision.dest_stores is False


def test_datetimeoffset_to_datetimeoffset_is_carried():
    decision = decide_offset_label(
        source_engine="sqlserver",
        source_type="DATETIMEOFFSET",
        dest_engine="sqlserver",
        dest_type="DATETIMEOFFSET(7)",
        source_column="ts",
        dest_column="ts",
    )
    assert decision is not None
    assert decision.status == "carried"


def test_pg_timestamptz_source_has_no_label_to_carry():
    decision = decide_offset_label(
        source_engine="postgresql",
        source_type="timestamp with time zone",
        dest_engine="mysql",
        dest_type="TIMESTAMP(6)",
        source_column="ts",
        dest_column="ts",
    )
    assert decision is not None
    assert decision.status == "skipped"
    assert decision.source_stores is False


def test_policy_does_not_claim_pg_with_time_zone_keeps_datetimeoffset_label():
    policy = resolve_timezone_policy(
        "DATETIMEOFFSET", "TIMESTAMP WITH TIME ZONE", dest_db="postgresql"
    )
    assert policy is not None
    assert policy.offset_label_preserved is False
    assert policy.instant_preserved is True
    assert policy.policy == POLICY_NATIVE_INSTANT


def test_sqlserver_datetimeoffset_bind_keeps_plus_0530():
    out = coerce_sql_temporal(WIRE, "DATETIMEOFFSET", engine="sqlserver")
    assert isinstance(out, datetime)
    assert out.utcoffset() == timedelta(hours=5, minutes=30)
    assert out.astimezone(timezone.utc) == UTC_INSTANT


def test_pg_timestamptz_bind_utc_normalizes_the_same_wire():
    out = coerce_sql_temporal(WIRE, "TIMESTAMPTZ", engine="postgresql")
    assert isinstance(out, datetime)
    assert out.utcoffset() == timedelta(0)
    assert out == UTC_INSTANT


def test_logical_datetimeoffset_is_not_folded_to_timestamptz():
    assert logical_to_temporal_ddl("datetimeoffset") == "DATETIMEOFFSET"
    assert logical_to_temporal_ddl("timestamptz") == "TIMESTAMPTZ"


def test_generic_sql_sqlserver_bind_does_not_strip_offset_to_utc():
    from connectors.generic_sql import _to_sa_value

    got = _to_sa_value(WIRE, "datetimeoffset", None, "", "sqlserver")
    assert isinstance(got, datetime)
    assert got.utcoffset() == timedelta(hours=5, minutes=30)


def test_generic_sql_pg_bind_utc_normalizes():
    from connectors.generic_sql import _to_sa_value

    got = _to_sa_value(WIRE, "timestamptz", None, "", "postgresql")
    assert isinstance(got, datetime)
    assert got == UTC_INSTANT


def test_create_new_fidelity_names_offset_label_unsupported_on_pg():
    catalog = SourceSchemaCatalog(
        dialect="sqlserver",
        columns=["id", "ts"],
        column_types={"id": "BIGINT", "ts": "DATETIMEOFFSET"},
        primary_key=["id"],
    )
    plan = plan_create_new_fidelity(
        catalog,
        dest_dialect="postgresql",
        target_columns=["id", "ts"],
        target_types=["BIGINT", "TIMESTAMPTZ"],
        source_to_target={"id": "id", "ts": "ts"},
    )
    items = [i for i in plan.report.items if i.aspect == "offset_label"]
    assert items
    assert any(i.status == "unsupported" for i in items)
    assert any("UTC instant" in i.reason or "offset" in i.reason.lower() for i in items)


def test_attach_offset_round_trips_the_instant():
    labelled = attach_offset_label(UTC_INSTANT, extract_offset_label(WIRE))
    assert labelled.utcoffset() == timedelta(hours=5, minutes=30)
    assert labelled.astimezone(timezone.utc) == UTC_INSTANT
