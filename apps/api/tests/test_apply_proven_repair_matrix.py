"""Apply-then-rescan must stay clean on every dest engine, not one pasted route.

Population-fit repairs share one scan / prove / apply path. A Snowflake CSV
overflow is a signal — the same algorithm binds MySQL, Postgres, Oracle,
BigQuery, SQL Server, Redshift, and untyped sources (file + Mongo).

Live destination DDL is named, never Apply (Map cannot ALTER).
"""

from __future__ import annotations

import pytest

from services.population_fit_scan import (
    apply_suggested_widens_and_rescan,
    applyable_widen_actions,
    scan_population_fit,
)

# Integer-digit overflow fails even on Postgres (which rounds extra *scale*).
# Avoid Auto-locale refuse (``123456.789`` is not a write-path decimal) and
# avoid a column named ``amount`` (currency transform).
DECIMAL_ROWS = (
    [{"measure": "12.30"}]
    + [{"measure": "1234.56"}]
    + [{"measure": "12.30"}] * 3
    + [{"measure": "99999.99"}]
)
VARCHAR_ROWS = (
    [{"name": "ok"}]
    + [{"name": "x" * 20}]
    + [{"name": "ok"}] * 2
    + [{"name": "y" * 50}]
)
INTEGER_ROWS = (
    [{"qty": 100}]
    + [{"qty": 40_000}]
    + [{"qty": 100}] * 2
    + [{"qty": 10**20}]
)
FRACTION_ROWS = (
    [{"clock": "1000"}]
    + [{"clock": "22.433332"}]
    + [{"clock": "21.833334"}]
)

DEST_DECIMAL = (
    ("snowflake", "NUMBER(5,2)"),
    ("postgresql", "NUMERIC(5,2)"),
    ("mysql", "DECIMAL(5,2)"),
    ("mariadb", "DECIMAL(5,2)"),
    ("sqlserver", "DECIMAL(5,2)"),
    ("oracle", "NUMBER(5,2)"),
    ("bigquery", "NUMERIC(5,2)"),
    ("redshift", "NUMERIC(5,2)"),
)
DEST_VARCHAR = (
    "snowflake",
    "postgresql",
    "mysql",
    "sqlserver",
    "oracle",
    "bigquery",
    "redshift",
)
DEST_INTEGER = (
    ("postgresql", "SMALLINT"),
    ("mysql", "SMALLINT"),
    ("sqlserver", "SMALLINT"),
    ("snowflake", "SMALLINT"),
    ("oracle", "NUMBER(5,0)"),
)
SOURCE_KINDS = (
    ("file", "csv"),
    ("file", "xlsx"),
    ("", "json"),
)


def _scan_kw(dest_db: str, *, dest_table_exists: bool = False, **extra):
    return dict(
        dest_db=dest_db,
        dialect_label=dest_db,
        job_error_policy="fail",
        rows_are_population=True,
        dest_table_exists=dest_table_exists,
        **extra,
    )


@pytest.mark.parametrize("source_kind,source_format", SOURCE_KINDS)
@pytest.mark.parametrize("dest_db,narrow", DEST_DECIMAL)
def test_decimal_apply_then_rescan_is_clean_on_every_dest(
    dest_db: str, narrow: str, source_kind: str, source_format: str
) -> None:
    mappings = [{"source": "measure", "target": "measure", "target_type": narrow}]
    kw = _scan_kw(
        dest_db,
        source_kind=source_kind,
        source_format=source_format,
        source_types={"measure": "VARCHAR"},
    )
    report = scan_population_fit(DECIMAL_ROWS, mappings, **kw)
    assert report.findings, (dest_db, source_kind)
    assert report.findings[0].apply_proven is True, dest_db
    actions = applyable_widen_actions(report)
    assert actions, dest_db
    assert actions[0]["mapping_applyable"] is True
    updated, after = apply_suggested_widens_and_rescan(
        DECIMAL_ROWS, mappings, report, **kw
    )
    assert after.findings == (), (
        dest_db,
        source_kind,
        updated[0].get("target_type"),
        [f.suggested_target_type for f in after.findings],
        [f.unfit_reason for f in after.findings],
    )


@pytest.mark.parametrize("dest_db", DEST_VARCHAR)
def test_varchar_apply_then_rescan_is_clean_on_every_dest(dest_db: str) -> None:
    mappings = [{"source": "name", "target": "name", "target_type": "VARCHAR(10)"}]
    kw = _scan_kw(
        dest_db,
        source_kind="file",
        source_format="csv",
        source_types={"name": "TEXT"},
    )
    report = scan_population_fit(VARCHAR_ROWS, mappings, **kw)
    assert report.findings
    assert report.findings[0].apply_proven is True
    _updated, after = apply_suggested_widens_and_rescan(
        VARCHAR_ROWS, mappings, report, **kw
    )
    assert after.findings == (), (dest_db, report.findings[0].suggested_target_type)


@pytest.mark.parametrize("dest_db,narrow", DEST_INTEGER)
def test_integer_envelope_apply_then_rescan_is_clean(dest_db: str, narrow: str) -> None:
    """First overflow is BIGINT-class; a later 10**20 still needs a decimal carrier."""
    mappings = [{"source": "qty", "target": "qty", "target_type": narrow}]
    kw = _scan_kw(
        dest_db,
        source_kind="file",
        source_format="csv",
        source_types={"qty": "BIGINT"},
    )
    report = scan_population_fit(INTEGER_ROWS, mappings, **kw)
    assert report.findings
    assert report.findings[0].apply_proven is True
    suggested = report.findings[0].suggested_target_type
    assert suggested
    # BIGINT cannot hold 10**20 — suggestion must be a decimal/number carrier
    # or the apply-rescan below fails.
    _updated, after = apply_suggested_widens_and_rescan(
        INTEGER_ROWS, mappings, report, **kw
    )
    assert after.findings == (), (dest_db, suggested)


@pytest.mark.parametrize(
    "dest_db,int_type",
    [
        ("mysql", "INT"),
        ("postgresql", "INTEGER"),
        ("snowflake", "NUMBER(38,0)"),
        ("sqlserver", "INT"),
        ("oracle", "NUMBER(10,0)"),
        ("bigquery", "INT64"),
    ],
)
def test_fractional_into_integer_apply_then_rescan(dest_db: str, int_type: str) -> None:
    mappings = [{"source": "clock", "target": "clock", "target_type": int_type}]
    kw = _scan_kw(
        dest_db,
        source_kind="file",
        source_format="csv",
        source_types={"clock": "DECIMAL(13,8)"},
    )
    report = scan_population_fit(FRACTION_ROWS, mappings, **kw)
    assert report.findings
    assert "fractional" in report.findings[0].unfit_reason
    suggested = (report.findings[0].suggested_target_type or "").upper()
    assert any(tok in suggested for tok in ("DECIMAL", "NUMERIC", "NUMBER")), suggested
    assert "FLOAT" not in suggested and "DOUBLE" not in suggested
    _updated, after = apply_suggested_widens_and_rescan(
        FRACTION_ROWS, mappings, report, **kw
    )
    assert after.findings == (), (dest_db, suggested)


@pytest.mark.parametrize("dest_db,narrow", DEST_DECIMAL)
def test_live_ddl_never_applyable_on_any_dest(dest_db: str, narrow: str) -> None:
    mappings = [{"source": "measure", "target": "measure", "target_type": "DECIMAL(18,6)"}]
    report = scan_population_fit(
        DECIMAL_ROWS,
        mappings,
        dest_types={"measure": narrow},
        source_types={"measure": "VARCHAR"},
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        **_scan_kw(dest_db, dest_table_exists=True),
    )
    assert report.findings
    assert report.findings[0].target.binds_live_ddl is True
    assert applyable_widen_actions(report) == []


@pytest.mark.parametrize("dest_db,narrow", (("snowflake", "NUMBER(5,2)"), ("mysql", "DECIMAL(5,2)")))
def test_overwrite_recreate_is_applyable(dest_db: str, narrow: str) -> None:
    """Overwrite drops/recreates — Map type is the write bind, Apply may widen it."""
    mappings = [{"source": "measure", "target": "measure", "target_type": narrow}]
    kw = _scan_kw(
        dest_db,
        dest_table_exists=True,
        source_kind="file",
        source_format="csv",
        source_types={"measure": "VARCHAR"},
        sync_mode="full_refresh_overwrite",
    )
    report = scan_population_fit(
        DECIMAL_ROWS, mappings, dest_types={"measure": narrow}, **kw
    )
    assert report.findings
    assert report.findings[0].target.binds_live_ddl is False
    assert applyable_widen_actions(report)
    _updated, after = apply_suggested_widens_and_rescan(
        DECIMAL_ROWS, mappings, report, dest_types={"amount": narrow}, **kw
    )
    assert after.findings == ()
