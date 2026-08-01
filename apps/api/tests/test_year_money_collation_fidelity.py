"""YEAR/MONEY/CI collation + temporal FSP + boolean quarantine — enterprise SSOT."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.writer_common import (  # noqa: E402
    parse_decimal_precision_scale,
    quarantine_currency_markers_into_numeric,
    quarantine_unfit_booleans,
    quarantine_unfit_temporals,
    quarantine_unfit_years,
)
from services.reconciliation import normalize_cell  # noqa: E402
from services.schema_introspect import (  # noqa: E402
    _mysql_to_logical,
    _pg_to_logical,
    _sqlserver_to_logical,
)
from services.type_system import (  # noqa: E402
    boolean_value_fits,
    ddl_type,
    has_currency_marker,
    is_case_insensitive_collation,
    is_precision_collapse_coercion,
    is_year_carrier,
    normalize_logical_type,
    parse_temporal_fractional_precision,
    temporal_precision_would_narrow,
    unique_equality_key,
    year_value_fits,
)


def test_mysql_year_carrier_preserved():
    assert _mysql_to_logical("year") == "YEAR"
    assert _mysql_to_logical("year(4)") == "YEAR"
    assert is_year_carrier("YEAR") is True
    assert normalize_logical_type("YEAR") == "integer"
    assert ddl_type("mysql", "YEAR") == "YEAR"
    assert ddl_type("postgresql", "YEAR") == "SMALLINT"


def test_year_value_range():
    assert year_value_fits(2024) is True
    assert year_value_fits(0) is True
    assert year_value_fits(1901) is True
    assert year_value_fits(2155) is True
    assert year_value_fits(1800) is False
    assert year_value_fits(2156) is False
    assert year_value_fits(69) is True  # → 2069
    assert year_value_fits(70) is True  # → 1970


def test_quarantine_unfit_years():
    details: list[dict] = []
    out = quarantine_unfit_years(
        [(2024,), (1800,), (0,)],
        ["model_year"],
        ["YEAR"],
        details,
        policy="quarantine",
    )
    assert out == [(2024,), (0,)]
    assert details and "1901" in details[0]["reason"]


def test_pg_money_and_sqlserver_money_scale():
    assert _pg_to_logical("money") == "DECIMAL(19,4)"
    assert parse_decimal_precision_scale("MONEY") == (19, 4)
    assert parse_decimal_precision_scale("SMALLMONEY") == (10, 4)
    assert ddl_type("sqlserver", "MONEY") == "MONEY"


def test_currency_marker_quarantine():
    assert has_currency_marker("$1,234.56") is True
    assert has_currency_marker("1234.56") is False
    details: list[dict] = []
    out = quarantine_currency_markers_into_numeric(
        [("$100",), ("100",)],
        ["amt"],
        ["DECIMAL(19,4)"],
        details,
        policy="quarantine",
    )
    assert out == [("100",)]
    assert details and "currency" in details[0]["reason"].lower()


def test_ci_collation_helpers_and_fingerprint():
    assert is_case_insensitive_collation("VARCHAR(50) COLLATE utf8mb4_unicode_ci") is True
    assert is_case_insensitive_collation("NVARCHAR(50) COLLATE Latin1_General_CI_AS") is True
    assert is_case_insensitive_collation("VARCHAR(50)") is False
    assert is_case_insensitive_collation("CITEXT") is True
    assert is_case_insensitive_collation(
        "VARCHAR COLLATE und-x-icu NONDETERMINISTIC"
    ) is True
    assert normalize_logical_type("VARCHAR(50) COLLATE utf8mb4_general_ci") == "string"
    assert normalize_logical_type("CITEXT") == "string"
    assert normalize_cell("Abc", ddl_type="VARCHAR(10) COLLATE utf8mb4_general_ci") == normalize_cell(
        "abc", ddl_type="VARCHAR(10) COLLATE utf8mb4_general_ci"
    )
    assert normalize_cell("Abc", ddl_type="CITEXT") == normalize_cell("abc", ddl_type="CITEXT")
    assert normalize_cell("Abc", ddl_type="VARCHAR(10)") != normalize_cell(
        "abc", ddl_type="VARCHAR(10)"
    )


def test_temporal_fsp_carriers_and_g3_narrow():
    assert _pg_to_logical("timestamp(6) without time zone") == "TIMESTAMP_NTZ(6)"
    assert _pg_to_logical("timestamptz(3)") == "TIMESTAMPTZ(3)"
    assert _pg_to_logical("time(3) without time zone") == "TIME(3)"
    assert _mysql_to_logical("datetime(6)") == "TIMESTAMP_NTZ(6)"
    assert _mysql_to_logical("timestamp(2)") == "TIMESTAMPTZ(2)"
    assert _sqlserver_to_logical("datetime2(7)") == "TIMESTAMP_NTZ(7)"
    assert _sqlserver_to_logical("time(3)") == "TIME(3)"
    assert parse_temporal_fractional_precision("TIMESTAMP_NTZ(6)") == 6
    assert parse_temporal_fractional_precision("TIME(0)") == 0
    assert temporal_precision_would_narrow("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(0)") is True
    assert temporal_precision_would_narrow("TIME(3)", "TIME(6)") is False
    assert is_precision_collapse_coercion("TIMESTAMP_NTZ(6)", "TIMESTAMP_NTZ(0)") is True
    assert normalize_logical_type("TIMESTAMP_NTZ(6)") == "datetime"
    assert normalize_logical_type("TIME(3)") == "time"
    # Create-new DDL must propagate FSP — not invent bare TIMESTAMP/DATETIME2.
    assert ddl_type("postgresql", "TIMESTAMP_NTZ(6)") == "TIMESTAMP(6)"
    assert ddl_type("sqlserver", "TIMESTAMP_NTZ(6)") == "DATETIME2(6)"
    assert ddl_type("mysql", "TIMESTAMP_NTZ(3)").upper().startswith("DATETIME(3)")
    # TIME(p) create-new must not invent bare TIME (MySQL default FSP=0).
    assert ddl_type("postgresql", "TIME(3)") == "TIME(3)"
    assert ddl_type("mysql", "TIME(6)") == "TIME(6)"
    assert ddl_type("sqlserver", "TIME(3)") == "TIME(3)"


def test_unique_equality_key_ci_casefold():
    assert unique_equality_key("Abc", "CITEXT") == unique_equality_key(
        "abc", "CITEXT"
    )
    assert unique_equality_key(
        "Abc", "VARCHAR(50) COLLATE utf8mb4_unicode_ci"
    ) == unique_equality_key("abc", "VARCHAR(50) COLLATE utf8mb4_unicode_ci")
    assert unique_equality_key("Abc", "VARCHAR(50)") != unique_equality_key(
        "abc", "VARCHAR(50)"
    )


def test_integrity_duplicate_keys_respect_ci_collation():
    from services.data_integrity import _check_duplicate_keys

    result = _check_duplicate_keys(
        [{"source": "email", "target": "email"}],
        [{"email": "Abc"}, {"email": "abc"}],
        "strict",
        dest_kind="postgresql",
        primary_key="email",
        sync_mode="upsert",
        target_types={"email": "CITEXT"},
    )
    assert result["passed"] is False
    assert result["blocks_transfer"] is True


def test_boolean_and_temporal_write_quarantine():
    assert boolean_value_fits(True) is True
    assert boolean_value_fits(0) is True
    assert boolean_value_fits("true") is True
    assert boolean_value_fits("yes") is False
    assert boolean_value_fits(2) is False
    details: list[dict] = []
    out = quarantine_unfit_booleans(
        [(True,), ("yes",), (1,)],
        ["flag"],
        ["BOOLEAN"],
        details,
        policy="quarantine",
    )
    assert out == [(True,), (1,)]
    assert details and "canonical" in details[0]["reason"].lower()

    tdetails: list[dict] = []
    tout = quarantine_unfit_temporals(
        [("2024-01-01 12:00:00.123456",), ("2024-01-01 12:00:00",)],
        ["ts"],
        ["TIMESTAMP_NTZ(0)"],
        tdetails,
        policy="quarantine",
    )
    assert tout == [("2024-01-01 12:00:00",)]
    assert tdetails and "fractional" in tdetails[0]["reason"].lower()
