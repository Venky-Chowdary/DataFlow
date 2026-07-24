"""Universal type harness — every alias × every DDL destination must be honest.

Proves:
- Vendor introspect strings normalize to the correct logical type (not silent STRING)
- Every DDL destination has an entry for every logical type
- Iceberg writer uses type_system (no parallel scale map)
- BIT(n>1) / TINYINT(1) / DECIMAL(p,s) semantics are fail-closed
- DECIMAL scale overflow never silently truncates
"""

from __future__ import annotations

import pytest

from services.type_system import (
    CANONICAL_TYPES,
    DDL_TYPES,
    DEFAULT_DDL,
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_BOOLEAN,
    LOGICAL_DATE,
    LOGICAL_DATETIME,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_GEOGRAPHY,
    LOGICAL_INTEGER,
    LOGICAL_INTERVAL,
    LOGICAL_JSON,
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_VECTOR,
    ddl_type,
    decimal_scale_would_truncate,
    is_lossy_coercion,
    normalize_logical_type,
)

ALL_LOGICALS = [
    LOGICAL_STRING,
    LOGICAL_TEXT,
    LOGICAL_INTEGER,
    LOGICAL_DECIMAL,
    LOGICAL_FLOAT,
    LOGICAL_BOOLEAN,
    LOGICAL_DATE,
    LOGICAL_DATETIME,
    LOGICAL_TIME,
    LOGICAL_UUID,
    LOGICAL_JSON,
    LOGICAL_ARRAY,
    LOGICAL_BINARY,
    LOGICAL_INTERVAL,
    LOGICAL_GEOGRAPHY,
    LOGICAL_VECTOR,
]

# Vendor introspect strings that MUST NOT fall through to bare string when
# they carry numeric / temporal / binary / boolean semantics.
VENDOR_MUST_MAP: list[tuple[str, str]] = [
    ("FLOAT", LOGICAL_FLOAT),
    ("DOUBLE", LOGICAL_FLOAT),
    ("REAL", LOGICAL_FLOAT),
    ("MONEY", LOGICAL_DECIMAL),
    ("SMALLMONEY", LOGICAL_DECIMAL),
    ("CURRENCY", LOGICAL_DECIMAL),
    ("FIXED", LOGICAL_DECIMAL),
    ("NUMBER(38,18)", LOGICAL_DECIMAL),
    ("DECIMAL(28,12)", LOGICAL_DECIMAL),
    ("NUMBER(38,0)", LOGICAL_DECIMAL),
    ("BIGINT UNSIGNED", LOGICAL_DECIMAL),
    ("UINT64", LOGICAL_DECIMAL),
    ("UINT128", LOGICAL_DECIMAL),
    ("INT128", LOGICAL_DECIMAL),
    ("HUGEINT", LOGICAL_DECIMAL),
    ("BIT", LOGICAL_BOOLEAN),
    ("BIT(1)", LOGICAL_BOOLEAN),
    ("BIT(32)", LOGICAL_BINARY),
    ("BIT VARYING", LOGICAL_BINARY),
    ("TINYINT(1)", LOGICAL_BOOLEAN),
    ("TINYINT(4)", LOGICAL_INTEGER),
    ("DATETIMEOFFSET", LOGICAL_DATETIME),
    ("DATETIME2", LOGICAL_DATETIME),
    ("TIMESTAMP_NTZ", LOGICAL_DATETIME),
    ("TIMESTAMP WITH TIME ZONE", LOGICAL_DATETIME),
    ("UNIQUEIDENTIFIER", LOGICAL_UUID),
    ("GUID", LOGICAL_UUID),
    ("VARIANT", LOGICAL_JSON),
    ("SUPER", LOGICAL_JSON),
    ("JSONB", LOGICAL_JSON),
    ("BYTEA", LOGICAL_BINARY),
    ("BLOB", LOGICAL_BINARY),
    ("BFILE", LOGICAL_BINARY),
    ("NCLOB", LOGICAL_TEXT),
    ("LONG", LOGICAL_INTEGER),  # lakehouse long; Oracle LONG text is legacy
    ("YEAR", LOGICAL_INTEGER),
    ("INTERVAL", LOGICAL_INTERVAL),
    ("INTERVAL DAY TO SECOND", LOGICAL_INTERVAL),
    ("GEOMETRY", LOGICAL_GEOGRAPHY),
    ("GEOGRAPHY", LOGICAL_GEOGRAPHY),
    ("VECTOR", LOGICAL_VECTOR),
    ("HALFVEC", LOGICAL_VECTOR),
]


@pytest.mark.parametrize("native,expected", VENDOR_MUST_MAP)
def test_vendor_native_normalizes(native: str, expected: str):
    assert normalize_logical_type(native) == expected, native


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
@pytest.mark.parametrize("logical", ALL_LOGICALS)
def test_every_dest_has_ddl_for_every_logical(dest: str, logical: str):
    ddl = ddl_type(dest, logical)
    assert ddl, f"{dest}/{logical} empty"
    assert ddl == DDL_TYPES[dest][logical] or logical == LOGICAL_DECIMAL
    # DECIMAL may be parameterized — still non-empty and not the unknown fallback
    # unless dest has no DECIMAL template (bigquery BIGNUMERIC, pg NUMERIC, etc.)
    assert isinstance(ddl, str) and len(ddl) >= 1


def test_all_canonical_aliases_resolve():
    for alias, logical in CANONICAL_TYPES.items():
        assert normalize_logical_type(alias) == logical, alias
        assert logical in ALL_LOGICALS or logical == LOGICAL_STRING


def test_default_ddl_covers_every_ddl_dest():
    for dest in DDL_TYPES:
        assert dest in DEFAULT_DDL, dest


def test_iceberg_writer_matches_type_system():
    from connectors.iceberg_writer import _logical_to_iceberg_type

    for logical in ALL_LOGICALS:
        assert _logical_to_iceberg_type(logical) == ddl_type("iceberg", logical)
    # Scale must match type_system (was decimal(38,9) in the parallel map)
    assert _logical_to_iceberg_type("DECIMAL") == ddl_type("iceberg", "DECIMAL")
    assert "decimal(38,10)" in _logical_to_iceberg_type("DECIMAL").lower() or _logical_to_iceberg_type(
        "DECIMAL"
    ).lower().startswith("decimal")


def test_decimal_scale_overflow_is_lossless_text():
    assert ddl_type("mysql", "NUMBER(38,31)") == "TEXT"
    assert decimal_scale_would_truncate("NUMBER(38,31)", "mysql") is True
    assert ddl_type("mysql", "NUMBER(38,18)") == "DECIMAL(38,18)"


def test_duckdb_clickhouse_trino_have_real_ddl():
    assert ddl_type("duckdb", "decimal") == "DECIMAL(38,15)"
    assert "Decimal" in ddl_type("clickhouse", "decimal") or "decimal" in ddl_type("clickhouse", "decimal").lower()
    assert "bigint" in ddl_type("trino", "integer").lower()
    assert ddl_type("redis", "decimal") == "string"
    assert ddl_type("dynamodb", "integer") == "N"


def test_lossy_coercion_matrix_hard_rules():
    assert is_lossy_coercion("decimal", "integer") is True
    assert is_lossy_coercion("float", "integer") is True
    assert is_lossy_coercion("float", "decimal") is True
    assert is_lossy_coercion("datetime", "date") is True
    assert is_lossy_coercion("integer", "decimal") is False
    assert is_lossy_coercion("date", "datetime") is False
    assert is_lossy_coercion("uuid", "string") is False


def test_specialty_native_ddl_and_lossless_sinks():
    assert ddl_type("postgresql", "INTERVAL") == "INTERVAL"
    assert ddl_type("postgresql", "GEOMETRY") == "GEOMETRY"
    # Bare VECTOR has no dimension — never invent 1536; sink to lossless text.
    assert ddl_type("postgresql", "VECTOR") == "TEXT"
    assert ddl_type("postgresql", "VECTOR(1536)") == "vector(1536)"
    assert ddl_type("snowflake", "VECTOR(768)") == "VECTOR(FLOAT, 768)"
    assert ddl_type("snowflake", "VECTOR") == "VARCHAR"
    assert ddl_type("snowflake", "VECTOR(FLOAT, 1024)") == "VECTOR(FLOAT, 1024)"
    assert ddl_type("bigquery", "GEOGRAPHY") == "GEOGRAPHY"
    assert ddl_type("sqlserver", "GEOGRAPHY") == "GEOGRAPHY"
    # Engines without native interval keep lossless text — never invent a fake cast
    assert ddl_type("mysql", "INTERVAL") in {"TEXT", "LONGTEXT"}
    assert is_lossy_coercion("interval", "integer") is True
    assert is_lossy_coercion("geography", "string") is False
    assert is_lossy_coercion("vector", "array") is False


def test_datetime_preserves_source_offset():
    from services.transform_engine import apply_transform

    val, err = apply_transform("2024-06-01T12:00:00+05:30", "datetime")
    assert err is None
    assert val == "2024-06-01T12:00:00+05:30"
    # Naive / Z still canonical UTC Z
    val2, err2 = apply_transform("2024-06-01T12:00:00Z", "datetime")
    assert err2 is None
    assert val2 == "2024-06-01T12:00:00Z"


def test_generic_sql_duckdb_decimal_not_float64():
    from connectors.generic_sql import _sa_type_for_logical
    import sqlalchemy as sa

    t = _sa_type_for_logical("decimal", "duckdb", "duckdb")
    assert isinstance(t, sa.Numeric)
    assert not isinstance(t, sa.Float)
