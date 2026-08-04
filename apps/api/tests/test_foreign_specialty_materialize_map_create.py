"""Map≡CREATE — foreign VECTOR/BIT/ENUM/MONEY/YEAR rematerialize to ddl_type SSOT.

Headline invents: Snowflake VECTOR(n) vs VECTOR(FLOAT,n); BIT→boolean polarity;
bare ENUM/SET illegal CREATE; MONEY/YEAR/MEDIUMINT off-engine invent.
"""

from __future__ import annotations

import pytest

from services.type_system import ddl_type, materialize_dest_ddl


_GAP_CASES: list[tuple[str, str]] = [
    # VECTOR family
    ("snowflake", "VECTOR(1536)"),
    ("snowflake", "VECTOR(3)"),
    ("snowflake", "HALFVEC(768)"),
    ("snowflake", "VECTOR"),
    ("postgresql", "VECTOR(1536)"),
    ("postgresql", "VECTOR(FLOAT, 768)"),
    ("postgresql", "VECTOR"),
    ("mysql", "VECTOR(1536)"),
    ("mysql", "HALFVEC(768)"),
    ("mysql", "VECTOR"),
    ("sqlserver", "VECTOR(1536)"),
    ("sqlserver", "VECTOR"),
    ("duckdb", "VECTOR(1536)"),
    ("duckdb", "VECTOR"),
    ("databricks", "VECTOR(1536)"),
    ("databricks", "HALFVEC(768)"),
    ("oracle", "VECTOR(1536)"),
    ("bigquery", "VECTOR(1536)"),
    ("redshift", "VECTOR(1536)"),
    # BIT / boolean polarity
    ("postgresql", "BIT"),
    ("postgresql", "BIT(1)"),
    ("postgresql", "BOOL"),
    ("postgresql", "TINYINT(1)"),
    ("mysql", "BIT"),
    ("mysql", "BIT(1)"),
    ("mysql", "BOOL"),
    ("mysql", "TINYINT(1)"),
    ("snowflake", "BIT"),
    ("snowflake", "BIT(1)"),
    ("snowflake", "BOOL"),
    ("sqlserver", "BOOLEAN"),
    ("sqlserver", "BOOL"),
    ("sqlserver", "TINYINT(1)"),
    ("sqlserver", "BIT(1)"),
    ("sqlserver", "BIT(8)"),
    ("oracle", "BIT"),
    ("oracle", "BOOLEAN"),
    ("oracle", "BIT(8)"),
    ("bigquery", "BIT"),
    ("bigquery", "BOOLEAN"),
    ("bigquery", "BIT(8)"),
    ("duckdb", "BIT"),
    ("databricks", "BIT"),
    ("databricks", "BIT(8)"),
    ("iceberg", "BIT"),
    ("spanner", "BIT"),
    ("spanner", "BOOLEAN"),
    # ENUM / SET
    ("postgresql", "ENUM"),
    ("postgresql", "SET"),
    ("postgresql", "ENUM('a','b')"),
    ("postgresql", "SET('x','y')"),
    ("mysql", "ENUM"),
    ("mysql", "SET"),
    ("snowflake", "ENUM"),
    ("snowflake", "ENUM('a','b')"),
    ("snowflake", "SET('x','y')"),
    ("sqlserver", "ENUM"),
    ("sqlserver", "ENUM('a','b')"),
    ("oracle", "ENUM('a','b')"),
    ("bigquery", "ENUM"),
    ("duckdb", "ENUM('a','b')"),
    ("databricks", "SET"),
    # MONEY / YEAR / MEDIUMINT
    ("postgresql", "MONEY"),
    ("postgresql", "SMALLMONEY"),
    ("postgresql", "MEDIUMINT"),
    ("postgresql", "YEAR"),
    ("mysql", "MONEY"),
    ("mysql", "SMALLMONEY"),
    ("mysql", "YEAR(4)"),
    ("snowflake", "MONEY"),
    ("snowflake", "YEAR"),
    ("snowflake", "MEDIUMINT"),
    ("sqlserver", "MEDIUMINT"),
    ("sqlserver", "YEAR"),
    ("oracle", "MONEY"),
    ("oracle", "MEDIUMINT"),
    ("bigquery", "MONEY"),
    ("bigquery", "MEDIUMINT"),
    ("duckdb", "MONEY"),
    ("databricks", "MONEY"),
    ("databricks", "MEDIUMINT"),
    ("iceberg", "MONEY"),
    ("iceberg", "MEDIUMINT"),
    ("spanner", "MONEY"),
    ("redshift", "MONEY"),
    ("redshift", "MEDIUMINT"),
]


_NATIVE_PASS: list[tuple[str, str]] = [
    ("postgresql", "BOOLEAN"),
    ("postgresql", "BIT(8)"),
    ("postgresql", "vector(1536)"),
    ("postgresql", "halfvec(768)"),
    ("mysql", "BOOLEAN"),
    ("mysql", "BIT(8)"),
    ("mysql", "YEAR"),
    ("mysql", "MEDIUMINT"),
    ("mysql", "ENUM('a','b')"),
    ("mysql", "SET('x','y')"),
    ("sqlserver", "BIT"),
    ("sqlserver", "MONEY"),
    ("sqlserver", "SMALLMONEY"),
    ("snowflake", "BOOLEAN"),
    ("snowflake", "VECTOR(FLOAT, 768)"),
    ("duckdb", "BOOLEAN"),
    ("duckdb", "BIT(8)"),
    ("oracle", "NUMBER(1)"),
    ("bigquery", "BOOL"),
]


_HEADLINES: list[tuple[str, str, str]] = [
    ("snowflake", "VECTOR(1536)", "VECTOR(FLOAT, 1536)"),
    ("snowflake", "HALFVEC(768)", "VECTOR(FLOAT, 768)"),
    ("postgresql", "VECTOR(1536)", "vector(1536)"),
    ("postgresql", "VECTOR(FLOAT, 768)", "vector(768)"),
    ("postgresql", "BIT", "BOOLEAN"),
    ("mysql", "BIT", "BOOLEAN"),
    ("sqlserver", "BOOLEAN", "BIT"),
    ("sqlserver", "BIT(8)", "VARCHAR(8)"),
    ("oracle", "BOOLEAN", "NUMBER(1)"),
    ("bigquery", "BIT", "BOOL"),
    ("mysql", "ENUM", "TEXT"),
    ("postgresql", "ENUM", "TEXT"),
    ("snowflake", "ENUM('a','b')", "VARCHAR(1)"),
    ("postgresql", "MONEY", "DECIMAL(19,4)"),
    ("mysql", "YEAR(4)", "YEAR"),
    ("postgresql", "MEDIUMINT", "INTEGER"),
    ("sqlserver", "MEDIUMINT", "INT"),
]


@pytest.mark.parametrize("dest,carrier", _GAP_CASES)
def test_specialty_materialize_matches_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest,carrier", _NATIVE_PASS)
def test_native_specialty_stamps_still_pass_through(dest: str, carrier: str):
    got = materialize_dest_ddl(dest, carrier)
    expected = ddl_type(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest,carrier,want", _HEADLINES)
def test_specialty_headlines(dest: str, carrier: str, want: str):
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == want.upper().replace(" ", ""), (
        f"{dest} {carrier}: got={got!r} want={want!r}"
    )
