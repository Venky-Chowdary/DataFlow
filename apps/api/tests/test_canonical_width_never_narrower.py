"""Audit P0 — integer/float invent must never silently narrow width.

Contract:
- Bare logical ``integer`` / ``float`` invent via DDL_TYPES (64-bit / IEEE-64).
- Explicit INT32 / REAL carriers stay width-preserving.
- PostgreSQL introspect emits BIGINT for bigint (never INTEGER).
- ``ddl_type(dest, logical)`` is never narrower than ``DDL_TYPES[dest][logical]``.
"""

from __future__ import annotations

import pytest

from services.decision_kernel import (
    ddl_invent_never_narrower_than_table,
    float_width_carrier,
    integer_width_carrier,
)
from services.schema_introspect import _pg_to_logical
from services.type_system import (
    DDL_TYPES,
    LOGICAL_FLOAT,
    LOGICAL_INTEGER,
    ddl_type,
    integer_bit_width,
    is_lossy_coercion,
)


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
def test_bare_logical_integer_never_narrower_than_ddl_types(dest: str):
    assert ddl_invent_never_narrower_than_table(dest, LOGICAL_INTEGER), (
        f"{dest}: ddl_type={ddl_type(dest, LOGICAL_INTEGER)!r} "
        f"DDL_TYPES={DDL_TYPES[dest][LOGICAL_INTEGER]!r}"
    )


@pytest.mark.parametrize("dest", sorted(DDL_TYPES))
def test_bare_logical_float_never_narrower_than_ddl_types(dest: str):
    assert ddl_invent_never_narrower_than_table(dest, LOGICAL_FLOAT), (
        f"{dest}: ddl_type={ddl_type(dest, LOGICAL_FLOAT)!r} "
        f"DDL_TYPES={DDL_TYPES[dest][LOGICAL_FLOAT]!r}"
    )


def test_pg_introspect_preserves_integer_float_width():
    assert _pg_to_logical("bigint") == "BIGINT"
    assert _pg_to_logical("int8") == "BIGINT"
    assert _pg_to_logical("integer") == "INTEGER"
    assert _pg_to_logical("int4") == "INTEGER"
    assert _pg_to_logical("smallint") == "SMALLINT"
    assert _pg_to_logical("double precision") == "DOUBLE PRECISION"
    assert _pg_to_logical("float8") == "DOUBLE PRECISION"
    assert _pg_to_logical("real") == "REAL"
    assert _pg_to_logical("float4") == "REAL"


def test_explicit_int32_carrier_stays_32_on_pg():
    assert integer_bit_width("INTEGER") == 32
    assert ddl_type("postgresql", "INTEGER") == "INTEGER"
    assert ddl_type("postgresql", "BIGINT") == "BIGINT"


def test_bare_logical_integer_invents_64_on_pg():
    assert integer_bit_width("integer") is None
    assert ddl_type("postgresql", "integer") == "BIGINT"
    assert ddl_type("clickhouse", "integer") == "Int64"
    assert ddl_type("iceberg", "integer") == "long"
    assert ddl_type("oracle", "integer") == "NUMBER(38,0)"
    assert ddl_type("duckdb", "integer") == "BIGINT"


def test_width_carriers_ssot():
    assert integer_width_carrier("bigint") == "BIGINT"
    assert integer_width_carrier("INTEGER") == "INTEGER"
    assert integer_width_carrier("integer") == "BIGINT"
    assert float_width_carrier("double precision") == "DOUBLE PRECISION"
    assert float_width_carrier("real") == "REAL"
    assert float_width_carrier("float") == "DOUBLE"


def test_bigint_to_integer_still_classified_lossy():
    assert is_lossy_coercion("BIGINT", "INTEGER", dest_db="postgresql") is True
    assert is_lossy_coercion("DOUBLE PRECISION", "FLOAT", dest_db="mysql") is True


def test_create_new_from_bigint_carrier_stays_64():
    """Map stamp path: BIGINT source must invent 64-bit on auto-create destinations."""
    assert ddl_type("postgresql", "BIGINT") == "BIGINT"
    assert ddl_type("mysql", "BIGINT") == "BIGINT"
    assert ddl_type("sqlserver", "BIGINT") == "BIGINT"
    assert ddl_type("iceberg", "BIGINT") == "long"
    assert ddl_type("clickhouse", "BIGINT") == "Int64"
