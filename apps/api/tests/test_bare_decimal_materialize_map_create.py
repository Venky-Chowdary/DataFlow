"""Map≡CREATE — bare DECIMAL rematerializes to ddl_type SSOT (p,s).

Bare DECIMAL/NUMERIC/NUMBER must not reach CREATE without (p,s): MySQL invents
DECIMAL(10,0) and quarantine_unfit_decimals no-ops (parse→None), silently
truncating fractional money.
"""

from __future__ import annotations

import pytest

from connectors.mysql_writer import mysql_type
from connectors.writer_common import parse_decimal_precision_scale, quarantine_unfit_decimals
from services.type_system import ddl_type, materialize_dest_ddl


_BARE_CARRIERS = ("DECIMAL", "NUMERIC", "NUMBER")

# Destinations where bare fixed-point must rematerialize to parameterized SSOT
# (or PG bare NUMERIC). sqlite already covered by P16 → TEXT.
_DESTS = (
    "mysql",
    "sqlserver",
    "oracle",
    "redshift",
    "snowflake",
    "databricks",
    "duckdb",
    "clickhouse",
    "trino",
    "presto",
    "bigquery",
    "iceberg",
    "postgresql",
    "spanner",
    "generic_sql",
)


@pytest.mark.parametrize("dest", _DESTS)
@pytest.mark.parametrize("carrier", _BARE_CARRIERS)
def test_bare_decimal_materialize_matches_ddl_type(dest: str, carrier: str):
    expected = ddl_type(dest, carrier)
    got = materialize_dest_ddl(dest, carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{dest} {carrier}: materialize={got!r} ddl_type={expected!r}"
    )


@pytest.mark.parametrize("dest", ("mysql", "sqlserver", "oracle", "snowflake", "duckdb"))
def test_parameterized_decimal_still_passes_through(dest: str):
    stamp = "DECIMAL(12,4)"
    if dest == "oracle":
        # ddl may legalize NUMBER(12,4); materialize must match ddl_type
        assert materialize_dest_ddl(dest, stamp) == ddl_type(dest, stamp)
    else:
        got = materialize_dest_ddl(dest, stamp)
        assert "12" in got and "4" in got
        assert parse_decimal_precision_scale(got) == (12, 4)


def test_mysql_type_bare_decimal_is_parameterized_not_platform_invent():
    """MySQL bare DECIMAL invents DECIMAL(10,0) — SSOT must stamp (38,15)."""
    t = mysql_type("DECIMAL")
    assert t == "DECIMAL(38,15)"
    assert parse_decimal_precision_scale(t) == (38, 15)


def test_mysql_bare_decimal_quarantine_holds_overflow_after_ssot():
    """After rematerialize, unfit values quarantine instead of silent truncate."""
    details: list[dict] = []
    carrier = mysql_type("DECIMAL")
    # Exceeds DECIMAL(38,15) integer capacity
    overflow = "9" * 30 + "." + "9" * 15
    ok = "123.45"
    out = quarantine_unfit_decimals(
        [(overflow,), (ok,)],
        ["amount"],
        [carrier],
        details,
        policy="quarantine",
        dialect_label="MySQL",
    )
    assert out == [(ok,)]
    assert details and "MySQL" in details[0]["reason"]


def test_snowflake_bare_decimal_create_types_use_ssot_not_batch_invent():
    from connectors.snowflake_writer import resolve_snowflake_create_types

    types = resolve_snowflake_create_types(["DECIMAL", "NUMBER(12,4)"], [("1.5", "1.0000")])
    assert types[0].upper().replace(" ", "") == "NUMBER(38,10)"
    assert "12" in types[1] and "4" in types[1]
