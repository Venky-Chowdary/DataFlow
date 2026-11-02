"""C02 — temporal edge values: carry natively or refuse with a named reason.

PostgreSQL stores ``infinity``/``-infinity`` as real data on DATE/TIMESTAMP/
TIMESTAMPTZ. Bounded carriers (MySQL DATETIME, SQL Server DATETIME/
SMALLDATETIME, warehouse 0001..9999 windows) cannot hold every finite value a
source produces. Policy: carry the value on the destination that natively
supports it, else refuse at preflight / quarantine the row with a named
reason — never clamp to a finite date and never fail after write.
"""

from __future__ import annotations

import pytest

from connectors.sql_temporal import (
    coerce_sql_temporal,
    dest_temporal_bounds,
    pg_temporal_special,
    wire_check_temporal,
)


@pytest.mark.parametrize("raw", ["infinity", "+infinity", "Infinity", "  INFINITY  "])
def test_pg_positive_infinity_recognized(raw):
    assert pg_temporal_special(raw) == "infinity"


@pytest.mark.parametrize("raw", ["-infinity", " -INFINITY "])
def test_pg_negative_infinity_recognized(raw):
    assert pg_temporal_special(raw) == "-infinity"


@pytest.mark.parametrize("engine", ["postgresql", "postgres", "redshift"])
def test_infinity_carries_verbatim_on_pg_family(engine):
    assert coerce_sql_temporal("infinity", "TIMESTAMPTZ", engine=engine) == "infinity"
    assert coerce_sql_temporal("-infinity", "DATE", engine=engine) == "-infinity"


@pytest.mark.parametrize(
    "engine", ["snowflake", "sqlite", "mysql", "sqlserver", "bigquery", ""]
)
def test_infinity_refused_with_named_reason_off_pg(engine):
    with pytest.raises(ValueError, match="infinity.*no .* carrier|quarantined"):
        coerce_sql_temporal("infinity", "TIMESTAMP", engine=engine)


def test_infinity_never_clamped_to_finite_date():
    """The exact D63 symptom: silent 9999-12-31 must not appear anywhere."""
    for engine in ("snowflake", "sqlite", "mysql", "sqlserver"):
        with pytest.raises(ValueError):
            coerce_sql_temporal("infinity", "TIMESTAMPTZ", engine=engine)
    assert coerce_sql_temporal("infinity", "TIMESTAMPTZ", engine="postgresql") != "9999-12-31"


def test_wire_check_surfaces_named_reason_not_post_write_error():
    out = wire_check_temporal("infinity", "TIMESTAMPTZ", engine="snowflake")
    assert out["ok"] is False
    assert "infinity" in out["reason"]
    assert "quarantined" in out["reason"]


def test_epoch_resolves_to_real_instant():
    got = coerce_sql_temporal("epoch", "TIMESTAMP", engine="bigquery")
    assert got is not None
    # BigQuery bare TIMESTAMP is an instant carrier → aware UTC.
    assert getattr(got, "year", None) == 1970
    assert getattr(got, "month", None) == 1


def test_mysql_datetime_bounds():
    assert dest_temporal_bounds("mysql", "DATETIME")[0].year == 1000
    assert dest_temporal_bounds("mysql", "TIMESTAMP") is None  # epoch branch owns it


def test_mysql_datetime_out_of_range_refused():
    with pytest.raises(ValueError, match="outside mysql DATETIME range"):
        coerce_sql_temporal("0999-06-01", "DATETIME", engine="mysql")


def test_unparseable_year_beyond_carriers_refused():
    """'10000-01-01' cannot become a datetime — refuse before write anyway."""
    with pytest.raises(ValueError, match="year 10000 is outside"):
        coerce_sql_temporal("10000-01-01", "DATETIME", engine="mysql")
    with pytest.raises(ValueError, match="year 10000 is outside"):
        coerce_sql_temporal("10000-01-01", "DATE", engine="snowflake")


def test_sqlserver_smalldatetime_upper_bound():
    with pytest.raises(ValueError, match="SMALLDATETIME range"):
        coerce_sql_temporal("2080-01-01", "SMALLDATETIME", engine="sqlserver")
    assert coerce_sql_temporal("2079-06-06", "SMALLDATETIME", engine="sqlserver") is not None


def test_sqlserver_datetime_lower_bound():
    with pytest.raises(ValueError, match="DATETIME range"):
        coerce_sql_temporal("1752-12-31", "DATETIME", engine="mssql")


def test_unbounded_carriers_do_not_refuse():
    # PG DATE holds year 10000; SQLite binds text. Neither may refuse.
    assert coerce_sql_temporal("10000-01-01", "DATE", engine="postgresql") == "10000-01-01"
    assert coerce_sql_temporal("0999-01-01", "DATETIME", engine="sqlite") is not None


def test_in_range_values_unchanged():
    assert coerce_sql_temporal("2024-06-01", "DATETIME", engine="mysql") is not None
    assert coerce_sql_temporal("9999-12-31", "DATE", engine="snowflake") is not None
