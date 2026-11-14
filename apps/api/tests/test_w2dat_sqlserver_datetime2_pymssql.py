"""SQL Server DATETIME2(7) under pymssql (CI C3 pymssql leg, PR #430).

pymssql builds a six-digit ``datetime`` and drops the 100 ns digit, so the
Validate sampler saw ``.123456`` for ``.1234567``, the microsecond-cap exemption
applied, and DATETIME2(7) → PostgreSQL TIMESTAMP(6) ran green while truncating
every value. The reader now reads DATETIME2(7) as SQL Server's style-121 text
under pymssql; pyodbc keeps its column objects (its converter carries the tick).
"""

from __future__ import annotations

from datetime import datetime
from types import SimpleNamespace

import sqlalchemy as sa
from sqlalchemy.dialects import mssql


def _table():
    return sa.Table(
        "events", sa.MetaData(),
        sa.Column("id", sa.Integer),
        sa.Column("at", mssql.DATETIME2(precision=7)),
        sa.Column("at6", mssql.DATETIME2(precision=6)),
    )


def _engine(driver: str):
    return SimpleNamespace(dialect=SimpleNamespace(name="mssql", driver=driver))


def _sql(cols) -> str:
    stmt = sa.select(*cols)
    return str(stmt.compile(dialect=mssql.dialect()))


def test_pymssql_reads_datetime2_7_as_seven_digit_text():
    from connectors.generic_sql import _tz_safe_projection

    table = _table()
    cfg = {"type": "sqlserver"}
    text = _sql(_tz_safe_projection(cfg, list(table.c), _engine("pymssql")))
    assert "convert(varchar(27), events.at, 121) AS at" in text
    assert "events.at6" in text and "convert(varchar(27), events.at6" not in text


def test_pyodbc_keeps_the_column_objects():
    from connectors.generic_sql import _tz_safe_projection

    table = _table()
    out = _tz_safe_projection({"type": "sqlserver"}, list(table.c), _engine("pyodbc"))
    assert [getattr(c, "name", None) for c in out] == ["id", "at", "at6"]
    assert "convert" not in _sql(out).lower()


def test_seventh_digit_in_either_driver_shape_refuses_microsecond_target():
    from connectors.sqlserver_tick import with_tick
    from services.decision_kernel.type_invent import temporal_precision_would_narrow

    # pymssql after the fix: SQL Server's own text, seven digits.
    pymssql_rows = ["2024-01-01 10:00:00.1234567", "2024-01-01 10:00:01.0000001"]
    # pyodbc: the output converter's Datetime2Tick.
    pyodbc_rows = [with_tick(datetime(2024, 1, 1, 10, 0, 0, 123456), 7)]
    for rows in (pymssql_rows, pyodbc_rows):
        assert temporal_precision_would_narrow(
            "DATETIME2(7)", "TIMESTAMP(6)", dest_db="postgresql", population=rows
        ) is True, rows
    # Values with no seventh digit still copy (the exemption is unchanged).
    assert temporal_precision_would_narrow(
        "DATETIME2(7)", "TIMESTAMP(6)", dest_db="postgresql",
        population=["2024-01-01 10:00:00.1234560"],
    ) is False


def test_seven_digit_text_keeps_its_tick_for_a_datetime2_7_copy():
    from connectors.sqlserver_tick import tick_from_text

    assert tick_from_text("2024-01-01 10:00:00.1234567") == 7
