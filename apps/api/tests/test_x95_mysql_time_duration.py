"""X-95: a bound MySQL source ``TIME`` is a duration (±838:59:59), not a
wall clock. Create-new DDL must carry it as INTERVAL where the engine holds
one (PostgreSQL, DuckDB), exact duration text where it does not (SQL Server,
SQLite, Snowflake…), and keep MySQL's own duration TIME on round-trip.

An *unbound* ``TIME`` stays a wall clock — a PG/SQL Server source must not
be reinterpreted as a MySQL duration.
"""

from __future__ import annotations

from datetime import timedelta

from services.source_engine_scope import bind_source_engine
from services.decision_kernel.type_invent import ddl_type


def _ddl(dest: str) -> str:
    with bind_source_engine("mysql"):
        return ddl_type(dest, "TIME")


def test_mysql_source_time_maps_to_interval_on_pg_and_duckdb():
    assert _ddl("postgresql") == "INTERVAL"
    assert _ddl("duckdb") == "INTERVAL"


def test_mysql_source_time_roundtrip_keeps_time():
    assert _ddl("mysql") == "TIME(6)"


def test_mysql_source_time_maps_to_text_on_wallclock_only_engines():
    # SQL Server TIME is 0–24h; a duration must land as exact text instead.
    for dest in ("sqlserver", "mssql", "sqlite"):
        ddl = _ddl(dest)
        logical = ddl.upper()
        assert (
            "CHAR" in logical or "TEXT" in logical or "VARCHAR" in logical
        ), f"{dest} stamped non-text carrier {ddl!r} for a duration source"


def test_unbound_time_stays_wallclock():
    with bind_source_engine("postgresql"):
        ddl = ddl_type("mysql", "TIME")
    assert ddl.upper().startswith("TIME"), ddl


def test_duration_renders_canonical_text_on_string_carrier():
    # timedelta(days=-1, 23:00:00) is pymysql's read of MySQL '-01:00:00';
    # str() spells "-1 day, 23:00:00" — the wire must be "-01:00:00".
    from datetime import timedelta as td

    from services.value_serializer import _format_timedelta

    assert _format_timedelta(td(hours=-1)) == "-01:00:00"
    assert _format_timedelta(td(seconds=-3020399)) == "-838:59:59"
    assert _format_timedelta(td(hours=100, minutes=30, seconds=5)) == "100:30:05"


def test_timedelta_binds_canonical_text_on_generic_string_carrier():
    # The generic bind path for a string target must render canonical
    # duration text — not the Python "-1 day, 23:00:00" repr.
    from connectors.generic_sql import _to_sa_value

    bound = _to_sa_value(
        timedelta(seconds=-3020399),
        logical="VARCHAR(64)",
        db_type="sqlserver",
    )
    assert bound == "-838:59:59"


def test_timedelta_binds_duration_text_on_mysql_time_carrier():
    from connectors.generic_sql import _to_sa_value

    bound = _to_sa_value(
        timedelta(seconds=-3020399),
        logical="TIME(6)",
        db_type="mysql",
    )
    assert bound == "-838:59:59"
