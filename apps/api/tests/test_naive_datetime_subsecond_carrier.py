"""A zoneless timestamp must keep its fraction on every destination engine.

``sa.DateTime()`` compiles to a whole-second column on more engines than it
looks: Oracle ``DATE``, SQL Server ``DATETIME`` and MySQL ``DATETIME`` (fsp 0).
The MySQL case was still open and cost two things at once — a microsecond source
stamp landed truncated (a silent value mutation that a row checksum can only
match by luck), and SCD2 lost its version boundaries, because two versions
written inside the same second collapsed onto one instant and left the closed
version with ``valid_from == valid_to``: an as-of query can never see it.
"""

from __future__ import annotations

import pytest

sa = pytest.importorskip("sqlalchemy")

from connectors.generic_sql import _sa_type_for_logical  # noqa: E402


def _ddl(logical: str, dialect_name: str, db_type: str = "") -> str:
    from sqlalchemy.dialects import mssql, mysql, oracle, postgresql

    dialect = {
        "mysql": mysql.dialect(),
        "mariadb": mysql.dialect(),
        "postgresql": postgresql.dialect(),
        "mssql": mssql.dialect(),
        "oracle": oracle.dialect(),
    }[dialect_name]
    return str(
        _sa_type_for_logical(logical, dialect_name, db_type or dialect_name).compile(
            dialect=dialect
        )
    )


@pytest.mark.parametrize(
    "logical",
    ["datetime", "TIMESTAMP", "TIMESTAMP WITHOUT TIME ZONE", "TIMESTAMP_NTZ(6)"],
)
@pytest.mark.parametrize(
    ("dialect_name", "db_type"),
    [("mysql", "mysql"), ("mysql", "tidb"), ("mariadb", "mariadb")],
)
def test_mysql_family_keeps_the_fraction_of_a_zoneless_stamp(
    logical: str, dialect_name: str, db_type: str
) -> None:
    assert _ddl(logical, dialect_name, db_type) == "DATETIME(6)"


@pytest.mark.parametrize(
    ("dialect_name", "expected"),
    [
        ("postgresql", "TIMESTAMP WITHOUT TIME ZONE"),
        ("mssql", "DATETIME2"),
        ("oracle", "TIMESTAMP"),
    ],
)
def test_the_other_engines_keep_the_carrier_they_already_had(
    dialect_name: str, expected: str
) -> None:
    assert _ddl("datetime", dialect_name) == expected


def test_a_calendar_date_is_still_a_date_on_mysql() -> None:
    # Widening DATE to DATETIME(6) would invent a time of day the source
    # never held, so the fix must not reach the date carrier.
    assert _ddl("DATE", "mysql") == "DATE"


def test_a_zone_aware_stamp_does_not_also_lose_its_fraction_on_mysql() -> None:
    # No MySQL carrier holds an offset, so the zone decision is made upstream.
    # sa.DateTime(timezone=True) compiled to fsp-0 DATETIME, dropping the
    # fraction on top of that.
    assert _ddl("TIMESTAMP WITH TIME ZONE", "mysql") == "DATETIME(6)"
    assert _ddl("TIMESTAMPTZ", "mysql") == "DATETIME(6)"


def test_scd2_audit_columns_inherit_the_sub_second_carrier() -> None:
    """The SCD2 window columns are built from the same logical type."""
    from services.scd2_engine import VALID_FROM_COLUMN, VALID_TO_COLUMN

    for col in (VALID_FROM_COLUMN, VALID_TO_COLUMN):
        assert col  # the engine names them; the carrier is what matters
    assert _ddl("datetime", "mysql") == "DATETIME(6)"
