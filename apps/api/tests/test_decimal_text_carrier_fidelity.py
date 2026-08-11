"""DECIMAL → TEXT is only a fidelity collapse where DECIMAL exists.

SQLite has no fixed-point carrier: our own DDL catalog maps logical ``decimal``
to TEXT there, and REAL would be the lossy alternative. Calling the catalog's
own best carrier a collapse blocked every CSV/JSON/SQLite → SQLite route in the
PRODUCTION_SKU matrix. Engines that *do* have DECIMAL still collapse.
"""

from __future__ import annotations

from services.type_system import (
    decimal_fixed_point_would_collapse_to_text as collapses,
)
from services.type_system import (
    ddl_type,
    dest_lacks_fixed_point_decimal,
)


def test_sqlite_decimal_carrier_is_text():
    assert ddl_type("sqlite", "DECIMAL(9,4)").upper() == "TEXT"
    assert dest_lacks_fixed_point_decimal("sqlite") is True


def test_sqlite_text_carrier_is_not_a_collapse():
    assert collapses("DECIMAL(9,4)", "TEXT", dest_db="sqlite") is False


def test_engine_with_decimal_still_collapses_to_text():
    assert collapses("DECIMAL(9,4)", "VARCHAR", dest_db="snowflake") is True
    assert collapses("DECIMAL(9,4)", "TEXT", dest_db="postgresql") is True
    assert dest_lacks_fixed_point_decimal("postgresql") is False


def test_unknown_destination_stays_fail_closed():
    # No destination context — keep the conservative verdict, never soft-green.
    assert collapses("DECIMAL(9,4)", "TEXT") is True


def test_numeric_target_is_never_a_text_collapse():
    assert collapses("DECIMAL(9,4)", "NUMBER(9,4)", dest_db="snowflake") is False
