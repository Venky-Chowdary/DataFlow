"""Reversibility gate: fifth adversarial review.

Real engine runs (SQLite files, in-process job store) wherever the finding was
confirmed on the engine; the rest at the function the finding names.
"""

from __future__ import annotations

import pytest

from tests.test_reversibility_review3 import _q, _run, fake  # noqa: F401

# ------------------------------------------- C-1 / M-3: the destination engine by its family

#: (value, declared type, canonical engine, alias): the alias must answer as the canonical engine does.
ALIASED_CASTS = [
    ("2024-01-02 10:00:00.500000", "TIMESTAMP", "postgresql", "supabase"),
    ("2024-01-02 10:00:00.5", "TIMESTAMP", "postgresql", "amazon_rds_postgresql"),
    ("3.14159265", "REAL", "postgresql", "supabase"),
    ("1", "DOUBLE PRECISION", "postgresql", "neon"),
    ("0.1", "REAL", "postgresql", "alloydb"),
    ("3.14159265", "REAL", "sqlserver", "azure_sql_database"),
    ("10:00:00.500000", "TIME", "postgresql", "supabase"),
]


@pytest.mark.parametrize("value, ddl, canonical, alias", ALIASED_CASTS)
def test_c1_m3_the_cast_rule_renders_an_aliased_destination_as_its_engine(value, ddl, canonical, alias):
    from services.reversibility_render import cast_changes

    expected = cast_changes(value, None, ddl, canonical, has_converted=False)
    assert expected is not None
    assert cast_changes(value, None, ddl, alias, has_converted=False) is expected, (value, ddl, alias)


def test_c1_m3_the_gate_names_the_destination_engine_by_its_family():
    from types import SimpleNamespace

    from services.reversibility_sources import _engine

    assert _engine({"format": "supabase"}) == "postgresql"
    assert _engine(SimpleNamespace(format="Amazon_RDS_PostgreSQL")) == "postgresql"
    assert _engine({"type": "azure_sql_database"}) == "sqlserver"
    assert _engine({"format": "amazon_rds_oracle"}) == "oracle"
    assert _engine({"format": "amazon_rds_mysql"}) == "mysql"
    assert _engine({"format": "csv"}) == "csv" and _engine({"format": "sqlite"}) == "sqlite"


# ------------------------------------- C-2 / M-3: floating point and declared precision

@pytest.mark.parametrize("value, ddl, engine, changed", [
    # MySQL shows a single-precision FLOAT to six significant digits.
    ("1.234567", "FLOAT", "mysql", True),
    ("16777217", "FLOAT", "mysql", True),
    ("1.23457", "FLOAT", "mysql", False),
    ("1.5", "FLOAT", "mysql", False),
    ("1.5", "FLOAT32", "mysql", False),
    ("0.1", "FLOAT32", "amazon_rds_mysql", False),
    ("1.0000001", "FLOAT32", "mysql", True),
    ("1e-07", "FLOAT", "mysql", True),
    ("1.5", "FLOAT(7,3)", "mysql", True),  # a FLOAT(M,D) shows D decimals: 1.500
    ("1.500", "FLOAT(7,3)", "mysql", False),
    ("0.1", "DOUBLE", "mysql", False),
    # Single precision elsewhere keeps the shortest text that reads back as the same float4.
    ("3.14159265", "REAL", "postgresql", True),
    ("16777217", "REAL", "postgresql", True),
    ("16777217", "DOUBLE PRECISION", "postgresql", False),
    ("0.1", "REAL", "postgresql", False),
    ("3.14159265", "REAL", "sqlserver", True),
    # SQL Server shows an integral float without ".0".
    ("1", "FLOAT", "sqlserver", False),
    ("1", "REAL", "sqlserver", False),
    ("1.0", "FLOAT", "sqlserver", True),
])
def test_m3_a_float_cast_from_text_is_checked_by_the_engines_display(value, ddl, engine, changed):
    from services.reversibility_render import cast_changes

    assert cast_changes(value, None, ddl, engine, has_converted=False) is changed, (value, ddl, engine)


@pytest.mark.parametrize("value, ddl, engine, changed", [
    ("2024-01-02 10:00:00.5", "TIMESTAMP(0)", "postgresql", True),
    ("10:00:00.5", "TIME(0)", "postgresql", True),
    ("2024-01-02 10:00:00", "TIMESTAMP(0)", "postgresql", False),
    ("2024-01-02 10:00:00.5", "TIMESTAMP(3)", "postgresql", False),
    ("2024-01-02 10:00:00.1234", "TIMESTAMP(3)", "postgresql", True),
    ("2024-01-02 10:00:00.5", "TIMESTAMP_NTZ(0)", "postgresql", True),
    ("2024-01-02 10:00:00.500000", "DATETIME", "mysql", True),  # a bare MySQL DATETIME keeps whole seconds
    ("2024-01-02 10:00:00", "DATETIME", "mysql", False),
    ("10:00:00.250000", "TIME(6)", "mysql", False),
])
def test_m3_a_timestamp_cast_honours_the_declared_fractional_precision(value, ddl, engine, changed):
    from services.reversibility_render import cast_changes

    assert cast_changes(value, None, ddl, engine, has_converted=False) is changed, (value, ddl, engine)


def test_m3_the_writer_check_compares_what_the_writer_converted_too():
    """The text renders back as itself, but the writer converted it to another value: that lands, so it changes."""
    from services import reversibility_render
    from services.reversibility_enforce import changes

    rule = {"code": "cast", "target_type": "INTEGER", "dest_db": "sqlite"}
    assert changes(rule, "7", 7) is False
    assert changes(rule, "7", 8) is True
    assert reversibility_render.cast_changes("2024-01-05", "2024-1-5", "DATE", "postgresql") is True
    assert reversibility_render.cast_changes("2024-01-05", __import__("datetime").date(2024, 1, 6), "DATE",
                                             "supabase") is True
