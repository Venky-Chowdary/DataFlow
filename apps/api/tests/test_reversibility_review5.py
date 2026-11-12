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
