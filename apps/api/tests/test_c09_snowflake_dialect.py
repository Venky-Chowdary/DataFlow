"""C09 regression: the snowflake SQLAlchemy dialect is packaged (D47/D30).

Mirror/SCD2 and every keyed Snowflake mode reach
``generic_sql.get_sqlalchemy_engine`` — with only ``snowflake-connector-python``
installed the dialect is missing and every run failed
'SQLAlchemy dialect/driver for snowflake is not available'. The package must
be declared in the deploy requirements AND resolvable at engine construction.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_REQUIREMENTS = Path(__file__).resolve().parents[1] / "requirements.txt"


def test_requirements_declares_snowflake_sqlalchemy():
    lines = [
        ln.strip()
        for ln in _REQUIREMENTS.read_text().splitlines()
        if ln.strip() and not ln.strip().startswith("#")
    ]
    pins = [ln for ln in lines if ln.lower().startswith("snowflake-sqlalchemy")]
    assert pins, "snowflake-sqlalchemy missing from apps/api/requirements.txt"
    spec = pins[0]
    # 2.x requires connector>=5.0.0 + Python 3.11 — above the deploy floor.
    assert re.search(r"<2\b", spec), f"pin must stay on 1.x: {spec}"


def test_snowflake_dialect_resolves():
    """create_engine resolves the snowflake dialect without connecting."""
    pytest.importorskip("sqlalchemy")
    pytest.importorskip("snowflake.sqlalchemy")
    from sqlalchemy import create_engine

    engine = create_engine(
        "snowflake://u:p@acct.example.com/DB/PUBLIC?warehouse=WH"
    )
    try:
        assert engine.dialect.name == "snowflake"
    finally:
        engine.dispose()
