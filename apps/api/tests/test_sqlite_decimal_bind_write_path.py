"""SQLite Decimal binds dest-canonical text, not IEEE float.

apply_transform decimal returns Decimal. SQLAlchemy SCD2 inserts skip
_to_sqlite_value; sqlite3 must still bind exact text.
"""

from __future__ import annotations

import sqlite3
import sys
from decimal import Decimal
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from connectors.sqlite_common import (  # noqa: E402
    register_sqlite_decimal_adapter,
    sqlite_decimal_bind_text,
)
from connectors.sqlite_writer import _to_sqlite_value  # noqa: E402
from services.transform_engine import apply_transform, set_active_number_locale  # noqa: E402


def test_scale_preserving_text():
    assert sqlite_decimal_bind_text(Decimal("10.00")) == "10.00"
    assert sqlite_decimal_bind_text(Decimal("1.2345")) == "1.2345"
    assert _to_sqlite_value(Decimal("10.00"), "DECIMAL") == "10.00"


def test_locale_money_transform_binds_text():
    set_active_number_locale("")
    bound, err = apply_transform("$10.00", "decimal")
    assert err is None
    assert bound == Decimal("10.00")
    assert sqlite_decimal_bind_text(bound) == "10.00"


def test_auto_grouping_still_refuses():
    set_active_number_locale("")
    value, err = apply_transform("1,234", "decimal")
    assert value is None
    assert err
    value, err = apply_transform("1.000", "decimal")
    assert value is None
    assert err


def test_non_finite_refuses():
    with pytest.raises(ValueError, match="non-finite"):
        sqlite_decimal_bind_text(Decimal("Infinity"))
    with pytest.raises(ValueError, match="non-finite"):
        sqlite_decimal_bind_text(Decimal("NaN"))


def test_raw_sqlite3_binds_decimal_as_text():
    register_sqlite_decimal_adapter()
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE t (price TEXT)")
    conn.execute("INSERT INTO t (price) VALUES (?)", (Decimal("10.00"),))
    assert conn.execute("SELECT price FROM t").fetchone()[0] == "10.00"
    conn.close()


def test_sqlalchemy_sqlite_inserts_decimal_as_text(tmp_path: Path):
    from connectors.generic_sql import get_sqlalchemy_engine
    import sqlalchemy as sa

    db = tmp_path / "scd2_decimal.db"
    cfg = {
        "type": "sqlite",
        "database": str(db),
        "connection_string": f"sqlite:///{db}",
    }
    engine = get_sqlalchemy_engine(cfg)
    with engine.begin() as conn:
        conn.execute(sa.text("CREATE TABLE products (price TEXT)"))
        bound, err = apply_transform("10.00", "decimal")
        assert err is None
        conn.execute(
            sa.text("INSERT INTO products (price) VALUES (:p)"),
            {"p": bound},
        )
        stored = conn.execute(sa.text("SELECT price FROM products")).scalar()
    assert stored == "10.00"
