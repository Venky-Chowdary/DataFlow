"""The append collision probe must reach a verdict on case-folding engines.

Two defects made an Oracle append lose its pre-write duplicate verdict and hit
``ORA-00001`` mid-write instead: the probe addressed a folded table name that
did not exist, and its ``Text`` cast compiled to CLOB, which Oracle refuses in a
comparison (ORA-22849). Both degraded to ``status="error"`` — a skip, never
proof of a clean append.
"""

from __future__ import annotations

import sqlalchemy as sa

from services.destination_key_collision_probe import (
    _BOUNDED_TEXT_LEN,
    key_comparison_carrier,
)


def test_oracle_keys_compare_through_a_bounded_varchar() -> None:
    carrier = key_comparison_carrier("oracle", ["1", "2"])
    assert isinstance(carrier, sa.String) and not isinstance(carrier, sa.Text)
    assert carrier.length == _BOUNDED_TEXT_LEN


def test_keys_wider_than_the_carrier_are_not_compared_truncated() -> None:
    carrier = key_comparison_carrier("oracle", ["x" * (_BOUNDED_TEXT_LEN + 1)])
    assert isinstance(carrier, sa.Text)


def test_other_dialects_keep_unbounded_text() -> None:
    for dialect in ("postgresql", "mysql", "mssql", "sqlite", "snowflake"):
        assert isinstance(key_comparison_carrier(dialect, ["1"]), sa.Text)


def test_oracle_cast_compiles_without_a_lob() -> None:
    from sqlalchemy.dialects import oracle as oracle_dialect

    col = sa.column("id")
    stmt = sa.select(col).where(
        sa.cast(col, key_comparison_carrier("oracle", ["1"])).in_(["1"])
    )
    sql = str(stmt.compile(dialect=oracle_dialect.dialect()))
    assert "CLOB" not in sql.upper()
    assert "VARCHAR" in sql.upper()
