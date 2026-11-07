"""ITEM 1 regression — SA CREATE path must not invent INT32 from bare integer.

``ddl_type(dest, 'integer')`` / ``DDL_TYPES`` invent 64-bit. The SQLAlchemy
CREATE mapper (``_sa_type_for_logical``) used to emit ``sa.Integer()`` for bare
``integer``, so auto-create destinations silently narrowed BIGINT-class values
even when the string DDL path was correct.

This test fails if that SA path regresses to 32-bit.
"""

from __future__ import annotations

import sqlalchemy as sa

from connectors.generic_sql import _sa_type_for_logical
from services.decision_kernel import ddl_type
from services.type_system import DDL_TYPES, LOGICAL_INTEGER, integer_bit_width


def test_sa_bare_integer_is_bigint_matching_ddl_types():
    """Bare logical integer → BigInteger; never sa.Integer (INT32)."""
    sa_t = _sa_type_for_logical("integer", "postgresql", nullable=True)
    assert isinstance(sa_t, sa.BigInteger), type(sa_t)
    # Same authority as string DDL invent.
    assert ddl_type("postgresql", LOGICAL_INTEGER) == DDL_TYPES["postgresql"][LOGICAL_INTEGER]
    assert ddl_type("postgresql", "integer") == "BIGINT"
    assert integer_bit_width(ddl_type("postgresql", "integer")) == 64


def test_sa_explicit_int32_carrier_stays_32():
    """Width-preserving: unambiguous INT4/INT32 stay 32; INTEGER/INT invent 64."""
    for raw in ("INT4", "INT32"):
        sa_t = _sa_type_for_logical(raw, "postgresql", nullable=True)
        assert isinstance(sa_t, sa.Integer), (raw, type(sa_t))
        assert not isinstance(sa_t, sa.BigInteger), raw
    for raw in ("INTEGER", "INT"):
        sa_t = _sa_type_for_logical(raw, "postgresql", nullable=True)
        assert isinstance(sa_t, sa.BigInteger), (raw, type(sa_t))


def test_sa_bigint_carrier_stays_64():
    sa_t = _sa_type_for_logical("BIGINT", "postgresql", nullable=True)
    assert isinstance(sa_t, sa.BigInteger)


def test_sa_bare_integer_never_narrower_across_sql_engines():
    """Cross-check: SA invent is BigInteger; ddl_type matches DDL_TYPES."""
    for dest in (
        "postgresql",
        "mysql",
        "sqlserver",
        "sqlite",
        "duckdb",
        "redshift",
    ):
        sa_t = _sa_type_for_logical("integer", dest, nullable=True)
        assert isinstance(sa_t, sa.BigInteger), (dest, type(sa_t))
        ddl = ddl_type(dest, "integer")
        assert ddl == DDL_TYPES[dest][LOGICAL_INTEGER], (dest, ddl)
        # sqlite INTEGER is affinity-wide (holds 64-bit values); SA still BigInteger.
        if dest == "sqlite":
            continue
        assert integer_bit_width(ddl) == 64 or "38" in (ddl or ""), (dest, ddl)
