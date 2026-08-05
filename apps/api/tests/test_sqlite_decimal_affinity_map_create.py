"""SQLite Map≡CREATE — DECIMAL affinity invent causes silent IEEE loss."""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

from connectors.sqlite_writer import sqlite_type
from services.type_system import ddl_type, materialize_dest_ddl


_HIGH_PRECISION = "12345678901234567890.123456789012345678"


def test_materialize_sqlite_decimal_rematerializes_to_text():
    """ddl_type SSOT is TEXT; materialize must not pass through DECIMAL affinity."""
    for carrier in (
        "DECIMAL",
        "DECIMAL(38,18)",
        "NUMERIC(12,2)",
        "NUMBER(10,2)",
        "BIGNUMERIC(76,38)",
    ):
        assert ddl_type("sqlite", carrier) == "TEXT", carrier
        assert materialize_dest_ddl("sqlite", carrier) == "TEXT", carrier
        assert sqlite_type(carrier) == "TEXT", carrier


def test_materialize_sqlite_money_rematerializes_to_text():
    assert ddl_type("sqlite", "MONEY") == "TEXT"
    assert materialize_dest_ddl("sqlite", "MONEY") == "TEXT"
    assert sqlite_type("MONEY") == "TEXT"
    assert materialize_dest_ddl("sqlite", "SMALLMONEY") == "TEXT"


def test_sqlite_create_decimal_stamp_preserves_exact_string():
    """E2E: CREATE TEXT, typeof=text, exact digits — not IEEE real invent."""
    assert sqlite_type("DECIMAL(38,18)") == "TEXT"
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "affinity.db"
        con = sqlite3.connect(db)
        try:
            con.execute(f"CREATE TABLE t (amt {sqlite_type('DECIMAL(38,18)')})")
            con.execute("INSERT INTO t VALUES (?)", (_HIGH_PRECISION,))
            con.commit()
            kind, stored = con.execute("SELECT typeof(amt), amt FROM t").fetchone()
            assert kind == "text"
            assert stored == _HIGH_PRECISION
        finally:
            con.close()


def test_sqlite_affinity_invent_would_corrupt_without_text_wire():
    """Regression anchor: DECIMAL affinity silently stores as real."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bad.db"
        con = sqlite3.connect(db)
        try:
            con.execute("CREATE TABLE t (amt DECIMAL(38,18))")
            con.execute("INSERT INTO t VALUES (?)", (_HIGH_PRECISION,))
            kind, stored = con.execute("SELECT typeof(amt), amt FROM t").fetchone()
            assert kind == "real"
            assert stored != _HIGH_PRECISION
        finally:
            con.close()
