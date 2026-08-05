"""SQLite Map≡CREATE — UUID/JSON/TIMESTAMP NUMERIC affinity invent.

Foreign stamps that are not INT/CHAR/CLOB/BLOB/REAL get SQLite NUMERIC
affinity. Digit-looking UUIDs/timestamps/JSON numbers silently become
integer/real (GUID '550e8400' → inf). ddl_type SSOT is TEXT/INTEGER.
"""

from __future__ import annotations

import sqlite3
import tempfile
from pathlib import Path

import pytest

from connectors.sqlite_writer import sqlite_type
from services.type_system import ddl_type, materialize_dest_ddl


_FOREIGN_STAMPS = (
    "UUID",
    "GUID",
    "UNIQUEIDENTIFIER",
    "JSON",
    "JSONB",
    "TIMESTAMP",
    "TIMESTAMPTZ",
    "TIMESTAMP_NTZ",
    "DATETIME",
    "DATETIME2",
    "DATE",
    "TIME",
    "BOOLEAN",
    "BIT",
    "ENUM",
    "INET",
    "STRING",
    "VARIANT",
    "SUPER",
    "OBJECTID",
    "VECTOR(768)",
    "TIMESTAMP WITH TIME ZONE",
)


@pytest.mark.parametrize("carrier", _FOREIGN_STAMPS)
def test_sqlite_foreign_stamp_rematerializes_to_ddl_type(carrier: str):
    expected = ddl_type("sqlite", carrier)
    got = materialize_dest_ddl("sqlite", carrier)
    assert got.upper().replace(" ", "") == expected.upper().replace(" ", ""), (
        f"{carrier}: materialize={got!r} ddl_type={expected!r}"
    )
    assert sqlite_type(carrier) == got


def test_sqlite_uuid_create_preserves_digit_looking_id():
    """E2E: UUID stamp must be TEXT — '12345' must not become integer 12345."""
    assert sqlite_type("UUID") == "TEXT"
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "uuid.db"
        con = sqlite3.connect(db)
        try:
            con.execute(f"CREATE TABLE t (id {sqlite_type('UUID')})")
            con.execute("INSERT INTO t VALUES (?)", ("12345",))
            kind, stored = con.execute("SELECT typeof(id), id FROM t").fetchone()
            assert kind == "text"
            assert stored == "12345"
        finally:
            con.close()


def test_sqlite_guid_create_preserves_hex_prefix_not_inf():
    """Regression: GUID affinity stored '550e8400' as real inf."""
    assert sqlite_type("GUID") == "TEXT"
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "guid.db"
        con = sqlite3.connect(db)
        try:
            con.execute(f"CREATE TABLE t (id {sqlite_type('GUID')})")
            con.execute("INSERT INTO t VALUES (?)", ("550e8400",))
            kind, stored = con.execute("SELECT typeof(id), id FROM t").fetchone()
            assert kind == "text"
            assert stored == "550e8400"
        finally:
            con.close()


def test_sqlite_json_create_preserves_high_precision_number_string():
    payload = "12.345678901234567890"
    assert sqlite_type("JSON") == "TEXT"
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "json.db"
        con = sqlite3.connect(db)
        try:
            con.execute(f"CREATE TABLE t (doc {sqlite_type('JSON')})")
            con.execute("INSERT INTO t VALUES (?)", (payload,))
            kind, stored = con.execute("SELECT typeof(doc), doc FROM t").fetchone()
            assert kind == "text"
            assert stored == payload
        finally:
            con.close()


def test_sqlite_affinity_invent_regression_anchor_uuid_numeric():
    """Bare CREATE UUID still has NUMERIC affinity without rematerialize."""
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bad.db"
        con = sqlite3.connect(db)
        try:
            con.execute("CREATE TABLE t (id UUID)")
            con.execute("INSERT INTO t VALUES (?)", ("12345",))
            kind, stored = con.execute("SELECT typeof(id), id FROM t").fetchone()
            assert kind == "integer"
            assert stored == 12345
        finally:
            con.close()
