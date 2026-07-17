"""Verify upsert (idempotent resume) semantics for SQL destinations."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from connectors.generic_sql import write_mapped_rows as generic_sql_write  # noqa: E402
from connectors.sqlite_writer import write_mapped_rows as sqlite_write  # noqa: E402

duckdb = pytest.importorskip("duckdb")


def _mapping(source: str, target: str) -> dict:
    return {"source": source, "target": target, "confidence": 0.95}


def test_generic_sql_duckdb_upsert():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "upsert.duckdb"
        common = {
            "host": "",
            "port": 0,
            "database": str(p),
            "username": "",
            "password": "",
            "schema": "",
            "connection_string": "",
            "ssl": False,
            "type": "duckdb",
            "table_name": "payments",
            "headers": ["id", "amount"],
            "mappings": [_mapping("id", "id"), _mapping("amount", "amount")],
            "column_types": {"id": "INTEGER", "amount": "DECIMAL"},
        }

        r1 = generic_sql_write(
            **common,
            data_rows=[["1", "1000.00"], ["2", "2000.50"]],
            create_table=True,
            write_mode="insert",
        )
        assert r1.ok, r1.error

        r2 = generic_sql_write(
            **common,
            data_rows=[["1", "1111.00"], ["3", "3000.00"]],
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
        )
        assert r2.ok, r2.error

        con = duckdb.connect(str(p))
        rows = con.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        con.close()
        assert rows == [(1, 1111.0), (2, 2000.5), (3, 3000.0)]


def test_sqlite_writer_upsert():
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "upsert.sqlite"
        common = {
            "host": "",
            "port": 0,
            "database": str(p),
            "username": "",
            "password": "",
            "schema": "",
            "connection_string": "",
            "ssl": False,
            "table_name": "payments",
            "headers": ["id", "amount"],
            "mappings": [_mapping("id", "id"), _mapping("amount", "amount")],
            "column_types": {"id": "INTEGER", "amount": "DECIMAL"},
        }

        r1 = sqlite_write(
            **common,
            data_rows=[["1", "1000.00"], ["2", "2000.50"]],
            create_table=True,
            write_mode="insert",
        )
        assert r1.ok, r1.error

        r2 = sqlite_write(
            **common,
            data_rows=[["1", "1111.00"], ["3", "3000.00"]],
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
        )
        assert r2.ok, r2.error

        import sqlite3

        con = sqlite3.connect(str(p))
        rows = con.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        con.close()
        assert rows == [(1, "1111.00"), (2, "2000.50"), (3, "3000.00")]


def test_generic_sql_duckdb_upsert_dedupes_within_batch():
    """If the source batch contains duplicate conflict keys, the last value wins."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp) / "dedup.duckdb"
        common = {
            "host": "",
            "port": 0,
            "database": str(p),
            "username": "",
            "password": "",
            "schema": "",
            "connection_string": "",
            "ssl": False,
            "type": "duckdb",
            "table_name": "payments",
            "headers": ["id", "amount"],
            "mappings": [_mapping("id", "id"), _mapping("amount", "amount")],
            "column_types": {"id": "INTEGER", "amount": "DECIMAL"},
        }

        r1 = generic_sql_write(
            **common,
            data_rows=[["1", "1000.00"], ["2", "2000.50"]],
            create_table=True,
            write_mode="insert",
        )
        assert r1.ok, r1.error

        r2 = generic_sql_write(
            **common,
            data_rows=[["1", "1111.00"], ["1", "2222.00"], ["3", "3000.00"]],
            create_table=False,
            write_mode="upsert",
            conflict_columns=["id"],
        )
        assert r2.ok, r2.error

        con = duckdb.connect(str(p))
        rows = con.execute("SELECT id, amount FROM payments ORDER BY id").fetchall()
        con.close()
        assert rows == [(1, 2222.0), (2, 2000.5), (3, 3000.0)]
