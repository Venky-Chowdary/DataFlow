"""Unit tests for the SCD2 history engine."""

from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.services.scd2_engine import (
    IS_CURRENT_COLUMN,
    ROW_HASH_COLUMN,
    VALID_FROM_COLUMN,
    VALID_TO_COLUMN,
    apply_scd2,
)


def _sqlite_endpoint(path: Path, table: str = "products"):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(
        kind="database",
        format="sqlite",
        connection_string=f"sqlite:///{path}",
        database=str(path),
        table=table,
    )


def _records():
    return [
        {"id": "1", "name": "A", "price": "10.00"},
        {"id": "2", "name": "B", "price": "20.00"},
    ]


def test_scd2_initial_load_creates_history_table():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        summary = apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 2
        assert summary["active_rows"] == 2
        assert summary["updated_rows"] == 0
        assert summary["active_checksum"]

        conn = sqlite3.connect(db_path)
        cur = conn.execute(f"SELECT * FROM products WHERE {IS_CURRENT_COLUMN} = 1")
        rows = cur.fetchall()
        assert len(rows) == 2
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scd2_update_closes_old_version_and_inserts_new():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )

        changed = [
            {"id": "1", "name": "A-updated", "price": "10.00"},
            {"id": "2", "name": "B", "price": "20.00"},
        ]
        summary = apply_scd2(
            endpoint,
            changed,
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 1
        assert summary["updated_rows"] == 1
        assert summary["active_rows"] == 2

        conn = sqlite3.connect(db_path)
        cur = conn.execute(f"SELECT id, name, {IS_CURRENT_COLUMN}, {VALID_TO_COLUMN} FROM products ORDER BY id, {VALID_FROM_COLUMN}")
        rows = cur.fetchall()
        assert len(rows) == 3
        # Two current rows: id 1 updated and id 2 unchanged.
        assert sum(1 for r in rows if r[2] == 1) == 2
        # One historical row for id 1 with valid_to set.
        historical = [r for r in rows if r[0] == "1" and r[2] == 0]
        assert len(historical) == 1
        assert historical[0][3] is not None
        conn.close()
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scd2_reidentical_snapshot_is_idempotent():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path))
        apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        summary = apply_scd2(
            endpoint,
            _records(),
            columns=["id", "name", "price"],
            schema={"id": "string", "name": "string", "price": "decimal"},
            mappings=None,
            conflict_columns=["id"],
        )
        assert summary["rows_written"] == 0
        assert summary["updated_rows"] == 0
        assert summary["active_rows"] == 2
    finally:
        Path(db_path).unlink(missing_ok=True)


def test_scd2_composite_primary_key():
    fd, db_path = tempfile.mkstemp(suffix=".db")
    try:
        endpoint = _sqlite_endpoint(Path(db_path), table="line_items")
        rows = [
            {"order_id": "o1", "line": "1", "sku": "A"},
            {"order_id": "o1", "line": "2", "sku": "B"},
        ]
        summary = apply_scd2(
            endpoint,
            rows,
            columns=["order_id", "line", "sku"],
            schema={"order_id": "string", "line": "string", "sku": "string"},
            mappings=None,
            conflict_columns=["order_id", "line"],
        )
        assert summary["rows_written"] == 2
        assert summary["primary_key_columns"] == ["order_id", "line"]

        updated = [
            {"order_id": "o1", "line": "1", "sku": "A2"},
            {"order_id": "o1", "line": "2", "sku": "B"},
        ]
        summary2 = apply_scd2(
            endpoint,
            updated,
            columns=["order_id", "line", "sku"],
            schema={"order_id": "string", "line": "string", "sku": "string"},
            mappings=None,
            conflict_columns=["order_id", "line"],
        )
        assert summary2["rows_written"] == 1
        assert summary2["updated_rows"] == 1
        assert summary2["active_rows"] == 2
    finally:
        Path(db_path).unlink(missing_ok=True)
