"""Verify MongoDB cursor batches type-cast cursor values for correct $gt semantics."""

from __future__ import annotations

import sys
import uuid
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

pymongo = pytest.importorskip("pymongo")  # noqa: E402
from bson.decimal128 import Decimal128  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from connectors.mongodb_reader import read_collection_cursor_batch  # noqa: E402


def _client():
    return MongoClient("mongodb://localhost:27017/", serverSelectionTimeoutMS=5000)


def test_mongodb_cursor_batch_casts_integer_cursor():
    db_name = f"test_cursor_{uuid.uuid4().hex}"
    client = _client()
    try:
        coll = client[db_name]["orders"]
        for i in range(1, 6):
            coll.insert_one({"order_id": i, "amount": Decimal128(Decimal(f"{i}00.00"))})

        batch = read_collection_cursor_batch(
            cfg={"host": "localhost", "port": 27017},
            database=db_name,
            collection="orders",
            cursor_column="order_id",
            cursor_after="2",
            cursor_type="INTEGER",
            limit=100,
        )
        ids = [int(row[batch.headers.index("order_id")]) for row in batch.rows]
        assert ids == [3, 4, 5]
    finally:
        client.drop_database(db_name)


def test_mongodb_cursor_batch_casts_decimal_cursor():
    db_name = f"test_cursor_{uuid.uuid4().hex}"
    client = _client()
    try:
        coll = client[db_name]["orders"]
        for i in range(1, 6):
            coll.insert_one({"order_id": i, "amount": Decimal128(Decimal(f"{i}00.50"))})

        batch = read_collection_cursor_batch(
            cfg={"host": "localhost", "port": 27017},
            database=db_name,
            collection="orders",
            cursor_column="amount",
            cursor_after="200.50",
            cursor_type="DECIMAL",
            limit=100,
        )
        amounts = [row[batch.headers.index("amount")] for row in batch.rows]
        assert amounts == ["300.50", "400.50", "500.50"]
    finally:
        client.drop_database(db_name)


def test_mongodb_cursor_batch_casts_datetime_cursor():
    db_name = f"test_cursor_{uuid.uuid4().hex}"
    client = _client()
    try:
        coll = client[db_name]["events"]
        for i in range(1, 4):
            coll.insert_one({"event_id": i, "created_at": datetime(2024, 6, i, 12, 0, 0)})

        batch = read_collection_cursor_batch(
            cfg={"host": "localhost", "port": 27017},
            database=db_name,
            collection="events",
            cursor_column="created_at",
            cursor_after="2024-06-01T12:00:00",
            cursor_type="DATETIME",
            limit=100,
        )
        ids = [int(row[batch.headers.index("event_id")]) for row in batch.rows]
        assert ids == [2, 3]
    finally:
        client.drop_database(db_name)


def test_mongodb_cursor_batch_infers_decimal_when_no_type():
    db_name = f"test_cursor_{uuid.uuid4().hex}"
    client = _client()
    try:
        coll = client[db_name]["orders"]
        coll.insert_one({"order_id": 1, "amount": Decimal128(Decimal("100.50"))})
        coll.insert_one({"order_id": 2, "amount": Decimal128(Decimal("200.50"))})

        batch = read_collection_cursor_batch(
            cfg={"host": "localhost", "port": 27017},
            database=db_name,
            collection="orders",
            cursor_column="amount",
            cursor_after="100.50",
            limit=100,
        )
        ids = [int(row[batch.headers.index("order_id")]) for row in batch.rows]
        assert ids == [2]
    finally:
        client.drop_database(db_name)
