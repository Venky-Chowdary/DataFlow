"""Database sources → CSV file export end-to-end matrix."""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def _export(request: TransferRequest) -> None:
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, uuid.uuid4().hex[:24])
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert "path" in result.destination_summary
    path = result.destination_summary["path"]
    assert os.path.exists(path)
    assert os.path.getsize(path) > 0


def _file_export() -> EndpointConfig:
    return EndpointConfig(kind="file_export", format="csv")


def test_postgresql_to_csv():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import psycopg2

    src_table = "pg_to_csv_src_" + uuid.uuid4().hex[:8]
    conn = psycopg2.connect(
        host="localhost", port=5432, database="dataflow",
        user="dataflow", password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, name TEXT)")
        cur.execute(f"INSERT INTO {src_table} VALUES (1,'alice'),(2,'bob')")
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host="localhost", port=5432, database="dataflow",
            username="dataflow", password="dataflow",
            schema="public", table=src_table,
        ),
        destination=_file_export(),
        skip_preflight=True,
    )
    _export(request)


def test_mysql_to_csv():
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import pymysql

    src_table = "mysql_to_csv_src_" + uuid.uuid4().hex[:8]
    conn = pymysql.connect(
        host="localhost", port=3306, user="dataflow",
        password="dataflow", database="dataflow",
    )
    conn.autocommit(True)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TABLE {src_table} (id INT PRIMARY KEY, name VARCHAR(100))")
        cur.execute(f"INSERT INTO {src_table} VALUES (1,'alice'),(2,'bob')")
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql",
            host="localhost", port=3306, database="dataflow",
            username="dataflow", password="dataflow",
            schema="dataflow", table=src_table,
        ),
        destination=_file_export(),
        skip_preflight=True,
    )
    _export(request)


def test_mongodb_to_csv():
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from pymongo import MongoClient

    src_collection = "mdb_to_csv_src_" + uuid.uuid4().hex[:8]
    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    db = client["dataflow"]
    db[src_collection].insert_many([
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
    ])
    client.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mongodb",
            host="localhost", port=27017,
            database="dataflow", table=src_collection,
        ),
        destination=_file_export(),
        skip_preflight=True,
    )
    _export(request)


def test_dynamodb_to_csv():
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import boto3

    src_table = "ddb_to_csv_src_" + uuid.uuid4().hex[:8]
    db = boto3.resource(
        "dynamodb", endpoint_url="http://localhost:8000",
        region_name="us-east-1", aws_access_key_id="test", aws_secret_access_key="test",
    )
    db.create_table(
        TableName=src_table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    ).wait_until_exists()
    table = db.Table(src_table)
    table.put_item(Item={"id": "1", "name": "alice"})
    table.put_item(Item={"id": "2", "name": "bob"})

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="dynamodb",
            host="localhost", port=8000,
            database="us-east-1", username="test", password="test",
            table=src_table,
        ),
        destination=_file_export(),
        skip_preflight=True,
    )
    _export(request)
