"""Database and object-store sources → Snowflake end-to-end matrix."""

from __future__ import annotations

import csv
import io
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


def _csv_bytes(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "name"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode("utf-8")


def _snowflake_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="snowflake",
        host="localhost",
        port=443,
        database="dataflow",
        username="test",
        password="test",
        schema="public",
        table=table,
    )


def _run(request: TransferRequest, job_id: str) -> None:
    engine = UniversalTransferEngine()
    result = engine.execute_tracked(request, job_id)
    assert result.success is True, result.error
    assert result.records_transferred == 2
    assert result.reconciliation.get("passed") is True
    assert result.reconciliation.get("source_checksum") == result.reconciliation.get("target_checksum")


def test_postgresql_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import psycopg2

    src_table = "pg_to_sf_src_" + uuid.uuid4().hex[:8]
    dst_table = "pg_to_sf_" + uuid.uuid4().hex[:8]

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
        destination=_snowflake_endpoint(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_mysql_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 3306), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import pymysql

    src_table = "mysql_to_sf_src_" + uuid.uuid4().hex[:8]
    dst_table = "mysql_to_sf_" + uuid.uuid4().hex[:8]

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
        destination=_snowflake_endpoint(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_mongodb_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from pymongo import MongoClient

    src_collection = "mdb_to_sf_src_" + uuid.uuid4().hex[:8]
    dst_table = "mdb_to_sf_" + uuid.uuid4().hex[:8]

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
        destination=_snowflake_endpoint(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_dynamodb_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 8000), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import boto3

    src_table = "ddb_to_sf_src_" + uuid.uuid4().hex[:8]
    dst_table = "ddb_to_sf_" + uuid.uuid4().hex[:8]

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
        destination=_snowflake_endpoint(dst_table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_s3_minio_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 9000), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import boto3

    bucket_name = "s3-to-sf-" + uuid.uuid4().hex[:8]
    table_name = "s3_to_sf_" + uuid.uuid4().hex[:8]

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="dataflow",
        aws_secret_access_key="dataflowsecret",
        region_name="us-east-1",
    )
    s3.create_bucket(Bucket=bucket_name)
    s3.put_object(Bucket=bucket_name, Key="users.csv", Body=_csv_bytes([
        {"id": "1", "name": "alice"},
        {"id": "2", "name": "bob"},
    ]))

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="s3",
            host="localhost", port=9000,
            connection_string="http://localhost:9000",
            username="dataflow", password="dataflowsecret",
            database=bucket_name, table="users.csv",
        ),
        destination=_snowflake_endpoint(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_gcs_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 4443), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import storage

    bucket_name = "gcs_to_sf_" + uuid.uuid4().hex[:8]
    table_name = "gcs_to_sf_" + uuid.uuid4().hex[:8]

    client = storage.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint="http://localhost:4443"),
    )
    bucket = client.create_bucket(bucket_name)
    bucket.blob("users.csv").upload_from_string(
        _csv_bytes([
            {"id": "1", "name": "alice"},
            {"id": "2", "name": "bob"},
        ]),
        content_type="text/csv",
    )

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="gcs",
            host="localhost", port=4443,
            connection_string="http://localhost:4443",
            database=bucket_name, table="users.csv",
        ),
        destination=_snowflake_endpoint(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_adls_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 10000), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from azure.storage.blob import BlobServiceClient

    conn = (
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
        "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
        "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
    )
    container = "adlstosf" + uuid.uuid4().hex[:8]
    table_name = "adls_to_sf_" + uuid.uuid4().hex[:8]

    client = BlobServiceClient.from_connection_string(conn)
    client.create_container(container)
    client.get_blob_client(container, "users.csv").upload_blob(
        _csv_bytes([
            {"id": "1", "name": "alice"},
            {"id": "2", "name": "bob"},
        ]),
        overwrite=True,
    )

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="adls",
            connection_string=conn, database=container, table="users.csv",
        ),
        destination=_snowflake_endpoint(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_redis_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 6379), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    import json

    import redis

    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    prefix = "redis_to_sf_" + uuid.uuid4().hex[:8]
    for i in range(1, 3):
        r.set(f"{prefix}:user:{i}", json.dumps({"id": str(i), "name": "alice" if i == 1 else "bob"}))
    table_name = "redis_to_sf_" + uuid.uuid4().hex[:8]

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="redis",
            host="localhost", port=6379,
            database="0", table=f"{prefix}:user:*",
        ),
        destination=_snowflake_endpoint(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])


def test_elasticsearch_to_snowflake():
    fakesnow = pytest.importorskip("fakesnow")
    try:
        with socket.create_connection(("localhost", 9200), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"Emulator not reachable: {exc}")

    from elasticsearch import Elasticsearch

    index = "es_to_sf_" + uuid.uuid4().hex[:8]
    table_name = "es_to_sf_" + uuid.uuid4().hex[:8]

    es = Elasticsearch(hosts=["http://localhost:9200"])
    for i in range(1, 3):
        es.index(index=index, id=str(i), body={"id": str(i), "name": "alice" if i == 1 else "bob"})
    es.indices.refresh(index=index)

    request = TransferRequest(
        source=EndpointConfig(
            kind="database", format="elasticsearch",
            host="localhost", port=9200,
            database=index, table="",
        ),
        destination=_snowflake_endpoint(table_name),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": "users",
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
        skip_preflight=True,
    )
    _run(request, uuid.uuid4().hex[:24])
