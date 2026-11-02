"""Live MongoDB → PostgreSQL source-timezone transfer regression."""

from __future__ import annotations

import socket
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


def test_mongodb_to_postgres_applies_declared_zone_to_mixed_timestamp():
    pytest.importorskip("pymongo")
    try:
        with socket.create_connection(("localhost", 27017), timeout=1):
            pass
    except OSError as exc:
        pytest.skip(f"MongoDB not reachable: {exc}")

    psycopg2 = pytest.importorskip("psycopg2")
    try:
        probe = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=2,
        )
        probe.close()
    except Exception as exc:
        pytest.skip(f"PostgreSQL not reachable with test credentials: {exc}")

    from pymongo import MongoClient

    source_collection = "mongo_tz_src_" + uuid.uuid4().hex[:8]
    destination_table = "mongo_tz_dst_" + uuid.uuid4().hex[:8]
    mongo = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    mongo["dataflow"][source_collection].insert_many([
        {"id": 1, "created": datetime(2024, 1, 1, tzinfo=timezone.utc)},
        {"id": 2, "created": "2024-06-01"},
    ])
    mongo.close()

    request = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="mongodb",
            host="localhost",
            port=27017,
            database="dataflow",
            table=source_collection,
        ),
        destination=EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=destination_table,
        ),
        source_timezone="Asia/Kolkata",
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{
            "name": source_collection,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }],
    )
    try:
        result = UniversalTransferEngine().execute_tracked(
            request, uuid.uuid4().hex[:24]
        )
        assert result.success is True, result.error
        assert result.records_transferred == 2

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT "created" FROM public."{destination_table}" WHERE "id" = 2'
                )
                actual = cur.fetchone()[0]
        finally:
            conn.close()
        assert actual == datetime(2024, 5, 31, 18, 30, tzinfo=timezone.utc)

        conn = psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=5,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'SELECT "created" FROM public."{destination_table}" WHERE "id" = 1'
                )
                aware = cur.fetchone()[0]
        finally:
            conn.close()
        assert aware == datetime(2024, 1, 1, tzinfo=timezone.utc)
    finally:
        mongo = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
        mongo["dataflow"][source_collection].drop()
        mongo.close()
        with psycopg2.connect(
            host="localhost",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=5,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{destination_table}"')
