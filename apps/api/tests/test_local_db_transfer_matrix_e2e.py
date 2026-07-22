"""Live local DB matrix: source introspect sample rows + real transfers.

Skips cleanly when Postgres/MySQL/Mongo are not reachable (compose defaults).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

pg_ok = False
my_ok = False
mongo_ok = False

if psycopg2 is not None:
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            dbname="dataflow",
            user="dataflow",
            password="dataflow",
            connect_timeout=2,
        )
        conn.close()
        pg_ok = True
    except Exception:
        pg_ok = False

try:
    import pymysql

    conn = pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        connect_timeout=2,
    )
    conn.close()
    my_ok = True
except Exception:
    my_ok = False

try:
    from pymongo import MongoClient

    MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=1500).admin.command("ping")
    mongo_ok = True
except Exception:
    mongo_ok = False

pytestmark = pytest.mark.skipif(
    not (pg_ok and my_ok and mongo_ok),
    reason="Local Postgres/MySQL/Mongo (compose defaults) not all reachable",
)


def _seed():
    assert psycopg2 is not None
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        dbname="dataflow",
        user="dataflow",
        password="dataflow",
        connect_timeout=2,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS e2e_customers (
                  id SERIAL PRIMARY KEY,
                  email TEXT NOT NULL,
                  amount NUMERIC(12,2) NOT NULL
                );
                TRUNCATE e2e_customers RESTART IDENTITY;
                INSERT INTO e2e_customers (email, amount) VALUES
                  ('alice@example.com', 10.50),
                  ('bob@example.com', 22.00),
                  ('carol@example.com', 7.25);
                DROP TABLE IF EXISTS e2e_from_mongo;
                """
            )
    finally:
        conn.close()
    import pymysql

    with pymysql.connect(
        host="127.0.0.1",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS e2e_customers (
                  id INT AUTO_INCREMENT PRIMARY KEY,
                  email VARCHAR(255) NOT NULL,
                  amount DECIMAL(12,2) NOT NULL
                )
                """
            )
            cur.execute("TRUNCATE e2e_customers")
            cur.execute(
                "INSERT INTO e2e_customers (email, amount) VALUES "
                "('alice@example.com', 10.50), ('bob@example.com', 22.00), "
                "('carol@example.com', 7.25)"
            )
            cur.execute("DROP TABLE IF EXISTS e2e_from_pg")
    from pymongo import MongoClient

    db = MongoClient("mongodb://localhost:27017")["dataflow_test"]
    db.e2e_customers.delete_many({})
    db.e2e_customers.insert_many(
        [
            {"email": "alice@example.com", "amount": 10.5},
            {"email": "bob@example.com", "amount": 22.0},
            {"email": "carol@example.com", "amount": 7.25},
        ]
    )
    db.e2e_from_mysql.drop()


@pytest.fixture()
def seeded():
    _seed()
    return True


def test_source_introspect_sample_rows_and_missing_copy(seeded):
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    pg = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table="e2e_customers",
        extra={"introspect_purpose": "source"},
    )
    info = introspect_endpoint(pg)
    assert info.get("connected")
    assert info.get("columns")
    samples = info.get("data") or info.get("sample_data") or []
    assert len(samples) >= 1

    missing = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table="csv",
        extra={"introspect_purpose": "source"},
    )
    miss = introspect_endpoint(missing)
    msg = (miss.get("message") or "").lower()
    assert "first write" not in msg
    assert "not found on this source" in msg


def test_transfer_matrix_pg_mysql_mongo(seeded):
    from src.transfer.engine import get_transfer_engine
    from src.transfer.models import EndpointConfig, TransferRequest

    engine = get_transfer_engine()
    sqlite_path = Path("/tmp/dataflow_pytest_e2e.db")
    sqlite_path.unlink(missing_ok=True)

    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0},
        {"source": "email", "target": "email", "confidence": 1.0},
        {"source": "amount", "target": "amount", "confidence": 1.0},
    ]
    pg_src = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table="e2e_customers",
    )
    sqlite_dst = EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(sqlite_path),
        table="e2e_out",
    )
    r1 = engine.execute(
        TransferRequest(
            source=pg_src,
            destination=sqlite_dst,
            mappings=mappings,
            validation_mode="permissive",
            sync_mode="full_refresh_overwrite",
        )
    )
    assert r1.success, r1.error
    assert r1.records_transferred == 3
    con = sqlite3.connect(sqlite_path)
    try:
        assert con.execute("select count(*) from e2e_out").fetchone()[0] == 3
    finally:
        con.close()

    my_src = EndpointConfig(
        kind="database",
        format="mysql",
        host="127.0.0.1",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        table="e2e_customers",
    )
    mongo_dst = EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="dataflow_test",
        collection="e2e_from_mysql",
    )
    r2 = engine.execute(
        TransferRequest(
            source=my_src,
            destination=mongo_dst,
            mappings=mappings,
            validation_mode="permissive",
            sync_mode="full_refresh_overwrite",
        )
    )
    assert r2.success, r2.error
    assert r2.records_transferred == 3

    from pymongo import MongoClient

    assert MongoClient("mongodb://localhost:27017")["dataflow_test"].e2e_from_mysql.count_documents({}) == 3

    mongo_src = EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="dataflow_test",
        collection="e2e_customers",
    )
    pg_dst = EndpointConfig(
        kind="database",
        format="postgresql",
        host="127.0.0.1",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table="e2e_from_mongo",
    )
    r3 = engine.execute(
        TransferRequest(
            source=mongo_src,
            destination=pg_dst,
            mappings=[
                {"source": "email", "target": "email", "confidence": 1.0},
                {"source": "amount", "target": "amount", "confidence": 1.0},
            ],
            validation_mode="permissive",
            sync_mode="full_refresh_overwrite",
        )
    )
    assert r3.success, r3.error
    assert r3.records_transferred == 3
