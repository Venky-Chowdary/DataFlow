"""Store adapters for the connector readiness live matrix (Track E).

One adapter per locally-startable store. Each adapter owns three things and
nothing else:

* ``endpoint(name)``  — the ``EndpointConfig`` the engine is given;
* ``seed(name, rows)`` — writing the fixture with the **store's own driver**,
  never through the transfer engine, so a source count is independent evidence;
* ``measure(name)``   — ``(count, checksum)`` read back with an independent
  driver connection over the mapped projection.

``measure`` is the only accepted destination proof: the writer's own
acknowledgement is not evidence that the rows landed. Checksums are computed
over ``id`` and ``amount`` so a row that arrives with a mangled value fails the
cell even when the count matches.

Reused by ``scripts/connector_readiness_live_proof.py`` and
``tests/test_connector_readiness_live_matrix.py`` — do not fork these builders.
"""

from __future__ import annotations

import json
import socket
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable, Iterable

from src.transfer.models import EndpointConfig

# Fixture projection. ``id`` is the primary key for every sync mode, ``amount``
# is the numeric fidelity witness, ``name``/``code``/``flag`` cover text and
# boolean coercion.
COLUMNS = ("id", "name", "amount", "code", "flag")
CODES = ("USD", "EUR", "GBP")

PG = dict(host="localhost", port=5432, database="dataflow", user="dataflow", password="dataflow")
MYSQL = dict(host="localhost", port=3306, database="dataflow", user="dataflow", password="dataflow")
MINIO = dict(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="dataflow",
    aws_secret_access_key="dataflowsecret",
    region_name="us-east-1",
)


def reachable(host: str, port: int, timeout: float = 1.5) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def expected_row(i: int) -> dict[str, Any]:
    return {
        "id": i,
        "name": f"name{i}",
        "amount": Decimal(i) / 100,
        "code": CODES[i % 3],
        "flag": i % 2 == 0,
    }


def expected_measure(rows: int) -> tuple[int, str]:
    """Checksum the fixture would produce if every row survives intact."""
    id_sum = rows * (rows + 1) // 2
    amount_sum = Decimal(id_sum) / 100
    return rows, _checksum(rows, id_sum, amount_sum)


def _checksum(count: int, id_sum: int | Decimal, amount_sum: int | Decimal) -> str:
    """Stable text checksum so stores with different numeric types compare."""
    amt = Decimal(str(amount_sum or 0)).quantize(Decimal("0.01"))
    return f"n={int(count)};id_sum={int(id_sum or 0)};amount_sum={amt}"


def _to_decimal(value: Any) -> Decimal:
    """Decimal for any store's numeric carrier.

    Goes through ``str`` on purpose: BSON ``Decimal128``, DynamoDB ``Decimal``,
    CSV text and floats all render an exact decimal string, whereas ``float()``
    raises on Decimal128 and would re-introduce binary rounding on the others.
    """
    if isinstance(value, Decimal):
        return value
    return Decimal(str(value))


def _measure_from_records(records: Iterable[dict[str, Any]]) -> tuple[int, str]:
    count = 0
    id_sum = 0
    amount_sum = Decimal(0)
    for rec in records:
        count += 1
        id_sum += int(_to_decimal(rec["id"]))
        amount_sum += _to_decimal(rec["amount"])
    return count, _checksum(count, id_sum, amount_sum)


@dataclass(frozen=True)
class Store:
    """A store the matrix can drive as source, destination or both."""

    driver: str
    label: str
    service: str
    ports: tuple[int, ...]
    endpoint: Callable[[str], EndpointConfig]
    #: ``seed(name, rows, start=1)`` — ``start`` shifts the fixture's ``id``
    #: range so an append probe can deliver a *disjoint* second batch. Reusing
    #: the same keys would measure the destination's uniqueness gate instead of
    #: what the mode does.
    seed: Callable[..., None] | None
    measure: Callable[[str], tuple[int, str]]
    drop: Callable[[str], None]
    can_source: bool = True
    can_dest: bool = True
    # Rows the store takes comfortably in one matrix cell. Object stores and
    # HTTP-batched stores are slower per row; the honest number is recorded in
    # the artifact rather than a claim of 100k everywhere.
    max_rows: int | None = None
    # Stores that carry no declared column types (documents, KV blobs, CSV
    # objects). Reading one into a typed destination needs the operator's Map
    # types, otherwise CREATE types are inferred from the peek sample.
    schemaless: bool = False
    # Store-assigned columns that are real source columns but have no place in
    # the destination (Mongo's ``_id``). They must be declared omitted, never
    # dropped silently.
    omit_columns: tuple[str, ...] = ()
    note: str = ""

    def available(self) -> tuple[bool, str]:
        for port in self.ports:
            if not reachable("localhost", port):
                return False, f"service {self.service} not reachable on localhost:{port}"
        return True, ""


# ── PostgreSQL ───────────────────────────────────────────────────────────────


def _pg_conn():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["user"], password=PG["password"],
    )
    conn.autocommit = True
    return conn


def _pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="postgresql", host="localhost", port=5432,
        database="dataflow", username="dataflow", password="dataflow",
        schema="public", table=table,
    )


def _pg_seed(table: str, rows: int, start: int = 1) -> None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f'CREATE TABLE public."{table}" ('
                "id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, "
                "amount DECIMAL(12,2) NOT NULL, code VARCHAR(8) NOT NULL, flag BOOLEAN NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{table}" '
                "SELECT i, 'name' || i, i::numeric/100, "
                "(ARRAY['USD','EUR','GBP'])[1 + i %% 3], (i %% 2 = 0) "
                "FROM generate_series(%s, %s) s(i)",
                (start, start + rows - 1),
            )
    finally:
        conn.close()


def _pg_measure(table: str) -> tuple[int, str]:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            # CAST so the checksum survives a destination whose declared type is
            # text: SQLite has no DECIMAL affinity, so a round-trip through it
            # legitimately lands amounts as text and the value must still match.
            cur.execute(
                "SELECT COUNT(*), COALESCE(SUM(id::numeric),0), "
                f'COALESCE(SUM(amount::numeric),0) FROM public."{table}"'
            )
            count, id_sum, amount_sum = cur.fetchone()
        return int(count), _checksum(count, id_sum, amount_sum)
    finally:
        conn.close()


def _pg_drop(table: str) -> None:
    conn = _pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


# ── MySQL ────────────────────────────────────────────────────────────────────


def _mysql_conn():
    import pymysql

    return pymysql.connect(
        host="localhost", port=3306, user="dataflow", password="dataflow",
        database="dataflow", autocommit=True,
    )


def _mysql_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mysql", host="localhost", port=3306,
        database="dataflow", username="dataflow", password="dataflow",
        schema="dataflow", table=table,
    )


def _mysql_seed(table: str, rows: int, start: int = 1) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` ("
                "id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, "
                "amount DECIMAL(12,2) NOT NULL, code VARCHAR(8) NOT NULL, flag TINYINT(1) NOT NULL)"
            )
            cur.execute("SET SESSION cte_max_recursion_depth = 4294967295")
            cur.execute(
                f"INSERT INTO `{table}` "
                "WITH RECURSIVE seq(i) AS (SELECT %s UNION ALL SELECT i+1 FROM seq WHERE i < %s) "
                "SELECT i, CONCAT('name', i), i/100, "
                "ELT(1 + i %% 3, 'USD','EUR','GBP'), (i %% 2 = 0) FROM seq",
                (start, start + rows - 1),
            )
    finally:
        conn.close()


def _mysql_measure(table: str) -> tuple[int, str]:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*), COALESCE(SUM(CAST(id AS SIGNED)),0), "
                f"COALESCE(SUM(CAST(amount AS DECIMAL(20,2))),0) FROM `{table}`"
            )
            count, id_sum, amount_sum = cur.fetchone()
        return int(count), _checksum(count, id_sum, amount_sum)
    finally:
        conn.close()


def _mysql_drop(table: str) -> None:
    conn = _mysql_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


# ── SQLite (file, always local) ──────────────────────────────────────────────

SQLITE_PATH = "/tmp/dataflow_track_e.sqlite"


def _sqlite_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=SQLITE_PATH, table=table)


def _sqlite_conn():
    import sqlite3

    return sqlite3.connect(SQLITE_PATH)


def _sqlite_seed(table: str, rows: int, start: int = 1) -> None:
    conn = _sqlite_conn()
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(
            f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name TEXT NOT NULL, '
            "amount NUMERIC NOT NULL, code TEXT NOT NULL, flag INTEGER NOT NULL)"
        )
        conn.executemany(
            f'INSERT INTO "{table}" (id, name, amount, code, flag) VALUES (?,?,?,?,?)',
            [
                (i, f"name{i}", float(Decimal(i) / 100), CODES[i % 3], 1 if i % 2 == 0 else 0)
                for i in range(start, start + rows)
            ],
        )
        conn.commit()
    finally:
        conn.close()


def _sqlite_measure(table: str) -> tuple[int, str]:
    conn = _sqlite_conn()
    try:
        # SQLite stores DECIMAL as TEXT by design (no Decimal affinity), so sum
        # in Python with exact Decimal rather than through float affinity.
        cur = conn.execute(f'SELECT id, amount FROM "{table}"')
        return _measure_from_records({"id": r[0], "amount": r[1]} for r in cur)
    finally:
        conn.close()


def _sqlite_drop(table: str) -> None:
    conn = _sqlite_conn()
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.commit()
    finally:
        conn.close()


# ── MongoDB ──────────────────────────────────────────────────────────────────


def _mongo_client():
    from pymongo import MongoClient

    return MongoClient("mongodb://localhost:27017/?directConnection=true", serverSelectionTimeoutMS=5000)


def _mongo_endpoint(collection: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="mongodb", host="localhost", port=27017,
        database="dataflow", table=collection,
    )


def _mongo_seed(collection: str, rows: int, start: int = 1) -> None:
    client = _mongo_client()
    try:
        coll = client["dataflow"][collection]
        coll.drop()
        batch: list[dict[str, Any]] = []
        for i in range(start, start + rows):
            row = expected_row(i)
            batch.append({**row, "amount": float(row["amount"])})
            if len(batch) >= 10_000:
                coll.insert_many(batch)
                batch = []
        if batch:
            coll.insert_many(batch)
    finally:
        client.close()


def _mongo_measure(collection: str) -> tuple[int, str]:
    client = _mongo_client()
    try:
        coll = client["dataflow"][collection]
        cursor = coll.find({}, {"_id": 0, "id": 1, "amount": 1})
        return _measure_from_records(
            {"id": d["id"], "amount": _to_decimal(d["amount"])} for d in cursor
        )
    finally:
        client.close()


def _mongo_drop(collection: str) -> None:
    client = _mongo_client()
    try:
        client["dataflow"][collection].drop()
    finally:
        client.close()


# ── SQL Server ───────────────────────────────────────────────────────────────

SQLSERVER_PASSWORD = "DataFlow_CDC_2022!"


def _sqlserver_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlserver", host="localhost", port=1433,
        database="dataflow", username="sa", password=SQLSERVER_PASSWORD,
        schema="dbo", table=table,
    )


def _sqlserver_conn():
    import pymssql

    return pymssql.connect(
        server="localhost", port=1433, user="sa", password=SQLSERVER_PASSWORD,
        database="dataflow", autocommit=True,
    )


def _sqlserver_seed(table: str, rows: int, start: int = 1) -> None:
    conn = _sqlserver_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"IF OBJECT_ID('dbo.[{table}]','U') IS NOT NULL DROP TABLE dbo.[{table}]")
        cur.execute(
            f"CREATE TABLE dbo.[{table}] (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL, "
            "amount DECIMAL(12,2) NOT NULL, code VARCHAR(8) NOT NULL, flag BIT NOT NULL)"
        )
        cur.execute(
            f"WITH seq AS (SELECT TOP ({int(rows)}) "
            f"ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) + {int(start) - 1} AS i "
            "FROM sys.all_objects a CROSS JOIN sys.all_objects b) "
            f"INSERT INTO dbo.[{table}] (id, name, amount, code, flag) "
            "SELECT i, CONCAT('name', i), CAST(i AS DECIMAL(12,2))/100, "
            # pymssql only treats %% as an escape when parameters are bound;
            # this statement binds none, so the literal modulo must stay single.
            "CASE i % 3 WHEN 1 THEN 'EUR' WHEN 2 THEN 'GBP' ELSE 'USD' END, "
            "CASE WHEN i % 2 = 0 THEN 1 ELSE 0 END FROM seq"
        )
    finally:
        conn.close()


def _sqlserver_measure(table: str) -> tuple[int, str]:
    conn = _sqlserver_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            f"SELECT COUNT(*), ISNULL(SUM(CAST(id AS BIGINT)),0), ISNULL(SUM(amount),0) FROM dbo.[{table}]"
        )
        count, id_sum, amount_sum = cur.fetchone()
        return int(count), _checksum(count, id_sum, amount_sum)
    finally:
        conn.close()


def _sqlserver_drop(table: str) -> None:
    conn = _sqlserver_conn()
    try:
        conn.cursor().execute(
            f"IF OBJECT_ID('dbo.[{table}]','U') IS NOT NULL DROP TABLE dbo.[{table}]"
        )
    finally:
        conn.close()


# ── Redis ────────────────────────────────────────────────────────────────────


def _redis_client():
    import redis

    return redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def _redis_endpoint(prefix: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="redis", host="localhost", port=6379,
        database="0", table=prefix,
    )


def _redis_measure(prefix: str) -> tuple[int, str]:
    client = _redis_client()
    try:
        records = []
        for key in client.scan_iter(match=f"{prefix}:*", count=1000):
            raw = client.get(key)
            if raw is None:
                continue
            try:
                doc = json.loads(raw)
            except (TypeError, ValueError):
                continue
            if "id" in doc and "amount" in doc:
                records.append({"id": doc["id"], "amount": _to_decimal(doc["amount"])})
        return _measure_from_records(records)
    finally:
        client.close()


def _redis_drop(prefix: str) -> None:
    client = _redis_client()
    try:
        keys = list(client.scan_iter(match=f"{prefix}:*", count=1000))
        for i in range(0, len(keys), 5000):
            client.delete(*keys[i : i + 5000])
    finally:
        client.close()


# ── S3 / MinIO (CSV objects) ─────────────────────────────────────────────────

S3_BUCKET = "track-e"


def _s3_client():
    import boto3

    return boto3.client("s3", **MINIO)


def _s3_ensure_bucket() -> None:
    client = _s3_client()
    try:
        client.create_bucket(Bucket=S3_BUCKET)
    except Exception:
        pass


def _s3_endpoint(key: str) -> EndpointConfig:
    _s3_ensure_bucket()
    return EndpointConfig(
        kind="database", format="s3", host="localhost", port=9000,
        database=S3_BUCKET, table=key,
        username="dataflow", password="dataflowsecret",
        endpoint_url="http://localhost:9000", path_style=True, region="us-east-1",
    )


def _s3_read_csv(key: str) -> list[dict[str, Any]]:
    import csv
    import io

    client = _s3_client()
    body = client.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read().decode("utf-8")
    return list(csv.DictReader(io.StringIO(body)))


def _s3_measure(key: str) -> tuple[int, str]:
    return _measure_from_records(_s3_read_csv(key))


def _s3_drop(key: str) -> None:
    try:
        _s3_client().delete_object(Bucket=S3_BUCKET, Key=key)
    except Exception:
        pass


# ── DynamoDB Local ───────────────────────────────────────────────────────────


def _dynamo_resource():
    import boto3

    return boto3.resource(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def _dynamo_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="dynamodb", host="localhost", port=8000,
        database="us-east-1", username="test", password="test", table=table,
    )


def _dynamo_measure(table: str) -> tuple[int, str]:
    tbl = _dynamo_resource().Table(table)
    records = []
    kwargs: dict[str, Any] = {"ProjectionExpression": "id, amount"}
    while True:
        page = tbl.scan(**kwargs)
        for item in page.get("Items", []):
            if "id" in item and "amount" in item:
                records.append({"id": item["id"], "amount": round(float(item["amount"]), 2)})
        key = page.get("LastEvaluatedKey")
        if not key:
            break
        kwargs["ExclusiveStartKey"] = key
    return _measure_from_records(records)


def _dynamo_drop(table: str) -> None:
    try:
        _dynamo_resource().Table(table).delete()
    except Exception:
        pass


# ── Elasticsearch ────────────────────────────────────────────────────────────


def _es_client():
    from elasticsearch import Elasticsearch

    return Elasticsearch("http://localhost:9200", request_timeout=60)


def _es_endpoint(index: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="elasticsearch", host="localhost", port=9200,
        database=index, table=index,
    )


def _es_measure(index: str) -> tuple[int, str]:
    client = _es_client()
    try:
        client.indices.refresh(index=index)
        records = []
        resp = client.search(
            index=index, size=10000, scroll="2m",
            source_includes=["id", "amount"], query={"match_all": {}},
        )
        scroll_id = resp.get("_scroll_id")
        while True:
            hits = resp["hits"]["hits"]
            if not hits:
                break
            for hit in hits:
                src = hit.get("_source", {})
                if "id" in src and "amount" in src:
                    records.append({"id": src["id"], "amount": _to_decimal(src["amount"])})
            resp = client.scroll(scroll_id=scroll_id, scroll="2m")
        return _measure_from_records(records)
    finally:
        client.close()


def _es_drop(index: str) -> None:
    try:
        client = _es_client()
        client.indices.delete(index=index, ignore_unavailable=True)
        client.close()
    except Exception:
        pass


# ── DuckDB (embedded warehouse) ──────────────────────────────────────────────

DUCKDB_PATH = "/tmp/dataflow_track_e.duckdb"


def _duckdb_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(kind="database", format="duckdb", database=DUCKDB_PATH, table=table)


def _duckdb_conn():
    import duckdb

    return duckdb.connect(DUCKDB_PATH)


def _duckdb_seed(table: str, rows: int, start: int = 1) -> None:
    conn = _duckdb_conn()
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(
            f'CREATE TABLE "{table}" (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, '
            "amount DECIMAL(12,2) NOT NULL, code VARCHAR NOT NULL, flag BOOLEAN NOT NULL)"
        )
        conn.execute(
            f'INSERT INTO "{table}" SELECT i, \'name\' || i, i::DECIMAL(12,2)/100, '
            "['USD','EUR','GBP'][1 + i % 3], (i % 2 = 0) "
            f"FROM range({int(start)}, {int(start) + int(rows)}) t(i)"
        )
    finally:
        conn.close()


def _duckdb_measure(table: str) -> tuple[int, str]:
    conn = _duckdb_conn()
    try:
        count, id_sum, amount_sum = conn.execute(
            f'SELECT COUNT(*), COALESCE(SUM(id),0), COALESCE(SUM(amount),0) FROM "{table}"'
        ).fetchone()
        return int(count), _checksum(count, id_sum, amount_sum)
    finally:
        conn.close()


def _duckdb_drop(table: str) -> None:
    conn = _duckdb_conn()
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    finally:
        conn.close()


STORES: dict[str, Store] = {
    "postgresql": Store(
        driver="postgresql", label="PostgreSQL 16", service="postgres", ports=(5432,),
        endpoint=_pg_endpoint, seed=_pg_seed, measure=_pg_measure, drop=_pg_drop,
    ),
    "mysql": Store(
        driver="mysql", label="MySQL 8", service="mysql", ports=(3306,),
        endpoint=_mysql_endpoint, seed=_mysql_seed, measure=_mysql_measure, drop=_mysql_drop,
    ),
    "sqlite": Store(
        driver="sqlite", label="SQLite (embedded)", service="none (file)", ports=(),
        endpoint=_sqlite_endpoint, seed=_sqlite_seed, measure=_sqlite_measure, drop=_sqlite_drop,
    ),
    "duckdb": Store(
        driver="duckdb", label="DuckDB (embedded)", service="none (file)", ports=(),
        endpoint=_duckdb_endpoint, seed=_duckdb_seed, measure=_duckdb_measure, drop=_duckdb_drop,
    ),
    "mongodb": Store(
        driver="mongodb", label="MongoDB 7 (rs0)", service="mongodb", ports=(27017,),
        endpoint=_mongo_endpoint, seed=_mongo_seed, measure=_mongo_measure, drop=_mongo_drop,
        schemaless=True, omit_columns=("_id",),
    ),
    "sqlserver": Store(
        driver="sqlserver", label="SQL Server 2022", service="sqlserver", ports=(1433,),
        endpoint=_sqlserver_endpoint, seed=_sqlserver_seed,
        measure=_sqlserver_measure, drop=_sqlserver_drop,
    ),
    "redis": Store(
        driver="redis", label="Redis 7", service="redis", ports=(6379,),
        endpoint=_redis_endpoint, seed=None, measure=_redis_measure, drop=_redis_drop,
        schemaless=True, omit_columns=("redis_key", "redis_type"),
        note="Seeded only by the engine; Redis source reads the JSON docs the writer produced.",
    ),
    "s3": Store(
        driver="s3", label="MinIO (S3 API)", service="minio", ports=(9000,),
        endpoint=_s3_endpoint, seed=None, measure=_s3_measure, drop=_s3_drop,
        schemaless=True,
        note="CSV object round-trip; object key is the table name.",
    ),
    "dynamodb": Store(
        driver="dynamodb", label="DynamoDB Local", service="dynamodb-local", ports=(8000,),
        endpoint=_dynamo_endpoint, seed=None, measure=_dynamo_measure, drop=_dynamo_drop,
        schemaless=True, max_rows=100_000,
    ),
    "elasticsearch": Store(
        driver="elasticsearch", label="Elasticsearch 8", service="elasticsearch", ports=(9200,),
        endpoint=_es_endpoint, seed=None, measure=_es_measure, drop=_es_drop,
        schemaless=True,
    ),
}
