"""One adapter per engine: how to seed it, and how to read it back *independently*.

Every adapter has two halves that must not share code with the product:

* ``seed`` puts the fixture into the store with the store's **own** driver, so a
  route that uses this engine as a source is reading data DataFlow did not
  write. A fixture written by the writer under test would make a round trip
  prove only that the writer is self-consistent.
* ``read_projection`` reads the mapped projection back with the store's own
  driver — ``pymongo``, ``redis-py``, ``boto3``, ``duckdb``, ``psycopg2``,
  ``PyMySQL``, ``google-cloud-bigquery`` — and never through
  ``connectors/``. The count and the content checksum in the evidence table
  come from here, which is the only reason they are evidence at all: the
  writer's ``rows_written`` is an acknowledgement, not a measurement.

``key_addressed`` is declared per engine with its reason, because it changes
what ``full_refresh_append`` is *supposed* to do (see
``sync_mode_probe.expected_rows``). A keyspace keyed by the row id cannot hold
the same row twice, so append landing N there is correct; a SQL table that did
the same thing would be silently deduplicating an operator's rows.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Callable, Iterator

from src.transfer.models import EndpointConfig
from tests.scale.nosql_fixture import (
    RELATIONAL_DDL_MYSQL,
    RELATIONAL_DDL_PG,
    iter_rows,
)


def env(name: str, default: str) -> str:
    return os.environ.get(name, "") or default


PG = {
    "host": env("DATAFLOW_PG_HOST", "localhost"),
    "port": int(env("DATAFLOW_PG_PORT", "5432")),
    "database": env("DATAFLOW_PG_DB", "dataflow"),
    "username": env("DATAFLOW_PG_USER", "dataflow"),
    "password": env("DATAFLOW_PG_PASSWORD", "dataflow"),
    "schema": env("DATAFLOW_PG_SCHEMA", "public"),
}
MYSQL = {
    "host": env("DATAFLOW_MYSQL_HOST", "localhost"),
    "port": int(env("DATAFLOW_MYSQL_PORT", "3306")),
    "database": env("DATAFLOW_MYSQL_DB", "dataflow"),
    "username": env("DATAFLOW_MYSQL_USER", "dataflow"),
    "password": env("DATAFLOW_MYSQL_PASSWORD", "dataflow"),
}
MONGO_PORT = int(env("DATAFLOW_MONGO_PORT", "27017"))
REDIS_PORT = int(env("DATAFLOW_REDIS_PORT", "6379"))
DYNAMO_PORT = int(env("DATAFLOW_DYNAMODB_PORT", "8000"))
BIGQUERY_PORT = int(env("DATAFLOW_BIGQUERY_EMULATOR_PORT", "9050"))
ES_PORT = int(env("DATAFLOW_ELASTICSEARCH_PORT", "9200"))
CLICKHOUSE_PORT = int(env("DATAFLOW_CLICKHOUSE_PORT", "9100"))
HOST = env("DATAFLOW_SCALE_HOST", "localhost")
DUCKDB_DIR = env("DATAFLOW_DUCKDB_DIR", "/tmp/dataflow-scale-duckdb")

PROJECTION_KEYS = ("id", "uid", "big_int", "amount", "unicode_key", "payload")


@dataclass
class EngineSpec:
    """An engine the matrix can use as a source, a destination, or both."""

    name: str
    #: Writing the same identity twice replaces it, so append lands N not 2N.
    key_addressed: bool
    #: Why — quoted into the evidence doc, never assumed by the reader.
    key_addressed_reason: str
    endpoint: Callable[[str], EndpointConfig]
    read_projection: Callable[[str], Iterator[dict[str, Any]]]
    availability: Callable[[], tuple[bool, str]]
    seed: Callable[[str, int], None] | None = None
    drop: Callable[[str], None] | None = None
    #: The destination's date carrier is instant-only (no zoneless spelling), so
    #: a naive source column has to be *declared* with ``assume_timezone`` or the
    #: engine quarantines it rather than stamping a zone the source never had.
    assume_timezone: str | None = None
    source_role: bool = True
    dest_role: bool = True
    notes: str = ""
    #: The product's catalog id for this engine, when the matrix row name is a
    #: deployment label rather than a connector: ``bigquery_emulator`` is the
    #: BigQuery connector talking to the emulator, and handing the label to the
    #: Map SSOT loses the dialect (``DECIMAL(24,6)`` invents ``TEXT`` instead of
    #: ``BIGNUMERIC(24,6)``).
    db_type: str = ""
    #: Columns whose fidelity the store cannot carry, named rather than hidden.
    carrier_notes: dict[str, str] = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# PostgreSQL
# --------------------------------------------------------------------------- #
def _pg_connect():
    import psycopg2

    return psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        dbname=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )


def pg_available() -> tuple[bool, str]:
    try:
        conn = _pg_connect()
        conn.close()
        return True, ""
    except Exception as exc:  # noqa: BLE001 — a dead engine is a measured skip
        return False, f"{type(exc).__name__}: {exc}"[:300]


def pg_seed(table: str, rows: int, *, start: int = 1) -> None:
    import psycopg2.extras

    conn = _pg_connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{PG["schema"]}"."{table}"')
            cur.execute(
                f'CREATE TABLE "{PG["schema"]}"."{table}" ({RELATIONAL_DDL_PG})'
            )
            batch: list[tuple] = []
            for row in iter_rows(rows, start=start):
                batch.append(
                    (
                        row["id"],
                        row["uid"],
                        row["big_int"],
                        row["amount"],
                        row["ts_naive"],
                        row["ts_zoned"],
                        row["unicode_key"],
                        json.dumps(row["payload"], ensure_ascii=False),
                    )
                )
                if len(batch) >= 10000:
                    psycopg2.extras.execute_values(
                        cur,
                        f'INSERT INTO "{PG["schema"]}"."{table}" '
                        "(id,uid,big_int,amount,ts_naive,ts_zoned,unicode_key,payload)"
                        " VALUES %s",
                        batch,
                    )
                    batch = []
            if batch:
                psycopg2.extras.execute_values(
                    cur,
                    f'INSERT INTO "{PG["schema"]}"."{table}" '
                    "(id,uid,big_int,amount,ts_naive,ts_zoned,unicode_key,payload)"
                    " VALUES %s",
                    batch,
                )
    finally:
        conn.close()


def pg_read(table: str) -> Iterator[dict[str, Any]]:
    conn = _pg_connect()
    try:
        with conn.cursor(name=f"scale_{table}") as cur:
            cur.itersize = 5000
            cur.execute(
                "SELECT id, uid, big_int, amount, unicode_key, payload, "
                f'ts_naive, ts_zoned FROM "{PG["schema"]}"."{table}"'
            )
            for rec in cur:
                yield {
                    "id": rec[0],
                    "uid": rec[1],
                    "big_int": rec[2],
                    "amount": rec[3],
                    "unicode_key": rec[4],
                    "payload": rec[5],
                    "ts_naive": rec[6],
                    "ts_zoned": rec[7],
                }
    finally:
        conn.close()


def pg_drop(table: str) -> None:
    conn = _pg_connect()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS "{PG["schema"]}"."{table}"')
    finally:
        conn.close()


def pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        username=PG["username"],
        password=PG["password"],
        schema=PG["schema"],
        table=table,
    )


# --------------------------------------------------------------------------- #
# MySQL
# --------------------------------------------------------------------------- #
def _mysql_connect():
    import pymysql

    return pymysql.connect(
        host=MYSQL["host"],
        port=MYSQL["port"],
        database=MYSQL["database"],
        user=MYSQL["username"],
        password=MYSQL["password"],
        charset="utf8mb4",
        autocommit=True,
    )


def mysql_available() -> tuple[bool, str]:
    try:
        conn = _mysql_connect()
        conn.close()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def mysql_seed(table: str, rows: int, *, start: int = 1) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(f"CREATE TABLE `{table}` ({RELATIONAL_DDL_MYSQL})")
            sql = (
                f"INSERT INTO `{table}` "
                "(id,uid,big_int,amount,ts_naive,ts_zoned,unicode_key,payload)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
            )
            batch: list[tuple] = []
            for row in iter_rows(rows, start=start):
                batch.append(
                    (
                        row["id"],
                        row["uid"],
                        row["big_int"],
                        str(row["amount"]),
                        row["ts_naive"],
                        row["ts_zoned"].replace(tzinfo=None),
                        row["unicode_key"],
                        json.dumps(row["payload"], ensure_ascii=False),
                    )
                )
                if len(batch) >= 5000:
                    cur.executemany(sql, batch)
                    batch = []
            if batch:
                cur.executemany(sql, batch)
    finally:
        conn.close()


def mysql_read(table: str) -> Iterator[dict[str, Any]]:
    import pymysql.cursors

    conn = _mysql_connect()
    try:
        with conn.cursor(pymysql.cursors.SSCursor) as cur:
            cur.execute(
                "SELECT id, uid, big_int, amount, unicode_key, payload, "
                f"ts_naive, ts_zoned FROM `{table}`"
            )
            for rec in cur:
                yield {
                    "id": rec[0],
                    "uid": rec[1],
                    "big_int": rec[2],
                    "amount": rec[3],
                    "unicode_key": rec[4],
                    "payload": rec[5],
                    "ts_naive": rec[6],
                    "ts_zoned": rec[7],
                }
    finally:
        conn.close()


def mysql_drop(table: str) -> None:
    conn = _mysql_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def mysql_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="mysql",
        host=MYSQL["host"],
        port=MYSQL["port"],
        database=MYSQL["database"],
        username=MYSQL["username"],
        password=MYSQL["password"],
        table=table,
    )


# --------------------------------------------------------------------------- #
# MongoDB
# --------------------------------------------------------------------------- #
def _mongo_client():
    import pymongo

    return pymongo.MongoClient(
        f"mongodb://{HOST}:{MONGO_PORT}/?replicaSet=rs0&directConnection=true",
        serverSelectionTimeoutMS=4000,
    )


def mongo_available() -> tuple[bool, str]:
    try:
        client = _mongo_client()
        client.admin.command("ping")
        client.close()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def mongo_seed(collection: str, rows: int, *, start: int = 1) -> None:
    from bson.decimal128 import Decimal128

    client = _mongo_client()
    try:
        coll = client[PG["database"]][collection]
        coll.drop()
        batch: list[dict[str, Any]] = []
        for row in iter_rows(rows, start=start):
            doc = dict(row)
            doc["amount"] = Decimal128(row["amount"])
            # BSON date is an instant: the naive column is seeded as the same
            # wall clock in UTC and the route declares that with
            # ``assume_timezone``, so the reinterpretation is the operator's,
            # not the engine's.
            batch.append(doc)
            if len(batch) >= 5000:
                coll.insert_many(batch, ordered=False)
                batch = []
        if batch:
            coll.insert_many(batch, ordered=False)
    finally:
        client.close()


def mongo_read(collection: str) -> Iterator[dict[str, Any]]:
    client = _mongo_client()
    try:
        cursor = client[PG["database"]][collection].find(
            {}, {"_id": 0}, batch_size=5000
        )
        for doc in cursor:
            amount = doc.get("amount")
            yield {
                "id": doc.get("id"),
                "uid": doc.get("uid"),
                "big_int": doc.get("big_int"),
                "amount": (
                    Decimal(str(amount)) if amount is not None else None
                ),
                "unicode_key": doc.get("unicode_key"),
                "payload": doc.get("payload"),
                "ts_naive": doc.get("ts_naive"),
                "ts_zoned": doc.get("ts_zoned"),
            }
    finally:
        client.close()


def mongo_drop(collection: str) -> None:
    client = _mongo_client()
    try:
        client[PG["database"]][collection].drop()
    finally:
        client.close()


def mongo_endpoint(collection: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="mongodb",
        host=HOST,
        port=MONGO_PORT,
        database=PG["database"],
        table=collection,
    )


# --------------------------------------------------------------------------- #
# Redis
# --------------------------------------------------------------------------- #
def _redis_client():
    import redis

    return redis.Redis(host=HOST, port=REDIS_PORT, db=0, socket_timeout=10)


def redis_available() -> tuple[bool, str]:
    try:
        client = _redis_client()
        client.ping()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def _redis_doc(row: dict[str, Any]) -> str:
    return json.dumps(
        {
            "id": str(row["id"]),
            "uid": row["uid"],
            "big_int": str(row["big_int"]),
            "amount": f"{row['amount']}",
            "ts_naive": row["ts_naive"].isoformat(),
            "ts_zoned": row["ts_zoned"].isoformat().replace("+00:00", "Z"),
            "unicode_key": row["unicode_key"],
            "payload": json.dumps(row["payload"], ensure_ascii=False),
        },
        ensure_ascii=False,
    )


def redis_seed(prefix: str, rows: int, *, start: int = 1) -> None:
    client = _redis_client()
    redis_drop(prefix)
    pipe = client.pipeline(transaction=False)
    for n, row in enumerate(iter_rows(rows, start=start), start=1):
        pipe.set(f"{prefix}:col_{row['id']}", _redis_doc(row))
        if n % 5000 == 0:
            pipe.execute()
            pipe = client.pipeline(transaction=False)
    pipe.execute()


def redis_read(prefix: str) -> Iterator[dict[str, Any]]:
    client = _redis_client()
    for key in client.scan_iter(match=f"{prefix}:*", count=2000):
        raw = client.get(key)
        if raw is None:
            continue
        doc = json.loads(raw)
        yield {
            "id": doc.get("id"),
            "uid": doc.get("uid"),
            "big_int": doc.get("big_int"),
            "amount": doc.get("amount"),
            "unicode_key": doc.get("unicode_key"),
            "payload": doc.get("payload"),
            "ts_naive": doc.get("ts_naive"),
            "ts_zoned": doc.get("ts_zoned"),
        }


def redis_drop(prefix: str) -> None:
    client = _redis_client()
    batch: list[Any] = []
    for key in client.scan_iter(match=f"{prefix}:*", count=2000):
        batch.append(key)
        if len(batch) >= 2000:
            client.delete(*batch)
            batch = []
    if batch:
        client.delete(*batch)


def redis_endpoint(prefix: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="redis",
        host=HOST,
        port=REDIS_PORT,
        database="0",
        table=prefix,
    )


# --------------------------------------------------------------------------- #
# DynamoDB Local
# --------------------------------------------------------------------------- #
def _dynamo_client():
    import boto3

    return boto3.client(
        "dynamodb",
        endpoint_url=f"http://{HOST}:{DYNAMO_PORT}",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )


def dynamo_available() -> tuple[bool, str]:
    try:
        _dynamo_client().list_tables()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def dynamo_seed(table: str, rows: int, *, start: int = 1) -> None:
    client = _dynamo_client()
    dynamo_drop(table)
    client.create_table(
        TableName=table,
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )
    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table)
    batch: list[dict[str, Any]] = []
    for row in iter_rows(rows, start=start):
        batch.append(
            {
                "PutRequest": {
                    "Item": {
                        "id": {"S": str(row["id"])},
                        "uid": {"S": row["uid"]},
                        "big_int": {"N": str(row["big_int"])},
                        "amount": {"N": f"{row['amount']}"},
                        "ts_naive": {"S": row["ts_naive"].isoformat()},
                        "ts_zoned": {
                            "S": row["ts_zoned"].isoformat().replace("+00:00", "Z")
                        },
                        "unicode_key": {"S": row["unicode_key"]},
                        "payload": {
                            "S": json.dumps(row["payload"], ensure_ascii=False)
                        },
                    }
                }
            }
        )
        if len(batch) >= 25:
            client.batch_write_item(RequestItems={table: batch})
            batch = []
    if batch:
        client.batch_write_item(RequestItems={table: batch})


def dynamo_read(table: str) -> Iterator[dict[str, Any]]:
    client = _dynamo_client()
    kwargs: dict[str, Any] = {"TableName": table}
    while True:
        page = client.scan(**kwargs)
        for item in page.get("Items", []):
            yield {
                "id": (item.get("id") or {}).get("S")
                or (item.get("id") or {}).get("N"),
                "uid": (item.get("uid") or {}).get("S"),
                "big_int": (item.get("big_int") or {}).get("N")
                or (item.get("big_int") or {}).get("S"),
                "amount": (item.get("amount") or {}).get("N")
                or (item.get("amount") or {}).get("S"),
                "unicode_key": (item.get("unicode_key") or {}).get("S"),
                "payload": (item.get("payload") or {}).get("S")
                or (item.get("payload") or {}).get("M"),
                "ts_naive": (item.get("ts_naive") or {}).get("S"),
                "ts_zoned": (item.get("ts_zoned") or {}).get("S"),
            }
        key = page.get("LastEvaluatedKey")
        if not key:
            return
        kwargs["ExclusiveStartKey"] = key


def dynamo_drop(table: str) -> None:
    client = _dynamo_client()
    try:
        client.delete_table(TableName=table)
        client.get_waiter("table_not_exists").wait(TableName=table)
    except Exception:  # noqa: BLE001 — absent table is the desired state
        pass


def dynamo_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="dynamodb",
        host=HOST,
        port=DYNAMO_PORT,
        database="us-east-1",
        username="test",
        password="test",
        region="us-east-1",
        table=table,
    )


# --------------------------------------------------------------------------- #
# DuckDB (file / in-process)
# --------------------------------------------------------------------------- #
def duckdb_path() -> str:
    os.makedirs(DUCKDB_DIR, exist_ok=True)
    return os.path.join(DUCKDB_DIR, "scale.duckdb")


def duckdb_available() -> tuple[bool, str]:
    try:
        import duckdb

        con = duckdb.connect(duckdb_path())
        con.execute("SELECT 1")
        con.close()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def duckdb_seed(table: str, rows: int, *, start: int = 1) -> None:
    import duckdb

    con = duckdb.connect(duckdb_path())
    try:
        con.execute(f'DROP TABLE IF EXISTS "{table}"')
        con.execute(
            f'CREATE TABLE "{table}" ('
            "id BIGINT PRIMARY KEY, uid VARCHAR, big_int BIGINT, "
            "amount DECIMAL(24,6), ts_naive TIMESTAMP, ts_zoned TIMESTAMPTZ, "
            "unicode_key VARCHAR, payload VARCHAR)"
        )
        con.execute("BEGIN")
        batch = []
        for row in iter_rows(rows, start=start):
            batch.append(
                (
                    row["id"],
                    row["uid"],
                    row["big_int"],
                    row["amount"],
                    row["ts_naive"],
                    row["ts_zoned"],
                    row["unicode_key"],
                    json.dumps(row["payload"], ensure_ascii=False),
                )
            )
            if len(batch) >= 10000:
                con.executemany(
                    f'INSERT INTO "{table}" VALUES (?,?,?,?,?,?,?,?)', batch
                )
                batch = []
        if batch:
            con.executemany(f'INSERT INTO "{table}" VALUES (?,?,?,?,?,?,?,?)', batch)
        con.execute("COMMIT")
    finally:
        con.close()


def duckdb_read(table: str) -> Iterator[dict[str, Any]]:
    import duckdb

    con = duckdb.connect(duckdb_path(), read_only=True)
    try:
        cur = con.execute(
            "SELECT id, uid, big_int, amount, unicode_key, payload, "
            f'ts_naive, ts_zoned FROM "{table}"'
        )
        while True:
            chunk = cur.fetchmany(5000)
            if not chunk:
                return
            for rec in chunk:
                yield {
                    "id": rec[0],
                    "uid": rec[1],
                    "big_int": rec[2],
                    "amount": rec[3],
                    "unicode_key": rec[4],
                    "payload": rec[5],
                    "ts_naive": rec[6],
                    "ts_zoned": rec[7],
                }
    finally:
        con.close()


def duckdb_drop(table: str) -> None:
    import duckdb

    con = duckdb.connect(duckdb_path())
    try:
        con.execute(f'DROP TABLE IF EXISTS "{table}"')
    finally:
        con.close()


def duckdb_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="duckdb",
        database=duckdb_path(),
        table=table,
    )


# --------------------------------------------------------------------------- #
# BigQuery emulator
# --------------------------------------------------------------------------- #
BQ_PROJECT = env("DATAFLOW_BQ_PROJECT", "dataflow-test")
BQ_DATASET = env("DATAFLOW_BQ_DATASET", "dataflow")


def _bq_client():
    from google.api_core.client_options import ClientOptions
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    return bigquery.Client(
        project=BQ_PROJECT,
        credentials=AnonymousCredentials(),
        client_options=ClientOptions(api_endpoint=f"http://{HOST}:{BIGQUERY_PORT}"),
    )


def bigquery_available() -> tuple[bool, str]:
    try:
        client = _bq_client()
        list(client.list_datasets(max_results=1))
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def bigquery_read(table: str) -> Iterator[dict[str, Any]]:
    client = _bq_client()
    query = (
        "SELECT id, uid, big_int, amount, unicode_key, payload, ts_naive, ts_zoned "
        f"FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    )
    for row in client.query(query).result(page_size=5000):
        yield {
            "id": row["id"],
            "uid": row["uid"],
            "big_int": row["big_int"],
            "amount": row["amount"],
            "unicode_key": row["unicode_key"],
            "payload": row["payload"],
            "ts_naive": row["ts_naive"],
            "ts_zoned": row["ts_zoned"],
        }


def bigquery_drop(table: str) -> None:
    """Drop the emulator table so each cell starts from a declared shape.

    Without this the second cell inherits the first cell's invented DDL: a table
    left behind as ``STRING`` made Validate fail-closed on ``DECIMAL(24,6) →
    TEXT`` for a route that had never been asked to write text.
    """
    client = _bq_client()
    client.query(
        f"DROP TABLE IF EXISTS `{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    ).result()


def bigquery_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="bigquery",
        host=HOST,
        port=BIGQUERY_PORT,
        connection_string=f"http://{HOST}:{BIGQUERY_PORT}",
        database=BQ_PROJECT,
        schema=BQ_DATASET,
        table=table,
    )


# --------------------------------------------------------------------------- #
# Elasticsearch
# --------------------------------------------------------------------------- #
def elasticsearch_available() -> tuple[bool, str]:
    """Reachability *and* the privilege probe the engine's preflight requires.

    A cluster that answers ``/`` but not ``/_security/user/_has_privileges``
    cannot be validated fail-closed, and this harness will not paper over that
    by skipping the gate: the route is reported ``skip`` with the probe error.
    """
    import urllib.error
    import urllib.request

    base = f"http://{HOST}:{ES_PORT}"
    try:
        with urllib.request.urlopen(f"{base}/", timeout=5) as resp:
            json.loads(resp.read())
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]
    req = urllib.request.Request(
        f"{base}/_security/user/_has_privileges",
        data=json.dumps({"cluster": ["monitor"]}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5).read()
        return True, ""
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")[:200]
        return False, f"HTTP {exc.code} on _has_privileges: {body}"
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def elasticsearch_read(index: str) -> Iterator[dict[str, Any]]:
    import urllib.request

    base = f"http://{HOST}:{ES_PORT}"
    body = json.dumps({"size": 2000, "sort": ["_doc"]}).encode()
    req = urllib.request.Request(
        f"{base}/{index}/_search?scroll=2m",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    payload = json.loads(urllib.request.urlopen(req, timeout=30).read())
    while True:
        hits = payload.get("hits", {}).get("hits", [])
        if not hits:
            return
        for hit in hits:
            doc = hit.get("_source", {})
            yield {k: doc.get(k) for k in PROJECTION_KEYS} | {
                "ts_naive": doc.get("ts_naive"),
                "ts_zoned": doc.get("ts_zoned"),
            }
        scroll_id = payload.get("_scroll_id")
        if not scroll_id:
            return
        nxt = urllib.request.Request(
            f"{base}/_search/scroll",
            data=json.dumps({"scroll": "2m", "scroll_id": scroll_id}).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        payload = json.loads(urllib.request.urlopen(nxt, timeout=30).read())


def elasticsearch_endpoint(index: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="elasticsearch",
        host=HOST,
        port=ES_PORT,
        database=index,
        table=index,
    )


# --------------------------------------------------------------------------- #
# ClickHouse
# --------------------------------------------------------------------------- #
def clickhouse_available() -> tuple[bool, str]:
    """Reachable *and* allowed to run a production transfer.

    ClickHouse ships as a ``Planned`` connector: the engine refuses the run
    rather than moving a customer's rows through an uncertified path. That
    refusal is the measured reason this route skips.
    """
    from src.transfer.connector_capabilities import (
        effective_status,
        get_capabilities,
        resolve_driver_type,
    )

    driver = resolve_driver_type("clickhouse")
    caps = get_capabilities(driver, "clickhouse")
    status = effective_status(caps)
    return False, (
        f"connector status={status} for driver={driver} — the engine refuses a "
        "production transfer on a non-certified connector"
    )


def clickhouse_read(table: str) -> Iterator[dict[str, Any]]:
    raise RuntimeError("clickhouse route is skipped; no read performed")


def clickhouse_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="clickhouse",
        host=HOST,
        port=CLICKHOUSE_PORT,
        database=PG["database"],
        username="default",
        password=env("DATAFLOW_CLICKHOUSE_PASSWORD", "dataflow"),
        table=table,
    )


# --------------------------------------------------------------------------- #
# Iceberg
# --------------------------------------------------------------------------- #
def iceberg_available() -> tuple[bool, str]:
    """An Iceberg destination needs a catalog this fleet does not run.

    ``docker-compose.yml`` provides MinIO for the warehouse but no REST catalog
    service, and the filesystem/SQL catalog fallbacks cannot prove destination
    table existence or CREATE privileges to the preflight gate. Skipped with
    that reason rather than validated with a weakened gate.
    """
    rest = os.environ.get("DATAFLOW_ICEBERG_REST_URI", "")
    if not rest:
        return False, (
            "no Iceberg REST catalog in docker-compose.yml (MinIO warehouse only) "
            "and no DATAFLOW_ICEBERG_REST_URI configured"
        )
    import urllib.request

    try:
        urllib.request.urlopen(f"{rest.rstrip('/')}/v1/config", timeout=5).read()
        return True, ""
    except Exception as exc:  # noqa: BLE001
        return False, f"{type(exc).__name__}: {exc}"[:300]


def iceberg_read(table: str) -> Iterator[dict[str, Any]]:
    raise RuntimeError("iceberg route is skipped; no read performed")


def iceberg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="iceberg",
        connection_string=os.environ.get("DATAFLOW_ICEBERG_REST_URI", ""),
        database=env("DATAFLOW_ICEBERG_WAREHOUSE", "s3://dataflow/warehouse"),
        schema="default",
        table=table,
    )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #
RELATIONAL: dict[str, EngineSpec] = {
    "postgresql": EngineSpec(
        name="postgresql",
        key_addressed=False,
        key_addressed_reason=(
            "row-addressed SQL table: an INSERT adds a row even when the id "
            "repeats, so append must land 2N"
        ),
        endpoint=pg_endpoint,
        read_projection=pg_read,
        availability=pg_available,
        seed=pg_seed,
        drop=pg_drop,
    ),
    "mysql": EngineSpec(
        name="mysql",
        key_addressed=False,
        key_addressed_reason=(
            "row-addressed SQL table; the fixture PK is declared so upsert is "
            "keyed, but a plain append still accumulates"
        ),
        endpoint=mysql_endpoint,
        read_projection=mysql_read,
        availability=mysql_available,
        seed=mysql_seed,
        drop=mysql_drop,
        carrier_notes={
            "ts_zoned": "MySQL TIMESTAMP stores UTC instants; the zone label is "
            "not carried, only the instant"
        },
    ),
}

TRACK_C: dict[str, EngineSpec] = {
    "mongodb": EngineSpec(
        name="mongodb",
        key_addressed=False,
        key_addressed_reason=(
            "row-addressed: the collection mints its own ``_id`` per insert, so "
            "the fixture id is an ordinary field and append lands 2N"
        ),
        endpoint=mongo_endpoint,
        read_projection=mongo_read,
        availability=mongo_available,
        seed=mongo_seed,
        drop=mongo_drop,
        assume_timezone="UTC",
        carrier_notes={
            "ts_naive": "BSON date is an instant with no zoneless spelling — the "
            "route must declare assume_timezone or the column is quarantined",
            "ts_zoned": "BSON date is millisecond precision; microseconds cannot "
            "round-trip",
        },
    ),
    "redis": EngineSpec(
        name="redis",
        key_addressed=True,
        key_addressed_reason=(
            "keyspace addressed by ``<prefix>:col_<id>``: SET on an existing key "
            "replaces the value, so append lands N"
        ),
        endpoint=redis_endpoint,
        read_projection=redis_read,
        availability=redis_available,
        seed=redis_seed,
        drop=redis_drop,
        carrier_notes={
            "all": "every value is stored as JSON text, so numeric and temporal "
            "fidelity is textual — checked by the content checksum"
        },
    ),
    "dynamodb": EngineSpec(
        name="dynamodb",
        key_addressed=True,
        key_addressed_reason=(
            "table with HASH key ``id``: PutItem on the same key replaces the "
            "item, so append lands N"
        ),
        endpoint=dynamo_endpoint,
        read_projection=dynamo_read,
        availability=dynamo_available,
        seed=dynamo_seed,
        drop=dynamo_drop,
    ),
    "duckdb": EngineSpec(
        name="duckdb",
        key_addressed=False,
        key_addressed_reason="row-addressed analytical SQL table",
        endpoint=duckdb_endpoint,
        read_projection=duckdb_read,
        availability=duckdb_available,
        seed=duckdb_seed,
        drop=duckdb_drop,
    ),
    "bigquery_emulator": EngineSpec(
        name="bigquery_emulator",
        db_type="bigquery",
        key_addressed=False,
        key_addressed_reason="row-addressed analytical table",
        endpoint=bigquery_endpoint,
        read_projection=bigquery_read,
        availability=bigquery_available,
        seed=None,
        drop=bigquery_drop,
        # BigQuery *does* spell a zoneless instant: DATETIME. So the naive
        # column lands as the wall clock it is and the zoned column lands in
        # TIMESTAMP — declaring assume_timezone here would promote the naive
        # column to TIMESTAMPTZ and then ask DATETIME to hold an offset, which
        # the product correctly refuses as a fidelity collapse.
        assume_timezone=None,
        source_role=False,
        notes="emulator only; hosted BigQuery is skip (no credentials)",
    ),
    "elasticsearch": EngineSpec(
        name="elasticsearch",
        key_addressed=True,
        key_addressed_reason=(
            "documents addressed by ``_id`` derived from the mapped key, so a "
            "re-index replaces rather than duplicates"
        ),
        endpoint=elasticsearch_endpoint,
        read_projection=elasticsearch_read,
        availability=elasticsearch_available,
        seed=None,
        drop=None,
        assume_timezone="UTC",
    ),
    "clickhouse": EngineSpec(
        name="clickhouse",
        key_addressed=False,
        key_addressed_reason="row-addressed analytical table (MergeTree)",
        endpoint=clickhouse_endpoint,
        read_projection=clickhouse_read,
        availability=clickhouse_available,
        seed=None,
        drop=None,
    ),
    "iceberg": EngineSpec(
        name="iceberg",
        key_addressed=False,
        key_addressed_reason="row-addressed table format",
        endpoint=iceberg_endpoint,
        read_projection=iceberg_read,
        availability=iceberg_available,
        seed=None,
        drop=None,
    ),
}

ALL_ENGINES: dict[str, EngineSpec] = {**RELATIONAL, **TRACK_C}
