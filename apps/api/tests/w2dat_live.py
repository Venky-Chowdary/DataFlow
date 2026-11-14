"""Live-engine helpers for the data-correctness fixes (object prefix ``w2dat_``).

Runs go through the real ``UniversalTransferEngine.execute_tracked`` and the
destinations are read back with native drivers, never through Gate-8.
"""

from __future__ import annotations

import os
import socket
import uuid
from typing import Any

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")
os.environ.setdefault("DATAFLOW_CDC_LEASE_BACKEND", "memory")

PG = dict(
    host=os.environ.get("PGHOST") or "127.0.0.1",
    port=int(os.environ.get("PGPORT") or 5432),
    database=os.environ.get("PGDATABASE") or "dataflow",
    username=os.environ.get("PGUSER") or "dataflow",
    password=os.environ.get("PGPASSWORD") or "dataflow",
)
MY = dict(
    host=os.environ.get("MYSQL_HOST") or "127.0.0.1",
    port=int(os.environ.get("MYSQL_PORT") or 3306),
    database=os.environ.get("MYSQL_DATABASE") or "dataflow",
    username=os.environ.get("MYSQL_USER") or "dataflow",
    password=os.environ.get("MYSQL_PASSWORD") or "dataflow",
)


def uid() -> str:
    return uuid.uuid4().hex[:8]


def reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def need_pg() -> None:
    if not reachable(PG["host"], PG["port"]):
        pytest.skip("PostgreSQL is not reachable")


def need_mysql() -> None:
    if not reachable(MY["host"], MY["port"]):
        pytest.skip("MySQL is not reachable")


def need_redis() -> None:
    if not reachable("127.0.0.1", 6379):
        pytest.skip("Redis is not reachable")


def need_logical_pg() -> None:
    need_pg()
    if str(pg("SHOW wal_level")[0][0]) != "logical":
        pytest.skip("PostgreSQL wal_level is not logical")


def pg_conn():
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"], port=PG["port"], dbname=PG["database"],
        user=PG["username"], password=PG["password"],
    )
    conn.autocommit = True
    return conn


def pg(sql: str, params: Any = None) -> list[tuple]:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            try:
                return list(cur.fetchall())
            except Exception:
                return []
    finally:
        conn.close()


def my_conn():
    import pymysql

    return pymysql.connect(
        host=MY["host"], port=MY["port"], database=MY["database"], user=MY["username"],
        password=MY["password"], autocommit=True, charset="utf8mb4",
    )


def my(sql: str, params: Any = None) -> list[tuple]:
    conn = my_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return list(cur.fetchall() or [])
    finally:
        conn.close()


def ep_pg(table: str, schema: str = "public"):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="postgresql", schema=schema, table=table, **PG)


def ep_my(table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="mysql", table=table, **MY)


def ep_sqlite(path: Any, table: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="sqlite", database=str(path), table=table)


def ep_redis(prefix: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="redis", host="127.0.0.1", port=6379,
                          database="0", table=prefix)


def ep_mongo(coll: str):
    from src.transfer.models import EndpointConfig

    return EndpointConfig(kind="database", format="mongodb", host="127.0.0.1", port=27017,
                          database="dataflow", table=coll, collection=coll)


def run(source, dest, mode: str = "full_refresh_overwrite", *, key: Any = None,
        cols: list[str] | None = None, validation_mode: str = "strict",
        contract_extra: dict | None = None, extra_req: dict | None = None,
        job_id: str | None = None):
    """``execute_tracked`` one stream; returns the TransferResult."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    contract: dict[str, Any] = {"name": source.table or "t", "sync_mode": mode, "selected": True}
    if key:
        contract["primary_key"] = key
    contract.update(contract_extra or {})
    kw: dict[str, Any] = dict(
        source=source, destination=dest, sync_mode=mode, validation_mode=validation_mode,
        stream_contracts=[contract], ddl_changes_acknowledged=True,
    )
    if cols:
        kw["mappings"] = [
            {"source": c, "target": c, "confidence": 1.0, "approved": True, "user_override": True}
            for c in cols
        ]
    kw.update(extra_req or {})
    return UniversalTransferEngine().execute_tracked(TransferRequest(**kw), job_id or uuid.uuid4().hex[:24])


def drop_pg(*tables: str) -> None:
    for t in tables:
        pg(f'DROP TABLE IF EXISTS public."{t}" CASCADE')


def drop_my(*tables: str) -> None:
    for t in tables:
        my(f"DROP TABLE IF EXISTS `{t}`")


def drop_cdc_artifacts(token: str) -> None:
    """Drop this test's replication slots and publications (named after ``token``)."""
    for (slot,) in pg(
        "SELECT slot_name FROM pg_replication_slots WHERE NOT active AND slot_name ILIKE %s",
        (f"%{token}%",),
    ):
        pg("SELECT pg_drop_replication_slot(%s)", (slot,))
    for (pub,) in pg("SELECT pubname FROM pg_publication WHERE pubname ILIKE %s", (f"%{token}%",)):
        pg(f'DROP PUBLICATION IF EXISTS "{pub}"')


def redis_drop(prefix: str) -> None:
    import redis

    client = redis.Redis(host="127.0.0.1", port=6379, db=0)
    for k in client.scan_iter(match=f"{prefix}*", count=1000):
        client.delete(k)
