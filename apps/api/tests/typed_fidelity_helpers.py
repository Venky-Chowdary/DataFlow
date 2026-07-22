"""Shared helpers for typed transfer fidelity e2e (preflight ON).

One source of truth for endpoint builders, typed seeds, and native PG
readback assertions. Matrices import these — do not copy-paste seeds.
"""

from __future__ import annotations

import socket
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest, TransferResult

# Canonical fixture shape used across SQL / schemaless sources.
FIDELITY_COLUMNS = (
    "id",
    "amt_dec",
    "amt_float",
    "note_null",
    "note_empty",
    "ts_utc",
    "flag",
)

# Expected native values after round-trip into PostgreSQL (row id=1).
EXPECTED_PG_ROW_1 = {
    "id": 1,
    "amt_dec": Decimal("10.5000"),
    "amt_float_approx": 1.5e3,  # IEEE — compare with float tolerance
    "note_null": None,
    "note_empty": "",
    "ts_utc": datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
    "flag": True,
}


def reachable(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        socket.create_connection((host, port), timeout=timeout).close()
        return True
    except OSError:
        return False


def require_ports(*ports: int, host: str = "localhost") -> None:
    import pytest

    missing = [p for p in ports if not reachable(host, p)]
    if missing:
        pytest.skip(f"Emulator not reachable on {host}:{missing}")


def uniq(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def pg_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host="localhost",
        port=5432,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def mysql_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="mysql",
        host="localhost",
        port=3306,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="dataflow",
        table=table,
    )


def mongo_endpoint(collection: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="mongodb",
        host="localhost",
        port=27017,
        database="dataflow",
        table=collection,
    )


def snowflake_endpoint(table: str) -> EndpointConfig:
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


def dynamo_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="dynamodb",
        host="localhost",
        port=8000,
        database="us-east-1",
        username="test",
        password="test",
        table=table,
    )


def seed_postgresql_typed(table: str) -> None:
    """Native PG types: NUMERIC vs DOUBLE PRECISION, NULL vs '', timestamptz, bool."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        cur.execute(
            f"""
            CREATE TABLE public."{table}" (
              id INT PRIMARY KEY,
              amt_dec NUMERIC(12,4) NOT NULL,
              amt_float DOUBLE PRECISION NOT NULL,
              note_null TEXT,
              note_empty TEXT NOT NULL,
              ts_utc TIMESTAMPTZ NOT NULL,
              flag BOOLEAN NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO public."{table}"
              (id, amt_dec, amt_float, note_null, note_empty, ts_utc, flag)
            VALUES
              (1, 10.5000, 1500.0, NULL, '',
               '2024-12-31 23:59:59+00', TRUE),
              (2, 0.0001, 2.5e-2, NULL, '',
               '2025-01-01 00:00:00+00', FALSE)
            """
        )
    conn.close()


def seed_mysql_typed(table: str) -> None:
    import pymysql

    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    with conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(
            f"""
            CREATE TABLE `{table}` (
              id INT PRIMARY KEY,
              amt_dec DECIMAL(12,4) NOT NULL,
              amt_float DOUBLE NOT NULL,
              note_null TEXT,
              note_empty VARCHAR(64) NOT NULL,
              ts_utc DATETIME(6) NOT NULL,
              flag TINYINT(1) NOT NULL
            )
            """
        )
        cur.execute(
            f"""
            INSERT INTO `{table}`
              (id, amt_dec, amt_float, note_null, note_empty, ts_utc, flag)
            VALUES
              (1, 10.5000, 1500.0, NULL, '',
               '2024-12-31 23:59:59.000000', 1),
              (2, 0.0001, 0.025, NULL, '',
               '2025-01-01 00:00:00.000000', 0)
            """
        )
    conn.close()


def seed_mongodb_typed(collection: str) -> None:
    from bson.decimal128 import Decimal128
    from pymongo import MongoClient

    client = MongoClient("localhost", 27017, serverSelectionTimeoutMS=5000)
    try:
        db = client["dataflow"]
        db[collection].drop()
        db[collection].insert_many(
            [
                {
                    "id": 1,
                    "amt_dec": Decimal128("10.5000"),
                    "amt_float": 1500.0,
                    "note_null": None,
                    "note_empty": "",
                    "ts_utc": datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc),
                    "flag": True,
                },
                {
                    "id": 2,
                    "amt_dec": Decimal128("0.0001"),
                    "amt_float": 0.025,
                    "note_null": None,
                    "note_empty": "",
                    "ts_utc": datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
                    "flag": False,
                },
            ]
        )
    finally:
        client.close()


def seed_dynamodb_typed(table: str) -> None:
    import boto3

    client = boto3.client(
        "dynamodb",
        endpoint_url="http://localhost:8000",
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
    )
    try:
        client.create_table(
            TableName=table,
            KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "N"}],
            BillingMode="PAY_PER_REQUEST",
        )
        waiter = client.get_waiter("table_exists")
        waiter.wait(TableName=table)
    except Exception:
        pass

    # Low-level API so Dynamo NULL is explicit (resource API strips None).
    for item in (
        {
            "id": {"N": "1"},
            "amt_dec": {"N": "10.5000"},
            "amt_float": {"N": "1500"},
            "note_null": {"NULL": True},
            "note_empty": {"S": ""},
            "ts_utc": {"S": "2024-12-31T23:59:59+00:00"},
            "flag": {"BOOL": True},
        },
        {
            "id": {"N": "2"},
            "amt_dec": {"N": "0.0001"},
            "amt_float": {"N": "0.025"},
            "note_null": {"NULL": True},
            "note_empty": {"S": ""},
            "ts_utc": {"S": "2025-01-01T00:00:00+00:00"},
            "flag": {"BOOL": False},
        },
    ):
        client.put_item(TableName=table, Item=item)


def seed_postgresql_decimal_sink(table: str) -> None:
    """Existing dest where amt_float is DECIMAL — proves float→decimal warning path."""
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        cur.execute(
            f"""
            CREATE TABLE public."{table}" (
              id INT PRIMARY KEY,
              amt_dec NUMERIC(12,4),
              amt_float NUMERIC(12,4),
              note_null TEXT,
              note_empty TEXT,
              ts_utc TIMESTAMPTZ,
              flag BOOLEAN
            )
            """
        )
    conn.close()


def drop_pg_table(table: str) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    conn.close()


def fidelity_stream_contract(name: str = "fidelity") -> list[dict]:
    return [
        {
            "name": name,
            "sync_mode": "full_refresh_overwrite",
            "primary_key": "id",
            "selected": True,
        }
    ]


def run_typed_transfer(
    source: EndpointConfig,
    destination: EndpointConfig,
    *,
    validation_mode: str = "strict",
    sync_mode: str = "full_refresh_overwrite",
    stream_name: str = "fidelity",
) -> TransferResult:
    """Execute with preflight ON (default). Never set skip_preflight=True here."""
    request = TransferRequest(
        source=source,
        destination=destination,
        sync_mode=sync_mode,
        validation_mode=validation_mode,
        stream_contracts=fidelity_stream_contract(stream_name),
        # skip_preflight defaults False — that is the point of this harness.
    )
    engine = UniversalTransferEngine()
    return engine.execute_tracked(request, uuid.uuid4().hex[:24])


def assert_preflight_ran(result: TransferResult) -> None:
    """Prove we did not silently skip Validate."""
    plan = result.validation_plan or {}
    assert plan, (
        "validation_plan empty — transfer likely skipped preflight "
        f"(success={result.success} error={result.error!r})"
    )
    # Prefer explicit passed flag when present; otherwise require gate inventory
    # (success path may store hard_gates/soft_gates summary only).
    if "passed" in plan:
        assert plan["passed"] is True, plan.get("blockers") or plan
        return
    if plan.get("gates"):
        return
    assert plan.get("hard_gates") or plan.get("soft_gates") or plan.get("total"), (
        f"preflight plan missing gate evidence: {plan.keys()}"
    )


def read_pg_row(table: str, row_id: int = 1) -> dict[str, Any]:
    import psycopg2

    cols = ", ".join(FIDELITY_COLUMNS)
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f'SELECT {cols} FROM public."{table}" WHERE id = %s',
                (row_id,),
            )
            row = cur.fetchone()
            assert row is not None, f"no row id={row_id} in {table}"
            out = dict(zip(FIDELITY_COLUMNS, row))
            # Native type metadata for FLOAT vs NUMERIC honesty.
            cur.execute(
                f"""
                SELECT a.attname, format_type(a.atttypid, a.atttypmod)
                FROM pg_attribute a
                JOIN pg_class c ON a.attrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE n.nspname = 'public' AND c.relname = %s
                  AND a.attnum > 0 AND NOT a.attisdropped
                """,
                (table,),
            )
            out["_pg_types"] = {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()
    return out


def assert_pg_typed_fidelity(
    table: str,
    *,
    expect_float_ddl: bool = True,
    float_may_be_numeric: bool = False,
    expect_decimal_scale: bool = True,
) -> None:
    """Assert native PG types + values for the fidelity fixture (row 1)."""
    row = read_pg_row(table, 1)
    types = row.pop("_pg_types")

    assert row["id"] == EXPECTED_PG_ROW_1["id"]
    assert row["amt_dec"] == EXPECTED_PG_ROW_1["amt_dec"], row["amt_dec"]
    assert isinstance(row["amt_dec"], Decimal)

    # FLOAT must stay IEEE when greenfield DDL is honest.
    float_ddl = (types.get("amt_float") or "").lower()
    if expect_float_ddl and not float_may_be_numeric:
        assert "double precision" in float_ddl or float_ddl.startswith("real"), (
            f"amt_float collapsed to non-float DDL: {types.get('amt_float')}"
        )
        assert not isinstance(row["amt_float"], Decimal), (
            f"amt_float stored as Decimal (lossy collapse): {row['amt_float']!r}"
        )
        assert abs(float(row["amt_float"]) - EXPECTED_PG_ROW_1["amt_float_approx"]) < 1e-9
    else:
        # Intentional DECIMAL sink (lossy path) — still must land the value.
        assert abs(float(row["amt_float"]) - EXPECTED_PG_ROW_1["amt_float_approx"]) < 1e-6

    dec_ddl = (types.get("amt_dec") or "").lower()
    if expect_decimal_scale:
        assert "numeric(12,4)" in dec_ddl or "decimal(12,4)" in dec_ddl, (
            f"DECIMAL scale stripped on CREATE: {types.get('amt_dec')}"
        )
    else:
        assert "numeric" in dec_ddl or "decimal" in dec_ddl, types.get("amt_dec")

    assert row["note_null"] is None, f"NULL became {row['note_null']!r}"
    assert row["note_empty"] == "", f"empty string became {row['note_empty']!r}"

    ts = row["ts_utc"]
    assert isinstance(ts, datetime), ts
    # MySQL DATETIME is naive; accept UTC-equivalent.
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    assert ts == EXPECTED_PG_ROW_1["ts_utc"], ts

    assert row["flag"] is True


def assert_lossy_float_decimal_surfaced(result: TransferResult) -> None:
    """Operator must be told about float→decimal — block or explicit warning."""
    plan = result.validation_plan or {}
    blob = (
        str(plan).lower()
        + " "
        + str(result.error or "").lower()
        + " "
        + str(result.error_details or "").lower()
        + " "
        + str(result.explanation or "").lower()
    )
    markers = (
        "float",
        "decimal",
        "lossy",
        "ieee",
        "precision",
        "coerce",
    )
    hit = sum(1 for m in markers if m in blob)
    assert hit >= 2, (
        "expected float→decimal risk to surface in validation_plan/error; "
        f"got plan keys={list(plan.keys())} error={result.error!r} "
        f"gates={[g.get('id') for g in (plan.get('gates') or [])][:8]}"
    )
