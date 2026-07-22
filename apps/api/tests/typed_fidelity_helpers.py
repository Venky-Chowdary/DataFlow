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


def sqlite_endpoint(db_path: str, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(db_path),
        table=table,
    )


def redis_endpoint(prefix: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="redis",
        host="localhost",
        port=6379,
        database="0",
        table=prefix,
    )


def sqlserver_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlserver",
        host="localhost",
        port=1433,
        database="dataflow",
        username="sa",
        password="DataFlow_CDC_2022!",
        schema="dbo",
        table=table,
    )


def redshift_endpoint(table: str, *, port: int = 5432) -> EndpointConfig:
    """Redshift writer path against local PG stand-in (honest label in asserts)."""
    return EndpointConfig(
        kind="database",
        format="redshift",
        host="localhost",
        port=port,
        database="dataflow",
        username="dataflow",
        password="dataflow",
        schema="public",
        table=table,
    )


def bigquery_endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="bigquery",
        host="localhost",
        port=9050,
        connection_string="http://localhost:9050",
        database="dataflow-test",
        schema="dataflow",
        table=table,
    )


def oracle_endpoint(table: str) -> EndpointConfig:
    import os

    return EndpointConfig(
        kind="database",
        format="oracle",
        host=os.getenv("DATAFLOW_ORACLE_HOST", "localhost"),
        port=int(os.getenv("DATAFLOW_ORACLE_PORT", "1521")),
        database=os.getenv("DATAFLOW_ORACLE_SERVICE")
        or os.getenv("DATAFLOW_ORACLE_DATABASE", "ORCLPDB1"),
        username=os.getenv("DATAFLOW_ORACLE_USER", "dataflow"),
        password=os.getenv("DATAFLOW_ORACLE_PASSWORD", "dataflow"),
        schema=os.getenv("DATAFLOW_ORACLE_SCHEMA")
        or os.getenv("DATAFLOW_ORACLE_USER", "dataflow"),
        table=table,
    )


def duckdb_endpoint(db_path: str, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="duckdb",
        database=str(db_path),
        table=table,
    )


def require_oracle_env() -> None:
    import os

    import pytest

    if os.getenv("DATAFLOW_ORACLE_ENABLE", "").strip() not in {"1", "true", "yes"}:
        pytest.skip("Oracle typed e2e requires DATAFLOW_ORACLE_ENABLE=1")
    require_ports(int(os.getenv("DATAFLOW_ORACLE_PORT", "1521")), host=os.getenv("DATAFLOW_ORACLE_HOST", "localhost"))


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


def drop_mysql_table(table: str) -> None:
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
    conn.close()


def read_mysql_row(table: str, row_id: int = 1) -> dict[str, Any]:
    import pymysql

    cols = ", ".join(f"`{c}`" for c in FIDELITY_COLUMNS)
    conn = pymysql.connect(
        host="localhost",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT {cols} FROM `{table}` WHERE `id` = %s",
                (row_id,),
            )
            row = cur.fetchone()
            assert row is not None, f"no row id={row_id} in {table}"
            out = dict(zip(FIDELITY_COLUMNS, row))
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
                """,
                (table,),
            )
            out["_mysql_types"] = {r[0]: (r[1], r[2]) for r in cur.fetchall()}
    finally:
        conn.close()
    return out


def assert_mysql_typed_fidelity(table: str) -> None:
    """Native MySQL types + values — catches ISO-Z DATETIME 1292 class bugs."""
    row = read_mysql_row(table, 1)
    types = row.pop("_mysql_types")

    assert row["id"] == 1
    assert isinstance(row["amt_dec"], Decimal), type(row["amt_dec"])
    assert row["amt_dec"] == Decimal("10.5000"), row["amt_dec"]
    assert abs(float(row["amt_float"]) - 1500.0) < 1e-9
    assert row["note_null"] is None, f"NULL became {row['note_null']!r}"
    assert row["note_empty"] == "", f"empty string became {row['note_empty']!r}"

    ts = row["ts_utc"]
    assert isinstance(ts, datetime), ts
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    assert ts == datetime(2024, 12, 31, 23, 59, 59), ts

    assert row["flag"] in (True, 1)

    dec_meta = types.get("amt_dec") or ("", "")
    assert "decimal" in (dec_meta[0] or "").lower(), types.get("amt_dec")
    float_meta = types.get("amt_float") or ("", "")
    assert (float_meta[0] or "").lower() in {"double", "float", "real"}, types.get(
        "amt_float"
    )
    ts_meta = types.get("ts_utc") or ("", "")
    assert (ts_meta[0] or "").lower() in {"datetime", "timestamp"}, types.get("ts_utc")


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


def assert_redis_typed_fidelity(prefix: str) -> None:
    """JSON wire honesty — Redis has no SQL DDL; prove NULL≠'' and decimal fidelity."""
    import json

    import redis

    client = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5, decode_responses=True)
    try:
        keys = list(client.scan_iter(match=f"{prefix}:*", count=200))
        assert len(keys) >= 2, f"expected redis keys under {prefix}:*, got {keys}"
        doc = None
        for key in keys:
            candidate = json.loads(client.get(key) or "null")
            if not isinstance(candidate, dict):
                continue
            if candidate.get("id") in (1, "1"):
                doc = candidate
                break
        assert doc is not None, f"no doc with id=1 under {prefix}:* keys={keys}"
    finally:
        client.close()

    assert doc.get("note_null") is None, f"NULL became {doc.get('note_null')!r}"
    assert doc.get("note_empty") == "", f"empty string became {doc.get('note_empty')!r}"
    # Decimal must not collapse through float64 into something like 10.499999…
    dec = doc.get("amt_dec")
    assert Decimal(str(dec)) == Decimal("10.5000") or Decimal(str(dec)) == Decimal("10.5"), dec
    assert abs(float(doc.get("amt_float")) - 1500.0) < 1e-6
    ts = str(doc.get("ts_utc") or "")
    assert "2024-12-31" in ts, ts
    assert doc.get("flag") in (True, "true", 1, "1")


def assert_redshift_standin_typed_fidelity(table: str) -> None:
    """PG stand-in with redshift DDL branch — TIMESTAMP not TIMESTAMPTZ."""
    assert_pg_typed_fidelity(table, expect_float_ddl=True, expect_decimal_scale=True)
    row = read_pg_row(table, 1)
    types = row.pop("_pg_types")
    ts_ddl = (types.get("ts_utc") or "").lower()
    # Redshift DDL map uses TIMESTAMP (no TZ). Stand-in must not invent timestamptz.
    assert "timestamp" in ts_ddl, types.get("ts_utc")
    assert "timestamptz" not in ts_ddl and "with time zone" not in ts_ddl, (
        f"redshift stand-in used PG timestamptz: {types.get('ts_utc')}"
    )


def drop_sqlserver_table(table: str) -> None:
    import pymssql

    conn = pymssql.connect(
        server="localhost",
        port=1433,
        user="sa",
        password="DataFlow_CDC_2022!",
        database="dataflow",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"IF OBJECT_ID('dbo.[{table}]', 'U') IS NOT NULL DROP TABLE dbo.[{table}]")
        conn.commit()
    finally:
        conn.close()


def read_sqlserver_row(table: str, row_id: int = 1) -> dict[str, Any]:
    """Prefer pymssql; fall back to pyodbc."""
    cols = ", ".join(f"[{c}]" for c in FIDELITY_COLUMNS)
    try:
        import pymssql

        conn = pymssql.connect(
            server="localhost",
            port=1433,
            user="sa",
            password="DataFlow_CDC_2022!",
            database="dataflow",
        )
        try:
            with conn.cursor(as_dict=True) as cur:
                cur.execute(
                    f"SELECT {cols} FROM dbo.[{table}] WHERE [id] = %s",
                    (row_id,),
                )
                row = cur.fetchone()
                assert row is not None, f"no row id={row_id} in {table}"
                cur.execute(
                    """
                    SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = %s
                    """,
                    (table,),
                )
                meta = {r["COLUMN_NAME"]: r for r in cur.fetchall()}
        finally:
            conn.close()
        out = {c: row[c] for c in FIDELITY_COLUMNS}
        out["_mssql_types"] = meta
        return out
    except ImportError:
        import pyodbc

        conn = pyodbc.connect(
            "DRIVER={ODBC Driver 17 for SQL Server};"
            "SERVER=localhost,1433;DATABASE=dataflow;UID=sa;PWD=DataFlow_CDC_2022!;"
            "TrustServerCertificate=yes;"
        )
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT {cols} FROM dbo.[{table}] WHERE [id] = ?", row_id)
            values = cur.fetchone()
            assert values is not None
            out = dict(zip(FIDELITY_COLUMNS, values))
            cur.execute(
                """
                SELECT COLUMN_NAME, DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = ?
                """,
                table,
            )
            meta = {r[0]: {"COLUMN_NAME": r[0], "DATA_TYPE": r[1],
                           "NUMERIC_PRECISION": r[2], "NUMERIC_SCALE": r[3]}
                    for r in cur.fetchall()}
            out["_mssql_types"] = meta
            return out
        finally:
            conn.close()


def assert_sqlserver_typed_fidelity(table: str) -> None:
    row = read_sqlserver_row(table, 1)
    types = row.pop("_mssql_types")

    assert row["id"] == 1
    assert Decimal(str(row["amt_dec"])) == Decimal("10.5000"), row["amt_dec"]
    assert abs(float(row["amt_float"]) - 1500.0) < 1e-6
    assert row["note_null"] is None, f"NULL became {row['note_null']!r}"
    assert row["note_empty"] == "", f"empty string became {row['note_empty']!r}"
    ts = row["ts_utc"]
    assert isinstance(ts, datetime), ts
    if ts.tzinfo is not None:
        ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
    assert ts.replace(microsecond=0) == datetime(2024, 12, 31, 23, 59, 59)
    assert row["flag"] in (True, 1)

    dec = types.get("amt_dec") or {}
    assert (dec.get("DATA_TYPE") or "").lower() in {"decimal", "numeric"}, dec
    # Prefer preserved scale 4 when carrier DECIMAL(12,4) survived CREATE.
    scale = dec.get("NUMERIC_SCALE")
    if scale is not None:
        assert int(scale) == 4, f"DECIMAL scale stripped: {dec}"
    float_t = (types.get("amt_float") or {}).get("DATA_TYPE", "").lower()
    assert float_t in {"float", "real"}, types.get("amt_float")
    ts_t = (types.get("ts_utc") or {}).get("DATA_TYPE", "").lower()
    assert ts_t in {"datetime2", "datetime", "datetimeoffset"}, types.get("ts_utc")


def assert_duckdb_typed_fidelity(db_path: str, table: str) -> None:
    import duckdb

    con = duckdb.connect(str(db_path))
    try:
        row = con.execute(
            f'SELECT id, amt_dec, amt_float, note_null, note_empty, ts_utc, flag '
            f'FROM "{table}" WHERE id = 1'
        ).fetchone()
        assert row is not None
        assert row[0] == 1
        assert Decimal(str(row[1])) == Decimal("10.5000"), row[1]
        assert abs(float(row[2]) - 1500.0) < 1e-9
        assert row[3] is None, f"NULL became {row[3]!r}"
        assert row[4] == ""
        meta = con.execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_name = '{table}'"
        ).fetchall()
        types = {r[0]: r[1] for r in meta}
    finally:
        con.close()
    dec_t = (types.get("amt_dec") or "").upper()
    assert "DECIMAL" in dec_t or "NUMERIC" in dec_t, types.get("amt_dec")
    # Prefer (12,4) when carrier preserved.
    if "(" in dec_t:
        assert "12,4" in dec_t.replace(" ", "") or "12, 4" in dec_t, types.get("amt_dec")
    float_t = (types.get("amt_float") or "").upper()
    assert any(x in float_t for x in ("DOUBLE", "FLOAT", "REAL")), types.get("amt_float")


def assert_bigquery_typed_fidelity(table: str) -> None:
    from google.cloud import bigquery
    from google.auth.credentials import AnonymousCredentials

    client = bigquery.Client(
        project="dataflow-test",
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": "http://localhost:9050"},
    )
    full = f"dataflow-test.dataflow.{table}"
    tbl = client.get_table(full)
    field_types = {f.name: f.field_type for f in tbl.schema}
    rows = list(client.list_rows(tbl, max_results=10))
    row = next(r for r in rows if int(r.get("id")) == 1)

    assert Decimal(str(row.get("amt_dec"))) == Decimal("10.5000") or Decimal(
        str(row.get("amt_dec"))
    ) == Decimal("10.5"), row.get("amt_dec")
    assert abs(float(row.get("amt_float")) - 1500.0) < 1e-6
    assert row.get("note_null") is None, row.get("note_null")
    assert row.get("note_empty") == ""
    assert field_types.get("amt_float") in {"FLOAT64", "FLOAT"}, field_types
    assert field_types.get("amt_dec") in {"BIGNUMERIC", "NUMERIC"}, field_types
    # Prefer fixed-point for DECIMAL source.
    assert field_types.get("amt_dec") != "FLOAT64", (
        f"DECIMAL collapsed to FLOAT64: {field_types}"
    )
    assert field_types.get("ts_utc") in {"TIMESTAMP", "DATETIME"}, field_types


def require_sqlserver_drivers() -> None:
    import pytest

    try:
        import pymssql  # noqa: F401
        return
    except ImportError:
        pass
    try:
        import pyodbc  # noqa: F401
        return
    except ImportError:
        pytest.skip("Neither pymssql nor pyodbc installed for SQL Server typed e2e")


def cleanup_redis_prefix(prefix: str) -> None:
    import redis

    client = redis.Redis(host="localhost", port=6379, db=0, socket_timeout=5)
    try:
        keys = list(client.scan_iter(match=f"{prefix}:*", count=100))
        if keys:
            client.delete(*keys)
    finally:
        client.close()
