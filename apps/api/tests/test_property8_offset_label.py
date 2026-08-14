"""PROPERTY 8 (offset-label) — dest engine stores +05:30, or we say it does not.

PostgreSQL TIMESTAMPTZ stores UTC and drops the INSERT offset. EXTRACT(TIMEZONE)
under SET TIME ZONE UTC is 0 even when the client sent +05:30. That is dest-engine
proof, not Python tzinfo. Instant (epoch 1709271000) may still land.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.offset_label import postgres_session_timezone_seconds_sql
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

IST = timezone(timedelta(hours=5, minutes=30))
INSTANT = datetime(2024, 3, 1, 12, 0, 0, tzinfo=IST)
EPOCH = 1709271000


def _run(req: TransferRequest):
    return UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])


def _pg_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=0.4):
            return True
    except OSError:
        return False


def _mysql_up() -> bool:
    try:
        with socket.create_connection(("127.0.0.1", 3306), timeout=0.4):
            return True
    except OSError:
        return False


def _pg_creds() -> dict:
    return {
        "host": os.environ.get("P8_PG_HOST", os.environ.get("P2_PG_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P8_PG_PORT", os.environ.get("P2_PG_PORT", "5432"))),
        "database": os.environ.get("P8_PG_DB", os.environ.get("P2_PG_DB", "dataflow")),
        "username": os.environ.get("P8_PG_USER", os.environ.get("P2_PG_USER", "dataflow")),
        "password": os.environ.get(
            "P8_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")
        ),
    }


def _mysql_creds() -> dict:
    return {
        "host": os.environ.get("P8_MYSQL_HOST", os.environ.get("P2_MYSQL_HOST", "127.0.0.1")),
        "port": int(os.environ.get("P8_MYSQL_PORT", os.environ.get("P2_MYSQL_PORT", "3306"))),
        "database": os.environ.get("P8_MYSQL_DB", os.environ.get("P2_MYSQL_DB", "dataflow")),
        "username": os.environ.get("P8_MYSQL_USER", os.environ.get("P2_MYSQL_USER", "dataflow")),
        "password": os.environ.get(
            "P8_MYSQL_PASSWORD", os.environ.get("P2_MYSQL_PASSWORD", "dataflow")
        ),
    }


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening")
def test_pg_timestamptz_drops_originating_offset_under_utc_session():
    """INSERT +05:30; dest EXTRACT(TIMEZONE) under UTC is 0; epoch is 1709271000."""
    import psycopg2

    pg = _pg_creds()
    table = f"p8_off_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f'CREATE TABLE public."{table}" (id BIGINT PRIMARY KEY, ts TIMESTAMPTZ)'
            )
            cur.execute(
                f'INSERT INTO public."{table}" (id, ts) VALUES (%s, %s)',
                (1, INSTANT),
            )
            cur.execute("SET TIME ZONE 'UTC'")
            cur.execute(
                f'SELECT EXTRACT(EPOCH FROM ts), '
                f'{postgres_session_timezone_seconds_sql("ts")}, ts::text '
                f'FROM public."{table}" WHERE id = 1'
            )
            epoch, session_tz, rendered = cur.fetchone()
        assert int(epoch) == EPOCH, (epoch, rendered)
        assert int(session_tz) == 0, (session_tz, rendered)
        text = str(rendered)
        assert "+00" in text or text.endswith("Z") or "+00:00" in text, text
        assert "+05:30" not in text, text
    finally:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_to_mariadb_instant_lands_offset_label_is_skipped():
    """Source TIMESTAMPTZ never stored a label; dest TIMESTAMP cannot invent one."""
    import psycopg2

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_off_src_{suffix}"
    dst_table = f"p8_off_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                f"(id BIGINT PRIMARY KEY, ts TIMESTAMPTZ NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, ts) VALUES (%s, %s)',
                (1, INSTANT),
            )
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="postgresql",
            host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"],
            schema="public", table=src_table, ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="mysql",
            host=my["host"], port=my["port"], database=my["database"],
            username=my["username"], password=my["password"],
            schema="", table=dst_table, ssl=False,
        ),
        mappings=[
            {
                "source": "id", "target": "id",
                "source_type": "BIGINT", "target_type": "BIGINT",
                "approved": True, "confidence": 0.99,
            },
            {
                "source": "ts", "target": "ts",
                "source_type": "TIMESTAMPTZ", "target_type": "TIMESTAMP(6)",
                "approved": True, "confidence": 0.99,
            },
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        statuses = {
            i.get("status")
            for i in (fid.get("items") or [])
            if i.get("aspect") == "offset_label"
        }
        assert "skipped" in statuses, fid
        assert "carried" not in statuses, fid
    finally:
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        import pymysql

        dest = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with dest.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            dest.close()
