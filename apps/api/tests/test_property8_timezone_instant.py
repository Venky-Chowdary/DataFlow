"""PROPERTY 8 (timezone instant) — the instant survives a hostile session TZ.

DMS copies MySQL TIMESTAMP digits under the server's session time_zone.
A dest session at +05:30 then displays a different wall clock while
UNIX_TIMESTAMP (the actual instant) has moved. DataFlow pins source and dest
to UTC, and MySQL TIMESTAMP leaves the wire as an instant (Z / +00:00) so
PostgreSQL TIMESTAMPTZ does not refuse naive digits.

Proof is UNIX_TIMESTAMP / EXTRACT(EPOCH) under a non-UTC dest session — not
the displayed civil clock.
"""

from __future__ import annotations

import os
import socket
import uuid
from datetime import datetime, timedelta, timezone

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

IST = timezone(timedelta(hours=5, minutes=30))
# 2024-03-01 12:00:00+05:30 == 2024-03-01 06:30:00 UTC
INSTANT = datetime(2024, 3, 1, 12, 0, 0, tzinfo=IST)
EPOCH = int(INSTANT.timestamp())


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
        "password": os.environ.get("P8_PG_PASSWORD", os.environ.get("P2_PG_PASSWORD", "dataflow")),
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


def _maps_pg_to_mysql():
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "instant_at",
            "target": "instant_at",
            "source_type": "TIMESTAMPTZ",
            "target_type": "TIMESTAMP(6)",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "wall_at",
            "target": "wall_at",
            "source_type": "TIMESTAMP",
            "target_type": "DATETIME(6)",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _maps_mysql_to_pg():
    return [
        {
            "source": "id",
            "target": "id",
            "source_type": "BIGINT",
            "target_type": "BIGINT",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "instant_at",
            "target": "instant_at",
            "source_type": "TIMESTAMP(6)",
            "target_type": "TIMESTAMPTZ",
            "approved": True,
            "confidence": 0.99,
        },
        {
            "source": "wall_at",
            "target": "wall_at",
            "source_type": "DATETIME(6)",
            "target_type": "TIMESTAMP",
            "approved": True,
            "confidence": 0.99,
        },
    ]


@pytest.mark.skipif(not _mysql_up(), reason="MariaDB not listening")
def test_generic_sql_mysql_pool_pins_session_utc():
    """Every pooled MySQL connection — source or dest — is UTC-pinned."""
    from connectors.generic_sql import get_sqlalchemy_engine
    from services.engine_pool import release_engine

    my = _mysql_creds()
    cfg = {
        "type": "mysql",
        "host": my["host"],
        "port": my["port"],
        "database": my["database"],
        "username": my["username"],
        "password": my["password"],
    }
    engine = get_sqlalchemy_engine(cfg)
    try:
        with engine.connect() as conn:
            zone = conn.exec_driver_sql("SELECT @@SESSION.time_zone").scalar()
        assert str(zone) == "+00:00", zone
    finally:
        release_engine(engine)


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_timestamptz_instant_survives_mariadb_hostile_session_tz():
    """Dest session +05:30 must not shift UNIX_TIMESTAMP of a carried instant."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_tz_src_{suffix}"
    dst_table = f"p8_tz_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["username"], password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  instant_at TIMESTAMPTZ NOT NULL,
                  wall_at TIMESTAMP NOT NULL
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, instant_at, wall_at)
                    VALUES (1, %s, %s)''',
                (INSTANT, datetime(2024, 3, 1, 12, 0, 0)),
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
        mappings=_maps_pg_to_mysql(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 1, result.destination_summary

        conn = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SET time_zone = '+05:30'")
                cur.execute(
                    f"SELECT instant_at, UNIX_TIMESTAMP(instant_at), wall_at FROM `{dst_table}`"
                )
                instant_disp, unix_ts, wall = cur.fetchone()
                assert int(float(unix_ts)) == EPOCH, (unix_ts, instant_disp)
                # Hostile session shows the IST wall clock of the same instant.
                assert str(instant_disp).startswith("2024-03-01 12:00:00"), instant_disp
                # Naive PG TIMESTAMP stayed wall-clock digits, not UTC-shifted.
                assert str(wall).startswith("2024-03-01 12:00:00"), wall
        finally:
            conn.close()
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
        conn = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            conn.close()


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_mysql_timestamp_written_under_ist_lands_as_pg_timestamptz_epoch():
    """Source session +05:30 stored UTC 06:30; reader pin must not re-shift it."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_tz_my_{suffix}"
    dst_table = f"p8_tz_pg_{suffix}"
    conn = pymysql.connect(
        host=my["host"], port=my["port"], database=my["database"],
        user=my["username"], password=my["password"], autocommit=True,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
            cur.execute(
                f"""
                CREATE TABLE `{src_table}` (
                  id BIGINT NOT NULL,
                  instant_at TIMESTAMP(6) NOT NULL,
                  wall_at DATETIME(6) NOT NULL,
                  PRIMARY KEY (id)
                )
                """
            )
            # Write under IST so stored TIMESTAMP UTC is 06:30, DATETIME stays 12:00.
            cur.execute("SET time_zone = '+05:30'")
            cur.execute(
                f"INSERT INTO `{src_table}` (id, instant_at, wall_at) "
                "VALUES (1, '2024-03-01 12:00:00', '2024-03-01 12:00:00')"
            )
            cur.execute("SET time_zone = '+00:00'")
            cur.execute(
                f"SELECT UNIX_TIMESTAMP(instant_at) FROM `{src_table}`"
            )
            assert int(float(cur.fetchone()[0])) == EPOCH
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database", format="mysql",
            host=my["host"], port=my["port"], database=my["database"],
            username=my["username"], password=my["password"],
            schema="", table=src_table, ssl=False,
        ),
        destination=EndpointConfig(
            kind="database", format="postgresql",
            host=pg["host"], port=pg["port"], database=pg["database"],
            username=pg["username"], password=pg["password"],
            schema="public", table=dst_table, ssl=False,
        ),
        mappings=_maps_mysql_to_pg(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        assert result.records_transferred == 1, (result.error, result.destination_summary)

        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute("SET TIME ZONE 'Asia/Kolkata'")
                cur.execute(
                    f'SELECT instant_at, EXTRACT(EPOCH FROM instant_at)::bigint, wall_at '
                    f'FROM public."{dst_table}"'
                )
                instant_disp, epoch, wall = cur.fetchone()
                assert int(epoch) == EPOCH, (epoch, instant_disp)
                assert wall.hour == 12, wall
                assert wall.tzinfo is None
        finally:
            conn.close()
    finally:
        conn = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{src_table}`")
        finally:
            conn.close()
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_table}"')
        finally:
            conn.close()
