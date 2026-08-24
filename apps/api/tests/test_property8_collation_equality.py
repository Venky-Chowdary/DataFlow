"""PROPERTY 8 (collation equality) — UNIQUE meaning travels, or we say it did not.

DMS copies bytes into the destination's default collation. MySQL CI then
treats Alpha and alpha as one key; the second row is MISSING_TARGET while
checksums of *accepted* rows stay green. DataFlow classifies source equality
and emits a destination-native CS spelling (utf8mb4_bin) so both rows land,
or refuses to claim CI was carried onto PostgreSQL.
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


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


def _collation_status(report: dict) -> set[str]:
    return {
        i["status"]
        for i in (report.get("items") or [])
        if i.get("aspect") == "collation"
    }


def _maps():
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
            "source": "code",
            "target": "code",
            "source_type": "TEXT",
            "target_type": "VARCHAR(32)",
            "approved": True,
            "confidence": 0.99,
        },
    ]


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_cs_unique_pair_survives_mariadb_instead_of_ci_collision():
    """Source UNIQUE accepted Alpha and alpha; dest default CI would not."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_cs_src_{suffix}"
    dst_table = f"p8_cs_dst_{suffix}"
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
                  code TEXT NOT NULL,
                  UNIQUE (code)
                )
                '''
            )
            cur.execute(
                f'''INSERT INTO public."{src_table}" (id, code)
                    VALUES (1, 'Alpha'), (2, 'alpha')'''
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
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        assert "carried" in _collation_status(fid), fid

        conn = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT collation_name FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = %s AND column_name = 'code'
                    """,
                    (dst_table,),
                )
                coll = str((cur.fetchone() or [""])[0] or "").lower()
                assert "_bin" in coll or coll.endswith("_cs"), coll
                cur.execute(f"SELECT code FROM `{dst_table}` ORDER BY id")
                rows = [r[0] for r in cur.fetchall()]
                assert rows == ["Alpha", "alpha"], rows
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
def test_mysql_ci_unique_is_not_claimed_on_postgres_and_widens():
    """Dest PG will accept alpha; source UNIQUE would not. Certificate must say so."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_ci_src_{suffix}"
    dst_table = f"p8_ci_dst_{suffix}"
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
                  code VARCHAR(32) NOT NULL COLLATE utf8mb4_unicode_ci,
                  PRIMARY KEY (id),
                  UNIQUE (code)
                )
                """
            )
            cur.execute(f"INSERT INTO `{src_table}` (id, code) VALUES (1, 'Alpha')")
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
        mappings=_maps(),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        assert "unsupported" in _collation_status(fid), fid

        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'INSERT INTO public."{dst_table}" (id, code) VALUES (%s, %s)',
                    (2, "alpha"),
                )
                conn.commit()
                cur.execute(f'SELECT code FROM public."{dst_table}" ORDER BY id')
                rows = [r[0] for r in cur.fetchall()]
                assert rows == ["Alpha", "alpha"], rows
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
