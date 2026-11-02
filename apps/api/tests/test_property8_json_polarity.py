"""PROPERTY 8 (JSON polarity) — \"1\" and 1 are different values.

psycopg2 decodes JSONB into Python. cell_to_string then json.loads turns the
JSON string \"1\" into the number 1. Dest JSON_TYPE says INTEGER; source
jsonb_typeof said string. DataFlow projects col::text so the engine spelling
is the wire, and certifies polarity from JSON_TYPE / jsonb_typeof.
"""

from __future__ import annotations

import json
import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.json_polarity import IEEE754_SAFE_INT, polarities_match
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PAYLOAD = {
    "n": 1,
    "s": "1",
    "b": True,
    "z": None,
    "big": IEEE754_SAFE_INT + 1,
}


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
            "source": "payload",
            "target": "payload",
            "source_type": "JSONB",
            "target_type": "JSON",
            "approved": True,
            "confidence": 0.99,
        },
    ]


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_jsonb_number_and_string_one_stay_distinct_on_mariadb():
    """Source jsonb_typeof n=number s=string; dest JSON_TYPE must agree."""
    import psycopg2
    import pymysql
    from psycopg2.extras import Json

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_json_src_{suffix}"
    dst_table = f"p8_json_dst_{suffix}"
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
                  payload JSONB
                )
                '''
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, payload) VALUES (%s, %s), (%s, %s)',
                (1, Json(PAYLOAD), 2, None),
            )
            cur.execute(
                f'''
                SELECT jsonb_typeof(payload->'n'),
                       jsonb_typeof(payload->'s'),
                       jsonb_typeof(payload->'b'),
                       jsonb_typeof(payload->'z'),
                       jsonb_typeof(payload->'big')
                  FROM public."{src_table}" WHERE id = 1
                '''
            )
            kinds = [str(x) for x in cur.fetchone()]
            assert kinds == ["number", "string", "boolean", "null", "number"], kinds
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
        assert result.records_transferred == 2, result.destination_summary

        conn = pymysql.connect(
            host=my["host"], port=my["port"], database=my["database"],
            user=my["username"], password=my["password"], autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT
                      JSON_TYPE(JSON_EXTRACT(payload, '$.n')),
                      JSON_TYPE(JSON_EXTRACT(payload, '$.s')),
                      JSON_TYPE(JSON_EXTRACT(payload, '$.b')),
                      JSON_TYPE(JSON_EXTRACT(payload, '$.z')),
                      JSON_TYPE(JSON_EXTRACT(payload, '$.big')),
                      JSON_UNQUOTE(JSON_EXTRACT(payload, '$.big'))
                    FROM `{dst_table}` WHERE id = 1
                    """
                )
                n, s, b, z, big, big_digits = cur.fetchone()
                assert polarities_match("number", str(n)), n
                assert polarities_match("string", str(s)), s
                assert polarities_match("boolean", str(b)), b
                assert polarities_match("null", str(z)), z
                assert polarities_match("number", str(big)), big
                assert str(big_digits) == str(IEEE754_SAFE_INT + 1), big_digits
                cur.execute(f"SELECT payload FROM `{dst_table}` WHERE id = 2")
                assert cur.fetchone()[0] is None
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
def test_mysql_json_string_one_stays_string_on_postgres():
    """Reverse route: dest jsonb_typeof(s) is string, not number."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_json_my_{suffix}"
    dst_table = f"p8_json_pg_{suffix}"
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
                  payload JSON,
                  PRIMARY KEY (id)
                )
                """
            )
            cur.execute(
                f"INSERT INTO `{src_table}` (id, payload) VALUES (1, %s)",
                (json.dumps(PAYLOAD),),
            )
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
        mappings=[
            {
                "source": "id",
                "target": "id",
                "source_type": "BIGINT",
                "target_type": "BIGINT",
                "approved": True,
                "confidence": 0.99,
            },
            {
                "source": "payload",
                "target": "payload",
                "source_type": "JSON",
                "target_type": "JSONB",
                "approved": True,
                "confidence": 0.99,
            },
        ],
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        conn = psycopg2.connect(
            host=pg["host"], port=pg["port"], dbname=pg["database"],
            user=pg["username"], password=pg["password"],
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f'''
                    SELECT jsonb_typeof(payload->'n'),
                           jsonb_typeof(payload->'s'),
                           (payload->>'big')
                      FROM public."{dst_table}" WHERE id = 1
                    '''
                )
                n, s, big = cur.fetchone()
                assert n == "number", n
                assert s == "string", s
                assert str(big) == str(IEEE754_SAFE_INT + 1), big
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
