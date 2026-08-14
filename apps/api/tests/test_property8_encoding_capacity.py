"""PROPERTY 8 (encoding capacity) — dest engine stores the code points, or we say it does not.

MySQL utf8/utf8mb3 cannot store supplementary-plane characters. PostgreSQL UTF8
and MariaDB utf8mb4 store them as 4 UTF-8 bytes. OCTET_LENGTH / HEX on the
destination is dest-engine proof, not Python len(s.encode('utf-8')).
"""

from __future__ import annotations

import os
import socket
import uuid

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.encoding_capacity import dest_utf8_hex_sql, dest_utf8_octet_length_sql
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

EMOJI = "\U0001f600"
UTF8_HEX = EMOJI.encode("utf-8").hex().upper()


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
            "source": "txt",
            "target": "txt",
            "source_type": "TEXT",
            "target_type": "TEXT",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _encoding_status(report: dict) -> set[str]:
    return {
        i["status"]
        for i in (report.get("items") or [])
        if i.get("aspect") == "encoding"
    }


@pytest.mark.skipif(not _mysql_up(), reason="MariaDB not listening")
def test_mariadb_utf8mb3_rejects_emoji_under_strict_sql():
    """Dest-engine proof: utf8mb3 cannot store U+1F600. Strict SQL errors; we never '?'."""
    import pymysql

    my = _mysql_creds()
    table = f"p8_enc_mb3_{uuid.uuid4().hex[:8]}"
    conn = pymysql.connect(
        host=my["host"],
        port=my["port"],
        database=my["database"],
        user=my["username"],
        password=my["password"],
        autocommit=True,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'")
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  id BIGINT PRIMARY KEY,
                  txt VARCHAR(32) CHARACTER SET utf8mb3
                )
                """
            )
            with pytest.raises(pymysql.err.MySQLError):
                cur.execute(
                    f"INSERT INTO `{table}` (id, txt) VALUES (%s, %s)",
                    (1, EMOJI),
                )
            cur.execute(f"SELECT COUNT(*) FROM `{table}`")
            assert int(cur.fetchone()[0]) == 0
            cur.execute("SET SESSION sql_mode = ''")
            cur.execute(
                f"INSERT INTO `{table}` (id, txt) VALUES (%s, %s)",
                (2, EMOJI),
            )
            cur.execute(f"SELECT txt, HEX(txt) FROM `{table}` WHERE id = 2")
            stored, hexed = cur.fetchone()
            assert stored != EMOJI
            assert "?" in str(stored) or (hexed or "") in {"3F", "3F3F", "3F3F3F"}
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            conn.close()


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_emoji_lands_on_mariadb_utf8mb4_as_four_utf8_bytes():
    """Source TEXT emoji; dest OCTET_LENGTH=4 and HEX is F09F9880, not CESU-8."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_enc_src_{suffix}"
    dst_table = f"p8_enc_dst_{suffix}"
    conn = psycopg2.connect(
        host=pg["host"],
        port=pg["port"],
        dbname=pg["database"],
        user=pg["username"],
        password=pg["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(
                f'''
                CREATE TABLE public."{src_table}" (
                  id BIGINT PRIMARY KEY,
                  txt TEXT
                )
                '''
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, txt) VALUES (%s, %s), (%s, %s), (%s, %s)',
                (1, EMOJI, 2, "Alpha", 3, None),
            )
            cur.execute(
                f'SELECT {dest_utf8_octet_length_sql("postgresql", "txt")}, '
                f'{dest_utf8_hex_sql("postgresql", "txt")} '
                f'FROM public."{src_table}" WHERE id = 1'
            )
            octets, hexed = cur.fetchone()
            assert int(octets) == 4, (octets, hexed)
            assert str(hexed).upper() == UTF8_HEX.lower() or str(hexed).upper() == UTF8_HEX
    finally:
        conn.close()

    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="postgresql",
            host=pg["host"],
            port=pg["port"],
            database=pg["database"],
            username=pg["username"],
            password=pg["password"],
            schema="public",
            table=src_table,
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database",
            format="mysql",
            host=my["host"],
            port=my["port"],
            database=my["database"],
            username=my["username"],
            password=my["password"],
            schema="",
            table=dst_table,
            ssl=False,
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
        assert "carried" in _encoding_status(fid), fid

        conn = pymysql.connect(
            host=my["host"],
            port=my["port"],
            database=my["database"],
            user=my["username"],
            password=my["password"],
            autocommit=True,
            charset="utf8mb4",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT CHARACTER_SET_NAME FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = %s AND column_name = 'txt'
                    """,
                    (dst_table,),
                )
                charset = str((cur.fetchone() or [""])[0] or "").lower()
                assert charset in {"utf8mb4", "utf8"}, charset
                cur.execute(
                    f"SELECT id, txt, {dest_utf8_octet_length_sql('mysql', 'txt')}, "
                    f"{dest_utf8_hex_sql('mysql', 'txt')} "
                    f"FROM `{dst_table}` WHERE id = 1"
                )
                row_id, stored, octets, hexed = cur.fetchone()
                assert stored == EMOJI, stored
                assert int(octets) == 4, (octets, hexed)
                assert str(hexed).upper() == UTF8_HEX
                assert str(hexed).upper() != "EDA0BDEDB880"
                cur.execute(f"SELECT txt FROM `{dst_table}` WHERE id = 2")
                assert cur.fetchone()[0] == "Alpha"
                cur.execute(f"SELECT txt FROM `{dst_table}` WHERE id = 3")
                assert cur.fetchone()[0] is None
        finally:
            conn.close()
    finally:
        conn = psycopg2.connect(
            host=pg["host"],
            port=pg["port"],
            dbname=pg["database"],
            user=pg["username"],
            password=pg["password"],
        )
        conn.autocommit = True
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
        finally:
            conn.close()
        conn = pymysql.connect(
            host=my["host"],
            port=my["port"],
            database=my["database"],
            user=my["username"],
            password=my["password"],
            autocommit=True,
        )
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{dst_table}`")
        finally:
            conn.close()
