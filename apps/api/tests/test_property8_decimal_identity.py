"""PROPERTY 8 (decimal identity) — dest engine stores the unscaled integer, or we say it does not.

PostgreSQL NUMERIC(p,s) rounds excess fractional digits (ties away from zero).
MySQL DECIMAL under STRICT refuses them. SQLite DECIMAL affinity is IEEE REAL.
Certify from dest ``::text`` / ``CAST AS CHAR``, not from Python Decimal.
"""

from __future__ import annotations

import os
import socket
import uuid
from decimal import Decimal

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.decimal_identity import (
    IEEE754_SAFE_INT,
    dest_numeric_text_sql,
    extract_decimal_identity,
    identities_same_magnitude,
)
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

TIE = "1.225"
MONEY = "1.2300"
WIDE = str(IEEE754_SAFE_INT + 1) + ".25"


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


def _maps(dest_amt_type: str = "DECIMAL(20,4)"):
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
            "source": "amt",
            "target": "amt",
            "source_type": "NUMERIC(20,4)",
            "target_type": dest_amt_type,
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _decimal_status(report: dict) -> set[str]:
    return {
        i["status"]
        for i in (report.get("items") or [])
        if i.get("aspect") == "decimal"
    }


@pytest.mark.skipif(not _pg_up(), reason="PostgreSQL not listening")
def test_pg_numeric_scale_two_rounds_tie_away_from_zero():
    """Dest-engine proof: NUMERIC(10,2) stores 1.23 for INSERT 1.225, not 1.225."""
    import psycopg2

    pg = _pg_creds()
    table = f"p8_dec_pg_{uuid.uuid4().hex[:8]}"
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
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f'CREATE TABLE public."{table}" (amt NUMERIC(10,2) PRIMARY KEY)'
            )
            cur.execute(f'INSERT INTO public."{table}" (amt) VALUES (%s)', (Decimal(TIE),))
            cur.execute(
                f'SELECT {dest_numeric_text_sql("postgresql", "amt")} FROM public."{table}"'
            )
            stored = str(cur.fetchone()[0])
            src = extract_decimal_identity(TIE)
            dst = extract_decimal_identity(stored)
            assert src is not None and dst is not None
            assert Decimal(stored) == Decimal("1.23")
            assert stored != TIE
            assert not identities_same_magnitude(src, dst)
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        finally:
            conn.close()


@pytest.mark.skipif(not _mysql_up(), reason="MariaDB not listening")
def test_mariadb_decimal_strict_still_rounds_excess_scale():
    """Dest-engine proof: STRICT_TRANS_TABLES still stores 1.23 for INSERT 1.225.

    Strict mode is not an exact-decimal guarantee. The unscaled integer of
    1.225 does not land. That is why narrower dest scale is ``unsupported``.
    """
    import pymysql

    my = _mysql_creds()
    table = f"p8_dec_my_{uuid.uuid4().hex[:8]}"
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
            cur.execute(
                "SET SESSION sql_mode = "
                "'STRICT_TRANS_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'"
            )
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"CREATE TABLE `{table}` (id BIGINT PRIMARY KEY, amt DECIMAL(10,2))"
            )
            cur.execute(
                f"INSERT INTO `{table}` (id, amt) VALUES (%s, %s)",
                (1, Decimal(TIE)),
            )
            cur.execute(
                f"SELECT {dest_numeric_text_sql('mysql', 'amt')} FROM `{table}` WHERE id = 1"
            )
            stored = str(cur.fetchone()[0])
            src = extract_decimal_identity(TIE)
            dst = extract_decimal_identity(stored)
            assert src is not None and dst is not None
            assert stored != TIE
            assert Decimal(stored) == Decimal("1.23")
            assert not identities_same_magnitude(src, dst)
    finally:
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        finally:
            conn.close()


@pytest.mark.skipif(not (_pg_up() and _mysql_up()), reason="PostgreSQL or MariaDB not listening")
def test_pg_numeric_digits_land_on_mariadb_decimal_not_float():
    """Source NUMERIC(20,4) money + beyond-IEEE unscaled integer; dest CAST AS CHAR matches."""
    import psycopg2
    import pymysql

    pg = _pg_creds()
    my = _mysql_creds()
    suffix = uuid.uuid4().hex[:8]
    src_table = f"p8_dec_src_{suffix}"
    dst_table = f"p8_dec_dst_{suffix}"
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
                  amt NUMERIC(20,4)
                )
                '''
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (id, amt) VALUES (%s, %s), (%s, %s), (%s, %s)',
                (1, Decimal(MONEY), 2, Decimal(WIDE), 3, None),
            )
            cur.execute(
                f'SELECT {dest_numeric_text_sql("postgresql", "amt")} '
                f'FROM public."{src_table}" WHERE id = 1'
            )
            src_text = str(cur.fetchone()[0])
            assert Decimal(src_text) == Decimal(MONEY)
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
        mappings=_maps("DECIMAL(20,4)"),
        sync_mode="full_refresh_overwrite",
        validation_mode="warn",
        skip_preflight=True,
    )
    try:
        result = _run(req)
        assert result.success, result.error
        fid = (result.destination_summary or {}).get("schema_fidelity") or {}
        assert "carried" in _decimal_status(fid), fid

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
                cur.execute(
                    """
                    SELECT DATA_TYPE, NUMERIC_PRECISION, NUMERIC_SCALE
                      FROM information_schema.columns
                     WHERE table_schema = DATABASE()
                       AND table_name = %s AND column_name = 'amt'
                    """,
                    (dst_table,),
                )
                dtype, prec, scale = cur.fetchone()
                assert str(dtype).lower() in {"decimal", "numeric"}
                assert int(scale) >= 4, (dtype, prec, scale)
                cur.execute(
                    f"SELECT id, {dest_numeric_text_sql('mysql', 'amt')} "
                    f"FROM `{dst_table}` ORDER BY id"
                )
                rows = cur.fetchall()
                by_id = {int(r[0]): r[1] for r in rows}
                dest_money = extract_decimal_identity(str(by_id[1]))
                src_money = extract_decimal_identity(MONEY)
                assert dest_money is not None and src_money is not None
                assert identities_same_magnitude(src_money, dest_money)
                dest_wide = extract_decimal_identity(str(by_id[2]))
                src_wide = extract_decimal_identity(WIDE)
                assert dest_wide is not None and src_wide is not None
                assert dest_wide.beyond_ieee is True
                assert identities_same_magnitude(src_wide, dest_wide)
                assert float(WIDE) != Decimal(WIDE)
                assert by_id[3] is None
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
