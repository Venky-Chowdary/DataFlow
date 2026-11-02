"""Case A live — rounded decimals land as integers, proven by a dest re-read.

The Studio path (shape on the read → Validate image → Execute) writes into an
*existing* INT column. An independent second connection then ``SUM``s the
destination. The fixture distinguishes rounding from truncation:

    22.6 + 21.4 + 22.0 = 66.0 source
    round-half-up  → 23 + 21 + 22 = 66
    truncate       → 22 + 21 + 22 = 65

A product that truncated would pass a row-count check and still fail this test.
PostgreSQL and MySQL are both required when their ports are up; a missing
engine is a skip, never a silent pass.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import TransferRequest
from tests.typed_fidelity_helpers import mysql_endpoint, pg_endpoint, reachable, require_ports, uniq

CASE_A_SOURCE = (
    (1, Decimal("22.6")),
    (2, Decimal("21.4")),
    (3, Decimal("22.0")),
)
ROUNDED_SUM = 66
TRUNCATED_SUM = 65
ROUND_RECIPE = {
    "steps": [{"op": "round_number", "column": "arr_time", "options": {"places": 0}}]
}


def _pg():
    import psycopg2

    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="dataflow",
        user="dataflow",
        password="dataflow",
    )


def _mysql():
    import pymysql

    return pymysql.connect(
        host="localhost",
        port=3306,
        user="dataflow",
        password="dataflow",
        database="dataflow",
        autocommit=True,
    )


def _mappings() -> list[dict[str, object]]:
    return [
        {"source": "id", "target": "id", "target_type": "INT", "approved": True, "confidence": 0.99},
        {
            "source": "arr_time",
            "target": "arr_time",
            "target_type": "INT",
            "approved": True,
            "confidence": 0.99,
        },
    ]


def _seed_pg_source(table: str) -> None:
    conn = _pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{table}" (
                  id INT PRIMARY KEY,
                  arr_time NUMERIC(12,1) NOT NULL
                )
                """
            )
            cur.executemany(
                f'INSERT INTO public."{table}" (id, arr_time) VALUES (%s, %s)',
                list(CASE_A_SOURCE),
            )
    finally:
        conn.close()


def _seed_pg_dest(table: str) -> None:
    conn = _pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
            cur.execute(
                f"""
                CREATE TABLE public."{table}" (
                  id INT PRIMARY KEY,
                  arr_time INT NOT NULL
                )
                """
            )
    finally:
        conn.close()


def _seed_mysql_dest(table: str) -> None:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
            cur.execute(
                f"""
                CREATE TABLE `{table}` (
                  id INT PRIMARY KEY,
                  arr_time INT NOT NULL
                )
                """
            )
    finally:
        conn.close()


def _drop_pg(table: str) -> None:
    conn = _pg()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


def _drop_mysql(table: str) -> None:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS `{table}`")
    finally:
        conn.close()


def _sum_pg(table: str) -> tuple[int, list[int]]:
    conn = _pg()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT arr_time FROM public."{table}" ORDER BY id')
            values = [int(r[0]) for r in cur.fetchall()]
            cur.execute(f'SELECT COALESCE(SUM(arr_time), 0) FROM public."{table}"')
            total = int(cur.fetchone()[0])
            return total, values
    finally:
        conn.close()


def _sum_mysql(table: str) -> tuple[int, list[int]]:
    conn = _mysql()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT arr_time FROM `{table}` ORDER BY id")
            values = [int(r[0]) for r in cur.fetchall()]
            cur.execute(f"SELECT COALESCE(SUM(arr_time), 0) FROM `{table}`")
            total = int(cur.fetchone()[0])
            return total, values
    finally:
        conn.close()


def _execute(request: TransferRequest):
    return UniversalTransferEngine().execute_tracked(request, uuid.uuid4().hex[:24])


def test_case_a_postgresql_existing_int_sum_is_rounded_not_truncated() -> None:
    require_ports(5432)
    src = uniq("case_a_pg_src")
    dst = uniq("case_a_pg_dst")
    _seed_pg_source(src)
    _seed_pg_dest(dst)
    try:
        result = _execute(
            TransferRequest(
                source=pg_endpoint(src),
                destination=pg_endpoint(dst),
                mappings=_mappings(),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                shape_recipe=ROUND_RECIPE,
            )
        )
        assert result.success, result.error
        assert result.records_transferred == 3, result.error
        total, values = _sum_pg(dst)
        assert values == [23, 21, 22], values
        assert total == ROUNDED_SUM
        assert total != TRUNCATED_SUM
    finally:
        _drop_pg(src)
        _drop_pg(dst)


def test_case_a_mysql_existing_int_sum_is_rounded_not_truncated() -> None:
    if not reachable("localhost", 3306):
        pytest.skip("MySQL not reachable on localhost:3306")
    require_ports(5432, 3306)
    src = uniq("case_a_my_src")
    dst = uniq("case_a_my_dst")
    _seed_pg_source(src)
    _seed_mysql_dest(dst)
    try:
        result = _execute(
            TransferRequest(
                source=pg_endpoint(src),
                destination=mysql_endpoint(dst),
                mappings=_mappings(),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
                shape_recipe=ROUND_RECIPE,
            )
        )
        assert result.success, result.error
        assert result.records_transferred == 3, result.error
        total, values = _sum_mysql(dst)
        assert values == [23, 21, 22], values
        assert total == ROUNDED_SUM
        assert total != TRUNCATED_SUM
    finally:
        _drop_pg(src)
        _drop_mysql(dst)
