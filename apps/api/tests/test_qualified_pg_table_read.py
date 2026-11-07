"""Studio ``public.table`` must read ``public.table``, not ``public.public.table``."""

from __future__ import annotations

import uuid

from connectors.postgresql_reader import _bind, count_table_rows
from connectors.sql_identifiers import split_qualified_table

_PG = dict(
    host="127.0.0.1",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)


def test_pg_reader_bind_does_not_double_prefix() -> None:
    assert _bind("public", "public.case_a_src") == ("public", "case_a_src")
    assert _bind("public", "case_a_src") == ("public", "case_a_src")
    assert split_qualified_table("public.case_a_dst", "public") == ("public", "case_a_dst")


def _count(table: str) -> int:
    return count_table_rows(
        schema="public", connection_string="", ssl=False, table=table, **_PG
    )


def test_live_pg_count_accepts_qualified_studio_name() -> None:
    import psycopg2

    from tests.typed_fidelity_helpers import require_ports

    require_ports(5432)
    table = f"qual_src_{uuid.uuid4().hex[:8]}"
    conn = psycopg2.connect(
        host=_PG["host"],
        port=_PG["port"],
        dbname=_PG["database"],
        user=_PG["username"],
        password=_PG["password"],
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f'CREATE TABLE public."{table}" (id int)')
            cur.execute(f'INSERT INTO public."{table}" VALUES (1), (2), (3)')
        assert _count(f"public.{table}") == _count(table) == 3
    finally:
        with conn, conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
        conn.close()
