"""Live Postgres proof for G20 — a rare population code missing from the map.

The sample of A/B/C would make a crosswalk look complete. The table also holds
Z. Validate/Execute must block, and an independent psycopg2 reread of the
destination must count 0 rows.

A second case with Z in the map must rewrite every code and land COUNT=5.

Skips when Postgres is not authenticating. Does not claim a cloud warehouse.
"""

from __future__ import annotations

import os
import uuid

import pytest

os.environ.setdefault("PGHOST", "127.0.0.1")
os.environ.setdefault("PGPORT", "5432")
os.environ.setdefault("PGDATABASE", "dataflow")
os.environ.setdefault("PGUSER", "dataflow")
os.environ.setdefault("PGPASSWORD", "dataflow")
os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from tests.helpers.live_env import pg_creds, pg_up

pytestmark = pytest.mark.skipif(not pg_up(), reason="PostgreSQL not authenticating")


def _connect():
    import psycopg2

    creds = pg_creds()
    return psycopg2.connect(
        host=creds["host"],
        port=int(creds["port"]),
        dbname=creds["database"],
        user=creds["username"],
        password=creds["password"],
        connect_timeout=5,
    )


def _exec(sql: str, params: tuple | None = None) -> None:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    finally:
        conn.close()


def _fetch(sql: str):
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()
    finally:
        conn.close()


def _count(table: str) -> int:
    rows = _fetch(f'SELECT COUNT(*) FROM public."{table}"')
    return int(rows[0][0])


def _ep(table: str):
    from src.transfer.models import EndpointConfig

    creds = pg_creds()
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=str(creds["host"]),
        port=int(creds["port"]),
        database=str(creds["database"]),
        schema="public",
        table=table,
        username=str(creds["username"]),
        password=str(creds["password"]),
        ssl=False,
    )


def _seed(src: str, dst: str) -> None:
    _exec(
        f'CREATE TABLE public."{src}" (id INTEGER PRIMARY KEY, status VARCHAR(8))'
    )
    _exec(
        f'CREATE TABLE public."{dst}" (id INTEGER PRIMARY KEY, status VARCHAR(16))'
    )
    # Population: A/B/C dominate; Z is the rare code a 25-row sample would miss.
    _exec(
        f'INSERT INTO public."{src}" (id, status) VALUES '
        "(1,'A'),(2,'A'),(3,'B'),(4,'C'),(5,'Z')"
    )


def _crosswalk(*, include_z: bool) -> dict[str, str]:
    table = {"A": "active", "B": "blocked", "C": "closed"}
    if include_z:
        table["Z"] = "archived"
    return table


def _mappings(*, include_z: bool) -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "INTEGER",
        },
        {
            "source": "status",
            "target": "status",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "VARCHAR",
            "code_crosswalk": _crosswalk(include_z=include_z),
            "code_crosswalk_system": "legacy_status→v2",
        },
    ]


def test_live_pg_unmapped_population_code_blocks_dest_empty() -> None:
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src = f"g20_src_{suffix}"
    dst = f"g20_dst_{suffix}"
    _seed(src, dst)
    independent = _fetch(
        f'SELECT status, COUNT(*) FROM public."{src}" GROUP BY status ORDER BY status'
    )
    assert ("Z", 1) in independent

    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_ep(src),
                destination=_ep(dst),
                mappings=_mappings(include_z=False),
                sync_mode="full_refresh_append",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is False
        blob = f"{result.error or ''} {result.error_details or ''}".lower()
        assert (
            "g20" in blob
            or "crosswalk" in blob
            or "unmapped" in blob
        ), blob
        # Independent reread — connection the transfer engine never touched.
        assert _count(dst) == 0
        remaining = _fetch(f'SELECT id, status FROM public."{src}" ORDER BY id')
        assert remaining[-1] == (5, "Z")
    finally:
        _exec(f'DROP TABLE IF EXISTS public."{dst}"')
        _exec(f'DROP TABLE IF EXISTS public."{src}"')


def test_live_pg_covered_population_rewrites_codes() -> None:
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src = f"g20_ok_src_{suffix}"
    dst = f"g20_ok_dst_{suffix}"
    _seed(src, dst)
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_ep(src),
                destination=_ep(dst),
                mappings=_mappings(include_z=True),
                sync_mode="full_refresh_append",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        # Independent reread — rewritten codes, never identity Z.
        dest = _fetch(f'SELECT id, status FROM public."{dst}" ORDER BY id')
        assert [row[1] for row in dest] == [
            "active",
            "active",
            "blocked",
            "closed",
            "archived",
        ]
        assert _count(dst) == 5
        assert _count(src) == 5
        assert ("Z",) not in _fetch(f'SELECT status FROM public."{dst}"')
    finally:
        _exec(f'DROP TABLE IF EXISTS public."{dst}"')
        _exec(f'DROP TABLE IF EXISTS public."{src}"')
