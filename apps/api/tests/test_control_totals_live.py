"""Live Postgres proof for G21 control totals and G22 dest RI.

Independent psycopg2 connections (never the transfer engine's) re-read SUM
and dest anti-join after the write. Skips when Postgres is not authenticating.
"""

from __future__ import annotations

import os
import uuid
from decimal import Decimal

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


def _amount_mappings() -> list[dict]:
    return [
        {
            "source": "id",
            "target": "id",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "INTEGER",
        },
        {
            "source": "amount",
            "target": "amount",
            "confidence": 0.99,
            "transform": "none",
            "target_type": "NUMERIC(12,2)",
            "control_total": True,
        },
    ]


def test_live_pg_control_total_match_independent_sum() -> None:
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src = f"g21_src_{suffix}"
    dst = f"g21_dst_{suffix}"
    _exec(
        f'CREATE TABLE public."{src}" '
        "(id INTEGER PRIMARY KEY, amount NUMERIC(12,2) NOT NULL)"
    )
    _exec(
        f'CREATE TABLE public."{dst}" '
        "(id INTEGER PRIMARY KEY, amount NUMERIC(12,2) NOT NULL)"
    )
    _exec(
        f'INSERT INTO public."{src}" (id, amount) VALUES '
        "(1, 10.00), (2, 20.50)"
    )
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_ep(src),
                destination=_ep(dst),
                mappings=_amount_mappings(),
                sync_mode="full_refresh_append",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is True, result.error
        # Independent reread — connection the transfer engine never touched.
        src_sum = _fetch(f'SELECT CAST(COALESCE(SUM(amount), 0) AS TEXT) FROM public."{src}"')
        dst_sum = _fetch(f'SELECT CAST(COALESCE(SUM(amount), 0) AS TEXT) FROM public."{dst}"')
        assert Decimal(str(src_sum[0][0])) == Decimal("30.50")
        assert Decimal(str(dst_sum[0][0])) == Decimal("30.50")
        dst_count = _fetch(f'SELECT COUNT(*) FROM public."{dst}"')
        assert int(dst_count[0][0]) == 2
        recon = result.reconciliation if isinstance(result.reconciliation, dict) else {}
        ct = recon.get("control_totals") or {}
        assert ct.get("declared") is True
        assert (recon.get("g21_control_totals") or {}).get("status") == "pass"
    finally:
        _exec(f'DROP TABLE IF EXISTS public."{dst}"')
        _exec(f'DROP TABLE IF EXISTS public."{src}"')


def test_live_pg_control_total_mismatch_same_count_fails() -> None:
    """COUNT matches, SUM does not — the bank-examiner case."""
    from services.control_totals import verify_control_totals

    suffix = uuid.uuid4().hex[:8]
    src = f"g21_mis_src_{suffix}"
    dst = f"g21_mis_dst_{suffix}"
    _exec(
        f'CREATE TABLE public."{src}" '
        "(id INTEGER PRIMARY KEY, amount NUMERIC(12,2) NOT NULL)"
    )
    _exec(
        f'CREATE TABLE public."{dst}" '
        "(id INTEGER PRIMARY KEY, amount NUMERIC(12,2) NOT NULL)"
    )
    _exec(
        f'INSERT INTO public."{src}" (id, amount) VALUES (1, 10.00), (2, 20.50)'
    )
    # Same two rows, one cent short — COUNT(*)=2 on both sides.
    _exec(
        f'INSERT INTO public."{dst}" (id, amount) VALUES (1, 10.00), (2, 20.49)'
    )
    creds = pg_creds()
    cfg = {
        "type": "postgresql",
        "host": creds["host"],
        "port": creds["port"],
        "database": creds["database"],
        "username": creds["username"],
        "password": creds["password"],
        "schema": "public",
    }
    try:
        src_count = _fetch(f'SELECT COUNT(*) FROM public."{src}"')[0][0]
        dst_count = _fetch(f'SELECT COUNT(*) FROM public."{dst}"')[0][0]
        assert int(src_count) == int(dst_count) == 2
        report, gate = verify_control_totals(
            mappings=_amount_mappings(),
            source_db_type="postgresql",
            source_cfg=cfg,
            source_schema="public",
            source_table=src,
            dest_db_type="postgresql",
            dest_cfg=dict(cfg),
            dest_schema="public",
            dest_table=dst,
            phase="execute",
        )
        assert gate["status"] == "block"
        col = report["columns"][0]
        assert Decimal(str(col["source_sum"])) == Decimal("30.50")
        assert Decimal(str(col["dest_sum"])) == Decimal("20.49") + Decimal("10.00")
        independent = _fetch(
            f'SELECT CAST(COALESCE(SUM(amount), 0) AS TEXT) FROM public."{dst}"'
        )
        assert Decimal(str(independent[0][0])) == Decimal("30.49")
    finally:
        _exec(f'DROP TABLE IF EXISTS public."{dst}"')
        _exec(f'DROP TABLE IF EXISTS public."{src}"')


def test_live_pg_dest_ri_orphan_fails_after_write() -> None:
    """Parent incomplete on dest: child COUNT>0, dest anti-join finds orphans."""
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    suffix = uuid.uuid4().hex[:8]
    src_schema = f"g22s_{suffix}"
    dst_schema = f"g22d_{suffix}"
    creds = pg_creds()

    def _ep_schema(schema: str, table: str) -> EndpointConfig:
        return EndpointConfig(
            kind="database",
            format="postgresql",
            host=str(creds["host"]),
            port=int(creds["port"]),
            database=str(creds["database"]),
            schema=schema,
            table=table,
            username=str(creds["username"]),
            password=str(creds["password"]),
            ssl=False,
        )

    _exec(f'CREATE SCHEMA "{src_schema}"')
    _exec(f'CREATE SCHEMA "{dst_schema}"')
    _exec(f'CREATE TABLE "{src_schema}".parent (id INTEGER PRIMARY KEY)')
    _exec(
        f'CREATE TABLE "{src_schema}".child ('
        f'id INTEGER PRIMARY KEY, '
        f'parent_id INTEGER REFERENCES "{src_schema}".parent(id))'
    )
    _exec(f'INSERT INTO "{src_schema}".parent (id) VALUES (1), (2)')
    _exec(f'INSERT INTO "{src_schema}".child (id, parent_id) VALUES (1, 1), (2, 2)')
    # Dest parent incomplete (load-speed / partial parent load). Dest child has no FK.
    _exec(f'CREATE TABLE "{dst_schema}".parent (id INTEGER PRIMARY KEY)')
    _exec(f'INSERT INTO "{dst_schema}".parent (id) VALUES (1)')
    _exec(
        f'CREATE TABLE "{dst_schema}".child '
        "(id INTEGER PRIMARY KEY, parent_id INTEGER)"
    )
    try:
        result = UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=_ep_schema(src_schema, "child"),
                destination=_ep_schema(dst_schema, "child"),
                mappings=[
                    {
                        "source": "id",
                        "target": "id",
                        "confidence": 0.99,
                        "transform": "none",
                        "target_type": "INTEGER",
                    },
                    {
                        "source": "parent_id",
                        "target": "parent_id",
                        "confidence": 0.99,
                        "transform": "none",
                        "target_type": "INTEGER",
                    },
                ],
                sync_mode="full_refresh_append",
                validation_mode="strict",
            ),
            uuid.uuid4().hex[:24],
        )
        assert result.success is False, (
            f"dest orphans must fail Gate-8: {result.error!r} {result.reconciliation!r}"
        )
        blob = f"{result.error or ''} {result.error_details or ''} {result.reconciliation or ''}".lower()
        assert (
            "g22" in blob
            or "referential" in blob
            or "orphan" in blob
        ), blob
        dest_count = int(
            _fetch(f'SELECT COUNT(*) FROM "{dst_schema}".child')[0][0]
        )
        assert dest_count >= 1, "rows landed; dest RI is the closure, not empty dest"
        orphan = _fetch(
            f'SELECT COUNT(*) FROM "{dst_schema}".child c '
            f'LEFT JOIN "{dst_schema}".parent p ON c.parent_id = p.id '
            "WHERE c.parent_id IS NOT NULL AND p.id IS NULL"
        )
        assert int(orphan[0][0]) >= 1
    finally:
        _exec(f'DROP SCHEMA IF EXISTS "{dst_schema}" CASCADE')
        _exec(f'DROP SCHEMA IF EXISTS "{src_schema}" CASCADE')
