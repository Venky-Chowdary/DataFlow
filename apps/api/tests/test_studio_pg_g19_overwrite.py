"""Live PG overwrite: G19 hard-blocks DECIMAL → INTEGER and dest stays INTEGER.

Unsigned full_refresh_overwrite onto an existing INTEGER column must not
recreate the table as NUMERIC. Independent ``information_schema`` + COUNT(*)
are the proof. Does not claim 100K, CRM overwrite, or a contracted Execute.
"""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

PG = dict(
    host="127.0.0.1",
    port=5432,
    database="dataflow",
    user="dataflow",
    password="dataflow",
)


def _pg_or_skip():
    try:
        with socket.create_connection((PG["host"], PG["port"]), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pg_connect():
    psycopg2 = pytest.importorskip("psycopg2")
    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        user=PG["user"],
        password=PG["password"],
        dbname=PG["database"],
    )
    conn.autocommit = True
    return conn


def _endpoint(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        username=PG["user"],
        password=PG["password"],
        schema="public",
        table=table,
    )


def test_studio_pg_overwrite_g19_blocks_and_dest_stays_integer(monkeypatch):
    _pg_or_skip()
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down

    ensure_memory_job_store_if_mongo_down()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")

    tag = uuid.uuid4().hex[:8]
    src_table = f"g19_src_{tag}"
    dest_table = f"g19_dst_{tag}"
    pg = _pg_connect()
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
            cur.execute(
                f'CREATE TABLE public."{src_table}" '
                f"(amt_dec numeric(20,9) NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src_table}" (amt_dec) VALUES '
                f"(123.456789000), (1.5), (999.001)"
            )
            cur.execute(
                f'CREATE TABLE public."{dest_table}" '
                f"(amt_dec integer NOT NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{dest_table}" (amt_dec) VALUES (7)'
            )

        engine = UniversalTransferEngine()
        result = engine.execute_tracked(
            TransferRequest(
                source=_endpoint(src_table),
                destination=_endpoint(dest_table),
                sync_mode="full_refresh_overwrite",
                skip_preflight=False,
                validation_mode="strict",
                mappings=[
                    {
                        "source": "amt_dec",
                        "target": "amt_dec",
                        "confidence": 0.99,
                        "source_type": "DECIMAL(20,9)",
                    }
                ],
            ),
            f"g19-{tag}",
        )
        assert result.success is False, result.error
        assert "g19" in (result.error or "").lower() or "replace" in (
            result.error or ""
        ).lower(), result.error

        with pg.cursor() as cur:
            cur.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = %s "
                "AND column_name = 'amt_dec'",
                (dest_table,),
            )
            data_type = (cur.fetchone() or [None])[0]
            cur.execute(f'SELECT COUNT(*) FROM public."{dest_table}"')
            count = int(cur.fetchone()[0])
            cur.execute(f'SELECT amt_dec FROM public."{dest_table}"')
            leftover = [int(r[0]) for r in cur.fetchall()]
        assert data_type == "integer", data_type
        assert count == 1, count
        assert leftover == [7], leftover
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src_table}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest_table}"')
        pg.close()
