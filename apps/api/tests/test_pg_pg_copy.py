"""PostgreSQL → PostgreSQL binary COPY on append — dest COUNT(*) required."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _pg_or_skip():
    try:
        with socket.create_connection(("localhost", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def test_live_pg_to_pg_append_stream_dest_count(monkeypatch):
    """full_refresh_append must take binary COPY, not the 406 rows/s row path."""
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_pg_src_{tag}"
    dest = f"pg_pg_dst_{tag}"
    pg = psycopg2.connect(
        host="localhost", port=5432, user="dataflow", password="dataflow", dbname="dataflow",
    )
    pg.autocommit = True
    try:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
            cur.execute(
                f'CREATE TABLE public."{src}" (id bigint PRIMARY KEY, label varchar(32))'
            )
            cur.execute(
                f"""
                INSERT INTO public."{src}" (id, label)
                SELECT i, 'r' || i FROM generate_series(1, 800) AS s(i)
                """
            )
        job_id = f"pg-pg-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        cfg = {
            "format": "postgresql",
            "host": "localhost",
            "port": 5432,
            "database": "dataflow",
            "username": "dataflow",
            "password": "dataflow",
            "schema": "public",
        }
        source = EndpointConfig.from_dict("database", {**cfg, "table": src})
        destination = EndpointConfig.from_dict("database", {**cfg, "table": dest})
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {"source": "label", "target": "label", "type": "VARCHAR(32)", "transform": "none"},
        ]
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "VARCHAR(32)"},
            sync_mode="full_refresh_append",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_binary_server_to_server"
        assert summary.get("rejected_rows") == 0
        assert summary.get("source_row_count") == 800
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
            cur.execute(f'SELECT id, label FROM public."{dest}" WHERE id = 1')
            assert cur.fetchone() == (1, "r1")
    finally:
        with pg.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        pg.close()
