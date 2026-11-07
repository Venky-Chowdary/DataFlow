"""Create-new INTEGER→SERIAL invent must refuse — dest COUNT 0.

Capacity promotion used to rewrite SERIAL into BIGINT so Validate never saw
the identity invent and Execute landed dest COUNT=1. Identity polarity is
not width. ``100%`` is not claimed here. Skip when Postgres is closed.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

PG = dict(
    host="localhost",
    port=5432,
    database="dataflow",
    username="dataflow",
    password="dataflow",
)


def _reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.5):
            return True
    except OSError:
        return False


def _sqlite_ep(path: Path, table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="sqlite",
        database=str(path),
        table=table,
        connection_string=f"sqlite:///{path}",
        ssl=False,
    )


def _pg_ep(table: str) -> EndpointConfig:
    return EndpointConfig(
        kind="database",
        format="postgresql",
        schema="public",
        table=table,
        connection_string="",
        ssl=False,
        **PG,
    )


def _pg_dest_count(table: str) -> int:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = 'public' AND table_name = %s",
                (table,),
            )
            if int(cur.fetchone()[0]) == 0:
                return 0
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            return int(cur.fetchone()[0])
    finally:
        conn.close()


def _pg_drop(table: str) -> None:
    import psycopg2

    conn = psycopg2.connect(
        host=PG["host"],
        port=PG["port"],
        database=PG["database"],
        user=PG["username"],
        password=PG["password"],
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    finally:
        conn.close()


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="Postgres not reachable")
@pytest.mark.parametrize(
    "invented",
    ("SERIAL", "BIGSERIAL", "INTEGER GENERATED ALWAYS AS IDENTITY"),
)
def test_live_integer_to_serial_invent_is_refused(invented: str) -> None:
    """Plain INTEGER must not invent a sequence generator. Dest COUNT stays 0."""
    suffix = uuid.uuid4().hex[:8]
    src_t = f"id_src_{suffix}"
    dst_t = f"id_inv_{suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        src_path = Path(tmp) / "src.db"
        con = sqlite3.connect(src_path)
        try:
            con.execute(f'CREATE TABLE "{src_t}" (id INTEGER, v INTEGER)')
            con.execute(f'INSERT INTO "{src_t}" VALUES (1, 42)')
            con.commit()
        finally:
            con.close()
        maps = [
            {
                "source": "id",
                "target": "id",
                "confidence": 0.99,
                "source_type": "INTEGER",
                "target_type": "INTEGER",
                "create_new": True,
            },
            {
                "source": "v",
                "target": "v",
                "confidence": 0.99,
                "source_type": "INTEGER",
                "target_type": invented,
                "create_new": True,
            },
        ]
        req = TransferRequest(
            source=_sqlite_ep(src_path, src_t),
            destination=_pg_ep(dst_t),
            mappings=maps,
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
        )
        try:
            result = UniversalTransferEngine().execute_tracked(
                req, uuid.uuid4().hex[:24]
            )
            assert result.success is False, f"{invented} must refuse, got success"
            err = str(result.error or "")
            assert (
                "fidelity" in err.lower()
                or "collapse" in err.lower()
                or "identity" in err.lower()
                or "serial" in err.lower()
                or "risk" in err.lower()
            ), err
            dest_n = _pg_dest_count(dst_t)
            assert dest_n == 0, f"{invented} invent still landed dest COUNT={dest_n}"
        finally:
            _pg_drop(dst_t)
