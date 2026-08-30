"""Live leftover writes — SQLite always; PG/MySQL/Mongo when reachable.

Named fixture. Not customer-tenant warehouse SKU. CDC at-least-once.
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

from services.unique_engine_leftovers import leftover_column_mappings
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

_HIGH = "123456789012345678.123456"


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


def _seed_sqlite(path: Path, table: str) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(f'CREATE TABLE "{table}" (id INTEGER, amount TEXT)')
        con.execute(f'INSERT INTO "{table}" VALUES (1, ?)', (_HIGH,))
        con.commit()
    finally:
        con.close()


def test_live_sqlite_to_sqlite_amount_stays_text() -> None:
    suffix = uuid.uuid4().hex[:8]
    src_t = f"leftover_src_{suffix}"
    dst_t = f"leftover_dst_{suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        src_path = Path(tmp) / "src.db"
        dst_path = Path(tmp) / "dst.db"
        _seed_sqlite(src_path, src_t)
        maps = leftover_column_mappings(
            source_format="sqlite",
            dest_format="sqlite",
            source_columns=["id", "amount"],
        )
        req = TransferRequest(
            source=_sqlite_ep(src_path, src_t),
            destination=_sqlite_ep(dst_path, dst_t),
            mappings=maps,
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
        )
        result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        assert result.success, result.error
        con = sqlite3.connect(dst_path)
        try:
            kind, stored = con.execute(
                f'SELECT typeof(amount), amount FROM "{dst_t}"'
            ).fetchone()
            dest_n = con.execute(f'SELECT COUNT(*) FROM "{dst_t}"').fetchone()[0]
        finally:
            con.close()
        assert kind == "text"
        assert stored == _HIGH
        assert dest_n == 1


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="Postgres not reachable")
def test_live_sqlite_text_to_postgres_numeric_is_refused() -> None:
    """Validate owns fit. Binding sqlite TEXT digits to invented NUMERIC is
    refused — dest COUNT stays 0. The 100% live write is sqlite TEXT dest.
    """
    import psycopg2

    suffix = uuid.uuid4().hex[:8]
    src_t = f"leftover_src_{suffix}"
    dst_t = f"leftover_dst_{suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        src_path = Path(tmp) / "src.db"
        _seed_sqlite(src_path, src_t)
        dst = EndpointConfig(
            kind="database",
            format="postgresql",
            host="localhost",
            port=5432,
            database="dataflow",
            schema="public",
            username="dataflow",
            password="dataflow",
            table=dst_t,
            connection_string="",
            ssl=False,
        )
        maps = leftover_column_mappings(
            source_format="sqlite",
            dest_format="postgresql",
            source_columns=["id", "amount"],
        )
        req = TransferRequest(
            source=_sqlite_ep(src_path, src_t),
            destination=dst,
            mappings=maps,
            sync_mode="full_refresh_overwrite",
            skip_preflight=False,
            validation_mode="strict",
        )
        result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
        try:
            assert result.success is False
            err = str(result.error or "")
            assert "fidelity" in err.lower() or "collapse" in err.lower()
            conn = psycopg2.connect(
                host="localhost", port=5432, database="dataflow",
                user="dataflow", password="dataflow",
            )
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT COUNT(*) FROM information_schema.tables "
                        "WHERE table_schema = 'public' AND table_name = %s",
                        (dst_t,),
                    )
                    exists = int(cur.fetchone()[0])
                    if exists:
                        cur.execute(f'SELECT COUNT(*) FROM public."{dst_t}"')
                        dest_n = int(cur.fetchone()[0])
                    else:
                        dest_n = 0
            finally:
                conn.close()
            assert dest_n == 0, f"refused write still landed dest COUNT={dest_n}"
        finally:
            conn = psycopg2.connect(
                host="localhost", port=5432, database="dataflow",
                user="dataflow", password="dataflow",
            )
            conn.autocommit = True
            try:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS public."{dst_t}"')
            finally:
                conn.close()
