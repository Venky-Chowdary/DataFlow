"""Live leftover writes — SQLite always; PG when reachable.

Named fixture. Not customer-tenant warehouse SKU. CDC at-least-once.
``100%`` on this file is sqlite TEXT dest COUNT=1 and sqlite TEXT →
postgresql TEXT dest COUNT=1. Invented NUMERIC / DECIMAL dest COUNT=0
is refuse proof, not a transfer.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import tempfile
import uuid
from pathlib import Path
from typing import Any

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

import pytest

from services.unique_engine_leftovers import leftover_column_mappings
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

_HIGH = "123456789012345678.123456"

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


def _seed_sqlite(path: Path, table: str) -> None:
    con = sqlite3.connect(path)
    try:
        con.execute(f'CREATE TABLE "{table}" (id INTEGER, amount TEXT)')
        con.execute(f'INSERT INTO "{table}" VALUES (1, ?)', (_HIGH,))
        con.commit()
    finally:
        con.close()


def _invent_amount_maps(target_type: str) -> list[dict[str, Any]]:
    """Explicit invent stamps for refuse fixtures. Not the honest helper."""
    return [
        {
            "source": "id",
            "target": "id",
            "confidence": 0.99,
            "source_type": "INTEGER",
            "target_type": "BIGINT",
        },
        {
            "source": "amount",
            "target": "amount",
            "confidence": 0.99,
            "source_type": "TEXT",
            "target_type": target_type,
        },
    ]


def _pg_dest_state(table: str) -> tuple[int, str | None, str | None]:
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
            exists = int(cur.fetchone()[0])
            if not exists:
                return 0, None, None
            cur.execute(f'SELECT COUNT(*) FROM public."{table}"')
            dest_n = int(cur.fetchone()[0])
            if dest_n == 0:
                return dest_n, None, None
            cur.execute(
                f'SELECT pg_typeof(amount)::text, amount::text FROM public."{table}" LIMIT 1'
            )
            kind, stored = cur.fetchone()
            return dest_n, str(kind), str(stored)
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
def test_live_sqlite_text_to_postgres_text_dest_count() -> None:
    """Honest leftover write: sqlite TEXT carrier → postgresql TEXT. Dest COUNT=1."""
    suffix = uuid.uuid4().hex[:8]
    src_t = f"leftover_src_{suffix}"
    dst_t = f"leftover_pg_txt_{suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        src_path = Path(tmp) / "src.db"
        _seed_sqlite(src_path, src_t)
        maps = leftover_column_mappings(
            source_format="sqlite",
            dest_format="postgresql",
            source_columns=["id", "amount"],
        )
        amount = next(m for m in maps if m.get("source") == "amount")
        assert str(amount.get("target_type") or "").upper() == "TEXT"
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
            assert result.success, result.error
            dest_n, kind, stored = _pg_dest_state(dst_t)
            assert dest_n == 1, f"dest COUNT={dest_n} (writer ack is not dest proof)"
            assert kind == "text", f"pg_typeof(amount)={kind}"
            assert stored == _HIGH
        finally:
            _pg_drop(dst_t)


@pytest.mark.skipif(not _reachable("127.0.0.1", 5432), reason="Postgres not reachable")
@pytest.mark.parametrize(
    "invented",
    ("NUMERIC", "DECIMAL(18,2)", "NUMERIC(18,2)"),
)
def test_live_sqlite_text_to_postgres_invented_numeric_is_refused(invented: str) -> None:
    """Validate owns fit. Invented NUMERIC/DECIMAL from sqlite TEXT digits is
    refused — dest COUNT stays 0. The live write is TEXT dest.
    """
    suffix = uuid.uuid4().hex[:8]
    src_t = f"leftover_src_{suffix}"
    dst_t = f"leftover_pg_inv_{suffix}"
    with tempfile.TemporaryDirectory() as tmp:
        src_path = Path(tmp) / "src.db"
        _seed_sqlite(src_path, src_t)
        maps = _invent_amount_maps(invented)
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
            assert "fidelity" in err.lower() or "collapse" in err.lower(), err
            dest_n, _kind, _stored = _pg_dest_state(dst_t)
            assert dest_n == 0, f"{invented} refused write still landed dest COUNT={dest_n}"
        finally:
            _pg_drop(dst_t)
