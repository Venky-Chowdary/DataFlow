"""Redshift PRODUCTION_SKU gate — execute + dest COUNT on a named fixture.

PG-wire local stand-in (docker-compose Postgres or host :5432), not an AWS
cluster. Databricks stays Planned. Do not add Redshift to PRODUCTION_SKU
until this file measures dest COUNT.
"""

from __future__ import annotations

import socket
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.dest_precount import destination_row_count
from src.transfer.connector_capabilities import get_capabilities, transfer_ready
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from src.transfer.registry import PRODUCTION_SKU

PG_HOST = "127.0.0.1"
PG_PORT = 5432


def _pg_up() -> bool:
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=1.5):
            return True
    except OSError:
        return False


def test_redshift_sku_requires_execute_dest_count() -> None:
    """Redshift may join PRODUCTION_SKU only after this named fixture is green."""
    in_sku = ("database", "postgresql", "database", "redshift") in PRODUCTION_SKU or (
        "database",
        "sqlite",
        "database",
        "redshift",
    ) in PRODUCTION_SKU
    ready = transfer_ready(get_capabilities("redshift", "redshift"))
    if not in_sku:
        assert ready is False
        pytest.skip(
            "Redshift stays Planned until execute + dest COUNT on this named fixture"
        )
    assert ready is True


def test_sqlite_to_redshift_execute_dest_count(tmp_path: Path) -> None:
    if not transfer_ready(get_capabilities("redshift", "redshift")):
        pytest.skip("Redshift not certified — dest COUNT unmeasured")
    if not _pg_up():
        pytest.skip("Postgres :5432 unreachable — Redshift PG-wire dest COUNT unmeasured")

    src_t = "rs_src_" + uuid.uuid4().hex[:8]
    dst_t = "rs_dst_" + uuid.uuid4().hex[:8]
    src_path = tmp_path / "rs_src.db"
    con = sqlite3.connect(src_path)
    try:
        con.execute(f'CREATE TABLE "{src_t}" (id INTEGER, amount TEXT)')
        con.executemany(f'INSERT INTO "{src_t}" VALUES (?, ?)', [(1, "10.00"), (2, "20.50")])
        con.commit()
    finally:
        con.close()

    maps = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "amount", "target": "amount", "confidence": 0.99},
    ]
    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=str(src_path),
            table=src_t,
            connection_string=f"sqlite:///{src_path}",
            ssl=False,
        ),
        destination=EndpointConfig(
            kind="database",
            format="redshift",
            host=PG_HOST,
            port=PG_PORT,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            table=dst_t,
            ssl=False,
        ),
        mappings=maps,
        sync_mode="full_refresh_overwrite",
        skip_preflight=False,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(req, uuid.uuid4().hex[:24])
    try:
        assert result.success, result.error
        dest_n = destination_row_count(
            "redshift",
            {
                "host": PG_HOST,
                "port": PG_PORT,
                "database": "dataflow",
                "username": "dataflow",
                "password": "dataflow",
            },
            schema="public",
            table_name=dst_t,
        )
        assert dest_n == 2, f"Redshift dest COUNT={dest_n} (writer ack is not dest proof)"
        assert result.records_transferred == 2
    finally:
        try:
            import psycopg2

            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname="dataflow",
                user="dataflow",
                password="dataflow",
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'DROP TABLE IF EXISTS public."{dst_t}"')
            conn.close()
        except Exception:
            pass
