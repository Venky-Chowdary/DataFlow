"""M6 — SQLite has no temporal storage class: DataFlow-created tables keep ISO text.

Decision, pinned here: a DataFlow-created SQLite table declares TEXT for
TIMESTAMP / DATE columns and stores canonical ISO-8601 text. Declaring
``TIMESTAMP`` or ``DATETIME`` instead would give the column NUMERIC affinity,
which silently turns numeric-looking text ('2025') into an integer — data
corruption the TEXT carrier cannot suffer. The carrier is only acceptable if
reads and verification agree with it, so this proves the round trip
PostgreSQL → SQLite → PostgreSQL: Gate-8 verifies both legs, the values are
ISO text in SQLite, and the second leg recovers real temporal columns.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from datetime import date, datetime
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.mongodb_service import MemoryMongoDBService  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_up  # noqa: E402
from tests.test_d124_watermark_tie_group_e2e import _Postgres  # noqa: E402

pytestmark = pytest.mark.skipif(not pg_up(), reason="PostgreSQL not available")


def test_timestamp_round_trips_through_a_text_sqlite_carrier(tmp_path: Path, monkeypatch):
    store = MemoryMongoDBService()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    # "verified" needs the verify-time population digest, which is opt-in.
    monkeypatch.setenv("DATAFLOW_ENGINE_DIGEST", "1")
    pg = _Postgres()
    suffix = uuid.uuid4().hex[:6]
    first, last = f"m6a_{suffix}", f"m6b_{suffix}"
    mid = tmp_path / "mid.db"
    pg.execute(
        f'CREATE TABLE "{first}" (id INTEGER PRIMARY KEY, created_at TIMESTAMP NOT NULL, d DATE)'
    )
    pg.execute(
        f"INSERT INTO \"{first}\" VALUES (1, '2025-01-01 10:00:00', '2025-01-01'), "
        "(2, '2025-06-30 23:59:59.5', '2025-06-30')"
    )

    def run(request):
        job_id = uuid.uuid4().hex[:24]
        store.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert result.success, result.error
        assert store.get_job(job_id)["reconciliation"]["verification_verdict"] == "verified"

    try:
        run(
            TransferRequest(
                source=pg.endpoint(first),
                destination=EndpointConfig(kind="database", format="sqlite", database=str(mid), table="m"),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
            )
        )
        with sqlite3.connect(mid) as conn:
            ddl = conn.execute("SELECT sql FROM sqlite_master WHERE name='m'").fetchone()[0]
            rows = conn.execute("SELECT created_at, d FROM m ORDER BY id").fetchall()
        assert '"created_at" TEXT NOT NULL' in ddl and '"d" TEXT' in ddl, ddl
        assert rows == [
            ("2025-01-01T10:00:00", "2025-01-01"),
            ("2025-06-30T23:59:59.500000", "2025-06-30"),
        ]

        run(
            TransferRequest(
                source=EndpointConfig(kind="database", format="sqlite", database=str(mid), table="m"),
                destination=pg.endpoint(last),
                sync_mode="full_refresh_overwrite",
                validation_mode="strict",
            )
        )
        conn = pg._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name, data_type FROM information_schema.columns "
                    "WHERE table_name = %s ORDER BY ordinal_position",
                    (last,),
                )
                types = dict(cur.fetchall())
                cur.execute(f'SELECT created_at, d FROM public."{last}" ORDER BY id')
                back = cur.fetchall()
        finally:
            conn.close()
        assert types["created_at"] == "timestamp without time zone" and types["d"] == "date"
        assert back == [
            (datetime(2025, 1, 1, 10, 0, 0), date(2025, 1, 1)),
            (datetime(2025, 6, 30, 23, 59, 59, 500000), date(2025, 6, 30)),
        ]
    finally:
        pg.drop(first)
        pg.drop(last)
