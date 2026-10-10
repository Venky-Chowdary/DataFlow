"""QA MXD09 — jobs into a SQLite destination sat at 98% "Writing batch 1/1" forever.

Root cause (reproduced, not a destination lock): the Gate-8 independent re-read
of a query-mode / procedure source never advanced. ``reread_pagination_plan``
picks a held snapshot scan (``use_offset=False``, offset always 0) for SQL
sources, but a callable source pages its result spool *by offset* and ignores
``scan_state``. Every re-read returned page one again and the digest loop span
at 100% CPU after all rows were committed. Concurrent jobs only made the GIL
contention visible; one job of >1 page hangs alone.
"""

from __future__ import annotations

import os
import sqlite3
import uuid

import pytest

from services.source_reread import reread_pagination_plan
from src.transfer.models import EndpointConfig, TransferRequest

_ROWS = 20_000


@pytest.mark.parametrize("src", ["sqlite", "postgresql", "mysql", "snowflake"])
def test_callable_source_rereads_by_offset(src):
    plan = reread_pagination_plan(src_type=src, incremental=False, callable_source=True)
    assert plan["use_offset"] is True
    assert plan["scan_state"] is None


def test_table_source_keeps_held_scan():
    plan = reread_pagination_plan(src_type="sqlite", incremental=False)
    assert plan["use_offset"] is False and plan["scan_state"] == {}


def test_query_mode_sqlite_job_finishes_and_verifies(tmp_path, monkeypatch):
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    import src.transfer.engine as engine_mod
    import src.transfer.stream as stream_mod
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Mongo(_FakeMongo):
        def update_job_fields(self, *_a, **_k):
            return None

    fake = _Mongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)

    reads = {"n": 0}
    real_read = stream_mod._read_batch_impl

    def _bounded_read(*args, **kwargs):
        reads["n"] += 1
        if reads["n"] > 40:
            raise RuntimeError("re-read never advanced past the first page (MXD09 hang)")
        return real_read(*args, **kwargs)

    monkeypatch.setattr(stream_mod, "_read_batch_impl", _bounded_read)

    src = str(tmp_path / "src.db")
    dst = str(tmp_path / "dest.db")
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, v TEXT, amt REAL)")
        conn.executemany(
            "INSERT INTO orders VALUES (?, ?, ?)",
            [(i, f"v{i}", i * 1.5) for i in range(_ROWS)],
        )
    sqlite3.connect(dst).close()
    req = TransferRequest(
        source=EndpointConfig(
            kind="database",
            format="sqlite",
            database=src,
            table="orders",
            extra={"source_read_mode": "query", "source_query": "SELECT * FROM orders"},
        ),
        destination=EndpointConfig(kind="database", format="sqlite", database=dst, table="orders"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
    )
    job_id = "mxd09" + uuid.uuid4().hex[:10]
    fake.update_job_status(job_id, "pending", transfer_request={})
    result = UniversalTransferEngine().execute_tracked(req, job_id)

    assert result.success, result.error
    assert result.records_transferred == _ROWS
    with sqlite3.connect(dst) as conn:
        assert conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0] == _ROWS
    assert os.path.exists(dst)
