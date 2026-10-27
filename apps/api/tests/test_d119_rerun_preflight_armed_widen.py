"""D119 / D131 — every preflight that arms the widen block crashed.

``services/preflight_service.py`` called ``ci_get`` in the D107 "armed widen"
block without importing it. Any run with ``backfill_new_fields``,
``schema_drift_acknowledged`` or a propagate schema policy — including every
scheduled run 2+ onto a table run 1 created (the stored create_new mapping
arms backfill) — failed preflight with ``name 'ci_get' is not defined``.

Real SQLite → SQLite through the engine with preflight ON: run 1 creates the
destination, run 2 re-runs into the now-existing table with the widen block
armed and must pass preflight and land the delta.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            if rows is None:
                conn.execute(sql)
            else:
                conn.executemany(sql, rows)
    finally:
        conn.close()


def _count(path: Path) -> int:
    conn = sqlite3.connect(str(path))
    try:
        return int(conn.execute("SELECT COUNT(*) FROM t").fetchone()[0])
    finally:
        conn.close()


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _run(fake, request: TransferRequest):
    job_id = "d119" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _request(src: Path, dst: Path, **overrides) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "id",
                "cursor_semantics": "monotonic_sequence",
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        validation_mode="strict",
        **overrides,
    )


@pytest.mark.parametrize(
    "armed",
    [
        {"backfill_new_fields": True},
        {"schema_drift_acknowledged": True},
        {"schema_policy": "propagate_columns"},
    ],
    ids=["backfill", "drift_ack", "propagate"],
)
def test_second_run_into_created_table_passes_armed_preflight(tmp_path, isolated, armed):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, amount REAL)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, f"n{i}", i * 1.5) for i in range(1, 11)])

    first = _run(isolated, _request(src, dst))
    assert first.success, first.error
    assert _count(dst) == 10

    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, f"n{i}", i * 1.5) for i in range(11, 16)])
    second = _run(isolated, _request(src, dst, **armed))
    assert "ci_get" not in str(second.error or ""), second.error
    assert second.success, second.error
    assert _count(dst) == 15
