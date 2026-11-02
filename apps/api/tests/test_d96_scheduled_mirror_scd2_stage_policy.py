"""D96 — scheduled mirror and SCD2 pipelines always failed "Upsert needs a business key".

A keyed schedule defaults ``duplicate_policy`` to ``upsert`` on the destination
endpoint. Mirror and SCD2 first stream the source into a keyless overwrite
staging spool cloned from that endpoint, and the clone inherited the policy:
the stage write refused itself before the keyed merge into the target ever ran.
One-shot runs set no policy, so only schedules hit it.

Real SQLite → SQLite through ``build_schedule_request`` and the engine with
preflight ON, twice (run 2 merges into the table run 1 created).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.schedule_runner as runner  # noqa: E402
import services.schedule_store as store  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402


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


def _rows(path: Path, sql: str) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return list(conn.execute(sql).fetchall())
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
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    return fake


@pytest.mark.parametrize("mode", ["mirror", "scd2"])
def test_scheduled_merge_mode_runs_with_keyed_upsert_policy(tmp_path, isolated, mode):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(1, 6)])
    # The shape Pilot's create_schedule persists: key on the schedule and as a
    # list on the stream contract.
    sched = store.PipelineSchedule.from_dict({
        "id": "d96-" + mode,
        "name": "d96 " + mode,
        "source_connector_id": "src",
        "source_table": "t",
        "dest_connector_id": "dst",
        "dest_table": "t",
        "interval": "hourly",
        "sync_mode": mode,
        "primary_key": "id",
        "stream_contracts": [
            {"name": "t", "selected": True, "primary_key": ["id"], "sync_mode": mode}
        ],
        "mappings": [
            {"source": "id", "target": "id", "confidence": 0.95, "create_new": True,
             "source_type": "BIGINT", "target_type": "INTEGER", "transform": "none"},
            {"source": "name", "target": "name", "confidence": 0.95, "create_new": True,
             "source_type": "VARCHAR", "target_type": "TEXT", "transform": "none"},
        ],
    })
    src_conn = {"type": "sqlite", "database": str(src)}
    dst_conn = {"type": "sqlite", "database": str(dst)}

    for run in range(2):
        if run == 1:
            _exec(src, "UPDATE t SET name = 'changed' WHERE id = 2")
        request = runner.build_schedule_request(sched, src_conn, dst_conn)
        assert (request.destination.extra or {}).get("duplicate_policy") == "upsert"
        job_id = "d96" + uuid.uuid4().hex[:16]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        result = UniversalTransferEngine().execute_tracked(request, job_id)
        assert "business key" not in str(result.error or ""), result.error
        assert result.success, result.error

    names = {r[0]: r[1] for r in _rows(dst, "SELECT id, name FROM t")}
    assert set(names) == {1, 2, 3, 4, 5}
    if mode == "mirror":
        assert names[2] == "changed"
    else:
        # SCD2 keeps the old version and adds the current one.
        current = _rows(
            dst, "SELECT name FROM t WHERE id = 2 ORDER BY valid_from"
        )
        assert [r[0] for r in current] == ["n2", "changed"]
