"""D204 AC-1 — after rebaseline_transfer, the next append completes against the new baseline.

tests/test_d204_rebaseline_keys_like_job.py proves the rebaseline lands under
the key a job reads; the story's follow-up run was never exercised. Here the
whole chain runs on the engine: run 1, a hand-added destination row refuses
run 2, the audited rebaseline accepts the destination, and run 3 appends its
new row and is green. A change after the rebaseline is refused again.
Already correct on a4e7d104d; this locks it on SQLite and PostgreSQL.
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
from services import append_continuity as ac  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

pytestmark = pytest.mark.timeout(600)


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _ts(i: int) -> str:
    return f"2026-11-01 10:{i:02d}:00"


def _chain(store, *, source, dest, src_exec, dst_exec, dest_count) -> None:
    def run():
        job_id = "d204" + uuid.uuid4().hex[:12]
        store.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(
            TransferRequest(
                source=source, destination=dest, sync_mode="incremental_append",
                validation_mode="strict",
                stream_contracts=[{
                    "name": source.table, "cursor_field": "updated_at",
                    "cursor_semantics": "modification_timestamp",
                    "sync_mode": "incremental_append", "selected": True,
                }],
            ),
            job_id,
        )

    for i in range(1, 11):
        src_exec(f"INSERT INTO {source.table} VALUES (%s, %s, %s)", (i, f"n{i}", _ts(i)))
    assert run().success
    dst_exec(f"INSERT INTO {dest.table} (id, name, updated_at) VALUES (99, 'by hand', %s)", (_ts(0),))
    src_exec(f"INSERT INTO {source.table} VALUES (%s, %s, %s)", (11, "n11", _ts(11)))
    refused = run()
    assert not refused.success and "changed since the last verified run" in str(refused.error)
    assert dest_count() == 11

    accepted = ac.rebaseline(dest, actor="qa", reason="the hand-added row is wanted")
    assert accepted["current"]["rows"] == 11

    follow_up = run()
    assert follow_up.success, follow_up.error
    assert follow_up.records_transferred == 1
    assert dest_count() == 12

    dst_exec(f"DELETE FROM {dest.table} WHERE id = 99")
    src_exec(f"INSERT INTO {source.table} VALUES (%s, %s, %s)", (12, "n12", _ts(12)))
    assert not run().success
    assert dest_count() == 11


def test_sqlite_rebaseline_then_append_completes(tmp_path, store):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"

    def _exec(path: Path):
        def run(sql: str, params: tuple = ()) -> None:
            conn = sqlite3.connect(path)
            conn.execute(sql.replace("%s", "?"), params)
            conn.commit()
            conn.close()

        return run

    _exec(src)("CREATE TABLE q3_late (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT NOT NULL)")

    def count() -> int:
        conn = sqlite3.connect(dst)
        try:
            return int(conn.execute("SELECT count(*) FROM q3_late").fetchone()[0])
        finally:
            conn.close()

    _chain(
        store,
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="q3_late"),
        dest=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="q3_late"),
        src_exec=_exec(src), dst_exec=_exec(dst), dest_count=count,
    )


def test_pg_to_pg_rebaseline_then_append_completes(store):
    from tests.c3_live_helpers import pg_endpoint, pg_sql
    from tests.helpers.live_env import pg_up

    if not pg_up():
        pytest.skip("live PostgreSQL not available")
    src, dst = f"d204s_{uuid.uuid4().hex[:8]}", f"d204d_{uuid.uuid4().hex[:8]}"
    pg_sql(f"CREATE TABLE {src} (id INT PRIMARY KEY, name TEXT, updated_at TIMESTAMP NOT NULL)")
    try:
        _chain(
            store,
            source=pg_endpoint(src), dest=pg_endpoint(dst),
            src_exec=pg_sql, dst_exec=pg_sql,
            dest_count=lambda: int(pg_sql(f"SELECT count(*) FROM {dst}", fetch=True)[0][0]),
        )
    finally:
        pg_sql(f"DROP TABLE IF EXISTS {src}")
        pg_sql(f"DROP TABLE IF EXISTS {dst}")
