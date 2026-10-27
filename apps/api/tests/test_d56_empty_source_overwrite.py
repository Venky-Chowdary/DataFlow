"""D56 — an overwrite from an empty source silently wiped the destination.

The load-volume guard compares only against this route's own history, so the
first DataFlow overwrite of a populated table from a 0-row source replaced
every row with nothing and reported success. Preflight now refuses it unless
the operator sets ``empty_source_overwrite_acknowledged``.

Real SQLite → SQLite through the engine with preflight ON.
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
from src.transfer.models import (  # noqa: E402
    EndpointConfig,
    TransferRequest,
    transfer_request_from_dict,
    transfer_request_to_dict,
)


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
    job_id = "d56" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _overwrite(src: Path, dst: Path, **kw) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
        **kw,
    )


@pytest.fixture()
def empty_source_populated_dest(tmp_path: Path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    _exec(dst, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT)")
    _exec(dst, "INSERT INTO t VALUES (?, ?)", [(i, "kept") for i in range(1, 6)])
    return src, dst


def test_empty_source_overwrite_is_refused(isolated, empty_source_populated_dest):
    src, dst = empty_source_populated_dest
    result = _run(isolated, _overwrite(src, dst))
    assert not result.success
    assert "source holds 0 rows" in str(result.error), result.error
    assert "5 row(s)" in str(result.error)
    assert _count(dst) == 5


def test_acknowledged_empty_source_overwrite_empties_on_purpose(
    isolated, empty_source_populated_dest
):
    src, dst = empty_source_populated_dest
    request = _overwrite(src, dst, empty_source_overwrite_acknowledged=True)
    # The acknowledgement survives the job-store round trip a worker replays.
    assert transfer_request_from_dict(
        transfer_request_to_dict(request)
    ).empty_source_overwrite_acknowledged
    result = _run(isolated, request)
    assert result.success, result.error
    assert _count(dst) == 0
