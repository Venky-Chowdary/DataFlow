"""D81 / D2 — the strict load-history null-rate gate must measure SQL NULL only.

D81: a strict rerun of an UNCHANGED table was blocked with "null-rate shifted
10% -> 33%". ``profile_column`` counted ``''`` and the literal strings
``'null'`` / ``'none'`` as NULL, while the persisted baseline profiled the
PII-redacted rows, where ``mask('null')`` is ``'****'`` (a value) and
``mask(SQL NULL wire)`` was a masked sentinel string (a value). The two sides
of the compare classified the same cells differently.

D2: a small earlier load (a 5-row change batch) became a full vote in the
baseline median, so the first honest full load after it was blocked.

Real SQLite -> SQLite through the engine with preflight ON and strict mode.
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


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.data_quality_history as dqh
    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    # History must live in this test only — never the shared live Mongo.
    monkeypatch.setattr(dqh, "_profile_collection", lambda: None)
    monkeypatch.setenv("DATAFLOW_QUALITY_PROFILES_DIR", str(tmp_path / "profiles"))
    monkeypatch.delenv("DATAWRAP_QUALITY_PROFILES_DIR", raising=False)
    return fake


def _run(fake, src: Path, dst: Path):
    job_id = "d81" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _city(i: int):
    # 30 rows: 3 SQL NULL (10%), plus 3 '' and 4 literal 'null'/'none' text
    # values that are real data, not absence.
    if i % 10 == 0:
        return None
    if i % 10 == 1:
        return ""
    if i % 10 in (2, 3):
        return "null" if i % 2 else "none"
    return f"city{i}"


def test_unchanged_rerun_with_empty_and_null_text_passes_strict(tmp_path, isolated):
    """D81: '' and 'null'/'none' text are values; an unchanged rerun passes."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, city TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [(i, _city(i)) for i in range(30)])

    first = _run(isolated, src, dst)
    assert first.success, first.error
    second = _run(isolated, src, dst)
    assert "null-rate shifted" not in str(second.error or ""), second.error
    assert second.success, second.error


def test_small_prior_load_does_not_poison_null_rate_baseline(tmp_path, isolated):
    """D2: a 5-row prior load (40% NULL) must not block a 40-row 0% load."""
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, note TEXT)")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [(1, None), (2, None), (3, "a"), (4, "b"), (5, "c")])
    small = _run(isolated, src, dst)
    assert small.success, small.error

    _exec(src, "UPDATE t SET note = 'x' WHERE note IS NULL")
    _exec(src, "INSERT INTO t VALUES (?, ?)", [(i, f"n{i}") for i in range(6, 41)])
    full = _run(isolated, src, dst)
    assert "null-rate shifted" not in str(full.error or ""), full.error
    assert full.success, full.error
