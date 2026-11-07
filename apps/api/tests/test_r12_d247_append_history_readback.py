"""D247: incremental_append run 2 read back the old history row of an updated key.

Append keeps every version of a key. Gate-8's keyed read-back asked for
``LIMIT <batch size>`` rows, so a one-row batch for id 4 read one row for id 4 —
the run-1 version — and failed the run on data that had landed exactly
(QAENV Oracle -> Timescale job 6ae91b02: 31 rows, both versions exact, FAILED).

Real SQLite -> SQLite runs through the engine.
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from services.reconciliation import sample_compare_rows  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "updated_at",
                "primary_key": ["id"],
                "sync_mode": "incremental_append",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def test_second_append_run_verifies_the_new_version_of_an_updated_key(tmp_path, isolated):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, updated_at TEXT)")
    _exec(
        src,
        "INSERT INTO t VALUES (?, ?, ?)",
        [(i, f"name_{i}", f"2026-10-01 00:{i:02d}:00.654321") for i in range(1, 31)],
    )
    # The run-1 Timescale table had no key constraint: append keeps history.
    _exec(dst, "CREATE TABLE t (id INTEGER, name TEXT, updated_at TEXT)")
    engine = UniversalTransferEngine()
    first = engine.execute_tracked(_request(src, dst), "d247run1")
    assert first.success, first.error

    _exec(src, "UPDATE t SET name='Ωmega 漢字 upd-1830', updated_at='2026-11-02 23:30:00.654321' WHERE id=4")
    second = engine.execute_tracked(_request(src, dst), "d247run2")
    assert second.success, second.error

    conn = sqlite3.connect(str(dst))
    try:
        assert conn.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 31
        assert conn.execute("SELECT name FROM t WHERE id=4 ORDER BY updated_at").fetchall() == [
            ("name_4",),
            ("Ωmega 漢字 upd-1830",),
        ]
    finally:
        conn.close()


MAPPINGS = [{"source": c, "target": c} for c in ("id", "name", "updated_at")]
NEW = {"id": 4, "name": "upd", "updated_at": "2026-11-02 23:30:00"}
OLD = {"id": 4, "name": "name_4", "updated_at": "2026-10-01 00:04:00"}


@pytest.mark.parametrize("order", [(OLD, NEW), (NEW, OLD)])
def test_history_read_back_aligns_to_the_version_this_batch_wrote(order):
    report = sample_compare_rows([NEW], list(order), MAPPINGS, sort_key="id", history_keys=True)
    assert report["passed"] and report["rows_compared"] == 1, report


def test_history_read_back_still_fails_a_corrupted_append_row():
    bad = {**NEW, "name": "corrupted"}
    report = sample_compare_rows([NEW], [OLD, bad], MAPPINGS, sort_key="id", history_keys=True)
    assert not report["passed"], report


def test_duplicate_keys_without_history_still_decline_alignment():
    report = sample_compare_rows([NEW], [OLD, NEW], MAPPINGS, sort_key="id")
    assert report.get("alignment") == "declined", report
