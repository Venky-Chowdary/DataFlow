"""QA ACC-02 — second incremental run broke on a composite watermark.

``incremental_append`` with no contract primary key: run 1's row path takes the
tie-break from the source catalog PK (a cursor with ties must not skip peers)
and stores ``cursor␟id``. Run 2's read scope and COPY fast path saw only the
contract PK (none) and decoded that composite single-column:
"composite watermark requires the tie-break column it was written with".

Real SQLite → SQLite runs through the engine, three passes, exact counts.

Measured on the pre-fix engine with this fixture: run 2 reported success and
Gate-8 passed, yet landed 43 of 45 rows — the two rows tying the stored cursor
value were skipped silently (COPY seeked ``seq > 9`` instead of
``(seq, id) > (9, 30)``). Run 3, a quiet poll, then failed Gate-8 because the
verification ladder replaced the measured 0 with a whole-table re-read.
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


def _rows(path: Path) -> list[tuple]:
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute("SELECT id, seq FROM t ORDER BY id").fetchall()
    finally:
        conn.close()


def _request(src: Path, dst: Path) -> TransferRequest:
    return TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="incremental_append",
        stream_contracts=[
            # No primary_key: incremental_append does not require one.
            {"name": "t", "cursor_field": "seq", "sync_mode": "incremental_append", "selected": True}
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


def _run(fake, src: Path, dst: Path):
    job_id = "acc02" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(_request(src, dst), job_id)


def test_acc02_three_incremental_runs_on_a_tied_cursor(tmp_path: Path, isolated):
    from services import sync_cursor as sc

    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)")
    # Ties on the cursor: several rows share each seq value.
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, i // 3, f"v{i}") for i in range(1, 31)])

    first = _run(isolated, src, dst)
    assert first.success, first.error
    assert len(_rows(dst)) == 30

    stored = sc.get_watermark_record(sc.list_cursor_keys()[0])
    watermark, meta = stored
    assert sc.watermark_is_composite(watermark), watermark
    assert meta.get("tiebreak_column") == "id", meta

    # New rows, including ones that tie with the last committed cursor value.
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(i, i // 3, f"v{i}") for i in range(31, 46)])
    second = _run(isolated, src, dst)
    assert second.success, second.error  # ACC-02: raised on the composite here
    rows = _rows(dst)
    assert len(rows) == 45
    assert len({r[0] for r in rows}) == 45, "duplicate or missing ids after run 2"

    third = _run(isolated, src, dst)  # quiet poll
    assert third.success, third.error
    assert len(_rows(dst)) == 45


def test_scope_uses_the_stored_tiebreak_for_a_composite_watermark(tmp_path, isolated):
    from services import sync_cursor as sc

    key = "sqlite:db:t→sqlite:db:t:t"
    sc.set_watermark(key, "5␟12".replace("␟", sc.KEYSET_SEP), metadata={
        "cursor_column": "seq", "tiebreak_column": "id",
    })
    scope = sc.IncrementalReadScope(
        cursor_column="seq", primary_key="", watermark="5" + sc.KEYSET_SEP + "12",
        cursor_key=key, watermark_cursor_column="seq", watermark_tiebreak_column="id",
    )
    assert sc.reconcile_cursor_tiebreak(scope, "") == "id"
    # A cursor-only watermark never borrows a stored column.
    cursor_only = sc.IncrementalReadScope(
        cursor_column="seq", watermark="5", watermark_tiebreak_column="id"
    )
    assert sc.reconcile_cursor_tiebreak(cursor_only, "") == ""
