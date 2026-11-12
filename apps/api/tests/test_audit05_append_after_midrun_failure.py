"""Audit finding 5 (H3) — an append that failed mid-run must not be duplicated by the next run.

A ``full_refresh_append`` into a table that already holds rows commits chunk
by chunk. When a later chunk fails, the committed rows cannot be removed (no
key list is the batch) and they stay. Before this fix:

* the failure named three different row counts, none of them the truth
  ("partial write (4096 rows committed ...)", records_transferred 20000,
  "20000 row(s) were already written" — the table held 24,096 new rows);
* the next run of the route appended the whole source again on top of them
  and Gate-8 passed: 54,099 rows, 30,003 distinct ids.

Now the failed run measures and states what it left behind, and the next
append to that table is refused before any write until the operator settles
it: an audited accept (rebaseline) or a full-refresh overwrite.
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

ROWS = 6000
POISON = 5000
PRIOR = [(900001, 0, 1), (900002, 0, 2), (900003, 0, 3)]


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _source(tmp_path: Path) -> Path:
    src = tmp_path / "src.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v INTEGER)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?)",
        [(i, i, -1 if i == POISON else i) for i in range(1, ROWS + 1)],
    )
    conn.commit()
    conn.close()
    return src


def _fix_source(src: Path) -> None:
    conn = sqlite3.connect(src)
    conn.execute("UPDATE t SET v = ? WHERE id = ?", (POISON, POISON))
    conn.commit()
    conn.close()


def _occupied_destination(tmp_path: Path) -> Path:
    dst = tmp_path / "dst.db"
    conn = sqlite3.connect(dst)
    conn.execute("CREATE TABLE t (id INTEGER, seq INTEGER, v INTEGER CHECK (v >= 0))")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)", PRIOR)
    conn.commit()
    conn.close()
    return dst


def _state(dst: Path) -> tuple[int, int]:
    conn = sqlite3.connect(dst)
    try:
        return conn.execute("SELECT COUNT(*), COUNT(DISTINCT id) FROM t").fetchone()
    finally:
        conn.close()


def _endpoint(dst: Path) -> EndpointConfig:
    return EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t")


def _run(fake, src: Path, dst: Path, mode: str = "full_refresh_append"):
    job_id = "a05" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=_endpoint(dst),
        sync_mode=mode,
        skip_preflight=True,
        validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(request, job_id), job_id


def _failed_first_run(fake, tmp_path: Path):
    src, dst = _source(tmp_path), _occupied_destination(tmp_path)
    first, job_id = _run(fake, src, dst)
    assert first.success is False
    count, distinct = _state(dst)
    committed = count - len(PRIOR)
    # The scenario under test: some chunks committed, then the run failed.
    assert 0 < committed < ROWS and count == distinct
    return src, dst, first, job_id, committed


def test_partial_write_message_reports_run_total(tmp_path, isolated):
    _src, _dst, first, _job, committed = _failed_first_run(isolated, tmp_path)

    assert first.records_transferred == committed
    error = str(first.error)
    assert f"{committed:,} row(s)" in error and "committed" in error
    # The failing batch's own count is named as that, never as the run's.
    assert "rows committed before failure" not in error
    assert "were already written and were not removed" not in error


def test_append_retry_after_partial_failure_no_duplicates(tmp_path, isolated):
    src, dst, _first, failed_job, committed = _failed_first_run(isolated, tmp_path)
    _fix_source(src)
    before = _state(dst)

    second, _job = _run(isolated, src, dst)

    assert second.success is False
    assert second.error_details["mismatch"] == "unfinished_append"
    assert second.error_details["unfinished_job_id"] == failed_job
    assert second.error_details["rows_committed"] == committed
    assert f"{committed:,}" in str(second.error) and "second time" in str(second.error)
    # Nothing was written: no row of the failed run is in the table twice.
    assert _state(dst) == before
    count, distinct = _state(dst)
    assert count == distinct == len(PRIOR) + committed
    # ...and it stays refused, run after run, until someone settles it.
    third, _job = _run(isolated, src, dst)
    assert third.success is False and _state(dst) == before


def test_an_audited_accept_settles_the_unfinished_append(tmp_path, isolated, monkeypatch):
    import services.audit_log as audit_log

    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or kw)
    src, dst, _first, failed_job, committed = _failed_first_run(isolated, tmp_path)
    _fix_source(src)

    ac.rebaseline(_endpoint(dst), actor="ops@example.com", reason="partial rows reviewed")

    accepted = [e for e in events if e["action"] == "append_continuity.rebaselined"]
    assert len(accepted) == 1
    settled = accepted[0]["details"]["unfinished_append_settled"]
    assert settled == {"job_id": failed_job, "rows_committed": committed, "rows_before": len(PRIOR)}
    second, _job = _run(isolated, src, dst)
    assert second.success, second.error
    # The operator accepted the partial rows; the append then adds the source.
    assert _state(dst)[0] == len(PRIOR) + committed + ROWS


def test_a_full_refresh_overwrite_settles_the_unfinished_append(tmp_path, isolated):
    src, dst, _first, _job, _committed = _failed_first_run(isolated, tmp_path)
    _fix_source(src)

    rebuilt, _job = _run(isolated, src, dst, mode="full_refresh_overwrite")

    assert rebuilt.success, rebuilt.error
    assert _state(dst) == (ROWS, ROWS)
    target = ac.resolve_target(_endpoint(dst))
    assert ac.unfinished_append(target) is None
    # The next append is judged on its own, no longer refused for the old run.
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=_endpoint(dst), sync_mode="full_refresh_append",
    )
    assert ac.check_before_write(request) is None


def test_a_failed_run_that_was_rolled_back_leaves_nothing_to_settle(tmp_path, isolated):
    """An empty destination is cleared on failure: the next run is a first run."""
    src = _source(tmp_path)
    dst = tmp_path / "dst.db"
    conn = sqlite3.connect(dst)
    conn.execute("CREATE TABLE t (id INTEGER, seq INTEGER, v INTEGER CHECK (v >= 0))")
    conn.commit()
    conn.close()

    first, _job = _run(isolated, src, dst)
    assert first.success is False and _state(dst) == (0, 0)
    assert first.records_transferred == 0
    assert ac.unfinished_append(ac.resolve_target(_endpoint(dst))) is None

    _fix_source(src)
    second, _job = _run(isolated, src, dst)
    assert second.success, second.error
    assert _state(dst) == (ROWS, ROWS)


def test_unfinished_append_marker_roundtrip(tmp_path, isolated):
    dst = _occupied_destination(tmp_path)
    target = ac.resolve_target(_endpoint(dst))
    assert ac.unfinished_append(target) is None
    ac.record_unfinished_append(_endpoint(dst), job_id="job-1", rows_before=3, rows_committed=40)
    assert ac.unfinished_append(target) == {
        "job_id": "job-1", "rows_committed": 40, "rows_before": 3,
        "recorded_at": ac.unfinished_append(target)["recorded_at"],
    }
    # Nothing committed, nothing to settle.
    ac.clear_unfinished_append(target)
    ac.record_unfinished_append(_endpoint(dst), job_id="job-2", rows_before=3, rows_committed=0)
    assert ac.unfinished_append(target) is None
