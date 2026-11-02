"""D134 — resume after a cancel: verify the job's population, keep the committed prefix.

A cancelled incremental run keeps the rows it committed (D102) and advances
the route watermark with every batch. ``resume_job`` then continued from the
checkpoint and wrote every remaining row correctly, but:

* Gate-8 digested only the resumed tail on the source side against a
  destination holding the whole job's rows → "Checksum mismatch", and the
  undo, trusting the job's first-pass "before" of 0, dropped the whole table
  (rows committed before the cancel included) saying the destination "had no
  such table before this run";
* on a populated PostgreSQL destination the cancel itself restored the D111
  pre-run backup, silently removing the committed prefix the job reported as
  kept — the resume then skipped those rows and still verified;
* the keyed census only counted the resumed pass's new keys.

Real engine runs with preflight ON; the cancel lands after a few committed
batches; the resume is ``execute_tracked(..., resume=True)`` on the same job.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import services.cancel_check as cancel_check  # noqa: E402
import src.transfer.engine as engine_mod  # noqa: E402
import src.transfer.stream as stream_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import TransferRequest  # noqa: E402
from tests.helpers.live_env import pg_up  # noqa: E402
from tests.test_d124_watermark_tie_group_e2e import _backend  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    # Small pages so the cancel lands between committed batches.
    monkeypatch.setattr(stream_mod, "CHUNK_SIZE", 5)
    return fake


def _request(src, dst, st: str, dt: str, mode: str) -> TransferRequest:
    return TransferRequest(
        source=src.endpoint(st),
        destination=dst.endpoint(dt),
        sync_mode=mode,
        stream_contracts=[
            {
                "name": st,
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": ["id"],
                "sync_mode": mode,
                "selected": True,
            }
        ],
        validation_mode="strict",
        # Pass-through filter: the row path (COPY has no mid-run cancel point).
        source_filter={"column": "id", "op": "gt", "value": 0},
    )


def _insert(src, table: str, ids: range) -> None:
    src.execute(
        f'INSERT INTO "{table}" VALUES ' + ", ".join(f"({i}, {i}, 'v{i}')" for i in ids)
    )


def _cancel_then_resume(fake, monkeypatch, request):
    job_id = "d134" + uuid.uuid4().hex[:16]
    fake.update_job_status(job_id, "pending", transfer_request={})
    probes = {"n": 0}

    def cancel_after_a_few_batches(_mongo, _job_id):
        probes["n"] += 1
        return probes["n"] >= 4

    with monkeypatch.context() as m:
        m.setattr(cancel_check, "job_cancel_requested", cancel_after_a_few_batches)
        cancelled = UniversalTransferEngine().execute_tracked(request, job_id)
    assert not cancelled.success and "cancel" in str(cancelled.error).lower()
    fake.update_job_status(job_id, "pending", message="resume", allow_terminal_exit=True)
    return UniversalTransferEngine().execute_tracked(request, job_id, resume=True)


_ROUTES = {"sqlite": None, "pg": pg_up}


@pytest.mark.parametrize("mode", ["incremental_deduped", "incremental_append"])
@pytest.mark.parametrize("kind", sorted(_ROUTES))
@pytest.mark.parametrize("populated", [False, True], ids=["first_load", "populated"])
def test_resume_after_cancel_verifies_the_whole_job(
    tmp_path, isolated, monkeypatch, kind, mode, populated
):
    probe = _ROUTES[kind]
    if probe is not None and not probe():
        pytest.skip(f"{kind}: live database not available")
    src, dst = _backend(kind, tmp_path, "src"), _backend(kind, tmp_path, "dst")
    suffix = uuid.uuid4().hex[:8]
    st, dt = f"d134s_{suffix}", f"d134d_{suffix}"
    src.execute(f'CREATE TABLE "{st}" (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v TEXT)')
    try:
        _insert(src, st, range(1, 31))
        request = _request(src, dst, st, dt, mode)
        expected = 30
        if populated:
            job_id = "d134p" + uuid.uuid4().hex[:16]
            isolated.update_job_status(job_id, "pending", transfer_request={})
            first = UniversalTransferEngine().execute_tracked(request, job_id)
            assert first.success, first.error
            _insert(src, st, range(31, 61))
            expected = 60

        resumed = _cancel_then_resume(isolated, monkeypatch, request)
        assert resumed.success, resumed.error
        rows = dst.rows(dt)
        assert [r[0] for r in rows] == list(range(1, expected + 1)), rows
        recon = resumed.reconciliation or {}
        assert recon.get("passed") is True, recon
        # A first load earns a full digest; into a populated table the
        # job-level delta proof is the ceiling (same as an uninterrupted run).
        assert recon.get("verification_verdict") in (
            {"verified", "partial"} if populated else {"verified"}
        ), recon
    finally:
        src.drop(st)
        dst.drop(dt)


def test_failed_resumed_pass_never_undoes_rows_committed_before_it():
    from types import SimpleNamespace

    from services.batch_undo import undo_failed_batch_if_dest_was_empty

    summary = {
        "target_rows_before": 0,  # the job's first pass found it empty
        "dest_table_existed_before": False,
        "resume_rows_before": 15,  # committed before the cancel
        "table": "t",
    }
    note = undo_failed_batch_if_dest_was_empty(
        destination=SimpleNamespace(kind="database", format="postgresql", extra={}),
        dest_summary=summary,
        sync_mode="incremental_deduped",
    )
    assert summary["partial_batch_undo"] == "retained"
    assert "15 row(s) when this job was resumed" in note
    assert "empty before" not in note and "no such table" not in note
