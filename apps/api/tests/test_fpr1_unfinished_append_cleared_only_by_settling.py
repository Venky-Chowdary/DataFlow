"""PR 411 review 1, M1: only an overwrite or an audited rebaseline settles it.

A full_refresh_append that failed mid-run leaves its committed rows in the
table and an ``unfinished_append`` marker that refuses the next append. Any
successful run cleared the marker, so a one-row incremental upsert into the same
table re-opened the door and the next full_refresh_append landed 4,096 rows a
second time (10,099 rows, 6,003 distinct). The marker is now cleared only by a
full-refresh overwrite success (which rebuilds the table) or an audited
rebaseline, and every clear is audited.
"""

from __future__ import annotations

import sqlite3
import uuid

from services import append_continuity as ac
from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest
from tests.test_audit05_append_after_midrun_failure import (  # noqa: F401  (fixture)
    PRIOR,
    _endpoint,
    _failed_first_run,
    _fix_source,
    _run,
    _state,
    isolated,
)


def _one_row_upsert(fake, tmp_path, dst):
    s2 = tmp_path / "s2.db"
    conn = sqlite3.connect(s2)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, seq INTEGER NOT NULL, v INTEGER)")
    conn.execute("INSERT INTO t VALUES (900001, 0, 1)")
    conn.commit()
    conn.close()
    job_id = "fpr1" + uuid.uuid4().hex[:10]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(s2), table="t"),
        destination=_endpoint(dst),
        sync_mode="incremental_deduped",
        stream_contracts=[
            {
                "name": "t",
                "cursor_field": "seq",
                "cursor_semantics": "monotonic_sequence",
                "primary_key": ["id"],
                "sync_mode": "incremental_deduped",
                "selected": True,
            }
        ],
        skip_preflight=True,
        validation_mode="strict",
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_an_unrelated_upsert_does_not_settle_the_unfinished_append(tmp_path, isolated):
    src, dst, _first, failed_job, committed = _failed_first_run(isolated, tmp_path)
    _fix_source(src)

    upsert = _one_row_upsert(isolated, tmp_path, dst)
    assert upsert.success, upsert.error
    target = ac.resolve_target(_endpoint(dst))
    pending = ac.unfinished_append(target)
    assert pending and pending["job_id"] == failed_job
    before = _state(dst)

    again, _job = _run(isolated, src, dst)

    assert again.success is False
    assert again.error_details["mismatch"] == "unfinished_append"
    # Nothing landed twice: the failed run's rows are still there once.
    assert _state(dst) == before
    count, distinct = before
    assert count == distinct == len(PRIOR) + committed


def test_an_overwrite_settles_it_and_the_clear_is_audited(tmp_path, isolated, monkeypatch):
    import services.audit_log as audit_log

    events: list[dict] = []
    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: events.append(kw) or kw)
    src, dst, _first, failed_job, committed = _failed_first_run(isolated, tmp_path)
    _fix_source(src)

    rebuilt, rebuild_job = _run(isolated, src, dst, mode="full_refresh_overwrite")

    assert rebuilt.success, rebuilt.error
    assert ac.unfinished_append(ac.resolve_target(_endpoint(dst))) is None
    settled = [e for e in events if e["action"] == "append_continuity.unfinished_append_settled"]
    assert len(settled) == 1, events
    details = settled[0]["details"]
    assert details["unfinished_job_id"] == failed_job
    assert details["rows_committed"] == committed
    assert details["settled_by_job_id"] == rebuild_job
    assert details["sync_mode"] == "full_refresh_overwrite"
