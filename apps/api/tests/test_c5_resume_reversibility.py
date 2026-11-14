"""C5 follow-up — a table an earlier pass finished is not written by the resume.

C5 skips, on resume, the tables an interrupted pass of the same job had
finished. The skipped stream's entry carried that pass's row count as its
``records_processed``, so with reversibility required the backstop read "N
written, 0 checked" for a table this run never wrote and failed the run
(tests/test_reversibility_review6.py on PR 427). Its progress record also
outlived a pass that *finished*: a resume of a completed job skipped tables
and missed rows added since.

* a skipped table records 0 rows written by this pass, names the earlier pass's
  rows as ``rows_from_earlier_pass``, and carries that pass's reversibility
  check counts (``carried_from_earlier_pass``) instead of dropping them;
* a pass that finishes every table clears the record, so a later resume reads
  every table again.
"""

from __future__ import annotations

import uuid

from tests.test_reversibility_review3 import fake  # noqa: F401
from tests.test_reversibility_review6 import _RESUME_MAPS, _resumable  # noqa: F401


def _seed(tmp_path):
    from tests.test_reversibility_review3 import _exec

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    specs = (("people", "email"), ("orders", "note"))
    for table, col in specs:
        _exec(src, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {col} TEXT)")
        _exec(src, f"INSERT INTO {table} VALUES (?, ?)", [(i, f"V{i}") for i in (1, 2, 3)])
        # Each destination table exists (empty) before the job, so Validate on the
        # resume reads every table's live schema, not a half-created destination.
        _exec(dst, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {col} TEXT)")
    return src, dst, specs


def _request(src, dst, specs):
    from tests.test_reversibility_review3 import _ID, _sqlite_request

    up = dict(_RESUME_MAPS[1])
    contracts = [{"name": t, "primary_key": ["id"], "selected": True, "sync_mode": "upsert",
                  "mappings": [dict(_ID), dict(up, source=c, target=c)]} for t, c in specs]
    return _sqlite_request(src, dst, mappings=[], stream_contracts=contracts, sync_mode="upsert")


def test_a_resume_after_an_interrupted_pass_carries_the_finished_tables_proof(
    tmp_path, _resumable, monkeypatch  # noqa: F811
):
    import src.transfer.stream as stream_mod
    import src.transfer.stream_multi as stream_multi
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine

    src, dst, specs = _seed(tmp_path)
    engine, job = UniversalTransferEngine(), "c5r" + uuid.uuid4().hex[:16]
    store = get_mongodb_service()
    real_publish = stream_multi._publish_stream_written

    def cancel_after_people(jid, name, rows):
        real_publish(jid, name, rows)
        if name == "people":
            store.request_job_cancel(job)
            store.update_job_status(job, "cancelled", operator_command=True)

    with monkeypatch.context() as m:
        m.setattr(stream_multi, "_publish_stream_written", cancel_after_people)
        first = engine.execute_tracked(_request(src, dst, specs), job)
    assert not first.success and "cancel" in str(first.error).lower(), first.error
    progress = (store.get_job(job) or {}).get("stream_progress") or {}
    assert sorted(progress) == ["people"], progress
    assert progress["people"]["reversibility_checked"] == {"email": 3}, progress

    read: list[str] = []
    real_stream = stream_mod.stream_database_transfer

    def spy(source, *args, **kwargs):
        read.append(str(source.table))
        return real_stream(source, *args, **kwargs)

    monkeypatch.setattr(stream_mod, "stream_database_transfer", spy)
    store.update_job_status(job, "pending", message="resume", allow_terminal_exit=True)
    second = engine.execute_tracked(_request(src, dst, specs), job, resume=True)
    summary = second.destination_summary or {}
    enforcement = summary.get("reversibility_enforcement") or {}
    assert second.success is True, (second.error, enforcement)
    assert read == ["orders"], read
    streams = {s["name"]: s for s in summary["streams"]}
    assert streams["people"]["records_processed"] == 0, streams["people"]
    assert streams["people"]["rows_from_earlier_pass"] == 3, streams["people"]
    people = [c for c in enforcement["columns"] if c["table"] == "people"]
    assert people and people[0]["rows_written"] == 0, people
    assert people[0]["carried_from_earlier_pass"] == 3, people
    assert "unverified" not in enforcement and enforcement.get("enforced") is not False, enforcement


def test_a_finished_pass_leaves_nothing_for_a_resume_to_skip(tmp_path, _resumable):  # noqa: F811
    from services.mongodb_service import get_mongodb_service
    from src.transfer.engine import UniversalTransferEngine

    src, dst, specs = _seed(tmp_path)
    engine, job = UniversalTransferEngine(), "c5r" + uuid.uuid4().hex[:16]
    assert engine.execute_tracked(_request(src, dst, specs), job).success is True
    assert not (get_mongodb_service().get_job(job) or {}).get("stream_progress")
