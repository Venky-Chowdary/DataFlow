"""Reversibility gate: sixth adversarial review.

Real engine runs (SQLite files, in-process job store) wherever the finding was
confirmed on the engine; the rest at the function the finding names.
"""

from __future__ import annotations

import csv
import io
import sqlite3
import uuid

import pytest

from tests.test_reversibility_review3 import fake  # noqa: F401

# ------------------------------------- High: the backstop is resume-aware

_RESUME_MAPS = [
    {"source": "id", "target": "id", "transform": "none", "confidence": 0.99, "source_type": "INTEGER",
     "target_type": "INTEGER"},
    {"source": "email", "target": "email", "transform": "uppercase", "confidence": 0.99, "source_type": "TEXT",
     "target_type": "TEXT"},
]


@pytest.fixture()
def _resumable(monkeypatch):
    """The in-memory job store (it keeps checkpoints) and two-row batches, so a run resumes mid-file."""
    import services.mongodb_service as mongo_mod
    from src.transfer import file_stream

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setattr(mongo_mod, "_mongodb_service", None)
    monkeypatch.setattr(file_stream, "CHUNK_SIZE", 2)
    yield
    monkeypatch.setattr(mongo_mod, "_mongodb_service", None)


def _people_csv(rows: list[dict]) -> bytes:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=["id", "email"])
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue().encode()


def _resume_request(db, rows: list[dict]):
    from src.transfer.models import EndpointConfig, TransferRequest

    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"), source_filename="people.csv",
        source_content=_people_csv(rows),
        destination=EndpointConfig(kind="database", format="sqlite", connection_string=str(db), table="people"),
        sync_mode="upsert",
        stream_contracts=[{"name": "people", "sync_mode": "upsert", "primary_key": "id", "selected": True}],
        skip_preflight=False, validation_mode="strict", mappings=[dict(m) for m in _RESUME_MAPS],
        require_reversibility=True, compliance_acknowledged=True, acknowledgment_actor="owner@example.com",
        acknowledgment_reason="reviewed run of the reversibility gate")


def _first_attempt_then_resume(tmp_path):
    """The review's scenario: the first attempt writes 2 rows; the resumed run has 7."""
    from src.transfer.engine import UniversalTransferEngine

    db = tmp_path / "d.db"
    rows = [{"id": str(i), "email": f"U{i}@EXAMPLE.COM"} for i in range(1, 8)]
    engine, job = UniversalTransferEngine(), "rev6" + uuid.uuid4().hex[:16]
    first = engine.execute_tracked(_resume_request(db, rows[:2]), job)
    assert first.success is True, first.error
    return db, lambda: engine.execute_tracked(_resume_request(db, rows), job, resume=True)


def _count(db) -> int:
    conn = sqlite3.connect(str(db))
    try:
        return conn.execute("SELECT count(*) FROM people").fetchone()[0]
    finally:
        conn.close()


def test_high_an_honest_enforced_run_resumed_from_a_checkpoint_succeeds_checked_on_the_rows_it_wrote(
        tmp_path, _resumable):
    db, resume = _first_attempt_then_resume(tmp_path)
    second = resume()
    enforcement = (second.destination_summary or {}).get("reversibility_enforcement") or {}
    assert second.success is True, (second.error, enforcement)
    assert second.records_transferred == 7
    assert enforcement.get("enforced") is True, enforcement
    by = {c["column"]: c for c in enforcement["columns"]}
    # 5 rows written after the resume, each checked; the 2 the first attempt committed are named, not compared.
    assert {(c["rows_checked"], c["rows_written"], c["resumed_from"]) for c in by.values()} == {(5, 5, 2)}, by
    assert "unverified" not in enforcement, enforcement
    assert _count(db) == 7


def test_high_a_resumed_run_whose_bulk_path_bypasses_the_check_still_fails(tmp_path, _resumable, monkeypatch):
    import connectors.writer_common as writer_common
    from services import reversibility_enforce as enforce

    db, resume = _first_attempt_then_resume(tmp_path)
    real, calls = writer_common.note_rows_checked, []

    def first_batch_only(mappings, first_row, rows):  # a bulk path wrote every later write batch unchecked
        jobs = {str(m[enforce.STAMP].get("job_id")) for m in mappings or []
                if isinstance(m, dict) and isinstance(m.get(enforce.STAMP), dict)}
        if rows <= 0 or enforce._UNCOUNTED.get() or any(j.startswith("prescan:") for j in jobs):  # scans: not writes
            return real(mappings, first_row, rows)
        calls.append(rows)
        if len(calls) == 1:
            real(mappings, first_row, rows)

    monkeypatch.setattr(writer_common, "note_rows_checked", first_batch_only)
    second = resume()
    enforcement = (second.destination_summary or {}).get("reversibility_enforcement") or {}
    assert len(calls) > 1, calls
    assert second.success is False
    assert "people.email was checked on only 2 of 5 written row(s)" in str(second.error), second.error
    assert enforcement.get("enforced") is False, enforcement


def test_high_a_resumed_run_with_no_recorded_resume_count_is_unverified_never_failed():
    from services import reversibility_enforce as enforce

    mapping = {"source": "email"}
    assert enforce.stamp(mapping, table="people", column="email", code="normalize", job_id="r6unknown")
    enforce.note_resumed("r6unknown", rows=2, stream="")
    enforce.note_rows_checked([mapping], 1, 5)
    fields = {"records_processed": 7, "destination_summary": {}}  # a route that did not record the count
    assert enforce.record_on_success("r6unknown", fields) is None
    found = fields["destination_summary"][enforce.SUMMARY_KEY]
    assert found["enforced"] is True, found
    assert found["columns"][0]["resumed_from"] is None, found
    assert "resumed from a checkpoint" in found["unverified"], found
    assert found["resumed"] == {"rows": 2, "stream": ""}, found


def test_high_multi_stream_each_table_subtracts_only_its_own_checkpoint():
    from services import reversibility_enforce as enforce

    people, orders = {"source": "email"}, {"source": "note"}
    assert enforce.stamp(people, table="people", column="email", code="normalize", job_id="r6multi")
    assert enforce.stamp(orders, table="orders", column="note", code="normalize", job_id="r6multi")
    enforce.note_resumed("r6multi", rows=20, stream="people")
    enforce.note_rows_checked([people], 1, 10)
    enforce.note_rows_checked([orders], 1, 5)  # orders read from its start: it owns no checkpoint
    streams = [{"name": "people", "records_processed": 30, enforce.RESUMED_KEY: 20},
               {"name": "orders", "records_processed": 30}]
    fields = {"records_processed": 60, "destination_summary": {"streams": streams}}
    note = enforce.record_on_success("r6multi", fields)
    assert note and "orders.note was checked on only 5 of 30 written row(s)" in note, note
    assert "people.email" not in note, note
    by = {c["table"]: c for c in fields["destination_summary"][enforce.SUMMARY_KEY]["columns"]}
    assert (by["people"]["rows_checked"], by["people"]["rows_written"], by["people"]["resumed_from"]) == (10, 10, 20)
    assert "resumed_from" not in by["orders"], by
    # The table that owns the checkpoint, with no count recorded for it, is unverified rather than failed.
    assert enforce.stamp(people, table="people", column="email", code="normalize", job_id="r6owner")
    enforce.note_resumed("r6owner", rows=20, stream="people")
    enforce.note_rows_checked([people], 1, 10)
    fields = {"records_processed": 30, "destination_summary": {"streams": [{"name": "people",
                                                                             "records_processed": 30},
                                                                            {"name": "orders",
                                                                             "records_processed": 0}]}}
    assert enforce.record_on_success("r6owner", fields) is None
    found = fields["destination_summary"][enforce.SUMMARY_KEY]
    assert "people.email: the run resumed from a checkpoint" in found["unverified"], found


def test_high_a_database_source_resumed_from_a_checkpoint_is_checked_on_the_rows_it_wrote(tmp_path, _resumable):
    """The table-streaming route (``src.transfer.stream``) records the checkpoint's rows the same way."""
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_reversibility_review3 import _ID, _exec, _sqlite_request

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"U{i}@EXAMPLE.COM") for i in (1, 2)])
    maps = [dict(_ID), dict(_RESUME_MAPS[1])]
    engine, job = UniversalTransferEngine(), "rev6" + uuid.uuid4().hex[:16]

    def request():
        return _sqlite_request(src, dst, mappings=[dict(m) for m in maps], sync_mode="upsert",
                               stream_contracts=[{"name": "people", "sync_mode": "upsert", "primary_key": ["id"],
                                                  "selected": True}])

    first = engine.execute_tracked(request(), job)
    assert first.success is True, first.error
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"U{i}@EXAMPLE.COM") for i in range(3, 8)])
    second = engine.execute_tracked(request(), job, resume=True)
    summary = second.destination_summary or {}
    enforcement = summary.get("reversibility_enforcement") or {}
    assert second.success is True, (second.error, enforcement)
    assert enforcement.get("enforced") is True, enforcement
    assert second.records_transferred == 7
    for column in enforcement["columns"]:
        assert (column["rows_checked"], column["rows_written"], column["resumed_from"]) == (5, 5, 2), column
    assert "unverified" not in enforcement, enforcement


def test_high_a_resumed_multi_stream_run_compares_each_table_its_checkpoint_did_not_seed(tmp_path, _resumable):
    """A table handed no checkpoint rows records 0: it is compared in full, never left unverified."""
    from src.transfer.engine import UniversalTransferEngine
    from tests.test_reversibility_review3 import _ID, _exec, _sqlite_request

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    specs = (("people", "email"), ("orders", "note"))
    for table, col in specs:
        _exec(src, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {col} TEXT)")
        _exec(src, f"INSERT INTO {table} VALUES (?, ?)", [(i, f"V{i}") for i in (1, 2)])
    up = dict(_RESUME_MAPS[1])

    def request():
        contracts = [{"name": t, "primary_key": ["id"], "selected": True, "sync_mode": "upsert",
                      "mappings": [dict(_ID), dict(up, source=c, target=c)]} for t, c in specs]
        return _sqlite_request(src, dst, mappings=[], stream_contracts=contracts, sync_mode="upsert")

    engine, job = UniversalTransferEngine(), "rev6" + uuid.uuid4().hex[:16]
    assert engine.execute_tracked(request(), job).success is True
    for table, _col in specs:
        _exec(src, f"INSERT INTO {table} VALUES (?, ?)", [(i, f"V{i}") for i in range(3, 8)])
    second = engine.execute_tracked(request(), job, resume=True)
    summary = second.destination_summary or {}
    enforcement = summary.get("reversibility_enforcement") or {}
    assert second.success is True, (second.error, enforcement)
    assert enforcement.get("resumed", {}).get("rows") == 2, enforcement
    assert {(c["table"], c["rows_checked"], c["rows_written"]) for c in enforcement["columns"]} == {
        ("people", 7, 7), ("orders", 7, 7)}, enforcement
    assert "unverified" not in enforcement, enforcement
    assert {s["name"]: s["resumed_checkpoint_rows"] for s in summary["streams"]} == {"people": 0, "orders": 0}


def test_high_the_streaming_routes_stamp_the_backstops_resume_key():
    """``src.transfer.stream`` names the key literally (no budget for an import): keep it in step."""
    import inspect

    from services.reversibility_enforce import RESUMED_KEY
    from src.transfer import stream, stream_multi

    assert f'{{"{RESUMED_KEY}": run.tally.written}} if resumed_pass' in inspect.getsource(stream)
    assert f'summary.get("{RESUMED_KEY}")' in inspect.getsource(stream_multi)



def test_high_the_buffered_route_re_applies_its_population_and_is_compared_in_full(tmp_path, _resumable):
    """A buffered (staging) resume writes every row again: its count starts at 0, never left unverified."""
    from src.transfer.engine import UniversalTransferEngine

    db = tmp_path / "d.db"
    rows = [{"id": str(i), "email": f"U{i}@EXAMPLE.COM"} for i in range(1, 8)]

    def request(part):
        req = _resume_request(db, part)
        req.write_via_staging = True
        return req

    engine, job = UniversalTransferEngine(), "rev6" + uuid.uuid4().hex[:16]
    first = engine.execute_tracked(request(rows[:2]), job)
    assert first.success is True, first.error
    second = engine.execute_tracked(request(rows), job, resume=True)
    enforcement = (second.destination_summary or {}).get("reversibility_enforcement") or {}
    assert second.success is True, (second.error, enforcement)
    assert enforcement.get("resumed", {}).get("reapplied") is True, enforcement
    assert {(c["rows_checked"], c["rows_written"]) for c in enforcement["columns"]} == {(7, 7)}, enforcement
    assert all("resumed_from" not in c for c in enforcement["columns"]), enforcement
    assert "unverified" not in enforcement, enforcement
    assert _count(db) == 7

# ------------------------------------- Medium: an empty stream in a multi-stream run

@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "full_refresh_append"])
def test_medium_an_empty_stream_is_written_as_zero_rows_not_failed_as_an_empty_source(tmp_path, fake, mode):  # noqa: F811
    from tests.test_reversibility_review3 import _ID, _exec, _q, _run, _sqlite_request

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    specs = (("people", "email", 30), ("orders", "note", 0), ("items", "label", 7))
    up = dict(_RESUME_MAPS[1])
    for table, col, n in specs:
        _exec(src, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {col} TEXT)")
        if n:
            _exec(src, f"INSERT INTO {table} VALUES (?, ?)", [(i, f"V{i}") for i in range(1, n + 1)])
        if mode == "full_refresh_append":
            _exec(dst, f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, {col} TEXT)")
    contracts = [{"name": t, "primary_key": ["id"], "selected": True,
                  "mappings": [dict(_ID), dict(up, source=c, target=c)]} for t, c, _n in specs]
    res = _run(fake, _sqlite_request(src, dst, mappings=[], stream_contracts=contracts, sync_mode=mode))
    enforcement = (res.destination_summary or {}).get("reversibility_enforcement") or {}
    assert res.success is True, (res.error, enforcement)
    assert res.records_transferred == 37
    assert enforcement.get("enforced") is True, enforcement
    assert {(c["table"], c["rows_checked"], c["rows_written"]) for c in enforcement["columns"]} == {
        ("people", 30, 30), ("orders", 0, 0), ("items", 7, 7)}, enforcement
    # Every stream landed: no partial write behind an "empty source" failure.
    assert _q(dst, "SELECT (SELECT count(*) FROM people), (SELECT count(*) FROM orders), "
                   "(SELECT count(*) FROM items)") == [(30, 0, 7)]

# ------------------------------------- Low: the staging route counts each written row once

def test_low_the_staging_route_counts_the_stage_write_as_checked_not_written(tmp_path, fake):  # noqa: F811
    from tests.test_reversibility_review3 import _ID, _exec, _q, _run, _sqlite_request

    src, dst = tmp_path / "s.sqlite", tmp_path / "d.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"U{i}@EXAMPLE.COM") for i in range(1, 41)])
    request = _sqlite_request(src, dst, mappings=[dict(_ID), dict(_RESUME_MAPS[1])])
    request.write_via_staging = True
    res = _run(fake, request)
    summary = res.destination_summary or {}
    enforcement = summary.get("reversibility_enforcement") or {}
    assert res.success is True, (res.error, enforcement)
    assert (summary.get("pre_ingestion_staging") or {}).get("enabled") is True, sorted(summary)
    assert [(c["column"], c["rows_checked"], c["rows_written"]) for c in enforcement["columns"]] == [
        ("email", 40, 40)], enforcement
    assert _q(dst, "SELECT count(*) FROM people") == [(40,)]
