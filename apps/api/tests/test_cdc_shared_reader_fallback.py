"""Shared multi-table CDC reader → per-table fallback scope.

Fallback is a retry of the *same* work on another reader, so it is only safe
when the shared reader left no trace: 0 rows applied, shared watermark equal to
its value at entry, and the error is not a cancel. Otherwise the original
exception leaves unchanged and the job fails on it.
"""

from __future__ import annotations

import logging
from unittest.mock import patch

import pytest

import src.transfer.cdc_transfer as cdc_transfer
from services.cdc_engine import ChangeBatch
from services.error_handling import TransferCancelled
from services.logging_config import JsonFormatter, TextFormatter
from services.sync_cursor import SyncContract
from src.transfer.models import EndpointConfig

TABLE = "zz_secret_table"
LSN = "0/DEADBEEF"
SECRET = f"boom at lsn={LSN} in {TABLE}"
TOKEN = f"slot={TABLE}_slot|phase=streaming|lsn={LSN}"
SEQUENTIAL_RESULT = (7, [], {"streams": [], "via": "sequential"}, [])


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)

    def named(self, name):
        return [r for r in self.records if r.getMessage() == name]


@pytest.fixture
def capture():
    log = logging.getLogger(cdc_transfer.logger.name)
    handler, level = _Capture(), log.level
    log.addHandler(handler)
    log.setLevel(logging.DEBUG)
    try:
        yield handler
    finally:
        log.removeHandler(handler)
        log.setLevel(level)


def _fake_apply(*args, **_kwargs):
    change = args[4]
    return (len(change.inserts) + len(change.updates), "ck", {}, len(change.deletes))


def _reader(snapshot, poll):
    class FakeCdc:
        def __init__(self, *a, **k):
            pass

        def is_available(self):
            return True

        def snapshot(self):
            yield from snapshot()

        def poll(self):
            yield from poll()

        def ack(self, token=None):
            pass

        def close(self):
            pass

    return FakeCdc


def _raise(error):
    def gen():
        raise error
        yield  # pragma: no cover

    return gen


def _empty():
    return iter(())


def _run(tmp_path, monkeypatch, reader, *, health=None):
    monkeypatch.setattr("services.sync_cursor.STORE_PATH", tmp_path / "cursors.json")
    monkeypatch.setattr("services.sync_cursor._mongo_cursors", lambda: None)
    sequential = []

    def fake_sequential(*a, **k):
        sequential.append(1)
        return SEQUENTIAL_RESULT

    source = EndpointConfig(kind="database", format="postgresql", database="app", table=TABLE)
    destination = EndpointConfig(
        kind="database", format="sqlite", database=str(tmp_path / "d.db"), table=TABLE
    )
    patches = [
        patch.object(cdc_transfer, "PostgreSqlChangeStreamCdc", reader),
        patch.object(cdc_transfer, "_apply_change_batch", side_effect=_fake_apply),
        patch.object(cdc_transfer, "resolve_dest_table", side_effect=lambda *_a, **_k: "t"),
        patch.object(cdc_transfer, "_run_cdc_multi_stream_sequential", side_effect=fake_sequential),
        patch.dict("os.environ", {"DATAFLOW_CDC_MAX_IDLE_POLLS": "1", "DATAFLOW_CDC_MAX_POLL_ROUNDS": "1"}),
    ]
    if health is not None:
        patches.append(patch("services.row_conservation.record_stream_health", side_effect=health))
    for p in patches:
        p.start()
    try:
        result = cdc_transfer._run_cdc_multi_stream(
            source, destination, [{"source": "id", "target": "id"}], {"id": "string"}, None,
            sync_mode="cdc",
            stream_contracts=[
                {"name": TABLE, "selected": True, "primary_key": "id"},
                {"name": "users", "selected": True, "primary_key": "id"},
            ],
            selected=[
                SyncContract(name=TABLE, primary_key="id", sync_mode="cdc"),
                SyncContract(name="users", primary_key="id", sync_mode="cdc"),
            ],
            job_id="job-fb", checkpoint=None, checkpoint_service=None,
            backfill_new_fields=False, validation_mode="strict", limit=0,
            workspace_id="ws-1",
        )
    finally:
        for p in reversed(patches):
            p.stop()
    return result, sequential


def _clean(records):
    for record in records:
        for fmt in (JsonFormatter(), TextFormatter()):
            out = fmt.format(record)
            assert LSN not in out and TABLE not in out and "DEADBEEF" not in out, out


def test_a_failure_before_any_write_falls_back_once_with_an_event(tmp_path, monkeypatch, capture):
    boom = RuntimeError(SECRET)
    result, sequential = _run(tmp_path, monkeypatch, _reader(_raise(boom), _empty))
    assert result == SEQUENTIAL_RESULT
    assert sequential == [1]
    (event,) = capture.named("cdc.reader.fallback")
    assert (event.outcome, event.levelno) == ("retrying", logging.WARNING)
    assert event.error_type == "RuntimeError"
    assert event.table_count == 2
    assert event.reason_code == "shared_reader_failed_before_write"
    assert event.job_id == "job-fb"
    _clean(capture.records)


def test_b_failure_after_rows_written_reraises_without_fallback(tmp_path, monkeypatch, capture):
    boom = RuntimeError(SECRET)

    def snapshot():
        yield ChangeBatch(inserts=[{"id": "1"}], resume_token=TOKEN, table=TABLE)
        raise boom

    with pytest.raises(RuntimeError) as raised:
        _run(tmp_path, monkeypatch, _reader(snapshot, _empty))
    assert raised.value is boom
    assert capture.named("cdc.reader.fallback") == []
    (cycle,) = capture.named("cdc.cycle.completed")
    assert cycle.outcome == "errored" and cycle.upserts == 1
    _clean(capture.records)


def test_c_advanced_watermark_with_zero_rows_reraises_without_fallback(tmp_path, monkeypatch, capture):
    boom = RuntimeError(SECRET)
    seen: list[dict] = []
    real_shared = cdc_transfer._run_cdc_shared_multi_table

    def spy(*a, **k):
        seen.append(k["progress"])
        return real_shared(*a, **k)

    monkeypatch.setattr(cdc_transfer, "_run_cdc_shared_multi_table", spy)

    def snapshot():
        yield ChangeBatch(resume_token=TOKEN, ack_barrier=True, table=TABLE)
        raise boom

    with pytest.raises(RuntimeError) as raised:
        _run(tmp_path, monkeypatch, _reader(snapshot, _empty))
    (progress,) = seen
    assert sum(progress["counts"].values()) == 0
    assert cdc_transfer.get_watermark(progress["cursor_key"]) != progress["watermark_at_entry"], (
        "precondition: the empty barrier advanced the shared watermark"
    )
    assert raised.value is boom
    assert capture.named("cdc.reader.fallback") == []


def test_d_cancel_inside_the_shared_reader_is_cancelled_without_fallback(tmp_path, monkeypatch, capture):
    cancel = TransferCancelled(SECRET)
    with pytest.raises(TransferCancelled) as raised:
        _run(tmp_path, monkeypatch, _reader(_raise(cancel), _empty))
    assert raised.value is cancel
    assert capture.named("cdc.reader.fallback") == []
    (cycle,) = capture.named("cdc.cycle.completed")
    assert (cycle.outcome, cycle.levelno) == ("cancelled", logging.INFO)


def test_e_health_step_failure_after_close_reraises_without_fallback(tmp_path, monkeypatch, capture):
    boom = RuntimeError(SECRET)

    def snapshot():
        yield ChangeBatch(inserts=[{"id": "1"}], resume_token=TOKEN, table=TABLE)
        yield ChangeBatch(resume_token=TOKEN, ack_barrier=True)

    with pytest.raises(RuntimeError) as raised:
        _run(tmp_path, monkeypatch, _reader(snapshot, _empty), health=boom)
    assert raised.value is boom
    assert capture.named("cdc.reader.fallback") == []
    assert capture.named("cdc.cycle.completed") == []
