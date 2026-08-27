"""P2 scheduling hardening — cron/timezone, incremental request build, retry, run history, concurrency."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import services.schedule_runner as runner
import services.schedule_store as store
from services.cron_schedule import CronError, next_run, validate_cron


# --------------------------------------------------------------------------- #
# Cron parser + IANA timezone next-run                                        #
# --------------------------------------------------------------------------- #

def _utc(y, mo, d, h, mi):
    return datetime(y, mo, d, h, mi, tzinfo=timezone.utc)


def test_cron_every_15_minutes():
    nxt = next_run("*/15 * * * *", _utc(2026, 1, 1, 10, 7), "UTC")
    assert nxt == _utc(2026, 1, 1, 10, 15)


def test_cron_daily_specific_time():
    nxt = next_run("30 2 * * *", _utc(2026, 1, 1, 3, 0), "UTC")
    assert nxt == _utc(2026, 1, 2, 2, 30)


def test_cron_weekday_range_named_optional():
    # 09:00 on weekdays (Mon-Fri). 2026-01-03 is a Saturday -> next is Mon Jan 5.
    nxt = next_run("0 9 * * 1-5", _utc(2026, 1, 3, 12, 0), "UTC")
    assert nxt == _utc(2026, 1, 5, 9, 0)


def test_cron_timezone_conversion_est():
    # Midnight in New York during January (EST = UTC-5) -> 05:00 UTC.
    nxt = next_run("0 0 * * *", _utc(2026, 1, 10, 12, 0), "America/New_York")
    assert nxt == _utc(2026, 1, 11, 5, 0)


def test_cron_timezone_dst_summer():
    # Midnight in New York during July (EDT = UTC-4) -> 04:00 UTC.
    nxt = next_run("0 0 * * *", _utc(2026, 7, 10, 12, 0), "America/New_York")
    assert nxt == _utc(2026, 7, 11, 4, 0)


def test_cron_vixie_dom_or_dow():
    # Day-of-month 13 OR Friday, at midnight. From Jan 1 2026 (Thu):
    # first match is Friday Jan 2 (dow), before the 13th.
    nxt = next_run("0 0 13 * 5", _utc(2026, 1, 1, 6, 0), "UTC")
    assert nxt == _utc(2026, 1, 2, 0, 0)


def test_cron_named_month_and_weekday():
    validate_cron("0 0 1 JAN MON")
    nxt = next_run("0 12 * JUL *", _utc(2026, 6, 30, 0, 0), "UTC")
    assert nxt == _utc(2026, 7, 1, 12, 0)


@pytest.mark.parametrize("expr", [
    "* * * *",            # too few fields
    "60 * * * *",         # minute out of range
    "* 24 * * *",         # hour out of range
    "*/0 * * * *",        # invalid step
    "5-2 * * * *",        # inverted range
])
def test_cron_invalid_raises(expr):
    with pytest.raises(CronError):
        validate_cron(expr)


def test_cron_invalid_timezone_raises():
    with pytest.raises(CronError):
        next_run("0 0 * * *", _utc(2026, 1, 1, 0, 0), "Mars/Phobos")


# --------------------------------------------------------------------------- #
# Store: cadence precedence + new-field persistence + validation              #
# --------------------------------------------------------------------------- #

@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    monkeypatch.setattr(store, "STORE_PATH", path)
    # Force file-backed store even if a real Mongo is reachable in the env.
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield path


def test_compute_next_run_cron_beats_interval():
    out = store.compute_next_run(
        "weekly", _utc(2026, 1, 1, 10, 7), cron="*/15 * * * *", tz="UTC"
    )
    assert out == _utc(2026, 1, 1, 10, 15).isoformat()


def test_create_schedule_persists_new_fields(temp_store):
    sched = store.create_schedule({
        "name": "Incremental orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "cron": "0 3 * * *",
        "timezone": "America/New_York",
        "sync_mode": "incremental",
        "validation_mode": "balanced",
        "cursor_column": "updated_at",
        "primary_key": "id",
        "max_retries": 2,
        "retry_backoff_seconds": 30,
        "notify_on_success": True,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.cron == "0 3 * * *"
    assert reloaded.timezone == "America/New_York"
    assert reloaded.sync_mode == "incremental"
    assert reloaded.validation_mode == "balanced"
    assert reloaded.cursor_column == "updated_at"
    assert reloaded.primary_key == "id"
    assert reloaded.max_retries == 2
    assert reloaded.notify_on_success is True
    # next_run is computed from cron in the schedule timezone.
    assert reloaded.next_run_at is not None
    parsed = datetime.fromisoformat(reloaded.next_run_at)
    assert parsed.tzinfo is not None


def test_create_schedule_persists_studio_data_and_migration_rules(temp_store):
    sched = store.create_schedule({
        "name": "Migrate orders type-locked",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "incremental",
        "validation_mode": "strict",
        "schema_policy": "type_locked",
        "backfill_new_fields": False,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.validation_mode == "strict"
    assert reloaded.schema_policy == "type_locked"
    assert reloaded.backfill_new_fields is False
    assert not hasattr(reloaded, "skip_preflight") or not getattr(reloaded, "skip_preflight", False)


def test_create_schedule_persists_cdc_exactly_once_delivery(temp_store):
    sched = store.create_schedule({
        "name": "CDC EOS orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "cdc",
        "primary_key": "id",
        "delivery_guarantee": "exactly_once",
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.delivery_guarantee == "exactly_once"
    legacy = store.PipelineSchedule.from_dict({
        "id": "legacy-eos",
        "name": "old",
        "source_connector_id": "a",
        "source_table": "t",
        "dest_connector_id": "b",
        "dest_table": "u",
        "interval": "daily",
    })
    assert legacy.delivery_guarantee == "at_least_once"


def test_create_schedule_rejects_bad_cron(temp_store):
    with pytest.raises(ValueError):
        store.create_schedule({
            "name": "bad", "source_connector_id": "a", "source_table": "t",
            "dest_connector_id": "b", "dest_table": "u", "cron": "not a cron",
        })


def test_create_schedule_rejects_bad_sync_mode(temp_store):
    with pytest.raises(ValueError):
        store.create_schedule({
            "name": "bad", "source_connector_id": "a", "source_table": "t",
            "dest_connector_id": "b", "dest_table": "u", "sync_mode": "teleport",
        })


def test_backward_compat_defaults_for_legacy_doc(temp_store):
    legacy = {
        "id": "legacy-1", "name": "old", "source_connector_id": "a",
        "source_table": "t", "dest_connector_id": "b", "dest_table": "u",
        "interval": "daily",
    }
    sched = store.PipelineSchedule.from_dict(legacy)
    assert sched.sync_mode == "full_refresh_overwrite"
    assert sched.validation_mode == "strict"
    assert sched.cron == ""
    assert sched.timezone == "UTC"
    assert sched.max_retries == 0
    assert sched.run_history == []


# --------------------------------------------------------------------------- #
# Store: run history + concurrency guard                                      #
# --------------------------------------------------------------------------- #

def _make(store_mod, **overrides):
    data = {
        "name": "sched", "source_connector_id": "src", "source_table": "t",
        "dest_connector_id": "dst", "dest_table": "u", "interval": "hourly",
        "mappings": [{"source": "id", "target": "id"}],
    }
    data.update(overrides)
    return store_mod.create_schedule(data)


def test_run_history_appends_and_caps(temp_store, monkeypatch):
    monkeypatch.setattr(store, "RUN_HISTORY_LIMIT", 3)
    sched = _make(store)
    for i in range(5):
        store.mark_schedule_run(
            sched.id, f"job-{i}", status="completed",
            run_entry={"job_id": f"job-{i}", "status": "completed"},
        )
    reloaded = store.get_schedule(sched.id)
    assert len(reloaded.run_history) == 3
    assert reloaded.run_history[-1]["job_id"] == "job-4"
    assert reloaded.run_count == 5
    assert reloaded.last_status == "completed"


def test_mark_run_advances_cursor(temp_store):
    sched = _make(store, sync_mode="incremental", cursor_column="updated_at", primary_key="id")
    store.mark_schedule_run(sched.id, "job-1", status="completed", cursor_value="2026-01-01T00:00:00")
    assert store.get_schedule(sched.id).cursor_value == "2026-01-01T00:00:00"


def test_concurrency_guard_same_connector_pair(temp_store):
    a = _make(store, name="a")
    b = _make(store, name="b")  # same src/dst connector pair
    assert store.mark_schedule_running(a.id, "inst-1") is not None
    # Second schedule for the same connector pair is blocked while a is running.
    assert store.mark_schedule_running(b.id, "inst-2") is None
    store.clear_schedule_running(a.id)
    assert store.mark_schedule_running(b.id, "inst-2") is not None


def test_concurrency_guard_same_schedule(temp_store):
    a = _make(store, name="a")
    assert store.mark_schedule_running(a.id, "inst-1") is not None
    assert store.mark_schedule_running(a.id, "inst-1") is None


# --------------------------------------------------------------------------- #
# Runner: incremental/CDC request construction                                #
# --------------------------------------------------------------------------- #

_SRC_CONN = {"_id": "src", "id": "src", "type": "postgresql", "host": "h", "port": 5432,
             "database": "db", "schema": "public", "username": "u", "password": "p"}
_DST_CONN = {"_id": "dst", "id": "dst", "type": "snowflake", "host": "h2", "database": "wh",
             "username": "u", "password": "p", "warehouse": "W"}
_MAPPINGS = [{"source": "id", "target": "id"}]


def test_build_request_full_refresh_default():
    sched = store.PipelineSchedule.from_dict({
        "id": "s1", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "daily",
        "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "full_refresh_overwrite"
    assert req.stream_contracts == []
    assert req.source.table == "orders"
    assert req.destination.table == "orders_wh"
    assert req.skip_preflight is False


def test_build_request_incremental_with_primary_key():
    sched = store.PipelineSchedule.from_dict({
        "id": "s2", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "daily",
        "sync_mode": "incremental", "cursor_column": "updated_at", "primary_key": "id",
        "validation_mode": "balanced", "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "incremental_deduped"
    assert req.validation_mode == "balanced"
    assert len(req.stream_contracts) == 1
    contract = req.stream_contracts[0]
    assert contract["sync_mode"] == "incremental_deduped"
    assert contract["cursor_field"] == "updated_at"
    assert contract["primary_key"] == "id"
    # The engine must be able to resolve the contract we produced.
    from services.sync_cursor import resolve_sync_contract
    resolved = resolve_sync_contract(req.stream_contracts)
    assert resolved is not None
    assert resolved.primary_key == "id"
    assert resolved.cursor_field == "updated_at"


def test_build_request_incremental_append_without_pk():
    sched = store.PipelineSchedule.from_dict({
        "id": "s3", "name": "n", "source_connector_id": "src", "source_table": "events",
        "dest_connector_id": "dst", "dest_table": "events_wh", "interval": "hourly",
        "sync_mode": "incremental", "cursor_column": "ts", "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "incremental_append"
    assert req.stream_contracts[0]["sync_mode"] == "incremental_append"


def test_build_request_procedure_stamps_extra_and_refuses_cdc():
    sched = store.PipelineSchedule.from_dict({
        "id": "s-proc", "name": "n", "source_connector_id": "src", "source_table": "get_orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "daily",
        "source_read_mode": "procedure",
        "procedure_call": "CALL get_orders(:since)",
        "procedure_params": {"since": "2024-01-01"},
        "sync_mode": "full_refresh_append",
        "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.source.extra.get("source_read_mode") == "procedure"
    assert req.source.extra.get("procedure_call") == "CALL get_orders(:since)"
    assert req.source.extra.get("procedure_params") == {"since": "2024-01-01"}
    from services.procedure_source import is_callable_source

    assert is_callable_source(req.source)

    bad = store.PipelineSchedule.from_dict({
        "id": "s-cdc", "name": "n", "source_connector_id": "src", "source_table": "get_orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "hourly",
        "source_read_mode": "procedure",
        "procedure_call": "CALL get_orders()",
        "sync_mode": "cdc",
    })
    with pytest.raises(ValueError, match="snapshot|CDC"):
        runner.build_schedule_request(bad, _SRC_CONN, _DST_CONN)


def test_create_schedule_refuses_scd2_on_procedure(temp_store):
    with pytest.raises(ValueError, match="table identity|snapshot|SCD2"):
        store.create_schedule({
            "name": "proc",
            "source_connector_id": "a",
            "source_table": "get_orders",
            "dest_connector_id": "b",
            "dest_table": "u",
            "source_read_mode": "procedure",
            "procedure_call": "CALL get_orders()",
            "sync_mode": "scd2",
        })


def test_build_request_cdc():
    sched = store.PipelineSchedule.from_dict({
        "id": "s4", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "hourly",
        "sync_mode": "cdc", "primary_key": "id", "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "cdc"
    assert req.stream_contracts[0]["sync_mode"] == "cdc"
    assert req.stream_contracts[0]["snapshot_mode"] == "initial"


def test_build_request_explicit_contracts_preserved():
    explicit = [{"selected": True, "name": "orders", "sync_mode": "cdc", "primary_key": "id"}]
    sched = store.PipelineSchedule.from_dict({
        "id": "s5", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "hourly",
        "sync_mode": "cdc", "stream_contracts": explicit, "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.stream_contracts[0]["name"] == "orders"
    assert req.stream_contracts[0]["sync_mode"] == "cdc"
    assert req.stream_contracts[0]["snapshot_mode"] == "initial"


# --------------------------------------------------------------------------- #
# Runner: retry policy + finalize                                             #
# --------------------------------------------------------------------------- #

def test_should_retry_logic():
    assert runner._should_retry("failed", attempt=0, max_retries=2) is True
    assert runner._should_retry("failed", attempt=2, max_retries=2) is False
    assert runner._should_retry("completed", attempt=0, max_retries=2) is False
    assert runner._should_retry("completed_with_quarantine", attempt=0, max_retries=2) is False


def test_finalize_run_parks_a_durable_retry_on_failure(temp_store, monkeypatch):
    sched = _make(store, max_retries=1, retry_backoff_seconds=0)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {"status": "failed", "error": "boom", "records_processed": 0},
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))

    # The retry is store state, not an in-process timer, so it survives the
    # restart that a rolling deploy performs between the two attempts.
    reloaded = store.get_schedule(sched.id)
    assert reloaded.retry_attempt == 1
    assert reloaded.retry_at is not None
    assert reloaded.running is False
    assert any(r.get("retry_scheduled") for r in reloaded.run_history)
    assert sched.id in {s.id for s in store.due_schedules()}


def test_finalize_run_records_terminal_failure_without_retry(temp_store, monkeypatch):
    sched = _make(store, max_retries=0)
    monkeypatch.setattr(runner, "_job_doc", lambda jid: {"status": "failed", "error": "boom", "records_processed": 0})
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    reloaded = store.get_schedule(sched.id)
    assert reloaded.last_status == "failed"
    assert reloaded.running is False
    assert reloaded.run_history[-1]["status"] == "failed"


def test_finalize_run_success_records_and_notifies(temp_store, monkeypatch):
    sched = _make(store, notify_on_success=True)
    monkeypatch.setattr(
        runner, "_job_doc",
        lambda jid: {"status": "completed", "records_processed": 100, "rejected_rows": 0, "coerced_null_rows": 0},
    )
    notified = {}
    monkeypatch.setattr(runner, "_notify_schedule", lambda s, jid, status, doc: notified.update({"status": status}))

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    reloaded = store.get_schedule(sched.id)
    assert reloaded.last_status == "completed"
    assert reloaded.run_history[-1]["records_transferred"] == 100
    assert notified["status"] == "completed"


def test_committed_append_that_cannot_be_replayed_parks_the_cadence(temp_store, monkeypatch):
    """The append that grew a destination 5 → 25 across four identical beats.

    The attempt committed rows, the retry was refused because a from-zero run
    would duplicate them — and the cadence, which also starts from zero, has to
    be held on a decision rather than left to append the same rows again.
    """
    sched = _make(store, max_retries=2, retry_backoff_seconds=0, sync_mode="full_refresh_append")
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "error": "checksum mismatch after load",
            "records_processed": 5,
            "row_accounting": {"writer_ack": 5, "dest_count": 5},
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))

    reloaded = store.get_schedule(sched.id)
    assert reloaded.run_history[-1].get("retry_refused")
    assert store.has_open_approval(reloaded)
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_deterministic_gate_refusal_parks_on_the_first_beat(temp_store, monkeypatch):
    """A gate verdict is not a cadence event: it parks at once, retry budget or not."""
    sched = _make(store, max_retries=2, retry_backoff_seconds=0)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "phase": "validate",
            "error": "schema drift requires manual review: column dep_time narrowed",
            "records_processed": 0,
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))

    reloaded = store.get_schedule(sched.id)
    assert store.has_open_approval(reloaded)
    assert reloaded.approval_request["evidence"]["park_reason"] == "deterministic_refusal"
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_zero_retry_schedule_still_parks_a_gate_refusal(temp_store, monkeypatch):
    """An exhausted budget is not an unclassified verdict — the gate refusal parks."""
    sched = _make(store, max_retries=0)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "phase": "validate",
            "error": "schema drift requires manual review: column dep_time narrowed",
            "records_processed": 0,
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))

    reloaded = store.get_schedule(sched.id)
    assert reloaded.approval_request["evidence"]["park_reason"] == "deterministic_refusal"
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_parked_schedule_does_not_open_a_second_finding(temp_store, monkeypatch):
    """One unresolved decision per schedule — a repeat must not fan out the inbox."""
    sched = _make(store, max_retries=2)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "phase": "validate",
            "error": "schema drift requires manual review: column dep_time narrowed",
            "records_processed": 0,
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    first = store.get_schedule(sched.id).approval_request
    runner._finalize_run(sched.id, "job-1", attempt=0, started_at=datetime.now(timezone.utc))
    again = store.get_schedule(sched.id).approval_request

    assert first["id"] and first["id"] == again["id"]
    assert int(again.get("occurrences") or 1) == 2
    assert store.has_open_approval(store.get_schedule(sched.id))


def test_second_identical_unrecognised_failure_parks_instead_of_repeating(temp_store, monkeypatch):
    """One beat is the benefit of the doubt; the same verdict twice is evidence."""
    sched = _make(store, max_retries=0, retry_backoff_seconds=0, sync_mode="full_refresh_overwrite")
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "error": "source schema altered: column dep_time",
            "records_processed": 0,
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    assert not store.has_open_approval(store.get_schedule(sched.id))

    runner._finalize_run(sched.id, "job-1", attempt=1, started_at=datetime.now(timezone.utc))
    reloaded = store.get_schedule(sched.id)
    assert store.has_open_approval(reloaded)
    assert reloaded.approval_request["evidence"]["park_reason"] == "identical_failure_repeated"
    assert sched.id not in {s.id for s in store.due_schedules()}


def test_transient_failure_is_still_retried_not_parked(temp_store, monkeypatch):
    sched = _make(store, max_retries=2, retry_backoff_seconds=0)
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(
        runner,
        "_job_doc",
        lambda jid: {
            "status": "failed",
            "error": "connection reset by peer",
            "records_processed": 0,
        },
    )

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    reloaded = store.get_schedule(sched.id)
    assert not store.has_open_approval(reloaded)
    assert reloaded.retry_attempt == 1


def test_failure_signature_ignores_row_numbers_and_ids():
    a = {"error": "Snowflake rejected 27 cell finding(s) — arr_time (row 431) does not fit"}
    b = {"error": "Snowflake rejected 31 cell finding(s) — arr_time (row 902) does not fit"}
    assert runner._failure_signature(a) == runner._failure_signature(b)
    other = {"error": "connection reset by peer"}
    assert runner._failure_signature(a) != runner._failure_signature(other)


def test_run_entry_copies_dest_count_ledger_not_only_writer_ack():
    started = datetime.now(timezone.utc)
    entry = runner._run_entry(
        "job-ledger",
        "completed",
        0,
        started,
        {
            "records_processed": 10_000,
            "rejected_rows": 0,
            "coerced_null_rows": 0,
            "row_accounting": {
                "dest_count": 4,
                "writer_ack": 10_000,
                "balanced": True,
                "conservation_kind": "overwrite",
                "rows_written_source": "gate8_dest_readback",
            },
        },
    )
    assert entry["records_transferred"] == 10_000
    assert entry["row_accounting"]["dest_count"] == 4
    assert entry["row_accounting"]["writer_ack"] == 10_000


def test_missing_connector_records_failed_history(temp_store, monkeypatch):
    sched = _make(store)
    before = sched.run_count
    monkeypatch.setattr(runner, "_resolve_connector", lambda _id: None)
    monkeypatch.setattr(runner, "_scheduler_instance_id", lambda: "test-instance")

    job_id = runner._run_schedule(sched.id)
    assert job_id is None
    reloaded = store.get_schedule(sched.id)
    assert reloaded.run_count == before + 1
    # The beat is recorded failed, and the schedule now additionally parks on a
    # finding: a connector that no longer resolves does not come back on its own,
    # and every later beat only added another identical failed row.
    assert reloaded.run_history[-1].get("status") == "failed"
    assert reloaded.last_status == "needs_approval"
    assert (reloaded.approval_request or {}).get("status") == "open"
    assert reloaded.running is False
    assert "connector" in (reloaded.run_history[-1].get("error") or "").lower()
    assert reloaded.next_run_at is not None


def test_import_file_schedules_into_mongo_when_empty(tmp_path, monkeypatch):
    path = tmp_path / "schedules.json"
    migrated = tmp_path / "schedules.json.migrated"
    path.write_text(
        '{"schedules":[{"id":"s1","name":"Recovered","source_connector_id":"a","source_table":"t",'
        '"dest_connector_id":"b","dest_table":"d","interval":"daily","enabled":true,'
        '"cron":"","timezone":"UTC","run_count":2,"next_run_at":"2026-07-19T14:10:00+00:00"}]}',
        encoding="utf-8",
    )
    monkeypatch.setattr(store, "STORE_PATH", path)
    monkeypatch.setattr(store, "STORE_MIGRATED_PATH", migrated)
    store._file_import_attempted = False

    mem: dict[str, dict] = {}

    class FakeColl:
        def find(self, *_a, **_k):
            return list(mem.values())

        def find_one(self, filt, *_a, **_k):
            if filt.get("_id") == "primary":
                return None
            return mem.get(filt.get("_id"))

        def find_one_and_update(self, filt, update, upsert=False, return_document=True):
            oid = filt.get("_id")
            set_doc = dict(update.get("$set") or {})
            set_on_insert = dict(update.get("$setOnInsert") or {})
            # Mirror Mongo ConflictingUpdateOperators for `_id`
            if "_id" in set_doc or "_id" in set_on_insert:
                raise AssertionError(
                    "Updating the path '_id' would create a conflict at '_id' "
                    f"(set={'_id' in set_doc}, setOnInsert={'_id' in set_on_insert})"
                )
            payload = {**set_doc, "_id": oid}
            mem[oid] = payload
            return payload

        def replace_one(self, filt, doc, upsert=False):
            mem[filt["_id"]] = doc

        def delete_many(self, filt):
            keep = set(filt.get("_id", {}).get("$nin") or [])
            for k in list(mem):
                if k not in keep:
                    del mem[k]

        def update_one(self, filt, update, upsert=False):
            doc = mem.get(filt.get("_id"))
            if doc is None:
                return None
            doc.update(dict(update.get("$set") or {}))
            return doc

    class FakeDB:
        def __getitem__(self, name):
            return FakeColl()

    class FakeMongo:
        client = object()

        def get_database(self):
            return FakeDB()

    monkeypatch.setattr(store, "_mongo_backend", lambda: FakeMongo())
    imported = store.import_file_schedules_into_mongo(force=True)
    assert imported == 1
    assert "s1" in mem
    assert mem["s1"]["name"] == "Recovered"
    assert mem["s1"]["run_count"] == 2
    assert migrated.exists() or not path.exists()
    # Second call is a no-op once mongo has schedules.
    assert store.import_file_schedules_into_mongo(force=True) == 0


def test_save_mongo_never_sets_id_path(monkeypatch):
    """Regression: $set/_id + $setOnInsert/_id → ConflictingUpdateOperators."""
    mem: dict[str, dict] = {}
    updates: list[dict] = []

    class FakeColl:
        def find_one(self, filt, *_a, **_k):
            return mem.get(filt.get("_id"))

        def find_one_and_update(self, filt, update, upsert=False, return_document=True):
            updates.append(update)
            set_doc = dict(update.get("$set") or {})
            set_on_insert = dict(update.get("$setOnInsert") or {})
            if "_id" in set_doc or "_id" in set_on_insert:
                raise RuntimeError(
                    "Updating the path '_id' would create a conflict at '_id'"
                )
            oid = filt["_id"]
            mem[oid] = {**set_doc, "_id": oid}
            return mem[oid]

        def replace_one(self, filt, doc, upsert=False):
            mem[filt["_id"]] = doc

        def delete_many(self, filt):
            keep = set(filt.get("_id", {}).get("$nin") or [])
            for k in list(mem):
                if k not in keep:
                    del mem[k]

        def update_one(self, filt, update, upsert=False):
            doc = mem.get(filt.get("_id"))
            if doc is None:
                return None
            doc.update(dict(update.get("$set") or {}))
            return doc

        def find(self, *_a, **_k):
            return list(mem.values())

    class FakeDB:
        def __getitem__(self, name):
            return FakeColl()

    class FakeMongo:
        client = object()

        def get_database(self):
            return FakeDB()

    sched = store.PipelineSchedule(
        id="8b15732d-98f3-4a2d-97ab-cf5e600fd61b",
        name="Test",
        source_connector_id="src",
        source_table="t",
        dest_connector_id="dst",
        dest_table="u",
        interval="daily",
        running=True,
        running_instance="inst-1",
    )
    # Simulate a payload that historically leaked mongo `_id` into $set.
    original_to_dict = sched.to_dict

    def leaky_to_dict():
        d = original_to_dict()
        d["_id"] = sched.id
        return d

    sched.to_dict = leaky_to_dict  # type: ignore[method-assign]
    store._save_mongo(FakeMongo(), [sched])
    assert updates, "expected find_one_and_update"
    assert "_id" not in (updates[0].get("$set") or {})
    assert "_id" not in (updates[0].get("$setOnInsert") or {})
    assert mem[sched.id]["running"] is True
    assert mem[sched.id]["id"] == sched.id


def test_build_request_replays_studio_validate_identity():
    """A scheduled run must carry the same recipe/locales/hashes as Studio Execute."""
    sched = store.PipelineSchedule.from_dict({
        "id": "s-studio",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "daily",
        "date_locale": "DMY",
        "number_locale": "EU",
        "shape_recipe": {"version": 1, "steps": [{"op": "trim", "column": "name"}]},
        "approved_shape_recipe_hash": "a" * 64,
        "approved_decision_artifact_hash": "b" * 64,
        "approved_ddl_identity_hash": "c" * 64,
        "stream_contracts": [{"stream": "orders", "primary_key": "id"}],
        "mappings": [
            {
                "source": "amt",
                "target": "amt",
                "target_type": "DECIMAL(18,4)",
                "risk_contract": {"status": "signed"},
            }
        ],
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.date_locale == "DMY"
    assert req.number_locale == "EU"
    assert req.shape_recipe.get("version") == 1
    assert req.approved_shape_recipe_hash == "a" * 64
    assert req.approved_decision_artifact_hash == "b" * 64
    assert req.approved_ddl_identity_hash == "c" * 64
    assert req.mappings[0]["risk_contract"]["status"] == "signed"
    assert req.stream_contracts[0]["primary_key"] == "id"
    assert req.skip_preflight is False


def test_create_schedule_persists_advanced_write_knobs(temp_store):
    sched = store.create_schedule({
        "name": "Stage then promote",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "full_refresh_append",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.write_via_staging is True
    assert reloaded.priority_column == "updated_at"
    assert reloaded.priority_direction == "asc"
    assert reloaded.row_limit == 2500
    legacy = store.PipelineSchedule.from_dict({
        "id": "legacy-adv",
        "name": "old",
        "source_connector_id": "a",
        "source_table": "t",
        "dest_connector_id": "b",
        "dest_table": "u",
        "interval": "daily",
    })
    assert legacy.write_via_staging is False
    assert legacy.priority_column == ""
    assert legacy.priority_direction == "desc"
    assert legacy.row_limit == 0


def test_build_request_replays_advanced_write_knobs():
    sched = store.PipelineSchedule.from_dict({
        "id": "s-adv",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "daily",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
        "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.write_via_staging is True
    assert req.priority_column == "updated_at"
    assert req.priority_direction == "asc"
    assert req.limit == 2500
    assert req.skip_preflight is False


def test_create_schedule_persists_cdc_snapshot_mode(temp_store):
    """Studio Advanced when_needed must survive onto the hourly beat."""
    sched = store.create_schedule({
        "name": "CDC when_needed orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "cdc",
        "primary_key": "id",
        "snapshot_mode": "when_needed",
        "mappings": _MAPPINGS,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.snapshot_mode == "when_needed"
    req = runner.build_schedule_request(reloaded, _SRC_CONN, _DST_CONN)
    assert req.stream_contracts[0]["snapshot_mode"] == "when_needed"
    overwrite = store.create_schedule({
        "name": "Full overwrite",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "full_refresh_overwrite",
        "snapshot_mode": "when_needed",
    })
    assert overwrite.snapshot_mode == ""


def test_create_schedule_persists_cdc_advanced_extras(temp_store):
    """Studio allow_append_only / row filter / MultiSubnetFailover survive the beat."""
    sched = store.create_schedule({
        "name": "CDC append-only orders",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "cdc",
        "primary_key": "id",
        "allow_append_only": True,
        "cdc_row_filter": "net",
        "multi_subnet_failover": True,
        "mappings": _MAPPINGS,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.allow_append_only is True
    assert reloaded.cdc_row_filter == "net"
    assert reloaded.multi_subnet_failover is True
    sql_src = {**_SRC_CONN, "type": "sqlserver"}
    req = runner.build_schedule_request(reloaded, sql_src, _DST_CONN)
    assert req.destination.extra.get("allow_append_only") is True
    assert req.source.extra.get("cdc_row_filter") == "net"
    assert req.source.extra.get("multi_subnet_failover") is True
    overwrite = store.create_schedule({
        "name": "Full overwrite",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "sync_mode": "full_refresh_overwrite",
        "allow_append_only": True,
        "cdc_row_filter": "net",
        "multi_subnet_failover": True,
    })
    assert overwrite.allow_append_only is False
    assert overwrite.cdc_row_filter == ""
    assert overwrite.multi_subnet_failover is False


def test_build_request_replays_cdc_advanced_extras():
    sched = store.PipelineSchedule.from_dict({
        "id": "s-cdc-x",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "cdc",
        "primary_key": "id",
        "allow_append_only": True,
        "cdc_row_filter": "all update old",
        "multi_subnet_failover": True,
        "mappings": _MAPPINGS,
    })
    req = runner.build_schedule_request(sched, {**_SRC_CONN, "type": "sqlserver"}, _DST_CONN)
    assert req.destination.extra.get("allow_append_only") is True
    assert req.source.extra.get("cdc_row_filter") == "all update old"
    assert req.source.extra.get("multi_subnet_failover") is True
    assert req.skip_preflight is False


def test_list_summary_exposes_cdc_advanced_extras():
    from src.routers.schedules_router import ScheduleSummaryResponse

    sched = store.PipelineSchedule.from_dict({
        "id": "s-cdc-sum",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "sync_mode": "cdc",
        "allow_append_only": True,
        "cdc_row_filter": "net",
        "multi_subnet_failover": True,
    })
    summary = ScheduleSummaryResponse.from_schedule(sched)
    assert summary.allow_append_only is True
    assert summary.cdc_row_filter == "net"
    assert summary.multi_subnet_failover is True


def test_list_summary_exposes_advanced_write_knobs():
    """List/edit/drawer read the summary. Omitting knobs made Save wipe Studio values."""
    from src.routers.schedules_router import ScheduleSummaryResponse

    sched = store.PipelineSchedule.from_dict({
        "id": "s-sum",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "daily",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
        "date_locale": "DMY",
        "number_locale": "EU",
    })
    summary = ScheduleSummaryResponse.from_schedule(sched)
    assert summary.write_via_staging is True
    assert summary.priority_column == "updated_at"
    assert summary.priority_direction == "asc"
    assert summary.row_limit == 2500
    assert summary.date_locale == "DMY"
    assert summary.number_locale == "EU"


def test_update_omitting_write_knobs_preserves_them(temp_store):
    sched = store.create_schedule({
        "name": "Stage then promote",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
    })
    renamed = store.update_schedule(sched.id, {"name": "renamed only"})
    assert renamed is not None
    assert renamed.write_via_staging is True
    assert renamed.priority_column == "updated_at"
    assert renamed.priority_direction == "asc"
    assert renamed.row_limit == 2500


def test_update_explicit_false_clears_write_knobs(temp_store):
    sched = store.create_schedule({
        "name": "Stage then promote",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "write_via_staging": True,
        "priority_column": "updated_at",
        "priority_direction": "asc",
        "row_limit": 2500,
    })
    cleared = store.update_schedule(sched.id, {
        "write_via_staging": False,
        "priority_column": "",
        "priority_direction": "desc",
        "row_limit": 0,
    })
    assert cleared is not None
    assert cleared.write_via_staging is False
    assert cleared.priority_column == ""
    assert cleared.row_limit == 0


def test_create_schedule_persists_studio_locales(temp_store):
    sched = store.create_schedule({
        "name": "EU dates",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "date_locale": "DMY",
        "number_locale": "EU",
        "mappings": _MAPPINGS,
    })
    reloaded = store.get_schedule(sched.id)
    assert reloaded.date_locale == "DMY"
    assert reloaded.number_locale == "EU"
    req = runner.build_schedule_request(reloaded, _SRC_CONN, _DST_CONN)
    assert req.date_locale == "DMY"
    assert req.number_locale == "EU"


def test_update_explicit_empty_clears_locales(temp_store):
    sched = store.create_schedule({
        "name": "EU dates",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "daily",
        "date_locale": "DMY",
        "number_locale": "EU",
    })
    cleared = store.update_schedule(sched.id, {"date_locale": "", "number_locale": ""})
    assert cleared is not None
    assert cleared.date_locale == ""
    assert cleared.number_locale == ""


def test_build_schedule_request_refuses_empty_mappings():
    """Unattended runs must not invent _auto_map when ScheduleForm omitted mappings."""
    sched = store.PipelineSchedule.from_dict({
        "id": "s-empty",
        "name": "n",
        "source_connector_id": "src",
        "source_table": "orders",
        "dest_connector_id": "dst",
        "dest_table": "orders_wh",
        "interval": "hourly",
    })
    with pytest.raises(ValueError, match="no persisted column mappings"):
        runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)


def test_create_schedule_pauses_when_mappings_empty(temp_store):
    sched = store.create_schedule({
        "name": "No map",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "enabled": True,
    })
    assert sched.enabled is False
    with pytest.raises(ValueError, match="no persisted column mappings"):
        store.update_schedule(sched.id, {"enabled": True})


def test_create_schedule_stays_enabled_when_mappings_present(temp_store):
    sched = store.create_schedule({
        "name": "Mapped",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "enabled": True,
        "mappings": _MAPPINGS,
    })
    assert sched.enabled is True
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.mappings[0]["source"] == "id"


def test_due_schedules_skips_enabled_legacy_without_mappings(temp_store):
    """A hand-edited enabled row still must not enter the due set."""
    leaked = store.PipelineSchedule.from_dict({
        "id": "legacy-empty",
        "name": "legacy empty",
        "source_connector_id": "src-1",
        "source_table": "orders",
        "dest_connector_id": "dst-1",
        "dest_table": "orders_wh",
        "interval": "hourly",
        "enabled": True,
        "mappings": [],
        "next_run_at": "2000-01-01T00:00:00+00:00",
    })
    store._save_all([leaked])
    assert leaked.id not in {s.id for s in store.due_schedules()}
