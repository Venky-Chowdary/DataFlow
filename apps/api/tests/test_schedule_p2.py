"""P2 scheduling hardening — cron/timezone, incremental request build, retry, run history, concurrency."""

from __future__ import annotations

import threading
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


def test_build_request_full_refresh_default():
    sched = store.PipelineSchedule.from_dict({
        "id": "s1", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "daily",
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
        "validation_mode": "balanced",
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
        "sync_mode": "incremental", "cursor_column": "ts",
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "incremental_append"
    assert req.stream_contracts[0]["sync_mode"] == "incremental_append"


def test_build_request_cdc():
    sched = store.PipelineSchedule.from_dict({
        "id": "s4", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "hourly",
        "sync_mode": "cdc", "primary_key": "id",
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.sync_mode == "cdc"
    assert req.stream_contracts[0]["sync_mode"] == "cdc"


def test_build_request_explicit_contracts_preserved():
    explicit = [{"selected": True, "name": "orders", "sync_mode": "cdc", "primary_key": "id"}]
    sched = store.PipelineSchedule.from_dict({
        "id": "s5", "name": "n", "source_connector_id": "src", "source_table": "orders",
        "dest_connector_id": "dst", "dest_table": "orders_wh", "interval": "hourly",
        "sync_mode": "cdc", "stream_contracts": explicit,
    })
    req = runner.build_schedule_request(sched, _SRC_CONN, _DST_CONN)
    assert req.stream_contracts == explicit


# --------------------------------------------------------------------------- #
# Runner: retry policy + finalize                                             #
# --------------------------------------------------------------------------- #

def test_should_retry_logic():
    assert runner._should_retry("failed", attempt=0, max_retries=2) is True
    assert runner._should_retry("failed", attempt=2, max_retries=2) is False
    assert runner._should_retry("completed", attempt=0, max_retries=2) is False
    assert runner._should_retry("completed_with_quarantine", attempt=0, max_retries=2) is False


def test_finalize_run_retries_on_failure(temp_store, monkeypatch):
    sched = _make(store, max_retries=1, retry_backoff_seconds=0)
    monkeypatch.setattr(runner, "_job_doc", lambda jid: {"status": "failed", "error": "boom"})

    dispatched = {}
    fired = threading.Event()

    def fake_dispatch(schedule_id, attempt=0):
        dispatched["attempt"] = attempt
        fired.set()
        return "job-retry"

    monkeypatch.setattr(runner, "_dispatch_transfer", fake_dispatch)

    runner._finalize_run(sched.id, "job-0", attempt=0, started_at=datetime.now(timezone.utc))
    assert fired.wait(2.0), "retry was not dispatched"
    assert dispatched["attempt"] == 1
    # An intermediate retry entry is recorded.
    reloaded = store.get_schedule(sched.id)
    assert any(r.get("retry_scheduled") for r in reloaded.run_history)


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


def test_missing_connector_records_failed_history(temp_store, monkeypatch):
    sched = _make(store)
    before = sched.run_count
    monkeypatch.setattr(runner, "_resolve_connector", lambda _id: None)
    monkeypatch.setattr(runner, "_scheduler_instance_id", lambda: "test-instance")

    job_id = runner._run_schedule(sched.id)
    assert job_id is None
    reloaded = store.get_schedule(sched.id)
    assert reloaded.run_count == before + 1
    assert reloaded.last_status == "failed"
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
