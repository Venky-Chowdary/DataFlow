"""Unit legs for QA r12 Mongo D4/D7/D9 and Snowflake D4 (no services).

The live legs are ``test_r12_mongo_incremental_schedule_e2e`` (real schedule
runner, PG -> Mongo and PG -> PG) and
``test_r12_snowflake_incremental_append_second_run`` (fakesnow -> PG).
"""

from __future__ import annotations

import datetime as dt
from datetime import datetime, timezone

import pytest


def _naive_utc(*parts: int) -> dt.datetime:
    """A naive UTC instant — the shape pymongo decodes a BSON date to."""
    return dt.datetime(*parts, tzinfo=dt.timezone.utc).replace(tzinfo=None)


# --- D7/D4: the Mongo tie-set lookup must match a BSON date -------------------


@pytest.mark.parametrize(
    "key, want",
    [
        ("2024-01-01T07:00:00", _naive_utc(2024, 1, 1, 7)),
        ("2024-01-01 07:00:00", _naive_utc(2024, 1, 1, 7)),
        ("2024-01-01T09:30:00+02:00", _naive_utc(2024, 1, 1, 7, 30)),
        ("2024-01-01T07:00:00Z", _naive_utc(2024, 1, 1, 7)),
        ("2024-03-03", _naive_utc(2024, 3, 3)),
    ],
)
def test_watermark_text_widens_to_the_bson_date_the_writer_stored(key, want):
    from services.target_sample import mongo_query_key_variants

    variants = mongo_query_key_variants(key)
    assert key in variants
    assert want in variants


def test_sub_millisecond_watermark_also_matches_the_bson_truncated_instant():
    from services.target_sample import mongo_instant_key_variants

    got = mongo_instant_key_variants("2024-01-01T07:00:00.123456")
    assert _naive_utc(2024, 1, 1, 7, 0, 0, 123456) in got
    assert _naive_utc(2024, 1, 1, 7, 0, 0, 123000) in got


@pytest.mark.parametrize("key", ["7", "", "ü✓", "2024", "not-a-date", "12:00", None, 7])
def test_non_iso_keys_add_no_instant(key):
    from services.target_sample import mongo_instant_key_variants

    assert mongo_instant_key_variants(key) == set()


# --- Snowflake D4: the contract is bound to the source catalog's spelling ----


def test_contract_cursor_and_key_take_the_catalog_spelling():
    from services.sync_cursor import bind_contract_columns_to_source, resolve_sync_contract

    bound = bind_contract_columns_to_source(
        [{"name": "t", "cursor_field": "updated_at", "primary_key": ["id"], "selected": True}],
        ["ID", "NAME", "UPDATED_AT"],
    )
    contract = resolve_sync_contract(bound)
    assert contract.cursor_field == "UPDATED_AT"
    assert contract.primary_key_columns() == ["ID"]


def test_exact_or_ambiguous_names_keep_the_operator_spelling():
    from services.sync_cursor import bind_contract_columns_to_source

    exact = bind_contract_columns_to_source(
        [{"cursor_column": "Orders", "primary_key": "a,b"}], ["Orders", "orders", "A", "b"]
    )
    assert exact[0]["cursor_column"] == "Orders"  # PG: a different column
    assert exact[0]["primary_key"] == "A,b"
    ambiguous = bind_contract_columns_to_source(
        [{"cursor_field": "ORDERS"}], ["Orders", "orders"]
    )
    assert ambiguous[0]["cursor_field"] == "ORDERS"
    assert bind_contract_columns_to_source(None, ["a"]) is None


def test_key_probe_bounds_uppercase_rows_to_the_delta_once_bound():
    """The probe's own lookup stays exact; the bound contract is what matches."""
    from services.destination_key_collision_probe import rows_a_cursor_read_will_deliver
    from services.keyset_pagination import encode_keyset_bookmark

    rows = [{"ID": i, "UPDATED_AT": f"2024-01-01T0{i}:00:00"} for i in range(1, 8)]
    wm = encode_keyset_bookmark(["2024-01-01T07:00:00", "7"])
    assert rows_a_cursor_read_will_deliver(
        rows, cursor_column="UPDATED_AT", watermark=wm, tiebreak_column="ID"
    ) == []


# --- D9: a plan change re-arms; an explicit stalled state otherwise ----------


@pytest.fixture
def store(tmp_path, monkeypatch):
    import services.schedule_store as schedule_store

    monkeypatch.setattr(schedule_store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(schedule_store, "_mongo_backend", lambda: None)
    monkeypatch.setattr(schedule_store, "_guard_schedule_destination", lambda *a, **k: None)
    return schedule_store


_MAPS = [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}]


def _parked(store, *, park_reason: str = "deterministic_refusal", approvable: bool = False):
    from services.schedule_approvals import build_approval_request, open_approval_request
    from services.standing_authorization import binding_from_schedule

    sched = store.create_schedule({
        "name": f"r12 {park_reason}", "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": f"t_{park_reason}", "interval": "hourly",
        "sync_mode": "incremental_append", "cursor_column": "id",
        "cursor_semantics": "monotonic_sequence", "enabled": True, "mappings": _MAPS,
    })
    open_approval_request(sched.id, build_approval_request(
        kind="run_refused", code="RUN_REFUSED", finding="name does not fit",
        corrective_action="Remap name.", binding=binding_from_schedule(sched),
        requested_scopes=("schema_drift",) if approvable else (),
        evidence={"park_reason": park_reason},
    ))
    return store.get_schedule(sched.id)


def test_parked_schedule_reads_stalled_with_the_finding(store):
    sched = _parked(store)
    state, reason = store.schedule_run_state(sched)
    assert state == "stalled"
    assert "RUN_REFUSED" in reason and "name does not fit" in reason and "Remap name." in reason
    assert store.visible_next_run_at(sched) is None


def test_mapping_patch_re_arms_and_an_identical_patch_does_not(store):
    sched = _parked(store)
    same = store.update_schedule(sched.id, {"mappings": list(_MAPS)})
    assert store.has_open_approval(same)
    fixed = store.update_schedule(
        sched.id, {"mappings": [*_MAPS[:1], {"source": "name", "target": "full_name"}]}
    )
    assert not store.has_open_approval(fixed)
    assert fixed.approval_request["status"] == "approved"
    assert store.visible_next_run_at(fixed)
    assert store.schedule_run_state(fixed) == ("scheduled", "")


def test_bookkeeping_writes_never_release_a_finding(store):
    sched = _parked(store)
    after = store.update_schedule(sched.id, {"name": "renamed", "notify_on_success": True})
    assert store.has_open_approval(after)


def test_committed_append_park_stays_a_human_decision(store):
    sched = _parked(store, park_reason="committed_rows_cannot_be_replayed")
    after = store.update_schedule(
        sched.id, {"mappings": [*_MAPS[:1], {"source": "name", "target": "full_name"}]}
    )
    assert store.has_open_approval(after)


def test_paused_schedule_reads_paused(store):
    sched = _parked(store)
    paused = store.update_schedule(sched.id, {"enabled": False})
    assert store.schedule_run_state(paused) == ("paused", "")


# --- D4: a transient fault backs off, it never parks for approval -----------


def test_repeated_transient_failure_backs_off_instead_of_parking(store, monkeypatch):
    from services import schedule_runner as runner

    sched = store.create_schedule({
        "name": "r12 transient", "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": "t_transient", "interval": "hourly",
        "max_retries": 0, "retry_backoff_seconds": 0, "enabled": True, "mappings": _MAPS,
        "sync_mode": "full_refresh_overwrite",
    })
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_job_doc", lambda jid: {
        "status": "failed", "error": "connection reset by peer", "records_processed": 0,
    })
    for attempt in range(4):
        runner._finalize_run(sched.id, f"job-{attempt}", attempt=0,
                             started_at=datetime.now(timezone.utc))
        now = store.get_schedule(sched.id)
        assert not store.has_open_approval(now), (attempt, now.approval_request)
        assert store.visible_next_run_at(now), attempt
        assert store.schedule_run_state(now)[0] == "scheduled"


def test_unrecognised_repeated_failure_still_parks(store, monkeypatch):
    """The owner's grind guard stands for anything that is not transient."""
    from services import schedule_runner as runner

    sched = store.create_schedule({
        "name": "r12 unknown", "source_connector_id": "a", "source_table": "t",
        "dest_connector_id": "b", "dest_table": "t_unknown", "interval": "hourly",
        "max_retries": 0, "retry_backoff_seconds": 0, "enabled": True, "mappings": _MAPS,
        "sync_mode": "full_refresh_overwrite",
    })
    monkeypatch.setattr(runner, "_notify_schedule", lambda *a, **k: None)
    monkeypatch.setattr(runner, "_job_doc", lambda jid: {
        "status": "failed", "error": "source schema altered: column dep_time",
        "records_processed": 0,
    })
    for attempt in range(2):
        runner._finalize_run(sched.id, f"job-{attempt}", attempt=0,
                             started_at=datetime.now(timezone.utc))
    assert store.has_open_approval(store.get_schedule(sched.id))
