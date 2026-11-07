"""A schedule carries the meaning of its cursor to every unattended run.

The g9 gate refuses an undeclared cursor on a strict incremental/CDC run
(services.cursor_semantics). A schedule that cannot persist the declaration
would therefore be blocked on every beat — the live matrix measured exactly
that (`Sync mode contract incomplete` on all incremental cells). These tests
pin the owner chain: store → runner → stream contract.
"""

import pytest

import services.schedule_runner as runner
import services.schedule_store as store

_CONN = {
    "_id": "src-pg", "id": "src-pg", "type": "postgresql", "host": "h", "port": 5432,
    "database": "db", "schema": "public", "username": "u", "password": "p",
}


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")


def _base(**extra):
    return {
        "name": "inc", "source_connector_id": "src-pg", "source_table": "orders",
        "dest_connector_id": "dst-pg", "dest_table": "orders_wh", "interval": "hourly",
        "sync_mode": "incremental_deduped", "cursor_column": "updated_at", "primary_key": "id",
        "mappings": [{"source": "id", "target": "id"}], **extra,
    }


def test_store_persists_and_normalizes_cursor_semantics(temp_store):
    sched = store.create_schedule(_base(cursor_semantics=" Modification_Timestamp "))
    reloaded = store.get_schedule(sched.id)
    assert reloaded is not None
    assert reloaded.cursor_semantics == "modification_timestamp"
    assert reloaded.to_dict()["cursor_semantics"] == "modification_timestamp"


def test_store_refuses_a_cursor_meaning_the_product_does_not_define(temp_store):
    with pytest.raises(ValueError, match="Unknown cursor semantics 'whenever'"):
        store.create_schedule(_base(cursor_semantics="whenever"))
    sched = store.create_schedule(_base())
    with pytest.raises(ValueError, match="Unknown cursor semantics"):
        store.update_schedule(sched.id, {"cursor_semantics": "sometimes"})
    assert store.get_schedule(sched.id).cursor_semantics == ""


def test_update_can_declare_semantics_on_an_existing_schedule(temp_store):
    sched = store.create_schedule(_base())
    store.update_schedule(sched.id, {"cursor_semantics": "insert_only"})
    assert store.get_schedule(sched.id).cursor_semantics == "insert_only"


def test_runner_fallback_stream_contract_carries_the_declaration(temp_store):
    sched = store.create_schedule(_base(cursor_semantics="modification_timestamp"))
    request = runner.build_schedule_request(sched, _CONN, {**_CONN, "id": "dst-pg", "_id": "dst-pg"})
    contracts = request.stream_contracts or []
    assert len(contracts) == 1
    assert contracts[0]["cursor_field"] == "updated_at"
    assert contracts[0]["cursor_semantics"] == "modification_timestamp"


def test_runner_stamps_streams_without_their_own_word_and_keeps_those_with_one(temp_store):
    sched = store.create_schedule(_base(
        cursor_semantics="modification_timestamp",
        stream_contracts=[
            {"selected": True, "name": "orders", "stream": "orders", "sync_mode": "incremental_deduped",
             "cursor_field": "updated_at", "primary_key": "id"},
            {"selected": True, "name": "events", "stream": "events", "sync_mode": "incremental_append",
             "cursor_field": "event_seq", "cursor_semantics": "monotonic_sequence"},
        ],
    ))
    request = runner.build_schedule_request(sched, _CONN, {**_CONN, "id": "dst-pg", "_id": "dst-pg"})
    by_name = {c["name"]: c for c in request.stream_contracts}
    assert by_name["orders"]["cursor_semantics"] == "modification_timestamp"
    assert by_name["events"]["cursor_semantics"] == "monotonic_sequence"
