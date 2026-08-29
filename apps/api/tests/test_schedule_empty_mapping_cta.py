"""Empty-mapping parks must name Transfer Studio, not a phantom Validate button."""

from __future__ import annotations

import pytest

from services.failure_retry_policy import DETERMINISTIC, classify_failure
from services.schedule_mapping_contract import (
    EMPTY_MAPPING_CODE,
    EMPTY_MAPPING_CORRECTIVE,
    EMPTY_MAPPING_REFUSAL,
    is_empty_mapping_finding,
    is_empty_mapping_refusal,
    persisted_mapping_rows,
)


def test_empty_mapping_message_is_recognised():
    assert is_empty_mapping_refusal(EMPTY_MAPPING_REFUSAL) is True
    assert is_empty_mapping_refusal("connection reset") is False
    assert is_empty_mapping_finding(EMPTY_MAPPING_CODE, "") is True
    assert is_empty_mapping_finding("RUN_REFUSED", EMPTY_MAPPING_REFUSAL) is True
    assert is_empty_mapping_finding("SOURCE_SCHEMA_DRIFT", "column added") is False


def test_classify_empty_mapping_names_studio_not_job_validate():
    result = classify_failure(error=EMPTY_MAPPING_REFUSAL, phase="validate", rows_written=0)
    assert result.kind == DETERMINISTIC
    assert "Transfer Studio" in result.corrective_action
    assert "Open Validate for this job" not in result.corrective_action


def test_corrective_copy_refuses_a_signature():
    assert "signature" in EMPTY_MAPPING_CORRECTIVE.lower()
    assert EMPTY_MAPPING_CODE == "EMPTY_MAPPING_CONTRACT"


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield store


def _empty_draft(store, **overrides):
    payload = {
        "name": "Crown → Snowflake",
        "source_connector_id": "src-mysql",
        "source_table": "sample",
        "dest_connector_id": "dst-snow",
        "dest_table": "newtable",
        "interval": "hourly",
        "sync_mode": "full_refresh_append",
    }
    payload.update(overrides)
    return store.create_schedule(payload)


def _park_empty_mapping(store, sched):
    from services.schedule_approvals import build_approval_request, open_approval_request
    from services.standing_authorization import binding_from_schedule

    request = build_approval_request(
        kind="run_refused",
        code=EMPTY_MAPPING_CODE,
        finding=EMPTY_MAPPING_REFUSAL,
        corrective_action=(
            "Open Validate for this job and resolve the blocking check, "
            "then run the schedule again."
        ),
        binding=binding_from_schedule(sched),
        requested_scopes=(),
    )
    return open_approval_request(sched.id, request)


def test_persist_mappings_closes_empty_mapping_park(temp_store):
    store = temp_store
    draft = _empty_draft(store)
    parked = _park_empty_mapping(store, draft)
    assert parked is not None
    assert parked.last_status == "needs_approval"
    assert parked.approval_request["status"] == "open"

    from services.schedule_approvals import open_approvals

    assert len(open_approvals()) == 1

    updated = store.update_schedule(
        draft.id,
        {
            "source_table": "orders",
            "dest_table": "orders_dw",
            "mappings": [{"source": "id", "target": "id"}],
            "enabled": True,
        },
    )
    assert updated is not None
    assert persisted_mapping_rows(updated.mappings)
    assert updated.enabled is True
    assert updated.approval_request["status"] == "approved"
    assert updated.last_status == "approved"
    assert open_approvals() == []


def test_create_with_mappings_replays_unique_empty_draft_on_same_pair(temp_store):
    """Studio footer POSTs a new pipeline today — persist onto the parked draft."""
    store = temp_store
    draft = _empty_draft(store)
    _park_empty_mapping(store, draft)

    live = store.create_schedule({
        "name": "Crown → orders_dw",
        "source_connector_id": "src-mysql",
        "source_table": "orders",
        "dest_connector_id": "dst-snow",
        "dest_table": "orders_dw",
        "interval": "hourly",
        "sync_mode": "full_refresh_append",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert live.id == draft.id
    assert live.source_table == "orders"
    assert live.dest_table == "orders_dw"
    assert live.enabled is True
    assert persisted_mapping_rows(live.mappings)
    assert live.approval_request["status"] == "approved"
    assert len(store.list_schedules()) == 1

    from services.schedule_approvals import open_approvals

    assert open_approvals() == []


def test_explicit_replay_schedule_id_wins(temp_store):
    store = temp_store
    draft = _empty_draft(store)
    other = _empty_draft(
        store,
        name="Other pair",
        source_connector_id="src-other",
        dest_connector_id="dst-other",
    )
    live = store.create_schedule({
        "name": "Replayed",
        "source_connector_id": "src-mysql",
        "source_table": "orders",
        "dest_connector_id": "dst-snow",
        "dest_table": "orders_dw",
        "interval": "hourly",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
        "replay_schedule_id": draft.id,
    })
    assert live.id == draft.id
    assert other.id != draft.id
    assert len(store.list_schedules()) == 2


def test_two_empty_drafts_on_same_pair_do_not_hijack(temp_store):
    store = temp_store
    first = _empty_draft(store, source_table="a", dest_table="a")
    second = _empty_draft(store, source_table="b", dest_table="b")
    live = store.create_schedule({
        "name": "Third",
        "source_connector_id": "src-mysql",
        "source_table": "c",
        "dest_connector_id": "dst-snow",
        "dest_table": "c",
        "interval": "hourly",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    assert live.id not in {first.id, second.id}
    assert live.enabled is True
    assert len(store.list_schedules()) == 3


def test_open_approvals_hides_stale_empty_mapping_after_mappings_exist(temp_store):
    store = temp_store
    sched = store.create_schedule({
        "name": "Already mapped",
        "source_connector_id": "src-mysql",
        "source_table": "orders",
        "dest_connector_id": "dst-snow",
        "dest_table": "orders_dw",
        "interval": "hourly",
        "enabled": True,
        "mappings": [{"source": "id", "target": "id"}],
    })
    from services.schedule_approvals import build_approval_request, open_approvals
    from services.standing_authorization import binding_from_schedule

    stale = build_approval_request(
        kind="run_refused",
        code=EMPTY_MAPPING_CODE,
        finding=EMPTY_MAPPING_REFUSAL,
        corrective_action="Open Validate for this job",
        binding=binding_from_schedule(sched),
        requested_scopes=(),
    )
    # Inject the stale park without triggering release (approval_request in PATCH).
    store.update_schedule(sched.id, {"approval_request": stale, "last_status": "needs_approval"})
    assert store.has_open_approval(store.get_schedule(sched.id))
    assert open_approvals() == []
