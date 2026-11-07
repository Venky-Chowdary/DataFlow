"""D-03: enabled downgrade surfaced; parked corrective derived; stack sanitized."""

from __future__ import annotations

import pytest

from services.blocker_titles import sanitize_operator_finding
from services.schedule_mapping_contract import (
    EMPTY_MAPPING_CODE,
    EMPTY_MAPPING_CORRECTIVE,
    EMPTY_MAPPING_REFUSAL,
)


@pytest.fixture
def temp_store(tmp_path, monkeypatch):
    import services.schedule_store as store

    monkeypatch.setattr(store, "STORE_PATH", tmp_path / "schedules.json")
    monkeypatch.setattr(store, "_mongo_backend", lambda: None)
    yield store


def _draft(store, **overrides):
    payload = {
        "name": "no-mappings draft",
        "source_connector_id": "src",
        "source_table": "t",
        "dest_connector_id": "dst",
        "dest_table": "u",
        "interval": "hourly",
        "enabled": True,
        "sync_mode": "incremental_deduped",
        "primary_key": "id",
        "cursor_column": "updated_at",
    }
    payload.update(overrides)
    return store.create_schedule(payload)


def test_enabled_downgrade_is_surfaced_not_silent(temp_store):
    sched = _draft(temp_store)
    assert sched.enabled is False
    assert sched.enabled_note
    assert "Transfer Studio" in sched.enabled_note
    assert sched.enabled_note in sched.to_dict()["enabled_note"]


def test_enabled_with_mappings_records_no_note(temp_store):
    sched = _draft(
        temp_store, mappings=[{"source": "id", "target": "id"}]
    )
    assert sched.enabled is True
    assert sched.enabled_note == ""


def test_parked_empty_corrective_is_derived(temp_store):
    sched = _draft(temp_store)
    from services.schedule_approvals import (
        build_approval_request,
        open_approval_request,
        open_approvals,
    )
    from services.standing_authorization import binding_from_schedule

    request = build_approval_request(
        kind="run_refused",
        code=EMPTY_MAPPING_CODE,
        finding=EMPTY_MAPPING_REFUSAL,
        corrective_action="",
        binding=binding_from_schedule(sched),
        requested_scopes=(),
    )
    open_approval_request(sched.id, request)
    inbox = open_approvals()
    assert len(inbox) == 1
    corrective = inbox[0]["approval"]["corrective_action"]
    assert corrective == EMPTY_MAPPING_CORRECTIVE


def test_parked_generic_finding_gets_validate_corrective(temp_store):
    sched = _draft(temp_store)
    from services.schedule_approvals import (
        build_approval_request,
        open_approval_request,
        open_approvals,
    )
    from services.standing_authorization import binding_from_schedule

    request = build_approval_request(
        kind="run_refused",
        code="RUN_REFUSED",
        finding="some other gate said no",
        corrective_action="",
        binding=binding_from_schedule(sched),
        requested_scopes=(),
    )
    open_approval_request(sched.id, request)
    inbox = open_approvals()
    assert len(inbox) == 1
    assert "Validate" in inbox[0]["approval"]["corrective_action"]


def test_sanitize_drops_driver_stack_lines():
    raw = (
        "pymssql._pymssql.ProgrammingError: (1101, \"BLOB, TEXT, GEOMETRY or "
        "JSON column 'channel' can't have a default value\") "
        "DB-Lib error message 20018, severity 16:\n"
        "General SQL Server error: Check messages from the SQL Server\n"
        '  File "/usr/lib/pymssql/_mssql.pyx", line 1101, in pymssql.connect\n'
        "Traceback (most recent call last):"
    )
    out = sanitize_operator_finding(raw)
    assert "1101" in out
    assert "can't have a default value" in out
    assert "File \"/usr/lib" not in out
    assert "Traceback" not in out


def test_sanitize_passthrough_clean_message():
    msg = "Missing primary key for incremental_deduped."
    assert sanitize_operator_finding(msg) == msg
