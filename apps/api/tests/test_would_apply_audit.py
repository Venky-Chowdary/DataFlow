"""One would-apply audit helper: deduplicated per key, never raises into the caller."""

from __future__ import annotations

import pytest

from services import would_apply_audit
from services.would_apply_audit import audit_would_apply


@pytest.fixture
def events(monkeypatch):
    would_apply_audit.reset_dedupe()
    seen: list[dict] = []
    import services.audit_log as audit_log

    monkeypatch.setattr(audit_log, "append_audit_event", lambda **kw: seen.append(kw))
    return seen


def _call(key="k", **over):
    audit_would_apply(
        "area.would_do",
        resource="r",
        dedupe_key=key,
        details={"why": "because"},
        flag="DATAFLOW_SOME_FLAG",
        workspace_id=over.pop("workspace_id", "ws"),
        **over,
    )


def test_event_carries_flag_and_not_enforced(events):
    _call()
    assert events == [
        {
            "action": "area.would_do",
            "resource": "r",
            "actor": "system",
            "level": "warn",
            "workspace_id": "ws",
            "details": {"why": "because", "enforced": False, "flag": "DATAFLOW_SOME_FLAG"},
        }
    ]


def test_same_key_is_reported_once_and_new_key_again(events):
    _call("a")
    _call("a")
    _call("b")
    assert len(events) == 2


def test_empty_workspace_is_recorded_as_none(events):
    _call(workspace_id="")
    assert events[0]["workspace_id"] is None


def test_audit_failure_is_swallowed(monkeypatch):
    would_apply_audit.reset_dedupe()
    import services.audit_log as audit_log

    def boom(**_kw):
        raise OSError("disk full")

    monkeypatch.setattr(audit_log, "append_audit_event", boom)
    _call("x")
