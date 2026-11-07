"""Audit log hash chaining."""

from __future__ import annotations

from services import audit_log as audit


def test_audit_events_are_hash_chained(tmp_path, monkeypatch):
    path = tmp_path / "audit.jsonl"
    monkeypatch.setattr(audit, "STORE_PATH", path)
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)

    e1 = audit.append_audit_event(action="a", resource="r1", actor="t")
    assert e1.get("event_hash")
    assert e1.get("prev_hash") in (None, "")

    e2 = audit.append_audit_event(action="b", resource="r2", actor="t")
    assert e2["prev_hash"] == e1["event_hash"]
    assert e2["event_hash"] != e1["event_hash"]

    assert audit.latest_event_hash() == e2["event_hash"]


def test_a_synonym_level_is_stored_in_the_vocabulary_readers_filter_on(tmp_path, monkeypatch):
    """``level="warning"`` must not file a serious event as informational.

    The audit API's ``level`` filter and the Settings audit table match the stored
    string exactly against ``info|success|warn|error``, so an English synonym made
    the event invisible in the Warnings view instead of failing loudly.
    """
    monkeypatch.setattr(audit, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit, "_mongo_collection", lambda: None)

    assert audit.append_audit_event(action="a", resource="r", level="warning")["level"] == "warn"
    assert audit.append_audit_event(action="b", resource="r", level="critical")["level"] == "error"
    assert audit.append_audit_event(action="c", resource="r", level="WARN ")["level"] == "warn"
    assert audit.append_audit_event(action="d", resource="r", level="warn")["level"] == "warn"

    assert [e["action"] for e in audit.list_audit_events(limit=10, level="warn")] == ["d", "c", "a"]
