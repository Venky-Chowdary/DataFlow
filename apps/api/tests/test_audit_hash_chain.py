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
