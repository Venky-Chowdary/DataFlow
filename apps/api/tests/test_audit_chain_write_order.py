"""Two records written in the same clock tick still form one line of history.

The chain was linked and re-walked by ``time`` alone. A timestamp is not an
ordering: records sharing one sort arbitrarily, so the tip query could hand the
next append a predecessor that was not the last record written (a fork), and a
later verification could read the pair back in the opposite order (a broken
link) — on a chain nobody had tampered with. Verify reported the platform's own
evidence as damaged.
"""

from typing import Any

import pytest

import services.audit_log as audit_log
import services.evidence_chain as evidence_chain


class _Cursor:
    """Just enough of a pymongo cursor: sort(spec) then limit(n)."""

    def __init__(self, docs: list[dict[str, Any]]):
        self._docs = docs

    def sort(self, spec):
        for key, direction in reversed(list(spec)):
            self._docs.sort(
                key=lambda d, k=key: (d.get(k) is None, d.get(k) or 0),
                reverse=direction < 0,
            )
        return self

    def limit(self, n: int):
        return iter(self._docs[:n])

    def __iter__(self):
        return iter(self._docs)


class _Collection:
    """Insertion order is deliberately *not* the order documents come back in."""

    def __init__(self) -> None:
        self.docs: list[dict[str, Any]] = []

    def insert_one(self, doc: dict[str, Any]) -> None:
        self.docs.insert(0, dict(doc))

    def find(self, _query: dict[str, Any] | None = None) -> _Cursor:
        return _Cursor([dict(d) for d in self.docs])


@pytest.fixture
def one_tick_store(tmp_path, monkeypatch):
    coll = _Collection()
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: coll)
    # Every record claims the same instant, the way a burst of evidence writes
    # inside one job does at second — or in a fast test, microsecond — resolution.
    monkeypatch.setattr(audit_log, "_now", lambda: "2026-01-01T00:00:00+00:00")
    monkeypatch.setattr(
        evidence_chain, "list_truncations", lambda: []
    )
    yield coll


def _append(n: int) -> list[dict[str, Any]]:
    return [
        audit_log.append_audit_event(
            action="migration.run_evidence",
            resource=f"job-{i}",
            workspace_id="ws-1",
        )
        for i in range(n)
    ]


def test_a_burst_of_records_in_one_tick_is_a_single_line(one_tick_store):
    events = _append(12)

    assert [e["chain_seq"] for e in events] == list(range(1, 13))
    # Each record names the one written before it, not whichever the clock
    # happened to surface.
    assert events[0]["prev_hash"] is None
    for earlier, later in zip(events, events[1:]):
        assert later["prev_hash"] == earlier["event_hash"]

    report = evidence_chain.verify_chain()
    assert report["checked"] == 12
    assert report["findings"] == [], report["findings"]
    assert report["verified"] is True


def test_verification_reads_them_back_in_write_order(one_tick_store):
    _append(6)
    walked = evidence_chain.read_chain()
    assert [e["chain_seq"] for e in walked] == [1, 2, 3, 4, 5, 6]


def test_a_record_altered_after_the_fact_is_still_caught(one_tick_store):
    """Ordering by write position must not soften tamper detection."""
    coll = one_tick_store
    _append(4)
    for doc in coll.docs:
        if doc.get("chain_seq") == 2:
            doc["resource"] = "job-rewritten"
    report = evidence_chain.verify_chain()
    assert report["verified"] is False
    assert any(f["kind"] == "event_hash_mismatch" for f in report["findings"])
