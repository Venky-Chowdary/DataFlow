"""C4, fourth review: the key history's position is signed into every record.

Four rounds found anchors by fields an attacker can edit on an unverifiable
record. Now every signed record carries ``kh_seq`` (the chain position of the
history's "began" / current re-anchor record) and ``kh_epoch`` inside its
HMAC, and the verifier derives where the history began from VERIFIED records
only. Live MongoDB, real ``GET /api/v1/audit/verify`` (``r8_ent_harness``).
Each test is a reviewer probe.
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime, timedelta, timezone

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_r12_audit_key_persists_e2e import (  # noqa: F401 - autouse fixture
    _forget_process_keys,
    _no_configured_audit_key,
    _records,
    _verify,
    _write,
)

NOBODY = "k-nobody-ever-had"
ROUTE = "/api/v1/audit/chain/key-history/reanchor"


def _set(ent, record, **fields) -> None:
    ent.db["audit_events"].update_one({"_id": record["_id"]}, {"$set": fields})


def _tampered(report: dict) -> list[str]:
    return [f["kind"] for f in report["findings"] if f["category"] == "tampering"]


def _wipe_history(ent) -> None:
    """A database-level writer removes every trace of the key history."""
    from services import audit_chain_keystore as store

    ent.db[store.HISTORY_COLLECTION].delete_many({})
    ent.db[store.COLLECTION].update_many({}, {"$unset": {"history_epoch": ""}})


def _began(ent) -> dict:
    from services import audit_chain_keystore as store

    return ent.db["audit_events"].find_one({"action": store.EPOCH_ACTION})


def _time_of(record, delta: timedelta):
    t = record["time"]
    if isinstance(t, datetime):
        return t + delta
    return (datetime.fromisoformat(str(t).replace("Z", "+00:00")) + delta).isoformat()


# ── BLOCKING 1 ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize("with_victim", [False, True])
def test_b1_relabelling_the_began_record_then_wiping_the_history_is_tampering(ent, with_victim):
    ws = ent.workspace(f"R14 C4 r4 B1 {with_victim}")
    _write(4, action="r14c4r4.b1", workspace_id=ws)
    assert _verify(ent)["key_history_tampered"] is None
    began = _began(ent)
    _set(ent, began, key_id=NOBODY)  # the anchor no longer verifies
    if with_victim:
        _set(ent, _records(ent, "r14c4r4.b1")[1], key_id=NOBODY, resource="/edited")
    _wipe_history(ent)
    _forget_process_keys()
    _write(1, action="r14c4r4.b1.after", workspace_id=ws)
    report = _verify(ent)
    assert "reset" in (report["key_history_tampered"] or ""), report["key_history_tampered"]
    tampered = {f["event_id"] for f in report["findings"] if f["category"] == "tampering"}
    assert str(began["id"]) in tampered, report["findings"]
    if with_victim:
        assert str(_records(ent, "r14c4r4.b1")[1]["id"]) in tampered
    assert report["unverifiable_key_lost"]["count"] == 0, report["unverifiable_key_lost"]
    assert report["status"] == "findings" and report["verified"] is False


def test_b1_every_new_record_signs_the_history_position(ent):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r4 B1 signed")
    _write(2, action="r14c4r4.signed", workspace_id=ws)
    epoch = store.epoch_record(fresh=True)
    for record in _records(ent, "r14c4r4.signed"):
        assert record["kh_seq"] == int(epoch["head_seq"]) and record["kh_epoch"] == epoch["epoch_id"]
        _set(ent, record, kh_seq=record["kh_seq"] + 1000)  # inside the HMAC: an edit, not a move
    report = _verify(ent, ws)
    assert _tampered(report).count("event_hash_mismatch") == 2, report["findings"]


# ── BLOCKING 2 ─────────────────────────────────────────────────────────────


def _future_pending(ent) -> None:
    from services import audit_chain_keystore as store

    _wipe_history(ent)
    ent.db[store.HISTORY_COLLECTION].insert_one({
        "_id": "__history_epoch__", "pending": True, "epoch_id": "attacker",
        "at": (datetime.now(timezone.utc) + timedelta(days=365)).isoformat(),
    })
    _forget_process_keys()


def test_b2_a_pending_epoch_dated_in_the_future_is_tampering_not_lost_keys(ent):
    ws = ent.workspace("R14 C4 r4 B2 future")
    _write(3, action="r14c4r4.future", workspace_id=ws)
    _set(ent, _records(ent, "r14c4r4.future")[1], key_id=NOBODY, resource="/edited")
    _future_pending(ent)
    report = _verify(ent)
    assert "future" in (report["key_history_tampered"] or ""), report
    assert report["status"] == "findings"
    assert report["unverifiable_key_lost"]["count"] == 0
    assert "unknown_key_after_keyring" in _tampered(report)


class _Unreachable:
    """The history collection during an outage: each call fails after ``delay``."""

    def __init__(self, delay: float = 0.0):
        self.delay = delay
        self.full_name = "down.audit_chain_key_history"

    def _fail(self, *_a, **_k):
        from pymongo.errors import ServerSelectionTimeoutError

        time.sleep(self.delay)
        raise ServerSelectionTimeoutError("history unreachable")

    find_one = find = update_one = delete_one = insert_one = distinct = _fail


def test_b2_an_unreadable_history_is_inconclusive_never_lost_keys(ent, monkeypatch):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r4 B2 unreadable")
    _write(3, action="r14c4r4.unreadable", workspace_id=ws)
    _set(ent, _records(ent, "r14c4r4.unreadable")[1], key_id=NOBODY, resource="/edited")
    _forget_process_keys()
    monkeypatch.setattr(store, "_history_coll", lambda: _Unreachable())
    report = _verify(ent, ws)
    assert report["status"] == "inconclusive", (report["status"], report["findings"])
    assert report["verified"] is False and report["key_history_unreadable"]
    assert report["unverifiable_key_lost"]["count"] == 0
    assert report["categories"]["inconclusive"] == 1


def test_b2_audit_writes_never_wait_on_an_unreachable_history(ent, monkeypatch):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r4 B2 outage")
    _forget_process_keys()
    monkeypatch.setattr(store, "_history_coll", lambda: _Unreachable(delay=0.3))
    started = time.monotonic()
    _write(10, action="r14c4r4.outage", workspace_id=ws)
    elapsed = time.monotonic() - started
    assert len(_records(ent, "r14c4r4.outage")) == 10
    assert elapsed < 1.0, f"10 audit writes took {elapsed:.2f}s during a history outage"


def test_b2_audit_writes_never_wait_on_a_future_pending_epoch(ent):
    ws = ent.workspace("R14 C4 r4 B2 pending wait")
    _future_pending(ent)
    started = time.monotonic()
    _write(5, action="r14c4r4.pendingwait", workspace_id=ws)
    elapsed = time.monotonic() - started
    assert len(_records(ent, "r14c4r4.pendingwait")) == 5
    assert elapsed < 1.0, f"5 audit writes took {elapsed:.2f}s behind a pending epoch"


# ── HIGH 3 ─────────────────────────────────────────────────────────────────


def _reanchor(ent, **body):
    return ent.admin.post(ROUTE, json={"reason": "operator re-anchor after review", **body})


def test_h3_a_reanchor_never_whitewashes_a_relabel(ent):
    ws = ent.workspace("R14 C4 r4 H3 relabel")
    _write(3, action="r14c4r4.h3", workspace_id=ws)
    victim = _records(ent, "r14c4r4.h3")[1]
    _set(ent, victim, key_id=NOBODY, resource="/edited")
    for body in ({}, {"dry_run": False}):
        refused = _reanchor(ent, **body)
        assert refused.status_code == 409, refused.text
    applied = _reanchor(ent, dry_run=False, acknowledge_findings=True)
    assert applied.status_code == 200, applied.text
    _forget_process_keys()
    _write(1, action="r14c4r4.h3.after", workspace_id=ws)
    report = _verify(ent)
    assert str(victim["id"]) in {f["event_id"] for f in report["findings"]
                                 if f["category"] == "tampering"}, report["findings"]
    assert str(victim["id"]) in {p.get("event_id") for p in report["prior_tampering"]}
    assert report["verified"] is False and report["tampering_detected"] is True
    event = ent.db["audit_events"].find_one({"action": "audit.key_history.reanchor"},
                                            sort=[("chain_seq", -1)])
    assert event["details"]["applied"] is True
    assert event["details"]["reason"] == "operator re-anchor after review"


def test_h3_a_reanchor_after_a_reset_keeps_reporting_the_reset(ent):
    ws = ent.workspace("R14 C4 r4 H3 reset")
    _write(3, action="r14c4r4.h3reset", workspace_id=ws)
    _wipe_history(ent)
    _forget_process_keys()
    _write(1, action="r14c4r4.h3reset.after", workspace_id=ws)
    assert "reset" in (_verify(ent)["key_history_tampered"] or "")
    applied = _reanchor(ent, dry_run=False, acknowledge_findings=True)
    assert applied.status_code == 200, applied.text
    _forget_process_keys()
    report = _verify(ent)
    assert report["key_history_tampered"] is None, report["key_history_tampered"]
    assert any(p.get("kind") == "key_history_tampered" and "reset" in p.get("problem", "")
               for p in report["prior_tampering"]), report["prior_tampering"]
    assert report["verified"] is False and report["tampering_detected"] is True


# ── MEDIUM 4 ───────────────────────────────────────────────────────────────


def test_m4_ordinary_clock_skew_is_not_an_edit(ent, monkeypatch):
    """A pre-history lost-key record 10 minutes behind its verified
    predecessor: two nodes' clocks, not an edit (tolerance defaults to 1 h)."""
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r4 M4 skew")
    env_key = "env-m4-" + secrets.token_urlsafe(24)
    # A deployment from before the history: an empty store, no history yet.
    ent.db["audit_events"].delete_many({})
    ent.db[store.COLLECTION].delete_many({})
    ent.db[store.HISTORY_COLLECTION].delete_many({})
    _forget_process_keys()
    with monkeypatch.context() as m:
        m.setattr(store, "_history_coll", lambda: None)
        _write(1, action="r14c4r4.m4.held", workspace_id=ws)
        m.setenv("DATAFLOW_AUDIT_HMAC_KEY", env_key)
        _write(1, action="r14c4r4.m4.node", workspace_id=ws)
    _forget_process_keys()
    held = _records(ent, "r14c4r4.m4.held")[0]
    node = _records(ent, "r14c4r4.m4.node")[0]
    _set(ent, node, time=_time_of(held, timedelta(minutes=-10)))  # its clock ran behind
    _write(1, action="r14c4r4.m4.after", workspace_id=ws)
    report = _verify(ent, ws)
    assert _tampered(report) == [], report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 1


def test_m4_a_key_on_the_history_inside_its_use_window_skips_the_order_test(ent, monkeypatch):
    from services import audit_chain_keystore, evidence_chain

    monkeypatch.setattr(audit_chain_keystore, "HISTORY_REFRESH_S", 0.0)
    ws = ent.workspace("R14 C4 r4 M4 window")
    env_key = "env-m4w-" + secrets.token_urlsafe(24)
    monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", env_key)
    _forget_process_keys()
    _write(3, action="r14c4r4.m4w", workspace_id=ws)
    monkeypatch.delenv("DATAFLOW_AUDIT_HMAC_KEY")
    _forget_process_keys()
    _write(1, action="r14c4r4.m4w.after", workspace_id=ws)
    rows = _records(ent, "r14c4r4.m4w")
    # 90 minutes behind its predecessor (beyond the tolerance), inside the key's use.
    _set(ent, rows[2], time=_time_of(rows[1], timedelta(minutes=-90)))
    monkeypatch.setattr(evidence_chain, "_clock_skew", lambda: timedelta(minutes=5))
    report = _verify(ent, ws)
    assert _tampered(report) == [], report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 3


# ── LOW ────────────────────────────────────────────────────────────────────


def _stale_pending(ent, epoch_id: str) -> str:
    from services import audit_chain_keystore as store

    at = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
    ent.db["audit_events"].delete_many({})
    ent.db[store.COLLECTION].delete_many({})
    ent.db[store.HISTORY_COLLECTION].delete_many({})
    _forget_process_keys()
    ent.db[store.HISTORY_COLLECTION].insert_one(
        {"_id": "__history_epoch__", "pending": True, "epoch_id": epoch_id, "at": at}
    )
    return at


def test_low_a_creator_that_crashed_after_anchoring_is_adopted_not_reset(ent):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 r4 crash")
    at = _stale_pending(ent, "crashed-creator")
    store._anchor_in_chain(store.EPOCH_ACTION, at, "crashed-creator")  # then the process died
    _forget_process_keys()
    _write(2, action="r14c4r4.crash", workspace_id=ws)
    report = _verify(ent)
    assert report["key_history_tampered"] is None, report["key_history_tampered"]
    assert ent.db["audit_events"].count_documents({"action": store.EPOCH_ACTION}) == 1
    assert store.epoch_record(fresh=True)["epoch_id"] == "crashed-creator"


def test_low_a_stale_pending_epoch_with_no_anchor_is_tampering(ent):
    ws = ent.workspace("R14 C4 r4 stale")
    _stale_pending(ent, "never-anchored")
    _write(1, action="r14c4r4.stale", workspace_id=ws)
    report = _verify(ent)
    assert "unfinished" in (report["key_history_tampered"] or ""), report["key_history_tampered"]
