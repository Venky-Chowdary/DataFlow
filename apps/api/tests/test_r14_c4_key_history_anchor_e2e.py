"""C4, review of a350ff42a: the key history is anchored in the hash chain.

Live MongoDB, through the real ``GET /api/v1/audit/verify`` (``r8_ent_harness``).
Each test is a reviewer probe:

* F1 — wiping the history, its keyring copy and the epoch used to start the
  history over at "now", so a relabel written before the wipe read as a lost
  key. The epoch is a signed chain record now; an earlier one is found.
* F2 — "before the history" is judged by chain position, not by the time a
  record claims; a record's own time running backwards against its verified
  neighbours is an edit.
* F5 — a reader that sees the epoch missing re-reads before raising, so two
  workers starting together never produce a false finding.
* F6 — a ``DATAFLOW_SECRETS_KEY`` change is fixed by an audited platform-admin
  re-anchor; the history's MACs bind its namespace, so a row MAC'd for
  another database (same secrets key) is not accepted here.
"""

from __future__ import annotations

import secrets
from datetime import datetime

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_r12_audit_key_persists_e2e import (  # noqa: F401 - autouse fixture
    _forget_process_keys,
    _fresh_container,
    _no_configured_audit_key,
    _records,
    _verify,
    _write,
)

NOBODY = "k-nobody-ever-had"


def _relabel(ent, record, key_id: str, **extra) -> None:
    ent.db["audit_events"].update_one(
        {"_id": record["_id"]}, {"$set": {"resource": "/edited", "key_id": key_id, **extra}}
    )


def _back_dated(record, iso: str):
    """``iso`` in the type the store holds the record's time in."""
    return datetime.fromisoformat(iso) if isinstance(record.get("time"), datetime) else iso


def _tampered(report: dict) -> list[str]:
    return [f["kind"] for f in report["findings"] if f["category"] == "tampering"]


def _wipe_to_an_empty_store(ent) -> None:
    from services import audit_chain_keystore as store

    ent.db["audit_events"].delete_many({})
    ent.db[store.COLLECTION].delete_many({})
    ent.db[store.HISTORY_COLLECTION].delete_many({})
    _forget_process_keys()


# ── F1 ─────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("deployment", ["keyring", "env_key"])
def test_f1_wiping_history_copy_and_epoch_does_not_restart_the_history(ent, monkeypatch, deployment):
    from services import audit_chain_keystore as store

    if deployment == "env_key":
        monkeypatch.setenv("DATAFLOW_AUDIT_HMAC_KEY", "env-f1-" + secrets.token_urlsafe(24))
        _forget_process_keys()
    ws = ent.workspace(f"R14 C4 F1 {deployment}")
    _write(4, action="r14c4.f1", workspace_id=ws)
    assert _verify(ent, ws)["status"] == "verified"
    # A database-level writer: relabel an edited record, then remove every
    # trace of the key history so it restarts after the relabel.
    _relabel(ent, _records(ent, "r14c4.f1")[1], NOBODY)
    ent.db[store.HISTORY_COLLECTION].delete_many({})
    ent.db[store.COLLECTION].update_many({}, {"$unset": {"history_epoch": ""}})
    _forget_process_keys()  # every process restarts
    _write(1, action="r14c4.f1.after", workspace_id=ws)
    for report in (_verify(ent, ws), _verify(ent)):
        assert report["key_history_tampered"] and "reset" in report["key_history_tampered"], report
        assert "unknown_key_after_keyring" in _tampered(report), report["findings"]
        assert report["status"] == "findings" and report["tampering_detected"] is True


def test_f1_the_anchor_is_one_signed_chain_record(ent):
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 F1 anchor")
    _write(1, action="r14c4.f1.anchor", workspace_id=ws)
    epoch = store.epoch_record()
    anchors = list(ent.db["audit_events"].find({"action": store.EPOCH_ACTION}))
    assert len(anchors) == 1, anchors
    assert str(anchors[0]["chain_seq"]) == epoch["head_seq"]
    assert anchors[0]["event_hash"] == epoch["head_hash"]
    assert _verify(ent)["key_history_tampered"] is None


def test_f1_a_history_begun_mid_chain_does_not_flag_the_records_before_it(ent, monkeypatch):
    """Upgrading a deployment creates the epoch after records exist: those
    records verify, so they must not read as tampering."""
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 F1 mid-chain")
    _wipe_to_an_empty_store(ent)
    with monkeypatch.context() as m:
        m.setattr(store, "_history_coll", lambda: None)
        _write(3, action="r14c4.f1.pre", workspace_id=ws)
    _forget_process_keys()
    _write(2, action="r14c4.f1.post", workspace_id=ws)
    report = _verify(ent)
    assert report["status"] == "verified", report["findings"][:3]
    assert report["key_history_tampered"] is None


# ── F2 ─────────────────────────────────────────────────────────────────────


def test_f2_a_back_dated_relabel_with_no_verified_record_before_it_is_tampering(ent):
    """Reviewer repro: on a fresh store the first written record has no
    verified predecessor; relabelled and back-dated before the epoch, its own
    time made it read as a lost key."""
    ws = ent.workspace("R14 C4 F2 first")
    _wipe_to_an_empty_store(ent)
    _write(3, action="r14c4.f2", workspace_id=ws)
    first = _records(ent, "r14c4.f2")[0]
    _relabel(ent, first, NOBODY, time=_back_dated(first, "2001-01-01T00:00:00+00:00"))
    for report in (_verify(ent, ws), _verify(ent)):
        assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]
        assert report["unverifiable_key_lost"]["count"] == 0


def test_f2_a_back_dated_relabel_past_a_run_of_unverifiable_records_is_judged_by_position(
    ent, monkeypatch
):
    """Reviewer probe: put more unverifiable records before the relabel than the
    trusted-clock lookback reads (a node-local key lost in an outage), so no
    verified record anchors it; back-dated before the epoch, its own time made
    it read as a lost key. Its chain position is after the history began."""
    from services import audit_chain_keystore as store

    # A workspace line that starts at the victim: no verified record of it
    # before, and the platform records before it are the outage's.
    ws = "r14c4-f2-position-" + secrets.token_hex(4)
    outage_ws = ent.workspace("R14 C4 F2 outage")
    _fresh_container(ent, monkeypatch, "f2-outage-node")
    with monkeypatch.context() as m:
        m.setattr(store, "current_key", lambda *a, **k: (_ for _ in ()).throw(store.KeystoreUnavailable("down")))
        _write(55, action="r14c4.f2pos.outage", workspace_id=outage_ws)
    _fresh_container(ent, monkeypatch, "f2-after-outage")
    _write(1, action="r14c4.f2pos", workspace_id=ws)
    victim = _records(ent, "r14c4.f2pos")[0]
    _relabel(ent, victim, NOBODY, time=_back_dated(victim, "2001-01-01T00:00:00+00:00"))
    report = _verify(ent, ws)
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]
    assert report["unverifiable_key_lost"]["count"] == 0


def test_f2_time_running_backwards_before_the_history_is_an_edit(ent, monkeypatch):
    """Before the history began an unknown key is a lost key (an earlier
    deploy's) — unless the record's time runs backwards against the verified
    record before it, which only an edit does."""
    from services import audit_chain_keystore as store

    ws = ent.workspace("R14 C4 F2 backwards")
    _wipe_to_an_empty_store(ent)
    with monkeypatch.context() as m:
        m.setattr(store, "_history_coll", lambda: None)
        _write(4, action="r14c4.f2.pre", workspace_id=ws)
    _forget_process_keys()
    _write(1, action="r14c4.f2.post", workspace_id=ws)
    pre = _records(ent, "r14c4.f2.pre")
    _relabel(ent, pre[1], "k-an-earlier-deploy")
    report = _verify(ent)
    assert report["unverifiable_key_lost"]["by_key_id"] == {"k-an-earlier-deploy": 1}, report["findings"]
    assert _tampered(report) == []
    _relabel(ent, pre[2], "k-an-earlier-deploy", time=_back_dated(pre[2], "2001-01-01T00:00:00+00:00"))
    report = _verify(ent)
    assert _tampered(report) == ["unknown_key_after_keyring"], report["findings"]
    assert "back-dated" in next(f["detail"] for f in report["findings"] if f["category"] == "tampering")
