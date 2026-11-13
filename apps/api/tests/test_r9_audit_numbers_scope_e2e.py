"""QA D220: a new signup must not see platform-wide audit numbers.

Any self-serve account is admin of its own workspace, and three audit routes
answered it with figures about the whole deployment: ``GET /audit/tip`` (the
platform chain head, every anchor receipt, the process-wide count of audit
write failures), ``POST /audit/retention/purge`` (the platform's removed /
kept counts — and the purge itself ran across every tenant) and ``POST
/audit/tip/anchor`` (sealed and returned the platform head). ``GET
/audit/verify`` listed the platform's retention checkpoints with their removed
counts.

Through the real REST routes on the live MongoDB metadata store: Bravo (fresh
signup) gets only its own workspace's head and none of the platform figures,
and is refused the platform-wide actions; the platform admin still gets them.
"""

from __future__ import annotations

import sys
from pathlib import Path

_TESTS = Path(__file__).resolve().parent
if str(_TESTS) not in sys.path:
    sys.path.insert(0, str(_TESTS))

from r9_workspace_world import r9  # noqa: E402,F401

_SEEDED_REMOVED_COUNT = 4321


def _leaks_platform_retention(body: object) -> bool:
    """True when a JSON body carries the platform's retention figures.

    Reads the parsed body: a ``removed_count`` key anywhere, or the seeded
    count as a value. A raw ``"4321" not in text`` check also matched the
    digits inside the keyring's random key ids, the chain head hash or a
    microsecond timestamp, which a workspace's own verify body carries, and
    failed about one run in fifty.
    """
    found = False

    def walk(node: object) -> None:
        nonlocal found
        if isinstance(node, dict):
            if "removed_count" in node:
                found = True
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)
        elif not isinstance(node, bool) and node in (_SEEDED_REMOVED_COUNT, str(_SEEDED_REMOVED_COUNT)):
            found = True

    walk(body)
    return found


def test_the_leak_check_reads_figures_not_digits_inside_hashes():
    own = {
        "signing": {"known_key_ids": ["ak_08d04321088c6276"]},
        "chain_head": "ab4321cd" * 8,
        "retention_checkpoints": [{"kind": "chain_truncation", "at": "2026-11-13T00:43:21.432100+00:00"}],
    }
    assert "4321" in str(own)  # the old substring check failed this body
    assert not _leaks_platform_retention(own)
    assert _leaks_platform_retention({"retention_checkpoints": [{"removed_count": 1}]})
    assert _leaks_platform_retention({"retention_checkpoints": [{"kind": "x", "removed": 4321}]})
    assert _leaks_platform_retention({"detail": "4321"})


def _seed_platform_audit(r9) -> dict[str, str]:
    from services import audit_anchor, audit_log, evidence_chain

    acme = audit_log.append_audit_event(
        action="r9.acme.activity", resource="/r9", actor="owner@acme.test",
        workspace_id=r9.acme, details={"n": 1},
    )
    bravo = audit_log.append_audit_event(
        action="r9.bravo.activity", resource="/r9", actor="bravo-signup@example.test",
        workspace_id=r9.bravo_workspace_id, details={"n": 2},
    )
    platform = audit_log.append_audit_event(
        action="r9.platform.activity", resource="/r9", actor="root@example.test",
        workspace_id="", details={"n": 3},
    )
    audit_anchor.anchor_tip(platform["event_hash"])
    evidence_chain.record_truncation(
        removed_count=_SEEDED_REMOVED_COUNT, last_removed_event_hash="feed" * 16, first_kept_event_hash=None
    )
    return {"acme": acme["event_hash"], "bravo": bravo["event_hash"], "platform": platform["event_hash"]}


def test_signup_sees_its_own_audit_head_not_platform_numbers(r9, tmp_path, monkeypatch):
    from services import audit_anchor, audit_log, evidence_chain

    # The audit trail itself on the live Mongo database, like production.
    monkeypatch.setattr(audit_log, "_mongo_collection", lambda: r9.db["audit_events"])
    monkeypatch.setattr(audit_anchor, "_store_path", lambda: tmp_path / "anchors.jsonl")
    monkeypatch.setattr(evidence_chain, "truncation_store_path", lambda: tmp_path / "truncations.jsonl")
    from services import audit_coverage

    # The process-wide counter, set for this test only.
    monkeypatch.setattr(audit_coverage, "_AUDIT_ACCESS_FAILURES", 7)
    hashes = _seed_platform_audit(r9)
    bravo = r9.bravo()

    tip = bravo.get("/api/v1/audit/tip")
    assert tip.status_code == 200, tip.text
    body = tip.json()
    assert body["scope"] == "workspace"
    assert body["workspace_id"] == r9.bravo_workspace_id
    assert body["tip_hash"] == hashes["bravo"]
    assert body.get("audit_access_failures") is None
    assert body["anchor"] is None and body["anchors_recent"] == []
    for foreign in (hashes["platform"], hashes["acme"]):
        assert foreign not in tip.text

    verify = bravo.get("/api/v1/audit/verify")
    assert verify.status_code == 200, verify.text
    assert not _leaks_platform_retention(verify.json()), verify.text

    # Platform-wide actions are the platform admin's, and refusing them must
    # not leak the figures either.
    for path in ("/api/v1/audit/retention/purge?dry_run=true", "/api/v1/audit/tip/anchor"):
        refused = bravo.post(path)
        assert refused.status_code == 403, (path, refused.text)
        assert hashes["platform"] not in refused.text and "removed" not in refused.text

    # The platform admin (no header = platform scope) still gets every figure.
    root = r9.login("root@example.test", "Bootstrap-Admin-2026!", None)
    platform_tip = root.get("/api/v1/audit/tip")
    assert platform_tip.status_code == 200, platform_tip.text
    ptip = platform_tip.json()
    assert ptip["scope"] == "platform"
    assert ptip["audit_access_failures"] >= 7
    assert ptip["anchors_recent"]
    purge = root.post("/api/v1/audit/retention/purge?dry_run=true")
    assert purge.status_code == 200, purge.text
    assert purge.json()["dry_run"] is True
    assert _leaks_platform_retention(root.get("/api/v1/audit/verify").json())
