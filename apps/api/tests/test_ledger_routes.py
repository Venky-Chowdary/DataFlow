"""Evidence Ledger REST surface on the real app and a live MongoDB (r8 enterprise harness).

Covers: the exports carry a receipt; keys (list, JWK Set, JWK / PEM, create /
rotate / retire) with RBAC and workspace isolation, and no private material in
any answer; the log reads (checkpoint, entries, proofs, consistency, receipt,
self-check); online verification including the ledger lookup for a pack
without a receipt; the public verification route (no session, verdicts only,
rate limited, size limited); withdrawal; the platform-only checkpoint anchor;
the audit events.
"""

from __future__ import annotations

import copy
import json

import pytest

from r8_ent_harness import ent  # noqa: F401
from tests.test_parity_api import _job
from tools import datawrap_verify as verifier

SECRET_MARKERS = ("private_enc", "private_key", "enc:v1:", "ledgerkey:", '"d"', '"priv"')


@pytest.fixture()
def world(ent, monkeypatch):  # noqa: F811
    from services import ledger_log, ledger_store
    from services.orphan_jobs import stop_orphan_sweeper
    from src.routers import evidence_router

    monkeypatch.setenv("DATAFLOW_LLM_ENABLED", "false")
    monkeypatch.setenv("DATAFLOW_API_ORPHAN_RESUME", "0")
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(ent.tmp / "anchors.jsonl"))
    monkeypatch.delenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", raising=False)
    stop_orphan_sweeper()
    ledger_log.reset_cache()
    evidence_router._public_buckets.clear()
    ws, other = ent.workspace("Ledger"), ent.workspace("Ledger elsewhere")
    out = {
        "ws": ws, "other": other,
        "job": _job(ws, "payments nightly"), "foreign_job": _job(other, "theirs"),
        "admin": ent.member(email="ledger-admin@example.com", workspace_id=ws, role="admin"),
        "editor": ent.member(email="ledger-editor@example.com", workspace_id=ws, role="editor"),
        "viewer": ent.member(email="ledger-viewer@example.com", workspace_id=ws, role="viewer"),
        "stranger": ent.member(email="ledger-stranger@example.com", workspace_id=other, role="admin"),
    }
    yield out
    ledger_log.reset_cache()
    ledger_store._indexed.clear()
    evidence_router._public_buckets.clear()


def _pack(client, job: str) -> dict:
    res = client.get(f"/api/v1/transfer/{job}/proof-pack")
    assert res.status_code == 200, res.text
    return res.json()


def _statuses(evidence: dict) -> dict[str, str]:
    return {c["id"]: c["status"] for c in evidence["checks"]}


def test_exports_carry_a_receipt_and_the_existing_verify_routes_report_the_evidence(ent, world):  # noqa: F811
    viewer, editor = world["viewer"], world["editor"]
    pack = _pack(viewer, world["job"])
    receipt = pack["ledger"]
    assert receipt["type"] == "datawrap.evidence.receipt.v1" and receipt["statement"]["kind"] == "proof_pack"
    assert receipt["statement"]["subject"] == world["job"] and pack["signature"]["alg"] == "HMAC-SHA256"
    verified = editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": pack})
    assert verified.status_code == 200, verified.text
    body = verified.json()
    assert body["ok"] is True and body["kind"] == "proof_pack"
    assert body["evidence"]["verdict"] == "incomplete" and _statuses(body["evidence"])["signatures"] == "passed"
    assert _statuses(body["evidence"])["anchor"] == "not_checked"
    chain = ent.admin.post("/api/v1/audit/verify-pack", json=pack, headers={"X-Workspace-Id": world["ws"]})
    assert chain.status_code == 200, chain.text
    assert chain.json()["evidence"]["signature_scheme"] == receipt["signature_scheme"]
    old = {k: v for k, v in pack.items() if k != "ledger"}
    legacy = editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": old}).json()
    assert legacy["ok"] is True and legacy["evidence"]["verdict"] == "hmac_only"


def test_keys_are_listed_exported_and_never_show_private_material(ent, world):  # noqa: F811
    viewer, stranger = world["viewer"], world["stranger"]
    _pack(viewer, world["job"])  # first issuance creates the workspace's and the log's keys
    listing = viewer.get("/api/v1/evidence/keys")
    assert listing.status_code == 200, listing.text
    body = listing.json()
    assert set(body) == {"workspace_id", "signature_scheme", "post_quantum", "keys", "log_keys"}
    assert body["workspace_id"] == world["ws"] and body["signature_scheme"] in {"hybrid", "classical"}
    assert body["post_quantum"]["alg"] == "ML-DSA-65"
    assert body["post_quantum"]["enabled"] is (body["signature_scheme"] == "hybrid")
    assert {k["role"] for k in body["keys"]} == {"statement"} and {k["status"] for k in body["keys"]} == {"active"}
    assert {k["role"] for k in body["log_keys"]} == {"checkpoint"}
    assert {k["workspace_id"] for k in body["keys"]} == {world["ws"]}
    jwks = viewer.get("/api/v1/evidence/keys/jwks")
    assert jwks.status_code == 200
    kids = {k["kid"] for k in jwks.json()["keys"]}
    assert kids == {k["key_id"] for k in body["keys"] + body["log_keys"]}
    key_id = body["keys"][0]["key_id"]
    jwk = viewer.get(f"/api/v1/evidence/keys/{key_id}")
    pem = viewer.get(f"/api/v1/evidence/keys/{key_id}", params={"format": "pem"})
    assert jwk.status_code == 200 and jwk.json()["kid"] == key_id
    assert pem.status_code == 200 and pem.text.startswith("-----BEGIN PUBLIC KEY-----")
    loaded = verifier.KeySet()
    loaded.add_text(pem.text)
    assert loaded.get(key_id) is not None  # the key id is recomputable from the PEM
    for res in (listing, jwks, jwk, pem):
        for marker in SECRET_MARKERS:
            assert marker not in res.text, marker
    # Another workspace sees its own (empty) key list, the log's keys, and not this key.
    theirs = stranger.get("/api/v1/evidence/keys").json()
    assert theirs["keys"] == [] and theirs["workspace_id"] == world["other"]
    assert {k["key_id"] for k in theirs["log_keys"]} == {k["key_id"] for k in body["log_keys"]}
    assert stranger.get(f"/api/v1/evidence/keys/{key_id}").status_code == 404
    assert stranger.get(f"/api/v1/evidence/keys/{body['log_keys'][0]['key_id']}").status_code == 200
    assert viewer.get("/api/v1/evidence/keys/" + "0" * 64).status_code == 404
    assert ent.anonymous().get("/api/v1/evidence/keys").status_code in {401, 403}


def test_key_management_needs_workspace_manage_in_that_workspace(ent, world):  # noqa: F811
    admin, editor, viewer, stranger = world["admin"], world["editor"], world["viewer"], world["stranger"]
    for client in (viewer, editor):
        assert client.post("/api/v1/evidence/keys").status_code == 403
        assert client.post("/api/v1/evidence/keys/rotate").status_code == 403
    created = admin.post("/api/v1/evidence/keys")
    assert created.status_code == 201, created.text
    keys = created.json()["keys"]
    assert created.json()["workspace_id"] == world["ws"] and {k["status"] for k in keys} == {"active"}
    assert {k["created_by"] for k in keys} == {"ledger-admin@example.com"}
    assert admin.post("/api/v1/evidence/keys").status_code == 409
    # A workspace admin elsewhere cannot touch this workspace's keys.
    crossed = stranger.post("/api/v1/evidence/keys/rotate", headers={"X-Workspace-Id": world["ws"]})
    assert crossed.status_code in {403, 404}, crossed.text
    assert stranger.post(f"/api/v1/evidence/keys/{keys[0]['key_id']}/retire").status_code == 404
    rotated = admin.post("/api/v1/evidence/keys/rotate")
    assert rotated.status_code == 200, rotated.text
    by_status = {s: [k for k in rotated.json()["keys"] if k["status"] == s] for s in ("active", "retired")}
    assert {k["key_id"] for k in by_status["retired"]} == {k["key_id"] for k in keys}
    assert len(by_status["active"]) == len(keys)
    for key in by_status["active"]:
        retired = admin.post(f"/api/v1/evidence/keys/{key['key_id']}/retire")
        assert retired.status_code == 200 and retired.json()["key"]["status"] == "retired"
    assert admin.post(f"/api/v1/evidence/keys/{by_status['active'][0]['key_id']}/retire").status_code == 409
    assert admin.get("/api/v1/evidence/keys").json()["signature_scheme"] == "none"
    # Every key retired: the export still works and the pack is HMAC only.
    pack = _pack(viewer, world["job"])
    assert "ledger" not in pack
    actions = [e.get("action") for e in ent.audit(world["ws"])]
    for name in ("evidence.key.created", "evidence.key.rotated", "evidence.key.retired", "evidence.issue.failed"):
        assert name in actions, name


def test_log_reads_are_scoped_and_the_proofs_verify_offline(ent, world):  # noqa: F811
    viewer, stranger = world["viewer"], world["stranger"]
    packs = [_pack(viewer, world["job"]) for _ in range(3)]
    foreign = _pack(stranger, world["foreign_job"])
    page = viewer.get("/api/v1/evidence/entries", params={"limit": 2})
    assert page.status_code == 200, page.text
    body = page.json()
    assert [e["content_sha256"] for e in body["entries"]] == [packs[2]["content_sha256"], packs[1]["content_sha256"]]
    assert body["tree_size"] == 4 and {e["workspace_id"] for e in body["entries"]} == {world["ws"]}
    rest = viewer.get("/api/v1/evidence/entries", params={"limit": 2, "before": body["next_before"]}).json()
    assert [e["content_sha256"] for e in rest["entries"]] == [packs[0]["content_sha256"]]
    assert rest["next_before"] is None
    theirs = stranger.get("/api/v1/evidence/entries").json()
    assert [e["content_sha256"] for e in theirs["entries"]] == [foreign["content_sha256"]]
    index = body["entries"][0]["index"]
    assert viewer.get(f"/api/v1/evidence/entries/{index}").json()["status"] == "issued"
    assert stranger.get(f"/api/v1/evidence/entries/{index}").status_code == 404
    assert viewer.get("/api/v1/evidence/entries/999").status_code == 404
    assert viewer.get("/api/v1/evidence/entries", params={"kind": "certificate"}).status_code == 422

    checkpoint = viewer.get("/api/v1/evidence/checkpoint").json()
    assert checkpoint["checkpoint"]["tree_size"] == 4 and checkpoint["anchor"]["status"] == "not_anchored"
    assert checkpoint["checkpoint_sha256"] == verifier.checkpoint_sha256(checkpoint["checkpoint"])
    assert viewer.get("/api/v1/evidence/checkpoint", params={"tree_size": 2}).json()["checkpoint"]["tree_size"] == 2
    assert viewer.get("/api/v1/evidence/checkpoint", params={"tree_size": 40}).status_code == 404
    proof = viewer.get(f"/api/v1/evidence/entries/{index}/proof").json()
    root = verifier.root_from_inclusion(bytes.fromhex(proof["leaf_hash"]), index, proof["tree_size"],
                                        [bytes.fromhex(h) for h in proof["hashes"]])
    assert root.hex() == proof["root_hash"] == checkpoint["checkpoint"]["root_hash"]
    assert viewer.get(f"/api/v1/evidence/entries/{index}/proof", params={"tree_size": 1}).status_code == 422
    earlier = viewer.get("/api/v1/evidence/checkpoint", params={"tree_size": 2}).json()["checkpoint"]
    consistency = viewer.get("/api/v1/evidence/consistency", params={"from": 2, "to": 4})
    assert consistency.status_code == 200, consistency.text
    assert verifier.check_consistency(earlier, consistency.json(), checkpoint["checkpoint"])["status"] == "passed"
    assert viewer.get("/api/v1/evidence/consistency", params={"from": 3, "to": 2}).status_code == 422
    assert viewer.get("/api/v1/evidence/consistency", params={"from": 1, "to": 50}).status_code == 422

    receipt = viewer.get(f"/api/v1/evidence/receipts/{packs[0]['content_sha256']}")
    assert receipt.status_code == 200, receipt.text
    assert receipt.json()["checkpoint"]["tree_size"] == 4  # against the latest checkpoint
    assert receipt.json()["statement"] == packs[0]["ledger"]["statement"]
    keys = verifier.KeySet()
    keys.add_text(viewer.get("/api/v1/evidence/keys/jwks").text)
    offline = verifier.verify_evidence({k: v for k, v in packs[0].items() if k != "ledger"}, receipt.json(), keys,
                                       previous_checkpoint=earlier, consistency_proof=consistency.json())
    offline_statuses = _statuses(offline)
    assert [offline_statuses[c] for c in ("content_digest", "statement", "signatures", "inclusion",
                                          "checkpoint_signature", "consistency")] == ["passed"] * 6
    assert stranger.get(f"/api/v1/evidence/receipts/{packs[0]['content_sha256']}").status_code == 404
    assert viewer.get("/api/v1/evidence/receipts/" + "0" * 64).status_code == 404
    audit = viewer.get("/api/v1/evidence/log/audit").json()
    assert audit == {"ok": True, "tree_size": 4, "checkpoints_checked": 4, "problems": []}


def test_online_verify_looks_up_a_missing_receipt_only_inside_the_callers_workspace(ent, world):  # noqa: F811
    viewer, stranger = world["viewer"], world["stranger"]
    pack = _pack(viewer, world["job"])
    full = viewer.post("/api/v1/evidence/verify", json={"pack": pack})
    assert full.status_code == 200, full.text
    body = full.json()
    assert set(body) == {"kind", "content_sha256", "hmac", "evidence"}
    assert body["hmac"] == {"ok": True, "errors": []} and body["kind"] == "proof_pack"
    assert body["evidence"]["verdict"] == "incomplete" and body["evidence"]["mode"] == "online"
    assert [c["id"] for c in body["evidence"]["checks"]] == list(verifier.CHECK_IDS)
    bare = {k: v for k, v in pack.items() if k != "ledger"}
    looked_up = viewer.post("/api/v1/evidence/verify", json=bare).json()
    assert _statuses(looked_up["evidence"]) == _statuses(body["evidence"])
    beside = viewer.post("/api/v1/evidence/verify", json={"pack": bare, "receipt": pack["ledger"]}).json()
    assert beside["evidence"]["verdict"] == "incomplete"
    # Someone in another workspace holding the bare pack is not told it is in the ledger.
    outside = stranger.post("/api/v1/evidence/verify", json=bare).json()
    assert outside["hmac"]["ok"] is True and outside["evidence"]["verdict"] == "hmac_only"
    forged = {**copy.deepcopy(pack), "actor": "mallory"}
    bad = viewer.post("/api/v1/evidence/verify", json={"pack": forged}).json()
    assert bad["hmac"]["ok"] is False and bad["evidence"]["verdict"] == "failed"
    assert viewer.post("/api/v1/evidence/verify", json={"pack": {"hello": 1}}).status_code == 422
    assert viewer.post("/api/v1/evidence/verify", json={"pack": bare, "receipt": "x"}).status_code == 422


def test_public_verify_needs_no_session_and_answers_verdicts_only(ent, world):  # noqa: F811
    viewer, editor = world["viewer"], world["editor"]
    pack = _pack(viewer, world["job"])
    public = ent.anonymous()
    res = public.post("/api/v1/evidence/public/verify", json={"pack": pack})
    assert res.status_code == 200, res.text
    body = res.json()
    assert set(body) == {"evidence"}
    evidence = body["evidence"]
    assert evidence["verdict"] == "incomplete" and _statuses(evidence)["signatures"] == "passed"
    assert _statuses(evidence)["consistency"] == _statuses(evidence)["withdrawal"] == "passed"
    assert evidence["anchor"] == {"status": "not_anchored"}
    assert next(c for c in evidence["checks"] if c["id"] == "anchor")["detail"] == "Not anchored."
    for private in (world["ws"], world["other"], "ledger-viewer@example.com", "workspace_id", "issued_by"):
        assert private not in res.text, private
    # The receipt beside the pack, and the pack unwrapped, answer the same.
    bare = {k: v for k, v in pack.items() if k != "ledger"}
    beside = public.post("/api/v1/evidence/public/verify", json={"pack": bare, "receipt": pack["ledger"]})
    assert beside.json()["evidence"]["verdict"] == "incomplete"
    # No receipt: the public route does not search the ledger.
    assert public.post("/api/v1/evidence/public/verify", json=bare).json()["evidence"]["verdict"] == "hmac_only"
    forged = {**copy.deepcopy(pack), "actor": "mallory"}
    assert public.post("/api/v1/evidence/public/verify", json=forged).json()["evidence"]["verdict"] == "failed"
    # A receipt that is not this log's learns nothing about this log.
    alien = copy.deepcopy(pack)
    alien["ledger"]["checkpoint"]["origin"] = "datawrap-evidence-ledger/" + "0" * 32
    statuses = _statuses(public.post("/api/v1/evidence/public/verify", json=alien).json()["evidence"])
    assert statuses["consistency"] == "failed" and statuses["withdrawal"] == statuses["anchor"] == "not_checked"
    assert public.post("/api/v1/evidence/public/verify", content=b"{not json",
                       headers={"content-type": "application/json"}).status_code == 422
    assert public.post("/api/v1/evidence/public/verify", json={"pack": {"x": 1}}).status_code == 422
    # A withdrawn pack reads "issued, later withdrawn" without the workspace's reason.
    index = pack["ledger"]["leaf_index"]
    done = editor.post(f"/api/v1/evidence/entries/{index}/withdraw", json={"reason": "internal: wrong tenant run"})
    assert done.status_code == 201, done.text
    withdrawn = public.post("/api/v1/evidence/public/verify", json=pack)
    assert withdrawn.json()["evidence"]["verdict"] == "withdrawn"
    assert withdrawn.json()["evidence"]["issuance"] == "issued_later_withdrawn"
    assert withdrawn.json()["evidence"]["withdrawal"]["reason"] is None
    assert "wrong tenant run" not in withdrawn.text
    # The other evidence routes stay closed without a session.
    assert public.post("/api/v1/evidence/verify", json={"pack": pack}).status_code in {401, 403}
    assert public.get("/api/v1/evidence/entries").status_code in {401, 403}


def test_public_verify_is_rate_limited_and_size_limited(ent, world, monkeypatch):  # noqa: F811
    from src.routers import evidence_router

    public = ent.anonymous()
    body = {"pack": {"content_sha256": "0" * 64, "signature": {"value": "x"}, "job_id": "j"}}
    codes = [public.post("/api/v1/evidence/public/verify", json=body).status_code for _ in range(23)]
    assert codes[:20] == [200] * 20 and set(codes[20:]) == {429}
    limited = public.post("/api/v1/evidence/public/verify", json=body)
    assert limited.status_code == 429 and int(limited.headers["Retry-After"]) >= 1
    assert limited.json() == {"error": "rate_limited", "detail": "Too many verification requests."}
    evidence_router._public_buckets.clear()
    monkeypatch.setattr(evidence_router, "PUBLIC_MAX_BYTES", 200)
    big = {"pack": {**body["pack"], "padding": "x" * 500}}
    assert public.post("/api/v1/evidence/public/verify", json=big).status_code == 413
    assert public.post("/api/v1/evidence/public/verify", content=json.dumps(big).encode(),
                       headers={"content-type": "application/json", "content-length": "10"}).status_code in {400, 413}


def test_withdrawal_needs_job_manage_and_write_access_and_never_erases(ent, world):  # noqa: F811
    viewer, editor, stranger = world["viewer"], world["editor"], world["stranger"]
    pack, replacement = _pack(viewer, world["job"]), _pack(viewer, world["job"])
    index = pack["ledger"]["leaf_index"]
    url = f"/api/v1/evidence/entries/{index}/withdraw"
    assert viewer.post(url, json={"reason": "no"}).status_code == 403
    assert stranger.post(url, json={"reason": "no"}).status_code == 404
    assert editor.post(url, json={}).status_code == 422
    assert editor.post(url, json={"reason": ""}).status_code == 422
    assert editor.post(url, json={"reason": "x", "superseded_by": "nothex"}).status_code == 422
    assert editor.post(url, json={"reason": "x", "superseded_by": "0" * 64}).status_code == 422
    assert editor.post("/api/v1/evidence/entries/999/withdraw", json={"reason": "x"}).status_code == 404
    done = editor.post(url, json={"reason": "Superseded by the re-run.",
                                  "superseded_by": replacement["content_sha256"]})
    assert done.status_code == 201, done.text
    body = done.json()
    assert set(body) == {"entry", "withdrawn", "receipt"}
    assert body["entry"]["kind"] == "withdrawal" and body["entry"]["issued_by"] == "ledger-editor@example.com"
    assert body["withdrawn"]["status"] == "withdrawn" and body["withdrawn"]["index"] == index
    assert body["withdrawn"]["withdrawal"]["superseded_by"] == replacement["content_sha256"]
    assert editor.post(url, json={"reason": "again"}).status_code == 409
    assert editor.post(f"/api/v1/evidence/entries/{body['entry']['index']}/withdraw",
                       json={"reason": "undo"}).status_code == 422
    listed = {e["index"]: e["status"] for e in viewer.get("/api/v1/evidence/entries").json()["entries"]}
    assert listed[index] == "withdrawn" and listed[body["entry"]["index"]] == "withdrawal"
    verdict = viewer.post("/api/v1/evidence/verify", json={"pack": pack}).json()
    assert verdict["hmac"]["ok"] is True and verdict["evidence"]["verdict"] == "withdrawn"
    assert verdict["evidence"]["withdrawal"]["reason"] == "Superseded by the re-run."
    legacy = editor.post("/api/v1/transfer/proof-pack/verify", json={"pack": pack}).json()
    assert legacy["ok"] is True and legacy["evidence"]["issuance"] == "issued_later_withdrawn"
    kept = viewer.get(f"/api/v1/evidence/receipts/{pack['content_sha256']}")
    assert kept.status_code == 200 and kept.json()["leaf_index"] == index  # still there, still provable
    gone = viewer.get(f"/api/v1/evidence/receipts/{pack['content_sha256']}", params={"kind": "withdrawal"})
    assert gone.status_code == 200 and gone.json()["statement"]["kind"] == "withdrawal"
    assert viewer.get("/api/v1/evidence/log/audit").json()["ok"] is True
    actions = [e.get("action") for e in ent.audit(world["ws"])]
    assert "evidence.entry.issued" in actions and "evidence.entry.withdrawn" in actions


def test_anchoring_the_log_is_a_platform_action(ent, world):  # noqa: F811
    _pack(world["viewer"], world["job"])
    assert world["viewer"].post("/api/v1/evidence/checkpoint/anchor").status_code == 403
    assert world["admin"].post("/api/v1/evidence/checkpoint/anchor").status_code == 403  # a workspace admin
    done = ent.admin.post("/api/v1/evidence/checkpoint/anchor")
    assert done.status_code == 200, done.text
    body = done.json()
    assert body["ok"] is True and body["anchored"] is False  # the stub is not a write-once store
    assert body["anchor"]["status"] == "not_anchored" and len(body["checkpoint_sha256"]) == 64
