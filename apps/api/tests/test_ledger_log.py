"""The Evidence Ledger log: append-only, unforkable, with checkpoints, proofs and receipts.

Pins: an entry's index is assigned by insert-if-absent, so concurrent appenders
(threads on the file store, separate clients on a live MongoDB) never share an
index and every size has one root; appending the same pack twice is one entry;
every entry has a receipt that verifies offline against every later
checkpoint, and any two checkpoints are consistent; one checkpoint is stored
per size; a rewritten leaf is caught by the self-check, and by a client that
kept an older checkpoint; a tampered receipt fails.
"""

from __future__ import annotations

import copy
import json
import threading
import uuid

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tools import datawrap_verify as verifier


@pytest.fixture()
def ledger(tmp_path, monkeypatch):
    """The ledger on an isolated file store, with the stub anchor provider."""
    from services import ledger_log, ledger_store

    (tmp_path / "data").mkdir(exist_ok=True)
    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path / "data"))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.delenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", raising=False)
    monkeypatch.delenv("DATAWRAP_AUDIT_ANCHOR_PROVIDER", raising=False)
    monkeypatch.setattr(ledger_store, "_db", lambda: None)
    ledger_log.reset_cache()
    yield ledger_log
    ledger_log.reset_cache()


def _warm(*workspaces: str) -> int:
    """Create the keys the test will sign with (each key event is a leaf); returns the log size after."""
    from services import ledger_keys, ledger_store

    for workspace in workspaces or ("w1",):
        ledger_keys.signer(ledger_keys.scope_for(workspace))
    ledger_keys.signer(ledger_keys.LOG_SCOPE)
    return ledger_store.tree_size()


def _digest(n: int) -> str:
    return verifier.sha256_hex(f"pack-{n}".encode())


def _append(log, n: int, *, workspace: str = "w1", kind: str = "proof_pack"):
    from services import ledger_keys

    signer = ledger_keys.signer(ledger_keys.scope_for(workspace))
    return log.append(kind=kind, subject=f"job-{n}", content_sha256=_digest(n), workspace_id=workspace,
                      actor="tester@example.com", signer=signer)


def _keys(*receipts) -> verifier.KeySet:
    from services import ledger_keys

    ids: list[str] = []
    for r in receipts:
        ids += list(r["statement"]["signer_key_ids"]) + list(r["checkpoint"]["signer_key_ids"])
    return ledger_keys.trusted_keys(ids)


def _core(receipt, keys=None) -> dict[str, str]:
    result = verifier.verify_evidence(None, receipt, keys or _keys(receipt))
    return {c["id"]: c["status"] for c in result["checks"]}


def test_append_assigns_consecutive_indexes_and_is_idempotent_per_pack(ledger):
    base = _warm()
    first, created = _append(ledger, 1)
    again, created_again = _append(ledger, 1)
    second, _ = _append(ledger, 2)
    assert (first["index"], created, created_again, second["index"]) == (base, True, False, base + 1)
    assert again["index"] == base and again["leaf_hash"] == first["leaf_hash"]
    assert ledger.tree_size() == base + 2
    statement = first["statement"]
    assert set(statement) == {"type", "kind", "subject", "content_sha256", "signer_key_ids", "signer_algs",
                              "issued_at", "nonce"}
    assert statement["type"] == "datawrap.evidence.statement.v1" and len(statement["nonce"]) == 32
    assert len(statement["signer_algs"]) == len(statement["signer_key_ids"]) >= 1
    assert first["leaf_hash"] == verifier.leaf_hash(verifier.statement_bytes(statement)).hex()
    assert ledger.find_entry(_digest(2))["index"] == base + 1 and ledger.find_entry("0" * 64) is None
    with pytest.raises(ledger.LedgerInvalid):
        _append(ledger, 3, kind="certificate")


def test_a_checkpoint_is_signed_once_per_size_and_names_the_log(ledger):
    assert ledger.checkpoint() is None
    base = _warm()
    _append(ledger, 1)
    size = base + 1
    record = ledger.checkpoint()
    signed = record["checkpoint"]
    assert set(signed) == {"type", "origin", "tree_size", "root_hash", "timestamp", "signer_key_ids",
                           "signer_algs", "keys", "signatures"}
    assert signed["tree_size"] == size and signed["root_hash"] == ledger.root_hash(size)
    assert signed["origin"].startswith("datawrap-evidence-ledger/") and signed["origin"] == ledger.origin()
    assert record["checkpoint_sha256"] == verifier.checkpoint_sha256(signed)
    # The checkpoint publishes its own public keys; each key id is the hash of the key it names.
    assert sorted(k["kid"] for k in signed["keys"]) == signed["signer_key_ids"]
    published = verifier.KeySet()
    assert sorted(published.adopt(signed["keys"], role="checkpoint")) == signed["signer_key_ids"]
    # A witness co-signature can be added later without touching what the log signed.
    assert verifier.checkpoint_bytes({**signed, "cosignatures": [{"alg": "Ed25519", "key_id": "w", "value": "x"}]}) \
        == verifier.checkpoint_bytes(signed)
    assert ledger.checkpoint() == ledger.checkpoint(size)  # stored, not re-signed
    assert ledger.checkpoint(size)["checkpoint"]["timestamp"] == signed["timestamp"]
    _append(ledger, 2)
    assert ledger.checkpoint()["tree_size"] == size + 1 and ledger.latest_checkpoint()["tree_size"] == size + 1
    assert ledger.checkpoint(size)["checkpoint"] == signed
    assert ledger.checkpoint(size + 9, create=False) is None


def test_every_entry_has_a_receipt_that_verifies_at_every_later_checkpoint(ledger):
    base = _warm()
    entries = [_append(ledger, n)[0] for n in range(9)]
    for n in range(9):
        ledger.checkpoint(base + n + 1)
    for entry in entries:
        for size in range(entry["index"] + 1, base + 10):
            receipt = ledger.receipt(entry, size=size)
            checks = _core(receipt)
            assert checks["signatures"] == checks["inclusion"] == checks["checkpoint_signature"] == "passed", (
                entry["index"], size, checks)
            assert receipt["checkpoint"]["tree_size"] == size and receipt["type"] == "datawrap.evidence.receipt.v1"
    with pytest.raises(ledger.LedgerInvalid):
        ledger.receipt(entries[5], size=entries[5]["index"])


def test_any_two_checkpoints_are_consistent(ledger):
    base = _warm()
    for n in range(11):
        _append(ledger, n)
        ledger.checkpoint()
    total = base + 11
    for second in range(base + 1, total + 1):
        for first in range(base + 1, second + 1):
            proof = ledger.consistency_proof(first, second)
            older, newer = ledger.checkpoint(first)["checkpoint"], ledger.checkpoint(second)["checkpoint"]
            assert proof["from_root"] == older["root_hash"] and proof["to_root"] == newer["root_hash"]
            assert verifier.check_consistency(older, proof, newer)["status"] == "passed"
            # Held the newer one, shown the older: the same proof answers it.
            assert verifier.check_consistency(newer, proof, older)["status"] == "passed"
    for bad in ((0, 3), (4, 3), (1, 99)):
        with pytest.raises(ledger.LedgerInvalid):
            ledger.consistency_proof(*bad)
    with pytest.raises(ledger.LedgerInvalid):
        ledger.inclusion_proof(total)


def test_property_inclusion_and_consistency_over_a_stored_log(ledger):
    _warm()
    for n in range(37):
        _append(ledger, n)
    from services import ledger_store

    entries = ledger_store.find(ledger_store.ENTRIES, newest_first=False)  # key events and packs alike
    total = ledger.tree_size()

    @settings(max_examples=120, deadline=None)
    @given(index=st.integers(min_value=0, max_value=total - 1), data=st.data())
    def check(index, data):
        entry = entries[index]
        at = data.draw(st.integers(min_value=index + 1, max_value=total))
        proof = ledger.inclusion_proof(index, at)
        root = verifier.root_from_inclusion(bytes.fromhex(entry["leaf_hash"]), index, at,
                                            [bytes.fromhex(h) for h in proof["hashes"]])
        assert root.hex() == proof["root_hash"] == ledger.root_hash(at)
        first = data.draw(st.integers(min_value=1, max_value=at))
        cproof = ledger.consistency_proof(first, at)
        assert verifier.consistency_holds(
            first, at, bytes.fromhex(cproof["from_root"]), bytes.fromhex(cproof["to_root"]),
            [bytes.fromhex(h) for h in cproof["hashes"]])
        assert cproof["from_root"] == ledger.root_hash(first)

    check()


def test_a_tampered_receipt_fails_the_check_it_breaks(ledger):
    entries = [_append(ledger, n)[0] for n in range(6)]
    receipt = ledger.receipt(entries[2])
    keys = _keys(receipt)
    assert set(_core(receipt, keys).values()) >= {"passed"}

    def broken(mutate) -> dict[str, str]:
        forged = copy.deepcopy(receipt)
        mutate(forged)
        return _core(forged, keys)

    def flip(text: str) -> str:
        return ("0" if text[0] != "0" else "1") + text[1:]

    leaf = broken(lambda r: r["statement"].update(content_sha256=flip(r["statement"]["content_sha256"])))
    assert leaf["statement"] == "failed" and leaf["signatures"] == "failed" and leaf["inclusion"] == "failed"
    subject = broken(lambda r: r["statement"].update(subject="job-other"))
    assert subject["signatures"] == "failed"
    path = broken(lambda r: r["inclusion_proof"].__setitem__(0, flip(r["inclusion_proof"][0])))
    assert path["inclusion"] == "failed" and path["signatures"] == "passed"
    index = broken(lambda r: r.update(leaf_index=r["leaf_index"] + 1))
    assert index["inclusion"] == "failed"
    root = broken(lambda r: r["checkpoint"].update(root_hash=flip(r["checkpoint"]["root_hash"])))
    assert root["inclusion"] == "failed" and root["checkpoint_signature"] == "failed"
    size = broken(lambda r: r["checkpoint"].update(tree_size=r["checkpoint"]["tree_size"] + 1))
    assert size["checkpoint_signature"] == "failed"
    # A statement key cannot sign a checkpoint, nor a checkpoint key a statement.
    swapped = broken(lambda r: r["checkpoint"].update(signatures=r["signatures"],
                                                      signer_key_ids=r["statement"]["signer_key_ids"],
                                                      signer_algs=r["statement"]["signer_algs"]))
    assert swapped["checkpoint_signature"] == "failed"
    unsigned = broken(lambda r: r.update(signatures=[]))
    assert unsigned["signatures"] == "failed"
    # One signature of several removed, or its algorithm relabelled: failed, whatever else is intact.
    if len(receipt["signatures"]) > 1:
        dropped = broken(lambda r: r["signatures"].pop())
        assert dropped["signatures"] == "failed" and dropped["inclusion"] == "passed"
        relisted = broken(lambda r: (r["signatures"].pop(), r["statement"].update(
            signer_key_ids=r["statement"]["signer_key_ids"][:1], signer_algs=r["statement"]["signer_algs"][:1])))
        assert relisted["signatures"] == "failed"  # the list is inside the signed statement and the leaf
        assert relisted["statement"] == "failed" and relisted["inclusion"] == "failed"
    renamed = broken(lambda r: r["signatures"][0].update(alg="RSA"))
    assert renamed["signatures"] == "failed"
    # A key that is not trusted is "not checked", never "passed".
    untrusted = verifier.verify_evidence(None, receipt, verifier.KeySet())
    statuses = {c["id"]: c["status"] for c in untrusted["checks"]}
    assert statuses["signatures"] == statuses["checkpoint_signature"] == "not_checked"
    assert untrusted["verdict"] == "incomplete"


def test_the_self_check_catches_a_rewritten_leaf_and_a_removed_entry(ledger, tmp_path):
    base = _warm()
    for n in range(5):
        _append(ledger, n)
        ledger.checkpoint()
    assert ledger.audit_log() == {"ok": True, "tree_size": base + 5, "checkpoints_checked": 5, "problems": []}
    path = tmp_path / "data" / "evidence_ledger.json"
    original = path.read_text()
    target = str(base + 2)

    data = json.loads(original)
    data["evidence_ledger_entries"][target]["statement"]["content_sha256"] = "f" * 64
    path.write_text(json.dumps(data))
    report = ledger.audit_log()
    assert report["ok"] is False
    assert {"kind": "leaf_mismatch", "index": base + 2,
            "detail": "the stored statement does not hash to the stored leaf hash"} in report["problems"]
    # Every checkpoint that covers the rewritten leaf no longer matches; the earlier ones still do.
    assert [p["index"] for p in report["problems"] if p["kind"] == "checkpoint_root_mismatch"] == [
        base + 3, base + 4, base + 5]

    # A careful forger also rewrites the leaf hash: the signed checkpoints still give it away.
    statement = data["evidence_ledger_entries"][target]["statement"]
    data["evidence_ledger_entries"][target]["leaf_hash"] = verifier.leaf_hash(
        verifier.statement_bytes(statement)).hex()
    path.write_text(json.dumps(data))
    report = ledger.audit_log()
    assert [p["kind"] for p in report["problems"]] == ["checkpoint_root_mismatch"] * 3

    data = json.loads(original)
    del data["evidence_ledger_entries"][str(base + 1)]
    path.write_text(json.dumps(data))
    assert ledger.audit_log()["problems"][0]["kind"] == "missing_entry"

    data = json.loads(original)
    data["evidence_ledger_checkpoints"][str(base + 4)]["checkpoint"]["timestamp"] = "2020-01-01T00:00:00+00:00"
    path.write_text(json.dumps(data))
    assert [p["kind"] for p in ledger.audit_log()["problems"]] == ["checkpoint_signature_invalid"]


def test_a_client_holding_an_older_checkpoint_detects_a_rewritten_log(ledger, tmp_path):
    base = _warm()
    entries = [_append(ledger, n)[0] for n in range(6)]
    held = ledger.checkpoint()["checkpoint"]  # the auditor saved this at size base + 6
    honest = _append(ledger, 6)[0]
    receipt = ledger.receipt(honest)
    proof = ledger.consistency_proof(base + 6, base + 7)
    assert verifier.check_consistency(held, proof, receipt["checkpoint"])["status"] == "passed"

    # The operator rewrites leaf 3 (statement and leaf hash), drops the checkpoints that would
    # give it away, and issues new, validly signed ones over the rewritten history.
    path = tmp_path / "data" / "evidence_ledger.json"
    data = json.loads(path.read_text())
    target = data["evidence_ledger_entries"][str(base + 3)]
    target["statement"]["content_sha256"] = "e" * 64
    target["leaf_hash"] = verifier.leaf_hash(verifier.statement_bytes(target["statement"])).hex()
    data["evidence_ledger_checkpoints"] = {}
    path.write_text(json.dumps(data))
    ledger.reset_cache()
    _append(ledger, 7)
    forged_receipt = ledger.receipt(ledger.get_entry(base + 7))
    assert ledger.audit_log()["ok"] is True  # internally consistent again: the service cannot tell
    core = _core(forged_receipt)
    assert core["inclusion"] == core["checkpoint_signature"] == core["signatures"] == "passed"
    forged_proof = ledger.consistency_proof(base + 6, base + 8)
    verdict = verifier.check_consistency(held, forged_proof, forged_receipt["checkpoint"])
    assert verdict["status"] == "failed" and "rewritten" in verdict["detail"]
    # End to end through the verifier: the held checkpoint turns the verdict to failed.
    result = verifier.verify_evidence(None, forged_receipt, _keys(forged_receipt), previous_checkpoint=held,
                                      consistency_proof=forged_proof)
    assert result["verdict"] == "failed"
    assert {c["id"]: c["status"] for c in result["checks"]}["consistency"] == "failed"
    assert entries[3]["leaf_hash"] != target["leaf_hash"]


def _hammer(log, threads: int, each: int, workspace: str) -> dict[int, list[tuple[int, str]]]:
    """``threads`` appenders, ``each`` distinct packs apiece; returns what each one was told."""
    base = _warm(workspace)  # the keyrings (and their key events) exist before the race
    barrier = threading.Barrier(threads)
    told: dict[int, list[tuple[int, str]]] = {t: [] for t in range(threads)}
    errors: list[BaseException] = []

    def work(t: int) -> None:
        try:
            barrier.wait()
            for n in range(each):
                entry, created = _append(log, t * 1000 + n, workspace=workspace)
                assert created
                told[t].append((entry["index"], entry["leaf_hash"]))
                log.checkpoint(entry["index"] + 1)
        except BaseException as exc:  # noqa: BLE001 - surfaced by the assertion below
            errors.append(exc)

    workers = [threading.Thread(target=work, args=(t,)) for t in range(threads)]
    for w in workers:
        w.start()
    for w in workers:
        w.join()
    assert not errors, errors
    told[-1] = [(base, "")]  # where the race started
    return told


def _assert_one_history(log, told: dict[int, list[tuple[int, str]]], total: int) -> None:
    from services import ledger_store
    from services.ledger_merkle import Tree

    base = told.pop(-1)[0][0]
    claimed = [pair for pairs in told.values() for pair in pairs]
    assert sorted(i for i, _h in claimed) == list(range(base, base + total)), "an index was shared or skipped"
    stored = ledger_store.leaf_hashes(0)
    assert len(stored) == base + total
    for index, leaf in claimed:
        assert stored[index] == leaf, f"entry {index} is not the leaf its appender wrote"
    tree = Tree(bytes.fromhex(h) for h in stored)
    records = ledger_store.find(ledger_store.CHECKPOINTS, newest_first=False)
    assert [r["tree_size"] for r in records] == sorted({i + 1 for i, _h in claimed})
    for record in records:  # one root per size, and it is the root of the leaves below it
        assert record["checkpoint"]["root_hash"] == tree.root(record["tree_size"]).hex()
    for older, newer in zip(records, records[1:]):
        proof = log.consistency_proof(older["tree_size"], newer["tree_size"])
        assert verifier.check_consistency(older["checkpoint"], proof, newer["checkpoint"])["status"] == "passed"
    assert log.audit_log()["ok"] is True


def test_concurrent_appenders_never_share_an_index_or_fork_the_log(ledger):
    told = _hammer(ledger, threads=4, each=6, workspace="w1")
    _assert_one_history(ledger, told, 24)


@pytest.fixture()
def mongo_ledger(tmp_path, monkeypatch):
    """The ledger on a live MongoDB, in a throwaway database; skipped when none is reachable."""
    pymongo = pytest.importorskip("pymongo")
    from services import ledger_log, ledger_store

    name = f"e1_ledger_test_{uuid.uuid4().hex[:10]}"
    probe = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=1500)
    try:
        probe.admin.command("ping")
    except Exception:  # noqa: BLE001 - no live MongoDB on this machine
        pytest.skip("no MongoDB on localhost:27017")
    # One client per thread: each appender talks to the store like a separate replica would.
    local = threading.local()
    clients: list = []

    def db():
        if not hasattr(local, "client"):
            local.client = pymongo.MongoClient("mongodb://localhost:27017", serverSelectionTimeoutMS=3000)
            clients.append(local.client)
        return local.client[name]

    monkeypatch.setenv("DATAFLOW_DATA_DIR", str(tmp_path))
    monkeypatch.setenv("DATAFLOW_AUDIT_ANCHOR_STORE", str(tmp_path / "anchors.jsonl"))
    monkeypatch.delenv("DATAFLOW_AUDIT_ANCHOR_PROVIDER", raising=False)
    monkeypatch.setattr(ledger_store, "_db", db)
    ledger_log.reset_cache()
    try:
        yield ledger_log
    finally:
        probe.drop_database(name)
        for client in clients:
            client.close()
        probe.close()
        ledger_store._indexed.clear()
        ledger_log.reset_cache()


def test_live_mongo_concurrent_replicas_never_fork_the_log(mongo_ledger):
    told = _hammer(mongo_ledger, threads=4, each=5, workspace="w1")
    first = list(told[0])
    _assert_one_history(mongo_ledger, told, 20)
    # Idempotent across replicas too: the same pack again is the same entry.
    entry, created = _append(mongo_ledger, 0)
    assert created is False and (entry["index"], entry["leaf_hash"]) in first


def test_live_mongo_store_refuses_a_second_leaf_at_an_index_and_a_second_checkpoint(mongo_ledger):
    from services import ledger_store

    base = _warm()
    first, _ = _append(mongo_ledger, 1)
    assert first["index"] == base
    assert ledger_store.put_if_absent(ledger_store.ENTRIES, first["index"], {"leaf_hash": "0" * 64}) is False
    assert ledger_store.get(ledger_store.ENTRIES, first["index"])["leaf_hash"] == first["leaf_hash"]
    record = mongo_ledger.checkpoint()
    size = base + 1
    assert ledger_store.put_if_absent(ledger_store.CHECKPOINTS, size, {"tree_size": size, "checkpoint": {}}) is False
    assert mongo_ledger.checkpoint(size)["checkpoint"] == record["checkpoint"]
    page = mongo_ledger.list_entries(allows=lambda ws: ws == "w1", kind="proof_pack")
    assert [e["index"] for e in page["entries"]] == [base] and page["tree_size"] == size
    # The workspace's key event is one of its entries; so is the log's own (public checkpoint keys).
    mine = mongo_ledger.list_entries(allows=lambda ws: ws == "w1")
    assert [(e["kind"], e["subject"]) for e in mine["entries"]] == [
        ("proof_pack", "job-1"), ("key_set", "key_set:checkpoint"), ("key_set", "key_set:statement")]


def test_listing_is_scoped_paged_and_marks_withdrawn_entries(ledger):
    from services import ledger_keys

    base = _warm("w1", "w2")
    for n in range(7):
        _append(ledger, n, workspace="w1" if n % 2 == 0 else "w2")
    mine = ledger.list_entries(allows=lambda ws: ws == "w1", limit=3, kind="proof_pack")
    assert [e["index"] for e in mine["entries"]] == [base + 6, base + 4, base + 2] and mine["next_before"] == base + 2
    rest = ledger.list_entries(allows=lambda ws: ws == "w1", limit=3, before=mine["next_before"], kind="proof_pack")
    assert [e["index"] for e in rest["entries"]] == [base] and rest["next_before"] is None
    assert {e["workspace_id"] for e in mine["entries"] + rest["entries"]} == {"w1"}
    assert set(mine["entries"][0]) == {"index", "leaf_hash", "kind", "subject", "content_sha256", "workspace_id",
                                      "issued_at", "issued_by", "signature_scheme", "signer_key_ids", "status",
                                      "withdrawal"}
    assert mine["entries"][0]["status"] == "issued" and mine["tree_size"] == base + 7
    # Without a kind filter the workspace's own key event and the log's are listed too; w2's is not.
    everything = ledger.list_entries(allows=lambda ws: ws == "w1", limit=50)
    assert [(e["kind"], e["subject"], e["workspace_id"]) for e in everything["entries"]] == [
        ("proof_pack", f"job-{n}", "w1") for n in (6, 4, 2, 0)] + [
        ("key_set", "key_set:checkpoint", ""), ("key_set", "key_set:statement", "w1")]
    assert everything["entries"][-1]["status"] == "issued"
    signer = ledger_keys.signer(ledger_keys.scope_for("w1"))
    ledger.append(kind="withdrawal", subject="job-4", content_sha256=_digest(4), workspace_id="w1", actor="a",
                  signer=signer, detail={"withdrawal": {"withdrawn_kind": "proof_pack", "reason": "wrong run",
                                                        "superseded_by": None}})
    page = ledger.list_entries(allows=lambda ws: ws == "w1", limit=10)
    by_index = {e["index"]: e for e in page["entries"]}
    assert by_index[base + 4]["status"] == "withdrawn" and by_index[base + 4]["withdrawal"]["reason"] == "wrong run"
    assert by_index[base + 4]["withdrawal"]["leaf_index"] == base + 7 and by_index[base + 7]["status"] == "withdrawal"
    assert by_index[base + 6]["status"] == "issued" and by_index[base + 6]["withdrawal"] is None
    only = ledger.list_entries(allows=lambda ws: True, kind="withdrawal")
    assert [e["index"] for e in only["entries"]] == [base + 7]


def test_a_receipt_carries_the_key_events_so_a_pinned_log_key_is_enough(ledger):
    """The trust root: the verifier pins the log's key id; everything else is checked against the log."""
    _warm()
    entry = _append(ledger, 1)[0]
    receipt = ledger.receipt(entry)
    statement, checkpoint = receipt["statement"], receipt["checkpoint"]
    assert len(receipt["key_sets"]) == 1
    event = receipt["key_sets"][0]
    assert event["statement"]["kind"] == "key_set" and event["leaf_index"] < checkpoint["tree_size"]
    assert sorted(k["kid"] for k in event["statement"]["key_set"]["keys"]) == statement["signer_key_ids"]

    def checks(keys, mutate=None) -> dict[str, str]:
        doc = copy.deepcopy(receipt)
        if mutate:
            mutate(doc)
        return {c["id"]: c["status"] for c in verifier.verify_evidence(None, doc, keys)["checks"]}

    # Nothing pinned: nothing about the signatures is claimed, although the keys travel with the receipt.
    nothing = checks(verifier.KeySet())
    assert nothing["signatures"] == nothing["checkpoint_signature"] == "not_checked"
    # The log's key ids pinned (out of band): the checkpoint's own keys are taken from the checkpoint,
    # the workspace's keys from the key event the log holds.
    pinned = verifier.KeySet()
    for key_id in checkpoint["signer_key_ids"]:
        pinned.pin(key_id)
    result = verifier.verify_evidence(None, receipt, pinned)
    statuses = {c["id"]: c["status"] for c in result["checks"]}
    assert statuses["checkpoint_signature"] == statuses["signatures"] == statuses["inclusion"] == "passed"
    assert {s["trust"].split(" (")[0] for s in result["signatures"]} == {"the log published this key"}
    # One of two log keys pinned is not the checkpoint the log signed with both.
    if len(checkpoint["signer_key_ids"]) > 1:
        half = verifier.KeySet()
        half.pin(checkpoint["signer_key_ids"][0])
        assert checks(half)["checkpoint_signature"] == "not_checked"
        assert checks(half)["signatures"] == "not_checked"  # no trusted checkpoint, no trusted key event
    # Another key under a pinned id is not accepted: the id is recomputed from the key material.
    def swap_checkpoint_key(doc):
        other = event["statement"]["key_set"]["keys"][0]
        doc["checkpoint"]["keys"][0] = {**other, "kid": doc["checkpoint"]["keys"][0]["kid"]}

    assert checks(pinned, swap_checkpoint_key)["checkpoint_signature"] in {"failed", "not_checked"}
    # A key event that is not in the log under this checkpoint lends no trust.
    def forge_key_event(doc):
        doc["key_sets"][0]["statement"]["nonce"] = "0" * 32

    assert checks(pinned, forge_key_event)["signatures"] == "not_checked"
    assert checks(pinned, lambda doc: doc.pop("key_sets"))["signatures"] == "not_checked"
    # The workspace keys pinned directly work too, with the key material from the key event.
    direct = verifier.KeySet()
    for key_id in statement["signer_key_ids"]:
        direct.pin(key_id)
    partial = checks(direct)
    assert partial["signatures"] == "passed" and partial["checkpoint_signature"] == "not_checked"
    with pytest.raises(ValueError):
        verifier.KeySet().pin("not-a-key-id")
    # After a rotation, a new pack's receipt carries the rotation event; the earlier receipt still verifies.
    from services import ledger_keys

    ledger_keys.rotate("ws:w1", actor="admin")
    later = ledger.receipt(_append(ledger, 2)[0])
    assert later["key_sets"][0]["statement"]["key_set"]["event"] == "rotated"
    assert {c["id"]: c["status"] for c in verifier.verify_evidence(None, later, pinned)["checks"]}["signatures"] == "passed"
    again = ledger.receipt(entry)
    assert {c["id"]: c["status"] for c in verifier.verify_evidence(None, again, pinned)["checks"]}["signatures"] == "passed"
