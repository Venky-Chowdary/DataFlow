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
    first, created = _append(ledger, 1)
    again, created_again = _append(ledger, 1)
    second, _ = _append(ledger, 2)
    assert (first["index"], created, created_again, second["index"]) == (0, True, False, 1)
    assert again["index"] == 0 and again["leaf_hash"] == first["leaf_hash"]
    assert ledger.tree_size() == 2
    statement = first["statement"]
    assert set(statement) == {"type", "kind", "subject", "content_sha256", "signer_key_ids", "issued_at", "nonce"}
    assert statement["type"] == "datawrap.evidence.statement.v1" and len(statement["nonce"]) == 32
    assert first["leaf_hash"] == verifier.leaf_hash(verifier.statement_bytes(statement)).hex()
    assert ledger.find_entry(_digest(2))["index"] == 1 and ledger.find_entry("0" * 64) is None
    with pytest.raises(ledger.LedgerInvalid):
        _append(ledger, 3, kind="certificate")


def test_a_checkpoint_is_signed_once_per_size_and_names_the_log(ledger):
    assert ledger.checkpoint() is None
    _append(ledger, 1)
    record = ledger.checkpoint()
    signed = record["checkpoint"]
    assert set(signed) == {"type", "origin", "tree_size", "root_hash", "timestamp", "signer_key_ids", "signatures"}
    assert signed["tree_size"] == 1 and signed["root_hash"] == ledger.root_hash(1)
    assert signed["origin"].startswith("datawrap-evidence-ledger/") and signed["origin"] == ledger.origin()
    assert record["checkpoint_sha256"] == verifier.checkpoint_sha256(signed)
    assert ledger.checkpoint() == ledger.checkpoint(1)  # stored, not re-signed
    assert ledger.checkpoint(1)["checkpoint"]["timestamp"] == signed["timestamp"]
    _append(ledger, 2)
    assert ledger.checkpoint()["tree_size"] == 2 and ledger.latest_checkpoint()["tree_size"] == 2
    assert ledger.checkpoint(1)["checkpoint"] == signed
    assert ledger.checkpoint(5, create=False) is None


def test_every_entry_has_a_receipt_that_verifies_at_every_later_checkpoint(ledger):
    entries = [_append(ledger, n)[0] for n in range(9)]
    for n in range(9):
        ledger.checkpoint(n + 1)
    for entry in entries:
        for size in range(entry["index"] + 1, 10):
            receipt = ledger.receipt(entry, size=size)
            checks = _core(receipt)
            assert checks["signatures"] == checks["inclusion"] == checks["checkpoint_signature"] == "passed", (
                entry["index"], size, checks)
            assert receipt["checkpoint"]["tree_size"] == size and receipt["type"] == "datawrap.evidence.receipt.v1"
    with pytest.raises(ledger.LedgerInvalid):
        ledger.receipt(entries[5], size=5)


def test_any_two_checkpoints_are_consistent(ledger):
    for n in range(11):
        _append(ledger, n)
        ledger.checkpoint()
    for second in range(1, 12):
        for first in range(1, second + 1):
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
        ledger.inclusion_proof(11)


def test_property_inclusion_and_consistency_over_a_stored_log(ledger):
    entries = [_append(ledger, n)[0] for n in range(37)]
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
    index = broken(lambda r: r.update(leaf_index=3))
    assert index["inclusion"] == "failed"
    root = broken(lambda r: r["checkpoint"].update(root_hash=flip(r["checkpoint"]["root_hash"])))
    assert root["inclusion"] == "failed" and root["checkpoint_signature"] == "failed"
    size = broken(lambda r: r["checkpoint"].update(tree_size=7))
    assert size["checkpoint_signature"] == "failed"
    # A statement key cannot sign a checkpoint, nor a checkpoint key a statement.
    swapped = broken(lambda r: r["checkpoint"].update(signatures=r["signatures"],
                                                      signer_key_ids=r["statement"]["signer_key_ids"]))
    assert swapped["checkpoint_signature"] == "failed"
    unsigned = broken(lambda r: r.update(signatures=[]))
    assert unsigned["signatures"] == "failed"
    # A key that is not trusted is "not checked", never "passed".
    untrusted = verifier.verify_evidence(None, receipt, verifier.KeySet())
    statuses = {c["id"]: c["status"] for c in untrusted["checks"]}
    assert statuses["signatures"] == statuses["checkpoint_signature"] == "not_checked"
    assert untrusted["verdict"] == "incomplete"


def test_the_self_check_catches_a_rewritten_leaf_and_a_removed_entry(ledger, tmp_path):
    for n in range(5):
        _append(ledger, n)
        ledger.checkpoint()
    assert ledger.audit_log() == {"ok": True, "tree_size": 5, "checkpoints_checked": 5, "problems": []}
    path = tmp_path / "data" / "evidence_ledger.json"
    original = path.read_text()

    data = json.loads(original)
    data["evidence_ledger_entries"]["2"]["statement"]["content_sha256"] = "f" * 64
    path.write_text(json.dumps(data))
    report = ledger.audit_log()
    assert report["ok"] is False
    assert {"kind": "leaf_mismatch", "index": 2,
            "detail": "the stored statement does not hash to the stored leaf hash"} in report["problems"]
    # Every checkpoint that covers the rewritten leaf no longer matches; the earlier ones still do.
    assert [p["index"] for p in report["problems"] if p["kind"] == "checkpoint_root_mismatch"] == [3, 4, 5]

    # A careful forger also rewrites the leaf hash: the signed checkpoints still give it away.
    statement = data["evidence_ledger_entries"]["2"]["statement"]
    data["evidence_ledger_entries"]["2"]["leaf_hash"] = verifier.leaf_hash(verifier.statement_bytes(statement)).hex()
    path.write_text(json.dumps(data))
    report = ledger.audit_log()
    assert [p["kind"] for p in report["problems"]] == ["checkpoint_root_mismatch"] * 3

    data = json.loads(original)
    del data["evidence_ledger_entries"]["1"]
    path.write_text(json.dumps(data))
    assert ledger.audit_log()["problems"][0]["kind"] == "missing_entry"

    data = json.loads(original)
    data["evidence_ledger_checkpoints"]["4"]["checkpoint"]["timestamp"] = "2020-01-01T00:00:00+00:00"
    path.write_text(json.dumps(data))
    assert [p["kind"] for p in ledger.audit_log()["problems"]] == ["checkpoint_signature_invalid"]


def test_a_client_holding_an_older_checkpoint_detects_a_rewritten_log(ledger, tmp_path):
    entries = [_append(ledger, n)[0] for n in range(6)]
    held = ledger.checkpoint()["checkpoint"]  # the auditor saved this at size 6
    honest = _append(ledger, 6)[0]
    receipt = ledger.receipt(honest)
    proof = ledger.consistency_proof(6, 7)
    assert verifier.check_consistency(held, proof, receipt["checkpoint"])["status"] == "passed"

    # The operator rewrites leaf 3 (statement and leaf hash), drops the checkpoints that would
    # give it away, and issues new, validly signed ones over the rewritten history.
    path = tmp_path / "data" / "evidence_ledger.json"
    data = json.loads(path.read_text())
    target = data["evidence_ledger_entries"]["3"]
    target["statement"]["content_sha256"] = "e" * 64
    target["leaf_hash"] = verifier.leaf_hash(verifier.statement_bytes(target["statement"])).hex()
    data["evidence_ledger_checkpoints"] = {}
    path.write_text(json.dumps(data))
    ledger.reset_cache()
    _append(ledger, 7)
    forged_receipt = ledger.receipt(ledger.get_entry(7))
    assert ledger.audit_log()["ok"] is True  # internally consistent again: the service cannot tell
    core = _core(forged_receipt)
    assert core["inclusion"] == core["checkpoint_signature"] == core["signatures"] == "passed"
    forged_proof = ledger.consistency_proof(6, 8)
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
    from services import ledger_keys

    ledger_keys.signer(ledger_keys.scope_for(workspace))  # one keyring before the race
    ledger_keys.signer(ledger_keys.LOG_SCOPE)
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
    return told


def _assert_one_history(log, told: dict[int, list[tuple[int, str]]], total: int) -> None:
    from services import ledger_store
    from services.ledger_merkle import Tree

    claimed = [pair for pairs in told.values() for pair in pairs]
    assert sorted(i for i, _h in claimed) == list(range(total)), "an index was shared or skipped"
    stored = ledger_store.leaf_hashes(0)
    assert len(stored) == total
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
    _assert_one_history(mongo_ledger, told, 20)
    # Idempotent across replicas too: the same pack again is the same entry.
    entry, created = _append(mongo_ledger, 0)
    assert created is False and (entry["index"], entry["leaf_hash"]) in told[0]


def test_live_mongo_store_refuses_a_second_leaf_at_an_index_and_a_second_checkpoint(mongo_ledger):
    from services import ledger_store

    first, _ = _append(mongo_ledger, 1)
    assert ledger_store.put_if_absent(ledger_store.ENTRIES, first["index"], {"leaf_hash": "0" * 64}) is False
    assert ledger_store.get(ledger_store.ENTRIES, first["index"])["leaf_hash"] == first["leaf_hash"]
    record = mongo_ledger.checkpoint()
    assert ledger_store.put_if_absent(ledger_store.CHECKPOINTS, 1, {"tree_size": 1, "checkpoint": {}}) is False
    assert mongo_ledger.checkpoint(1)["checkpoint"] == record["checkpoint"]
    page = mongo_ledger.list_entries(allows=lambda ws: ws == "w1")
    assert [e["index"] for e in page["entries"]] == [0] and page["tree_size"] == 1


def test_listing_is_scoped_paged_and_marks_withdrawn_entries(ledger):
    from services import ledger_keys

    for n in range(7):
        _append(ledger, n, workspace="w1" if n % 2 == 0 else "w2")
    mine = ledger.list_entries(allows=lambda ws: ws == "w1", limit=3)
    assert [e["index"] for e in mine["entries"]] == [6, 4, 2] and mine["next_before"] == 2
    rest = ledger.list_entries(allows=lambda ws: ws == "w1", limit=3, before=mine["next_before"])
    assert [e["index"] for e in rest["entries"]] == [0] and rest["next_before"] is None
    assert {e["workspace_id"] for e in mine["entries"] + rest["entries"]} == {"w1"}
    assert set(mine["entries"][0]) == {"index", "leaf_hash", "kind", "subject", "content_sha256", "workspace_id",
                                      "issued_at", "issued_by", "signature_scheme", "signer_key_ids", "status",
                                      "withdrawal"}
    assert mine["entries"][0]["status"] == "issued" and mine["tree_size"] == 7
    signer = ledger_keys.signer(ledger_keys.scope_for("w1"))
    ledger.append(kind="withdrawal", subject="job-4", content_sha256=_digest(4), workspace_id="w1", actor="a",
                  signer=signer, withdrawal={"withdrawn_kind": "proof_pack", "reason": "wrong run",
                                             "superseded_by": None})
    page = ledger.list_entries(allows=lambda ws: ws == "w1", limit=10)
    by_index = {e["index"]: e for e in page["entries"]}
    assert by_index[4]["status"] == "withdrawn" and by_index[4]["withdrawal"]["reason"] == "wrong run"
    assert by_index[4]["withdrawal"]["leaf_index"] == 7 and by_index[7]["status"] == "withdrawal"
    assert by_index[6]["status"] == "issued" and by_index[6]["withdrawal"] is None
    only = ledger.list_entries(allows=lambda ws: True, kind="withdrawal")
    assert [e["index"] for e in only["entries"]] == [7]
