"""The Evidence Ledger's log: append-only statements, signed checkpoints, proofs, receipts.

One log per deployment. A leaf is the canonical JSON of a signed *statement*
(``kind``, ``subject``, ``content_sha256``, ``signer_key_ids``, ``issued_at``,
a nonce); the tree is RFC 6962 (:mod:`services.ledger_merkle`). Appending is
insert-if-absent on the leaf index (:func:`services.ledger_store.append_entry`),
so two replicas cannot put two leaves at one position, and the root of any
size is a function of the leaves below it alone.

A **checkpoint** is the signed tree head of one size (one stored per size,
insert-if-absent; a second replica adopts the stored one). A **receipt** is a
statement, its signatures, its audit path and a checkpoint: everything an
auditor needs to check a pack without calling the service.

Nothing in the log is updated or deleted. A withdrawal is a new leaf.
"""

from __future__ import annotations

import logging
import secrets
import threading
from datetime import datetime, timezone
from typing import Any

from services import ledger_keys, ledger_store
from services.ledger_merkle import Tree
from tools import datawrap_verify as verifier

logger = logging.getLogger(__name__)

KINDS = (*verifier.PACK_KINDS, verifier.WITHDRAWAL)
ORIGIN_PREFIX = "datawrap-evidence-ledger/"
_lock = threading.RLock()
_trees: dict[str, Tree] = {}


class LedgerError(RuntimeError):
    """A ledger request that cannot be answered; ``status_code`` is the HTTP answer."""

    status_code = 400


class LedgerNotFound(LedgerError):
    status_code = 404


class LedgerConflict(LedgerError):
    status_code = 409


class LedgerInvalid(LedgerError):
    status_code = 422


class LedgerUnavailable(LedgerError):
    status_code = 503


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def reset_cache() -> None:
    """Forget the in-process tree (tests; a process rebuilds it from the store on next use)."""
    with _lock:
        _trees.clear()


def origin() -> str:
    """The log's name: fixed at first use, the same on every replica."""
    meta = ledger_store.get(ledger_store.META, "log")
    if not meta:
        ledger_store.put_if_absent(ledger_store.META, "log",
                                   {"log_id": secrets.token_hex(16), "created_at": now_iso()})
        meta = ledger_store.get(ledger_store.META, "log") or {}
    return ORIGIN_PREFIX + str(meta.get("log_id") or "")


def _tree() -> Tree:
    """The tree over every stored leaf. Call with ``_lock`` held."""
    namespace = ledger_store.namespace()
    tree = _trees.get(namespace)
    stored = ledger_store.tree_size()
    if tree is None or len(tree) > stored:
        tree = _trees[namespace] = Tree()
    if stored > len(tree):
        tree.extend(bytes.fromhex(h) for h in ledger_store.leaf_hashes(len(tree)))
    return tree


def tree_size() -> int:
    return ledger_store.tree_size()


def root_hash(size: int | None = None) -> str:
    with _lock:
        try:
            return _tree().root(size).hex()
        except ValueError as exc:
            raise LedgerInvalid(str(exc)) from exc


# ------------------------------------------------------------------- append


def append(*, kind: str, subject: str, content_sha256: str, workspace_id: str, actor: str,
           signer: ledger_keys.Signer, withdrawal: dict[str, Any] | None = None) -> tuple[dict[str, Any], bool]:
    """Sign a statement and append it. Returns ``(entry, created)``; idempotent on kind + digest."""
    if kind not in KINDS:
        raise LedgerInvalid(f"Unknown evidence kind {kind!r}.")
    statement: dict[str, Any] = {
        "type": verifier.STATEMENT_TYPE, "kind": kind, "subject": subject, "content_sha256": content_sha256,
        "signer_key_ids": signer.key_ids, "issued_at": now_iso(), "nonce": secrets.token_hex(16),
    }
    if withdrawal is not None:
        statement["withdrawal"] = withdrawal
    body = verifier.statement_bytes(statement)
    doc = {
        "dedupe": f"{kind}:{content_sha256}", "leaf_hash": verifier.leaf_hash(body).hex(), "kind": kind,
        "subject": subject, "content_sha256": content_sha256, "workspace_id": workspace_id or "",
        "issued_at": statement["issued_at"], "issued_by": actor or "system",
        "signature_scheme": signer.scheme, "signer_key_ids": signer.key_ids, "statement": statement,
        "signatures": signer.sign(verifier.STATEMENT_PAYLOAD, body),
    }
    try:
        return ledger_store.append_entry(doc)
    except ledger_store.StoreConflict as exc:
        raise LedgerUnavailable(str(exc)) from exc


def get_entry(index: int) -> dict[str, Any] | None:
    return ledger_store.get(ledger_store.ENTRIES, int(index))


def find_entry(content_sha256: str, kind: str = "") -> dict[str, Any] | None:
    """The entry of a pack by its digest (never a withdrawal unless ``kind`` asks for one)."""
    kinds = [kind] if kind else list(verifier.PACK_KINDS)
    for candidate in kinds:
        found = ledger_store.find_by_dedupe(f"{candidate}:{content_sha256}")
        if found:
            return found
    return None


def withdrawal_of(content_sha256: str) -> dict[str, Any] | None:
    return ledger_store.find_by_dedupe(f"{verifier.WITHDRAWAL}:{content_sha256}")


def _withdrawal_info(entry: dict[str, Any]) -> dict[str, Any]:
    detail = (entry.get("statement") or {}).get("withdrawal") or {}
    return {"leaf_index": entry.get("index"), "withdrawn_at": entry.get("issued_at"),
            "reason": detail.get("reason"), "superseded_by": detail.get("superseded_by")}


def public_entry(entry: dict[str, Any], withdrawn_by: dict[str, Any] | None = None) -> dict[str, Any]:
    """The Entry shape of the contract; ``status`` is computed, nothing is stored on the entry."""
    is_withdrawal = entry.get("kind") == verifier.WITHDRAWAL
    if not is_withdrawal and withdrawn_by is None:
        withdrawn_by = withdrawal_of(str(entry.get("content_sha256")))
    out = {k: entry.get(k) for k in ("index", "leaf_hash", "kind", "subject", "content_sha256", "workspace_id",
                                     "issued_at", "issued_by", "signature_scheme", "signer_key_ids")}
    out["status"] = "withdrawal" if is_withdrawal else ("withdrawn" if withdrawn_by else "issued")
    out["withdrawal"] = _withdrawal_info(withdrawn_by) if withdrawn_by and not is_withdrawal else None
    return out


def list_entries(*, allows: Any, limit: int = 50, before: int | None = None, kind: str = "",
                 subject: str = "") -> dict[str, Any]:
    """A page of entries the caller may see (``allows(workspace_id)``), newest first."""
    query: dict[str, Any] = {}
    if kind:
        query["kind"] = kind
    if subject:
        query["subject"] = subject
    page: list[dict[str, Any]] = []
    cursor, more, chunk = before, False, max(limit * 2, 50)
    while True:
        batch = ledger_store.find(ledger_store.ENTRIES, query, before=cursor, limit=chunk)
        for doc in batch:
            if len(page) == limit:
                more = True
                break
            cursor = int(doc["index"])
            if allows(doc.get("workspace_id") or None):
                page.append(doc)
        if more or len(batch) < chunk:
            break
    digests = [str(d.get("content_sha256")) for d in page if d.get("kind") != verifier.WITHDRAWAL]
    withdrawals = {str(w.get("content_sha256")): w for w in ledger_store.find(
        ledger_store.ENTRIES, {"kind": verifier.WITHDRAWAL, "content_sha256": digests})} if digests else {}
    return {"entries": [public_entry(d, withdrawals.get(str(d.get("content_sha256")))) for d in page],
            "next_before": cursor if more else None, "tree_size": tree_size()}


# --------------------------------------------------------------- checkpoints


def checkpoint(size: int | None = None, *, create: bool = True) -> dict[str, Any] | None:
    """The stored checkpoint record of ``size`` (default: the whole log), signing one if absent.

    A record is ``{"tree_size", "checkpoint", "checkpoint_sha256", "anchor"}``.
    ``None`` for an empty log, or when ``create`` is false and none is stored.
    """
    size = tree_size() if size is None else int(size)
    if size <= 0:
        return None
    stored = ledger_store.get(ledger_store.CHECKPOINTS, size)
    if stored or not create:
        return stored
    signer = ledger_keys.signer(ledger_keys.LOG_SCOPE)
    if signer is None:
        raise LedgerUnavailable("The log has no active checkpoint key.")
    body = {"type": verifier.CHECKPOINT_TYPE, "origin": origin(), "tree_size": size,
            "root_hash": root_hash(size), "timestamp": now_iso(), "signer_key_ids": signer.key_ids}
    signed = {**body, "signatures": signer.sign(verifier.CHECKPOINT_PAYLOAD, verifier.checkpoint_bytes(body))}
    record = {"tree_size": size, "checkpoint": signed, "checkpoint_sha256": verifier.checkpoint_sha256(signed),
              "anchor": None}
    if not ledger_store.put_if_absent(ledger_store.CHECKPOINTS, size, record):
        return ledger_store.get(ledger_store.CHECKPOINTS, size)  # another replica signed this size first
    from services.ledger_anchor import anchor_checkpoint

    return anchor_checkpoint(record)


def latest_checkpoint() -> dict[str, Any] | None:
    return ledger_store.latest_checkpoint()


# -------------------------------------------------------------------- proofs


def inclusion_proof(index: int, size: int | None = None) -> dict[str, Any]:
    with _lock:
        tree = _tree()
        n = len(tree) if size is None else int(size)
        try:
            hashes = tree.inclusion_proof(int(index), n)
            return {"leaf_index": int(index), "leaf_hash": tree.leaf(int(index)).hex(), "tree_size": n,
                    "root_hash": tree.root(n).hex(), "hashes": [h.hex() for h in hashes]}
        except ValueError as exc:
            raise LedgerInvalid(str(exc)) from exc


def consistency_proof(first: int, second: int) -> dict[str, Any]:
    first, second = int(first), int(second)
    if first < 1 or first > second:
        raise LedgerInvalid("A consistency proof needs 1 <= from <= to.")
    with _lock:
        tree = _tree()
        try:
            hashes = tree.consistency_proof(first, second)
            return {"from": first, "to": second, "from_root": tree.root(first).hex(),
                    "to_root": tree.root(second).hex(), "hashes": [h.hex() for h in hashes]}
        except ValueError as exc:
            raise LedgerInvalid(str(exc)) from exc


def receipt(entry: dict[str, Any], *, size: int | None = None) -> dict[str, Any]:
    """The receipt of ``entry`` against the checkpoint of ``size`` (default: the whole log now)."""
    record = checkpoint(size)
    if record is None or int(record["tree_size"]) <= int(entry["index"]):
        raise LedgerInvalid("That checkpoint does not cover this entry.")
    proof = inclusion_proof(int(entry["index"]), int(record["tree_size"]))
    return {
        "type": verifier.RECEIPT_TYPE, "statement": entry["statement"], "signatures": entry["signatures"],
        "signature_scheme": entry.get("signature_scheme"), "leaf_index": int(entry["index"]),
        "leaf_hash": entry["leaf_hash"], "inclusion_proof": proof["hashes"],
        "checkpoint": record["checkpoint"], "anchor": record.get("anchor"),
    }


# ---------------------------------------------------------------- self-check


def audit_log() -> dict[str, Any]:
    """Re-derive the whole log from the store and compare it with every stored checkpoint.

    Reads the store afresh (no cached tree). A leaf whose statement no longer
    hashes to its ``leaf_hash``, a missing index, a checkpoint whose root is
    not the root of the leaves below it, or a checkpoint signature that no
    longer verifies is reported; none of these can be produced by appending.
    """
    problems: list[dict[str, Any]] = []
    entries = ledger_store.find(ledger_store.ENTRIES, newest_first=False)
    leaves: list[bytes] = []
    for position, entry in enumerate(entries):
        if int(entry.get("index", -1)) != position:
            problems.append({"kind": "missing_entry", "index": position,
                             "detail": f"the log has no entry at index {position}"})
            break
        actual = verifier.leaf_hash(verifier.statement_bytes(entry.get("statement") or {}))
        if actual.hex() != entry.get("leaf_hash"):
            problems.append({"kind": "leaf_mismatch", "index": position,
                             "detail": "the stored statement does not hash to the stored leaf hash"})
        leaves.append(actual)
    tree = Tree(leaves)
    records = ledger_store.find(ledger_store.CHECKPOINTS, newest_first=False)
    for record in records:
        signed = record.get("checkpoint") or {}
        size = int(record.get("tree_size") or 0)
        if size > len(tree) or tree.root(size).hex() != signed.get("root_hash"):
            problems.append({"kind": "checkpoint_root_mismatch", "index": size,
                             "detail": f"the checkpoint of size {size} is not the root of the stored leaves"})
        keys = ledger_keys.trusted_keys(list(signed.get("signer_key_ids") or []))
        status, detail, _rows = verifier.check_signature_set(
            verifier.checkpoint_body(signed), signed.get("signatures"),
            message=verifier.pae(verifier.CHECKPOINT_PAYLOAD, verifier.checkpoint_bytes(signed)), keys=keys,
            role=verifier.ROLE_CHECKPOINT)
        if status != verifier.PASSED:
            problems.append({"kind": "checkpoint_signature_invalid", "index": size, "detail": detail})
    return {"ok": not problems, "tree_size": len(entries), "checkpoints_checked": len(records),
            "problems": problems}
