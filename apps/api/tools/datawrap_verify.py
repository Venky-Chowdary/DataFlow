#!/usr/bin/env python3
"""datawrap-verify: check a Datawrap evidence pack offline, without a secret.

One self-contained file: the Python standard library and the ``cryptography``
package, nothing else. Give it an exported pack, its ledger receipt (embedded
in the pack as ``ledger`` or as a separate file) and the public keys, and it
says which checks pass:

* content digest   - the pack is byte-for-byte the content the statement names
* statement        - the signed statement is about this pack (kind and subject)
* signatures       - every listed signer key signed the statement
* inclusion        - the statement is in the log at the receipt's checkpoint
* checkpoint signature - the log's keys signed that checkpoint
* consistency      - the log only grew since a checkpoint you saved earlier
* withdrawal       - the pack was not withdrawn later (needs the log, or a
                     withdrawal receipt)
* anchor           - the checkpoint sits in a write-once store (needs the service)

Every check is reported as passed, failed or not checked. The verdict is
"verified" only when every check passed; a check that could not run makes it
"incomplete", never "verified".

This shows that a record was issued, has not been altered and is in the log.
It does not show that the recorded facts are true.

The Datawrap service imports this same file for its own verification, so the
service and this tool give the same answer on the same input.

Exit status: 0 verified, 1 failed, 2 incomplete or HMAC only, 3 withdrawn,
64 unusable input.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
from typing import Any

STATEMENT_TYPE = "datawrap.evidence.statement.v1"
CHECKPOINT_TYPE = "datawrap.evidence.checkpoint.v1"
RECEIPT_TYPE = "datawrap.evidence.receipt.v1"
STATEMENT_PAYLOAD = "application/vnd.datawrap.evidence.statement+json"
CHECKPOINT_PAYLOAD = "application/vnd.datawrap.evidence.checkpoint+json"

#: Keys of a pack that are outside its content hash.
ENVELOPE_KEYS = ("content_sha256", "signature", "ledger")
PACK_KINDS = ("proof_pack", "parity_report", "exit_evidence")
WITHDRAWAL = "withdrawal"

ED25519 = "Ed25519"
ML_DSA_65 = "ML-DSA-65"
ROLE_STATEMENT = "statement"
ROLE_CHECKPOINT = "checkpoint"

PASSED, FAILED, NOT_CHECKED = "passed", "failed", "not_checked"
CHECK_IDS = ("content_digest", "statement", "signatures", "inclusion", "checkpoint_signature",
             "consistency", "withdrawal", "anchor")
LABELS = {
    "content_digest": "content digest", "statement": "statement", "signatures": "signatures",
    "inclusion": "inclusion in the log", "checkpoint_signature": "checkpoint signature",
    "consistency": "consistency", "withdrawal": "withdrawal", "anchor": "anchor",
}
EXIT_CODES = {"verified": 0, "failed": 1, "incomplete": 2, "hmac_only": 2, "withdrawn": 3}
SCOPE_NOTE = ("This shows that the record was issued, has not been altered and is in the log. "
              "It does not show that the recorded facts are true.")


# ------------------------------------------------------------ canonical bytes


def _canonical_numbers(value: Any) -> Any:
    """Fold an integral float to the integer it denotes (a JSON round trip does the same)."""
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {k: _canonical_numbers(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_numbers(v) for v in value]
    return value


def canonical_json(payload: Any) -> str:
    """Stable JSON: sorted keys, no insignificant whitespace, ASCII escapes."""
    return json.dumps(_canonical_numbers(payload), sort_keys=True, separators=(",", ":"))


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def pack_content_sha256(pack: dict[str, Any]) -> str:
    body = {k: v for k, v in pack.items() if k not in ENVELOPE_KEYS}
    return sha256_hex(canonical_json(body).encode("utf-8"))


def pack_kind(pack: dict[str, Any]) -> str:
    return str(pack.get("kind")) if pack.get("kind") in ("parity_report", "exit_evidence") else "proof_pack"


def pack_subject(pack: dict[str, Any]) -> str:
    """What a pack's signatures are bound to, domain-separated by its kind."""
    if pack.get("kind") == "exit_evidence":
        return f"exit_evidence:{pack.get('system_id') or ''}:{pack.get('rehearsal_id') or ''}"
    job_id = str(pack.get("job_id") or "")
    return f"parity_report:{job_id}" if pack.get("kind") == "parity_report" else job_id


def pae(payload_type: str, body: bytes) -> bytes:
    """DSSE pre-authentication encoding: the bytes a signature covers."""
    kind = payload_type.encode("utf-8")
    return b"DSSEv1 %d %s %d %s" % (len(kind), kind, len(body), body)


def statement_bytes(statement: dict[str, Any]) -> bytes:
    return canonical_json(statement).encode("utf-8")


def checkpoint_body(checkpoint: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in checkpoint.items() if k != "signatures"}


def checkpoint_bytes(checkpoint: dict[str, Any]) -> bytes:
    return canonical_json(checkpoint_body(checkpoint)).encode("utf-8")


def checkpoint_sha256(checkpoint: dict[str, Any]) -> str:
    return sha256_hex(checkpoint_bytes(checkpoint))


# ------------------------------------------------------- RFC 6962 Merkle tree

EMPTY_ROOT = hashlib.sha256(b"").digest()


def leaf_hash(data: bytes) -> bytes:
    return hashlib.sha256(b"\x00" + data).digest()


def node_hash(left: bytes, right: bytes) -> bytes:
    return hashlib.sha256(b"\x01" + left + right).digest()


def root_from_inclusion(leaf: bytes, index: int, size: int, proof: list[bytes]) -> bytes:
    """The root an audit path leads to (RFC 9162 2.1.3.2). ``ValueError`` for an impossible path."""
    if index < 0 or index >= size:
        raise ValueError("the leaf index is outside the tree")
    fn, sn, node = index, size - 1, leaf
    for sibling in proof:
        if sn == 0:
            raise ValueError("the proof is longer than the tree is deep")
        if fn & 1 or fn == sn:
            node = node_hash(sibling, node)
            if not fn & 1:
                while fn and not fn & 1:
                    fn >>= 1
                    sn >>= 1
        else:
            node = node_hash(node, sibling)
        fn >>= 1
        sn >>= 1
    if sn != 0:
        raise ValueError("the proof is shorter than the tree is deep")
    return node


def consistency_holds(first: int, second: int, first_root: bytes, second_root: bytes,
                      proof: list[bytes]) -> bool:
    """Whether a tree of ``second`` leaves extends the tree of ``first`` leaves (RFC 9162 2.1.4.2)."""
    if first < 0 or first > second:
        return False
    if first == second:
        return not proof and first_root == second_root
    if first == 0:
        return not proof and first_root == EMPTY_ROOT
    path = list(proof)
    if first & (first - 1) == 0:
        path.insert(0, first_root)
    if not path:
        return False
    fn, sn = first - 1, second - 1
    while fn & 1:
        fn >>= 1
        sn >>= 1
    fr = sr = path[0]
    for sibling in path[1:]:
        if sn == 0:
            return False
        if fn & 1 or fn == sn:
            fr = node_hash(sibling, fr)
            sr = node_hash(sibling, sr)
            if not fn & 1:
                while fn and not fn & 1:
                    fn >>= 1
                    sn >>= 1
        else:
            sr = node_hash(sr, sibling)
        fn >>= 1
        sn >>= 1
    return sn == 0 and fr == first_root and sr == second_root


def _hex_list(values: Any) -> list[bytes]:
    if not isinstance(values, list):
        raise ValueError("a proof must be a list of hex hashes")
    out = []
    for value in values:
        raw = bytes.fromhex(str(value))
        if len(raw) != 32:
            raise ValueError("a proof hash must be 32 bytes")
        out.append(raw)
    return out


# ----------------------------------------------------------------------- keys


class Key:
    """A trusted public key. ``role`` empty means it may sign statements and checkpoints."""

    __slots__ = ("key_id", "alg", "public", "role", "status")

    def __init__(self, key_id: str, alg: str, public: Any, role: str = "", status: str = "") -> None:
        self.key_id, self.alg, self.public, self.role, self.status = key_id, alg, public, role, status


def _b64url(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def _mldsa() -> Any:
    try:
        from cryptography.hazmat.primitives.asymmetric import mldsa
    except ImportError:
        return None
    return mldsa if hasattr(mldsa, "MLDSA65PublicKey") else None


def ml_dsa_available() -> bool:
    return _mldsa() is not None


def alg_of(public: Any) -> str:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    if isinstance(public, ed25519.Ed25519PublicKey):
        return ED25519
    mldsa = _mldsa()
    if mldsa is not None and isinstance(public, mldsa.MLDSA65PublicKey):
        return ML_DSA_65
    raise ValueError("unsupported public key type (expected Ed25519 or ML-DSA-65)")


def key_id_of(public: Any) -> str:
    """Key id: SHA-256 of the DER SubjectPublicKeyInfo."""
    from cryptography.hazmat.primitives import serialization

    return sha256_hex(public.public_bytes(serialization.Encoding.DER,
                                          serialization.PublicFormat.SubjectPublicKeyInfo))


def public_from_raw(alg: str, raw: bytes) -> Any:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    if alg == ED25519:
        return ed25519.Ed25519PublicKey.from_public_bytes(raw)
    if alg == ML_DSA_65:
        mldsa = _mldsa()
        if mldsa is None:
            raise NotImplementedError("this runtime has no ML-DSA implementation")
        return mldsa.MLDSA65PublicKey.from_public_bytes(raw)
    raise ValueError(f"unsupported signature algorithm {alg!r}")


class KeySet:
    """Trusted keys by key id. Keys of an algorithm this runtime lacks are remembered as unusable."""

    def __init__(self) -> None:
        self._keys: dict[str, Key] = {}
        self.unusable: dict[str, str] = {}

    def add(self, key: Key) -> None:
        self._keys[key.key_id] = key

    def get(self, key_id: str) -> Key | None:
        return self._keys.get(key_id)

    def __len__(self) -> int:
        return len(self._keys)

    def add_jwk(self, jwk: dict[str, Any]) -> None:
        kid = str(jwk.get("kid") or "")
        if jwk.get("kty") == "OKP" and jwk.get("crv") == ED25519:
            alg, raw = ED25519, _b64url(str(jwk.get("x") or ""))
        elif jwk.get("kty") == "AKP" and jwk.get("alg") == ML_DSA_65:
            alg, raw = ML_DSA_65, _b64url(str(jwk.get("pub") or ""))
        else:
            raise ValueError("unsupported JWK (expected OKP/Ed25519 or AKP/ML-DSA-65)")
        try:
            public = public_from_raw(alg, raw)
        except NotImplementedError as exc:
            if kid:
                self.unusable[kid] = str(exc)
            return
        key_id = key_id_of(public)
        if kid and kid != key_id:
            raise ValueError(f"JWK kid {kid[:16]} is not the SHA-256 of its public key")
        self.add(Key(key_id, alg, public, str(jwk.get("datawrap_role") or ""),
                     str(jwk.get("datawrap_status") or "")))

    def add_pem(self, text: str) -> None:
        from cryptography.hazmat.primitives import serialization

        begin, end = "-----BEGIN PUBLIC KEY-----", "-----END PUBLIC KEY-----"
        blocks = [begin + part.split(end)[0] + end for part in text.split(begin)[1:] if end in part]
        if not blocks:
            raise ValueError("no PEM public key found")
        for block in blocks:
            public = serialization.load_pem_public_key(block.encode("ascii"))
            self.add(Key(key_id_of(public), alg_of(public), public))

    def add_text(self, text: str) -> None:
        """A JWK Set, one JWK, or PEM public keys."""
        if "-----BEGIN" in text:
            self.add_pem(text)
            return
        doc = json.loads(text)
        jwks = doc.get("keys") if isinstance(doc, dict) and isinstance(doc.get("keys"), list) else [doc]
        for jwk in jwks:
            if not isinstance(jwk, dict):
                raise ValueError("a JWK must be an object")
            self.add_jwk(jwk)


def _verify_signature(key: Key, signature: bytes, message: bytes) -> bool:
    from cryptography.exceptions import InvalidSignature

    try:
        key.public.verify(signature, message)
    except InvalidSignature:
        return False
    return True


# --------------------------------------------------------------------- checks


def _short(key_id: str) -> str:
    return str(key_id)[:16]


def check_signature_set(body: dict[str, Any], signatures: Any, *, message: bytes, keys: KeySet,
                        role: str) -> tuple[str, str, list[dict[str, Any]]]:
    """Every listed signer key must have one verifying signature. Returns (status, detail, per-signature)."""
    signer_ids = body.get("signer_key_ids")
    if (not isinstance(signer_ids, list) or not signer_ids or len(set(signer_ids)) != len(signer_ids)
            or not all(isinstance(k, str) and k for k in signer_ids)):
        return FAILED, "signer_key_ids is missing or malformed", []
    signatures = signatures if isinstance(signatures, list) else []
    by_key: dict[str, list[dict[str, Any]]] = {}
    for sig in signatures:
        if not isinstance(sig, dict):
            return FAILED, "a signature is malformed", []
        by_key.setdefault(str(sig.get("key_id") or ""), []).append(sig)
    unlisted = sorted(set(by_key) - set(signer_ids))
    if unlisted:
        return FAILED, f"a signature is from a key the signed content does not list ({_short(unlisted[0])})", []
    results: list[dict[str, Any]] = []
    for key_id in signer_ids:
        found = by_key.get(key_id) or []
        row: dict[str, Any] = {"alg": None, "key_id": key_id, "status": FAILED, "key_status": None, "detail": ""}
        results.append(row)
        if len(found) != 1:
            row["detail"] = ("no signature for a listed signer key" if not found
                             else "more than one signature for one key")
            continue
        sig = found[0]
        row["alg"] = sig.get("alg")
        key = keys.get(key_id)
        if key is None:
            row["status"] = NOT_CHECKED
            row["detail"] = keys.unusable.get(key_id) or "the key is not in the trusted key set"
            continue
        row["key_status"] = key.status or None
        if key.role and key.role != role:
            row["detail"] = f"the key is a {key.role} key, not a {role} key"
            continue
        if sig.get("alg") != key.alg:
            row["detail"] = "the signature names a different algorithm than its key"
            continue
        try:
            raw = base64.b64decode(str(sig.get("value") or ""), validate=True)
        except ValueError:
            row["detail"] = "the signature value is not base64"
            continue
        if _verify_signature(key, raw, message):
            row["status"] = PASSED
            row["detail"] = "valid" + (" (the key is retired; it still verifies)" if key.status == "retired" else "")
        else:
            row["detail"] = "the signature does not verify"
    failed = [r for r in results if r["status"] == FAILED]
    skipped = [r for r in results if r["status"] == NOT_CHECKED]
    if failed:
        return FAILED, f"{failed[0]['alg'] or 'signature'} {_short(failed[0]['key_id'])}: {failed[0]['detail']}", results
    if skipped:
        return NOT_CHECKED, (f"{len(skipped)} of {len(results)} signature(s) not checked: "
                             f"{skipped[0]['detail']} ({_short(skipped[0]['key_id'])})"), results
    algs = " and ".join(str(r["alg"]) for r in results)
    return PASSED, f"{len(results)} signature(s) valid ({algs})", results


def signature_scheme(signatures: Any) -> str:
    algs = {str(s.get("alg")) for s in signatures or [] if isinstance(s, dict)}
    if not algs:
        return "hmac_only"
    return "hybrid" if {ED25519, ML_DSA_65} <= algs else "classical"


def _check(status: str, detail: str) -> dict[str, str]:
    return {"status": status, "detail": detail}


def _content_and_statement(pack: dict[str, Any] | None, receipt: dict[str, Any],
                           statement: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    leaf = leaf_hash(statement_bytes(statement)).hex()
    if statement.get("type") != STATEMENT_TYPE:
        stated = _check(FAILED, "the statement type is not one this verifier knows")
    elif str(receipt.get("leaf_hash") or "") != leaf:
        stated = _check(FAILED, "the receipt's leaf hash is not the hash of its statement")
    elif pack is None:
        stated = _check(NOT_CHECKED, "no pack was presented, so the statement was not compared with one")
    elif statement.get("kind") != pack_kind(pack) or statement.get("subject") != pack_subject(pack):
        stated = _check(FAILED, "the statement is about a different pack (kind or subject differs)")
    else:
        stated = _check(PASSED, f"the statement names this {statement.get('kind')} ({statement.get('subject')})")
    if pack is None:
        return _check(NOT_CHECKED, "no pack was presented, so its content was not hashed"), stated
    actual = pack_content_sha256(pack)
    if str(pack.get("content_sha256") or "") != actual:
        return _check(FAILED, "the pack's content does not match its content_sha256"), stated
    if str(statement.get("content_sha256") or "") != actual:
        return _check(FAILED, "the statement names a different content digest than this pack's"), stated
    return _check(PASSED, f"sha256 {actual}"), stated


def _inclusion(receipt: dict[str, Any], statement: dict[str, Any], checkpoint: dict[str, Any]) -> dict[str, str]:
    try:
        index, size = int(receipt.get("leaf_index")), int(checkpoint.get("tree_size"))
        root = root_from_inclusion(leaf_hash(statement_bytes(statement)), index, size,
                                   _hex_list(receipt.get("inclusion_proof")))
    except (TypeError, ValueError) as exc:
        return _check(FAILED, f"the inclusion proof is not usable: {exc}")
    if root.hex() != str(checkpoint.get("root_hash") or ""):
        return _check(FAILED, "the inclusion proof does not lead to the checkpoint's root")
    return _check(PASSED, f"leaf {index} of {size}")


def _checkpoint_signature(checkpoint: dict[str, Any], keys: KeySet) -> tuple[str, str]:
    if checkpoint.get("type") != CHECKPOINT_TYPE:
        return FAILED, "the checkpoint type is not one this verifier knows"
    status, detail, _rows = check_signature_set(
        checkpoint_body(checkpoint), checkpoint.get("signatures"),
        message=pae(CHECKPOINT_PAYLOAD, checkpoint_bytes(checkpoint)), keys=keys, role=ROLE_CHECKPOINT)
    return status, detail


def check_consistency(previous: dict[str, Any], proof: Any, checkpoint: dict[str, Any]) -> dict[str, str]:
    """An earlier checkpoint the caller holds against the receipt's (either may be the older one)."""
    if not isinstance(previous, dict) or previous.get("origin") != checkpoint.get("origin"):
        return _check(FAILED, "the earlier checkpoint is from a different log")
    try:
        older, newer = sorted((previous, checkpoint), key=lambda c: int(c.get("tree_size")))
        first, second = int(older["tree_size"]), int(newer["tree_size"])
        hashes = _hex_list((proof or {}).get("hashes") if isinstance(proof, dict) else proof)
        roots = bytes.fromhex(str(older.get("root_hash"))), bytes.fromhex(str(newer.get("root_hash")))
    except (TypeError, ValueError, KeyError) as exc:
        return _check(FAILED, f"the consistency proof is not usable: {exc}")
    if isinstance(proof, dict) and proof.get("from") is not None and (
            int(proof.get("from")) != first or int(proof.get("to") or 0) != second):
        return _check(FAILED, f"the consistency proof is for other sizes than {first} and {second}")
    if not consistency_holds(first, second, roots[0], roots[1], hashes):
        return _check(FAILED, f"the log at size {second} does not extend the log at size {first}: "
                              "it was rewritten, or these are two different histories")
    return _check(PASSED, f"the log at size {second} extends the log at size {first}")


def _receipt_core(receipt: dict[str, Any], keys: KeySet) -> dict[str, dict[str, str]]:
    """Signatures, inclusion and checkpoint signature of a receipt standing alone."""
    statement = receipt.get("statement") if isinstance(receipt.get("statement"), dict) else {}
    checkpoint = receipt.get("checkpoint") if isinstance(receipt.get("checkpoint"), dict) else {}
    sig_status, sig_detail, _rows = check_signature_set(
        statement, receipt.get("signatures"), message=pae(STATEMENT_PAYLOAD, statement_bytes(statement)),
        keys=keys, role=ROLE_STATEMENT)
    cp_status, cp_detail = _checkpoint_signature(checkpoint, keys)
    return {"signatures": _check(sig_status, sig_detail), "inclusion": _inclusion(receipt, statement, checkpoint),
            "checkpoint_signature": _check(cp_status, cp_detail)}


def check_withdrawal(withdrawal: Any, statement: dict[str, Any], keys: KeySet) -> tuple[dict[str, str],
                                                                                        dict[str, Any] | None]:
    """A withdrawal receipt presented beside a pack: it must verify and name this pack."""
    if not isinstance(withdrawal, dict) or not isinstance(withdrawal.get("statement"), dict):
        return _check(FAILED, "the withdrawal receipt is not a receipt"), None
    theirs = withdrawal["statement"]
    if theirs.get("type") != STATEMENT_TYPE or theirs.get("kind") != WITHDRAWAL:
        return _check(FAILED, "the withdrawal receipt does not hold a withdrawal statement"), None
    if str(withdrawal.get("leaf_hash") or "") != leaf_hash(statement_bytes(theirs)).hex():
        return _check(FAILED, "the withdrawal receipt's leaf hash is not the hash of its statement"), None
    if (theirs.get("content_sha256") != statement.get("content_sha256")
            or theirs.get("subject") != statement.get("subject")):
        return _check(FAILED, "the withdrawal receipt is about a different pack"), None
    core = _receipt_core(withdrawal, keys)
    bad = [k for k, v in core.items() if v["status"] == FAILED]
    if bad:
        return _check(FAILED, f"the withdrawal receipt does not verify ({LABELS[bad[0]]}: "
                              f"{core[bad[0]]['detail']})"), None
    skipped = [k for k, v in core.items() if v["status"] == NOT_CHECKED]
    if skipped:
        return _check(NOT_CHECKED, "a withdrawal receipt was given but could not be fully checked "
                                   f"({LABELS[skipped[0]]}: {core[skipped[0]]['detail']})"), None
    detail = theirs.get("withdrawal") if isinstance(theirs.get("withdrawal"), dict) else {}
    info = {"leaf_index": withdrawal.get("leaf_index"), "withdrawn_at": theirs.get("issued_at"),
            "reason": detail.get("reason"), "superseded_by": detail.get("superseded_by")}
    return _check(PASSED, f"the pack was withdrawn at {info['withdrawn_at']}"), info


# -------------------------------------------------------------------- verdict


def conclude(checks: dict[str, dict[str, str]], *, scheme: str, mode: str,
             withdrawal: dict[str, Any] | None = None, extra: dict[str, Any] | None = None) -> dict[str, Any]:
    """The one place a verdict is decided: ``verified`` only when every check passed."""
    rows = [{"id": cid, "status": checks[cid]["status"], "detail": checks[cid]["detail"]} for cid in CHECK_IDS]
    counts = {status: sum(1 for r in rows if r["status"] == status) for status in (PASSED, FAILED, NOT_CHECKED)}
    failed = [LABELS[r["id"]] for r in rows if r["status"] == FAILED]
    skipped = [LABELS[r["id"]] for r in rows if r["status"] == NOT_CHECKED]
    if scheme == "hmac_only":
        verdict = "hmac_only"
        summary = ("This pack carries no public-key signature and no ledger receipt (HMAC only). "
                   "Only the service that issued it can check it.")
    elif failed:
        verdict = "failed"
        summary = f"{len(failed)} check(s) failed ({', '.join(failed)}). Do not rely on this pack."
    elif withdrawal:
        verdict = "withdrawn"
        summary = (f"Issued, later withdrawn at {withdrawal.get('withdrawn_at')}"
                   + (f": {withdrawal.get('reason')}" if withdrawal.get("reason") else "") + ".")
        if skipped:
            summary += f" Not checked: {', '.join(skipped)}."
    elif skipped:
        verdict = "incomplete"
        summary = (f"No check failed, but {len(skipped)} {'was' if len(skipped) == 1 else 'were'} not checked "
                   f"({', '.join(skipped)}), so this is not a full verification.")
    else:
        verdict = "verified"
        summary = ("Every check passed: the pack is the one that was issued, it is in the log, "
                   "and the log's checkpoint was read back from a write-once store.")
    withdrawal_status = checks["withdrawal"]["status"]
    issuance = ("issued_later_withdrawn" if withdrawal else
                "issued" if withdrawal_status == PASSED else "unknown")
    out = {"verdict": verdict, "signature_scheme": scheme, "mode": mode, "issuance": issuance,
           "checks": rows, "counts": counts, "signatures": [], "leaf_index": None, "checkpoint": None,
           "withdrawal": withdrawal or None, "anchor": None, "summary": summary}
    out.update(extra or {})
    return out


def hmac_only_verdict(mode: str = "offline", *, detail: str = "") -> dict[str, Any]:
    reason = detail or "the pack carries no ledger receipt"
    return conclude({cid: _check(NOT_CHECKED, reason) for cid in CHECK_IDS}, scheme="hmac_only", mode=mode)


def verify_evidence(pack: dict[str, Any] | None, receipt: dict[str, Any] | None, keys: KeySet, *,
                    previous_checkpoint: dict[str, Any] | None = None, consistency_proof: Any = None,
                    withdrawal_receipt: dict[str, Any] | None = None,
                    overrides: dict[str, dict[str, Any]] | None = None, mode: str = "offline",
                    anchor: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run every check on a pack and its receipt and return the EvidenceVerdict.

    ``overrides`` lets the service supply the checks only it can make
    (``consistency`` against the live log, ``withdrawal``, ``anchor``); each is
    ``{"status", "detail"}`` and ``withdrawal`` may add ``"withdrawal": {...}``.
    """
    if receipt is None and isinstance(pack, dict) and isinstance(pack.get("ledger"), dict):
        receipt = pack["ledger"]
    if not isinstance(receipt, dict):
        return hmac_only_verdict(mode)
    statement = receipt.get("statement") if isinstance(receipt.get("statement"), dict) else {}
    checkpoint = receipt.get("checkpoint") if isinstance(receipt.get("checkpoint"), dict) else {}
    overrides = overrides or {}
    checks: dict[str, dict[str, str]] = {}
    checks["content_digest"], checks["statement"] = _content_and_statement(pack, receipt, statement)
    sig_status, sig_detail, sig_rows = check_signature_set(
        statement, receipt.get("signatures"), message=pae(STATEMENT_PAYLOAD, statement_bytes(statement)),
        keys=keys, role=ROLE_STATEMENT)
    checks["signatures"] = _check(sig_status, sig_detail)
    checks["inclusion"] = _inclusion(receipt, statement, checkpoint)
    checks["checkpoint_signature"] = _check(*_checkpoint_signature(checkpoint, keys))

    withdrawal_info: dict[str, Any] | None = None
    if "consistency" in overrides:
        checks["consistency"] = _check(overrides["consistency"]["status"], overrides["consistency"]["detail"])
    elif previous_checkpoint is not None:
        checks["consistency"] = check_consistency(previous_checkpoint, consistency_proof, checkpoint)
    else:
        checks["consistency"] = _check(NOT_CHECKED, "no earlier checkpoint and consistency proof were given")
    if "withdrawal" in overrides:
        checks["withdrawal"] = _check(overrides["withdrawal"]["status"], overrides["withdrawal"]["detail"])
        withdrawal_info = overrides["withdrawal"].get("withdrawal") or None
    elif withdrawal_receipt is not None:
        checks["withdrawal"], withdrawal_info = check_withdrawal(withdrawal_receipt, statement, keys)
    else:
        checks["withdrawal"] = _check(NOT_CHECKED, "the log was not consulted, so a later withdrawal "
                                                   "would not be seen here")
    if "anchor" in overrides:
        checks["anchor"] = _check(overrides["anchor"]["status"], overrides["anchor"]["detail"])
    else:
        checks["anchor"] = _check(NOT_CHECKED, "the write-once store was not read (ask the service)")

    leaf_index = receipt.get("leaf_index")
    extra = {
        "signatures": sig_rows,
        "leaf_index": leaf_index if isinstance(leaf_index, int) and not isinstance(leaf_index, bool) else None,
        "checkpoint": {k: checkpoint.get(k) for k in ("origin", "tree_size", "root_hash", "timestamp")}
        if checkpoint else None,
        "anchor": anchor,
    }
    scheme = signature_scheme(receipt.get("signatures"))
    if scheme == "hmac_only":
        # A receipt with no signature at all is not HMAC only: it is a broken receipt.
        scheme = "classical"
    return conclude(checks, scheme=scheme, mode=mode, withdrawal=withdrawal_info, extra=extra)


# ------------------------------------------------------------------------ CLI


def _load_json(path: str, what: str) -> Any:
    try:
        with open(path, encoding="utf-8") as handle:
            return json.load(handle)
    except (OSError, ValueError) as exc:
        raise SystemExit(_usage_error(f"{what} {path!r} could not be read as JSON: {exc}")) from exc


def _usage_error(message: str) -> int:
    print(f"datawrap-verify: {message}", file=sys.stderr)
    return 64


def render_text(result: dict[str, Any]) -> str:
    words = {PASSED: "passed", FAILED: "FAILED", NOT_CHECKED: "not checked"}
    lines = []
    for row in result["checks"]:
        label = LABELS[row["id"]]
        lines.append(f"{label} {'.' * (24 - len(label))} {words[row['status']]}: {row['detail']}")
    counts = result["counts"]
    lines += ["", f"signature scheme: {result['signature_scheme'].replace('_', ' ')}",
              f"result: {result['verdict'].replace('_', ' ').upper()} "
              f"({counts[PASSED]} passed, {counts[FAILED]} failed, {counts[NOT_CHECKED]} not checked)",
              result["summary"], SCOPE_NOTE]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="datawrap-verify",
        description="Check a Datawrap evidence pack offline: signatures, content digest, inclusion in the "
                    "log, the checkpoint signature and, optionally, consistency with a checkpoint saved earlier.")
    parser.add_argument("pack", help="the exported pack (JSON); the receipt may be embedded as 'ledger'")
    parser.add_argument("--receipt", help="the ledger receipt (JSON), when it is not embedded in the pack")
    parser.add_argument("--keys", action="append", default=[],
                        help="trusted public keys: a JWK Set, one JWK, or PEM public keys (repeatable)")
    parser.add_argument("--previous-checkpoint", help="a checkpoint (JSON) saved from an earlier verification")
    parser.add_argument("--consistency-proof", help="the consistency proof (JSON) between the two checkpoints")
    parser.add_argument("--withdrawal", help="the receipt (JSON) of a withdrawal of this pack, if you hold one")
    parser.add_argument("--json", action="store_true", help="print the verdict as JSON")
    args = parser.parse_args(argv)

    try:
        import cryptography  # noqa: F401
    except ImportError:
        return _usage_error("the 'cryptography' package is required (pip install cryptography)")
    pack = _load_json(args.pack, "pack")
    if not isinstance(pack, dict):
        return _usage_error("the pack must be a JSON object")
    if isinstance(pack.get("pack"), dict) and "content_sha256" not in pack:
        pack = pack["pack"]
    receipt = _load_json(args.receipt, "receipt") if args.receipt else None
    keys = KeySet()
    for path in args.keys:
        try:
            with open(path, encoding="utf-8") as handle:
                keys.add_text(handle.read())
        except (OSError, ValueError) as exc:
            return _usage_error(f"keys {path!r} could not be used: {exc}")
    if bool(args.previous_checkpoint) != bool(args.consistency_proof):
        return _usage_error("--previous-checkpoint and --consistency-proof go together")
    previous = _load_json(args.previous_checkpoint, "checkpoint") if args.previous_checkpoint else None
    if isinstance(previous, dict) and isinstance(previous.get("checkpoint"), dict):
        previous = previous["checkpoint"]
    proof = _load_json(args.consistency_proof, "consistency proof") if args.consistency_proof else None
    withdrawal = _load_json(args.withdrawal, "withdrawal receipt") if args.withdrawal else None
    result = verify_evidence(pack, receipt, keys, previous_checkpoint=previous, consistency_proof=proof,
                             withdrawal_receipt=withdrawal)
    print(json.dumps(result, indent=2) if args.json else render_text(result))
    return EXIT_CODES[result["verdict"]]


if __name__ == "__main__":
    raise SystemExit(main())
