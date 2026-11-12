"""Signing keys of the Evidence Ledger: create, rotate, retire, export, sign.

One keyring document per scope (the pattern of
:mod:`services.audit_chain_keystore`): a workspace's keys sign statements
about its packs (``ws:<workspace id>``), the log's own keys sign checkpoints
(``log``). Every key ever used stays on its keyring, so a retired key keeps
verifying what it signed; it never signs again.

Algorithms: Ed25519 always, and a post-quantum signature (ML-DSA-65) beside it
when the installed ``cryptography`` and its backend provide it and
``DATAWRAP_EVIDENCE_PQ`` is not ``off`` ("hybrid classical and post-quantum
signature"). Nothing here implements a primitive: keys, signatures and
encodings all come from ``cryptography``.

Trust root: every key event (a key set created, rotated, a key retired) is
itself an entry in the ledger's log (kind ``key_set``), carrying the public
keys and signed by the keys that were active before the event and by the new
ones. A verifier who holds the log's key id from outside can therefore check
a workspace's keys against the log instead of asking the issuing server.

Custody: a private key exists in this module only. It is generated here, kept
encrypted by :func:`services.secret_vault.encrypt_secret` (the deployment's
vault, or the tenant's own key when BYOK is active for the workspace), and
used through :class:`Signer`, which hands out signatures and public data and
nothing else. Private material is never logged, returned or put in a pack.

Key id: SHA-256 of the public key's DER SubjectPublicKeyInfo
(:func:`tools.datawrap_verify.key_id_of`).
"""

from __future__ import annotations

import base64
import logging
from datetime import datetime, timezone
from typing import Any

from services import ledger_store
from tools import datawrap_verify as verifier

logger = logging.getLogger(__name__)

LOG_SCOPE = "log"
ED25519, ML_DSA_65 = verifier.ED25519, verifier.ML_DSA_65
ROLE_STATEMENT, ROLE_CHECKPOINT = verifier.ROLE_STATEMENT, verifier.ROLE_CHECKPOINT
_VAULT_LABEL = "evidence_ledger_key"
_PLAIN_PREFIX = "ledgerkey:"
#: An opaque reference a real vault returns. ``enc:v0`` is base64, not encryption: refused.
_ENCRYPTED_PREFIXES = ("enc:v1:", "sm:", "byok:")
CAS_ATTEMPTS = 6
PQ_NOTE = "Hybrid classical and post-quantum signature: Ed25519 and a post-quantum signature (ML-DSA-65)."
CLASSICAL_NOTE = "Classical signature (Ed25519), no post-quantum signature: this build has no ML-DSA implementation."
PQ_OFF_NOTE = ("Classical signature (Ed25519), no post-quantum signature: it is switched off "
               "(DATAWRAP_EVIDENCE_PQ=off).")
KEY_INDEX = "key:"  # ledger_store.META id prefix: key id -> the leaf that published it
MAX_KEYS_PER_DOCUMENT = 16


class LedgerKeyError(RuntimeError):
    """A key operation that cannot be done; ``status_code`` is the HTTP answer."""

    status_code = 400


class KeyConflict(LedgerKeyError):
    status_code = 409


class KeyNotFound(LedgerKeyError):
    status_code = 404


class KeystoreUnavailable(LedgerKeyError):
    """The vault or the store cannot keep a private key safely right now."""

    status_code = 503


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def scope_for(workspace_id: str | None) -> str:
    return f"ws:{(workspace_id or '').strip()}"


def _workspace_of(scope: str) -> str | None:
    return scope[3:] if scope.startswith("ws:") else None


def _role_of(scope: str) -> str:
    return ROLE_CHECKPOINT if scope == LOG_SCOPE else ROLE_STATEMENT


def pq_available() -> bool:
    return verifier.ml_dsa_available()


def pq_enabled() -> bool:
    from services.brand_env import getenv_brand

    off = (getenv_brand("EVIDENCE_PQ", "auto") or "auto").strip().lower() in {"off", "0", "false", "no"}
    return pq_available() and not off


def post_quantum_status() -> dict[str, Any]:
    available, enabled = pq_available(), pq_enabled()
    note = PQ_NOTE if enabled else (PQ_OFF_NOTE if available else CLASSICAL_NOTE)
    return {"alg": ML_DSA_65, "available": available, "enabled": enabled, "note": note}


def _algorithms() -> list[str]:
    return [ED25519, ML_DSA_65] if pq_enabled() else [ED25519]


# ------------------------------------------------------------ key material


def _generate(alg: str) -> Any:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    if alg == ED25519:
        return ed25519.Ed25519PrivateKey.generate()
    from cryptography.hazmat.primitives.asymmetric import mldsa

    return mldsa.MLDSA65PrivateKey.generate()


def _private_from_raw(alg: str, raw: bytes) -> Any:
    from cryptography.hazmat.primitives.asymmetric import ed25519

    if alg == ED25519:
        return ed25519.Ed25519PrivateKey.from_private_bytes(raw)
    if alg == ML_DSA_65 and pq_available():
        from cryptography.hazmat.primitives.asymmetric import mldsa

        return mldsa.MLDSA65PrivateKey.from_seed_bytes(raw)
    raise KeystoreUnavailable(f"This build cannot sign with {alg}.")


def _tenant(scope: str) -> str | None:
    workspace_id = _workspace_of(scope)
    if not workspace_id:
        return None
    from services.secret_vault import tenant_id_from_workspace

    return tenant_id_from_workspace(workspace_id)


def _wrap(private: Any, scope: str) -> tuple[str, str]:
    """Encrypt a private key for storage. Returns (opaque reference, custody)."""
    from services.secret_vault import encrypt_secret

    plain = _PLAIN_PREFIX + base64.b64encode(private.private_bytes_raw()).decode("ascii")
    try:
        stored = encrypt_secret(plain, tenant_id=_tenant(scope), label=_VAULT_LABEL)
    except Exception as exc:  # noqa: BLE001 - any vault fault: no key is kept unencrypted
        raise KeystoreUnavailable("The secret vault could not encrypt a signing key.") from exc
    if not stored.startswith(_ENCRYPTED_PREFIXES):
        raise KeystoreUnavailable("The secret vault is not encrypting; a signing key cannot be kept.")
    return stored, "byok" if stored.startswith("byok:") else "vault"


def _unwrap(entry: dict[str, Any], scope: str) -> Any:
    from services.secret_vault import decrypt_secret

    try:
        plain = decrypt_secret(str(entry.get("private_enc") or ""), tenant_id=_tenant(scope))
    except Exception as exc:  # noqa: BLE001 - reported without the vault's message (it may echo input)
        raise KeystoreUnavailable(f"Signing key {str(entry.get('key_id'))[:16]} could not be decrypted "
                                  f"({type(exc).__name__}).") from exc
    if not plain.startswith(_PLAIN_PREFIX):
        raise KeystoreUnavailable(f"Signing key {str(entry.get('key_id'))[:16]} is not readable.")
    private = _private_from_raw(str(entry.get("alg")), base64.b64decode(plain[len(_PLAIN_PREFIX):]))
    if verifier.key_id_of(private.public_key()) != entry.get("key_id"):
        raise KeystoreUnavailable(f"Signing key {str(entry.get('key_id'))[:16]} does not match its id.")
    return private


def _new_entry(alg: str, scope: str, actor: str) -> tuple[dict[str, Any], Any]:
    """A new keyring entry and its private key (kept by the caller just long enough to sign the key event)."""
    private = _generate(alg)
    public = private.public_key()
    stored, custody = _wrap(private, scope)
    return {
        "key_id": verifier.key_id_of(public), "alg": alg, "role": _role_of(scope),
        "public_key": base64.b64encode(public.public_bytes_raw()).decode("ascii"),
        "private_enc": stored, "custody": custody, "status": "active",
        "created_at": _now(), "created_by": actor, "retired_at": None, "retired_by": None,
    }, private


def _new_set(scope: str, actor: str) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], Any]]]:
    made = [_new_entry(alg, scope, actor) for alg in _algorithms()]
    return [entry for entry, _private in made], made


# ------------------------------------------------------------------ keyring


def _keyring(scope: str) -> dict[str, Any] | None:
    return ledger_store.get(ledger_store.KEYS, scope)


def public_view(entry: dict[str, Any], scope: str) -> dict[str, Any]:
    return {"key_id": entry.get("key_id"), "alg": entry.get("alg"), "role": entry.get("role"),
            "workspace_id": _workspace_of(scope), "status": entry.get("status"),
            "custody": entry.get("custody"), "created_at": entry.get("created_at"),
            "created_by": entry.get("created_by"), "retired_at": entry.get("retired_at"),
            "retired_by": entry.get("retired_by")}


def list_keys(scope: str) -> list[dict[str, Any]]:
    doc = _keyring(scope) or {}
    return [public_view(k, scope) for k in doc.get("keys") or []]


def _scheme(entries: list[dict[str, Any]]) -> str:
    active = {k.get("alg") for k in entries if k.get("status") == "active"}
    if not active:
        return "none"
    return "hybrid" if {ED25519, ML_DSA_65} <= active else "classical"


def signature_scheme(scope: str) -> str:
    """What this scope would sign with now: hybrid, classical or none."""
    doc = _keyring(scope)
    if doc is None:
        return "hybrid" if pq_enabled() else "classical"  # created on first use
    return _scheme(doc.get("keys") or [])


def _audit(action: str, scope: str, actor: str, details: dict[str, Any]) -> None:
    from services.audit_log import append_audit_event

    try:
        append_audit_event(action=action, resource=f"evidence_keys:{scope}", actor=actor or "system",
                           level="info", details=details, workspace_id=_workspace_of(scope) or None)
    except Exception as exc:  # noqa: BLE001 - the key change stands; a failed append is reported
        logger.error("evidence audit %s failed (%s)", action, type(exc).__name__, exc_info=exc)


def create(scope: str, *, actor: str) -> list[dict[str, Any]]:
    """Create the scope's initial key set. :class:`KeyConflict` when it already has a keyring."""
    entries, made = _new_set(scope, actor)
    doc = {"scope": scope, "workspace_id": _workspace_of(scope), "revision": 1, "created_at": _now(),
           "keys": entries}
    if not ledger_store.put_if_absent(ledger_store.KEYS, scope, doc):
        raise KeyConflict("This workspace already has signing keys; rotate them instead.")
    _publish(scope, doc, "created", Signer(scope, made), actor)
    _audit("evidence.key.created", scope, actor,
           {"key_ids": [k["key_id"] for k in doc["keys"]], "algorithms": [k["alg"] for k in doc["keys"]]})
    return [public_view(k, scope) for k in doc["keys"]]


def _retire(entry: dict[str, Any], actor: str) -> None:
    entry.update(status="retired", retired_at=_now(), retired_by=actor)


def rotate(scope: str, *, actor: str) -> list[dict[str, Any]]:
    """A new key set becomes current; every active key is retired (it still verifies)."""
    for _ in range(CAS_ATTEMPTS):
        doc = _keyring(scope)
        if doc is None:
            try:
                return create(scope, actor=actor)
            except KeyConflict:
                continue
        revision = int(doc.get("revision") or 0)
        retired = [k["key_id"] for k in doc["keys"] if k.get("status") == "active"]
        outgoing = _outgoing(doc, scope)  # the keys being retired sign the announcement of their successors
        for entry in doc["keys"]:
            if entry.get("status") == "active":
                _retire(entry, actor)
        fresh, made = _new_set(scope, actor)
        doc["keys"] = doc["keys"] + fresh
        doc["revision"] = revision + 1
        if ledger_store.replace_if(ledger_store.KEYS, scope, doc, revision=revision):
            _publish(scope, doc, "rotated", Signer(scope, outgoing + made), actor)
            _audit("evidence.key.rotated", scope, actor,
                   {"new_key_ids": [k["key_id"] for k in fresh], "retired_key_ids": retired})
            return [public_view(k, scope) for k in doc["keys"]]
    raise KeyConflict("The keys changed while they were being rotated; try again.")


def retire(scope: str, key_id: str, *, actor: str) -> dict[str, Any]:
    for _ in range(CAS_ATTEMPTS):
        doc = _keyring(scope)
        entry = next((k for k in (doc or {}).get("keys") or [] if k.get("key_id") == key_id), None)
        if doc is None or entry is None:
            raise KeyNotFound("Signing key not found")
        if entry.get("status") != "active":
            raise KeyConflict("This key is already retired.")
        revision = int(doc.get("revision") or 0)
        outgoing = _outgoing(doc, scope)  # including the key being retired: its last signature
        _retire(entry, actor)
        doc["revision"] = revision + 1
        if ledger_store.replace_if(ledger_store.KEYS, scope, doc, revision=revision):
            _publish(scope, doc, "retired", Signer(scope, outgoing), actor)
            _audit("evidence.key.retired", scope, actor, {"key_id": key_id, "alg": entry.get("alg")})
            return public_view(entry, scope)
    raise KeyConflict("The keys changed while this one was being retired; try again.")


# ------------------------------------------------------------------ signing


class Signer:
    """The active keys of one scope, able to sign. It exposes key ids, public keys and signatures."""

    __slots__ = ("_keys", "scope")

    def __init__(self, scope: str, keys: list[tuple[dict[str, Any], Any]]) -> None:
        self.scope = scope
        self._keys = sorted(keys, key=lambda item: str(item[0]["key_id"]))

    def __repr__(self) -> str:
        return f"Signer(scope={self.scope!r}, keys={[str(e['key_id'])[:16] for e, _key in self._keys]})"

    @property
    def key_ids(self) -> list[str]:
        return [str(entry["key_id"]) for entry, _key in self._keys]

    @property
    def algs(self) -> list[str]:
        """The algorithm of each key, in the order of :attr:`key_ids` (bound into what is signed)."""
        return [str(entry["alg"]) for entry, _key in self._keys]

    @property
    def jwks(self) -> list[dict[str, Any]]:
        return [jwk(entry) for entry, _key in self._keys]

    @property
    def scheme(self) -> str:
        return verifier.scheme_of(self.algs)

    def sign(self, payload_type: str, body: bytes) -> list[dict[str, str]]:
        message = verifier.pae(payload_type, body)
        return [{"alg": str(entry["alg"]), "key_id": str(entry["key_id"]),
                 "value": base64.b64encode(key.sign(message)).decode("ascii")} for entry, key in self._keys]


def _usable(doc: dict[str, Any] | None, scope: str) -> list[tuple[dict[str, Any], Any]]:
    """The active keys of a keyring this build can sign with, unwrapped."""
    usable = []
    for entry in (doc or {}).get("keys") or []:
        if entry.get("status") != "active":
            continue
        if entry.get("alg") == ML_DSA_65 and not pq_available():
            continue
        usable.append((entry, _unwrap(entry, scope)))
    return usable


def _outgoing(doc: dict[str, Any], scope: str) -> list[tuple[dict[str, Any], Any]]:
    """The keys that sign a rotation or retirement; none when they cannot be read.

    Retiring or replacing a key must not depend on being able to decrypt it
    (that may be the very reason it is retired). The key event is then signed
    by the incoming keys alone, or not published when there are none.
    """
    try:
        return _usable(doc, scope)
    except LedgerKeyError as exc:
        logger.error("evidence keys of %s could not sign their own key event (%s)", scope, type(exc).__name__)
        return []


def signer(scope: str, *, actor: str = "system") -> Signer | None:
    """A signer over the scope's active keys; its initial key set is created on first use.

    ``None`` when the scope has a keyring and no active key (every key was
    retired): nothing is created silently after an explicit retirement. A key
    this build cannot use (ML-DSA on an older ``cryptography``) is left out, so
    the signature is then "classical" and says so.
    """
    doc = _keyring(scope)
    if doc is None:
        try:
            create(scope, actor=actor)
        except KeyConflict:
            pass  # another replica created it at the same moment: adopt its keys
        doc = _keyring(scope)
    usable = _usable(doc, scope)
    return Signer(scope, usable) if usable else None


# ----------------------------------------------------------------- key events


def _publish(scope: str, doc: dict[str, Any], event: str, signed_by: Signer, actor: str) -> None:
    """Append a key event to the ledger's log: the scope's public keys, signed by ``signed_by``.

    Best effort: the keyring change stands if the log cannot be written, and
    :func:`ensure_published` publishes the keys the next time they sign.
    """
    from services import ledger_log

    if not signed_by.key_ids:
        logger.error("evidence key event (%s) has no key to sign it; not published to the log", event)
        return
    active = [k for k in doc.get("keys") or [] if k.get("status") == "active"]
    key_set = {"role": _role_of(scope), "event": event, "keys": [jwk(k) for k in active],
               "retired_key_ids": sorted(str(k["key_id"]) for k in doc.get("keys") or []
                                         if k.get("status") != "active")}
    digest = verifier.sha256_hex(verifier.canonical_json(key_set).encode("utf-8"))
    try:
        entry, _created = ledger_log.append(
            kind=verifier.KEY_SET, subject=f"{verifier.KEY_SET}:{key_set['role']}", content_sha256=digest,
            workspace_id=_workspace_of(scope) or "", actor=actor, signer=signed_by, detail={verifier.KEY_SET: key_set})
        for key in active:
            ledger_store.put_if_absent(ledger_store.META, KEY_INDEX + str(key["key_id"]),
                                       {"leaf_index": int(entry["index"])})
    except Exception as exc:  # noqa: BLE001 - the keys exist and sign; publication is retried on next use
        logger.error("evidence key event (%s) was not published to the log (%s)", event, type(exc).__name__,
                     exc_info=exc)


def published_at(key_id: str) -> int | None:
    """The leaf index of the key event that published ``key_id``, or None."""
    meta = ledger_store.get(ledger_store.META, KEY_INDEX + str(key_id))
    return int(meta["leaf_index"]) if meta and meta.get("leaf_index") is not None else None


def ensure_published(signed_by: Signer) -> None:
    """Publish the signer's keys to the log if an earlier attempt did not reach it."""
    if all(published_at(key_id) is not None for key_id in signed_by.key_ids):
        return
    doc = _keyring(signed_by.scope)
    if doc is not None:
        _publish(signed_by.scope, doc, "published", signed_by, "system")


# ------------------------------------------------------------------- public


def find_key(key_id: str) -> tuple[dict[str, Any], str] | None:
    """(keyring entry, scope) for a key id, across every scope."""
    doc = ledger_store.find_keyring_of(str(key_id))
    if not doc:
        return None
    entry = next((k for k in doc.get("keys") or [] if k.get("key_id") == key_id), None)
    return (entry, str(doc.get("scope"))) if entry else None


def _public_raw(entry: dict[str, Any]) -> bytes:
    return base64.b64decode(str(entry.get("public_key") or ""))


def jwk(entry: dict[str, Any]) -> dict[str, Any]:
    x = base64.urlsafe_b64encode(_public_raw(entry)).rstrip(b"=").decode("ascii")
    common = {"use": "sig", "kid": entry.get("key_id"), "datawrap_role": entry.get("role"),
              "datawrap_status": entry.get("status")}
    if entry.get("alg") == ED25519:
        return {"kty": "OKP", "crv": "Ed25519", "x": x, "alg": "EdDSA", **common}
    return {"kty": "AKP", "alg": ML_DSA_65, "pub": x, **common}


def pem(entry: dict[str, Any]) -> str:
    from cryptography.hazmat.primitives import serialization

    public = verifier.public_from_raw(str(entry.get("alg")), _public_raw(entry))
    return public.public_bytes(serialization.Encoding.PEM,
                               serialization.PublicFormat.SubjectPublicKeyInfo).decode("ascii")


def jwks(scopes: list[str]) -> dict[str, Any]:
    """The JWK Set of these scopes (active and retired keys): what the offline verifier trusts."""
    keys = []
    for scope in scopes:
        keys += [jwk(k) for k in (_keyring(scope) or {}).get("keys") or []]
    return {"keys": keys}


def trusted_keys(key_ids: list[str]) -> verifier.KeySet:
    """The deployment's own registry, as the verifier's key set, for the ids a document names."""
    out = verifier.KeySet()
    # A document names a handful of keys; one that names a crowd is not looked up key by key.
    for key_id in list(dict.fromkeys(str(k) for k in key_ids if k))[:MAX_KEYS_PER_DOCUMENT]:
        found = find_key(key_id)
        if not found:
            continue
        entry, _scope = found
        try:
            public = verifier.public_from_raw(str(entry.get("alg")), _public_raw(entry))
        except NotImplementedError as exc:
            out.unusable[key_id] = str(exc)
            continue
        if verifier.key_id_of(public) != key_id:
            logger.error("evidence key %s does not match its id; ignored", key_id[:16])
            continue
        out.add(verifier.Key(key_id, str(entry.get("alg")), public, str(entry.get("role") or ""),
                             str(entry.get("status") or "")))
    return out
