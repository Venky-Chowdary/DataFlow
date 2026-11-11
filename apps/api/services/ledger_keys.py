"""Signing keys of the Evidence Ledger: create, rotate, retire, export, sign.

One keyring document per scope (the pattern of
:mod:`services.audit_chain_keystore`): a workspace's keys sign statements
about its packs (``ws:<workspace id>``), the log's own keys sign checkpoints
(``log``). Every key ever used stays on its keyring, so a retired key keeps
verifying what it signed; it never signs again.

Algorithms: Ed25519 always, and ML-DSA-65 beside it when the installed
``cryptography`` provides it and ``DATAWRAP_EVIDENCE_PQ`` is not ``off``
("hybrid classical and post-quantum signature"). Nothing here implements a
primitive: keys, signatures and encodings all come from ``cryptography``.

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
PQ_NOTE = "Hybrid classical and post-quantum signature: Ed25519 and ML-DSA-65."
CLASSICAL_NOTE = "Classical signature only: this build has no ML-DSA implementation."
PQ_OFF_NOTE = "Classical signature only: the post-quantum signature is switched off (DATAWRAP_EVIDENCE_PQ=off)."


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


def _new_entry(alg: str, scope: str, actor: str) -> dict[str, Any]:
    private = _generate(alg)
    public = private.public_key()
    stored, custody = _wrap(private, scope)
    return {
        "key_id": verifier.key_id_of(public), "alg": alg, "role": _role_of(scope),
        "public_key": base64.b64encode(public.public_bytes_raw()).decode("ascii"),
        "private_enc": stored, "custody": custody, "status": "active",
        "created_at": _now(), "created_by": actor, "retired_at": None, "retired_by": None,
    }


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
    """Create the scope's first key set. :class:`KeyConflict` when it already has a keyring."""
    doc = {"scope": scope, "workspace_id": _workspace_of(scope), "revision": 1, "created_at": _now(),
           "keys": [_new_entry(alg, scope, actor) for alg in _algorithms()]}
    if not ledger_store.put_if_absent(ledger_store.KEYS, scope, doc):
        raise KeyConflict("This workspace already has signing keys; rotate them instead.")
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
        for entry in doc["keys"]:
            if entry.get("status") == "active":
                _retire(entry, actor)
        fresh = [_new_entry(alg, scope, actor) for alg in _algorithms()]
        doc["keys"] = doc["keys"] + fresh
        doc["revision"] = revision + 1
        if ledger_store.replace_if(ledger_store.KEYS, scope, doc, revision=revision):
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
        _retire(entry, actor)
        doc["revision"] = revision + 1
        if ledger_store.replace_if(ledger_store.KEYS, scope, doc, revision=revision):
            _audit("evidence.key.retired", scope, actor, {"key_id": key_id, "alg": entry.get("alg")})
            return public_view(entry, scope)
    raise KeyConflict("The keys changed while this one was being retired; try again.")


# ------------------------------------------------------------------ signing


class Signer:
    """The active keys of one scope, able to sign. It exposes key ids and signatures only."""

    __slots__ = ("_keys", "scope")

    def __init__(self, scope: str, keys: list[tuple[str, str, Any]]) -> None:
        self.scope = scope
        self._keys = keys

    def __repr__(self) -> str:
        return f"Signer(scope={self.scope!r}, keys={[kid[:16] for kid, _alg, _key in self._keys]})"

    @property
    def key_ids(self) -> list[str]:
        return sorted(kid for kid, _alg, _key in self._keys)

    @property
    def scheme(self) -> str:
        return "hybrid" if {ED25519, ML_DSA_65} <= {alg for _kid, alg, _key in self._keys} else "classical"

    def sign(self, payload_type: str, body: bytes) -> list[dict[str, str]]:
        message = verifier.pae(payload_type, body)
        return [{"alg": alg, "key_id": kid, "value": base64.b64encode(key.sign(message)).decode("ascii")}
                for kid, alg, key in sorted(self._keys, key=lambda item: item[0])]


def signer(scope: str, *, actor: str = "system") -> Signer | None:
    """A signer over the scope's active keys; its first key set is created on first use.

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
            pass  # another replica created it first: adopt its keys
        doc = _keyring(scope)
    usable = []
    for entry in (doc or {}).get("keys") or []:
        if entry.get("status") != "active":
            continue
        if entry.get("alg") == ML_DSA_65 and not pq_available():
            continue
        usable.append((str(entry["key_id"]), str(entry["alg"]), _unwrap(entry, scope)))
    return Signer(scope, usable) if usable else None


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
    for key_id in dict.fromkeys(str(k) for k in key_ids if k):
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
