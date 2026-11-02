"""The audit chain's keyring, kept in the control-plane database (QA r12 #3).

D221 gave the audit chain its own signing key. With no key configured, each
process generated one and kept it at ``<data dir>/audit_chain.key``. On
Railway that directory is the container's own filesystem unless a volume is
attached, so every deploy (and every separate service: API, worker,
scheduler) signed with a key nobody else held, and verification reported the
earlier deploy's records as signed under a key it no longer has.

Now, when no key is configured and the control-plane MongoDB is reachable:

* The chain key lives in one document of ``audit_chain_keys``, encrypted with
  ``services.secret_vault`` (``enc:v1``, keyed by the deployment's secrets key,
  which is configuration and survives deploys) — the same durable home round 7
  gave AI keys and round 8 gave ``hash_pii`` keys. Every process reads the same
  document, so every process signs with the same key.
* The document is created once with an atomic ``insert_one`` on a fixed
  ``_id``; a second process racing it gets ``DuplicateKeyError`` and adopts
  the winner's key.
* Every key ever used stays in the document by ``key_id``. Rotation adds a new
  current key and marks the old one retired; retired keys keep verifying.
* A key file left on this node by the old scheme is imported on first use
  (as the current key when the keyring is created by that import, otherwise as
  a retired key), so the records it signed keep verifying.

Key material never leaves this module except as the bytes the chain HMACs
with; reports carry key ids only.
"""

from __future__ import annotations

import logging
import secrets
import threading
import time
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

COLLECTION = "audit_chain_keys"
DOC_ID = "deployment"
_VAULT_LABEL = "audit_chain_key"
#: How long a process trusts its cached copy of the keyring. A rotation in
#: another process is picked up within this window; until then this process
#: keeps signing with the previous key, which stays verifiable.
CACHE_TTL_S = 30.0
#: A lookup for a key id the cache does not hold re-reads the keyring, at most
#: this often, so a walk over many lost-key records costs one read, not one per record.
MISS_REFRESH_S = 2.0

_lock = threading.Lock()
_cache: dict[str, Any] = {"ns": None, "doc": None, "keys": {}, "read_at": 0.0, "imported": set()}


class KeystoreUnavailable(RuntimeError):
    """The control-plane keyring could not be read or written."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collection() -> Any | None:
    try:
        from services.control_plane_store import mongo_collection

        return mongo_collection(COLLECTION)
    except Exception:  # noqa: BLE001 - no database: the caller uses the node-local key
        return None


def _bound(coll: Any) -> Any:
    """Drop the cache when the keyring now lives in another database."""
    ns = getattr(coll, "full_name", None) or id(coll)
    if _cache["ns"] != ns:
        reset_cache()
        _cache["ns"] = ns
    return coll


def available() -> bool:
    return _collection() is not None


def reset_cache() -> None:
    """Forget the cached keyring (tests; a process re-reads on next use)."""
    with _lock:
        _cache.update({"ns": None, "doc": None, "keys": {}, "read_at": 0.0, "imported": set()})


def _key_id(key: bytes) -> str:
    from services.audit_chain_keys import key_id_for

    return key_id_for(key)


def _entry(key: bytes, *, origin: str, actor: str) -> dict[str, Any]:
    from services.secret_vault import encrypt_secret

    return {
        "key_id": _key_id(key),
        "key_enc": encrypt_secret(key.decode("utf-8"), label=_VAULT_LABEL),
        "origin": origin,
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _decrypt_all(doc: dict[str, Any]) -> dict[str, bytes]:
    from services.secret_vault import decrypt_secret

    keys: dict[str, bytes] = {}
    for entry in doc.get("keys") or []:
        kid = str(entry.get("key_id") or "")
        try:
            plain = decrypt_secret(str(entry.get("key_enc") or ""))
        except Exception as exc:  # noqa: BLE001 - one unreadable entry must not hide the rest
            logger.error("audit chain key %s could not be decrypted: %s", kid, type(exc).__name__)
            continue
        if plain and _key_id(plain.encode("utf-8")) == kid:
            keys[kid] = plain.encode("utf-8")
        elif plain:
            logger.error("audit chain key %s does not match its id; ignored", kid)
    return keys


def _read(coll: Any) -> dict[str, Any] | None:
    doc = coll.find_one({"_id": DOC_ID})
    if not isinstance(doc, dict):
        return None
    doc = dict(doc)
    with _lock:
        _cache.update({"doc": doc, "keys": _decrypt_all(doc), "read_at": time.monotonic()})
    return doc


def _create(coll: Any, local_key: bytes | None) -> dict[str, Any]:
    """Create the keyring once; a racing process adopts whichever insert won."""
    from pymongo.errors import DuplicateKeyError

    if local_key:
        first = _entry(local_key, origin="imported_local_file", actor="system")
    else:
        first = _entry(secrets.token_urlsafe(48).encode("utf-8"), origin="generated", actor="system")
    try:
        coll.insert_one(
            {
                "_id": DOC_ID,
                "current_key_id": first["key_id"],
                "keys": [first],
                "revision": 1,
                "created_at": _now(),
            }
        )
    except DuplicateKeyError:
        pass
    doc = _read(coll)
    if doc is None:
        raise KeystoreUnavailable("audit chain keyring was not created")
    return doc


def _import(coll: Any, key: bytes, *, origin: str) -> None:
    """Add ``key`` as a retired key unless the keyring already holds it (atomic)."""
    entry = _entry(key, origin=origin, actor="system")
    entry["retired_at"] = entry["created_at"]
    coll.update_one(
        {"_id": DOC_ID, "keys.key_id": {"$ne": entry["key_id"]}},
        {"$push": {"keys": entry}, "$inc": {"revision": 1}},
    )


def _fresh(max_age: float) -> dict[str, Any] | None:
    doc = _cache["doc"]
    if doc is not None and time.monotonic() - float(_cache["read_at"]) < max_age:
        return doc
    return None


def current_key(local_key: bytes | None = None) -> tuple[str, bytes] | None:
    """``(key_id, key)`` new records are signed with, or ``None`` without a database.

    Creates the keyring on first use; imports ``local_key`` (the old node-local
    key file, if this node still has one) the first time this process sees it.
    Raises :class:`KeystoreUnavailable` when the database is configured but the
    keyring cannot be read or created and no cached copy exists.
    """
    coll = _collection()
    if coll is None:
        return None
    _bound(coll)
    try:
        doc = _fresh(CACHE_TTL_S)
        if doc is None:
            doc = _read(coll) or _create(coll, local_key)
        if local_key:
            kid = _key_id(local_key)
            if kid not in _cache["imported"]:
                if kid not in _cache["keys"]:
                    _import(coll, local_key, origin="imported_local_file")
                    doc = _read(coll) or doc
                    logger.warning(
                        "audit chain: imported this node's key file (%s) into the shared keyring",
                        kid,
                    )
                _cache["imported"].add(kid)
    except Exception as exc:  # noqa: BLE001
        stale = _cache["doc"]
        if stale is None:
            raise KeystoreUnavailable(type(exc).__name__) from exc
        logger.warning("audit chain keyring unreadable, using cached copy: %s", type(exc).__name__)
        doc = stale
    kid = str(doc.get("current_key_id") or "")
    key = _cache["keys"].get(kid)
    if key is None:
        raise KeystoreUnavailable(f"current audit chain key {kid} is not readable")
    return kid, key


def all_keys(*, missing: str | None = None) -> dict[str, bytes]:
    """Every persisted key by id. ``missing`` names an id a caller could not
    find; the keyring is then re-read (at most every :data:`MISS_REFRESH_S`)."""
    coll = _collection()
    if coll is None:
        return {}
    _bound(coll)
    max_age = CACHE_TTL_S
    if missing and missing not in _cache["keys"]:
        max_age = MISS_REFRESH_S
    if _fresh(max_age) is None:
        try:
            if _read(coll) is None:
                with _lock:
                    _cache["read_at"] = time.monotonic()
        except Exception as exc:  # noqa: BLE001 - verification reports unknown keys instead
            logger.warning("audit chain keyring unreadable: %s", type(exc).__name__)
    return dict(_cache["keys"])


def rotate(*, actor: str, local_key: bytes | None = None) -> dict[str, Any]:
    """Start a new current key; the previous one stays in the keyring, retired."""
    coll = _collection()
    if coll is None:
        raise KeystoreUnavailable("no control-plane database to keep the keyring in")
    _bound(coll)
    current_key(local_key)
    for _attempt in range(3):
        doc = _read(coll) or {}
        previous = str(doc.get("current_key_id") or "")
        entry = _entry(secrets.token_urlsafe(48).encode("utf-8"), origin="rotation", actor=actor)
        when = entry["created_at"]
        keys = [dict(e) for e in doc.get("keys") or []]
        for old in keys:
            if old.get("key_id") == previous and not old.get("retired_at"):
                old["retired_at"] = when
        updated = {k: v for k, v in doc.items() if k != "_id"}
        revision = int(doc.get("revision") or 0)
        updated.update(
            {
                "current_key_id": entry["key_id"],
                "keys": [*keys, entry],
                "rotated_at": when,
                "revision": revision + 1,
            }
        )
        # Compare-and-swap on the revision: a concurrent rotation or import
        # makes this match nothing, and the loop re-reads instead of losing it.
        result = coll.replace_one(
            {"_id": DOC_ID, "current_key_id": previous, "revision": doc.get("revision")},
            {"_id": DOC_ID, **updated},
        )
        if result.matched_count == 1:
            _read(coll)
            logger.warning("audit chain key rotated: %s -> %s by %s", previous, entry["key_id"], actor)
            return status()
    raise KeystoreUnavailable("audit chain key rotation lost three races; try again")


def status() -> dict[str, Any]:
    """Public facts about the persisted keyring: ids and dates, never key material."""
    coll = _collection()
    if coll is None:
        return {"current_key_id": None, "keys": []}
    _bound(coll)
    doc = _fresh(CACHE_TTL_S) or _read(coll) or {}
    return {
        "current_key_id": doc.get("current_key_id"),
        "keys": [
            {k: e.get(k) for k in ("key_id", "origin", "created_at", "created_by", "retired_at")}
            for e in doc.get("keys") or []
        ],
    }
