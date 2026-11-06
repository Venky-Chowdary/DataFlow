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
        _noted.clear()
        _note_retry_at.clear()
        _epoch_cache.clear()
        _position_cache.update({"ns": None, "value": None, "until": 0.0})


def _key_id(key: bytes) -> str:
    from services.audit_chain_keys import key_id_for

    return key_id_for(key)


#: Derived keyring keys: generation N of this deployment's chain key is
#: HKDF(vault master, "audit-chain/v1/<keyring namespace>/N"). The keyring
#: document stores metadata only, so deleting or pulling an entry loses
#: nothing — the verifier re-derives every generation (fifth review).
DERIVED_SCAN = 64
KEYRING_SEAL_ACTION = "audit.keyring.sealed"
_derived_cache: dict[str, Any] = {"fp": None, "ids": {}}
_pending_seal: dict[str, Any] = {"keys": None}


def _ns(coll: Any) -> str:
    return str(getattr(coll, "full_name", "") or "")


def derive_key(generation: int, ns: str) -> bytes:
    """Generation ``generation`` of the derived chain key (urlsafe text bytes)."""
    import base64

    from services.secret_vault import derive_subkey

    raw = derive_subkey(f"audit-chain/v1/{ns}/{int(generation)}".encode("utf-8"))
    return base64.urlsafe_b64encode(raw).rstrip(b"=")


def derived_keys(ns: str, upto: int = 0) -> dict[str, bytes]:
    """Every derived generation 1..max(DERIVED_SCAN, upto) by key id (cached per master)."""
    from services.secret_vault import derive_subkey

    try:
        fp = (ns, derive_subkey(b"audit-chain/v1/fingerprint")[:8])
    except Exception:  # noqa: BLE001 - no vault key: nothing can be derived
        return {}
    top = max(DERIVED_SCAN, int(upto or 0))
    if _derived_cache["fp"] != fp or len(_derived_cache["ids"]) < top:
        ids: dict[str, bytes] = {}
        for gen in range(1, top + 1):
            key = derive_key(gen, ns)
            ids[_key_id(key)] = key
        _derived_cache.update({"fp": fp, "ids": ids})
    return dict(_derived_cache["ids"])


def _entry(key: bytes, *, origin: str, actor: str) -> dict[str, Any]:
    """A legacy (random / imported) key, kept encrypted on the document."""
    from services.secret_vault import encrypt_secret

    return {
        "key_id": _key_id(key),
        "key_enc": encrypt_secret(key.decode("utf-8"), label=_VAULT_LABEL),
        "origin": origin,
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _derived_entry(generation: int, ns: str, *, origin: str, actor: str) -> dict[str, Any]:
    """A derived key: metadata only, no key material on the document."""
    return {
        "key_id": _key_id(derive_key(generation, ns)),
        "generation": int(generation),
        "origin": origin,
        "created_at": _now(),
        "created_by": actor,
        "retired_at": None,
    }


def _meta(doc: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {k: e.get(k) for k in ("key_id", "generation", "origin", "created_at", "retired_at", "lost")}
        for e in sorted(doc.get("keys") or [], key=lambda e: str(e.get("key_id") or ""))
    ]


def keyring_seal(doc: dict[str, Any], ns: str) -> str:
    """MAC over the keyring's metadata (ids, generations, origins, dates, current id)."""
    import json

    body = json.dumps({"ns": ns, "current": doc.get("current_key_id"), "keys": _meta(doc)},
                      sort_keys=True, separators=(",", ":"), default=str)
    return _mac("keyring", body, ns=ns)


def _derive_or_none(generation: Any, ns: str, kid: str) -> bytes | None:
    try:
        return derive_key(int(generation), ns)
    except Exception as exc:  # noqa: BLE001 - reported per entry; the others still load
        logger.error("audit chain key %s could not be derived: %s", kid, type(exc).__name__)
        return None


def _decrypt_all(doc: dict[str, Any], ns: str = "") -> dict[str, bytes]:
    from services.secret_vault import decrypt_secret

    keys: dict[str, bytes] = {}
    for entry in doc.get("keys") or []:
        kid = str(entry.get("key_id") or "")
        if entry.get("generation"):
            key = _derive_or_none(entry["generation"], ns, kid)
            if key is None:
                continue
            if _key_id(key) == kid:
                keys[kid] = key
            else:
                logger.error("derived audit chain key %s does not match its id (secrets key changed?)", kid)
            continue
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


def _queue_seal_record(doc: dict[str, Any]) -> None:
    """Sign the keyring's key set into the chain (written after the append lock)."""
    _pending_seal["keys"] = [
        {k: e.get(k) for k in ("key_id", "generation", "origin")} for e in _meta(doc)
    ]


def flush_seal_record() -> None:
    """Write a queued ``audit.keyring.sealed`` record (audit_log, outside its lock)."""
    keys = _pending_seal["keys"]
    if not keys or getattr(_creating, "active", False):
        return
    _pending_seal["keys"] = None
    from services.audit_log import append_audit_event

    _creating.active = True
    try:
        append_audit_event(action=KEYRING_SEAL_ACTION, resource="audit_chain_keys",
                           details={"keys": keys})
    except Exception as exc:  # noqa: BLE001 - re-queued for the next write
        _pending_seal["keys"] = keys
        logger.warning("audit keyring seal record not written: %s", type(exc).__name__)
    finally:
        _creating.active = False


def _reseal(coll: Any) -> dict[str, Any] | None:
    """Re-MAC the document after one of this module's own changes."""
    doc = coll.find_one({"_id": DOC_ID})
    if not isinstance(doc, dict):
        return None
    coll.update_one({"_id": DOC_ID, "revision": doc.get("revision")},
                    {"$set": {"seal": keyring_seal(doc, _ns(coll))}})
    _queue_seal_record(doc)
    return coll.find_one({"_id": DOC_ID})


def _read(coll: Any) -> dict[str, Any] | None:
    doc = coll.find_one({"_id": DOC_ID})
    if not isinstance(doc, dict):
        return None
    doc = dict(doc)
    import hmac

    if not doc.get("seal"):
        doc = _upgrade(coll, doc) or doc
    seal_ok = bool(doc.get("seal")) and hmac.compare_digest(
        keyring_seal(doc, _ns(coll)), str(doc.get("seal") or ""))
    with _lock:
        _cache.update({"doc": doc, "keys": _decrypt_all(doc, _ns(coll)), "read_at": time.monotonic(),
                       "seal_ok": seal_ok})
    return doc


def _upgrade(coll: Any, doc: dict[str, Any]) -> dict[str, Any] | None:
    """A keyring from before derived keys: seal it, and sign with a derived key from now on.

    Its random keys stay decryptable from ``key_enc``; their ids are sealed
    and signed into the chain (``audit.keyring.sealed``, signed by the new
    derived key), so pulling one later reads as tampering, not a lost key.
    """
    ns = _ns(coll)
    keys = [dict(e) for e in doc.get("keys") or []]
    if not any(e.get("generation") for e in keys):
        when = _now()
        for e in keys:
            if not e.get("retired_at"):
                e["retired_at"] = when
        gen = 1
        entry = _derived_entry(gen, ns, origin="derived_upgrade", actor="system")
        keys.append(entry)
        updated = {**{k: v for k, v in doc.items() if k != "_id"}, "keys": keys,
                   "current_key_id": entry["key_id"], "revision": int(doc.get("revision") or 0) + 1}
    else:
        updated = {k: v for k, v in doc.items() if k != "_id"}
    updated["seal"] = keyring_seal(updated, ns)
    result = coll.replace_one({"_id": DOC_ID, "revision": doc.get("revision"), "seal": {"$exists": False}},
                              {"_id": DOC_ID, **updated})
    if result.matched_count == 1:
        _queue_seal_record(updated)
        logger.warning("audit chain keyring upgraded to derived keys and sealed")
    return coll.find_one({"_id": DOC_ID})


def keyring_state() -> dict[str, Any]:
    """What the verifier needs about the keyring: presence, seal, per-entry checks."""
    import hmac

    coll = _collection()
    if coll is None:
        return {"configured": False}
    ns = _ns(coll)
    doc = coll.find_one({"_id": DOC_ID})
    if not isinstance(doc, dict):
        return {"configured": True, "present": False, "ns": ns}
    keys = _decrypt_all(doc, ns)
    entries = _meta(doc)
    return {
        "configured": True, "present": True, "ns": ns,
        "seal_ok": bool(doc.get("seal")) and hmac.compare_digest(
            keyring_seal(doc, ns), str(doc.get("seal") or "")),
        "entries": entries,
        "readable": sorted(keys),
        "unreadable": sorted(e["key_id"] for e in entries if e["key_id"] not in keys and not e.get("lost")),
        "max_generation": max([int(e.get("generation") or 0) for e in entries] or [0]),
    }


def rekey_after_secrets_change(*, actor: str) -> dict[str, Any]:
    """After a vault secrets-key change (re-anchor only): every entry that no
    longer derives or decrypts is marked lost, and a new derived generation
    under the new master becomes current. Sealed and signed like any change."""
    coll = _collection()
    if coll is None:
        raise KeystoreUnavailable("no control-plane database to keep the keyring in")
    doc = coll.find_one({"_id": DOC_ID})
    if not isinstance(doc, dict):
        return {}
    ns = _ns(coll)
    readable = _decrypt_all(doc, ns)
    keys = [dict(e) for e in doc.get("keys") or []]
    when = _now()
    for e in keys:
        if e.get("key_id") not in readable and not e.get("lost"):
            e["lost"] = "secrets_key_changed"
            e["retired_at"] = e.get("retired_at") or when
    gen = max([int(e.get("generation") or 0) for e in keys] or [0]) + 1
    entry = _derived_entry(gen, ns, origin="rekey_after_secrets_change", actor=actor)
    updated = {**{k: v for k, v in doc.items() if k != "_id"}, "keys": [*keys, entry],
               "current_key_id": entry["key_id"], "revision": int(doc.get("revision") or 0) + 1}
    updated["seal"] = keyring_seal(updated, ns)
    coll.replace_one({"_id": DOC_ID, "revision": doc.get("revision")}, {"_id": DOC_ID, **updated})
    _queue_seal_record(updated)
    reset_cache()
    return {"current_key_id": entry["key_id"],
            "lost": sorted(str(e["key_id"]) for e in keys if e.get("lost"))}


def _create(coll: Any, local_key: bytes | None) -> dict[str, Any]:
    """Create the keyring once; a racing process adopts whichever insert won."""
    from pymongo.errors import DuplicateKeyError

    ns = _ns(coll)
    keys = []
    if local_key:
        # The old node-local key file: imported (retired), still verifiable.
        legacy = _entry(local_key, origin="imported_local_file", actor="system")
        legacy["retired_at"] = legacy["created_at"]
        keys.append(legacy)
    first = _derived_entry(1, ns, origin="derived", actor="system")
    keys.append(first)
    doc = {"_id": DOC_ID, "current_key_id": first["key_id"], "keys": keys,
           "revision": 1, "created_at": _now()}
    doc["seal"] = keyring_seal(doc, ns)
    try:
        coll.insert_one(doc)
        _queue_seal_record(doc)
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
    result = coll.update_one(
        {"_id": DOC_ID, "keys.key_id": {"$ne": entry["key_id"]}},
        {"$push": {"keys": entry}, "$inc": {"revision": 1}},
    )
    if result.modified_count:
        _reseal(coll)


def _fresh(max_age: float) -> dict[str, Any] | None:
    doc = _cache["doc"]
    if doc is not None and time.monotonic() - float(_cache["read_at"]) < max_age:
        return doc
    return None


def current_key(local_key: bytes | None = None) -> tuple[str, bytes] | None:
    """``(key_id, key)`` new records are signed with, or ``None`` without a database.

    Creates the keyring on first use (a derived generation-1 key); imports
    ``local_key`` (the old node-local key file, if this node still has one)
    the first time this process sees it. Raises :class:`KeystoreUnavailable`
    when the database is configured but the keyring cannot be read or created
    and no cached copy exists.
    """
    coll = _collection()
    if coll is None:
        return None
    _bound(coll)
    try:
        doc = _fresh(CACHE_TTL_S)
        if doc is None:
            doc = _read(coll) or _create(coll, local_key)
        if local_key and _cache.get("seal_ok", True):
            # Never into a keyring whose seal fails: re-sealing it would bless
            # whatever broke it (an edit, or a changed vault master).
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
    """Every persisted key by id, plus every derived generation (re-derived,
    so a pulled or deleted keyring entry still verifies). ``missing`` names an
    id a caller could not find; the keyring is then re-read (at most every
    :data:`MISS_REFRESH_S`)."""
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
    upto = max([int(e.get("generation") or 0) for e in (_cache["doc"] or {}).get("keys") or []] or [0])
    out = derived_keys(_ns(coll), upto)
    out.update(_cache["keys"])
    return out


def rotate(*, actor: str, local_key: bytes | None = None) -> dict[str, Any]:
    """Start a new current key (the next derived generation); the previous one
    stays on the keyring, retired, and keeps verifying."""
    coll = _collection()
    if coll is None:
        raise KeystoreUnavailable("no control-plane database to keep the keyring in")
    _bound(coll)
    try:
        current_key(local_key)
    except KeystoreUnavailable:
        if _read(coll) is None:
            raise
    ns = _ns(coll)
    for _attempt in range(3):
        doc = _read(coll) or {}
        previous = str(doc.get("current_key_id") or "")
        keys = [dict(e) for e in doc.get("keys") or []]
        gen = max([int(e.get("generation") or 0) for e in keys] or [0]) + 1
        entry = _derived_entry(gen, ns, origin="rotation", actor=actor)
        when = entry["created_at"]
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
        updated["seal"] = keyring_seal(updated, ns)
        # Compare-and-swap on the revision: a concurrent rotation or import
        # makes this match nothing, and the loop re-reads instead of losing it.
        result = coll.replace_one(
            {"_id": DOC_ID, "current_key_id": previous, "revision": doc.get("revision")},
            {"_id": DOC_ID, **updated},
        )
        if result.matched_count == 1:
            _queue_seal_record(updated)
            _read(coll)
            logger.warning("audit chain key rotated: %s -> %s by %s", previous, entry["key_id"], actor)
            return status()
    raise KeystoreUnavailable("audit chain key rotation lost three races; try again")


HISTORY_COLLECTION = "audit_chain_key_history"
_MAC_INFO = b"audit-key-history/v1"
#: A configured key's "last signed" stamp is refreshed at most this often.
HISTORY_REFRESH_S = 3600.0
_noted: dict[str, float] = {}
_EPOCH_ID = "__history_epoch__"
_epoch_cache: dict[str, dict[str, str]] = {}


def _mac(*parts: str, ns: str | None = None) -> str:
    """HMAC-SHA256 under a key HKDF-derived from the vault master key.

    Integrity only: a database-level writer cannot compute it, and no vault
    ciphertext (a connector password the attacker chose) can stand in for it
    — the earlier seal was ``encrypt_secret``, whose label the Fernet vault
    ignores (third review).
    """
    import hashlib
    import hmac

    from services.secret_vault import derive_subkey

    if ns is None:
        coll = _history_coll()
        ns = str(getattr(coll, "full_name", "") or "")
    msg = "\x1f".join(("v2", ns) + parts).encode("utf-8")
    return hmac.new(derive_subkey(_MAC_INFO), msg, hashlib.sha256).hexdigest()


def _entry_mac(key_id: str, origin: str, first_seen: str, last_seen: str) -> str:
    return _mac("entry", key_id, origin, first_seen, last_seen)


def _valid(doc: dict[str, Any]) -> dict[str, Any] | None:
    """The entry when its MAC matches its fields; None for a forged or edited row."""
    import hmac

    kid = str(doc.get("_id") or "")
    origin = str(doc.get("origin") or "")
    first = str(doc.get("first_seen") or "")
    last = str(doc.get("last_seen") or "")
    try:
        expected = _entry_mac(kid, origin, first, last)
    except Exception:  # noqa: BLE001 - no MAC key: nothing can be trusted
        return None
    if not hmac.compare_digest(expected, str(doc.get("mac") or "")):
        return None
    return {"key_id": kid, "origin": origin, "first_seen": first, "last_seen": last}


def _history_coll() -> Any | None:
    from services.control_plane_store import mongo_collection

    return mongo_collection(HISTORY_COLLECTION)


_note_retry_at: dict[str, float] = {}
#: After a failed history write, the next attempt waits this long, so an
#: outage costs one slow call, not one per audit write.
NOTE_RETRY_S = 10.0


def note_key_id(key_id: str, *, origin: str, seen_at: str | None = None) -> None:
    """Remember, tamper-evidently, that this deployment signed with ``key_id``.

    Ids and times only, each row MAC'd (``_mac``). ``last_seen`` is when the
    key last signed (refreshed at most hourly per process) — a record naming
    the key long after it stopped signing is a relabel, not a lost key. One
    row per key, updated in place. Only once this process knows the epoch
    (``current_position`` ran before the signing lock), so an entry is never
    written ahead of its epoch, and never does the epoch lookup itself (it is
    called under the audit append lock). Never raises.
    """
    if not key_id:
        return
    now_mono = time.monotonic()
    if seen_at is None and now_mono - _noted.get(key_id, -HISTORY_REFRESH_S) < HISTORY_REFRESH_S:
        return
    if now_mono < _note_retry_at.get(key_id, 0.0):
        return
    try:
        coll = _history_coll()
        if coll is None:
            return
        ns = str(getattr(coll, "full_name", "") or id(coll))
        if ns not in _epoch_cache:
            return  # epoch not known yet: noted on a later write
        when = seen_at or _now()
        current = _valid(coll.find_one({"_id": key_id}) or {}) or {}
        first = str(current.get("first_seen") or when)
        if current.get("last_seen") and str(current["last_seen"]) >= when:
            when = str(current["last_seen"])
        first = min(first, when) if first else when
        coll.update_one(
            {"_id": key_id},
            {"$set": {"origin": origin, "first_seen": first, "last_seen": when,
                      "mac": _entry_mac(key_id, origin, first, when)}},
            upsert=True,
        )
        _noted[key_id] = now_mono
    except Exception as exc:  # noqa: BLE001 - signing must never fail on bookkeeping
        _note_retry_at[key_id] = now_mono + NOTE_RETRY_S
        logger.debug("audit key history not recorded: %s", exc)


class HistoryTampered(RuntimeError):
    """The key history's anchor was removed, reset or altered."""


class EpochPending(KeystoreUnavailable):
    """Another process is creating the epoch right now (not a finding)."""


#: Chain actions that anchor the key history (written by this module).
EPOCH_ACTION = "audit.key_history.began"
REANCHOR_ACTION = "audit.key_history.reanchored"
_creating = threading.local()
#: A pending epoch older than this is a crashed creator's: adopted when its
#: anchor record is in the chain, otherwise reported as tampering.
PENDING_STALE_S = 15.0
#: A pending epoch dated more than this into the future was not written by a creator.
PENDING_FUTURE_S = 60.0
#: ``current_position`` re-reads the epoch this often; a failure is retried
#: after :data:`POSITION_RETRY_S` (negative cache), so an outage costs one call.
POSITION_TTL_S = 30.0
POSITION_RETRY_S = 10.0
_position_cache: dict[str, Any] = {"ns": None, "value": None, "until": 0.0}


def _epoch_mac(at: str, head_seq: str, head_hash: str, epoch_id: str) -> str:
    return _mac("epoch", at, head_seq, head_hash, epoch_id)


def _copy_mac(at: str, head_seq: str, head_hash: str, epoch_id: str) -> str:
    return _mac("epoch-copy", at, head_seq, head_hash, epoch_id)


_EPOCH_FIELDS = ("at", "head_seq", "head_hash", "epoch_id")


def _epoch_copy_in_keyring(coll_keys: Any) -> dict[str, str] | None:
    """The second copy of the epoch, MAC'd, on the keyring document."""
    import hmac

    doc = coll_keys.find_one({"_id": DOC_ID}, {"history_epoch": 1}) if coll_keys is not None else None
    copy = (doc or {}).get("history_epoch") or {}
    vals = [str(copy.get(k) or "") for k in _EPOCH_FIELDS]
    if vals[0] and hmac.compare_digest(_copy_mac(*vals), str(copy.get("mac") or "")):
        return dict(zip(_EPOCH_FIELDS, vals))
    return None


def _signed_key_set() -> list[dict[str, Any]]:
    """The keyring's key ids, generations and origins, as the anchor records sign them."""
    try:
        coll = _collection()
        doc = coll.find_one({"_id": DOC_ID}) if coll is not None else None
    except Exception:  # noqa: BLE001
        doc = None
    return [{k: e.get(k) for k in ("key_id", "generation", "origin")} for e in _meta(doc or {})]


def _anchor_in_chain(
    action: str, at: str, epoch_id: str, actor: str = "system", reason: str = "",
    extra: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Write the signed chain record that anchors the history; ``(seq, hash)``.

    The record carries its own position as its signed ``kh_seq`` and the
    ``epoch_id`` in its signed ``kh_epoch``: it is the anchor every later
    record points at.
    """
    from services.audit_log import append_audit_event

    _creating.active = True
    try:
        event = append_audit_event(
            action=action, resource="audit_chain_key_history", actor=actor,
            details={"epoch_at": at, "epoch_id": epoch_id, **({"reason": reason} if reason else {}),
                     "keyring": _signed_key_set(), **(extra or {})},
            key_history_anchor=epoch_id,
        )
    finally:
        _creating.active = False
    return str(event.get("chain_seq") or ""), str(event.get("event_hash") or "")


def _store_epoch(coll: Any, keys: Any, rec: dict[str, str]) -> None:
    vals = [str(rec.get(k) or "") for k in _EPOCH_FIELDS]
    coll.update_one(
        {"_id": _EPOCH_ID},
        {"$set": {**dict(zip(_EPOCH_FIELDS, vals)), "mac": _epoch_mac(*vals)},
         "$unset": {"pending": ""}},
        upsert=True,
    )
    if keys is not None:
        keys.update_one(
            {"_id": DOC_ID},
            {"$set": {"history_epoch": {**dict(zip(_EPOCH_FIELDS, vals)), "mac": _copy_mac(*vals)}}},
        )


def _anchor_pending(coll: Any, keys: Any, at: str, epoch_id: str) -> dict[str, str]:
    """This process holds the pending epoch row: anchor it in the chain."""
    try:
        seq, head = _anchor_in_chain(EPOCH_ACTION, at, epoch_id)
    except Exception:
        # Release the claim, so the next writer anchors it instead.
        coll.delete_one({"_id": _EPOCH_ID, "pending": True, "epoch_id": epoch_id})
        raise
    rec = {"at": at, "head_seq": seq, "head_hash": head, "epoch_id": epoch_id}
    _store_epoch(coll, keys, rec)
    return rec


def _verified_anchor_for(epoch_id: str) -> dict[str, Any] | None:
    """The verified chain record that anchors ``epoch_id``, if one was written."""
    from services.audit_chain_keys import check_signature
    from services.audit_log import _hmac_event_hash
    from services.control_plane_store import mongo_collection

    events = mongo_collection("audit_events")
    if events is None or not epoch_id:
        return None
    for doc in events.find({"kh_epoch": epoch_id}).sort("chain_seq", 1).limit(5):
        seq = doc.get("chain_seq")
        if (str(doc.get("kh_seq")) == str(seq) and doc.get("event_hash")
                and check_signature(doc, _hmac_event_hash)[0] == "ok"):
            return doc
    return None


def epoch_record(*, fresh: bool = False) -> dict[str, str] | None:
    """``{at, head_seq, head_hash, epoch_id}`` of the history epoch, or None (no database).

    The epoch is anchored IN the hash chain: creating it writes a signed
    ``audit.key_history.began`` record whose signed ``kh_seq`` is its own
    position, and every later record signs that position (``kh_seq``) and the
    epoch id (``kh_epoch``). The MAC'd epoch row and its MAC'd copy on the
    keyring name the anchor; the verifier (``evidence_chain``) judges where
    the history began from VERIFIED records' signed ``kh_seq`` only.

    Creation is claimed by an atomic insert of a ``pending`` row (with the
    epoch id); a process that finds it pending does not wait: it raises
    :class:`EpochPending`. A pending row dated in the future, or older than
    :data:`PENDING_STALE_S` with no verified anchor record for its epoch id,
    is :class:`HistoryTampered`; a stale one whose anchor IS in the chain (the
    creator crashed after writing it) is adopted, never re-anchored. Also
    tampered: the row failing its MAC, disagreeing with its copy, or missing
    while entries or the copy exist (re-read once first, F5).
    """
    import hmac
    import uuid

    if getattr(_creating, "active", False):
        return None
    coll = _history_coll()
    if coll is None:
        return None
    ns = str(getattr(coll, "full_name", "") or id(coll))
    if ns in _epoch_cache and not fresh:
        return dict(_epoch_cache[ns])
    keys = _collection()
    missing_seen = False
    while True:
        doc = coll.find_one({"_id": _EPOCH_ID})
        copy = _epoch_copy_in_keyring(keys)
        if doc is None:
            has_entries = any(_valid(d) for d in coll.find({"_id": {"$ne": _EPOCH_ID}}).limit(50))
            if has_entries or copy:
                if not missing_seen:
                    missing_seen = True  # F5: another worker may be mid-write
                    time.sleep(0.25)
                    continue
                raise HistoryTampered("the key history epoch was removed")
            at, eid = _now(), uuid.uuid4().hex
            won = coll.update_one(
                {"_id": _EPOCH_ID},
                {"$setOnInsert": {"at": at, "pending": True, "epoch_id": eid}}, upsert=True,
            )
            if getattr(won, "upserted_id", None) is None:
                raise EpochPending("another process is creating the key history epoch")
            rec = _anchor_pending(coll, keys, at, eid)
            break
        if doc.get("pending") and not doc.get("mac"):
            at, eid = str(doc.get("at") or ""), str(doc.get("epoch_id") or "")
            age = (datetime.now(timezone.utc) - _parse_time(at)).total_seconds()
            if age < -PENDING_FUTURE_S:
                raise HistoryTampered("a pending key history epoch is dated in the future")
            if age <= PENDING_STALE_S:
                raise EpochPending("another process is creating the key history epoch")
            anchor = _verified_anchor_for(eid)
            if anchor is None:
                raise HistoryTampered(
                    "a pending key history epoch was left unfinished with no anchor record"
                )
            # The creator crashed after anchoring: adopt its record (no reset).
            rec = {"at": at, "head_seq": str(anchor.get("chain_seq") or ""),
                   "head_hash": str(anchor.get("event_hash") or ""), "epoch_id": eid}
            _store_epoch(coll, keys, rec)
            break
        vals = [str(doc.get(k) or "") for k in _EPOCH_FIELDS]
        if not vals[0] or not hmac.compare_digest(_epoch_mac(*vals), str(doc.get("mac") or "")):
            raise HistoryTampered("the key history epoch fails its MAC")
        rec = dict(zip(_EPOCH_FIELDS, vals))
        if copy and copy != rec:
            raise HistoryTampered("the key history epoch disagrees with its keyring copy")
        if copy is None and keys is not None:
            keys.update_one(
                {"_id": DOC_ID, "history_epoch": {"$exists": False}},
                {"$set": {"history_epoch": {**rec, "mac": _copy_mac(*vals)}}},
            )
        break
    _epoch_cache[ns] = rec
    return dict(rec)


def current_position() -> dict[str, Any] | None:
    """``{kh_seq, kh_epoch}`` every new audit record signs, or None.

    Called by ``append_audit_event`` BEFORE it takes the append lock, never
    on the signing path. Cached for :data:`POSITION_TTL_S`; a failure (no
    database, pending epoch, unreadable or tampered history) is cached for
    :data:`POSITION_RETRY_S`, so N writes during an outage pay for one lookup.
    Never raises.
    """
    if getattr(_creating, "active", False):
        return None
    now = time.monotonic()
    try:
        coll = _history_coll()
    except Exception:  # noqa: BLE001
        coll = None
    ns = str(getattr(coll, "full_name", "") or id(coll)) if coll is not None else None
    if _position_cache["ns"] == ns and now < _position_cache["until"]:
        return _position_cache["value"]
    value: dict[str, Any] | None = None
    ttl = POSITION_RETRY_S
    try:
        rec = epoch_record(fresh=True) if coll is not None else None
        if rec and str(rec.get("head_seq") or "").isdigit():
            value = {"kh_seq": int(rec["head_seq"]), "kh_epoch": str(rec.get("epoch_id") or "")}
            ttl = POSITION_TTL_S
    except HistoryTampered as exc:
        logger.error("audit key history: %s", exc)
    except Exception as exc:  # noqa: BLE001 - audit writes never wait on the history
        logger.warning("audit key history unavailable, records go unanchored: %s", type(exc).__name__)
    _position_cache.update({"ns": ns, "value": value, "until": now + ttl})
    return value


def _parse_time(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, timezone.utc)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def history_epoch() -> str | None:
    """When this deployment began keeping the key history (ISO time), or None."""
    rec = epoch_record()
    return rec["at"] if rec else None


def reanchor_key_history(
    *, actor: str, reason: str, dry_run: bool = False,
    findings: list[dict[str, Any]] | None = None, history_seq: int = 0,
    superseded: list[int] | None = None, history_problem: str = "",
) -> dict[str, Any]:
    """Start the key history over (after a ``DATAFLOW_SECRETS_KEY`` change).

    Platform-admin action (``POST /api/v1/audit/chain/key-history/reanchor``,
    which runs ``verify_chain`` first and passes its tampering findings and
    the history's position here). It writes the signed
    ``audit.key_history.reanchored`` chain record whose signed details carry
    the previous ``history_seq``, every superseded ``kh_seq``, the history
    problem it resolves and the tampering findings at that moment: the
    verifier keeps surfacing those as ``prior_tampering``, so a re-anchor
    never whitewashes them. Entries whose MAC fails are re-sealed with
    ``last_seen`` = now (their original times cannot be trusted), so records
    before the re-anchor naming those keys read as lost keys and later ones
    as relabels. ``dry_run`` reports what it would do.

    Also the upgrade path from the a350ff42a row format (never released;
    test deployments only): those rows fail the current MAC and read as
    tampered until a platform admin re-anchors.
    """
    import uuid

    coll = _history_coll()
    if coll is None:
        raise KeystoreUnavailable("no control-plane database holds the key history")
    keys = _collection()
    stale = [d for d in coll.find({"_id": {"$ne": _EPOCH_ID}}) if _valid(d) is None]
    plan: dict[str, Any] = {
        "dry_run": dry_run, "history_problem": history_problem or None,
        "history_seq": history_seq, "entries_resealed": sorted(str(d["_id"]) for d in stale),
        "entries_kept": len(key_history()), "prior_tampering": list(findings or []),
    }
    if dry_run:
        return plan
    at, eid = _now(), uuid.uuid4().hex
    seq, head = _anchor_in_chain(
        REANCHOR_ACTION, at, eid, actor=actor, reason=reason,
        extra={"prev_history_seq": int(history_seq or 0),
               "superseded_kh_seq": sorted({int(x) for x in (superseded or []) if x}),
               "history_problem": history_problem, "findings": list(findings or [])[:500],
               "resealed_key_ids": plan["entries_resealed"]},
    )
    for doc in stale:
        kid = str(doc["_id"])
        coll.update_one(
            {"_id": kid},
            {"$set": {"origin": "reanchored", "first_seen": "", "last_seen": at,
                      "mac": _entry_mac(kid, "reanchored", "", at)}},
        )
    rec = {"at": at, "head_seq": seq, "head_hash": head, "epoch_id": eid}
    _store_epoch(coll, keys, rec)
    _epoch_cache.clear()
    _position_cache.update({"ns": None, "value": None, "until": 0.0})
    logger.warning("audit key history re-anchored at chain position %s by %s", seq, actor)
    return {**plan, "epoch_after": rec}


def key_history(*, strict: bool = False) -> dict[str, dict[str, Any]]:
    """MAC-valid history entries by key id (forged or edited rows left out).

    ``strict``: an unreadable history raises instead of reading as empty
    (the verifier must not judge keys on a history it could not read).
    """
    try:
        coll = _history_coll()
        if coll is None:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for doc in coll.find({"_id": {"$ne": _EPOCH_ID}}):
            entry = _valid(doc)
            if entry is not None:
                out[entry["key_id"]] = entry
        return out
    except Exception:  # noqa: BLE001
        if strict:
            raise
        return {}


def noted_key_ids() -> set[str]:
    """Every key id the history holds."""
    return set(key_history())


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
