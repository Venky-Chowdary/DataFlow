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
        _noted.clear()
        _epoch_cache.clear()


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


HISTORY_COLLECTION = "audit_chain_key_history"
_MAC_INFO = b"audit-key-history/v1"
#: A configured key's "last signed" stamp is refreshed at most this often.
HISTORY_REFRESH_S = 3600.0
_noted: dict[str, float] = {}
_EPOCH_ID = "__history_epoch__"
_epoch_cache: dict[str, dict[str, str]] = {}


def _mac(*parts: str) -> str:
    """HMAC-SHA256 under a key HKDF-derived from the vault master key.

    Integrity only: a database-level writer cannot compute it, and no vault
    ciphertext (a connector password the attacker chose) can stand in for it
    — the earlier seal was ``encrypt_secret``, whose label the Fernet vault
    ignores (third review).
    """
    import hashlib
    import hmac

    from services.secret_vault import derive_subkey

    msg = "\x1f".join(("v1",) + parts).encode("utf-8")
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


def note_key_id(key_id: str, *, origin: str, seen_at: str | None = None) -> None:
    """Remember, tamper-evidently, that this deployment signed with ``key_id``.

    Ids and times only, each row MAC'd (``_mac``). ``last_seen`` is when the
    key last signed (refreshed at most hourly per process) — a record naming
    the key long after it stopped signing is a relabel, not a lost key. One
    row per key, updated in place (no new secret per call). Never raises.
    """
    if not key_id:
        return
    now_mono = time.monotonic()
    if seen_at is None and now_mono - _noted.get(key_id, -HISTORY_REFRESH_S) < HISTORY_REFRESH_S:
        return
    try:
        coll = _history_coll()
        if coll is None:
            return
        history_epoch()
        when = seen_at or _now()
        current = _valid(coll.find_one({"_id": key_id}) or {}) or {}
        first = str(current.get("first_seen") or when)
        if current.get("last_seen") and str(current["last_seen"]) >= when:
            when = str(current["last_seen"])
        first = min(first, when)
        coll.update_one(
            {"_id": key_id},
            {"$set": {"origin": origin, "first_seen": first, "last_seen": when,
                      "mac": _entry_mac(key_id, origin, first, when)}},
            upsert=True,
        )
        _noted[key_id] = now_mono
    except Exception as exc:  # noqa: BLE001 - signing must never fail on bookkeeping
        logger.debug("audit key history not recorded: %s", exc)


class HistoryTampered(RuntimeError):
    """The key history's anchor was removed, reset or altered."""


#: Chain actions that anchor the key history (written by this module).
EPOCH_ACTION = "audit.key_history.began"
REANCHOR_ACTION = "audit.key_history.reanchored"
_creating = threading.local()


def _epoch_mac(at: str, head_seq: str, head_hash: str) -> str:
    return _mac("epoch", at, head_seq, head_hash)


def _epoch_copy_in_keyring(coll_keys: Any) -> dict[str, str] | None:
    """The second copy of the epoch, MAC'd, on the keyring document."""
    import hmac

    doc = coll_keys.find_one({"_id": DOC_ID}, {"history_epoch": 1}) if coll_keys is not None else None
    copy = (doc or {}).get("history_epoch") or {}
    at, seq, head = (str(copy.get(k) or "") for k in ("at", "head_seq", "head_hash"))
    if at and hmac.compare_digest(_mac("epoch-copy", at, seq, head), str(copy.get("mac") or "")):
        return {"at": at, "head_seq": seq, "head_hash": head}
    return None


def _anchor_in_chain(action: str, at: str, actor: str = "system", reason: str = "") -> tuple[str, str]:
    """Write the signed chain record that anchors the history; ``(seq, hash)``."""
    from services.audit_log import append_audit_event

    _creating.active = True
    try:
        event = append_audit_event(
            action=action, resource="audit_chain_key_history", actor=actor,
            details={"epoch_at": at, **({"reason": reason} if reason else {})},
        )
    finally:
        _creating.active = False
    return str(event.get("chain_seq") or ""), str(event.get("event_hash") or "")


def _store_epoch(coll: Any, keys: Any, at: str, seq: str, head: str) -> None:
    coll.update_one(
        {"_id": _EPOCH_ID},
        {"$set": {"at": at, "head_seq": seq, "head_hash": head, "mac": _epoch_mac(at, seq, head)},
         "$unset": {"pending": ""}},
        upsert=True,
    )
    if keys is not None:
        keys.update_one(
            {"_id": DOC_ID},
            {"$set": {"history_epoch": {"at": at, "head_seq": seq, "head_hash": head,
                                        "mac": _mac("epoch-copy", at, seq, head)}}},
        )


#: A pending epoch (its creator is writing the anchor record) is waited for this
#: long; older than :data:`PENDING_STALE_S`, another process takes it over.
PENDING_WAIT_S = 3.0
PENDING_STALE_S = 15.0


def _anchor_pending(coll: Any, keys: Any, at: str) -> dict[str, str]:
    """This process holds the pending epoch row: anchor it in the chain."""
    try:
        seq, head = _anchor_in_chain(EPOCH_ACTION, at)
    except Exception:
        # Release the claim, so the next writer anchors it instead of waiting.
        coll.delete_one({"_id": _EPOCH_ID, "pending": True, "at": at})
        raise
    _store_epoch(coll, keys, at, seq, head)
    return {"at": at, "head_seq": seq, "head_hash": head}


def epoch_record() -> dict[str, str] | None:
    """``{at, head_seq, head_hash}`` of the history epoch, or None (no database).

    The epoch is anchored IN the hash chain: creating it writes a signed
    ``audit.key_history.began`` record whose position and hash the MAC'd epoch
    row (and its MAC'd copy on the keyring document) store. Deleting the
    history and the copy recreates the epoch at a later position, and the
    verifier finds the earlier anchor record (review of a350ff42a, F1).

    Creation is claimed by an atomic insert of a ``pending`` row; other
    processes wait for it (a stale claim is taken over). Raises
    :class:`HistoryTampered` when the row fails its MAC, disagrees with its
    copy, or is missing while entries or the copy exist.
    """
    import hmac

    if getattr(_creating, "active", False):
        return None
    coll = _history_coll()
    if coll is None:
        return None
    ns = str(getattr(coll, "full_name", "") or id(coll))
    if ns in _epoch_cache:
        return dict(_epoch_cache[ns])
    keys = _collection()
    started = time.monotonic()
    while True:
        doc = coll.find_one({"_id": _EPOCH_ID})
        copy = _epoch_copy_in_keyring(keys)
        if doc is None:
            has_entries = any(_valid(d) for d in coll.find({"_id": {"$ne": _EPOCH_ID}}).limit(50))
            if has_entries or copy:
                raise HistoryTampered("the key history epoch was removed")
            at = _now()
            won = coll.update_one(
                {"_id": _EPOCH_ID}, {"$setOnInsert": {"at": at, "pending": True}}, upsert=True
            )
            if getattr(won, "upserted_id", None) is None:
                continue  # another process claimed it first: wait for its anchor
            rec = _anchor_pending(coll, keys, at)
            break
        if doc.get("pending") and not doc.get("mac"):
            at = str(doc.get("at") or "")
            age = (datetime.now(timezone.utc) - _parse_time(at)).total_seconds() if at else 1e9
            if age > PENDING_STALE_S:
                taken = coll.update_one(
                    {"_id": _EPOCH_ID, "pending": True, "at": doc.get("at"), "mac": {"$exists": False}},
                    {"$set": {"at": _now()}},
                )
                if taken.modified_count == 1:
                    fresh = coll.find_one({"_id": _EPOCH_ID}) or {}
                    rec = _anchor_pending(coll, keys, str(fresh.get("at") or ""))
                    break
            elif time.monotonic() - started > PENDING_WAIT_S:
                raise KeystoreUnavailable("the key history epoch is still being created")
            time.sleep(0.1)
            continue
        at, seq, head = (str(doc.get(k) or "") for k in ("at", "head_seq", "head_hash"))
        if not at or not hmac.compare_digest(_epoch_mac(at, seq, head), str(doc.get("mac") or "")):
            raise HistoryTampered("the key history epoch fails its MAC")
        if copy and (copy["at"], copy["head_seq"], copy["head_hash"]) != (at, seq, head):
            raise HistoryTampered("the key history epoch disagrees with its keyring copy")
        if copy is None and keys is not None:
            keys.update_one(
                {"_id": DOC_ID, "history_epoch": {"$exists": False}},
                {"$set": {"history_epoch": {"at": at, "head_seq": seq, "head_hash": head,
                                            "mac": _mac("epoch-copy", at, seq, head)}}},
            )
        rec = {"at": at, "head_seq": seq, "head_hash": head}
        break
    _epoch_cache[ns] = rec
    return dict(rec)


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


def key_history() -> dict[str, dict[str, Any]]:
    """MAC-valid history entries by key id (forged or edited rows left out)."""
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
