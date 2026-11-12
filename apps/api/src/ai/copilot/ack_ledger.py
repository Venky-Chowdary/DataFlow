"""Server-side mutation approval ledger for Datawrap Pilot.

Create-connector drafts (with secrets) stay on the server. The UI only
receives an ack_id + safe preview; Confirm consumes the ledger once.

At rest (the ledger file, or the shared Mongo collection) a pending payload is
sealed by the connector-secret vault — never plaintext. Each approval records
the workspace it was staged in and who staged it; a caller bound to a
workspace that may not read that one is told the approval does not exist
(audit L4).
"""

from __future__ import annotations

import re

import json
import logging
import os
from services.brand_env import getenv_brand
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from .sealed_state import owner_stamp, readable_by_caller, seal, unseal

_log = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 15 * 60  # 15 minutes
def _secret_keys() -> frozenset[str]:
    # The connector store's own list is the authority (secret_access_key,
    # client_secret, refresh_token, ...): one list, not two that drift.
    from services.connector_store import _CONNECTOR_SECRET_KEYS

    return frozenset(_CONNECTOR_SECRET_KEYS) | {"passwd", "pwd", "secret"}


_SECRET_KEYS = _secret_keys()
#: Secret-shaped names beyond the list: aws_secret_access_key, sas_token,
#: ssh_password, passphrase, access_token, bearer, a DSN or a URI that may
#: carry userinfo (review, low: the list was narrower than real payloads).
_SECRET_NAME = re.compile(
    r"(?:pass(?:word|wd|phrase)|pwd|secret|token|bearer|credential|private_key|api_?key|"
    r"access_key|sas|^dsn$|^uri$|_uri$|^url$|connection_string)",
    re.I,
)


def _secret_name(key: str) -> bool:
    low = str(key).lower()
    return low in _SECRET_KEYS or bool(_SECRET_NAME.search(low))
_OBJECT_STORE_TYPES = frozenset({
    "s3", "amazon_s3", "aws_s3", "minio", "s3_compatible",
    "gcs", "google_cloud_storage", "adls", "azure_blob_storage",
    "azure_data_lake", "azure_data_lake_storage",
})


def _default_path() -> Path:
    override = getenv_brand("PILOT_ACK_PATH", "").strip()
    if override:
        return Path(override)
    from services.platform_config import data_dir
    return data_dir() / "pilot_acks.json"


def _now() -> float:
    return time.time()


def _new_id() -> str:
    return f"ack_{uuid.uuid4().hex[:20]}"


def approval_restage_message(kind: str | None, *, expired: bool) -> str:
    """Say which action to stage again. A transfer ack is not a connector create."""
    token = (kind or "").strip()
    if token == "start_transfer":
        action = "plan the transfer again"
    elif token == "sync_now":
        action = "stage Sync now again"
    elif token in {
        "create_schedule",
        "run_schedule",
        "update_schedule",
        "delete_schedule",
        "set_schedule_enabled",
    }:
        action = "set the schedule again"
    elif token == "create_connector":
        action = "create the connector again"
    elif token in {"delete_connector", "update_connector", "test_connector"}:
        action = "repeat that connector action"
    else:
        action = "stage the action again"
    if expired and token:
        return f"Approval expired. Ask Pilot to {action}."
    return f"Approval not found or expired. Ask Pilot to {action}."


def _object_store_payload(payload: dict[str, Any]) -> bool:
    kind = str(payload.get("type") or payload.get("format") or payload.get("db_type") or "")
    return kind.strip().lower().replace("-", "_") in _OBJECT_STORE_TYPES


def redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Safe client-facing copy — never echo secrets."""
    hide_access_key = _object_store_payload(payload)
    out: dict[str, Any] = {}
    for k, v in (payload or {}).items():
        key = k.lower()
        if _secret_name(key) or (hide_access_key and key == "username"):
            out[k] = "***" if v else ""
            out[f"has_{k}"] = bool(v)
        elif isinstance(v, dict):
            out[k] = redact_payload(v)
        elif isinstance(v, str):
            # A host or endpoint written as a URL carries its password in the
            # userinfo; "host" is not secret-named (review 2, low).
            from services.secret_config import mask_secrets_in_text

            out[k] = mask_secrets_in_text(v)
        else:
            out[k] = v
    return out


def _staged_by() -> dict[str, str]:
    """Where and by whom an approval is being staged (from the bound request)."""
    stamp = owner_stamp()
    return {"workspace_id": stamp["workspace_id"], "staged_by": stamp["actor"]}


def _visible(doc: dict[str, Any] | None) -> bool:
    """Whether the bound caller's workspace scope covers this approval."""
    return bool(doc) and readable_by_caller(doc or {}, same_actor=False)


_SEALED = "__sealed__"


def _is_secret_key(payload: dict[str, Any], key: str) -> bool:
    """The keys ``redact_payload`` hides from a client are the ones sealed at rest."""
    low = str(key).lower()
    return _secret_name(low) or (low == "username" and _object_store_payload(payload))


def _seal_secrets(node: Any) -> Any:
    """``node`` with every secret value replaced by ``{"__sealed__": ciphertext}``.

    Field-level on purpose: the rest of a staged payload (mappings, statistics,
    Decimal and datetime values) keeps its exact types in the store.
    Raises ``LookupError`` when the vault cannot seal.
    """
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for key, value in node.items():
            if value and _is_secret_key(node, key) and not (isinstance(value, dict) and _SEALED in value):
                token = seal(value)
                if token is None:
                    raise LookupError("vault unavailable")
                out[key] = {_SEALED: token}
            else:
                out[key] = _seal_secrets(value)
        return out
    if isinstance(node, list):
        return [_seal_secrets(item) for item in node]
    return node


def _open_secrets(node: Any) -> Any:
    """Inverse of :func:`_seal_secrets`. Raises ``LookupError`` for an unopenable value."""
    if isinstance(node, dict):
        if set(node) == {_SEALED}:
            value = unseal(node[_SEALED])
            if value is None:
                raise LookupError("sealed under another key")
            return value
        return {key: _open_secrets(value) for key, value in node.items()}
    if isinstance(node, list):
        return [_open_secrets(item) for item in node]
    return node


def _has_plain_secret(node: Any) -> bool:
    """Whether a stored payload holds a secret in clear (written before sealing)."""
    if isinstance(node, dict):
        return any(
            (value and _is_secret_key(node, key) and not (isinstance(value, dict) and _SEALED in value))
            or _has_plain_secret(value)
            for key, value in node.items()
        )
    if isinstance(node, list):
        return any(_has_plain_secret(item) for item in node)
    return False


def _sealed_at_rest(doc: dict[str, Any]) -> dict[str, Any] | None:
    """The stored form of an approval: every secret in its payload sealed.

    None when the vault cannot seal — the caller must not store that record.
    """
    try:
        return {**doc, "payload": _seal_secrets(doc.get("payload") or {})}
    except LookupError:
        return None


def _opened(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    """A stored approval with its secrets unsealed (None if they cannot be opened)."""
    if not doc:
        return doc
    try:
        return {**doc, "payload": _open_secrets(doc.get("payload") or {})}
    except LookupError:
        return None


def _safe_peek(doc: dict[str, Any]) -> dict[str, Any]:
    """What a caller may see of an approval before confirming — no secrets."""
    return {
        "ack_id": doc["ack_id"],
        "kind": doc["kind"],
        "preview": dict(doc.get("preview") or {}),
        "expires_at": doc.get("expires_at"),
        "consumed": bool(doc.get("consumed_at")),
        "consumed_at": doc.get("consumed_at"),
        "workspace_id": str(doc.get("workspace_id") or ""),
        "staged_by": str(doc.get("staged_by") or ""),
    }


class PilotAckLedger:
    """One-shot approval records for mutate-risk Pilot actions."""

    def __init__(self, path: Path | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = path or _default_path()
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, Any]] = {}
        #: Approvals another worker wrote to the same file, kept as stored.
        self._carried: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            now = _now()
            legacy = False
            for aid, doc in (raw.get("entries") or {}).items():
                legacy = legacy or _has_plain_secret((doc or {}).get("payload") if isinstance(doc, dict) else None)
                doc = _opened(doc)
                if doc is None:
                    continue  # sealed under a key this process does not hold
                consumed = float(doc.get("consumed_at") or 0)
                exp = float(doc.get("expires_at") or 0)
                if consumed:
                    # Keep successful consumes for ~1h so confirm retries stay idempotent
                    # across API restarts.
                    if consumed + 3600 > now:
                        self._entries[str(aid)] = doc
                elif exp > now:
                    self._entries[str(aid)] = doc
        except (OSError, json.JSONDecodeError, TypeError) as exc:
            _log.warning("pilot ack ledger load failed: %s", exc)
            return
        if legacy:
            # A secret an earlier release wrote in clear is sealed now, not at
            # the next write, which may never come (review 2, low).
            self._persist()

    def _persist(self) -> None:
        from .spill_lock import read_spill, spill_lock

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with spill_lock(self.path):
                self._merge_disk_locked(read_spill(self.path))
                # Keep recently consumed for idempotent replay window. A pending
                # payload is sealed; one the vault cannot seal stays in memory only.
                entries = {
                    aid: stored
                    for aid, doc in self._entries.items()
                    if (stored := _sealed_at_rest(doc)) is not None
                }
                for aid, stored in self._carried.items():
                    entries.setdefault(aid, stored)
                tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
                tmp.write_text(json.dumps({"entries": entries}, default=str), encoding="utf-8")
                tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot ack ledger persist failed: %s", exc)

    def _merge_disk_locked(self, disk: dict[str, Any]) -> None:
        """Fold in another worker's approvals and consumes (review 2, low).

        An approval only this file knows is carried through as stored. A consume
        is one-way: when the file says an approval was used, this worker's
        pending copy takes that, so a later write cannot make it pending again.
        """
        now = _now()
        self._carried = {}
        for aid, stored in (disk.get("entries") or {}).items():
            if not isinstance(stored, dict):
                continue
            consumed = float(stored.get("consumed_at") or 0)
            if (consumed and consumed + 3600 < now) or (not consumed and float(stored.get("expires_at") or 0) <= now):
                continue
            mine = self._entries.get(aid)
            if mine is None:
                self._carried[aid] = stored
            elif consumed and not mine.get("consumed_at"):
                opened = _opened(stored)
                if opened is not None:
                    self._entries[aid] = opened

    def _gc_locked(self) -> None:
        now = _now()
        dead = []
        for k, v in self._entries.items():
            exp = float(v.get("expires_at") or 0)
            consumed = float(v.get("consumed_at") or 0)
            # Drop expired unused, or consumed > 1h ago
            if (not consumed and exp <= now) or (consumed and consumed + 3600 < now):
                dead.append(k)
        for k in dead:
            self._entries.pop(k, None)

    def put(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        preview: dict[str, Any] | None = None,
        ttl_sec: int | None = None,
        actor_hint: str = "",
    ) -> str:
        aid = _new_id()
        ttl = max(60, int(ttl_sec if ttl_sec is not None else self.ttl_sec))
        now = _now()
        doc = {
            "ack_id": aid,
            "kind": kind,
            "payload": dict(payload or {}),
            "preview": dict(preview or redact_payload(payload or {})),
            "created_at": now,
            "expires_at": now + ttl,
            "actor_hint": (actor_hint or "").strip(),
            **_staged_by(),
            "consumed_at": None,
            "consumed_by": None,
            "consume_reason": None,
            "result": None,
        }
        with self._lock:
            self._gc_locked()
            self._entries[aid] = doc
            self._persist()
        return aid

    def peek(self, ack_id: str) -> dict[str, Any] | None:
        """Safe peek — no secrets."""
        with self._lock:
            self._gc_locked()
            doc = self._entries.get((ack_id or "").strip())
            if not _visible(doc):
                return None
            if float(doc.get("expires_at") or 0) <= _now() and not doc.get("consumed_at"):
                return None
            return _safe_peek(doc)

    def _expired_message_locked(self, aid: str) -> str:
        """Drop one expired unused ack and name the action to stage again.

        GC used to delete the document first, so a transfer approval and a
        missing id produced the same "create the connector again" sentence.
        """
        doc = self._entries.get(aid)
        if not doc or doc.get("consumed_at"):
            return ""
        if float(doc.get("expires_at") or 0) > _now():
            return ""
        kind = str(doc.get("kind") or "")
        self._entries.pop(aid, None)
        self._persist()
        return approval_restage_message(kind, expired=True)

    def unusable_reason(self, ack_id: str) -> str:
        """Why Confirm cannot spend this id. Empty when the ack is still spendable."""
        aid = (ack_id or "").strip()
        if not aid:
            return "ack_id required"
        with self._lock:
            expired = self._expired_message_locked(aid)
            if expired:
                return expired
            doc = self._entries.get(aid)
        if not _visible(doc):
            return approval_restage_message(None, expired=False)
        return ""

    def get_pending_payload(self, ack_id: str) -> tuple[dict[str, Any] | None, str]:
        """Return secret payload for a still-pending ack (does not consume)."""
        aid = (ack_id or "").strip()
        if not aid:
            return None, "ack_id required"
        with self._lock:
            expired = self._expired_message_locked(aid)
            if expired:
                return None, expired
            self._gc_locked()
            doc = self._entries.get(aid)
            if not _visible(doc):
                return None, approval_restage_message(None, expired=False)
            if doc.get("consumed_at"):
                prior = doc.get("result")
                if isinstance(prior, dict) and prior:
                    return {"_idempotent": True, **prior}, ""
                return None, "This approval was already used."
            if float(doc.get("expires_at") or 0) <= _now():
                kind = str(doc.get("kind") or "")
                self._entries.pop(aid, None)
                self._persist()
                return None, approval_restage_message(kind, expired=True)
            return dict(doc.get("payload") or {}), ""

    def claim(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
        claim_ttl_sec: float = 60.0,
    ) -> tuple[dict[str, Any] | None, str]:
        """
        Atomically claim a pending ack for mutation.
        Concurrent claims fail until the claim expires or is released/finalized.
        """
        aid = (ack_id or "").strip()
        if not aid:
            return None, "ack_id required"
        actor = (actor or "").strip() or "pilot-ui"
        reason = (reason or "").strip() or "confirmed"
        now = _now()
        with self._lock:
            expired = self._expired_message_locked(aid)
            if expired:
                return None, expired
            self._gc_locked()
            doc = self._entries.get(aid)
            if not _visible(doc):
                return None, approval_restage_message(None, expired=False)
            if doc.get("consumed_at"):
                # Any stamped result means the mutation already happened. Replaying
                # the same ack must return that result, never run the action twice.
                prior = doc.get("result")
                if isinstance(prior, dict) and prior:
                    return {"_idempotent": True, **prior}, ""
                return None, "This approval was already used."
            if float(doc.get("expires_at") or 0) <= now:
                kind = str(doc.get("kind") or "")
                self._entries.pop(aid, None)
                self._persist()
                return None, approval_restage_message(kind, expired=True)
            claimed_at = float(doc.get("claimed_at") or 0)
            if claimed_at and (now - claimed_at) < max(5.0, float(claim_ttl_sec)):
                return None, "This approval is already being confirmed. Wait a moment and retry."
            doc["claimed_at"] = now
            doc["claimed_by"] = actor
            doc["consume_reason"] = reason
            self._persist()
            return dict(doc.get("payload") or {}), ""

    def release_claim(self, ack_id: str) -> None:
        """Clear an in-flight claim so the operator can retry after a failed save."""
        aid = (ack_id or "").strip()
        with self._lock:
            doc = self._entries.get(aid)
            if not doc or doc.get("consumed_at"):
                return
            doc.pop("claimed_at", None)
            doc.pop("claimed_by", None)
            self._persist()

    def finalize(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        """Mark consumed, stamp result, redact secrets."""
        aid = (ack_id or "").strip()
        actor = (actor or "").strip() or "pilot-ui"
        reason = (reason or "").strip() or "confirmed"
        with self._lock:
            doc = self._entries.get(aid)
            if not doc:
                return
            doc["consumed_at"] = _now()
            doc["consumed_by"] = actor
            doc["consume_reason"] = reason or doc.get("consume_reason") or "confirmed"
            doc["result"] = dict(result or {})
            doc.pop("claimed_at", None)
            doc.pop("claimed_by", None)
            doc["payload"] = redact_payload(doc.get("payload") or {})
            self._persist()

    def consume(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        """Claim + finalize in one step (for simple non-retryable consumers)."""
        payload, err = self.claim(ack_id, actor=actor, reason=reason)
        if err or payload is None:
            return None, err or "Approval not found"
        if payload.get("_idempotent"):
            return payload, ""
        self.finalize(ack_id, actor=actor, reason=reason, result={})
        return payload, ""

    def stamp_result(self, ack_id: str, result: dict[str, Any]) -> None:
        with self._lock:
            doc = self._entries.get((ack_id or "").strip())
            if not doc:
                return
            doc["result"] = dict(result or {})
            doc["payload"] = redact_payload(doc.get("payload") or {})
            if not doc.get("consumed_at"):
                doc["consumed_at"] = _now()
                doc["consumed_by"] = doc.get("claimed_by") or doc.get("consumed_by") or "pilot-ui"
                doc["consume_reason"] = doc.get("consume_reason") or "confirmed"
            doc.pop("claimed_at", None)
            doc.pop("claimed_by", None)
            self._persist()

    def clear_for_tests(self) -> None:
        with self._lock:
            self._entries.clear()
            self._persist()


class MongoAckLedger:
    """Same one-shot contract as :class:`PilotAckLedger`, stored in Mongo.

    Two API replicas share one collection. ``claim`` is a single
    ``find_one_and_update`` so the second replica cannot apply the same ack.
    A consumed ack with a stamped result replays that result.
    """

    def __init__(self, collection: Any | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.ttl_sec = max(60, int(ttl_sec))
        self.path = None  # file-ledger attribute; unused on this backend
        if collection is not None:
            self._coll = collection
        else:
            from services.control_plane_store import mongo_collection

            self._coll = mongo_collection("pilot_acks")
        if self._coll is None:
            raise RuntimeError(
                "ACK_BACKEND=mongo requires the shared pilot_acks collection. "
                "Refusing a pod-local file ledger."
            )

    def _gc(self) -> None:
        now = _now()
        try:
            self._coll.delete_many({"consumed_at": None, "expires_at": {"$lte": now}})
        except Exception:
            _log.debug("pilot ack gc skipped", exc_info=True)

    def put(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        preview: dict[str, Any] | None = None,
        ttl_sec: int | None = None,
        actor_hint: str = "",
    ) -> str:
        aid = _new_id()
        ttl = max(60, int(ttl_sec if ttl_sec is not None else self.ttl_sec))
        now = _now()
        doc = {
            "_id": aid,
            "ack_id": aid,
            "kind": kind,
            "payload": dict(payload or {}),
            "preview": dict(preview or redact_payload(payload or {})),
            "created_at": now,
            "expires_at": now + ttl,
            "actor_hint": (actor_hint or "").strip(),
            **_staged_by(),
            "consumed_at": None,
            "consumed_by": None,
            "consume_reason": None,
            "result": None,
        }
        stored = _sealed_at_rest(doc)
        if stored is None:
            # A shared collection must never hold a staged secret in plaintext.
            raise RuntimeError(
                "This approval could not be staged: the secret vault is unavailable, "
                "so its payload cannot be stored encrypted."
            )
        self._gc()
        self._coll.insert_one(stored)
        return aid

    def unusable_reason(self, ack_id: str) -> str:
        aid = (ack_id or "").strip()
        if not aid:
            return "ack_id required"
        doc = self._find(aid)
        if doc and not doc.get("consumed_at") and float(doc.get("expires_at") or 0) <= _now():
            kind = str(doc.get("kind") or "")
            try:
                self._coll.delete_one({"_id": aid})
            except Exception:
                _log.debug("pilot ack expire delete skipped", exc_info=True)
            return approval_restage_message(kind, expired=True)
        if not doc:
            return approval_restage_message(None, expired=False)
        return ""

    def _find(self, ack_id: str) -> dict[str, Any] | None:
        aid = (ack_id or "").strip()
        if not aid:
            return None
        doc = _opened(self._coll.find_one({"_id": aid}))
        # Another workspace's approval does not exist for this caller.
        return dict(doc) if doc and _visible(doc) else None

    def peek(self, ack_id: str) -> dict[str, Any] | None:
        self._gc()
        doc = self._find(ack_id)
        if not doc:
            return None
        if doc.get("consumed_at") is None and float(doc.get("expires_at") or 0) <= _now():
            return None
        return _safe_peek(doc)

    def _idempotent(self, doc: dict[str, Any] | None) -> tuple[dict[str, Any] | None, str] | None:
        if not doc or not doc.get("consumed_at"):
            return None
        prior = doc.get("result")
        if isinstance(prior, dict) and prior:
            return {"_idempotent": True, **prior}, ""
        return None, "This approval was already used."

    def get_pending_payload(self, ack_id: str) -> tuple[dict[str, Any] | None, str]:
        aid = (ack_id or "").strip()
        if not aid:
            return None, "ack_id required"
        doc = self._find(aid)
        if doc and not doc.get("consumed_at") and float(doc.get("expires_at") or 0) <= _now():
            kind = str(doc.get("kind") or "")
            self._coll.delete_one({"_id": aid})
            return None, approval_restage_message(kind, expired=True)
        self._gc()
        doc = self._find(aid)
        if not doc:
            return None, approval_restage_message(None, expired=False)
        replay = self._idempotent(doc)
        if replay is not None:
            return replay
        if float(doc.get("expires_at") or 0) <= _now():
            kind = str(doc.get("kind") or "")
            self._coll.delete_one({"_id": aid})
            return None, approval_restage_message(kind, expired=True)
        return dict(doc.get("payload") or {}), ""

    def claim(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
        claim_ttl_sec: float = 60.0,
    ) -> tuple[dict[str, Any] | None, str]:
        aid = (ack_id or "").strip()
        if not aid:
            return None, "ack_id required"
        actor = (actor or "").strip() or "pilot-ui"
        reason = (reason or "").strip() or "confirmed"
        now = _now()
        doc = self._find(aid)
        if doc and not doc.get("consumed_at") and float(doc.get("expires_at") or 0) <= now:
            kind = str(doc.get("kind") or "")
            self._coll.delete_one({"_id": aid})
            return None, approval_restage_message(kind, expired=True)
        self._gc()
        doc = self._find(aid)
        if not doc:
            return None, approval_restage_message(None, expired=False)
        replay = self._idempotent(doc)
        if replay is not None:
            return replay
        if float(doc.get("expires_at") or 0) <= now:
            kind = str(doc.get("kind") or "")
            self._coll.delete_one({"_id": aid})
            return None, approval_restage_message(kind, expired=True)
        window = now - max(5.0, float(claim_ttl_sec))
        try:
            from pymongo import ReturnDocument

            return_doc = ReturnDocument.AFTER
        except Exception:
            return_doc = True
        updated = self._coll.find_one_and_update(
            {
                "_id": aid,
                "consumed_at": None,
                "expires_at": {"$gt": now},
                "$or": [
                    {"claimed_at": {"$exists": False}},
                    {"claimed_at": None},
                    {"claimed_at": {"$lt": window}},
                ],
            },
            {"$set": {"claimed_at": now, "claimed_by": actor, "consume_reason": reason}},
            return_document=return_doc,
        )
        if not updated:
            again = self._find(aid)
            replay = self._idempotent(again)
            if replay is not None:
                return replay
            return None, "This approval is already being confirmed. Wait a moment and retry."
        return dict((_opened(dict(updated)) or {}).get("payload") or {}), ""

    def release_claim(self, ack_id: str) -> None:
        aid = (ack_id or "").strip()
        self._coll.update_one(
            {"_id": aid, "consumed_at": None},
            {"$unset": {"claimed_at": "", "claimed_by": ""}},
        )

    def finalize(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
        result: dict[str, Any] | None = None,
    ) -> None:
        aid = (ack_id or "").strip()
        actor = (actor or "").strip() or "pilot-ui"
        reason = (reason or "").strip() or "confirmed"
        doc = self._find(aid)
        if not doc:
            return
        self._coll.update_one(
            {"_id": aid},
            {
                "$set": {
                    "consumed_at": _now(),
                    "consumed_by": actor,
                    "consume_reason": reason or doc.get("consume_reason") or "confirmed",
                    "result": dict(result or {}),
                    "payload": redact_payload(doc.get("payload") or {}),
                },
                "$unset": {"claimed_at": "", "claimed_by": ""},
            },
        )

    def consume(
        self,
        ack_id: str,
        *,
        actor: str = "",
        reason: str = "",
    ) -> tuple[dict[str, Any] | None, str]:
        payload, err = self.claim(ack_id, actor=actor, reason=reason)
        if err or payload is None:
            return None, err or "Approval not found"
        if payload.get("_idempotent"):
            return payload, ""
        self.finalize(ack_id, actor=actor, reason=reason, result={})
        return payload, ""

    def stamp_result(self, ack_id: str, result: dict[str, Any]) -> None:
        aid = (ack_id or "").strip()
        doc = self._find(aid)
        if not doc:
            return
        fields: dict[str, Any] = {
            "result": dict(result or {}),
            "payload": redact_payload(doc.get("payload") or {}),
        }
        if not doc.get("consumed_at"):
            fields["consumed_at"] = _now()
            fields["consumed_by"] = doc.get("claimed_by") or doc.get("consumed_by") or "pilot-ui"
            fields["consume_reason"] = doc.get("consume_reason") or "confirmed"
        self._coll.update_one(
            {"_id": aid},
            {"$set": fields, "$unset": {"claimed_at": "", "claimed_by": ""}},
        )

    def clear_for_tests(self) -> None:
        self._coll.delete_many({})


_ledger: PilotAckLedger | MongoAckLedger | None = None
_ledger_lock = threading.Lock()


def get_ack_ledger() -> PilotAckLedger | MongoAckLedger:
    """Return the process ledger.

    File mode rebinds when ``PILOT_ACK_PATH`` changes so a test tmp path is
    not shadowed by the default file. Mongo mode is one collection shared by
    every API replica; a missing collection raises instead of falling back
    to a pod-local file.
    """
    global _ledger
    with _ledger_lock:
        from services.process_role import ack_backend

        if ack_backend() == "mongo":
            if not isinstance(_ledger, MongoAckLedger):
                _ledger = MongoAckLedger()
            return _ledger
        desired = _default_path()
        if (
            not isinstance(_ledger, PilotAckLedger)
            or Path(_ledger.path).resolve() != Path(desired).resolve()
        ):
            _ledger = PilotAckLedger(path=desired)
        return _ledger
