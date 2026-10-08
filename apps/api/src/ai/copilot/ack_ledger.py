"""Server-side mutation approval ledger for Datawrap Pilot.

Create-connector drafts (with secrets) stay on the server. The UI only
receives an ack_id + safe preview; Confirm consumes the ledger once.
"""

from __future__ import annotations

import json
import logging
import os
from services.brand_env import getenv_brand
import threading
import time
import uuid
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

_DEFAULT_TTL_SEC = 15 * 60  # 15 minutes
_SECRET_KEYS = frozenset({
    "password", "passwd", "pwd", "api_key", "token", "private_key",
    "connection_string", "service_account", "secret",
})


def _default_path() -> Path:
    override = getenv_brand("PILOT_ACK_PATH", "").strip()
    if override:
        return Path(override)
    return Path(__file__).resolve().parents[3] / "data" / "pilot_acks.json"


def _now() -> float:
    return time.time()


def _new_id() -> str:
    return f"ack_{uuid.uuid4().hex[:20]}"


def approval_restage_message(kind: str | None, *, expired: bool) -> str:
    """Say which action to stage again. A transfer ack is not a connector create."""
    token = (kind or "").strip()
    if token == "start_transfer":
        action = "plan the transfer again"
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
    elif token in {"delete_connector", "test_connector"}:
        action = "repeat that connector action"
    else:
        action = "stage the action again"
    if expired and token:
        return f"Approval expired. Ask Pilot to {action}."
    return f"Approval not found or expired. Ask Pilot to {action}."


def redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Safe client-facing copy — never echo secrets."""
    out: dict[str, Any] = {}
    for k, v in (payload or {}).items():
        if k.lower() in _SECRET_KEYS:
            out[k] = "***" if v else ""
            out[f"has_{k}"] = bool(v)
        elif isinstance(v, dict):
            out[k] = redact_payload(v)
        else:
            out[k] = v
    return out


class PilotAckLedger:
    """One-shot approval records for mutate-risk Pilot actions."""

    def __init__(self, path: Path | None = None, ttl_sec: int = _DEFAULT_TTL_SEC):
        self.path = path or _default_path()
        self.ttl_sec = max(60, int(ttl_sec))
        self._lock = threading.RLock()
        self._entries: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        try:
            if not self.path.exists():
                return
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            now = _now()
            for aid, doc in (raw.get("entries") or {}).items():
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

    def _persist(self) -> None:
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            # Keep recently consumed for idempotent replay window
            payload = {"entries": self._entries}
            tmp = self.path.with_suffix(f".tmp.{os.getpid()}.{threading.get_ident()}")
            tmp.write_text(json.dumps(payload, default=str), encoding="utf-8")
            tmp.replace(self.path)
        except OSError as exc:
            _log.warning("pilot ack ledger persist failed: %s", exc)

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
            if not doc:
                return None
            if float(doc.get("expires_at") or 0) <= _now() and not doc.get("consumed_at"):
                return None
            return {
                "ack_id": doc["ack_id"],
                "kind": doc["kind"],
                "preview": dict(doc.get("preview") or {}),
                "expires_at": doc.get("expires_at"),
                "consumed": bool(doc.get("consumed_at")),
                "consumed_at": doc.get("consumed_at"),
            }

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
        if not doc:
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
            if not doc:
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
            if not doc:
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
            "consumed_at": None,
            "consumed_by": None,
            "consume_reason": None,
            "result": None,
        }
        self._gc()
        self._coll.insert_one(doc)
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
        doc = self._coll.find_one({"_id": aid})
        return dict(doc) if doc else None

    def peek(self, ack_id: str) -> dict[str, Any] | None:
        self._gc()
        doc = self._find(ack_id)
        if not doc:
            return None
        if doc.get("consumed_at") is None and float(doc.get("expires_at") or 0) <= _now():
            return None
        return {
            "ack_id": doc["ack_id"],
            "kind": doc["kind"],
            "preview": dict(doc.get("preview") or {}),
            "expires_at": doc.get("expires_at"),
            "consumed": bool(doc.get("consumed_at")),
            "consumed_at": doc.get("consumed_at"),
        }

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
        return dict(updated.get("payload") or {}), ""

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
