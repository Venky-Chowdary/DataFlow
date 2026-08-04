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

    def get_pending_payload(self, ack_id: str) -> tuple[dict[str, Any] | None, str]:
        """Return secret payload for a still-pending ack (does not consume)."""
        aid = (ack_id or "").strip()
        if not aid:
            return None, "ack_id required"
        with self._lock:
            self._gc_locked()
            doc = self._entries.get(aid)
            if not doc:
                return None, "Approval not found or expired. Ask Pilot to create the connector again."
            if doc.get("consumed_at"):
                prior = doc.get("result")
                if isinstance(prior, dict) and prior:
                    return {"_idempotent": True, **prior}, ""
                return None, "This approval was already used."
            if float(doc.get("expires_at") or 0) <= _now():
                self._entries.pop(aid, None)
                self._persist()
                return None, "Approval expired. Ask Pilot to create the connector again."
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
            self._gc_locked()
            doc = self._entries.get(aid)
            if not doc:
                return None, "Approval not found or expired. Ask Pilot to create the connector again."
            if doc.get("consumed_at"):
                # Any stamped result means the mutation already happened. Replaying
                # the same ack must return that result, never run the action twice.
                prior = doc.get("result")
                if isinstance(prior, dict) and prior:
                    return {"_idempotent": True, **prior}, ""
                return None, "This approval was already used."
            if float(doc.get("expires_at") or 0) <= now:
                self._entries.pop(aid, None)
                self._persist()
                return None, "Approval expired. Ask Pilot to create the connector again."
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


_ledger: PilotAckLedger | None = None
_ledger_lock = threading.Lock()


def get_ack_ledger() -> PilotAckLedger:
    global _ledger
    with _ledger_lock:
        if _ledger is None:
            _ledger = PilotAckLedger()
        return _ledger
