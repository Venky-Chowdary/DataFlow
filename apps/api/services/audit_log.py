"""Append-only workspace audit log — real events, redacted secrets.

When MongoDB is connected, events are written to an ``audit_events`` collection
so they are shared across Railway replicas. Otherwise events fall back to a
local JSONL file.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from services.platform_config import data_dir
from services.value_serializer import json_default

STORE_PATH = data_dir() / "audit_events.jsonl"
MAX_EVENTS = int(os.getenv("DATAFLOW_AUDIT_MAX_EVENTS", "5000"))
_APPEND_LOCK = threading.RLock()
_LAST_RETENTION_PURGE = 0.0
_RETENTION_PURGE_INTERVAL_SEC = 60 * 60

_SENSITIVE_KEYS = frozenset({
    "password", "secret", "token", "api_key", "connection_string",
    "authorization", "credential", "private_key",
})


AUDIT_LEVELS = frozenset({"info", "success", "warn", "error"})


class AuditConfigError(ValueError):
    """An invalid audit-retention environment setting."""

_LEVEL_SYNONYMS = {
    "warning": "warn",
    "critical": "error",
    "fatal": "error",
    "err": "error",
    "debug": "info",
    "ok": "success",
}


def canonical_level(level: str) -> str:
    """The level string the readers actually filter on.

    Readers (the audit API's ``level`` filter and the Settings audit table) match
    the stored value exactly against ``info|success|warn|error``, so a caller that
    writes the English synonym ``"warning"`` files a destructive action as
    informational and it disappears from the Warnings view. Fold the synonym here
    rather than trusting every call site to remember the vocabulary.
    """
    raw = str(level or "").strip().lower()
    if raw in AUDIT_LEVELS:
        return raw
    mapped = _LEVEL_SYNONYMS.get(raw)
    if mapped:
        return mapped
    logging.getLogger(__name__).warning(
        "Audit level %r is not one of %s; recording as info",
        level,
        sorted(AUDIT_LEVELS),
    )
    return "info"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _redact(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if k.lower() in _SENSITIVE_KEYS:
                out[k] = "[REDACTED]"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(value, list):
        return [_redact(v) for v in value[:50]]
    if isinstance(value, str) and len(value) > 512:
        return value[:512] + "…"
    return value


def _mongo_collection():
    try:
        from services.mongodb_service import get_mongodb_service

        mongo = get_mongodb_service()
        if mongo and getattr(mongo, "client", None) and type(mongo).__name__ != "MemoryMongoDBService":
            # ``Database.get`` is not a valid PyMongo API; ``get_collection`` is.
            return mongo.get_database().get_collection("audit_events")
    except Exception as exc:
        logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
    return None


def _hmac_event_hash(event: dict[str, Any], secret: bytes) -> str:
    import hashlib
    import hmac as hmac_mod

    body_for_hash = {k: v for k, v in event.items() if k not in ("_id", "event_hash")}
    canon = json.dumps(body_for_hash, sort_keys=True, separators=(",", ":"), default=json_default)
    return hmac_mod.new(secret, canon.encode("utf-8"), hashlib.sha256).hexdigest()


def _platform_hmac_secret() -> bytes:
    try:
        from services.auth_service import _token_secret

        raw = _token_secret()
        return raw if isinstance(raw, bytes) else str(raw or "").encode("utf-8")
    except Exception:
        from services.brand_env import getenv_brand

        return (getenv_brand("AUTH_SECRET", "") or "dev-only-not-for-production").encode("utf-8")


def _latest_record_from_file() -> dict[str, Any] | None:
    """The last record actually written to the JSONL store, in write order."""
    if not STORE_PATH.exists():
        return None
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(ev, dict) and ev.get("event_hash"):
            return ev
    return None


def _latest_hash_from_file() -> str | None:
    tip = _latest_record_from_file()
    return str(tip.get("event_hash")) if tip else None


def chain_seq_of(event: dict[str, Any] | None) -> int:
    """Position of a record in the chain; 0 for records written before ordering existed."""
    try:
        return int((event or {}).get("chain_seq") or 0)
    except (TypeError, ValueError):
        return 0


#: Ordering key for the chain. ``time`` alone is not one: two records written in
#: the same clock tick sort arbitrarily, so the tip query could return the wrong
#: record to link to (a fork) and a later re-walk could read the pair back in the
#: opposite order from the one they were written in (a broken link) — on a chain
#: nobody had tampered with. ``chain_seq`` is strictly increasing per append and
#: breaks every tie.
CHAIN_ORDER: tuple[tuple[str, int], ...] = (("time", 1), ("chain_seq", 1))


def _chain_tip() -> dict[str, Any] | None:
    """The newest record in the chain, by write position rather than by clock."""
    coll = _mongo_collection()
    if coll is not None:
        try:
            for doc in coll.find({}).sort([(k, -d) for k, d in CHAIN_ORDER]).limit(1):
                return {k: v for k, v in doc.items() if k != "_id"}
            return None
        except Exception as exc:  # noqa: BLE001 - any driver error falls back to file
            logging.getLogger(__name__).warning(
                "Mongo chain tip read failed; falling back to file: %s", exc
            )
    return _latest_record_from_file()


def append_audit_event(
    *,
    action: str,
    resource: str,
    actor: str = "system",
    level: str = "info",
    correlation_id: str | None = None,
    details: dict[str, Any] | None = None,
    workspace_id: str | None = None,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Record a redacted audit event to MongoDB (preferred) or a local JSONL file.

    Events are hash-chained with HMAC-SHA256 (keyed by the platform auth secret):
    ``prev_hash`` + ``event_hash`` over a canonical payload. Plain SHA-256 chains
    can be reforged by anyone with store write access; HMAC requires the secret.

    Process-local ``_APPEND_LOCK`` serializes writers to reduce chain forks. Cross-
    process/replica races still need a single authoritative store (Mongo preferred).
    """
    with _APPEND_LOCK:
        secret = _platform_hmac_secret()
        tip = _chain_tip()
        prev = str(tip.get("event_hash")) if tip and tip.get("event_hash") else None
        redacted = _redact(details or {})
        ws = (workspace_id or "").strip()
        tid = (tenant_id or "").strip()
        if isinstance(redacted, dict):
            ws = ws or str(redacted.get("workspace_id") or "").strip()
            tid = tid or str(redacted.get("tenant_id") or "").strip()
        event = {
            "_id": str(uuid.uuid4()),
            "id": str(uuid.uuid4()),
            "time": _now(),
            "actor": actor,
            "action": action,
            "resource": resource,
            "level": canonical_level(level),
            "correlation_id": correlation_id,
            "workspace_id": ws,
            "tenant_id": tid,
            "details": redacted,
            "prev_hash": prev,
            "chain_seq": chain_seq_of(tip) + 1,
            "hash_alg": "HMAC-SHA256",
        }
        event["event_hash"] = _hmac_event_hash(event, secret)

        coll = _mongo_collection()
        if coll is not None:
            try:
                coll.insert_one(event)
                _maybe_anchor_tip(event)
                _maybe_opportunistic_audit_purge()
                return event
            except Exception as exc:
                logging.getLogger(__name__).warning(
                    "Mongo audit insert failed; falling back to file with file-local prev: %s",
                    exc,
                    exc_info=exc,
                )
                # Rebuild chain tip from the file store so we do not link to a Mongo
                # tip that never landed in JSONL.
                file_tip = _latest_record_from_file()
                event["prev_hash"] = (
                    str(file_tip.get("event_hash")) if file_tip else None
                )
                event["chain_seq"] = chain_seq_of(file_tip) + 1
                event["event_hash"] = _hmac_event_hash(event, secret)

        STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_event = {k: v for k, v in event.items() if k != "_id"}
        with STORE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(file_event, ensure_ascii=False, default=json_default) + "\n")
        _trim_if_needed()
        _maybe_opportunistic_audit_purge()
        _maybe_anchor_tip(event)
        return event


def _maybe_anchor_tip(event: dict[str, Any]) -> None:
    """Best-effort seal of the chain tip (never fails the audit append)."""
    try:
        from services.brand_env import getenv_brand
        from services.audit_anchor import anchor_tip

        tip = str(event.get("event_hash") or "")
        if not tip:
            return
        # Anchor every Nth event to limit write amplification (default: every event in stub).
        every = int(getenv_brand("AUDIT_ANCHOR_EVERY", "1") or "1")
        if every > 1:
            # Cheap sampling: last hex nibble mod every
            if int(tip[-1], 16) % every != 0:
                return
        anchor_tip(tip, event_time=str(event.get("time") or ""))
    except Exception as exc:
        logging.getLogger(__name__).debug("audit tip anchor skipped: %s", exc)


def latest_event_hash() -> str | None:
    """Return the most recent ``event_hash`` for hash chaining, if any."""
    events = list_audit_events(limit=1)
    if not events:
        return None
    h = events[0].get("event_hash")
    return str(h) if h else None


def _event_in_scope(
    ev: dict[str, Any],
    *,
    workspace_id: str | None,
    tenant_id: str | None,
    since: str | None,
    until: str | None,
) -> bool:
    if workspace_id and str(ev.get("workspace_id") or "") != workspace_id:
        return False
    if tenant_id and str(ev.get("tenant_id") or "") != tenant_id:
        return False
    when = str(ev.get("time") or "")
    if since and when and when < since:
        return False
    if until and when and when > until:
        return False
    return True


def list_audit_events(
    *,
    limit: int = 100,
    level: str | None = None,
    actor: str | None = None,
    workspace_id: str | None = None,
    tenant_id: str | None = None,
    since: str | None = None,
    until: str | None = None,
    after_seq: int | None = None,
) -> list[dict[str, Any]]:
    """Return recent events, or an ascending chain-sequence cursor page.

    ``workspace_id`` / ``tenant_id`` are exact matches. Unscoped historical
    events (empty workspace) are excluded from a scoped query so one tenant
    cannot export another tenant's rows — or the global leftovers.
    """
    if after_seq is not None and after_seq < 0:
        raise ValueError("after_seq must be nonnegative")
    ws = (workspace_id or "").strip() or None
    tid = (tenant_id or "").strip() or None
    since = (since or "").strip() or None
    until = (until or "").strip() or None
    coll = _mongo_collection()
    if coll is not None:
        try:
            query: dict[str, Any] = {}
            if level and level != "all":
                query["level"] = level
            if actor:
                query["actor"] = actor
            if ws:
                query["workspace_id"] = ws
            if tid:
                query["tenant_id"] = tid
            if since or until:
                query["time"] = {}
                if since:
                    query["time"]["$gte"] = since
                if until:
                    query["time"]["$lte"] = until
            if after_seq is not None:
                query["chain_seq"] = {"$gt": after_seq}
                sort_order = [("chain_seq", 1)]
            else:
                sort_order = [(key, -direction) for key, direction in CHAIN_ORDER]
            cursor = (
                coll.find(query)
                .sort(sort_order)
                .limit(limit)
            )
            return [{k: v for k, v in doc.items() if k != "_id"} for doc in cursor]
        except Exception as exc:
            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)

    if not STORE_PATH.exists():
        return []
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    events: list[dict[str, Any]] = []
    ordered_lines = lines if after_seq is not None else reversed(lines)
    for line in ordered_lines:
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if level and level != "all" and ev.get("level") != level:
            continue
        if actor and ev.get("actor") != actor:
            continue
        if after_seq is not None and chain_seq_of(ev) <= after_seq:
            continue
        if not _event_in_scope(ev, workspace_id=ws, tenant_id=tid, since=since, until=until):
            continue
        events.append(ev)
        if after_seq is None and len(events) >= limit:
            break
    if after_seq is not None:
        events.sort(key=chain_seq_of)
    return events[:limit]


def _event_hash_of_line(line: str) -> str | None:
    try:
        ev = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(ev, dict):
        return None
    h = ev.get("event_hash")
    return str(h) if h else None


def _trim_if_needed() -> None:
    """Enforce retention, and leave a signed record of what retention removed.

    Dropping the oldest lines leaves the first surviving record pointing at a
    hash that is no longer in the store, which reads exactly like a deleted
    record. The checkpoint is what lets chain verification say "retention
    removed N records" instead of reporting a broken chain.
    """
    with _APPEND_LOCK:
        if _legal_hold_enabled() or not STORE_PATH.exists():
            return
        lines = STORE_PATH.read_text(encoding="utf-8").splitlines()
        if len(lines) <= MAX_EVENTS:
            return
        removed = lines[:-MAX_EVENTS]
        trimmed = lines[-MAX_EVENTS:]
        last_removed = next(
            (h for h in (_event_hash_of_line(ln) for ln in reversed(removed)) if h), None
        )
        first_kept = next(
            (h for h in (_event_hash_of_line(ln) for ln in trimmed) if h), None
        )
        _atomic_write_lines(trimmed)
        _record_retention_checkpoint(
            removed_count=len(removed),
            last_removed_event_hash=last_removed,
            first_kept_event_hash=first_kept,
        )


def _atomic_write_lines(lines: list[str]) -> None:
    """Atomically replace the JSONL file while the append lock is held."""
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{STORE_PATH.name}.",
        suffix=".tmp",
        dir=str(STORE_PATH.parent),
        text=True,
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            if lines:
                stream.write("\n".join(lines) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, STORE_PATH)
    except Exception:
        try:
            os.unlink(temporary)
        except OSError:
            pass
        raise


def _record_retention_checkpoint(
    *,
    removed_count: int,
    last_removed_event_hash: str | None,
    first_kept_event_hash: str | None,
) -> dict[str, Any] | None:
    try:
        from services.evidence_chain import record_truncation

        return record_truncation(
            removed_count=removed_count,
            last_removed_event_hash=last_removed_event_hash,
            first_kept_event_hash=first_kept_event_hash,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning(
            "Retention removed %s audit record(s) without a checkpoint (%s)",
            removed_count,
            type(exc).__name__,
            exc_info=exc,
        )
        return None


def _legal_hold_enabled() -> bool:
    return os.getenv("DATAFLOW_AUDIT_LEGAL_HOLD", "").strip() == "1"


def _retention_days() -> int | None:
    raw = os.getenv("DATAFLOW_AUDIT_RETENTION_DAYS")
    value = (raw or "").strip()
    if not value or value.lower() == "off":
        return None
    try:
        days = int(value)
    except ValueError as exc:
        logging.getLogger(__name__).error(
            "Invalid DATAFLOW_AUDIT_RETENTION_DAYS=%r; expected 'off' or 30..3650",
            raw,
        )
        raise AuditConfigError(
            "DATAFLOW_AUDIT_RETENTION_DAYS must be 'off' or an integer from 30 to 3650"
        ) from exc
    if not 30 <= days <= 3650:
        logging.getLogger(__name__).error(
            "Invalid DATAFLOW_AUDIT_RETENTION_DAYS=%r; expected 'off' or 30..3650",
            raw,
        )
        raise AuditConfigError(
            "DATAFLOW_AUDIT_RETENTION_DAYS must be 'off' or an integer from 30 to 3650"
        )
    return days


def _parse_event_time(event: dict[str, Any]) -> datetime | None:
    raw = event.get("time")
    if not isinstance(raw, str) or not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _oldest_time_from_docs(documents: list[dict[str, Any]]) -> str | None:
    for event in documents:
        value = event.get("time")
        if value:
            return str(value)
    return None


def purge_expired_audit_events(
    *, now: datetime | None = None, dry_run: bool = False
) -> dict[str, Any]:
    """Purge events older than the configured retention window.

    A dry run reports the current store state and never deletes records.
    """
    retention_days = _retention_days()
    legal_hold = _legal_hold_enabled()
    now_utc = now or datetime.now(timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=timezone.utc)
    now_utc = now_utc.astimezone(timezone.utc)
    cutoff = (
        (now_utc - timedelta(days=retention_days)).isoformat()
        if retention_days is not None
        else None
    )

    with _APPEND_LOCK:
        collection = _mongo_collection()
        if collection is not None:
            return _purge_mongo_events(
                collection,
                cutoff=cutoff if not legal_hold else None,
                retention_days=retention_days,
                legal_hold=legal_hold,
                dry_run=dry_run,
            )
        return _purge_file_events(
            cutoff=(
                now_utc - timedelta(days=retention_days)
                if cutoff is not None and not legal_hold
                else None
            ),
            retention_days=retention_days,
            legal_hold=legal_hold,
            dry_run=dry_run,
        )


def _purge_file_events(
    *,
    cutoff: datetime | None,
    retention_days: int | None,
    legal_hold: bool,
    dry_run: bool,
) -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {
            "removed": 0,
            "oldest_kept_time": None,
            "legal_hold": legal_hold,
            "retention_days": retention_days,
            "dry_run": bool(dry_run),
        }

    lines = STORE_PATH.read_text(encoding="utf-8").splitlines()
    kept: list[str] = []
    kept_events: list[dict[str, Any]] = []
    removed_events: list[dict[str, Any]] = []
    for line in lines:
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            kept.append(line)
            continue
        if not isinstance(event, dict):
            kept.append(line)
            continue
        event_time = _parse_event_time(event)
        if cutoff is not None and event_time is not None and event_time < cutoff:
            removed_events.append(event)
        else:
            kept.append(line)
            kept_events.append(event)

    removed_count = len(removed_events) if not dry_run else 0
    if removed_events and not dry_run:
        _atomic_write_lines(kept)
        checkpoint = _record_retention_checkpoint(
            removed_count=len(removed_events),
            last_removed_event_hash=next(
                (
                    str(event["event_hash"])
                    for event in reversed(removed_events)
                    if event.get("event_hash")
                ),
                None,
            ),
            first_kept_event_hash=next(
                (str(event["event_hash"]) for event in kept_events if event.get("event_hash")),
                None,
            ),
        )
        if checkpoint is None:
            _atomic_write_lines(lines)
            raise RuntimeError("Audit retention checkpoint could not be recorded")
    current_events = (
        kept_events
        if removed_events and not dry_run
        else [
            event
            for line in lines
            if (event := _parse_event_line(line)) is not None
        ]
    )
    return {
        "removed": removed_count,
        "oldest_kept_time": _oldest_time_from_docs(current_events),
        "legal_hold": legal_hold,
        "retention_days": retention_days,
        "dry_run": bool(dry_run),
    }


def _parse_event_line(line: str) -> dict[str, Any] | None:
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None
    return event if isinstance(event, dict) else None


def _purge_mongo_events(
    collection: Any,
    *,
    cutoff: str | None,
    retention_days: int | None,
    legal_hold: bool,
    dry_run: bool,
) -> dict[str, Any]:
    query = {"time": {"$lt": cutoff}} if cutoff is not None else None
    removed_count = collection.count_documents(query) if query is not None else 0
    last_removed = None
    if removed_count:
        last_removed = next(
            iter(collection.find(query).sort([("chain_seq", -1)]).limit(1)), None
        )
        if last_removed is None:
            raise RuntimeError("Audit retention candidate disappeared before checkpointing")
    if dry_run or not removed_count:
        first_query = {}
    else:
        first_query = {"time": {"$gte": cutoff}}
    first_kept = next(
        iter(collection.find(first_query).sort([("chain_seq", 1)]).limit(1)), None
    )
    if first_kept is not None:
        first_kept = {key: value for key, value in first_kept.items() if key != "_id"}

    actually_removed = 0
    if removed_count and not dry_run:
        checkpoint = _record_retention_checkpoint(
            removed_count=removed_count,
            last_removed_event_hash=str(last_removed.get("event_hash") or "") or None,
            first_kept_event_hash=(
                str(first_kept.get("event_hash") or "") if first_kept else None
            ),
        )
        if checkpoint is None:
            raise RuntimeError("Audit retention checkpoint could not be recorded")
        actually_removed = collection.delete_many(query).deleted_count

    if not dry_run and actually_removed:
        first_kept = None
        for event in collection.find({}).sort([("chain_seq", 1)]).limit(1):
            first_kept = {key: value for key, value in event.items() if key != "_id"}
    return {
        "removed": actually_removed,
        "oldest_kept_time": (
            str(first_kept.get("time")) if first_kept and first_kept.get("time") else None
        ),
        "legal_hold": legal_hold,
        "retention_days": retention_days,
        "dry_run": bool(dry_run),
    }


def _maybe_opportunistic_audit_purge() -> None:
    global _LAST_RETENTION_PURGE
    current = time.monotonic()
    if current - _LAST_RETENTION_PURGE < _RETENTION_PURGE_INTERVAL_SEC:
        return
    _LAST_RETENTION_PURGE = current
    try:
        purge_expired_audit_events()
    except Exception as exc:
        logging.getLogger(__name__).error(
            "Opportunistic audit retention purge failed (%s)",
            type(exc).__name__,
            exc_info=exc,
        )


def workspace_id_from_request(request: Any) -> str:
    """Workspace the caller is looking at — header first, then request.state."""
    if request is None:
        return ""
    try:
        header = request.headers.get("X-Workspace-Id") or request.headers.get("x-workspace-id") or ""
    except Exception:
        header = ""
    if str(header).strip():
        return str(header).strip()
    return str(getattr(request.state, "workspace_id", "") or "").strip()


def actor_from_request(request: Any) -> str:
    """Extract the actor email from a FastAPI request, if available."""
    if request is None:
        return "anonymous"
    user = getattr(request.state, "user", None)
    if user and isinstance(user, dict):
        return str(user.get("email") or user.get("sub") or "anonymous")
    return "anonymous"
