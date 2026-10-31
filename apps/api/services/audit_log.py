"""Append-only workspace audit log — real events, redacted secrets.

When MongoDB is connected, events are written to an ``audit_events`` collection
so they are shared across Railway replicas. Otherwise events fall back to a
local JSONL file.
"""

from __future__ import annotations

import json
import re
import logging
import os
import threading
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from services.audit_chain_keys import hmac_hex, keyring, legacy_login_secret, signing_key
from services.audit_chain_link import (
    append_with_retry,
    canonical_record,
    ensure_chain_indexes,
    file_store_lock,
    file_workspace_tip,
    is_duplicate_position,
    link_fields,
    mongo_workspace_tip,
    position_id,
    workspace_key,
)
from services.log_redaction import (
    is_redactable_value,
    is_sensitive_key,
    redact_log_text,
    redact_secret_shapes,
)
from services.platform_config import data_dir
from services.value_serializer import json_default
from services.workspace_filter import WorkspaceFilter

STORE_PATH = data_dir() / "audit_events.jsonl"
MAX_EVENTS = int(os.getenv("DATAFLOW_AUDIT_MAX_EVENTS", "5000"))
_APPEND_LOCK = threading.RLock()
_LAST_RETENTION_PURGE = 0.0
_RETENTION_PURGE_INTERVAL_SEC = 60 * 60

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
            if is_sensitive_key(k) and is_redactable_value(v, k):
                out[k] = "[REDACTED]"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(value, (list, tuple)):
        return [_redact(v) for v in value[:50]]
    if isinstance(value, str):
        redacted = redact_log_text(value)
        if len(redacted) > 512:
            return redacted[:512] + "…"
        return redacted
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
    return hmac_hex(event, secret)


def _platform_hmac_secret() -> bytes:
    """The login secret. Chain records written before D221 were signed with it.

    New chain records are signed with the chain's own key
    (``services.audit_chain_keys.signing_key``); this stays for verifying
    those older records and for callers outside the chain.
    """
    return legacy_login_secret()


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
#: Position first (R7): writer processes with skewed clocks still agree on it.
#: ``time`` orders only records written before positions existed (seq 0).
CHAIN_ORDER: tuple[tuple[str, int], ...] = (("chain_seq", 1), ("time", 1))


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


def _file_tips(workspace_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not STORE_PATH.exists():
        return None, None
    lines = STORE_PATH.read_text(encoding="utf-8").strip().splitlines()
    return _latest_record_from_file(), file_workspace_tip(lines, workspace_id)


def _chain_tips(workspace_id: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """Newest record overall and newest of ``workspace_id`` (global + workspace links)."""
    tip = _chain_tip()
    coll = _mongo_collection()
    if coll is not None:
        try:
            return tip, mongo_workspace_tip(coll, workspace_id)
        except Exception as exc:  # noqa: BLE001 - any driver error falls back to file
            logging.getLogger(__name__).warning(
                "Mongo workspace tip read failed; falling back to file: %s", exc
            )
    return tip, _file_tips(workspace_id)[1]


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
    principal: dict[str, str] | None = None,
    outcome: str | None = None,
    reason_code: str | None = None,
    session_id: str | None = None,
    trace_id: str | None = None,
    route_template: str | None = None,
) -> dict[str, Any]:
    """Record a redacted audit event to MongoDB (preferred) or a local JSONL file.

    Events are hash-chained with HMAC-SHA256 under the chain's own key
    (``DATAFLOW_AUDIT_HMAC_KEY``; each record names it in ``key_id``), never the
    login secret: ``prev_hash`` + ``event_hash`` over a canonical payload. Plain
    SHA-256 chains can be reforged by anyone with store write access; HMAC
    requires the key.

    Writers in separate processes are serialized by the store, not by
    ``_APPEND_LOCK`` (which only covers this process's threads): in Mongo the
    record's ``_id`` is its chain position, so a second writer to the same
    position gets a duplicate-key error and re-links; the file store holds an
    ``flock`` across read-tip-and-append (D221).
    """
    from services.audit_coverage import current_request_audit

    tracker = current_request_audit()
    if tracker is not None:
        if correlation_id is None:
            from services.tracing import get_correlation_id

            correlation_id = get_correlation_id() or None
        if tracker.get("correlation_id_rejected") or tracker.get(
            "workspace_header_rejected"
        ):
            details = dict(details or {})
            if tracker.get("correlation_id_rejected"):
                details["correlation_id_rejected"] = True
            if tracker.get("workspace_header_rejected"):
                details["workspace_header_rejected"] = True

    if tracker is not None and not action.startswith("http."):
        from services.admin_audit import ADMIN_EVENTS

        if action in ADMIN_EVENTS:
            # A handler's own admin event: the middleware must not add a second
            # one, and it belongs to the workspace the request was made in.
            tracker["admin_event"] = action
            if not (workspace_id or "").strip():
                workspace_id = str(tracker.get("workspace_id") or "") or None

    if principal is None:
        from services.audit_coverage import current_request_principal

        principal = current_request_principal()

    principal_fields: dict[str, str] = {}
    if principal:
        actor_type = str(principal.get("actor_type") or "anonymous")
        actor_id = str(principal.get("actor_id") or "")
        principal_fields = {
            "actor_type": actor_type,
            "actor_id": actor_id,
            "auth_method": str(principal.get("auth_method") or ""),
        }
        if actor_type in {"service_account", "scim", "api_key"}:
            created_by = str(principal.get("created_by") or "")
            principal_fields["created_by"] = created_by
            if actor == created_by or actor == "api-key":
                actor = f"{actor_type}:{actor_id}"

    with _APPEND_LOCK:
        state: dict[str, Any] = {"stage": "chain_read", "event": None}
        try:
            key_id, secret, _key_source = signing_key()

            state["stage"] = "redact"
            redacted = _redact(details or {})
            ws = (workspace_id or "").strip()
            tid = (tenant_id or "").strip()
            if isinstance(redacted, dict):
                ws = ws or str(redacted.get("workspace_id") or "").strip()
                tid = tid or str(redacted.get("tenant_id") or "").strip()
            redacted_resource = redact_secret_shapes(resource)
            coll = _mongo_collection()
            if coll is not None:
                ensure_chain_indexes(coll)

            def build() -> dict[str, Any]:
                state["stage"] = "chain_read"
                tip, ws_tip = _chain_tips(ws)
                state["stage"] = "hash"
                event = {
                    "_id": str(uuid.uuid4()),
                    "id": str(uuid.uuid4()),
                    "time": _now(),
                    "actor": actor,
                    **principal_fields,
                    "action": action,
                    "resource": redacted_resource,
                    "level": canonical_level(level),
                    "correlation_id": correlation_id,
                    "workspace_id": ws,
                    "tenant_id": tid,
                    "details": redacted,
                    **{
                        key: value
                        for key, value in (
                            ("outcome", outcome),
                            ("reason_code", reason_code),
                            ("session_id", session_id),
                            ("trace_id", trace_id),
                            ("route_template", route_template),
                        )
                        if value is not None
                    },
                    **link_fields(tip=tip, workspace_tip=ws_tip),
                    "key_id": key_id,
                    "hash_alg": "HMAC-SHA256",
                }
                # The position is the _id: unique in every Mongo collection, so
                # two processes can never both take it (D221).
                event["_id"] = position_id(event["chain_seq"])
                state["event"] = event
                # Hash exactly what the store will hand back (R7).
                event = canonical_record(event)
                state["event"] = event
                event["event_hash"] = _hmac_event_hash(event, secret)
                state["stage"] = "persist"
                return event

            if coll is None:
                with file_store_lock(STORE_PATH):
                    persisted = append_with_retry(build, _persist_event)
            else:
                persisted = append_with_retry(build, _persist_event)
        except Exception as exc:
            event = state["event"]
            try:
                store = "mongo" if _mongo_collection() is not None else "file"
            except Exception:
                store = "unknown"
            _emit_audit_write_failed(
                action=str(event.get("action") if event else action),
                correlation_id=(
                    event.get("correlation_id") if event else correlation_id
                ),
                stage=state["stage"],
                store=store,
                fallback="none",
                error=exc,
            )
            raise
        if tracker is not None and not action.startswith("http."):
            tracker["named"] = int(tracker.get("named", 0)) + 1
        return persisted


def _persist_event(event: dict[str, Any]) -> dict[str, Any]:
    def persist_to_file() -> dict[str, Any]:
        STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
        file_event = {key: value for key, value in event.items() if key != "_id"}
        with STORE_PATH.open("a", encoding="utf-8") as fh:
            fh.write(
                json.dumps(file_event, ensure_ascii=False, default=json_default) + "\n"
            )
        _trim_if_needed()
        _maybe_opportunistic_audit_purge()
        _maybe_anchor_tip(event)
        return event

    coll = _mongo_collection()
    if coll is not None:
        try:
            coll.insert_one(event)
            _maybe_anchor_tip(event)
            _maybe_opportunistic_audit_purge()
            return event
        except Exception as exc:
            if is_duplicate_position(exc):
                # Another writer process took this position: re-link, not file.
                raise
            logging.getLogger(__name__).warning(
                "Mongo audit insert failed; falling back to file with file-local prev: %s",
                exc,
                exc_info=exc,
            )
            _emit_audit_write_failed(
                action=str(event.get("action") or ""),
                correlation_id=event.get("correlation_id"),
                stage="persist",
                store="mongo",
                fallback="file",
                error=exc,
            )
            with file_store_lock(STORE_PATH):
                file_tip, file_ws_tip = _file_tips(workspace_key(event))
                event.update(link_fields(tip=file_tip, workspace_tip=file_ws_tip))
                key = keyring().get(str(event.get("key_id") or ""))
                if key is None:
                    event["key_id"], key, _source = signing_key()
                event["event_hash"] = _hmac_event_hash(event, key)
                return persist_to_file()

    return persist_to_file()


def _emit_audit_write_failed(
    *,
    action: str,
    correlation_id: str | None,
    stage: str,
    store: str,
    fallback: str,
    error: Exception,
) -> None:
    try:
        from services import ops_events

        ops_events.emit_event(
            logging.getLogger(__name__),
            "audit.write.failed",
            outcome="errored",
            error=error,
            action=action,
            stage=stage,
            store=store,
            fallback=fallback,
            correlation_id=correlation_id,
        )
    except Exception as exc:
        logging.getLogger(__name__).error(
            "Could not emit audit write failure event (%s)",
            type(exc).__name__,
        )


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
    workspace_filter: WorkspaceFilter | None = None,
) -> bool:
    if workspace_filter is not None and not workspace_filter.matches(ev.get("workspace_id")):
        return False
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


def _action_terms(action: str | None) -> tuple[set[str], list[str]]:
    exact: set[str] = set()
    prefixes: list[str] = []
    for term in str(action or "").split(","):
        term = term.strip()
        if not term:
            continue
        if term.endswith("*"):
            prefixes.append(term[:-1])
        else:
            exact.add(term)
    return exact, prefixes


def _action_mongo_query(action: str | None) -> Any:
    exact, prefixes = _action_terms(action)
    if not exact and not prefixes:
        return None
    if not prefixes:
        return exact.pop() if len(exact) == 1 else {"$in": sorted(exact)}
    pattern = "|".join(
        [f"^{re.escape(p)}" for p in prefixes] + [f"^{re.escape(e)}$" for e in sorted(exact)]
    )
    return {"$regex": pattern}


def _action_matches(action: str | None, value: str) -> bool:
    exact, prefixes = _action_terms(action)
    if not exact and not prefixes:
        return True
    return value in exact or any(value.startswith(p) for p in prefixes)


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
    workspace_filter: WorkspaceFilter | None = None,
    action: str | None = None,
    search: str | None = None,
) -> list[dict[str, Any]]:
    """Return recent events, or an ascending chain-sequence cursor page.

    ``action`` is a comma-separated list of exact action names; an entry
    ending in ``*`` (``team.member.*``) matches that prefix. ``search`` is a
    case-insensitive substring of the action, resource or actor.

    ``workspace_id`` / ``tenant_id`` are exact matches. Unscoped historical
    events (empty workspace) are excluded from a scoped query so one tenant
    cannot export another tenant's rows — or the global leftovers.
    ``workspace_filter`` is the caller's tenant scope (several workspaces);
    ``None`` is system scope.
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
            action_query = _action_mongo_query(action)
            if action_query is not None:
                query["action"] = action_query
            if search and search.strip():
                needle = {"$regex": re.escape(search.strip()), "$options": "i"}
                # Under $and: the workspace filter below may bring its own $or.
                query["$and"] = [
                    {"$or": [{"action": needle}, {"resource": needle}, {"actor": needle}]}
                ]
            if workspace_filter is not None:
                query.update(workspace_filter.mongo_query())
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
        if not _action_matches(action, str(ev.get("action") or "")):
            continue
        if search and search.strip() and not any(
            search.strip().lower() in str(ev.get(field) or "").lower()
            for field in ("action", "resource", "actor")
        ):
            continue
        if after_seq is not None and chain_seq_of(ev) <= after_seq:
            continue
        if not _event_in_scope(
            ev, workspace_id=ws, tenant_id=tid, since=since, until=until,
            workspace_filter=workspace_filter,
        ):
            continue
        events.append(ev)
        if after_seq is None and len(events) >= limit:
            break
    if after_seq is not None:
        events.sort(key=chain_seq_of)
    return events[:limit]


def scan_audit_events_for_secrets(
    *,
    limit: int = 20000,
    workspace_id: str | None = None,
    workspace_filter: WorkspaceFilter | None = None,
) -> dict[str, Any]:
    """Report paths containing legacy unredacted values without changing the store."""
    events = list_audit_events(
        limit=limit, workspace_id=workspace_id, workspace_filter=workspace_filter
    )
    findings: list[dict[str, Any]] = []

    for event in events:
        keys: list[str] = []
        shapes: list[str] = []

        def inspect(value: Any, path: str) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    child_path = f"{path}.{key}" if path else str(key)
                    if (
                        is_sensitive_key(key)
                        and child not in ("[REDACTED]", "***")
                        and is_redactable_value(child, key)
                    ):
                        keys.append(child_path)
                    inspect(child, child_path)
            elif isinstance(value, (list, tuple)):
                for index, child in enumerate(value):
                    inspect(child, f"{path}[{index}]")
            elif isinstance(value, str) and redact_secret_shapes(value) != value:
                shapes.append(path)

        inspect(event.get("details"), "details")
        resource = event.get("resource")
        if isinstance(resource, str) and redact_secret_shapes(resource) != resource:
            shapes.append("resource")
        if keys or shapes:
            findings.append(
                {
                    "id": event.get("id"),
                    "time": event.get("time"),
                    "action": event.get("action"),
                    "keys": keys,
                    "shapes": shapes,
                }
            )

    return {"scanned": len(events), "flagged": len(findings), "events": findings}


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
        with file_store_lock(STORE_PATH):
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
        result = {
            "removed": 0,
            "oldest_kept_time": None,
            "legal_hold": legal_hold,
            "retention_days": retention_days,
            "dry_run": bool(dry_run),
        }
        if dry_run:
            result["would_remove"] = 0
        return result

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

    would_remove = len(removed_events)
    removed_count = 0 if dry_run else would_remove
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
    result = {
        "removed": removed_count,
        "oldest_kept_time": _oldest_time_from_docs(current_events),
        "legal_hold": legal_hold,
        "retention_days": retention_days,
        "dry_run": bool(dry_run),
    }
    if dry_run:
        result["would_remove"] = would_remove
    return result


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
    result = {
        "removed": actually_removed,
        "oldest_kept_time": (
            str(first_kept.get("time")) if first_kept and first_kept.get("time") else None
        ),
        "legal_hold": legal_hold,
        "retention_days": retention_days,
        "dry_run": bool(dry_run),
    }
    if dry_run:
        result["would_remove"] = removed_count
    return result


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
    """Authorized workspace scope for audit and log records."""
    if request is None:
        return ""
    try:
        from services.workspace_access import authorized_workspace_id

        return authorized_workspace_id(request)
    except Exception:
        return ""


def actor_from_request(request: Any) -> str:
    """Extract the actor email from a FastAPI request, if available."""
    if request is None:
        return "anonymous"
    user = getattr(request.state, "user", None)
    if user and isinstance(user, dict):
        return str(user.get("email") or user.get("sub") or "anonymous")
    return "anonymous"
