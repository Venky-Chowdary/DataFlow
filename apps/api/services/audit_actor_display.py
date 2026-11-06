"""What the audit actor column shows for a machine identity (R7).

A key-authenticated record stores ``actor = "api_key:<uuid>"`` (and
``actor_id``) — exact, and part of the hashed record, so it is never rewritten.
On screen the operator needs to know *which key*, not its id: the key's name
plus a short masked id. Revoked keys keep their name (the record stays).
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)

_MACHINE = {"api_key": "key", "service_account": "service account", "scim": "SCIM token"}


def _key_names() -> dict[str, str]:
    try:
        from services.integrations_store import load_api_key_records

        return {
            str(r.get("id")): str(r.get("name") or "").strip()
            for r in load_api_key_records()
            if r.get("id")
        }
    except Exception as exc:  # noqa: BLE001 — display only; the raw actor stays
        logger.debug("API key names unavailable for audit display: %s", exc)
        return {}


def _machine_identity(event: dict[str, Any]) -> tuple[str, str] | None:
    kind = str(event.get("actor_type") or "")
    key_id = str(event.get("actor_id") or "")
    if kind not in _MACHINE:
        actor = str(event.get("actor") or "")
        head, _, tail = actor.partition(":")
        if head in _MACHINE and tail:
            kind, key_id = head, tail
        else:
            return None
    return (kind, key_id) if key_id else None


def masked_id(key_id: str) -> str:
    return f"…{key_id[-4:]}" if key_id else "…"


def add_actor_display(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Set ``actor_display`` on each event (the actor itself is left exact)."""
    names: dict[str, str] | None = None
    for event in events:
        ident = _machine_identity(event)
        if ident is None:
            event["actor_display"] = str(event.get("actor") or "")
            continue
        if names is None:
            names = _key_names()
        kind, key_id = ident
        label = _MACHINE[kind]
        name = names.get(key_id, "")
        event["actor_display"] = (
            f"{name} ({label} {masked_id(key_id)})"
            if name
            else f"{label[:1].upper()}{label[1:]} {masked_id(key_id)}"
        )
    return events


# ─────────────────────────────────────────────────────────────────────────────
# Key identifiers for non-admin readers (Browser 20, QA r14)
# ─────────────────────────────────────────────────────────────────────────────
#
# Policy (secure default): an API key / service account / SCIM token id is not
# a secret, but it is the handle a revocation, rotation or forensic query is
# made with, so only the people who manage keys read it whole. A workspace
# admin (the role older builds called ``owner``) and a platform admin get the
# exact record — the export is evidence and must stay joinable to the key
# store. Every other reader (viewer, operator, editor; a key with those roles)
# gets each key id masked to its last four characters (``…1a2b``) in the
# event list and in every export format. The stored record and its hash are
# never changed; a masked export says so (``key_ids_masked``) because its
# records no longer re-hash to ``event_hash``.

_UUID = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


def reader_sees_key_ids(request: Any, workspace_id: str) -> bool:
    """Whether this reader manages keys in ``workspace_id`` (or the platform)."""
    from services.effective_principal import request_principal
    from services.rbac import Permission

    try:
        principal = request_principal(request, workspace_id or "")
    except Exception as exc:  # noqa: BLE001 — unknown reader: mask (fail closed)
        logger.debug("audit reader principal unavailable: %s", exc)
        return False
    return bool(principal.platform_admin or principal.has(Permission.WORKSPACE_MANAGE))


def _known_key_ids(events: list[dict[str, Any]]) -> set[str]:
    ids = set(_key_names())
    for event in events:
        ident = _machine_identity(event)
        if ident is not None:
            ids.add(ident[1])
    return {i.lower() for i in ids if i}


def _mask_value(value: Any, ids: set[str]) -> Any:
    if isinstance(value, str):
        return _UUID.sub(
            lambda m: masked_id(m.group(0)) if m.group(0).lower() in ids else m.group(0), value
        )
    if isinstance(value, dict):
        return {k: _mask_value(v, ids) for k, v in value.items()}
    if isinstance(value, list):
        return [_mask_value(v, ids) for v in value]
    return value


#: Fields that identify the record or the chain, never a key: left exact.
_EXACT_FIELDS = frozenset({"_id", "id", "seq", "event_hash", "prev_hash", "ws_prev_hash",
                           "workspace_id", "tenant_id", "correlation_id", "key_id"})


def mask_key_ids(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Copies of ``events`` with every API-key id masked (``…`` + last four).

    Only ids of keys this deployment issued (the key store, plus the actor of
    a machine-authenticated record) are masked, wherever they appear — actor,
    ``actor_id``, resource, details — so job, schedule and record ids stay
    readable. ``key_id`` is the audit-chain signing key id, not an API key.
    """
    ids = _known_key_ids(events)
    if not ids:
        return [dict(e) for e in events]
    out: list[dict[str, Any]] = []
    for event in events:
        out.append(
            {k: (v if k in _EXACT_FIELDS else _mask_value(v, ids)) for k, v in event.items()}
        )
    return out
