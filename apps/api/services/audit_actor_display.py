"""What the audit actor column shows for a machine identity (R7).

A key-authenticated record stores ``actor = "api_key:<uuid>"`` (and
``actor_id``) — exact, and part of the hashed record, so it is never rewritten.
On screen the operator needs to know *which key*, not its id: the key's name
plus a short masked id. Revoked keys keep their name (the record stays).
"""

from __future__ import annotations

import logging
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
