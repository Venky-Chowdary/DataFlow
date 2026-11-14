"""What a schedule save must say about its connectors' proof (CLM-7, CLM-13).

* An engine without CI proof is "Implemented — not yet proven" and is not
  offered for production schedules: the save says so by name.
* A Salesforce connector on a pasted session token cannot be refreshed, so a
  schedule on it fails once the token expires: the save says so and names
  the fix (OAuth client credentials).

Warnings, not refusals: the operator may still schedule a trial run.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


def schedule_claim_warnings(source_connector_id: str, dest_connector_id: str) -> list[str]:
    from connectors.salesforce_auth import session_token_schedule_warning
    from services.catalog_service import get_connector_by_id
    from services.connector_store import get_connector
    from src.transfer.connector_capabilities import resolve_driver_type

    out: list[str] = []
    for role, connector_id in (("Source", source_connector_id), ("Destination", dest_connector_id)):
        if not connector_id:
            continue
        try:
            conn = get_connector(str(connector_id))
        except Exception:  # noqa: BLE001 - a warning must never fail the save
            logger.debug("schedule warning: connector %s unreadable", connector_id, exc_info=True)
            continue
        if conn is None:
            continue
        ctype = str(getattr(conn, "type", "") or "").lower()
        tile = get_connector_by_id(ctype) or get_connector_by_id(resolve_driver_type(ctype)) or {}
        if tile.get("certification_tier") == "implemented":
            out.append(
                f"{role} {conn.name} ({tile.get('name') or ctype}) is Implemented — not yet "
                "proven: no green CI run proves this engine, so it is not offered for "
                "production schedules. The schedule is saved; treat its runs as a trial."
            )
        if resolve_driver_type(ctype) == "salesforce":
            note = session_token_schedule_warning(
                {
                    "auth_mode": getattr(conn, "auth_mode", ""),
                    "api_key": getattr(conn, "api_key", ""),
                    "connection_string": getattr(conn, "connection_string", ""),
                    "username": getattr(conn, "username", ""),
                    "password": getattr(conn, "password", ""),
                },
                name=f"{role} {conn.name}",
            )
            if note:
                out.append(note)
    return out
