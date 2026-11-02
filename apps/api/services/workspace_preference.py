"""Which workspace an account lands in when it signs in (QA D218).

The browser forgets the active workspace at sign-out (D154: the next account on
that browser must not inherit it), and the API never named one for a platform
admin, so the web app took the first row of ``GET /team/workspaces`` — the
oldest workspace in the deployment. The landing workspace is now decided here,
deterministically, in this order:

1. the workspace this account last used, while it still exists and the account
   may still read it (remembered server-side when ``/auth/me`` answers for a
   workspace the request named and was allowed to name);
2. the account's own workspace: its personal workspace (the one signup made),
   else its earliest membership (``tenant_scope.ordered_memberships``);
3. only for an account that belongs to no workspace at all (a bootstrap
   platform admin on a fresh deployment): the workspace it created, else the
   oldest one — there is nothing better to offer.

The preference lives next to the team store (same Mongo database, or a JSON
file beside ``teams.json``), keyed by normalized email. It is a convenience:
any failure to read or write it falls back to rule 2 and never fails a request.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

PREFERENCES_COLLECTION = "workspace_preferences"
_EMPTY_FILE: dict[str, Any] = {"preferences": {}}


def _file_path() -> Path:
    from services import team_store

    return team_store._store_path().with_name("workspace_preferences.json")


def last_used_workspace(email: str) -> str:
    from services import team_store
    from services.metadata_backend import load_json_doc

    key = team_store.normalize_email(email)
    if not key:
        return ""
    try:
        db = team_store._database()
        if db is not None:
            row = db[PREFERENCES_COLLECTION].find_one({"_id": key}) or {}
        else:
            row = (load_json_doc(_file_path(), _EMPTY_FILE).get("preferences") or {}).get(key) or {}
        return str(row.get("last_workspace_id") or "").strip()
    except Exception as exc:  # noqa: BLE001 - a preference must never fail sign-in
        logger.warning("last-used workspace read failed (%s)", type(exc).__name__)
        return ""


def remember_workspace(email: str, workspace_id: str) -> None:
    """Record ``workspace_id`` as the account's last-used workspace (no-op when unchanged)."""
    from services import team_store
    from services.metadata_backend import json_doc_transaction

    key = team_store.normalize_email(email)
    ws = str(workspace_id or "").strip()
    if not key or not ws or last_used_workspace(key) == ws:
        return
    row = {"last_workspace_id": ws, "updated_at": datetime.now(timezone.utc).isoformat()}
    try:
        db = team_store._database()
        if db is not None:
            db[PREFERENCES_COLLECTION].update_one({"_id": key}, {"$set": row}, upsert=True)
            return
        with json_doc_transaction(_file_path(), _EMPTY_FILE) as data:
            prefs = data.setdefault("preferences", {})
            prefs[key] = row
    except Exception as exc:  # noqa: BLE001
        logger.warning("last-used workspace write failed (%s)", type(exc).__name__)


def landing_workspace_id(scope: Any) -> str:
    """The workspace ``scope``'s actor should land in (see the module docstring)."""
    from services import team_store

    actor = str(getattr(scope, "actor", "") or "")
    members = [ws for ws, _role in getattr(scope, "memberships", ()) or ()]
    unrestricted = bool(getattr(scope, "unrestricted", False))
    last = last_used_workspace(actor) if actor and actor != "anonymous" else ""
    if last and (last in members or unrestricted) and team_store.get_workspace(last) is not None:
        return last
    if members:
        return members[0]
    if not unrestricted:
        return ""
    everything = team_store.list_workspaces()
    own = [ws for ws in everything if ws.created_by == team_store.normalize_email(actor)]
    pick = (own or everything)[:1]
    return pick[0].id if pick else ""


__all__ = ["landing_workspace_id", "last_used_workspace", "remember_workspace"]
