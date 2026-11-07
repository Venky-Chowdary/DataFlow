"""API keys with no workspace: who may revoke them, and the startup repair (K1).

Before round 8 the rotate endpoint wrote the new key with ``workspace_id ""``
and the rotating caller as ``created_by``. Such a key still authenticates
(``integrations_store.verify_workspace_api_key`` never looked at the
workspace), yet the key routes showed a workspace-less key to platform admins
only, so its owner could neither see nor revoke it (QA K1: ids 276d4753… and
1e68a5e6… on production). The repair existed, but only as a platform-admin
endpoint nobody ran.

Two things close it:

* **Access** (:func:`orphan_key_manageable`): besides platform admins, the key's
  creator — or an admin of the workspace being looked at, when the creator is a
  member of it — sees a workspace-less key in Settings -> API keys and revokes
  it with the normal DELETE. A platform admin's deliberately platform-wide key
  is not an orphan, and no platform admin's key is handed to workspace admins:
  only platform admins and the creator manage those. Signed-in people only: a key never manages
  another workspace-less key. Rotation stays refused (it would mint a second
  credential with no workspace).
* **Repair** (:func:`run_orphaned_api_key_migration`): once per deployment, at
  startup, every live workspace-less key gets a workspace back — through its
  rotation chain first (``integrations_store.repair_rotated_api_keys``), else
  the creator's sole workspace (platform-wide keys are skipped). A key whose owner is ambiguous (creator in
  several workspaces, or none) is left as it is and stays revocable as above
  and visible to platform admins. Idempotent; audited
  (``iam.api_key.orphan_migration``) whenever it changes or leaves something.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)

MIGRATION_ID = "orphaned_api_keys_v1"
_MARKER_COLLECTION = "platform_migrations"
_MARKER_FILE = "platform_migrations.json"


# ── access ──────────────────────────────────────────────────────────────────


def _norm(email: Any) -> str:
    return str(email or "").strip().lower()


def is_platform_key(key: dict[str, Any]) -> bool:
    """A platform admin's ``admin`` key with no workspace: deliberately platform-wide.

    The same rule the request gate and the key list's ``access`` label use
    (``tenant_scope.actor_is_unrestricted``). Such a key is not an orphan: it
    stays the platform admins' (and its creator's), and is never bound to a
    workspace by the repair.
    """
    from services.integrations_store import resolve_stored_api_key_role
    from services.tenant_scope import actor_is_unrestricted

    return bool(actor_is_unrestricted({
        "email": key.get("created_by") or "",
        "role": resolve_stored_api_key_role(key.get("role")),
        "auth_kind": "api_key",
        "workspace_id": "",
    }))


def _is_platform_admin(email: str) -> bool:
    from src.services.auth_service import lookup_user

    try:
        account = lookup_user(email)
    except Exception:  # noqa: BLE001 - unreadable: treat as platform-administered
        return True
    return bool(account) and str(account.get("role") or "").strip().lower() == "admin"


def orphan_key_manageable(request: Any, key: dict[str, Any]) -> bool:
    """May this signed-in person list and revoke this workspace-less key?"""
    if str(key.get("workspace_id") or "").strip():
        return False
    user = getattr(getattr(request, "state", None), "user", None) or {}
    if str(user.get("auth_kind") or "") in {"api_key", "service_account", "scim"}:
        return False
    caller = _norm(user.get("email"))
    creator = _norm(key.get("created_by"))
    if not caller or not creator:
        return False
    if caller == creator:
        return True
    if is_platform_key(key) or _is_platform_admin(creator):
        # A platform admin's keys are platform-administered: workspace admins
        # of workspaces the platform admin happens to belong to do not get them.
        return False
    from services.effective_principal import request_principal
    from services.team_store import get_workspace_role
    from services.workspace_access import tenant_scope

    workspace_id = str(tenant_scope(request).workspace_id or "")
    if not workspace_id:
        return False
    try:
        creator_is_member = bool(get_workspace_role(workspace_id=workspace_id, email=creator))
    except Exception:  # noqa: BLE001 - an unreadable store grants nothing
        return False
    return creator_is_member and request_principal(request, workspace_id).member_role == "admin"


# ── one-time repair ───────────────────────────────────────────────────────────


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _marker_path():
    from services.platform_config import data_dir

    return data_dir() / _MARKER_FILE


def _read_marker() -> dict[str, Any] | None:
    from services.control_plane_store import mongo_collection

    coll = mongo_collection(_MARKER_COLLECTION)
    if coll is not None:
        try:
            doc = coll.find_one({"_id": MIGRATION_ID})
            if doc:
                return {k: v for k, v in doc.items() if k != "_id"}
        except Exception:  # noqa: BLE001 - fall through to the file copy
            logger.warning("migration marker read failed", exc_info=True)
    try:
        data = json.loads(_marker_path().read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    entry = data.get(MIGRATION_ID) if isinstance(data, dict) else None
    return entry if isinstance(entry, dict) else None


def _write_marker(entry: dict[str, Any]) -> None:
    from services.control_plane_store import mongo_collection

    coll = mongo_collection(_MARKER_COLLECTION)
    if coll is not None:
        try:
            coll.replace_one({"_id": MIGRATION_ID}, {"_id": MIGRATION_ID, **entry}, upsert=True)
        except Exception:  # noqa: BLE001 - the file copy still records it
            logger.warning("migration marker write failed", exc_info=True)
    path = _marker_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
        if not isinstance(data, dict):
            data = {}
        data[MIGRATION_ID] = entry
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    except (OSError, ValueError):
        logger.warning("migration marker file write failed", exc_info=True)


def _creator_sole_workspace(creator: str) -> str:
    from services.team_store import list_memberships_for_user

    if not creator:
        return ""
    try:
        memberships = list_memberships_for_user(creator)
    except Exception:  # noqa: BLE001 - unreadable: leave the key as it is
        return ""
    spaces = {str(m.get("workspace_id") or "") for m in memberships} - {""}
    return next(iter(spaces)) if len(spaces) == 1 else ""


def migrate_orphaned_api_keys(*, dry_run: bool = False) -> dict[str, Any]:
    """Give every live workspace-less key a workspace where one is certain."""
    from services import integrations_store as store

    repairs = [
        {**row, "via": "rotation_chain"}
        for row in store.repair_rotated_api_keys(dry_run=dry_run)
    ]
    repaired = {row["id"] for row in repairs}
    unresolved: list[dict[str, Any]] = []
    for record in store.load_api_key_records():
        if record.get("workspace_id") or store._is_revoked(record) or record["id"] in repaired:
            continue
        if store._key_expired(record) or is_platform_key(record):
            continue
        creator = str(record.get("created_by") or "")
        workspace_id = _creator_sole_workspace(creator)
        row = {
            "id": record["id"],
            "prefix": record.get("prefix"),
            "kind": record.get("kind") or "api_key",
            "created_by": creator or None,
        }
        if not workspace_id:
            unresolved.append({**row, "reason": "creator_workspace_ambiguous"})
            continue
        repairs.append({**row, "workspace_id": workspace_id, "via": "creator_sole_workspace"})
        if not dry_run:
            store._upsert_api_key_record({**record, "workspace_id": workspace_id})
    return {"repairs": repairs, "unresolved": unresolved}


def run_orphaned_api_key_migration(*, force: bool = False) -> dict[str, Any]:
    """The startup step: once per deployment (``force`` re-runs it; it is idempotent).

    Never raises: a key store that cannot be read must not stop the API from
    booting. Returns ``{"ran": bool, "repairs": [...], "unresolved": [...]}``.
    """
    try:
        previous = _read_marker()
        if not force and previous is not None:
            return {"ran": False, "repairs": [], "unresolved": []}
        result = migrate_orphaned_api_keys()
    except Exception as exc:  # noqa: BLE001 - boot continues; next start retries
        logger.warning("orphaned API key migration failed (%s)", type(exc).__name__, exc_info=True)
        return {"ran": False, "repairs": [], "unresolved": [], "error": type(exc).__name__}
    # Audited when it changed something, or the first time it leaves a key as it
    # is; a forced re-run that finds the same ambiguous keys adds no noise.
    if result["repairs"] or (result["unresolved"] and previous is None):
        from services.audit_log import append_audit_event

        append_audit_event(
            action="iam.api_key.orphan_migration",
            resource="api_keys",
            actor="system",
            level="warn",
            details={
                "migration": MIGRATION_ID,
                "repaired": len(result["repairs"]),
                "unresolved": len(result["unresolved"]),
                "repairs": result["repairs"],
                "unresolved_keys": result["unresolved"],
            },
        )
    _write_marker({
        "at": _now(),
        "repaired": len(result["repairs"]),
        "unresolved": len(result["unresolved"]),
    })
    return {"ran": True, **result}
