"""Team/workspace membership store for multi-tenant RBAC.

Workspaces are the top-level isolation boundary.  Each workspace has a list of
members with roles: owner, editor, viewer.  Connectors, jobs, and API keys can
be scoped to a workspace.  The global/default workspace (id="") is a fallback
for existing un-scoped resources and is always accessible.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.platform_config import data_dir

try:
    from src.services.mongodb_service import get_mongodb_service
except ImportError:
    from services.mongodb_service import get_mongodb_service

STORE_PATH = data_dir() / "teams.json"

_ROLES = ("owner", "editor", "viewer")
_ROLE_RANK = {"owner": 3, "editor": 2, "viewer": 1, "": 0}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = os.getenv("DATAFLOW_TEAM_STORE", "").strip()
    return Path(env) if env else STORE_PATH


def _mongo_backend():
    """Return a real MongoDB service when connected, otherwise None."""
    try:
        svc = get_mongodb_service()
    except Exception:
        return None
    if type(svc).__name__ == "MemoryMongoDBService":
        return None
    return svc if getattr(svc, "client", None) is not None else None


def _load_mongo(svc):
    db = svc.get_database()
    doc = db["team_store"].find_one({"_id": "primary"})
    if not doc:
        return {"workspaces": [], "memberships": []}
    return {
        "workspaces": doc.get("workspaces", []),
        "memberships": doc.get("memberships", []),
    }


def _save_mongo(svc, data: dict[str, Any]) -> None:
    db = svc.get_database()
    db["team_store"].replace_one(
        {"_id": "primary"},
        {"_id": "primary", "workspaces": data.get("workspaces", []), "memberships": data.get("memberships", [])},
        upsert=True,
    )


@dataclass
class Workspace:
    id: str
    name: str
    created_at: str = field(default_factory=_now)
    created_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Workspace":
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            created_at=data.get("created_at", _now()),
            created_by=data.get("created_by", ""),
        )


@dataclass
class Membership:
    workspace_id: str
    email: str
    role: str = "viewer"
    added_at: str = field(default_factory=_now)
    added_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Membership":
        return cls(
            workspace_id=data.get("workspace_id", ""),
            email=data.get("email", ""),
            role=data.get("role", "viewer"),
            added_at=data.get("added_at", _now()),
            added_by=data.get("added_by", ""),
        )


def _load_raw() -> dict[str, Any]:
    svc = _mongo_backend()
    if svc:
        return _load_mongo(svc)
    path = _store_path()
    if not path.exists():
        return {"workspaces": [], "memberships": []}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"workspaces": [], "memberships": []}
        return raw
    except Exception:
        return {"workspaces": [], "memberships": []}


def _save(data: dict[str, Any]) -> None:
    svc = _mongo_backend()
    if svc:
        _save_mongo(svc, data)
        return
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(path)


def _workspaces(data: dict[str, Any]) -> list[Workspace]:
    return [Workspace.from_dict(w) for w in data.get("workspaces", []) if isinstance(w, dict)]


def _memberships(data: dict[str, Any]) -> list[Membership]:
    return [Membership.from_dict(m) for m in data.get("memberships", []) if isinstance(m, dict)]


def create_workspace(*, name: str, created_by: str) -> Workspace:
    data = _load_raw()
    ws = Workspace(id=str(uuid.uuid4()), name=name.strip()[:128] or "Workspace", created_by=created_by)
    data["workspaces"].append(ws.to_dict())
    data["memberships"].append(
        Membership(
            workspace_id=ws.id,
            email=created_by,
            role="owner",
            added_by=created_by,
        ).to_dict()
    )
    _save(data)
    return ws


def get_workspace(workspace_id: str) -> Workspace | None:
    if not workspace_id:
        return None
    for ws in _workspaces(_load_raw()):
        if ws.id == workspace_id:
            return ws
    return None


def list_workspaces_for_user(email: str) -> list[Workspace]:
    data = _load_raw()
    memberships = _memberships(data)
    user_ws_ids = {m.workspace_id for m in memberships if m.email == email}
    return [ws for ws in _workspaces(data) if ws.id in user_ws_ids]


def add_workspace_member(
    *,
    workspace_id: str,
    email: str,
    role: str,
    added_by: str,
) -> Membership | None:
    if not workspace_id:
        return None
    role = role if role in _ROLES else "viewer"
    data = _load_raw()
    ws = next((w for w in _workspaces(data) if w.id == workspace_id), None)
    if not ws:
        return None
    # Only owners/editors can add members; owners can add owners, editors cannot.
    actor_role = get_workspace_role(workspace_id=workspace_id, email=added_by)
    if actor_role not in ("owner", "editor"):
        return None
    if role == "owner" and actor_role != "owner":
        return None
    memberships = _memberships(data)
    existing = next((m for m in memberships if m.workspace_id == workspace_id and m.email == email), None)
    if existing:
        existing.role = role
        _save(data)
        return existing
    membership = Membership(workspace_id=workspace_id, email=email, role=role, added_by=added_by)
    data["memberships"].append(membership.to_dict())
    _save(data)
    return membership


def remove_workspace_member(*, workspace_id: str, email: str, removed_by: str) -> bool:
    if not workspace_id:
        return False
    actor_role = get_workspace_role(workspace_id=workspace_id, email=removed_by)
    if actor_role not in ("owner", "editor"):
        return False
    target_role = get_workspace_role(workspace_id=workspace_id, email=email)
    if not target_role:
        return False
    # Editors cannot remove owners.
    if actor_role == "editor" and target_role == "owner":
        return False
    data = _load_raw()
    before = len(data.get("memberships", []))
    data["memberships"] = [
        m for m in data.get("memberships", [])
        if not (m.get("workspace_id") == workspace_id and m.get("email") == email)
    ]
    if len(data["memberships"]) == before:
        return False
    _save(data)
    return True


def get_workspace_role(*, workspace_id: str, email: str) -> str:
    """Return the user's role in a workspace, or '' if not a member."""
    if not workspace_id:
        return ""
    for m in _memberships(_load_raw()):
        if m.workspace_id == workspace_id and m.email == email:
            return m.role if m.role in _ROLES else "viewer"
    return ""


def list_workspace_members(workspace_id: str) -> list[dict[str, Any]]:
    if not workspace_id:
        return []
    return [
        m.to_dict()
        for m in _memberships(_load_raw())
        if m.workspace_id == workspace_id
    ]


def can_read_workspace(workspace_id: str, email: str) -> bool:
    if not workspace_id:
        return True
    return get_workspace_role(workspace_id=workspace_id, email=email) in _ROLES


def can_write_workspace(workspace_id: str, email: str) -> bool:
    if not workspace_id:
        return True
    return get_workspace_role(workspace_id=workspace_id, email=email) in ("owner", "editor")


def can_admin_workspace(workspace_id: str, email: str) -> bool:
    return get_workspace_role(workspace_id=workspace_id, email=email) == "owner"
