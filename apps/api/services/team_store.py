"""Workspaces and who belongs to them.

Workspaces are the tenant isolation boundary: connectors, jobs, schedules and API
keys are scoped to one. Each workspace has members with a role — ``admin``,
``editor`` or ``viewer``. The global workspace (id ``""``) carries resources that
were created before scoping existed and is always readable unless hard isolation
is switched on.

Persistence is one document per workspace and one per membership, in the metadata
database, related by ``workspace_id``. It used to be a single document holding two
arrays, which meant every add re-wrote the whole team (two concurrent invites lost
one) and nothing stopped the same email appearing twice in one workspace. The
membership ``_id`` is now ``"<workspace_id>:<email>"``, so a duplicate invite is a
replace rather than a second row, and the write is atomic.
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pymongo.database import Database
from pymongo.errors import ConfigurationError, OperationFailure, PyMongoError

from services.brand_env import getenv_brand
from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir

WORKSPACES_COLLECTION = "workspaces"
MEMBERS_COLLECTION = "workspace_members"

STORE_PATH = data_dir() / "teams.json"

# Canonical workspace roles. ``owner`` is the pre-rename name for ``admin`` and is
# still accepted on read so deployments written by an older build keep working.
ROLES = ("admin", "editor", "viewer")
_LEGACY_ROLE_ALIASES = {"owner": "admin"}
_ADMIN_ROLES = ("admin",)
_WRITE_ROLES = ("admin", "editor")


class TeamStoreError(Exception):
    """A membership change the store must refuse, carrying the reason.

    Returning a bare ``False``/``None`` made every refusal look identical, so the
    API answered 403 whether the workspace did not exist, the actor lacked the
    role, or the change would leave the workspace with no administrator. Each
    reason is its own type so the operator is told which one happened.
    """


class WorkspaceNotFound(TeamStoreError):
    pass


class PermissionDenied(TeamStoreError):
    pass


class MemberNotFound(TeamStoreError):
    pass


class LastAdminProtected(TeamStoreError):
    pass


class MemberAlreadyExists(TeamStoreError):
    """An *add* was asked to overwrite a membership that already exists.

    Adding and re-roling are different acts: an invitation form that quietly
    re-roles reports "member added" for someone who was already here, and can
    silently demote an admin to viewer on a typo. The role change has its own
    route, so the add path refuses and names the role held today.
    """


def _assert_not_last_admin(workspace_id: str, email: str) -> None:
    """Refuse a change that would leave a workspace with nobody who can administer it."""
    admins = [m for m in _read_memberships(workspace_id=workspace_id) if m.role == "admin"]
    if len(admins) <= 1 and any(m.email == normalize_email(email) for m in admins):
        raise LastAdminProtected(
            "This is the only admin — grant the admin role to someone else first"
        )


def _is_transactions_unsupported(exc: OperationFailure) -> bool:
    """True when the server refused the transaction because it is a standalone.

    A standalone mongod answers ``IllegalOperation`` (code 20) with "Transaction
    numbers are only allowed on a replica set member or mongos". That is a
    deployment shape, not a failure of this write, so the caller falls back to
    sequential writes with a compensating delete instead of surfacing an error.
    """
    return exc.code == 20 or "replica set member or mongos" in str(exc)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = getenv_brand("TEAM_STORE", "").strip()
    return Path(env) if env else STORE_PATH


def normalize_role(role: str) -> str:
    """Map a stored or requested role onto a canonical role, defaulting to viewer."""
    value = (role or "").strip().lower()
    value = _LEGACY_ROLE_ALIASES.get(value, value)
    return value if value in ROLES else "viewer"


def normalize_email(email: str) -> str:
    return (email or "").strip().lower()


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
            email=normalize_email(data.get("email", "")),
            role=normalize_role(data.get("role", "viewer")),
            added_at=data.get("added_at", _now()),
            added_by=data.get("added_by", ""),
        )


def _member_key(workspace_id: str, email: str) -> str:
    return f"{workspace_id}:{normalize_email(email)}"


# ─────────────────────────────────────────────────────────────────────────────
# Persistence — MongoDB collections, or one JSON file on a developer box
# ─────────────────────────────────────────────────────────────────────────────


def _migrate_blob(db: Database) -> None:
    """Fan the old single ``team_store/primary`` document out into both collections.

    Runs at most once per deployment: the blob is deleted after its rows land, and
    each row is written with its deterministic ``_id`` so a partially-completed
    migration is safe to repeat.
    """
    blob = db["team_store"].find_one({"_id": "primary"})
    if not blob:
        return
    for raw in blob.get("workspaces", []):
        if isinstance(raw, dict) and raw.get("id"):
            ws = Workspace.from_dict(raw)
            db[WORKSPACES_COLLECTION].replace_one(
                {"_id": ws.id}, {"_id": ws.id, **ws.to_dict()}, upsert=True
            )
    for raw in blob.get("memberships", []):
        if isinstance(raw, dict) and raw.get("workspace_id") and raw.get("email"):
            m = Membership.from_dict(raw)
            key = _member_key(m.workspace_id, m.email)
            db[MEMBERS_COLLECTION].replace_one({"_id": key}, {"_id": key, **m.to_dict()}, upsert=True)
    db["team_store"].delete_one({"_id": "primary"})


_PREPARED_DATABASES: set[str] = set()


def _database() -> Database | None:
    """Return the metadata database with indexes and migration applied, or None."""
    db = mongo_database()
    if db is None:
        return None
    if db.name not in _PREPARED_DATABASES:
        db[MEMBERS_COLLECTION].create_index([("workspace_id", 1), ("email", 1)], unique=True)
        db[MEMBERS_COLLECTION].create_index([("email", 1)])
        db[WORKSPACES_COLLECTION].create_index([("created_by", 1)])
        _migrate_blob(db)
        _PREPARED_DATABASES.add(db.name)
    return db


_EMPTY_FILE: dict[str, Any] = {"workspaces": [], "memberships": []}


def _file_data() -> dict[str, Any]:
    return load_json_doc(_store_path(), _EMPTY_FILE)


def _read_workspaces() -> list[Workspace]:
    db = _database()
    if db is not None:
        rows = db[WORKSPACES_COLLECTION].find({}, {"_id": False})
    else:
        rows = [w for w in _file_data().get("workspaces", []) if isinstance(w, dict)]
    return [Workspace.from_dict(dict(row)) for row in rows if row.get("id")]


def _read_memberships(*, workspace_id: str | None = None, email: str | None = None) -> list[Membership]:
    query: dict[str, Any] = {}
    if workspace_id is not None:
        query["workspace_id"] = workspace_id
    if email is not None:
        query["email"] = normalize_email(email)
    db = _database()
    if db is not None:
        rows = [dict(r) for r in db[MEMBERS_COLLECTION].find(query, {"_id": False})]
    else:
        rows = [m for m in _file_data().get("memberships", []) if isinstance(m, dict)]
    members = [Membership.from_dict(row) for row in rows]
    if workspace_id is not None:
        members = [m for m in members if m.workspace_id == workspace_id]
    if email is not None:
        members = [m for m in members if m.email == normalize_email(email)]
    return members


def _write_workspace(ws: Workspace) -> None:
    db = _database()
    if db is not None:
        db[WORKSPACES_COLLECTION].replace_one({"_id": ws.id}, {"_id": ws.id, **ws.to_dict()}, upsert=True)
        return
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        data["workspaces"] = [w for w in data.get("workspaces", []) if w.get("id") != ws.id] + [ws.to_dict()]


def _write_membership(m: Membership) -> None:
    db = _database()
    if db is not None:
        key = _member_key(m.workspace_id, m.email)
        db[MEMBERS_COLLECTION].replace_one({"_id": key}, {"_id": key, **m.to_dict()}, upsert=True)
        return
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        data["memberships"] = [
            row
            for row in data.get("memberships", [])
            if not (
                row.get("workspace_id") == m.workspace_id
                and normalize_email(row.get("email", "")) == m.email
            )
        ] + [m.to_dict()]


def _delete_membership(workspace_id: str, email: str) -> bool:
    db = _database()
    if db is not None:
        return db[MEMBERS_COLLECTION].delete_one({"_id": _member_key(workspace_id, email)}).deleted_count > 0
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        before = len(data.get("memberships", []))
        data["memberships"] = [
            row
            for row in data.get("memberships", [])
            if not (
                row.get("workspace_id") == workspace_id
                and normalize_email(row.get("email", "")) == normalize_email(email)
            )
        ]
        removed = len(data["memberships"]) != before
    return removed


def _delete_workspace_rows(workspace_id: str) -> bool:
    """Delete a workspace and its memberships — never leave members without a workspace."""
    db = _database()
    if db is not None:
        removed = db[WORKSPACES_COLLECTION].delete_one({"_id": workspace_id}).deleted_count > 0
        db[MEMBERS_COLLECTION].delete_many({"workspace_id": workspace_id})
        return removed
    with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
        before = len(data.get("workspaces", []))
        data["workspaces"] = [w for w in data.get("workspaces", []) if w.get("id") != workspace_id]
        data["memberships"] = [
            m for m in data.get("memberships", []) if m.get("workspace_id") != workspace_id
        ]
        removed = len(data["workspaces"]) != before
    return removed


# ─────────────────────────────────────────────────────────────────────────────
# Workspace lifecycle
# ─────────────────────────────────────────────────────────────────────────────


def create_workspace(*, name: str, created_by: str) -> Workspace:
    """Create a workspace and make its creator the first admin, as one unit.

    The workspace row and its first membership are two documents, so writing them
    one after the other can leave a workspace nobody administers (and nobody can
    invite into) if the second write fails. On a replica set both land in one
    MongoDB transaction; on a standalone server — where transactions are not
    available — the workspace row is removed again before the failure is raised.
    """
    ws = Workspace(
        id=str(uuid.uuid4()),
        name=name.strip()[:128] or "Workspace",
        created_by=normalize_email(created_by),
    )
    first_admin = Membership(
        workspace_id=ws.id,
        email=normalize_email(created_by),
        role="admin",
        added_by=normalize_email(created_by),
    )
    db = _database()
    if db is None:
        # One file transaction already covers both rows.
        with json_doc_transaction(_store_path(), _EMPTY_FILE) as data:
            data["workspaces"] = list(data.get("workspaces", [])) + [ws.to_dict()]
            data["memberships"] = list(data.get("memberships", [])) + [first_admin.to_dict()]
        return ws
    member_key = _member_key(ws.id, first_admin.email)
    try:
        with db.client.start_session() as session:
            with session.start_transaction():
                db[WORKSPACES_COLLECTION].insert_one(
                    {"_id": ws.id, **ws.to_dict()}, session=session
                )
                db[MEMBERS_COLLECTION].insert_one(
                    {"_id": member_key, **first_admin.to_dict()}, session=session
                )
        return ws
    except ConfigurationError:
        pass  # Driver knows this deployment cannot start a session/transaction.
    except OperationFailure as exc:
        if not _is_transactions_unsupported(exc):
            raise
    db[WORKSPACES_COLLECTION].insert_one({"_id": ws.id, **ws.to_dict()})
    try:
        db[MEMBERS_COLLECTION].insert_one({"_id": member_key, **first_admin.to_dict()})
    except PyMongoError:
        db[WORKSPACES_COLLECTION].delete_one({"_id": ws.id})
        raise
    return ws


def rename_workspace(*, workspace_id: str, name: str) -> Workspace | None:
    ws = get_workspace(workspace_id)
    if not ws:
        return None
    ws.name = name.strip()[:128] or ws.name
    _write_workspace(ws)
    return ws


def delete_workspace(*, workspace_id: str) -> bool:
    if not workspace_id:
        return False
    return _delete_workspace_rows(workspace_id)


def get_workspace(workspace_id: str) -> Workspace | None:
    if not workspace_id:
        return None
    for ws in _read_workspaces():
        if ws.id == workspace_id:
            return ws
    return None


def list_workspaces() -> list[Workspace]:
    """Every workspace in the deployment — for platform administrators."""
    return sorted(_read_workspaces(), key=lambda w: (w.created_at, w.name))


def list_workspaces_for_user(email: str) -> list[Workspace]:
    member_of = {m.workspace_id for m in _read_memberships(email=email)}
    return [ws for ws in list_workspaces() if ws.id in member_of]


# ─────────────────────────────────────────────────────────────────────────────
# Membership
# ─────────────────────────────────────────────────────────────────────────────


def add_workspace_member(
    *,
    workspace_id: str,
    email: str,
    role: str,
    added_by: str,
    actor_is_platform_admin: bool = False,
    refuse_existing: bool = False,
) -> Membership:
    """Add or re-role a member, or raise the reason it cannot be done.

    ``refuse_existing`` is what an invitation passes: it makes an existing
    membership a conflict (``MemberAlreadyExists``) instead of a silent re-role.
    The role-change route leaves it false, because re-roling is its whole job.

    A platform administrator may seed the first member of any workspace: without
    that, a workspace created by one admin could never be handed to anyone else.
    """
    if not workspace_id or not get_workspace(workspace_id):
        raise WorkspaceNotFound("Workspace not found")
    role = normalize_role(role)
    actor_role = get_workspace_role(workspace_id=workspace_id, email=added_by)
    if actor_role not in _WRITE_ROLES and not actor_is_platform_admin:
        raise PermissionDenied("Adding members requires the workspace admin or editor role")
    # An editor may bring in peers but may not mint another administrator.
    if role == "admin" and actor_role != "admin" and not actor_is_platform_admin:
        raise PermissionDenied("Only a workspace admin can grant the admin role")
    existing = next(
        (m for m in _read_memberships(workspace_id=workspace_id, email=email)),
        None,
    )
    if existing and refuse_existing:
        raise MemberAlreadyExists(
            f"{normalize_email(email)} is already a member of this workspace "
            f"as {existing.role} — change their role instead of adding them again"
        )
    if existing and existing.role == "admin" and role != "admin":
        _assert_not_last_admin(workspace_id, email)
    membership = Membership(
        workspace_id=workspace_id,
        email=normalize_email(email),
        role=role,
        added_at=existing.added_at if existing else _now(),
        added_by=existing.added_by if existing else normalize_email(added_by),
    )
    _write_membership(membership)
    return membership


def remove_workspace_member(
    *,
    workspace_id: str,
    email: str,
    removed_by: str,
    actor_is_platform_admin: bool = False,
) -> None:
    """Remove a member, refusing to leave a workspace with no administrator."""
    if not workspace_id or not get_workspace(workspace_id):
        raise WorkspaceNotFound("Workspace not found")
    actor_role = get_workspace_role(workspace_id=workspace_id, email=removed_by)
    if actor_role not in _WRITE_ROLES and not actor_is_platform_admin:
        raise PermissionDenied("Removing members requires the workspace admin or editor role")
    target_role = get_workspace_role(workspace_id=workspace_id, email=email)
    if not target_role:
        raise MemberNotFound(f"{normalize_email(email)} is not a member of this workspace")
    if actor_role == "editor" and target_role == "admin" and not actor_is_platform_admin:
        raise PermissionDenied("Only a workspace admin can remove another admin")
    if target_role == "admin":
        _assert_not_last_admin(workspace_id, email)
    if not _delete_membership(workspace_id, email):
        raise MemberNotFound(f"{normalize_email(email)} is not a member of this workspace")


def get_workspace_role(*, workspace_id: str, email: str) -> str:
    """The user's role in a workspace, or '' when they are not a member."""
    if not workspace_id:
        return ""
    for m in _read_memberships(workspace_id=workspace_id, email=email):
        return m.role
    return ""


def list_workspace_members(workspace_id: str) -> list[dict[str, Any]]:
    if not workspace_id:
        return []
    members = _read_memberships(workspace_id=workspace_id)
    return [m.to_dict() for m in sorted(members, key=lambda m: (m.added_at, m.email))]


def list_memberships_for_user(email: str) -> list[dict[str, Any]]:
    return [m.to_dict() for m in _read_memberships(email=email)]


# ─────────────────────────────────────────────────────────────────────────────
# Authorization
# ─────────────────────────────────────────────────────────────────────────────


def require_workspace_isolation() -> bool:
    """When True, an empty workspace_id is denied (hard multi-tenant isolation).

    Opt-in via ``DATAFLOW_REQUIRE_WORKSPACE=1``. Production no longer defaults ON —
    the web client historically omitted ``X-Workspace-Id``, which caused schedules /
    contracts / usage to 400 and amplified false offline banners under load.
    """
    return getenv_brand("REQUIRE_WORKSPACE", "").lower() in ("1", "true", "yes")


def can_read_workspace(workspace_id: str, email: str) -> bool:
    if not workspace_id:
        return not require_workspace_isolation()
    return get_workspace_role(workspace_id=workspace_id, email=email) in ROLES


def can_write_workspace(workspace_id: str, email: str) -> bool:
    if not workspace_id:
        return not require_workspace_isolation()
    return get_workspace_role(workspace_id=workspace_id, email=email) in _WRITE_ROLES


def can_admin_workspace(workspace_id: str, email: str) -> bool:
    return get_workspace_role(workspace_id=workspace_id, email=email) in _ADMIN_ROLES
