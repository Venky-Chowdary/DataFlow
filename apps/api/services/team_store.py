"""Workspaces and who belongs to them.

Workspaces are the tenant isolation boundary: connectors, jobs, schedules and API
keys are scoped to one. Each workspace has members with a role — ``admin``,
``editor`` or ``viewer``. The global workspace (id ``""``) carries resources that
were created before scoping existed; once isolation is enforced (see
``require_workspace_isolation``) only platform admins see it.

Persistence is one document per workspace and one per membership, in the metadata
database, related by ``workspace_id``. It used to be a single document holding two
arrays, which meant every add re-wrote the whole team (two concurrent invites lost
one) and nothing stopped the same email appearing twice in one workspace. The
membership ``_id`` is now ``"<workspace_id>:<email>"``, so a duplicate invite is a
replace rather than a second row, and the write is atomic.
"""

from __future__ import annotations

import hashlib
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pymongo import ReturnDocument
from pymongo.database import Database
from pymongo.errors import (
    ConfigurationError,
    DuplicateKeyError,
    OperationFailure,
    PyMongoError,
)

from services.brand_env import getenv_brand
from services.metadata_backend import json_doc_transaction, load_json_doc, mongo_database
from services.platform_config import data_dir
from services.signup_config import signup_enabled

WORKSPACES_COLLECTION = "workspaces"
MEMBERS_COLLECTION = "workspace_members"

STORE_PATH = data_dir() / "teams.json"

# Canonical workspace roles. ``owner`` is the pre-rename name for ``admin`` and is
# still accepted on read so deployments written by an older build keep working.
ROLES = ("admin", "editor", "viewer")
_LEGACY_ROLE_ALIASES = {"owner": "admin"}
_ADMIN_ROLES = ("admin",)
_WRITE_ROLES = ("admin", "editor")
_membership_lock_registry: dict[str, threading.RLock] = {}
_membership_lock_registry_guard = threading.Lock()
_membership_lock_state = threading.local()


class TeamStoreError(Exception):
    """A membership change the store must refuse, carrying the reason.

    Returning a bare ``False``/``None`` made every refusal look identical, so the
    API answered 403 whether the workspace did not exist, the actor lacked the
    role, or the change would leave the workspace with no administrator. Each
    reason is its own type so the operator is told which one happened.
    """

    reason = "refused"


class TeamStoreBusy(TeamStoreError):
    """The workspace membership lock could not be acquired promptly."""

    reason = "busy"


class WorkspaceNotFound(TeamStoreError):
    reason = "workspace_not_found"


class PermissionDenied(TeamStoreError):
    reason = "permission"


class MemberNotFound(TeamStoreError):
    reason = "member_not_found"


class LastAdminProtected(TeamStoreError):
    reason = "last_admin"


class MemberAlreadyExists(TeamStoreError):
    """An *add* was asked to overwrite a membership that already exists.

    Adding and re-roling are different acts: an invitation form that quietly
    re-roles reports "member added" for someone who was already here, and can
    silently demote an admin to viewer on a typo. The role change has its own
    route, so the add path refuses and names the role held today.
    """

    reason = "already_member"


@contextmanager
def workspace_membership_lock(workspace_id: str):
    """Serialize membership changes for one workspace."""
    workspace_id = str(workspace_id or "")
    with _membership_lock_registry_guard:
        lock = _membership_lock_registry.setdefault(workspace_id, threading.RLock())
    lock.acquire()

    depths = getattr(_membership_lock_state, "depths", None)
    if depths is None:
        depths = {}
        _membership_lock_state.depths = depths
    depth = depths.get(workspace_id, 0)
    if depth:
        depths[workspace_id] = depth + 1
        try:
            yield
        finally:
            depths[workspace_id] = depth
            lock.release()
        return

    depths[workspace_id] = 1
    db: Database | None = None
    token: str | None = None
    lock_file = None
    file_locked = False
    try:
        db = _database()
        if db is None:
            try:
                import fcntl
            except ImportError:  # pragma: no cover - non-POSIX platform
                fcntl = None
            lock_path = (
                data_dir()
                / "team_locks"
                / f"{hashlib.sha256(workspace_id.encode('utf-8')).hexdigest()}.lock"
            )
            lock_path.parent.mkdir(parents=True, exist_ok=True)
            lock_file = lock_path.open("a+", encoding="utf-8")
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                file_locked = True
        else:
            token = uuid.uuid4().hex
            collection = db["workspace_locks"]
            deadline = time.monotonic() + 5.0
            while True:
                now = datetime.now(timezone.utc)
                try:
                    lease = collection.find_one_and_update(
                        {
                            "_id": workspace_id,
                            "$or": [
                                {"expires_at": {"$lt": now}},
                                {"_id": {"$exists": False}},
                            ],
                        },
                        {
                            "$set": {
                                "holder": token,
                                "expires_at": now + timedelta(seconds=15),
                            }
                        },
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                    )
                except DuplicateKeyError:
                    lease = None
                except PyMongoError as exc:
                    raise TeamStoreBusy("Workspace membership lock is unavailable") from exc
                if lease and lease.get("holder") == token:
                    break
                if time.monotonic() >= deadline:
                    raise TeamStoreBusy("Workspace membership lock is busy")
                time.sleep(0.05)
        yield
    finally:
        try:
            if db is not None and token is not None:
                db["workspace_locks"].delete_one(
                    {"_id": workspace_id, "holder": token}
                )
            elif lock_file is not None:
                if file_locked:
                    import fcntl

                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
        finally:
            depths.pop(workspace_id, None)
            if not depths:
                del _membership_lock_state.depths
            lock.release()


def _assert_not_last_admin(workspace_id: str, email: str) -> None:
    """Refuse a change that would leave no administrator who can sign in.

    Only admins who can actually log in count (``services.sign_in_capability``):
    an invitation creates a membership, not an account, and counting an invited
    admin with no login let the owner's demotion through on production. Callers
    hold ``workspace_membership_lock`` so two concurrent demotions cannot each
    see the other as the spare admin.
    """
    from services.sign_in_capability import can_sign_in

    target = normalize_email(email)
    others = [
        m
        for m in _read_memberships(workspace_id=workspace_id)
        if m.role == "admin" and m.email != target
    ]
    if not any(can_sign_in(m.email) for m in others):
        raise LastAdminProtected(
            "This is the last admin who can sign in — grant the admin role to "
            "someone with an account first"
        )


class _Authority:
    """What the acting principal may do in the workspace being changed."""

    def __init__(self, role: str, permissions: frozenset[str] | set[str], platform_admin: bool):
        self.role = role
        self.permissions = frozenset(permissions)
        self.platform_admin = platform_admin

    def has(self, permission: str) -> bool:
        return permission in self.permissions


_RANK = {"viewer": 0, "operator": 1, "editor": 2, "admin": 3}


def _authority(
    workspace_id: str, actor_email: str, actor_is_platform_admin: bool, principal: Any
) -> _Authority:
    """The acting principal's authority, from ``services.effective_principal``.

    Routes pass the principal resolved from the request (an API key's own role,
    capped at its creator's; a service account's scopes). Store-level callers
    that pass only an email (SCIM, signup, scripts) are a session principal for
    that email, which is what the email lookup always meant.
    """
    from services.rbac import role_permissions

    if principal is not None:
        return _Authority(principal.member_role, principal.permissions, principal.platform_admin)
    if actor_is_platform_admin:
        return _Authority("admin", role_permissions("admin"), True)
    role = get_workspace_role(workspace_id=workspace_id, email=actor_email)
    return _Authority(role, role_permissions(role) if role else set(), False)


def _authorize_change(
    authority: _Authority, *, current: str, requested: str | None
) -> None:
    """The membership rules every route shares.

    - Bringing someone in needs ``member.invite`` and never grants a role above
      the actor's own; granting admin needs workspace administration.
    - Changing an existing member's role is role management: admin only.
    - Removing a member needs ``member.invite``; removing an admin needs admin.
    """
    from services.rbac import Permission

    manage = authority.has(Permission.WORKSPACE_MANAGE)
    if requested is not None and current and requested != current and not manage:
        raise PermissionDenied("Changing a member's role requires the workspace admin role")
    if current == "admin" and not manage:
        raise PermissionDenied("Only a workspace admin can change or remove another admin")
    if not authority.has(Permission.MEMBER_INVITE) and not manage:
        raise PermissionDenied("Adding or removing members requires the workspace admin or editor role")
    if requested is not None:
        if _RANK.get(requested, 0) > _RANK.get(authority.role, -1):
            raise PermissionDenied(
                f"Cannot grant the {requested} role: it is above your own ({authority.role or 'none'})"
            )
        if requested == "admin" and not manage:
            raise PermissionDenied("Only a workspace admin can grant the admin role")


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
    if env:
        return Path(env)
    # Resolve data_dir() per call — the module-level STORE_PATH snapshot is
    # bound at import time, before test isolation (DATAWRAP_DATA_DIR) or a
    # reconfigured volume can take effect.
    return data_dir() / "teams.json"


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
    org_id: str = ""  # owning organization (signup-created orgs; "" = pre-org era)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Workspace":
        return cls(
            id=data["id"],
            name=data.get("name", ""),
            created_at=data.get("created_at", _now()),
            created_by=data.get("created_by", ""),
            org_id=data.get("org_id", ""),
        )


@dataclass
class Membership:
    workspace_id: str
    email: str
    role: str = "viewer"
    added_at: str = field(default_factory=_now)
    added_by: str = ""
    #: Display name typed on invite. Shown until the member's own account has
    #: one — an email invite creates no account, so this is the only place the
    #: name the admin typed can live (R7).
    name: str = ""

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
            name=clean_display_name(data.get("name")),
        )


def clean_display_name(value: Any) -> str:
    """Trimmed display name; ``None``/blank/whitespace is no name."""
    return " ".join(str(value or "").split())[:128]


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


def rekey_memberships(old_email: str, new_email: str) -> int:
    """Move every membership row from one account email to another
    (email-change flow). Memberships are keyed ``ws:email`` so each row is
    deleted and rewritten under the new key."""
    moved = 0
    for m in _read_memberships(email=old_email):
        _delete_membership(m.workspace_id, m.email)
        m.email = normalize_email(new_email)
        _write_membership(m)
        moved += 1
    return moved


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


def create_workspace(*, name: str, created_by: str, org_id: str = "") -> Workspace:
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
        org_id=org_id,
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
    name: str = "",
    principal: Any = None,
) -> Membership:
    with workspace_membership_lock(workspace_id):
        return _add_workspace_member(
            workspace_id=workspace_id,
            email=email,
            role=role,
            added_by=added_by,
            actor_is_platform_admin=actor_is_platform_admin,
            refuse_existing=refuse_existing,
            name=name,
            principal=principal,
        )


def _add_workspace_member(
    *,
    workspace_id: str,
    email: str,
    role: str,
    added_by: str,
    actor_is_platform_admin: bool = False,
    refuse_existing: bool = False,
    name: str = "",
    principal: Any = None,
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
    authority = _authority(workspace_id, added_by, actor_is_platform_admin, principal)
    existing = next(
        (m for m in _read_memberships(workspace_id=workspace_id, email=email)),
        None,
    )
    _authorize_change(authority, current=existing.role if existing else "", requested=role)
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
        name=clean_display_name(name) or (existing.name if existing else ""),
    )
    _write_membership(membership)
    return membership


def remove_workspace_member(
    *,
    workspace_id: str,
    email: str,
    removed_by: str,
    actor_is_platform_admin: bool = False,
    principal: Any = None,
) -> None:
    with workspace_membership_lock(workspace_id):
        _remove_workspace_member(
            workspace_id=workspace_id,
            email=email,
            removed_by=removed_by,
            actor_is_platform_admin=actor_is_platform_admin,
            principal=principal,
        )


def _remove_workspace_member(
    *,
    workspace_id: str,
    email: str,
    removed_by: str,
    actor_is_platform_admin: bool = False,
    principal: Any = None,
) -> None:
    """Remove a member, refusing to leave a workspace with no administrator."""
    if not workspace_id or not get_workspace(workspace_id):
        raise WorkspaceNotFound("Workspace not found")
    authority = _authority(workspace_id, removed_by, actor_is_platform_admin, principal)
    target_role = get_workspace_role(workspace_id=workspace_id, email=email)
    if not target_role:
        # Refuse an unauthorized caller before telling it who is a member.
        _authorize_change(authority, current="", requested=None)
        raise MemberNotFound(f"{normalize_email(email)} is not a member of this workspace")
    _authorize_change(authority, current=target_role, requested=None)
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
    """When True, unscoped (``workspace_id ""``) resources are platform-admin only.

    On with ``DATAWRAP_REQUIRE_WORKSPACE=1``, and always on while self-serve
    signup is enabled: strangers share the deployment then, and an unscoped row
    visible to every account was a cross-tenant leak (D154). A request without
    ``X-Workspace-Id`` no longer 400s under isolation — ``services.tenant_scope``
    defaults it to the actor's own workspace.
    """
    if getenv_brand("REQUIRE_WORKSPACE", "").lower() in ("1", "true", "yes"):
        return True
    return signup_enabled()


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
