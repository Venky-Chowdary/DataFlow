"""SCIM 2.0 provisioning behavior backed by the platform account stores."""

from __future__ import annotations

import logging
import re
import uuid
from contextlib import ExitStack
from datetime import datetime, timezone
from typing import Any

from services import auth_sessions, integrations_store, scim_store, team_store, user_store

_ERROR_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:Error"
_LIST_SCHEMA = "urn:ietf:params:scim:api:messages:2.0:ListResponse"
_ROLE_ORDER = {"viewer": 1, "editor": 2, "admin": 3}
_FILTER_ATTRIBUTES = {
    "user": {"username", "externalid", "emails.value", "active"},
    "group": {"displayname", "externalid"},
}
_CONDITION = re.compile(
    r'\s*([A-Za-z][A-Za-z0-9_.]*)\s+(eq|sw|co)\s+(?:"([^"\\]*)"|(true|false))\s*',
    re.IGNORECASE,
)
_LOGGER = logging.getLogger(__name__)


class ScimError(Exception):
    def __init__(self, status: int, scimType: str | None, detail: str):
        super().__init__(detail)
        self.status = status
        self.scimType = scimType
        self.detail = detail

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "schemas": [_ERROR_SCHEMA],
            "status": str(self.status),
            "detail": self.detail,
        }
        if self.scimType:
            body["scimType"] = self.scimType
        return body


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fail(status: int, detail: str, scim_type: str | None = None) -> None:
    raise ScimError(status, scim_type, detail)


def _store_call(func, *args, **kwargs):
    try:
        return func(*args, **kwargs)
    except scim_store.ScimStoreUnavailable as exc:
        if "already exists" in str(exc):
            _fail(409, "A resource with this unique value already exists", "uniqueness")
        _fail(500, "SCIM storage is unavailable")


def _audit(
    action: str,
    *,
    actor: str,
    resource: str,
    details: dict[str, Any],
    outcome: str = "succeeded",
    reason_code: str | None = None,
) -> None:
    from services.audit_coverage import record_security_event

    record_security_event(
        action,
        outcome=outcome,
        resource=resource,
        actor=actor or "scim",
        reason_code=reason_code,
        details=details,
    )


def _user_attributes(payload: dict[str, Any]) -> list[str]:
    allowed = {"userName", "externalId", "displayName", "active"}
    return sorted(attribute for attribute in allowed if attribute in payload)


def _changed_user_attributes(
    current: dict[str, Any], updated: dict[str, Any]
) -> list[str]:
    fields = {
        "userName": "userName",
        "externalId": "externalId",
        "displayName": "displayName",
        "active": "active",
    }
    return sorted(
        attribute
        for attribute, field in fields.items()
        if current.get(field, True if field == "active" else None)
        != updated.get(field, True if field == "active" else None)
    )


def _user_resource(user_id: str | None) -> str:
    return f"scim_user:{user_id}" if user_id else "scim_user"


def _group_changed_attributes(
    current: dict[str, Any], updated: dict[str, Any]
) -> list[str]:
    return sorted(
        attribute
        for attribute, field in (
            ("displayName", "displayName"),
            ("externalId", "externalId"),
            ("members", "members"),
        )
        if current.get(field) != updated.get(field)
    )


def _normalize_email(value: Any) -> str:
    try:
        return user_store.validate_email(str(value or ""))
    except ValueError as exc:
        _fail(400, str(exc), "invalidValue")


def _bool_value(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    _fail(400, "active must be a boolean", "invalidValue")


def _resource_user(user: dict[str, Any]) -> dict[str, Any]:
    result = dict(user)
    result["schemas"] = ["urn:ietf:params:scim:schemas:core:2.0:User"]
    result["emails"] = [{"value": user["userName"], "primary": True, "type": "work"}]
    return result


def _resource_group(group: dict[str, Any]) -> dict[str, Any]:
    result = dict(group)
    result["schemas"] = ["urn:ietf:params:scim:schemas:core:2.0:Group"]
    members = []
    for user_id in group.get("members", []):
        user = _store_call(scim_store.get_user, user_id)
        member = {"value": user_id}
        if user:
            member["display"] = user.get("displayName") or user["userName"]
        members.append(member)
    result["members"] = members
    return result


def _parse_filter(raw: str | None, resource_type: str) -> list[tuple[str, str, Any]]:
    if not raw:
        return []
    allowed = _FILTER_ATTRIBUTES[resource_type]
    remaining = raw.strip()
    conditions: list[tuple[str, str, Any]] = []
    while remaining:
        match = _CONDITION.match(remaining)
        if not match:
            _fail(400, "The filter is malformed or unsupported", "invalidFilter")
        attribute, operator, quoted, boolean = match.groups()
        normalized_attribute = attribute.lower()
        if normalized_attribute not in allowed:
            _fail(400, "The filter attribute is unsupported", "invalidFilter")
        if boolean is not None and normalized_attribute != "active":
            _fail(400, "Boolean filter values are only supported for active", "invalidFilter")
        value: Any = quoted if quoted is not None else boolean.lower() == "true"
        conditions.append((normalized_attribute, operator.lower(), value))
        remaining = remaining[match.end() :].lstrip()
        if not remaining:
            break
        and_match = re.match(r"(?i)^and\b", remaining)
        if not and_match:
            _fail(400, "Only and filter conjunctions are supported", "invalidFilter")
        remaining = remaining[and_match.end() :].lstrip()
        if not remaining:
            _fail(400, "The filter is malformed", "invalidFilter")
    return conditions


def _matches(resource: dict[str, Any], conditions, resource_type: str) -> bool:
    for attribute, operator, expected in conditions:
        if attribute == "username":
            actual = resource.get("userName", "")
            insensitive = True
        elif attribute == "displayname":
            actual = resource.get("displayName", "")
            insensitive = True
        elif attribute == "externalid":
            actual = resource.get("externalId", "")
            insensitive = False
        elif attribute == "active":
            actual = bool(resource.get("active", True))
            insensitive = False
        elif attribute == "emails.value":
            actual = resource.get("userName", "")
            insensitive = True
        else:
            return False
        if isinstance(actual, str) and isinstance(expected, str) and insensitive:
            actual, expected = actual.lower(), expected.lower()
        if operator == "eq":
            matched = actual == expected
        elif operator == "sw":
            matched = isinstance(actual, str) and isinstance(expected, str) and actual.startswith(expected)
        else:
            matched = isinstance(actual, str) and isinstance(expected, str) and expected in actual
        if not matched:
            return False
    return True


def _list_response(resources: list[dict[str, Any]], start_index: int, count: int) -> dict[str, Any]:
    if start_index < 1:
        _fail(400, "startIndex must be at least 1", "invalidValue")
    if count < 0:
        _fail(400, "count must not be negative", "invalidValue")
    start = start_index
    size = min(200, max(0, count))
    page = resources[start - 1 : start - 1 + size]
    return {
        "schemas": [_LIST_SCHEMA],
        "totalResults": len(resources),
        "startIndex": start,
        "itemsPerPage": len(page),
        "Resources": page,
    }


def list_users(*, filter: str | None, start_index: int, count: int) -> dict[str, Any]:
    conditions = _parse_filter(filter, "user")
    resources = [
        _resource_user(user)
        for user in _store_call(scim_store.list_users)
        if _matches(user, conditions, "user")
    ]
    return _list_response(resources, start_index, count)


def get_user(user_id: str) -> dict[str, Any]:
    user = _store_call(scim_store.get_user, user_id)
    if user is None:
        _fail(404, "User not found")
    return _resource_user(user)


def _make_user(
    *,
    user_id: str,
    user_name: str,
    external_id: Any,
    display_name: Any,
    active: bool,
    created: str,
    last_modified: str,
    version: str,
) -> dict[str, Any]:
    return {
        "id": user_id,
        "userName": user_name,
        "externalId": external_id,
        "displayName": str(display_name or user_name),
        "active": active,
        "meta": {
            "resourceType": "User",
            "created": created,
            "lastModified": last_modified,
            "version": version,
        },
    }


def create_user(payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
    user_name = _normalize_email(payload.get("userName"))
    if _store_call(scim_store.get_user_by_username, user_name):
        _fail(409, "userName is already provisioned", "uniqueness")
    now = _now()
    account = user_store.get_user(user_name)
    created_account = account is None
    if created_account:
        try:
            account, _one_time_password = user_store.create_user(
                email=user_name,
                name=str(payload.get("displayName") or user_name),
                role="member",
                created_by=actor,
            )
        except ValueError as exc:
            _fail(400, str(exc), "invalidValue")
    active = _bool_value(payload.get("active", True))
    user = _make_user(
        user_id=str(uuid.uuid4()),
        user_name=user_name,
        external_id=payload.get("externalId"),
        display_name=payload.get("displayName") or (account or {}).get("name") or user_name,
        active=active if active else True,
        created=now,
        last_modified=now,
        version="1",
    )
    stored_user = user
    try:
        _store_call(scim_store.create_user, user)
    except ScimError:
        if created_account:
            user_store.delete_user(email=user_name)
        raise
    if active:
        if account and account.get("status") == "disabled":
            reactivate_user(user_name, actor=actor, record_success=False)
    else:
        deprovision_user(user_name, actor=actor, record_success=False)
        stored_user = {**user, "active": False}
        _store_call(scim_store.update_user, stored_user)
    _audit(
        "scim.user.create",
        actor=actor,
        resource=_user_resource(user["id"]),
        details={
            "user_id": user["id"],
            "changed_attributes": _user_attributes(payload),
        },
    )
    return _resource_user(stored_user)


def replace_user(user_id: str, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
    return _update_user(user_id, payload, actor=actor, audit_action="scim.user.replace")


def _update_user(
    user_id: str,
    payload: dict[str, Any],
    *,
    actor: str,
    audit_action: str,
) -> dict[str, Any]:
    current = _store_call(scim_store.get_user, user_id)
    if current is None:
        _fail(404, "User not found")
    user_name = _normalize_email(payload.get("userName", current["userName"]))
    if user_name != current["userName"]:
        _fail(400, "userName cannot be changed because accounts are keyed by email", "mutability")
    active = _bool_value(payload.get("active", True))
    now = _now()
    updated = _make_user(
        user_id=user_id,
        user_name=user_name,
        external_id=payload.get("externalId"),
        display_name=payload.get("displayName") or user_name,
        active=active,
        created=current["meta"]["created"],
        last_modified=now,
        version=str(int(current["meta"].get("version", "1")) + 1),
    )
    account = user_store.get_user(user_name)
    deprovisioned = False
    reactivated = False
    if active and (
        current.get("active") is False
        or (account and account.get("status") == "disabled")
    ):
        reactivate_user(user_name, actor=actor)
        reactivated = True
    elif not active and (
        current.get("active", True) is not False
        or account is None
        or account.get("status") != "disabled"
    ):
        deprovision_user(user_name, actor=actor)
        deprovisioned = True
    _store_call(scim_store.update_user, updated)
    changed_attributes = _changed_user_attributes(current, updated)
    if not (
        (deprovisioned or reactivated)
        and set(changed_attributes) <= {"active"}
    ):
        _audit(
            audit_action,
            actor=actor,
            resource=_user_resource(user_id),
            details={
                "user_id": user_id,
                "changed_attributes": changed_attributes,
            },
        )
    return _resource_user(updated)


def patch_user(user_id: str, payload: dict[str, Any], *, actor: str) -> dict[str, Any]:
    current = _store_call(scim_store.get_user, user_id)
    if current is None:
        _fail(404, "User not found")
    changes: dict[str, Any] = {}
    operations = payload.get("Operations", payload.get("operations"))
    if operations is None and isinstance(payload.get("value"), dict):
        operations = [{"op": "replace", "value": payload["value"]}]
    if not isinstance(operations, list):
        _fail(400, "PATCH must contain an Operations array", "invalidSyntax")
    for operation in operations:
        if not isinstance(operation, dict):
            _fail(400, "PATCH operation must be an object", "invalidSyntax")
        op = str(operation.get("op") or "").lower()
        if op not in {"add", "replace", "remove"}:
            _fail(400, "Unsupported PATCH operation", "invalidSyntax")
        path = str(operation.get("path") or "").strip().lower()
        value = operation.get("value")
        if path in {"username"}:
            _fail(400, "userName cannot be changed because accounts are keyed by email", "mutability")
        if not path and isinstance(value, dict):
            for key, field_value in value.items():
                normalized = str(key).lower()
                if normalized == "username" and field_value != current["userName"]:
                    _fail(400, "userName cannot be changed because accounts are keyed by email", "mutability")
                if normalized in {"active", "displayname", "externalid"}:
                    changes[normalized] = field_value
        elif path in {"active", "displayname", "externalid"}:
            if op == "remove":
                changes[path] = False if path == "active" else None
            else:
                changes[path] = value
        else:
            _fail(400, "Unsupported PATCH path", "invalidPath")
    replacement = {
        "userName": current["userName"],
        "externalId": changes.get("externalid", current.get("externalId")),
        "displayName": changes.get("displayname", current.get("displayName")),
        "active": _bool_value(changes.get("active", current.get("active", True))),
    }
    return _update_user(
        user_id,
        replacement,
        actor=actor,
        audit_action="scim.user.patch",
    )


def _managed_membership_cleanup(email: str, *, actor: str) -> None:
    managed = _store_call(scim_store.get_managed_memberships)
    for (workspace_id, member_email), _role in list(managed.items()):
        if member_email != email:
            continue
        try:
            team_store.remove_workspace_member(
                workspace_id=workspace_id,
                email=email,
                removed_by=actor or "scim",
                actor_is_platform_admin=True,
            )
        except team_store.MemberNotFound:
            pass
        managed.pop((workspace_id, member_email), None)
    _store_call(scim_store.set_managed_memberships, managed)
    adopted = _store_call(scim_store.get_adopted_memberships)
    if any(member_email == email for _ws, member_email in adopted):
        _store_call(
            scim_store.set_adopted_memberships,
            {key: role for key, role in adopted.items() if key[1] != email},
        )


def deprovision_user(
    email: str,
    *,
    actor: str,
    record_success: bool = True,
) -> None:
    normalized = email.strip().lower()
    mapping = _store_call(scim_store.get_user_by_username, normalized)
    if mapping and mapping.get("active") is False:
        account = user_store.get_user(normalized)
        if account and account.get("status") == "disabled":
            return
    step = "disable_account"
    sessions_revoked = 0
    revoked_ids: list[str] = []
    kept_service_accounts = 0
    try:
        user_store.update_user(email=normalized, status="disabled")
        step = "revoke_sessions"
        sessions_revoked = auth_sessions.revoke_all_for_email(normalized)
        step = "revoke_api_keys"
        for key in integrations_store.load_api_key_records():
            kind = key.get("kind") or "api_key"
            owner = str(key.get("created_by") or "").strip().lower()
            if kind in {"service_account", "scim"} and owner == normalized and not key.get("revoked_at"):
                kept_service_accounts += 1
            if (
                kind == "api_key"
                and owner == normalized
                and not key.get("revoked_at")
            ):
                if integrations_store.revoke_api_key(str(key["id"])):
                    revoked_ids.append(str(key["id"]))
        if record_success:
            step = "audit_success"
            user_id = mapping.get("id") if mapping else None
            _audit(
                "scim.user.deprovision",
                actor=actor,
                resource=_user_resource(user_id),
                details={
                    "user_id": user_id,
                    "sessions_revoked": sessions_revoked,
                    "revoked_api_key_ids": revoked_ids,
                    "kept_service_accounts": kept_service_accounts,
                },
            )
    except Exception as exc:
        try:
            user_id = mapping.get("id") if mapping else None
            _audit(
                "scim.user.deprovision.failed",
                outcome="failed",
                reason_code=step,
                actor=actor,
                resource=_user_resource(user_id),
                details={"user_id": user_id},
            )
        except Exception as audit_exc:
            _LOGGER.warning(
                "SCIM deprovision failure audit failed (%s)",
                type(audit_exc).__name__,
            )
        raise ScimError(500, None, "User deprovisioning failed") from exc


def reactivate_user(
    email: str,
    *,
    actor: str,
    record_success: bool = True,
) -> None:
    normalized = email.strip().lower()
    try:
        mapping = _store_call(scim_store.get_user_by_username, normalized)
        user_store.update_user(email=normalized, status="active")
        if record_success:
            user_id = mapping.get("id") if mapping else None
            _audit(
                "scim.user.reactivate",
                actor=actor,
                resource=_user_resource(user_id),
                details={"user_id": user_id},
            )
    except Exception as exc:
        raise ScimError(500, None, "User reactivation failed") from exc


def delete_user(user_id: str, *, actor: str) -> None:
    user = _store_call(scim_store.get_user, user_id)
    if user is None:
        _fail(404, "User not found")
    deprovision_user(user["userName"], actor=actor, record_success=False)
    _managed_membership_cleanup(user["userName"], actor=actor)
    for group in _store_call(scim_store.list_groups):
        if user_id in group.get("members", []):
            group["members"] = [member for member in group["members"] if member != user_id]
            _store_call(scim_store.update_group, group)
    user_store.delete_user(email=user["userName"])
    _store_call(scim_store.delete_user, user_id)
    _audit(
        "scim.user.delete",
        actor=actor,
        resource=_user_resource(user_id),
        details={"user_id": user_id},
    )


def list_groups(*, filter: str | None, start_index: int, count: int) -> dict[str, Any]:
    conditions = _parse_filter(filter, "group")
    resources = [
        _resource_group(group)
        for group in _store_call(scim_store.list_groups)
        if _matches(group, conditions, "group")
    ]
    return _list_response(resources, start_index, count)


def get_group(group_id: str) -> dict[str, Any]:
    group = _store_call(scim_store.get_group, group_id)
    if group is None:
        _fail(404, "Group not found")
    return _resource_group(group)


def create_group(
    payload: dict[str, Any], *, actor: str, principal: dict[str, Any] | None = None
) -> dict[str, Any]:
    name = str(payload.get("displayName") or "").strip()
    if not name:
        _fail(400, "displayName is required", "invalidValue")
    if _store_call(scim_store.get_group_by_name, name):
        _fail(409, "displayName is already provisioned", "uniqueness")
    group = {
        "id": str(uuid.uuid4()),
        "displayName": name,
        "externalId": payload.get("externalId"),
        "members": [],
    }
    incoming = payload.get("members", [])
    ids = [
        str(member.get("value") if isinstance(member, dict) else member)
        for member in incoming
    ]
    for user_id in ids:
        if _store_call(scim_store.get_user, user_id) is None:
            _fail(400, f"Unknown SCIM user id: {user_id}", "invalidValue")
    _store_call(scim_store.create_group, group)
    if incoming:
        try:
            group = _apply_group_members(group, ids, actor=actor, principal=principal)
        except ScimError:
            _store_call(scim_store.delete_group, group["id"])
            raise
    _audit(
        "scim.group.create",
        actor=actor,
        resource=f"scim_group:{group['id']}",
        details={
            "group_id": group["id"],
            "displayName": name,
            "changed_attributes": sorted(
                attribute
                for attribute in ("displayName", "externalId", "members")
                if attribute in payload
            ),
        },
    )
    return _resource_group(group)


_KEY_ROLE_TO_MEMBERSHIP = {"admin": "admin", "editor": "editor", "operator": "editor", "viewer": "viewer"}


def role_ceiling(principal: dict[str, Any] | None, workspace_id: str) -> str | None:
    """Highest workspace role a SCIM caller may grant in ``workspace_id``.

    A mapped group applies a role on the caller's authority, so the escalation
    rule for minting credentials holds here too: never more than the token's
    own grant. A key is its own principal (``services.effective_role``): its
    stored role is the ceiling, and a key bound to one workspace has no
    authority in another (``None``: that workspace is left untouched). A
    session caller is bounded by its own role in the workspace. ``principal``
    ``None`` is an internal call with no request: uncapped.
    """
    if not principal:
        return "admin"
    from services.rbac import normalize_role

    own_role = normalize_role(principal.get("role"))
    ceiling = _KEY_ROLE_TO_MEMBERSHIP.get(own_role, "viewer")
    if str(principal.get("auth_kind") or "") == "api_key":
        key_id = str(principal.get("api_key_id") or "")
        record = next(
            (row for row in integrations_store.list_api_keys() if row.get("id") == key_id),
            None,
        )
        bound = str((record or {}).get("workspace_id") or "")
        if bound and bound != workspace_id:
            return None
        return ceiling
    if own_role == "admin":
        return ceiling
    member_role = team_store.get_workspace_role(
        workspace_id=workspace_id, email=str(principal.get("email") or "")
    )
    return member_role or None


def _min_role(left: str, right: str) -> str:
    return left if _ROLE_ORDER.get(left, 0) <= _ROLE_ORDER.get(right, 0) else right


def _group_proposals(changed_group: dict[str, Any], proposed_members: list[str] | None):
    groups = _store_call(scim_store.list_groups)
    mappings = _store_call(scim_store.get_group_mappings)
    user_ids = set(changed_group.get("members", [])) | set(proposed_members or [])
    roles_by_user_workspace: dict[tuple[str, str], str] = {}
    for group in groups:
        effective_group = changed_group if group["id"] == changed_group["id"] else group
        members = proposed_members if group["id"] == changed_group["id"] else group.get("members", [])
        mapping = mappings.get(str(effective_group.get("displayName", "")).lower())
        if not mapping:
            continue
        for user_id in members:
            if user_id not in user_ids:
                continue
            user = _store_call(scim_store.get_user, user_id)
            if user is None:
                continue
            key = (user_id, mapping["workspace_id"])
            role = mapping["role"]
            current = roles_by_user_workspace.get(key)
            if current is None or _ROLE_ORDER[role] > _ROLE_ORDER[current]:
                roles_by_user_workspace[key] = role
    return user_ids, roles_by_user_workspace


def _apply_group_members(
    group: dict[str, Any],
    proposed_members: list[str],
    *,
    actor: str,
    principal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    proposed = sorted(set(proposed_members))
    affected_ids, desired = _group_proposals(group, proposed)
    managed = _store_call(scim_store.get_managed_memberships)
    affected_emails = {
        user["userName"]
        for user_id in affected_ids
        if (user := _store_call(scim_store.get_user, user_id)) is not None
    }
    workspace_ids = {
        workspace_id
        for (user_id, workspace_id) in desired
        if user_id in affected_ids
    }
    workspace_ids.update(
        workspace_id
        for workspace_id, email in managed
        if email in affected_emails
    )
    try:
        with ExitStack() as locks:
            for workspace_id in sorted(workspace_ids):
                locks.enter_context(team_store.workspace_membership_lock(workspace_id))
            return _apply_group_members_locked(
                group, proposed, actor=actor, principal=principal
            )
    except team_store.TeamStoreBusy as exc:
        _fail(503, str(exc), "serverError")


def _apply_group_members_locked(
    group: dict[str, Any],
    proposed_members: list[str],
    *,
    actor: str,
    principal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Apply a group's new member list to the workspace roles its mapping names.

    A mapped group *sets* the member's role. A member who already belonged to
    the workspace before any mapped group (a manual membership) is adopted: the
    mapped role applies while they are in a mapped group, and their earlier
    role is restored — not deleted — when they leave the last one. Every role
    is capped by :func:`role_ceiling`, and no plan may leave a workspace
    without an administrator.
    """
    proposed = sorted(set(proposed_members))
    previous = set(group.get("members", []))
    affected_ids, desired = _group_proposals(group, proposed)
    managed = _store_call(scim_store.get_managed_memberships)
    adopted = _store_call(scim_store.get_adopted_memberships)
    affected_emails: dict[str, str] = {}
    for user_id in affected_ids:
        user = _store_call(scim_store.get_user, user_id)
        if user:
            affected_emails[user_id] = user["userName"]

    ceilings: dict[str, str | None] = {}

    def ceiling_for(workspace_id: str) -> str | None:
        if workspace_id not in ceilings:
            ceilings[workspace_id] = role_ceiling(principal, workspace_id)
        return ceilings[workspace_id]

    capped: list[dict[str, Any]] = []
    # (user_id, workspace_id, email, old_managed, current_role, new_role, adopt)
    plans: list[tuple[Any, str, str, Any, str, Any, bool]] = []
    for (user_id, workspace_id), wanted in desired.items():
        email = affected_emails.get(user_id)
        if not email:
            continue
        key = (workspace_id, email)
        old_managed_role = managed.get(key)
        current_role = team_store.get_workspace_role(workspace_id=workspace_id, email=email)
        ceiling = ceiling_for(workspace_id)
        new_role = wanted
        if ceiling is None or (
            current_role and _ROLE_ORDER.get(current_role, 0) > _ROLE_ORDER[ceiling]
        ):
            capped.append({"user_id": user_id, "workspace_id": workspace_id,
                           "mapped_role": wanted, "ceiling": ceiling, "applied": False})
            continue
        if _ROLE_ORDER[new_role] > _ROLE_ORDER[ceiling]:
            capped.append({"user_id": user_id, "workspace_id": workspace_id,
                           "mapped_role": wanted, "ceiling": ceiling, "applied": True})
            new_role = ceiling
        adopt = old_managed_role is None and bool(current_role) and key not in adopted
        plans.append((user_id, workspace_id, email, old_managed_role, current_role, new_role, adopt))
    desired_keys = {
        (workspace_id, affected_emails[user_id])
        for (user_id, workspace_id) in desired
        if user_id in affected_emails
    }
    for (workspace_id, email), old_role in list(managed.items()):
        if email not in affected_emails.values() or (workspace_id, email) in desired_keys:
            continue
        current_role = team_store.get_workspace_role(workspace_id=workspace_id, email=email)
        ceiling = ceiling_for(workspace_id)
        if ceiling is None or (
            current_role and _ROLE_ORDER.get(current_role, 0) > _ROLE_ORDER[ceiling]
        ):
            capped.append({"workspace_id": workspace_id, "mapped_role": None,
                           "ceiling": ceiling, "applied": False})
            continue
        # A membership this group adopted goes back to the role it had before.
        restore = adopted.get((workspace_id, email)) or None
        plans.append((None, workspace_id, email, old_role, current_role, restore, False))

    for workspace_id in {plan[1] for plan in plans}:
        admins = {
            str(row.get("email") or "").strip().lower()
            for row in team_store.list_workspace_members(workspace_id)
            if row.get("role") == "admin"
        }
        workspace_plans = [plan for plan in plans if plan[1] == workspace_id]
        removed_admins = {
            plan[2].strip().lower()
            for plan in workspace_plans
            if plan[4] == "admin" and plan[5] != "admin"
        }
        added_admins = {
            plan[2].strip().lower()
            for plan in workspace_plans
            if plan[4] != "admin" and plan[5] == "admin"
        }
        if len((admins - removed_admins) | added_admins) < 1:
            _fail(409, "SCIM group change would remove the last workspace admin", "mutability")

    try:
        for user_id, workspace_id, email, old_managed, current_role, new_role, adopt in plans:
            key = (workspace_id, email)
            if new_role is None:
                if current_role:
                    team_store.remove_workspace_member(
                        workspace_id=workspace_id,
                        email=email,
                        removed_by=actor or "scim",
                        actor_is_platform_admin=True,
                    )
                managed.pop(key, None)
                adopted.pop(key, None)
                continue
            if user_id is None:
                # Leaving the last mapped group: restore the adopted role.
                if current_role != new_role:
                    team_store.add_workspace_member(
                        workspace_id=workspace_id,
                        email=email,
                        role=new_role,
                        added_by="scim",
                        actor_is_platform_admin=True,
                    )
                    _audit_role_change(group, user_id, workspace_id, current_role, new_role,
                                       actor=actor, reason="restored_prior_role")
                managed.pop(key, None)
                adopted.pop(key, None)
                continue
            if adopt:
                adopted[key] = current_role
            if old_managed is None and not current_role:
                team_store.add_workspace_member(
                    workspace_id=workspace_id,
                    email=email,
                    role=new_role,
                    added_by="scim",
                    actor_is_platform_admin=True,
                )
                managed[key] = new_role
            elif old_managed != new_role or current_role != new_role:
                team_store.add_workspace_member(
                    workspace_id=workspace_id,
                    email=email,
                    role=new_role,
                    added_by="scim",
                    actor_is_platform_admin=True,
                )
                managed[key] = new_role
                role_before = current_role if adopt else (
                    old_managed if old_managed != new_role else current_role
                )
                if role_before and role_before != new_role:
                    _audit_role_change(group, user_id, workspace_id, role_before, new_role,
                                       actor=actor,
                                       reason="adopted_membership" if adopt else "group_mapping")
            else:
                managed[key] = new_role
    except team_store.LastAdminProtected as exc:
        _fail(409, str(exc), "mutability")
    except team_store.TeamStoreBusy as exc:
        _fail(503, str(exc), "serverError")
    except team_store.TeamStoreError as exc:
        _fail(409, str(exc), "invalidValue")

    group["members"] = proposed
    saved = _store_call(scim_store.update_group, group)
    _store_call(scim_store.set_managed_memberships, managed)
    _store_call(scim_store.set_adopted_memberships, adopted)
    for row in capped:
        _audit(
            "scim.group.role_capped",
            actor=actor,
            resource=f"workspace:{row['workspace_id']}",
            outcome="succeeded" if row["applied"] else "skipped",
            reason_code="grant_exceeds_token" if row["ceiling"] else "no_authority_in_workspace",
            details={"group_id": group["id"], **row},
        )
    for member_id in sorted(set(proposed) - previous):
        _audit(
            "scim.group.member_add",
            actor=actor,
            resource=f"scim_group:{group['id']}",
            details={
                "group_id": group["id"],
                "member_id": member_id,
                "direction": "add",
            },
        )
    for member_id in sorted(previous - set(proposed)):
        _audit(
            "scim.group.member_remove",
            actor=actor,
            resource=f"scim_group:{group['id']}",
            details={
                "group_id": group["id"],
                "member_id": member_id,
                "direction": "remove",
            },
        )
    return saved


def _audit_role_change(
    group: dict[str, Any],
    user_id: Any,
    workspace_id: str,
    role_before: str,
    role_after: str,
    *,
    actor: str,
    reason: str,
) -> None:
    _audit(
        "scim.group.role_change",
        actor=actor,
        resource=f"workspace:{workspace_id}",
        details={
            "group_id": group["id"],
            "user_id": user_id,
            "workspace_id": workspace_id,
            "changed_attributes": ["role"],
            "role_before": role_before,
            "role_after": role_after,
            "reason": reason,
        },
    )


def replace_group(
    group_id: str,
    payload: dict[str, Any],
    *,
    actor: str,
    principal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current = _store_call(scim_store.get_group, group_id)
    group = dict(current) if current is not None else None
    if group is None:
        _fail(404, "Group not found")
    name = str(payload.get("displayName") or group["displayName"]).strip()
    duplicate = _store_call(scim_store.get_group_by_name, name)
    if duplicate and duplicate["id"] != group_id:
        _fail(409, "displayName is already provisioned", "uniqueness")
    group["displayName"] = name
    group["externalId"] = payload.get("externalId")
    incoming = payload.get("members", [])
    ids = [
        str(member.get("value") if isinstance(member, dict) else member)
        for member in incoming
    ]
    for user_id in ids:
        if _store_call(scim_store.get_user, user_id) is None:
            _fail(400, f"Unknown SCIM user id: {user_id}", "invalidValue")
    saved = _apply_group_members(group, ids, actor=actor, principal=principal)
    _audit(
        "scim.group.replace",
        actor=actor,
        resource=f"scim_group:{group_id}",
        details={
            "group_id": group_id,
            "changed_attributes": _group_changed_attributes(current, saved),
        },
    )
    return _resource_group(saved)


def patch_group(
    group_id: str,
    payload: dict[str, Any],
    *,
    actor: str,
    principal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    group = _store_call(scim_store.get_group, group_id)
    if group is None:
        _fail(404, "Group not found")
    operations = payload.get("Operations", payload.get("operations"))
    if not isinstance(operations, list):
        _fail(400, "PATCH must contain an Operations array", "invalidSyntax")
    members = list(group.get("members", []))
    changed = False
    for operation in operations:
        if not isinstance(operation, dict):
            _fail(400, "PATCH operation must be an object", "invalidSyntax")
        op = str(operation.get("op") or "").lower()
        if op not in {"add", "replace", "remove"}:
            _fail(400, "Unsupported PATCH operation", "invalidSyntax")
        path = str(operation.get("path") or "").strip()
        selected_id = None
        if path:
            match = re.fullmatch(r'members\[value eq "([^"]+)"\]', path, re.IGNORECASE)
            if path.lower() != "members" and match is None:
                _fail(400, "Unsupported group PATCH path", "invalidPath")
            if match:
                selected_id = match.group(1)
        value = operation.get("value")
        values = value if isinstance(value, list) else [value]
        values = [
            str(item.get("value") if isinstance(item, dict) else item)
            for item in values
            if item is not None
        ]
        if selected_id:
            values = [selected_id]
        if op == "remove":
            members = [member for member in members if member not in values]
        elif op == "replace":
            members = values
        else:
            members.extend(member for member in values if member not in members)
        changed = True
    for user_id in members:
        if _store_call(scim_store.get_user, user_id) is None:
            _fail(400, f"Unknown SCIM user id: {user_id}", "invalidValue")
    if not changed:
        return _resource_group(group)
    old_members = set(group.get("members", []))
    saved = _apply_group_members(group, members, actor=actor, principal=principal)
    new_members = set(saved.get("members", []))
    if new_members == old_members:
        _audit(
            "scim.group.patch",
            actor=actor,
            resource=f"scim_group:{group_id}",
            details={
                "group_id": group_id,
                "changed_attributes": ["members"],
            },
        )
    return _resource_group(saved)


def delete_group(
    group_id: str, *, actor: str, principal: dict[str, Any] | None = None
) -> None:
    group = _store_call(scim_store.get_group, group_id)
    if group is None:
        _fail(404, "Group not found")
    _apply_group_members(group, [], actor=actor, principal=principal)
    _store_call(scim_store.delete_group, group_id)
    _audit(
        "scim.group.delete",
        actor=actor,
        resource=f"scim_group:{group_id}",
        details={"group_id": group_id},
    )


def get_group_mappings() -> dict[str, dict[str, str]]:
    return _store_call(scim_store.get_group_mappings)


def set_group_mappings(mappings: dict[str, dict[str, str]]) -> None:
    _store_call(scim_store.set_group_mappings, mappings)
