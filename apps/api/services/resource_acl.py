"""Resource-level ACL grants beyond coarse RBAC roles.

Grant shape: (tenant_id, resource_type, resource_id, principal, role)

Roles: viewer < editor < owner (same ranking spirit as team_store).
Coarse path RBAC still applies; ACL further restricts instance access when
any grants exist for that resource.
"""

from __future__ import annotations

import json
import logging
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.brand_env import getenv_brand
from services.platform_config import data_dir
from services.value_serializer import json_default

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "resource_acls.jsonl"
RESOURCE_TYPES = frozenset({"connector", "job", "contract", "transform", "schedule"})
ACL_ROLES = ("viewer", "editor", "owner")
_ROLE_RANK = {"viewer": 1, "editor": 2, "owner": 3}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _store_path() -> Path:
    env = (getenv_brand("RESOURCE_ACL_STORE", "") or "").strip()
    return Path(env) if env else STORE_PATH


@dataclass
class ResourceAclGrant:
    id: str
    tenant_id: str
    resource_type: str
    resource_id: str
    principal: str  # email or user id
    role: str
    created_at: str
    created_by: str = "system"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ResourceAclGrant":
        return cls(
            id=str(data.get("id") or uuid.uuid4()),
            tenant_id=str(data.get("tenant_id") or ""),
            resource_type=str(data.get("resource_type") or ""),
            resource_id=str(data.get("resource_id") or ""),
            principal=str(data.get("principal") or "").strip().lower(),
            role=str(data.get("role") or "viewer"),
            created_at=str(data.get("created_at") or _now()),
            created_by=str(data.get("created_by") or "system"),
        )


def _load_all() -> list[ResourceAclGrant]:
    path = _store_path()
    if not path.exists():
        return []
    out: list[ResourceAclGrant] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            out.append(ResourceAclGrant.from_dict(json.loads(line)))
    except Exception as exc:
        # Fail closed: a corrupt/unreadable ACL store must not look like "no grants"
        # (which would open every previously restricted resource).
        logger.error("Failed reading resource ACL store (deny-all until fixed): %s", exc)
        raise RuntimeError(f"Resource ACL store unreadable: {exc}") from exc
    return out


def _rewrite(grants: list[ResourceAclGrant]) -> None:
    path = _store_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        for g in grants:
            fh.write(json.dumps(g.to_dict(), default=json_default) + "\n")
    tmp.replace(path)


def list_grants(
    *,
    tenant_id: str = "",
    resource_type: str | None = None,
    resource_id: str | None = None,
    principal: str | None = None,
) -> list[ResourceAclGrant]:
    grants = _load_all()
    tid = tenant_id or ""
    out = [g for g in grants if g.tenant_id == tid]
    if resource_type:
        out = [g for g in out if g.resource_type == resource_type]
    if resource_id:
        out = [g for g in out if g.resource_id == resource_id]
    if principal:
        p = principal.strip().lower()
        out = [g for g in out if g.principal == p]
    return out


def upsert_grant(
    *,
    tenant_id: str,
    resource_type: str,
    resource_id: str,
    principal: str,
    role: str,
    created_by: str = "system",
) -> ResourceAclGrant:
    if resource_type not in RESOURCE_TYPES:
        raise ValueError(f"Unsupported resource_type: {resource_type}")
    if role not in ACL_ROLES:
        raise ValueError(f"Unsupported ACL role: {role}")
    principal_n = principal.strip().lower()
    if not principal_n or not resource_id.strip():
        raise ValueError("principal and resource_id are required")
    grants = _load_all()
    for g in grants:
        if (
            g.tenant_id == (tenant_id or "")
            and g.resource_type == resource_type
            and g.resource_id == resource_id
            and g.principal == principal_n
        ):
            g.role = role
            _rewrite(grants)
            return g
    grant = ResourceAclGrant(
        id=str(uuid.uuid4()),
        tenant_id=tenant_id or "",
        resource_type=resource_type,
        resource_id=resource_id,
        principal=principal_n,
        role=role,
        created_at=_now(),
        created_by=created_by,
    )
    grants.append(grant)
    _rewrite(grants)
    return grant


def revoke_grant(grant_id: str, *, tenant_id: str = "") -> bool:
    grants = _load_all()
    next_g = [g for g in grants if not (g.id == grant_id and g.tenant_id == (tenant_id or ""))]
    if len(next_g) == len(grants):
        return False
    _rewrite(next_g)
    return True


def principal_role_for_resource(
    *,
    tenant_id: str,
    resource_type: str,
    resource_id: str,
    principal: str,
) -> str | None:
    """Highest ACL role for principal on this resource, or None if no grants apply."""
    grants = list_grants(
        tenant_id=tenant_id,
        resource_type=resource_type,
        resource_id=resource_id,
        principal=principal,
    )
    if not grants:
        # If *any* grants exist for the resource, deny-by-default for non-grantees.
        any_for_resource = list_grants(
            tenant_id=tenant_id,
            resource_type=resource_type,
            resource_id=resource_id,
        )
        if any_for_resource:
            return None
        return "__open__"  # no ACL configured → fall through to coarse RBAC
    return max(grants, key=lambda g: _ROLE_RANK.get(g.role, 0)).role


def assert_resource_acl(
    *,
    tenant_id: str,
    resource_type: str,
    resource_id: str,
    principal: str,
    min_role: str = "viewer",
    is_admin: bool = False,
) -> None:
    """Raise PermissionError when ACL denies access."""
    if is_admin:
        return
    needed = _ROLE_RANK.get(min_role, 1)
    role = principal_role_for_resource(
        tenant_id=tenant_id,
        resource_type=resource_type,
        resource_id=resource_id,
        principal=principal,
    )
    if role == "__open__":
        return
    if role is None or _ROLE_RANK.get(role, 0) < needed:
        raise PermissionError(
            f"ACL denied: {principal} lacks {min_role} on {resource_type}/{resource_id}"
        )
