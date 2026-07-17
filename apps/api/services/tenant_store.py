"""Tenant store — workspace-bound enterprise SaaS settings.

A tenant represents the enterprise customer that accesses DataFlow through a
custom domain (e.g. ``dataflow.wellsfargo.com``).  Each tenant is linked to one
workspace; that workspace's connectors, jobs, and members are isolated under
the tenant's security and residency policies.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from services.byok_key_manager import is_ip_allowed
from services.platform_config import data_dir

logger = logging.getLogger(__name__)

STORE_PATH = data_dir() / "tenants.json"

_ALLOWED_REGIONS = {
    "us-east-1",
    "us-east-2",
    "us-west-1",
    "us-west-2",
    "eu-west-1",
    "eu-west-2",
    "eu-central-1",
    "ap-southeast-1",
    "ap-south-1",
    "ap-northeast-1",
    "ca-central-1",
    "sa-east-1",
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_raw() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"tenants": []}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return {"tenants": []}
        return raw
    except Exception:
        return {"tenants": []}


def _save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(STORE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2), encoding="utf-8")
    tmp.replace(STORE_PATH)


def _normalize_domain(domain: str) -> str:
    d = domain.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = d.split("/")[0]
    d = d.split(":")[0]
    d = re.sub(r"^www\\.", "", d)
    return d


@dataclass
class Tenant:
    id: str
    workspace_id: str = ""
    name: str = ""
    custom_domain: str = ""  # e.g. dataflow.wellsfargo.com
    data_region: str = ""      # e.g. us-east-1
    byok_key_id: str = ""
    security_contact_email: str = ""
    mfa_required: bool = False
    session_timeout_hours: int = 8
    ip_allowlist: list[str] = field(default_factory=list)
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Tenant":
        return cls(
            id=data.get("id", ""),
            workspace_id=data.get("workspace_id", ""),
            name=data.get("name", ""),
            custom_domain=data.get("custom_domain", ""),
            data_region=data.get("data_region", ""),
            byok_key_id=data.get("byok_key_id", ""),
            security_contact_email=data.get("security_contact_email", ""),
            mfa_required=bool(data.get("mfa_required", False)),
            session_timeout_hours=int(data.get("session_timeout_hours", 8)),
            ip_allowlist=list(data.get("ip_allowlist", []) or []),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
        )

    @property
    def normalized_domain(self) -> str:
        return _normalize_domain(self.custom_domain) if self.custom_domain else ""


def _tenants(data: dict[str, Any] | None = None) -> list[Tenant]:
    data = data if data is not None else _load_raw()
    return [Tenant.from_dict(t) for t in data.get("tenants", []) if isinstance(t, dict)]


def create_tenant(
    workspace_id: str,
    name: str,
    custom_domain: str = "",
    data_region: str = "",
    byok_key_id: str = "",
    security_contact_email: str = "",
    mfa_required: bool = False,
    session_timeout_hours: int = 8,
    ip_allowlist: list[str] | None = None,
) -> Tenant:
    """Create a new tenant bound to a workspace."""
    if custom_domain and get_tenant_by_domain(custom_domain):
        raise ValueError(f"Custom domain '{custom_domain}' is already in use")

    data = _load_raw()
    if workspace_id and any(t.workspace_id == workspace_id for t in _tenants(data)):
        raise ValueError(f"Workspace {workspace_id} already has a tenant")

    tenant = Tenant(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id or "",
        name=name.strip()[:128] or "Tenant",
        custom_domain=custom_domain.strip()[:253],
        data_region=_validate_region(data_region),
        byok_key_id=byok_key_id,
        security_contact_email=security_contact_email.strip()[:128],
        mfa_required=mfa_required,
        session_timeout_hours=max(1, min(24, int(session_timeout_hours))),
        ip_allowlist=_validate_allowlist(ip_allowlist or []),
    )
    data["tenants"].append(tenant.to_dict())
    _save(data)
    logger.info("Created tenant %s for workspace %s", tenant.id, workspace_id)
    return tenant


def get_tenant(tenant_id: str) -> Tenant | None:
    if not tenant_id:
        return None
    for t in _tenants():
        if t.id == tenant_id:
            return t
    return None


def get_tenant_by_domain(domain: str) -> Tenant | None:
    target = _normalize_domain(domain)
    if not target:
        return None
    for t in _tenants():
        if t.normalized_domain == target:
            return t
    return None


def get_tenant_for_workspace(workspace_id: str) -> Tenant | None:
    if not workspace_id:
        return None
    for t in _tenants():
        if t.workspace_id == workspace_id:
            return t
    return None


def list_tenants() -> list[Tenant]:
    return sorted(_tenants(), key=lambda t: t.created_at, reverse=True)


def update_tenant(tenant_id: str, **kwargs: Any) -> Tenant | None:
    data = _load_raw()
    for raw in data.get("tenants", []):
        if isinstance(raw, dict) and raw.get("id") == tenant_id:
            if "custom_domain" in kwargs:
                new_domain = _normalize_domain(kwargs["custom_domain"])
                existing = get_tenant_by_domain(kwargs["custom_domain"])
                if existing and existing.id != tenant_id:
                    raise ValueError(f"Custom domain '{kwargs['custom_domain']}' is already in use")
                raw["custom_domain"] = kwargs["custom_domain"].strip()[:253]
            if "name" in kwargs:
                raw["name"] = kwargs["name"].strip()[:128]
            if "data_region" in kwargs:
                raw["data_region"] = _validate_region(kwargs["data_region"])
            if "byok_key_id" in kwargs:
                raw["byok_key_id"] = kwargs["byok_key_id"]
            if "security_contact_email" in kwargs:
                raw["security_contact_email"] = kwargs["security_contact_email"].strip()[:128]
            if "mfa_required" in kwargs:
                raw["mfa_required"] = bool(kwargs["mfa_required"])
            if "session_timeout_hours" in kwargs:
                raw["session_timeout_hours"] = max(1, min(24, int(kwargs["session_timeout_hours"])))
            if "ip_allowlist" in kwargs:
                raw["ip_allowlist"] = _validate_allowlist(kwargs["ip_allowlist"])
            raw["updated_at"] = _now()
            _save(data)
            return Tenant.from_dict(raw)
    return None


def delete_tenant(tenant_id: str) -> bool:
    data = _load_raw()
    before = len(data.get("tenants", []))
    data["tenants"] = [t for t in data.get("tenants", []) if isinstance(t, dict) and t.get("id") != tenant_id]
    if len(data["tenants"]) == before:
        return False
    _save(data)
    return True


def _validate_region(region: str) -> str:
    r = (region or "").strip().lower()
    if not r:
        return ""
    if r in _ALLOWED_REGIONS:
        return r
    # Accept free-form region tags for private-cloud / on-prem SaaS nodes.
    if re.match(r"^[a-z0-9-]{2,32}$", r):
        return r
    raise ValueError(f"Unsupported data region: {region}")


def _validate_allowlist(entries: list[str]) -> list[str]:
    out: list[str] = []
    for e in entries:
        e = e.strip()
        if not e:
            continue
        try:
            # Accept single IP or CIDR; normalize to network string.
            network = ipaddress.ip_network(e, strict=False)
            out.append(str(network))
        except ValueError:
            logger.warning("Ignoring invalid allowlist entry: %s", e)
    return out


def resolve_tenant_from_host(host: str) -> Tenant | None:
    return get_tenant_by_domain(host)


def is_request_ip_allowed(request_client_ip: str | None, tenant: Tenant | None) -> bool:
    if not tenant or not tenant.ip_allowlist:
        return True
    return is_ip_allowed(request_client_ip or "", tenant.ip_allowlist)


def default_region() -> str:
    return os.getenv("DATAFLOW_DEFAULT_REGION", "us-east-1").strip() or "us-east-1"


def tenant_region(tenant: Tenant | None = None) -> str:
    if tenant and tenant.data_region:
        return tenant.data_region
    return default_region()
