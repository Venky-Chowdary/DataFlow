"""Tenant store — workspace-bound enterprise SaaS settings.

A tenant represents the enterprise customer that accesses Datawrap through a
custom domain (e.g. ``dataflow.wellsfargo.com``).  Each tenant is linked to one
workspace; that workspace's connectors, jobs, and members are isolated under
the tenant's security and residency policies.
"""

from __future__ import annotations

import ipaddress
import json
import logging
from services.brand_env import getenv_brand
from services.workspace_filter import WORKSPACE_VIEWS
import re
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from services.byok_key_manager import is_ip_allowed
from services.platform_config import data_dir
from services.shared_record_store import SharedRecordSet
from services.value_serializer import json_default

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


def _load_raw(**scope: Any) -> dict[str, Any]:
    """The store as a dict that always carries a ``tenants`` list.

    Callers append to ``data["tenants"]`` directly, so a file written by an
    older shape — or one missing the key — must not turn a tenant write into a
    KeyError the caller reports as a server fault.

    Read from the shared control-plane store when one is configured, so every
    API replica enforces the same IP allowlist and require-SSO flag (H-5).
    ``scope`` narrows the database read (``part=``, ``parts=``, ``keys=``,
    ``tag=``); a file store returns every tenant, so callers still filter.
    """
    data = _DOC.load(**scope)
    if not isinstance(data.get("tenants"), list):
        data["tenants"] = []
    return data


def _save(data: dict[str, Any]) -> None:
    _DOC.save(data)


def _file_load() -> dict[str, Any]:
    if not STORE_PATH.exists():
        return {"tenants": []}
    try:
        raw = json.loads(STORE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("Tenant store at %s is unreadable; starting from empty", STORE_PATH)
        return {"tenants": []}
    if not isinstance(raw, dict):
        return {"tenants": []}
    if not isinstance(raw.get("tenants"), list):
        raw["tenants"] = []
    return raw


def _file_save(data: dict[str, Any]) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STORE_PATH.with_suffix(STORE_PATH.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=json_default), encoding="utf-8")
    tmp.replace(STORE_PATH)


def _unique_claims(record: dict[str, Any]) -> list[str]:
    """What no two tenants may share across replicas: the workspace, and the custom domain.

    Ops review round 2, High 1 and Medium 9. A tenant with no workspace is
    never routed by its domain (:func:`get_tenant_by_domain`), so it claims none.
    """
    workspace = str(record.get("workspace_id") or "").strip()
    if not workspace:
        return []
    domain = _normalize_domain(str(record.get("custom_domain") or ""))
    return [f"workspace:{workspace}"] + ([f"domain:{domain}"] if domain else [])


def _lookup_tags(record: dict[str, Any]) -> list[str]:
    """Indexed lookups besides the workspace and the id: the custom domain (host routing, CORS)."""
    domain = _normalize_domain(str(record.get("custom_domain") or ""))
    return [f"domain:{domain}"] if domain else []


_DOC = SharedRecordSet(
    "tenant_settings",
    file_load=lambda: _file_load(),
    file_save=lambda data: _file_save(data),
    file_path=lambda: STORE_PATH,
    empty=lambda: {"tenants": []},
    list_key="tenants",
    partition_key="workspace_id",
    unique=_unique_claims,
    tags=_lookup_tags,
)


def _normalize_domain(domain: str) -> str:
    d = domain.lower().strip()
    d = re.sub(r"^https?://", "", d)
    d = d.split("/")[0]
    d = d.split(":")[0]
    d = re.sub(r"^www\.", "", d)
    return d


_LABEL = re.compile(r"^(?!-)[a-z0-9-]{1,63}(?<!-)$")
_TLD = re.compile(r"^(?:[a-z]{2,63}|xn--[a-z0-9-]{1,59})$")


def _validate_domain(domain: str) -> str:
    """A custom domain as a normalized DNS host name, or ``""`` to clear it.

    It becomes a DNS challenge name and, once proven, a browser origin, so
    anything that is not a plain host name (spaces, a path, an IP address, a
    single label) is refused rather than stored (D337/D341).
    """
    raw = str(domain or "").strip()
    if not raw:
        return ""
    bare = re.sub(r"^https?://", "", raw.lower()).rstrip("/")
    d = _normalize_domain(raw).rstrip(".")
    labels = d.split(".")
    valid = (
        not any(c in bare for c in "/?#:@ \t")  # a path, port or credentials is not a host name
        and 0 < len(d) <= 253
        and len(labels) >= 2
        and all(_LABEL.match(label) for label in labels)
        and bool(_TLD.match(labels[-1]))
    )
    if not valid:
        raise ValueError(
            f"Invalid custom domain {raw[:80]!r}: use a host name such as data.example.com "
            "(letters, digits and hyphens; no spaces, paths, ports or IP addresses)"
        )
    return d


def _new_domain_challenge() -> str:
    import secrets

    return secrets.token_hex(16)


def _set_custom_domain(raw: dict[str, Any], domain: str) -> None:
    """Store ``domain``; a changed (or cleared) domain loses its proof."""
    if domain == _normalize_domain(str(raw.get("custom_domain") or "")) and domain:
        raw["custom_domain"] = domain
        if not raw.get("custom_domain_challenge"):  # saved before D337
            raw["custom_domain_challenge"] = _new_domain_challenge()
        return
    raw["custom_domain"] = domain
    raw["custom_domain_verified"] = False
    raw["custom_domain_verified_at"] = ""
    raw["custom_domain_challenge"] = _new_domain_challenge() if domain else ""


@dataclass
class Tenant:
    id: str
    workspace_id: str = ""
    name: str = ""
    custom_domain: str = ""  # e.g. dataflow.wellsfargo.com
    # D337: a custom domain is a credentialed CORS origin only once its DNS
    # TXT challenge passed (services.custom_domain_verification). Changing
    # the domain clears the proof and issues a new challenge token.
    custom_domain_verified: bool = False
    custom_domain_verified_at: str = ""
    custom_domain_challenge: str = ""
    data_region: str = ""      # e.g. us-east-1
    byok_key_id: str = ""
    security_contact_email: str = ""
    mfa_required: bool = False
    session_timeout_hours: int = 8
    # QA round 11: a saved session_timeout_hours is enforced. On by default;
    # an admin can still record the hours without enforcing them (explicit
    # False). Tenants saved before round 11 keep the False they were stored
    # with until the hours are saved again (docs/DEPLOYMENT.md, backfill note).
    enforce_session_timeout: bool = True
    ip_allowlist: list[str] = field(default_factory=list)
    # D343: members must sign in through the deployment's SSO provider; their
    # password sign-in is refused (services.sso_enforcement).
    sso_enforced: bool = False
    # Connector egress. Empty means private ranges stay refused. This is not
    # the inbound API allowlist, and it is not a deploy setting.
    egress_allowlist: list[str] = field(default_factory=list)
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
            custom_domain_verified=bool(data.get("custom_domain_verified", False)),
            custom_domain_verified_at=str(data.get("custom_domain_verified_at", "") or ""),
            custom_domain_challenge=str(data.get("custom_domain_challenge", "") or ""),
            data_region=data.get("data_region", ""),
            byok_key_id=data.get("byok_key_id", ""),
            security_contact_email=data.get("security_contact_email", ""),
            mfa_required=bool(data.get("mfa_required", False)),
            session_timeout_hours=int(data.get("session_timeout_hours", 8)),
            enforce_session_timeout=bool(data.get("enforce_session_timeout", True)),
            ip_allowlist=list(data.get("ip_allowlist", []) or []),
            sso_enforced=bool(data.get("sso_enforced", False)),
            egress_allowlist=list(data.get("egress_allowlist", []) or []),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
        )

    @property
    def normalized_domain(self) -> str:
        return _normalize_domain(self.custom_domain) if self.custom_domain else ""


def _tenants(data: dict[str, Any] | None = None) -> list[Tenant]:
    data = data if data is not None else _load_raw()
    return [Tenant.from_dict(t) for t in data.get("tenants", []) if isinstance(t, dict)]


@_DOC.retrying
def create_tenant(
    workspace_id: str,
    name: str,
    custom_domain: str = "",
    data_region: str = "",
    byok_key_id: str = "",
    security_contact_email: str = "",
    mfa_required: bool = False,
    session_timeout_hours: int = 8,
    enforce_session_timeout: bool = True,
    ip_allowlist: list[str] | None = None,
    egress_allowlist: list[str] | None = None,
    sso_enforced: bool = False,
) -> Tenant:
    """Create a new tenant bound to a workspace."""
    custom_domain = _validate_domain(custom_domain)
    if custom_domain and get_tenant_by_domain(custom_domain):
        raise ValueError(f"Custom domain '{custom_domain}' is already in use")

    data = _load_raw(part=workspace_id or "")
    if workspace_id in WORKSPACE_VIEWS:
        # A platform admin's view names no workspace a tenant could belong to
        # (D218). Whether a real workspace exists is the route's check: the
        # store stays usable on its own.
        raise ValueError(f"Workspace {workspace_id} does not exist")
    if workspace_id and any(t.workspace_id == workspace_id for t in _tenants(data)):
        raise ValueError(f"Workspace {workspace_id} already has a tenant")

    tenant = Tenant(
        id=str(uuid.uuid4()),
        workspace_id=workspace_id or "",
        name=_validate_name(name) or "Tenant",
        custom_domain=custom_domain,
        custom_domain_challenge=_new_domain_challenge() if custom_domain else "",
        data_region=_validate_region(data_region),
        byok_key_id=byok_key_id,
        security_contact_email=_validate_contact_email(security_contact_email),
        mfa_required=mfa_required,
        session_timeout_hours=max(1, min(24, int(session_timeout_hours))),
        enforce_session_timeout=bool(enforce_session_timeout),
        ip_allowlist=_validate_allowlist(ip_allowlist or []),
        egress_allowlist=_validate_egress_allowlist(egress_allowlist or []),
        sso_enforced=bool(sso_enforced),
    )
    data["tenants"].append(tenant.to_dict())
    _save(data)
    logger.info("Created tenant %s for workspace %s", tenant.id, workspace_id)
    return tenant


def get_tenant(tenant_id: str) -> Tenant | None:
    if not tenant_id:
        return None
    for t in _tenants(_load_raw(keys=[tenant_id])):
        if t.id == tenant_id:
            return t
    return None


def get_tenant_by_domain(domain: str, *, verified_only: bool = False) -> Tenant | None:
    """The tenant that claims ``domain``.

    ``verified_only`` (browser-origin trust, D337) answers only a tenant whose
    DNS ownership challenge passed; a merely saved domain is a claim, not proof.
    """
    target = _normalize_domain(domain)
    if not target:
        return None
    for t in _tenants(_load_raw(tag=f"domain:{target}")):
        # A record written before tenants were workspace-scoped belongs to no
        # workspace, so host routing cannot decide what data it would serve:
        # resolving it would answer a client domain out of an empty scope.
        if t.normalized_domain == target and t.workspace_id:
            if verified_only and not t.custom_domain_verified:
                return None
            return t
    return None


@_DOC.retrying
def ensure_domain_challenge(tenant_id: str) -> Tenant | None:
    """The tenant, with a challenge token for its domain (a pre-D337 record has none)."""
    data = _load_raw(keys=[tenant_id])
    for raw in data.get("tenants", []):
        if isinstance(raw, dict) and raw.get("id") == tenant_id:
            if raw.get("custom_domain") and not raw.get("custom_domain_challenge"):
                raw["custom_domain_challenge"] = _new_domain_challenge()
                _save(data)
            return Tenant.from_dict(raw)
    return None


@_DOC.retrying
def mark_custom_domain_verified(tenant_id: str, domain: str) -> Tenant | None:
    """Record a passed DNS challenge for ``domain``.

    Only services.custom_domain_verification calls this, after the TXT record
    matched. A domain changed since the lookup started is not marked.
    """
    data = _load_raw(keys=[tenant_id])
    for raw in data.get("tenants", []):
        if isinstance(raw, dict) and raw.get("id") == tenant_id:
            if not domain or _normalize_domain(str(raw.get("custom_domain") or "")) != domain:
                return None
            raw["custom_domain_verified"] = True
            raw["custom_domain_verified_at"] = _now()
            raw["updated_at"] = _now()
            _save(data)
            return Tenant.from_dict(raw)
    return None


def get_tenant_for_workspace(workspace_id: str) -> Tenant | None:
    if not workspace_id:
        return None
    for t in _tenants(_load_raw(part=workspace_id)):
        if t.workspace_id == workspace_id:
            return t
    return None


def tenants_for_workspaces(workspace_ids: list[str] | set[str] | frozenset[str]) -> list[Tenant]:
    """The tenants of these workspaces, from one read of the store."""
    wanted = {w for w in workspace_ids if w}
    if not wanted:
        return []
    return [t for t in _tenants(_load_raw(parts=wanted)) if t.workspace_id in wanted]


def list_tenants() -> list[Tenant]:
    """Every tenant, newest first: one read of the whole store (maintenance and counts)."""
    return sorted(_tenants(), key=lambda t: t.created_at, reverse=True)


def list_tenants_page(*, limit: int = 100, offset: int = 0) -> tuple[list[Tenant], int | None]:
    """One page of tenants, newest first, and the offset of the next page (None: the last page)."""
    data, next_offset = _DOC.page(offset=offset, limit=limit)
    return _tenants(data), next_offset


@_DOC.retrying
def update_tenant(tenant_id: str, **kwargs: Any) -> Tenant | None:
    data = _load_raw(keys=[tenant_id])
    for raw in data.get("tenants", []):
        if isinstance(raw, dict) and raw.get("id") == tenant_id:
            if "custom_domain" in kwargs:
                new_domain = _validate_domain(kwargs["custom_domain"])
                existing = get_tenant_by_domain(new_domain)
                if existing and existing.id != tenant_id:
                    raise ValueError(f"Custom domain '{new_domain}' is already in use")
                _set_custom_domain(raw, new_domain)
            if "name" in kwargs:
                raw["name"] = _validate_name(kwargs["name"])
            if "data_region" in kwargs:
                raw["data_region"] = _validate_region(kwargs["data_region"])
            if "byok_key_id" in kwargs:
                raw["byok_key_id"] = kwargs["byok_key_id"]
            if "security_contact_email" in kwargs:
                raw["security_contact_email"] = _validate_contact_email(kwargs["security_contact_email"])
            if "mfa_required" in kwargs:
                raw["mfa_required"] = bool(kwargs["mfa_required"])
            if "session_timeout_hours" in kwargs:
                raw["session_timeout_hours"] = max(1, min(24, int(kwargs["session_timeout_hours"])))
                # Saving the hours means "enforce them", unless the same save
                # explicitly records them as policy only.
                raw["enforce_session_timeout"] = bool(kwargs.get("enforce_session_timeout", True))
            elif "enforce_session_timeout" in kwargs:
                raw["enforce_session_timeout"] = bool(kwargs["enforce_session_timeout"])
            if "ip_allowlist" in kwargs:
                raw["ip_allowlist"] = _validate_allowlist(kwargs["ip_allowlist"])
            if "egress_allowlist" in kwargs:
                raw["egress_allowlist"] = _validate_egress_allowlist(kwargs["egress_allowlist"])
            if "sso_enforced" in kwargs:
                raw["sso_enforced"] = bool(kwargs["sso_enforced"])
            raw["updated_at"] = _now()
            _save(data)
            return Tenant.from_dict(raw)
    return None


@_DOC.retrying
def delete_tenant(tenant_id: str) -> bool:
    data = _load_raw(keys=[tenant_id])
    before = len(data.get("tenants", []))
    data["tenants"] = [t for t in data.get("tenants", []) if isinstance(t, dict) and t.get("id") != tenant_id]
    if len(data["tenants"]) == before:
        return False
    _save(data)
    return True


def supported_regions() -> list[str]:
    """Regions a tenant may record (D341).

    The built-in cloud regions, this deployment's ``DATAWRAP_DEFAULT_REGION``,
    and any private-cloud / on-prem region tags the operator declares in
    ``DATAWRAP_DATA_REGIONS`` (comma-separated, ``[a-z0-9-]{2,32}`` each). A
    workspace admin can no longer invent one (``moon-1``).
    """
    extra = {
        tag.strip().lower()
        for tag in (getenv_brand("DATA_REGIONS", "") or "").split(",")
        if re.fullmatch(r"[a-z0-9-]{2,32}", tag.strip().lower())
    }
    return sorted(_ALLOWED_REGIONS | extra | {default_region().lower()})


def _validate_region(region: str) -> str:
    r = (region or "").strip().lower()
    if not r:
        return ""
    allowed = supported_regions()
    if r in allowed:
        return r
    raise ValueError(f"Unsupported data region {str(region)[:40]!r}: use one of {', '.join(allowed)}")


#: Markup and control characters: a tenant name is shown in the UI, the
#: posture and the compliance report, so it is plain text (D341).
_UNSAFE_TEXT = re.compile(r"[<>\x00-\x1f\x7f]")


def _validate_name(name: str) -> str:
    n = str(name or "").strip()
    if len(n) > 128:
        raise ValueError("Tenant name is longer than 128 characters")
    if _UNSAFE_TEXT.search(n):
        raise ValueError("Tenant name may not contain <, > or control characters")
    return n


def _validate_contact_email(email: str) -> str:
    e = str(email or "").strip()
    if not e:
        return ""
    from services.user_store import validate_email

    try:
        if len(e) > 128 or _UNSAFE_TEXT.search(e):
            raise ValueError(e)
        return validate_email(e)
    except ValueError:
        raise ValueError(f"Invalid security contact email {e[:80]!r}: use an address such as security@example.com") from None


def _validate_egress_allowlist(entries: list[str]) -> list[str]:
    """CIDRs a workspace may dial. Metadata and link-local cannot be listed."""
    from services.egress_guard import egress_allowlist_error

    cleaned = [str(e or "").strip() for e in entries if str(e or "").strip()]
    reason = egress_allowlist_error(cleaned)
    if reason:
        raise ValueError(reason)
    out: list[str] = []
    for entry in cleaned:
        out.append(str(ipaddress.ip_network(entry, strict=False)))
    return out


def _validate_allowlist(entries: list[str]) -> list[str]:
    """Normalize an inbound IP allowlist; an invalid entry is refused (D336).

    Dropping it silently stored ``[]`` for a list of one typo, and the admin
    read "saved" for a control that held nothing.
    """
    out: list[str] = []
    for e in entries:
        e = str(e or "").strip()
        if not e:
            continue
        try:
            # Accept single IP or CIDR; normalize to network string.
            network = ipaddress.ip_network(e, strict=False)
        except ValueError:
            raise ValueError(
                f"Invalid IP allowlist entry {e!r}: use an IP address or a CIDR range "
                "such as 203.0.113.0/24"
            ) from None
        out.append(str(network))
    return out


def resolve_tenant_from_host(host: str) -> Tenant | None:
    return get_tenant_by_domain(host)


def is_request_ip_allowed(request_client_ip: str | None, tenant: Tenant | None) -> bool:
    if not tenant or not tenant.ip_allowlist:
        return True
    return is_ip_allowed(request_client_ip or "", tenant.ip_allowlist)


def default_region() -> str:
    return getenv_brand("DEFAULT_REGION", "us-east-1").strip() or "us-east-1"


def tenant_region(tenant: Tenant | None = None) -> str:
    if tenant and tenant.data_region:
        return tenant.data_region
    return default_region()
