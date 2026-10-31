"""Workspace IP allowlist: which workspaces a request touches, and the decision.

QA round 11 (S7): the list a workspace admin saves on the Enterprise page is
enforced on every authenticated request to that workspace, on the shared app
host as on a custom domain, for sign-in sessions and API keys alike
(src/middleware/workspace_ip_allowlist_middleware.py runs it after auth).

* **Which workspaces.** The ``X-Workspace-Id`` header when sent; otherwise
  every workspace the caller can read without one (an API key: its own; a
  member: all their memberships, because a header-less list or count returns
  rows from each). A tenant custom-domain host adds its workspace. A platform
  administrator with no header acts deployment-wide and is not evaluated
  against any one workspace's list.
* **Kill switch.** ``DATAWRAP_TENANT_IP_ALLOWLIST_ENFORCE`` (DATAFLOW_ alias)
  defaults ON once a list is saved; ``0/false/off`` turns blocks into audited
  ``tenant.ip_allowlist.would_block`` admissions for the whole deployment.
* **Break-glass.** A platform administrator outside the list may send
  ``X-Datawrap-Break-Glass: <reason>``; the request is admitted and audited
  (``workspace.ip_allowlist.break_glass``) every time.
* **Lock-out guard.** :func:`lockout_refusal` refuses to save a list that
  excludes the address of the admin saving it.
* **Trusted address only.** A list is enforced only on a client address the
  server can trust: the trusted proxy hop count is configured (even to 0), or
  the resolved address is public. With the count unset and a private/CGNAT/
  loopback address (the proxy itself, on a first deploy before ops sets the
  count) nothing is blocked; the request is audited as
  ``tenant.ip_allowlist.would_block`` with reason ``client_ip_untrusted`` and
  the posture reports the list as not enforced.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Any

from services.brand_env import getenv_brand_bool
from services.byok_key_manager import is_ip_allowed
from services.client_ip import (
    client_ip_from_request,
    trusted_proxy_count,
    trusted_proxy_count_configured,
)

BREAK_GLASS_HEADER = "X-Datawrap-Break-Glass"
BREAK_GLASS_MIN_REASON = 8
BLOCK_REASON = "ip_not_in_workspace_allowlist"
UNTRUSTED_REASON = "client_ip_untrusted"
HOPS_UNSET_MESSAGE = (
    "the platform's proxy hop count is not configured; ask your platform administrator to set it"
)


def enforcement_on() -> bool:
    """Kill switch: on unless the platform turned allowlist enforcement off."""
    return getenv_brand_bool("TENANT_IP_ALLOWLIST_ENFORCE", True)


@dataclass(frozen=True)
class Refusal:
    client_ip: str
    workspace_id: str
    tenant_id: str
    unrestricted: bool


def _request_workspaces(request: Any) -> tuple[set[str], bool]:
    from services.tenant_scope import _base_scope, requested_header

    scope = _base_scope(request)
    header = requested_header(request)
    if header:
        ids = {header}
    elif scope.unrestricted:
        ids = set()
    else:
        ids = set(scope.member_workspace_ids)
    host_ws = str(getattr(request.state, "tenant_workspace_id", "") or "")
    if host_ws:
        ids.add(host_ws)
    return ids, scope.unrestricted


def evaluate(request: Any) -> Refusal | None:
    """The refusal this request earns, or ``None`` when it may proceed."""
    from services.tenant_store import tenants_for_workspaces

    ids, unrestricted = _request_workspaces(request)
    listed = [t for t in tenants_for_workspaces(ids) if t.ip_allowlist]
    if not listed:
        return None
    ip = client_ip_from_request(request)
    for tenant in sorted(listed, key=lambda t: t.workspace_id):
        if not is_ip_allowed(ip, tenant.ip_allowlist):
            return Refusal(ip, tenant.workspace_id, tenant.id, unrestricted)
    return None


def break_glass_reason(request: Any) -> str:
    raw = request.headers.get(BREAK_GLASS_HEADER) or ""
    reason = " ".join(str(raw).split())[:500]
    return reason if len(reason) >= BREAK_GLASS_MIN_REASON else ""


#: Where a proxy's own address comes from: RFC 1918, carrier-grade NAT
#: (Railway's edge), loopback, link-local and IPv6 unique-local.
_PROXY_NETWORKS = tuple(
    ipaddress.ip_network(n)
    for n in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "127.0.0.0/8",
              "169.254.0.0/16", "::1/128", "fc00::/7", "fe80::/10")
)


def _is_routable(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return False
    return not any(addr in net for net in _PROXY_NETWORKS if net.version == addr.version)


def client_ip_trusted(ip: str) -> bool:
    """Whether an allowlist may act on ``ip`` (see the module docstring)."""
    return trusted_proxy_count_configured() or _is_routable(ip)


def warn_if_hop_count_unset(logger: Any) -> bool:
    """Startup: one warning when lists exist but the hop count is unset."""
    if trusted_proxy_count_configured():
        return False
    try:
        from services.tenant_store import list_tenants

        listed = sum(1 for t in list_tenants() if t.ip_allowlist)
    except Exception:  # noqa: BLE001 — a startup hint must never stop startup
        return False
    if not listed:
        return False
    logger.warning(
        "%d workspace IP allowlist(s) are saved but DATAWRAP_TRUSTED_PROXY_COUNT is unset: "
        "they are enforced only for public client addresses; requests that resolve to a "
        "private/proxy address are admitted and audited (client_ip_untrusted). Set the hop "
        "count (docs/DEPLOYMENT.md).", listed,
    )
    return True


def proxy_hint(ip: str) -> str:
    """Admin-facing hint when the server cannot see real client addresses."""
    if _is_routable(ip):
        return ""
    return (
        f" {ip or 'That'} is a private or proxy address, not a public one. If it is "
        "not your address, ask your platform administrator to configure how many "
        "proxies sit in front of the API (the trusted proxy hop count) before "
        "saving an allowlist."
    )


def lockout_refusal(request: Any, entries: list[str] | None) -> str | None:
    """The message refusing a list that excludes the caller, or ``None``."""
    from services.tenant_store import _validate_allowlist

    normalized = _validate_allowlist(list(entries or []))
    if not normalized:
        return None
    ip = client_ip_from_request(request)
    if is_ip_allowed(ip, normalized):
        return None
    if not client_ip_trusted(ip):
        # The server cannot see the caller's real address yet (hop count
        # unset); the list is saved but not enforced, and the save response
        # says so. Refusing would block every save over the proxy's address.
        return None
    return (
        f"This IP allowlist does not include your current IP address ({ip or 'unknown'}). "
        "Saving it would lock you out of this workspace. Add your address, or a range "
        "that contains it, and save again." + proxy_hint(ip)
    )


def client_ip_source() -> str:
    n = trusted_proxy_count()
    if n <= 0:
        return "direct connection (no trusted proxies configured)"
    return f"proxy chain ({n} trusted {'hop' if n == 1 else 'hops'})"
