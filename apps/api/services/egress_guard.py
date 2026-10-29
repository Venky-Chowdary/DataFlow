"""Server-side egress policy — one deny set for webhooks and connector dials.

Notification URLs and connector hosts are operator-supplied. Scheme checks are
not enough: ``http://169.254.169.254/`` and ``http://10.1.2.3/`` are valid
http URLs, and a name that resolved to a public address at save time can
rebind to a metadata address before the socket opens.

Default is deny for RFC1918, loopback, link-local, CGNAT (100.64/10), IPv6
unique-local and link-local, and the cloud metadata addresses (IPv4
169.254.169.254, Alibaba 100.100.100.200, IPv6 fd00:ec2::254). A per-workspace
admin allowlist may permit an on-prem or VPC range. It is empty unless an
admin saves one. Metadata, link-local, multicast, and unspecified addresses
cannot be allowlisted. ``DATAFLOW_EGRESS_ALLOWLIST`` is an additional process
list for local proof runs; production deploy leaves it unset.

Two tiers of enforcement. The call sites production already enforced at
(connector Test, the SQL/MySQL/Postgres/AWS drivers, CDC, webhooks, the
Copilot connector tools) refuse private and loopback targets as before. The
newer sites (connector save/update, transfer reads and writes, introspection,
precount and probes, shared HTTP/SMTP/Mongo clients, redirects and
pagination) pass a ``point`` name and are AUDIT-ONLY by default: a private or
loopback target logs an ``egress.would_block`` audit event and is allowed.
``DATAFLOW_EGRESS_ENFORCE_NEW=1`` (or ``DATAWRAP_EGRESS_ENFORCE_NEW``) turns
those sites into refusals. Cloud metadata, link-local and multicast targets
are refused at every site regardless of the flag.
"""

from __future__ import annotations

import contextvars
import ipaddress
import os
import re
import socket
import threading
import urllib.parse
from contextlib import contextmanager
from typing import Any, Iterator, Sequence

from services.brand_env import getenv_brand
from services.would_apply_audit import audit_would_apply

_ALLOWED_SCHEMES = frozenset({"http", "https"})

_BLOCKED_NETWORKS = (
    ipaddress.ip_network("0.0.0.0/8"),
    ipaddress.ip_network("10.0.0.0/8"),
    ipaddress.ip_network("100.64.0.0/10"),
    ipaddress.ip_network("127.0.0.0/8"),
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("172.16.0.0/12"),
    ipaddress.ip_network("192.0.0.0/24"),
    ipaddress.ip_network("192.168.0.0/16"),
    ipaddress.ip_network("198.18.0.0/15"),
    ipaddress.ip_network("::1/128"),
    ipaddress.ip_network("fc00::/7"),
    ipaddress.ip_network("fe80::/10"),
    ipaddress.ip_network("fd00:ec2::254/128"),
    ipaddress.ip_network("100.100.100.200/32"),
)

# Cloud metadata and link-local stay refused even when a broader CIDR is listed.
_METADATA_ADDRESSES = frozenset(
    {
        ipaddress.ip_address("169.254.169.254"),
        ipaddress.ip_address("100.100.100.200"),
        ipaddress.ip_address("fd00:ec2::254"),
    }
)
_LINK_LOCAL_NETWORKS = (
    ipaddress.ip_network("169.254.0.0/16"),
    ipaddress.ip_network("fe80::/10"),
)

_METADATA_NAMES = frozenset(
    {
        "metadata",
        "metadata.google.internal",
        "metadata.google.com",
    }
)

_PHRASE_NETS: tuple[tuple[ipaddress.IPv4Network | ipaddress.IPv6Network, str], ...] = (
    (ipaddress.ip_network("10.0.0.0/8"), "private 10.x"),
    (ipaddress.ip_network("172.16.0.0/12"), "private 172.16/12"),
    (ipaddress.ip_network("192.168.0.0/16"), "private 192.168/16"),
    (ipaddress.ip_network("100.64.0.0/10"), "carrier-grade NAT"),
    (ipaddress.ip_network("fc00::/7"), "IPv6 unique-local"),
    (ipaddress.ip_network("127.0.0.0/8"), "loopback"),
    (ipaddress.ip_network("::1/128"), "loopback"),
)

_KEYWORD_HOST = re.compile(
    r"(?i)(?:^|[;\s])(?:host|hostname|server|data\s*source)\s*=\s*"
    r"(\[[^\]]+\]|[^\s;,]+)"
)
_EMBEDDED_IPV4 = re.compile(r"(?<![0-9])(?:\d{1,3}\.){3}\d{1,3}(?![0-9])")
_EMBEDDED_METADATA_NAME = re.compile(
    r"(?i)(?<![a-z0-9.-])metadata\.google\.(?:internal|com)(?![a-z0-9.-])"
)
_EMBEDDED_V6_META = re.compile(r"(?i)(?<![0-9a-f:])fd00:ec2::254(?![0-9a-f:])")
_BRACKET_V6 = re.compile(r"\[([0-9a-fA-F:]+)\]")

_PIN_LOCK = threading.RLock()


class EgressRefused(Exception):
    """A dial resolved to an address the egress policy refuses."""


def _unwrap(ip: ipaddress.IPv4Address | ipaddress.IPv6Address):
    mapped = getattr(ip, "ipv4_mapped", None)
    return mapped if mapped is not None else ip


def _normalize_host(host: str) -> str:
    raw = (host or "").strip().lower().rstrip(".")
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    if "%" in raw:
        raw = raw.split("%", 1)[0]
    return raw


def _name_is_non_overridable(host: str) -> bool:
    raw = _normalize_host(host)
    if raw in _METADATA_NAMES or raw == "localhost.localdomain":
        return True
    if raw.endswith(".internal") or raw.endswith(".localhost"):
        return True
    return False


def _is_non_overridable_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    ip = _unwrap(ip)
    if ip in _METADATA_ADDRESSES:
        return True
    if ip.is_link_local or ip.is_multicast or ip.is_unspecified:
        return True
    return any(ip in net for net in _LINK_LOCAL_NETWORKS)


def _blocked_phrase(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> str | None:
    ip = _unwrap(ip)
    if _is_non_overridable_ip(ip) and (ip.is_link_local or ip in _METADATA_ADDRESSES or any(ip in net for net in _LINK_LOCAL_NETWORKS)):
        return "cloud metadata and link-local"
    if ip.is_unspecified:
        return "unspecified"
    if ip.is_multicast:
        return "multicast"
    for net, phrase in _PHRASE_NETS:
        if ip in net:
            return phrase
    if ip.is_loopback:
        return "loopback"
    if ip.is_private or ip.is_reserved or any(ip in net for net in _BLOCKED_NETWORKS):
        return "private or non-routable"
    return None


def _parse_allowlist(entries: Sequence[str]) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    nets = []
    for entry in entries:
        text = str(entry or "").strip()
        if not text:
            continue
        try:
            nets.append(ipaddress.ip_network(text, strict=False))
        except ValueError:
            continue
    return nets


def _workspace_egress_entries(workspace_id: str) -> list[str]:
    wid = (workspace_id or "").strip()
    if not wid:
        return []
    try:
        from services.tenant_store import get_tenant_for_workspace
    except Exception:
        return []
    tenant = get_tenant_for_workspace(wid)
    if tenant is None:
        return []
    return list(getattr(tenant, "egress_allowlist", []) or [])


def _configured_networks(
    allowlist: Sequence[str] | None,
    workspace_id: str,
) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
    """Explicit ``allowlist`` wins. ``None`` reads the workspace, then the process env.

    The process env is how local proof dials loopback. Production deploy does
    not set it. An explicit empty list does not consult either store.
    """
    if allowlist is not None:
        return _parse_allowlist(allowlist)
    entries = _workspace_egress_entries(workspace_id)
    raw = os.environ.get("DATAFLOW_EGRESS_ALLOWLIST", "")
    if raw.strip():
        entries.extend(part.strip() for part in raw.split(",") if part.strip())
    return _parse_allowlist(entries)


def _ip_refusal_phrase(
    ip: ipaddress.IPv4Address | ipaddress.IPv6Address,
    nets: Sequence[ipaddress.IPv4Network | ipaddress.IPv6Network],
) -> str | None:
    ip = _unwrap(ip)
    phrase = _blocked_phrase(ip)
    if phrase is None:
        return None
    if _is_non_overridable_ip(ip):
        return phrase
    if any(ip in net for net in nets):
        return None
    return phrase


def _phrase_for_infos(infos, *, allowlist, workspace_id: str) -> str | None:
    nets = _configured_networks(allowlist, workspace_id)
    saw = False
    for info in infos or ():
        sockaddr = info[4] if len(info) > 4 else ()
        addr = sockaddr[0] if sockaddr else ""
        try:
            parsed = ipaddress.ip_address(_normalize_host(str(addr)))
        except ValueError:
            continue
        saw = True
        phrase = _ip_refusal_phrase(parsed, nets)
        if phrase:
            return phrase
    if not saw:
        return "DNS did not resolve to a public address"
    return None


def classify_host(
    host: str,
    *,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    fail_closed_dns: bool = True,
) -> str | None:
    """Refusal phrase for ``host``, or ``None`` when a dial is allowed.

    Names are resolved now. Every address must be public or inside the
    allowlist. One private answer refuses the name. DNS failure refuses
    when ``fail_closed_dns`` is set (webhooks and the connect-time recheck).
    A connector preview that cannot resolve a name is not a private hit —
    the dial rechecks and fail-closes before any socket.
    """
    raw = _normalize_host(host)
    if not raw:
        return "empty"
    if _name_is_non_overridable(raw):
        return "cloud metadata and link-local"
    nets = _configured_networks(allowlist, workspace_id)
    try:
        return _ip_refusal_phrase(ipaddress.ip_address(raw), nets)
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(raw, None)
    except EgressRefused as exc:
        # The preview check runs inside pinned_egress. A DNS miss there is
        # raised as EgressRefused; an unresolved name is not a private hit
        # when the caller asked to fail open. A resolved private address
        # still propagates.
        if "DNS did not resolve" not in str(exc):
            raise
        if fail_closed_dns:
            return "DNS did not resolve to a public address"
        return None
    except (OSError, UnicodeError):
        # UnicodeError is IDNA ("label empty or too long") on a string that
        # is not a hostname. Same outcome as a DNS miss: fail closed when the
        # caller asked for it, otherwise leave the dial to recheck.
        if fail_closed_dns:
            return "DNS did not resolve to a public address"
        return None
    phrase = _phrase_for_infos(infos, allowlist=allowlist, workspace_id=workspace_id)
    if phrase and phrase.startswith("DNS") and not fail_closed_dns:
        return None
    # ``localhost`` resolves to 127.0.0.1 and ::1. A list that covers
    # 127.0.0.0/8 is the operator's loopback exception. Another hostname that
    # happens to resolve to the same pair stays refused until ::1 is listed too.
    if (
        phrase == "loopback"
        and raw == "localhost"
        and _ip_refusal_phrase(ipaddress.ip_address("127.0.0.1"), nets) is None
    ):
        return None
    return phrase


def _refusal_text(host: str, phrase: str) -> str:
    if phrase.startswith("DNS"):
        return f"Refused connector host {host}: {phrase}."
    return (
        f"Refused connector host {host}: {phrase} addresses are not connection "
        "targets. To reach an on-prem or loopback target, allowlist its range "
        "(for example 127.0.0.0/8) via DATAFLOW_EGRESS_ALLOWLIST or the "
        "workspace egress allowlist."
    )


_ALWAYS_REFUSED_PHRASES = frozenset({"cloud metadata and link-local", "multicast"})
_AUDIT_CONNECTOR: contextvars.ContextVar[str] = contextvars.ContextVar(
    "egress_audit_connector", default=""
)


def enforce_new_points() -> bool:
    """True when ``DATAFLOW_EGRESS_ENFORCE_NEW`` makes the newer sites refuse.

    Default off: those sites audit a private or loopback target
    (``egress.would_block``) and allow it. See :func:`audit_would_block`.
    """
    raw = str(getenv_brand("EGRESS_ENFORCE_NEW", "") or "").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _real_getaddrinfo() -> Any:
    return _INSTALL.get("real") or socket.getaddrinfo


def _host_addresses(host: str) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    """Every address ``host`` is or resolves to, through the unguarded resolver."""
    raw = _normalize_host(host)
    try:
        return [_unwrap(ipaddress.ip_address(raw))]
    except ValueError:
        pass
    try:
        infos = _real_getaddrinfo()(raw, None)
    except (OSError, UnicodeError):
        return []
    found = []
    for info in infos:
        try:
            found.append(_unwrap(ipaddress.ip_address(_normalize_host(str(info[4][0])))))
        except (ValueError, IndexError, TypeError):
            continue
    return found


def _is_always_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return ip in _METADATA_ADDRESSES or ip.is_link_local or ip.is_multicast


def is_always_blocked_target(host: str) -> bool:
    """Cloud metadata, link-local and multicast: refused at every site, flag or not."""
    if _normalize_host(host) in _METADATA_NAMES:
        return True
    return any(_is_always_blocked_ip(ip) for ip in _host_addresses(host))


def host_class(host: str) -> str:
    """``metadata``, ``link-local``, ``loopback``, ``cgnat``, ``private``, ``public``
    or ``unresolved`` for ``host`` (the most restrictive answer wins)."""
    if _normalize_host(host) in _METADATA_NAMES:
        return "metadata"
    addresses = _host_addresses(host)
    if not addresses:
        return "unresolved"
    rank = ("metadata", "link-local", "loopback", "cgnat", "private", "public")
    classes = set()
    for ip in addresses:
        if ip in _METADATA_ADDRESSES:
            classes.add("metadata")
        elif ip.is_link_local:
            classes.add("link-local")
        elif ip.is_loopback:
            classes.add("loopback")
        elif ip in ipaddress.ip_network("100.64.0.0/10"):
            classes.add("cgnat")
        elif _blocked_phrase(ip):
            classes.add("private")
        else:
            classes.add("public")
    return min(classes, key=rank.index)


def audit_would_block(
    point: str,
    host: str,
    phrase: str,
    *,
    workspace_id: str = "",
    connector: str = "",
) -> None:
    """Record that the enforcing rule would have refused ``host`` at ``point``.

    One ``egress.would_block`` audit event per (point, connector, host,
    workspace) per hour, so a long transfer does not flood the audit chain.
    The audit write never raises into the dial.
    """
    connector = connector or _AUDIT_CONNECTOR.get()
    workspace = workspace_id or current_egress_workspace()
    shown = _normalize_host(host)
    if phrase in _ALWAYS_REFUSED_PHRASES:
        # Only a ``.internal`` / ``.localhost`` name reaches here with that
        # phrase: real metadata and link-local targets are refused, not audited.
        phrase = "private network name"
    audit_would_apply(
        "egress.would_block",
        resource=f"connector:{connector}" if connector else f"host:{shown}",
        dedupe_key=(point, connector, shown, workspace),
        workspace_id=workspace,
        flag="DATAFLOW_EGRESS_ENFORCE_NEW",
        details={
            "point": point,
            "connector": connector,
            "host": shown,
            "reason": phrase,
            "host_class": host_class(shown),
        },
    )


def _refuse_at_new_point(
    point: str,
    host: str,
    phrase: str,
    *,
    workspace_id: str = "",
    connector: str = "",
    from_address: bool = False,
) -> bool:
    """True when a newer enforcement site must refuse ``host``.

    Metadata/link-local always refuse. Anything else refuses only under
    ``DATAFLOW_EGRESS_ENFORCE_NEW``; otherwise it is audited and allowed. A DNS
    miss is not a policy hit and is left to the driver's own error.
    ``from_address`` says ``phrase`` came from the address the driver is about
    to use (dial time), so the phrase itself decides; a host-based check would
    resolve the name a second time and could be rebound between the answers.
    """
    always = phrase in _ALWAYS_REFUSED_PHRASES if from_address else is_always_blocked_target(host)
    if enforce_new_points() or always:
        return True
    if not phrase.startswith("DNS"):
        audit_would_block(point, host, phrase, workspace_id=workspace_id, connector=connector)
    return False


def host_is_blocked(host: str) -> bool:
    """True when ``host`` is denied with an empty allowlist. DNS failure denies."""
    return classify_host(host, allowlist=(), fail_closed_dns=True) is not None


def assert_connector_dial_allowed(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
    point: str = "",
    connector: str = "",
) -> None:
    """Raise ``EgressRefused`` before a driver dials a blocked address.

    Literal private, loopback, link-local, and CGNAT addresses are refused
    here so a C client (libpq, ODBC) cannot time out on them. Names are
    resolved; an unresolved name is left to :func:`pinned_egress`, which
    fail-closes at connect.

    ``point`` names a newer enforcement site: it audits instead of refusing a
    private or loopback target unless ``DATAFLOW_EGRESS_ENFORCE_NEW`` is set
    (:func:`new_point_refusal`). Without ``point`` the site is one production
    already enforced and always refuses.
    """
    workspace_id = workspace_id or current_egress_workspace()
    if point:
        refusal = new_point_refusal(
            point,
            *values,
            allowlist=allowlist,
            workspace_id=workspace_id,
            db_type=db_type,
            connector=connector,
        )
    else:
        refusal = metadata_endpoint_refusal(
            *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type
        )
    if refusal:
        raise EgressRefused(refusal)


# Engines whose ``host`` field is a label, not a dial target: the connection
# opens a file path, so a stored "localhost" must not read as a loopback dial
# (D4 — the same exemption ``connectors.generic_sql._build_engine`` applies).
FILE_ENGINE_TYPES = frozenset({
    "duckdb",
    "sqlite",
    "file",
    "local",
    "csv",
    "csv_local",
    "json",
    "parquet",
    "excel",
    "xlsx",
})


def iter_refusals(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
) -> Iterator[tuple[str, str]]:
    """Yield ``(host, phrase)`` for every dial target in ``values`` the policy refuses.

    Literal private, loopback, link-local, CGNAT, and metadata addresses are
    refused. A name is resolved; any private answer refuses it. An unresolved
    name is not refused here — ``pinned_egress`` resolves again at dial time
    and fail-closes, which is what stops a rebind onto a metadata address.
    Pass ``allowlist=()`` to ignore the workspace list and the process env.
    ``db_type`` of a file engine exempts the host fields entirely — those
    engines never open a socket.
    """
    if (db_type or "").strip().lower() in FILE_ENGINE_TYPES:
        return
    for host in _hosts_from_values(*values):
        phrase = classify_host(
            host,
            allowlist=allowlist,
            workspace_id=workspace_id,
            fail_closed_dns=False,
        )
        if phrase:
            yield host, phrase


def metadata_endpoint_refusal(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
) -> str:
    """Why a connector host is refused before any socket, or ``""``.

    This is the rule production sites enforce unconditionally; see
    :func:`iter_refusals` for what it covers and :func:`new_point_refusal` for
    the audit-first variant the newer sites use.
    """
    for host, phrase in iter_refusals(
        *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type
    ):
        return _refusal_text(host, phrase)
    return ""


def new_point_refusal(
    point: str,
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
    connector: str = "",
) -> str:
    """Refusal text at a newer enforcement site, or ``""`` to allow.

    Same rule as :func:`metadata_endpoint_refusal`, but a private, loopback or
    CGNAT target is only audited (``egress.would_block``) unless
    ``DATAFLOW_EGRESS_ENFORCE_NEW`` is set. Metadata and link-local always refuse.
    """
    for host, phrase in iter_refusals(
        *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type
    ):
        if _refuse_at_new_point(
            point, host, phrase, workspace_id=workspace_id, connector=connector
        ):
            return _refusal_text(host, phrase)
    return ""


_SAVE_TARGET_KEYS = ("host", "endpoint_url", "connection_string")


def connector_save_refusal(data: dict[str, Any], existing: Any = None) -> str:
    """Refusal text for saving ``data`` as a connector, or ``""`` to allow.

    A save that leaves the dial targets exactly as ``existing`` already holds
    them (a rename, a credential change) is never refused for a private or
    loopback host, flag or no flag: only metadata and link-local still are. A
    new or changed target is audited, or refused under the enforce flag.
    """
    values = [str(data.get(key) or "") for key in _SAVE_TARGET_KEYS]
    workspace_id = str(data.get("workspace_id") or "")
    db_type = str(data.get("type") or "")
    if existing is not None and values == [
        str(getattr(existing, key, "") or "") for key in _SAVE_TARGET_KEYS
    ]:
        for host, phrase in iter_refusals(
            *values, workspace_id=workspace_id, db_type=db_type
        ):
            if is_always_blocked_target(host):
                return _refusal_text(host, phrase)
        return ""
    return new_point_refusal(
        "connector_save",
        *values,
        workspace_id=workspace_id,
        db_type=db_type,
        connector=str(data.get("name") or ""),
    )


def egress_url_allowed(
    url: str,
    *,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
) -> bool:
    """True when ``url`` is http(s) and its host survives the egress policy."""
    parsed = urllib.parse.urlparse((url or "").strip())
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False
    host = parsed.hostname or ""
    return classify_host(host, allowlist=allowlist, workspace_id=workspace_id) is None


def redirect_target_allowed(url: str) -> bool:
    """A redirect may proceed only to a public http(s) host.

    Private, loopback, link-local, and metadata targets are refused even when
    a workspace allowlist would have permitted a direct dial.
    """
    return egress_url_allowed(url, allowlist=())


def egress_allowlist_error(entries: Sequence[str]) -> str:
    """Why an admin list cannot be saved, or ``""`` when every entry is usable."""
    for entry in entries:
        text = str(entry or "").strip()
        if not text:
            continue
        try:
            network = ipaddress.ip_network(text, strict=False)
        except ValueError:
            return f"Egress allowlist entry {text!r} is not an IP or CIDR."
        if network.prefixlen == 0:
            return f"Egress allowlist entry {text} is the whole address space and cannot be saved."
        if _is_non_overridable_ip(network.network_address) and all(
            _is_non_overridable_ip(ip) for ip in (network.network_address, network.broadcast_address)
        ):
            return (
                f"Egress allowlist entry {text} is cloud metadata or link-local "
                "and cannot be allowed."
            )
    return ""


def _authority_host(text: str) -> str:
    """Hostname of a URL, including one nested scheme (``jdbc:postgresql://host``).

    An empty result means there is no dial target. ``duckdb:///abs/file`` and
    ``sqlite:///abs/file`` are file engines: the path is not a DNS name.
    """
    if "://" not in text:
        return ""
    parsed = urllib.parse.urlparse(text)
    if parsed.hostname:
        return parsed.hostname
    path = parsed.path or ""
    if "://" in path:
        return urllib.parse.urlparse(path).hostname or ""
    return ""


def _hosts_from_values(*values: str) -> list[str]:
    hosts: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        found: list[str] = []
        if text.startswith("/") and "://" not in text:
            # A filesystem path (sqlite database=/tmp/lab.db). Not a host,
            # even when a directory name looks like an address.
            continue
        if "://" in text:
            parsed = urllib.parse.urlparse(text)
            host = _authority_host(text)
            if host:
                found.append(host)
            elif (parsed.path or "").startswith("/") and "://" not in (parsed.path or ""):
                # Embedded file URL. Do not scan the path for addresses and
                # do not DNS the URL string (IDNA: label empty or too long).
                continue
        found.extend(_KEYWORD_HOST.findall(text))
        found.extend(_EMBEDDED_IPV4.findall(text))
        found.extend(_EMBEDDED_METADATA_NAME.findall(text))
        found.extend(_EMBEDDED_V6_META.findall(text))
        found.extend(_BRACKET_V6.findall(text))
        if not found and "://" not in text and not text.startswith("/"):
            host = text
            if host.startswith("[") and "]" in host:
                host = host[1 : host.index("]")]
            elif host.count(":") == 1:
                left = host.split(":", 1)[0]
                if left and not any(ch.isalpha() for ch in left):
                    host = left
            found.append(host)
        for host in found:
            host = _normalize_host(host)
            if host:
                hosts.append(host)
    return hosts


# (allowlist, workspace_id, point). ``point`` is "" at the sites production
# already enforced and names a newer, audit-first site otherwise.
_DIAL_POLICY: contextvars.ContextVar[tuple[Sequence[str] | None, str, str] | None] = (
    contextvars.ContextVar("egress_dial_policy", default=None)
)
_EGRESS_WORKSPACE: contextvars.ContextVar[str] = contextvars.ContextVar(
    "egress_workspace", default=""
)
_INSTALL: dict[str, Any] = {"count": 0}

# Control-plane stores the API itself dials (job store, CDC leases). A writer
# callback can open a pooled socket to them while a connector scope is active;
# those dials are not connector egress. Connector configs naming these hosts
# are still refused before the dial by ``metadata_endpoint_refusal``.
_CONTROL_PLANE_ENV = (
    "MONGODB_URI",
    "MONGODB_URL",
    "MONGO_URI",
    "MONGO_URL",
    "MONGO_HOST",
    "REDIS_URL",
    "CDC_LEASE_REDIS_URL",
)


@contextmanager
def egress_workspace(workspace_id: str) -> Iterator[None]:
    """Record the workspace whose allowlist governs connector dials below."""
    token = _EGRESS_WORKSPACE.set(str(workspace_id or ""))
    try:
        yield
    finally:
        _EGRESS_WORKSPACE.reset(token)


def current_egress_workspace() -> str:
    """Workspace of the innermost active egress scope, or ``""``."""
    policy = _DIAL_POLICY.get()
    if policy is not None and policy[1]:
        return policy[1]
    return _EGRESS_WORKSPACE.get()


def _control_plane_names() -> frozenset[str]:
    names: set[str] = set()
    for key in _CONTROL_PLANE_ENV:
        raw = (os.environ.get(key) or "").strip()
        if not raw:
            continue
        for host in _hosts_from_values(raw) or [raw]:
            shown = _normalize_host(host)
            if shown:
                names.add(shown)
    return frozenset(names)


def _control_plane_addresses(real_getaddrinfo: Any) -> frozenset[str]:
    names = _control_plane_names()
    cached = _INSTALL.get("control_plane")
    if cached is not None and cached[0] == names:
        return cached[1]
    addresses: set[str] = set(names)
    for name in names:
        try:
            for info in real_getaddrinfo(name, None):
                addresses.add(_normalize_host(str(info[4][0])))
        except (OSError, UnicodeError, IndexError, TypeError):
            continue
    result = frozenset(addresses)
    _INSTALL["control_plane"] = (names, result)
    return result


def _is_control_plane_target(shown: str, real: Any) -> bool:
    """True for a host the operator configured as the API's own store.

    Checked before the ``.internal`` / ``.localhost`` name rule: a platform's
    private network name (``mongo.railway.internal``) is the control plane
    there. Metadata names and addresses are never exempt.
    """
    if shown in _METADATA_NAMES:
        return False
    try:
        if _unwrap(ipaddress.ip_address(shown)) in _METADATA_ADDRESSES:
            return False
    except ValueError:
        pass
    return shown in _control_plane_addresses(real)


def _dial_refusal(
    policy: tuple[Sequence[str] | None, str, str],
    shown: str,
    phrase: str,
    *,
    from_address: bool = True,
) -> str | None:
    """Refusal text for a dial-time hit, or ``None`` when a newer site only audits it.

    ``from_address`` is False for the ``.internal`` / metadata *name* rule,
    which fires before any address is known.
    """
    point = policy[2]
    if point and not _refuse_at_new_point(
        point, shown, phrase, workspace_id=policy[1], from_address=from_address
    ):
        return None
    return _refusal_text(shown, phrase)


def _refusal_for_target(
    host: str, policy: tuple[Sequence[str] | None, str, str], real: Any
) -> str | None:
    allowlist, workspace_id, _point = policy
    shown = _normalize_host(host)
    if not shown or "://" in shown or shown.startswith(("/", "\\")):
        return None
    if _is_control_plane_target(shown, real):
        return None
    if _name_is_non_overridable(shown):
        return _dial_refusal(policy, shown, "cloud metadata and link-local", from_address=False)
    try:
        literal = ipaddress.ip_address(shown)
    except ValueError:
        literal = None
    if literal is not None:
        phrase = _ip_refusal_phrase(literal, _configured_networks(allowlist, workspace_id))
        return _dial_refusal(policy, shown, phrase) if phrase else None
    try:
        infos = real(shown, None)
    except OSError:
        return _dial_refusal(policy, shown, "DNS did not resolve to a public address")
    phrase = _phrase_for_infos(infos, allowlist=allowlist, workspace_id=workspace_id)
    return _dial_refusal(policy, shown, phrase) if phrase else None


def _install_guards() -> None:
    with _PIN_LOCK:
        _INSTALL["count"] += 1
        if _INSTALL["count"] > 1:
            return
        real = socket.getaddrinfo
        real_connect = socket.socket.connect
        real_connect_ex = socket.socket.connect_ex
        _INSTALL.update(real=real, connect=real_connect, connect_ex=real_connect_ex)

        def _sockaddr_host(address: Any) -> str:
            if isinstance(address, tuple) and address:
                return str(address[0] or "")
            if isinstance(address, str):
                return address
            return ""

        def guarded_connect(self, address):
            policy = _DIAL_POLICY.get()
            if policy is not None:
                refusal = _refusal_for_target(_sockaddr_host(address), policy, real)
                if refusal:
                    raise EgressRefused(refusal)
            return real_connect(self, address)

        def guarded_connect_ex(self, address):
            policy = _DIAL_POLICY.get()
            if policy is not None:
                refusal = _refusal_for_target(_sockaddr_host(address), policy, real)
                if refusal:
                    raise EgressRefused(refusal)
            return real_connect_ex(self, address)

        def guarded(host, port, *args, **kwargs):
            policy = _DIAL_POLICY.get()
            if policy is None or host is None or str(host) == "":
                return real(host, port, *args, **kwargs)
            # A file URL is not a dial. Passing it to IDNA raises
            # "label empty or too long"; file engines do not use it as a host.
            raw_host = str(host)
            if "://" in raw_host or raw_host.startswith(("/", "\\")):
                return real(host, port, *args, **kwargs)
            shown = _normalize_host(raw_host)
            if _is_control_plane_target(shown, real):
                return real(host, port, *args, **kwargs)
            if _name_is_non_overridable(shown):
                refusal = _dial_refusal(
                    policy, shown, "cloud metadata and link-local", from_address=False
                )
                if refusal:
                    raise EgressRefused(refusal)
                return real(host, port, *args, **kwargs)
            try:
                literal = ipaddress.ip_address(shown)
            except ValueError:
                literal = None
            allowlist, workspace_id, _point = policy
            if literal is not None:
                phrase = _ip_refusal_phrase(literal, _configured_networks(allowlist, workspace_id))
                refusal = _dial_refusal(policy, shown, phrase) if phrase else None
                if refusal:
                    raise EgressRefused(refusal)
            try:
                infos = real(host, port, *args, **kwargs)
            except OSError as exc:
                refusal = _dial_refusal(policy, shown, "DNS did not resolve to a public address")
                if refusal:
                    raise EgressRefused(refusal) from exc
                raise
            phrase = _phrase_for_infos(infos, allowlist=allowlist, workspace_id=workspace_id)
            refusal = _dial_refusal(policy, shown or raw_host, phrase) if phrase else None
            if refusal:
                raise EgressRefused(refusal)
            return infos

        socket.getaddrinfo = guarded
        socket.socket.connect = guarded_connect
        socket.socket.connect_ex = guarded_connect_ex


def _uninstall_guards() -> None:
    with _PIN_LOCK:
        _INSTALL["count"] -= 1
        if _INSTALL["count"] > 0:
            return
        _INSTALL["count"] = 0
        socket.getaddrinfo = _INSTALL.pop("real")
        socket.socket.connect = _INSTALL.pop("connect")
        socket.socket.connect_ex = _INSTALL.pop("connect_ex")


@contextmanager
def pinned_egress(
    *,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    point: str = "",
) -> Iterator[None]:
    """Re-check every DNS answer and connect target in this context.

    A name that was public at the first check and private on the lookup the
    driver actually uses raises ``EgressRefused`` before that answer is returned
    to the connector. The policy is scoped to the current thread/task through a
    context variable, so concurrent transfers and the API's own control-plane
    dials on other threads are not affected and scopes never serialize.

    ``point`` names a newer, audit-first site (see :func:`new_point_refusal`):
    private and loopback answers are audited and allowed there unless
    ``DATAFLOW_EGRESS_ENFORCE_NEW`` is set; metadata and link-local still refuse.
    """
    policy = (allowlist, str(workspace_id or "") or current_egress_workspace(), point)
    token = _DIAL_POLICY.set(policy)
    _install_guards()
    try:
        yield
    finally:
        _DIAL_POLICY.reset(token)
        _uninstall_guards()


def _cfg_dial_values(cfg: Any, extra: Sequence[str]) -> list[str]:
    values: list[str] = []
    if isinstance(cfg, dict):
        for key in ("host", "endpoint_url", "connection_string", "base_url", "url"):
            raw = cfg.get(key)
            if isinstance(raw, str) and raw.strip():
                values.append(raw)
    values.extend(str(v) for v in extra if isinstance(v, str) and v.strip())
    return values


def _cfg_connector_name(cfg_map: dict[str, Any]) -> str:
    for key in ("connector_name", "name", "connector_id", "id"):
        raw = cfg_map.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def assert_cfg_dial_allowed(cfg: Any, *extra: str, db_type: str = "", point: str) -> None:
    """Check a connector config's dial targets at the newer site ``point``.

    Audit-first: see :func:`new_point_refusal`. Raises ``EgressRefused`` for
    metadata/link-local, and for private targets under the enforce flag.
    """
    cfg_map = cfg if isinstance(cfg, dict) else {}
    assert_connector_dial_allowed(
        *_cfg_dial_values(cfg_map, extra),
        workspace_id=str(cfg_map.get("workspace_id") or ""),
        db_type=str(db_type or cfg_map.get("type") or cfg_map.get("db_type") or ""),
        point=point,
        connector=_cfg_connector_name(cfg_map),
    )


@contextmanager
def connector_dial_scope(
    cfg: Any, *extra: str, db_type: str = "", point: str
) -> Iterator[None]:
    """Pre-dial check plus dial-time DNS pinning around one connector call.

    ``point`` names the newer site; both checks are audit-first for private
    and loopback targets (see :func:`new_point_refusal`).
    """
    cfg_map = cfg if isinstance(cfg, dict) else {}
    assert_cfg_dial_allowed(cfg_map, *extra, db_type=db_type, point=point)
    token = _AUDIT_CONNECTOR.set(_cfg_connector_name(cfg_map))
    try:
        with pinned_egress(workspace_id=str(cfg_map.get("workspace_id") or ""), point=point):
            yield
    finally:
        _AUDIT_CONNECTOR.reset(token)


def follow_url_allowed(url: str, origin: str) -> bool:
    """A server-supplied next/redirect URL may stay on ``origin`` or go public.

    Same scheme+host+port as the configured origin keeps the policy that
    already admitted that origin. Any other target must be a public http(s)
    host regardless of a workspace allowlist. Metadata and link-local targets
    are always refused; a private or loopback target is audited
    (``egress.would_block``) and followed unless ``DATAFLOW_EGRESS_ENFORCE_NEW``
    is set.
    """
    target = urllib.parse.urlparse((url or "").strip())
    if target.scheme not in _ALLOWED_SCHEMES or not target.hostname:
        return False
    base = urllib.parse.urlparse((origin or "").strip())
    if base.scheme in _ALLOWED_SCHEMES and base.hostname:
        try:
            same = (
                target.scheme == base.scheme
                and _normalize_host(target.hostname) == _normalize_host(base.hostname)
                and (target.port or 0) == (base.port or 0)
            )
        except ValueError:
            return False
        if same:
            return True
    phrase = classify_host(target.hostname, allowlist=())
    if phrase is None:
        return True
    return not _refuse_at_new_point("redirect", target.hostname, phrase)


def redirect_guard_hook(origin: str = ""):
    """A ``requests`` response hook that refuses an unsafe redirect target.

    ``requests`` copies hooks onto every redirected request, so the check runs
    before each hop is followed.
    """

    def _hook(resp: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(resp, "is_redirect", False):
            location = str(resp.headers.get("Location") or "")
            target = urllib.parse.urljoin(str(getattr(resp, "url", "") or origin), location)
            if not follow_url_allowed(target, origin or str(getattr(resp, "url", "") or "")):
                raise EgressRefused(
                    _refusal_text(urllib.parse.urlparse(target).hostname or target, "redirect target")
                )
        return resp

    return _hook
