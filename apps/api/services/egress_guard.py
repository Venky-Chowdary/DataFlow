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
"""

from __future__ import annotations

import ipaddress
import os
import re
import socket
import threading
import urllib.parse
from contextlib import contextmanager
from typing import Iterator, Sequence

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
    return phrase


def _refusal_text(host: str, phrase: str) -> str:
    if phrase.startswith("DNS"):
        return f"Refused connector host {host}: {phrase}."
    return f"Refused connector host {host}: {phrase} addresses are not connection targets."


def host_is_blocked(host: str) -> bool:
    """True when ``host`` is denied with an empty allowlist. DNS failure denies."""
    return classify_host(host, allowlist=(), fail_closed_dns=True) is not None


def metadata_endpoint_refusal(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
) -> str:
    """Why a connector host is refused before any socket, or ``""``.

    Literal private, loopback, link-local, CGNAT, and metadata addresses are
    refused. A name is resolved; any private answer refuses it. An unresolved
    name is not refused here — ``pinned_egress`` resolves again at dial time
    and fail-closes, which is what stops a rebind onto a metadata address.
    Pass ``allowlist=()`` to ignore the workspace list and the process env.
    """
    for host in _hosts_from_values(*values):
        phrase = classify_host(
            host,
            allowlist=allowlist,
            workspace_id=workspace_id,
            fail_closed_dns=False,
        )
        if phrase:
            return _refusal_text(host, phrase)
    return ""


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


@contextmanager
def pinned_egress(
    *,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
) -> Iterator[None]:
    """Re-check every DNS answer on this thread until the dial returns.

    A name that was public at the first check and private on the lookup the
    driver actually uses raises ``EgressRefused`` before that answer is returned
    to the connector.
    """
    with _PIN_LOCK:
        real = socket.getaddrinfo

        def guarded(host, port, *args, **kwargs):
            if host is None or str(host) == "":
                return real(host, port, *args, **kwargs)
            # A file URL is not a dial. Passing it to IDNA raises
            # "label empty or too long" and the probe reports that as the
            # driver error. Let the real resolver answer; file engines do
            # not use this name as a host.
            raw_host = str(host)
            if "://" in raw_host or raw_host.startswith(("/", "\\")):
                return real(host, port, *args, **kwargs)
            shown = _normalize_host(str(host))
            if _name_is_non_overridable(shown):
                raise EgressRefused(_refusal_text(shown, "cloud metadata and link-local"))
            try:
                literal = ipaddress.ip_address(shown)
            except ValueError:
                literal = None
            if literal is not None:
                phrase = _ip_refusal_phrase(literal, _configured_networks(allowlist, workspace_id))
                if phrase:
                    raise EgressRefused(_refusal_text(shown, phrase))
            try:
                infos = real(host, port, *args, **kwargs)
            except OSError as exc:
                raise EgressRefused(
                    _refusal_text(shown, "DNS did not resolve to a public address")
                ) from exc
            phrase = _phrase_for_infos(infos, allowlist=allowlist, workspace_id=workspace_id)
            if phrase:
                raise EgressRefused(_refusal_text(shown or str(host), phrase))
            return infos

        socket.getaddrinfo = guarded
        try:
            yield
        finally:
            socket.getaddrinfo = real
