"""Server-side HTTP egress allowlist — block SSRF to private/link-local hosts.

Notification webhooks, ServiceNow, and generic job-alert URLs are operator-
supplied. Scheme restriction alone is not enough: ``http://169.254.169.254/``
and ``http://127.0.0.1/`` are valid https/http URLs. Resolve the host and refuse
loopback, link-local, RFC1918, metadata, and IPv6 unique-local ranges.
Fail closed on DNS failure so a dangling name cannot be flipped later.
"""

from __future__ import annotations

import ipaddress
import re
import socket
import urllib.parse

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
)

_BLOCKED_HOSTS = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "metadata.google.internal",
        "metadata.google.com",
        "metadata",
    }
)


def _ip_is_blocked(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    if ip.is_loopback or ip.is_link_local or ip.is_private or ip.is_reserved:
        return True
    if ip.is_multicast or ip.is_unspecified:
        return True
    return any(ip in net for net in _BLOCKED_NETWORKS)


def host_is_blocked(host: str) -> bool:
    """True when ``host`` must not receive server-side HTTP egress."""
    raw = (host or "").strip().lower().rstrip(".")
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    if not raw:
        return True
    if raw in _BLOCKED_HOSTS or raw.endswith(".internal") or raw.endswith(".localhost"):
        return True
    try:
        ip = ipaddress.ip_address(raw)
        return _ip_is_blocked(ip)
    except ValueError:
        pass
    try:
        infos = socket.getaddrinfo(raw, None)
    except OSError:
        return True
    if not infos:
        return True
    for info in infos:
        addr = info[4][0]
        try:
            ip = ipaddress.ip_address(addr)
        except ValueError:
            continue
        if _ip_is_blocked(ip):
            return True
    return False


def metadata_endpoint_refusal(*values: str) -> str:
    """Why a connector host is refused before any socket, or ``""``.

    ``169.254.169.254`` is the cloud metadata address. A probe that dials it
    waits on a blackhole timeout instead of refusing (R8-007). Link-local and
    the metadata hostnames are refused here. RFC1918 stays allowed — customer
    databases live on those networks.
    """
    for host in _hosts_from_values(*values):
        if _is_metadata_host(host):
            return (
                f"Refused connector host {host}: cloud metadata and link-local "
                "addresses are not connection targets."
            )
    return ""


_KEYWORD_HOST = re.compile(
    r"(?i)(?:^|[;\s])(?:host|hostname|server|data\s*source)\s*=\s*"
    r"(\[[^\]]+\]|[^\s;,]+)"
)
_EMBEDDED_LINK_LOCAL = re.compile(r"(?<![0-9])169\.254\.\d{1,3}\.\d{1,3}(?![0-9])")
_EMBEDDED_METADATA_NAME = re.compile(
    r"(?i)(?<![a-z0-9.-])metadata\.google\.(?:internal|com)(?![a-z0-9.-])"
)


def _hosts_from_values(*values: str) -> list[str]:
    hosts: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        found: list[str] = []
        if "://" in text:
            host = urllib.parse.urlparse(text).hostname or ""
            if host:
                found.append(host)
        found.extend(_KEYWORD_HOST.findall(text))
        found.extend(_EMBEDDED_LINK_LOCAL.findall(text))
        found.extend(_EMBEDDED_METADATA_NAME.findall(text))
        if not found:
            host = text
            if host.startswith("[") and "]" in host:
                host = host[1:host.index("]")]
            elif host.count(":") == 1:
                left = host.split(":", 1)[0]
                if left and not any(ch.isalpha() for ch in left):
                    host = left
            found.append(host)
        for host in found:
            host = host.strip().lower().rstrip(".")
            if host.startswith("[") and host.endswith("]"):
                host = host[1:-1]
            if host:
                hosts.append(host)
    return hosts


def _is_metadata_host(host: str) -> bool:
    raw = (host or "").strip().lower().rstrip(".")
    if raw in {"metadata", "metadata.google.internal", "metadata.google.com"}:
        return True
    if raw.startswith("[") and raw.endswith("]"):
        raw = raw[1:-1]
    try:
        ip = ipaddress.ip_address(raw)
    except ValueError:
        return False
    mapped = getattr(ip, "ipv4_mapped", None)
    if mapped is not None:
        ip = mapped
    if ip.is_link_local:
        return True
    return ip in ipaddress.ip_network("169.254.0.0/16")


def egress_url_allowed(url: str) -> bool:
    """True when ``url`` is http(s) to a public hostname/IP."""
    parsed = urllib.parse.urlparse((url or "").strip())
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False
    host = parsed.hostname or ""
    return not host_is_blocked(host)
