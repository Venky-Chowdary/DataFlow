"""Server-side HTTP egress allowlist — block SSRF to private/link-local hosts.

Notification webhooks, ServiceNow, and generic job-alert URLs are operator-
supplied. Scheme restriction alone is not enough: ``http://169.254.169.254/``
and ``http://127.0.0.1/`` are valid https/http URLs. Resolve the host and refuse
loopback, link-local, RFC1918, metadata, and IPv6 unique-local ranges.
Fail closed on DNS failure so a dangling name cannot be flipped later.
"""

from __future__ import annotations

import ipaddress
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


def egress_url_allowed(url: str) -> bool:
    """True when ``url`` is http(s) to a public hostname/IP."""
    parsed = urllib.parse.urlparse((url or "").strip())
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False
    host = parsed.hostname or ""
    return not host_is_blocked(host)
