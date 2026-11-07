"""Name the network failure a connection test really hit (QA round 12, bug 8).

``humanize_connection_error`` folded every socket-level failure into a few
slogans. "Cannot reach the host on the specified port" covered a refused port,
an unroutable network and a down host alike; a host name that did not resolve
with "Temporary failure in name resolution" fell through to a raw dump; a
server that accepted the TCP connection and then never spoke its protocol (a
MySQL tile on a PostgreSQL port) read "Connection timed out", the same words
as a firewall dropping the SYN; and the two TLS mismatches (client requires,
server requires) read "SSL/TLS error. Try toggling SSL".

Each class below is decided from the driver's own text (psycopg2/libpq,
pymysql, pyodbc, generic sockets) and keeps that text as the ``Driver:`` cause.
"""

from __future__ import annotations

import re
from typing import Callable

# The client asked for TLS; the server does not offer it.
_TLS_REQUIRED_BY_CLIENT = re.compile(
    r"server does not support ssl, but ssl was required|"
    r"ssl is required but the server doesn'?t support it|"
    r"ssl connection is required but the server does not support|"
    r"server doesn'?t support ssl"
)
# The server demands TLS; the client connected in plaintext.
_TLS_REQUIRED_BY_SERVER = re.compile(
    r"insecure transport are prohibited|require_secure_transport|"
    r"no pg_hba\.conf entry for host .*(?:no encryption|ssl off)|"
    r"ssl connection is required|ssl connection required|"
    r"requires? (?:an )?(?:ssl|tls|encrypted) connection|"
    r"server requires encryption"
)
_DNS = re.compile(
    r"name or service not known|nodename nor servname|getaddrinfo|"
    r"temporary failure in name resolution|could not translate host name|"
    r"no address associated with hostname|unknown host|cannot resolve|"
    r"failed to resolve|resolve hostname|errno -[235]\b|\bdns\b|not known"
)
_REFUSED = re.compile(
    r"connection refused|errno 111\b|econnrefused|conn refused|actively refused|"
    r"\b10061\b"
)
_UNREACHABLE = re.compile(
    r"network is unreachable|no route to host|host is down|host is unreachable|"
    r"errno 11[23]\b|errno 101\b|cannot assign requested address|enetunreach|ehostunreach"
)
# TCP connected, then the server never completed its greeting / handshake.
_HANDSHAKE_TIMEOUT = re.compile(
    r"lost connection to (?:mysql )?server (?:during query|at '?reading initial "
    r"communication packet|at '?handshake)[^)]*\(timed out\)|"
    r"reading initial communication packet"
)
_TIMEOUT = re.compile(
    r"timed out|timeout expired|timeout|sockettimeout|connecttimeout"
)

_PG_ENDPOINT = re.compile(r'connection to server at "([^"]+)"(?:\s*\([^)]*\))?,\s*port (\d+)')
_MYSQL_HOST = re.compile(r"server on '([^']+)'")


def _where(raw: str) -> str:
    m = _PG_ENDPOINT.search(raw)
    if m:
        return f" ({m.group(1)}:{m.group(2)})"
    m = _MYSQL_HOST.search(raw)
    if m:
        return f" ({m.group(1)})"
    return ""


def tls_requirement_copy(text: str, raw: str) -> str:
    """Copy for a TLS requirement mismatch, or ``""``. ``text`` is ``raw`` lowercased."""
    if _TLS_REQUIRED_BY_CLIENT.search(text):
        return (
            f"TLS mismatch{_where(raw)}: this connector requires TLS, but the server "
            "does not offer it. Turn SSL off for this connector if the server is "
            "plaintext, or enable TLS on the server."
        )
    if _TLS_REQUIRED_BY_SERVER.search(text):
        return (
            f"TLS mismatch{_where(raw)}: the server requires TLS, but this connector "
            "connected without it. Turn SSL on for this connector."
        )
    return ""


def network_failure_copy(
    text: str, raw: str, *, driver: str = "", dns_hint: Callable[[str], str] | None = None
) -> str:
    """Copy for DNS / refused / unreachable / timeout, or ``""`` when none applies.

    Order matters: a pymysql 2003 carries the socket errno in parentheses, so
    the errno class (DNS, refused, unreachable) is decided before "timed out".
    """
    where = _where(raw)
    if _DNS.search(text):
        if dns_hint is not None:
            hinted = dns_hint(text)
            if hinted:
                return hinted
        return (
            f"DNS lookup failed{where}: the host name did not resolve. Check the "
            "host spelling and that this server can resolve it."
        )
    if _REFUSED.search(text):
        hint = ""
        if driver == "mysql" and re.search(r"\b5432\b", raw):
            hint = " 5432 is PostgreSQL's default port; MySQL listens on 3306 by default."
        return (
            f"Connection refused{where}: the host answered, but nothing accepts "
            "connections on this port. Check the port number and that the database "
            f"is listening on it.{hint}"
        )
    if _UNREACHABLE.search(text):
        return (
            f"Network unreachable{where}: there is no route from this server to the "
            "host. Check the host address, VPN / VPC peering and routing."
        )
    if _HANDSHAKE_TIMEOUT.search(text):
        return (
            f"Handshake timed out{where}: the TCP connection opened, but the server "
            "never completed the database handshake. This port may speak another "
            "protocol, or sit behind a proxy that does not forward it."
        )
    if _TIMEOUT.search(text):
        return (
            f"Connection timed out{where}: no answer before the timeout. A firewall "
            "or security group may be dropping traffic, or the host is down."
        )
    return ""
