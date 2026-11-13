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
from typing import Any, Iterator, Mapping, Sequence

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
#: URL query keys whose value the driver dials as the host: libpq ``host`` /
#: ``hostaddr`` (a comma list, replacing the authority) and the ODBC-style
#: ``server`` / ``address`` spellings; the keys residency judges
#: (``data_residency._URL_HOST_PARAMS``) (follow-up review 1, H1).
_QUERY_HOST_KEYS = frozenset({
    "host", "hostaddr", "server", "address", "addr", "data source", "network address",
})
#: A query value that is a whole ODBC connection string (``mssql+pyodbc:///?odbc_connect=``).
_QUERY_CONNECT_STRING_KEYS = frozenset({"odbc_connect"})
#: URL schemes (before ``+driver``) whose driver is libpq: no host anywhere is its default socket.
_LIBPQ_SCHEMES = frozenset({"postgresql", "postgres", "redshift", "cockroachdb"})
#: What the guard judges for libpq's default Unix socket (a URL that names no host).
DEFAULT_SOCKET = "/<default local socket>"
#: What the guard judges for an authority that yields no host once decoded (encoded twice).
UNREADABLE_HOST = "<unreadable host>"
_SOCKET_PHRASE = "local socket"
#: MySQL / MariaDB ``unix_socket`` (URL query key, connector field, ``extra``):
#: PyMySQL opens that socket on this host instead of dialling the URL's host.
_SOCKET_KEYS = frozenset({"unix_socket"})
_KEYWORD_SOCKET = re.compile(r"(?i)(?:^|[;\s])unix_socket\s*=\s*([^\s;]+)")
#: What the guard judges for a socket named by a relative path.
LOCAL_SOCKET = "/<local socket>"
_UNREADABLE_PHRASE = "unreadable host"

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
    raw = getenv_brand("EGRESS_ALLOWLIST", "") or ""
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
    if host == UNREADABLE_HOST:
        return _UNREADABLE_PHRASE
    if is_socket_target(host):
        # A Unix socket on this host: loopback, so the loopback allowlist admits it.
        nets = _configured_networks(allowlist, workspace_id)
        loopback = ipaddress.ip_address("127.0.0.1")
        return None if _ip_refusal_phrase(loopback, nets) is None else _SOCKET_PHRASE
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


def is_socket_target(host: str) -> bool:
    """A Unix-socket path (``/run/postgresql``, libpq's ``@abstract``), not a host name."""
    return str(host or "").startswith(("/", "@"))


def _socket_host(value: str) -> str:
    """A socket path as the guard judges it: the path, or :data:`LOCAL_SOCKET` for a relative one."""
    return value if is_socket_target(value) else LOCAL_SOCKET


def _dials_mysql_sockets(db_type: str) -> bool:
    if not str(db_type or "").strip():
        return False
    from src.transfer.connector_capabilities import resolve_driver_type

    return str(resolve_driver_type(str(db_type).strip()) or "").lower() == "mysql"


def mysql_socket_values(data: Any) -> list[str]:
    """A MySQL-family connector's ``unix_socket`` field and ``extra.unix_socket``, as
    ``unix_socket=`` keyword values the guard judges as a local socket (follow-up review 1).

    Judged at save. At the dial only a URL's ``?unix_socket=`` reaches PyMySQL
    (through SQLAlchemy), and :func:`_query_hosts` judges it there."""
    def get(source: Any, key: str) -> Any:
        return source.get(key) if isinstance(source, Mapping) else getattr(source, key, None)

    if not _dials_mysql_sockets(str(get(data, "type") or get(data, "format") or "")):
        return []
    extra = get(data, "extra")
    values = (get(data, "unix_socket"), extra.get("unix_socket") if isinstance(extra, Mapping) else None)
    return [f"unix_socket={text}" for text in (str(v or "").strip() for v in values) if text]


def _refusal_text(host: str, phrase: str) -> str:
    if phrase == _UNREADABLE_PHRASE:
        return (
            "Refused connector URL: its host cannot be read as one host name (it is "
            "percent-encoded more than once). Write the host name plainly."
        )
    if phrase.startswith("DNS"):
        return f"Refused connector host {host}: {phrase}."
    return (
        f"Refused connector host {host}: {phrase} addresses are not connection "
        "targets. To reach an on-prem or loopback target, allowlist its range "
        "(for example 127.0.0.0/8) in the workspace egress allowlist, or ask the "
        "deployment's operator to allow it."
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
    if is_socket_target(host):
        return [ipaddress.ip_address("127.0.0.1")]  # a socket on this host
    if host == UNREADABLE_HOST:
        return []
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
    """Cloud metadata, link-local and multicast: refused at every site, flag or not.

    So is a URL whose authority cannot be read (it fails closed)."""
    if host == UNREADABLE_HOST or _normalize_host(host) in _METADATA_NAMES:
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
    """True when ``host`` is denied. DNS failure denies.

    Default deny; the shared allowlist (workspace entries, then
    ``DATAFLOW_EGRESS_ALLOWLIST``) admits private/loopback targets the same way
    it does for connector dials. Metadata endpoints are never admitted.
    """
    return classify_host(host, allowlist=None, fail_closed_dns=True) is not None


def assert_connector_dial_allowed(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
    point: str = "",
    connector: str = "",
    aws_endpoint: str | None = None,
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
            aws_endpoint=aws_endpoint,
        )
    else:
        refusal = metadata_endpoint_refusal(
            *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type,
            aws_endpoint=aws_endpoint,
        )
    if refusal:
        raise EgressRefused(refusal)
    from connectors.mysql_options import url_option_refusal

    # A MySQL URL hands PyMySQL its query keys; only the ones the product uses
    # (follow-up review 2, H3 + H4: an option file or LOCAL INFILE on this host).
    mysql_family = _dials_mysql_sockets(db_type)
    for value in values:
        if mysql_family or _is_mysql_url(value):
            refusal = url_option_refusal(value)
            if refusal:
                raise EgressRefused(refusal)


def _is_mysql_url(value: str) -> bool:
    from connectors.mysql_options import is_mysql_url

    return is_mysql_url(value)


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


# AWS SDK connectors keep the signing region in ``host`` (ConnectorModal saves
# ``us-east-1``). The SDK dials ``endpoint_url`` or the region's AWS endpoint,
# so the label is not resolved: each lookup cost ~10 s on a stalled CI resolver
# and timed out a 3 s test (run 43151557376). But ``resolve_endpoint_url`` turns
# a dot-less host plus a service port into ``http://<host>:<port>``: then the
# label IS the dial target (PR 410 review H-E1). So the label is skipped only
# when it is the whole of a value, no URL among the values names it, and the
# caller passes the endpoint the SDK dials (``aws_endpoint``, "" for AWS's own)
# and that endpoint names another host. Only an exact ASCII region spelling, as
# botocore's partitions list them, is a label; ``us-db-01`` is a host name.
AWS_REGION_HOST_TYPES = frozenset({
    "s3",
    "amazon_s3",
    "aws_s3",
    "s3_compatible",
    "dynamodb",
    "amazon_dynamodb",
    "aws_dynamodb",
    "kinesis",
})
_AWS_REGION_LABEL = re.compile(
    r"(?:us|eu|ap|sa|ca|me|af|il|mx|cn|eusc-de)(?:-gov|-iso[a-z]?)?"
    r"-(?:north|south|east|west|central|northeast|northwest|southeast|southwest)-[0-9]{1,2}",
    re.ASCII,
)


def is_aws_region_label(host: str, db_type: str) -> bool:
    """True when ``host`` is spelled as an AWS region and ``db_type`` is an AWS SDK type."""
    kind = (db_type or "").strip().lower()
    return kind in AWS_REGION_HOST_TYPES and _AWS_REGION_LABEL.fullmatch(host or "") is not None


def aws_sdk_endpoint(cfg: Any, db_type: str = "") -> str | None:
    """The endpoint an AWS SDK client built from ``cfg`` dials ("" = AWS's own).

    ``None`` when ``cfg`` is not an AWS SDK connector or the endpoint cannot be
    worked out; the guard then skips no region label.
    """
    if not isinstance(cfg, dict):
        return None
    kind = str(db_type or cfg.get("type") or cfg.get("db_type") or "").strip().lower()
    if kind not in AWS_REGION_HOST_TYPES:
        return None
    from connectors.aws_common import resolve_endpoint_url

    try:
        return resolve_endpoint_url(cfg)
    except (TypeError, ValueError):
        return None


def _skipped_region_labels(
    values: Sequence[str], db_type: str, aws_endpoint: str | None
) -> set[str]:
    """Region labels in ``values`` that no URL and no SDK endpoint dials."""
    if aws_endpoint is None:
        return set()
    dialled = set(_hosts_from_values(aws_endpoint))
    labels: set[str] = set()
    for raw in values:
        hosts = _hosts_from_values(raw)
        if hosts == [_normalize_host(str(raw or ""))]:
            labels.update(hosts)  # the whole value is one bare name
        else:
            dialled.update(hosts)  # a URL or a connection string names it
    return {
        label for label in labels
        if label not in dialled and is_aws_region_label(label, db_type)
    }


def _dials_unix_sockets(db_type: str) -> bool:
    """libpq engines: a ``/``-prefixed ``host`` is a Unix socket on this host."""
    if not str(db_type or "").strip():
        return False
    from src.transfer.connector_capabilities import resolve_driver_type

    return str(resolve_driver_type(str(db_type).strip()) or "").lower() in {"postgresql", "redshift"}


def iter_refusals(
    *values: str,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
    db_type: str = "",
    aws_endpoint: str | None = None,
) -> Iterator[tuple[str, str]]:
    """Yield ``(host, phrase)`` for every dial target in ``values`` the policy refuses.

    Literal private, loopback, link-local, CGNAT, and metadata addresses are
    refused. A name is resolved; any private answer refuses it. An unresolved
    name is not refused here — ``pinned_egress`` resolves again at dial time
    and fail-closes, which is what stops a rebind onto a metadata address.
    Pass ``allowlist=()`` to ignore the workspace list and the process env.
    ``db_type`` of a file engine exempts the host fields entirely — those
    engines never open a socket. For an AWS SDK ``db_type`` with a known
    ``aws_endpoint`` a region label nothing dials is skipped (see
    :func:`aws_sdk_endpoint` and the note above ``AWS_REGION_HOST_TYPES``).
    """
    if (db_type or "").strip().lower() in FILE_ENGINE_TYPES:
        return
    skipped = _skipped_region_labels(values, db_type, aws_endpoint)
    for host in _hosts_from_values(*values, socket_values=_dials_unix_sockets(db_type)):
        if host in skipped:
            continue
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
    aws_endpoint: str | None = None,
) -> str:
    """Why a connector host is refused before any socket, or ``""``.

    This is the rule production sites enforce unconditionally; see
    :func:`iter_refusals` for what it covers and :func:`new_point_refusal` for
    the audit-first variant the newer sites use.
    """
    for host, phrase in iter_refusals(
        *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type,
        aws_endpoint=aws_endpoint,
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
    aws_endpoint: str | None = None,
) -> str:
    """Refusal text at a newer enforcement site, or ``""`` to allow.

    Same rule as :func:`metadata_endpoint_refusal`, but a private, loopback or
    CGNAT target is only audited (``egress.would_block``) unless
    ``DATAFLOW_EGRESS_ENFORCE_NEW`` is set. Metadata and link-local always refuse.
    """
    for host, phrase in iter_refusals(
        *values, allowlist=allowlist, workspace_id=workspace_id, db_type=db_type,
        aws_endpoint=aws_endpoint,
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
    from connectors.aws_common import default_credentials_refusal
    from services.iceberg_class_properties import iceberg_connector_class_refusal
    from services.platform_aws_credentials import iceberg_connector_refusal
    from services.platform_gcp_azure_credentials import iceberg_gcp_azure_connector_refusal

    from connectors.mysql_options import connector_option_refusal
    from connectors.mongodb_auth_mechanism import settings_refusal as mongo_auth_refusal
    from connectors.oracle_dsn_guard import settings_refusal as oracle_dsn_refusal
    from connectors.tls_file_options import connector_refusal as tls_files_refusal
    from services.google_credentials_policy import google_connector_refusal

    platform_credentials = (
        default_credentials_refusal(data)
        or iceberg_connector_class_refusal(data)
        or iceberg_connector_refusal(data)
        or iceberg_gcp_azure_connector_refusal(data)
        # No PyMySQL option that reads a host file or socket (follow-up review 2, H3 + H4).
        or connector_option_refusal(
            data, mysql_family=_dials_mysql_sockets(str(data.get("type") or data.get("format") or ""))
        )
        # BigQuery / GCS: never the host's Google identity or key files (follow-up review 2, BQ).
        or google_connector_refusal(data)
        # No TLS certificate / key / wallet file on this host (follow-up review 2, TLS files).
        or tls_files_refusal(data)
        # No Oracle config provider / LDAP DSN or pyo. wallet (follow-up review 3, M3 + M4).
        or oracle_dsn_refusal(data)
        # No MongoDB auth mechanism that uses this host's identity (follow-up review 3).
        or mongo_auth_refusal(data)
    )
    if platform_credentials:
        return platform_credentials
    values = [str(data.get(key) or "") for key in _SAVE_TARGET_KEYS]
    sockets = mysql_socket_values(data)
    workspace_id = str(data.get("workspace_id") or "")
    db_type = str(data.get("type") or "")
    # The endpoint an S3/DynamoDB client dials (``host`` + ``port`` may be it).
    aws_endpoint = aws_sdk_endpoint(data, db_type)
    unchanged = existing is not None and values == [
        str(getattr(existing, key, "") or "") for key in _SAVE_TARGET_KEYS
    ] and sockets == mysql_socket_values(existing)
    values += sockets
    if unchanged:
        for host, phrase in iter_refusals(
            *values, workspace_id=workspace_id, db_type=db_type, aws_endpoint=aws_endpoint
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
        aws_endpoint=aws_endpoint,
    )


def egress_url_allowed(
    url: str,
    *,
    allowlist: Sequence[str] | None = None,
    workspace_id: str = "",
) -> bool:
    """True when ``url`` is http(s) and its host survives the egress policy."""
    try:
        parsed = urllib.parse.urlparse((url or "").strip())
        host = parsed.hostname or ""
    except ValueError:  # a bracketed name urlparse rejects; HTTP clients read it otherwise
        return False
    if parsed.scheme not in _ALLOWED_SCHEMES:
        return False
    # urllib3 (requests) dials the percent-decoded host (security follow-up 7).
    return all(
        classify_host(name, allowlist=allowlist, workspace_id=workspace_id) is None
        for name in {host, urllib.parse.unquote(host)}
    )


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
    try:
        parsed = urllib.parse.urlparse(text)
        if parsed.hostname:
            return parsed.hostname
        path = parsed.path or ""
        if "://" in path:
            return urllib.parse.urlparse(path).hostname or ""
    except ValueError:  # ``[name]``: judged by :func:`_driver_authority_hosts`
        return ""
    return ""


def _driver_authority_hosts(text: str) -> list[str]:
    """The authority hosts as the drivers read them (security follow-up 7).

    The last-``@`` parse (``connectors.url_authority``), percent-decoded the
    way redis-py, libpq URIs and urllib3 decode it, each seed-list member, a
    bracketed name unwrapped. ``urlparse`` alone judged
    ``%31%36%39.254.169.254`` as nothing (or as an embedded ``39.254.169.254``)
    and raised on ``[metadata.google.internal]``.
    """
    from connectors.url_authority import parse_url_authority

    try:
        authority = parse_url_authority(text).host
    except (TypeError, ValueError):
        return []
    hosts: list[str] = []
    for part in authority.split(","):
        part = part.strip()
        if part.startswith("[") and "]" in part:
            part = part[1 : part.index("]")]
        elif part.count(":") == 1:
            part = part.rsplit(":", 1)[0]
        if part:
            hosts.append(part)
    return hosts


def _query_hosts(text: str) -> list[str]:
    """The hosts a URL's query hands the driver, read as residency reads them.

    ``residency_addressing.url_query_pairs`` keeps a ``#`` inside the query as
    SQLAlchemy does; each value is unquoted and comma-split (libpq's host
    list). An empty ``host=`` is libpq's default socket (follow-up review 1, H1).
    """
    from services.residency_addressing import url_query_pairs

    hosts: list[str] = []
    for key, value in url_query_pairs(text):
        name = key.strip().lower()
        if name in _QUERY_CONNECT_STRING_KEYS:
            hosts.extend(_KEYWORD_HOST.findall(value))
        elif name in _QUERY_HOST_KEYS:
            parts = [part.strip() for part in value.split(",")]
            if name in ("host", "hostaddr") and not any(parts):
                hosts.append(DEFAULT_SOCKET)
            hosts.extend(part for part in parts if part)
        elif name in _SOCKET_KEYS and value.strip():
            hosts.append(_socket_host(value.strip()))
    return hosts


def _url_hosts(text: str) -> tuple[list[str], bool]:
    """``(hosts, file_url)`` for one URL: its authority as urlparse and the drivers
    read it, and its query hosts. ``file_url`` is an embedded file URL
    (``sqlite:///abs/file``) whose path is not scanned."""
    try:
        path = urllib.parse.urlparse(text).path or ""
    except ValueError:
        path = ""
    found: list[str] = []
    host = _authority_host(text)
    if host:
        found.append(host)
    seen = {_normalize_host(name) for name in found}
    found.extend(
        name for name in _driver_authority_hosts(text) if _normalize_host(name) not in seen
    )
    query = _query_hosts(text)
    if found and not any(_normalize_host(name) for name in found):
        found = [UNREADABLE_HOST]  # e.g. ``%2531...``: nothing to judge fails closed
    scheme = text.split("://", 1)[0].split("+", 1)[0].strip().lower()
    if not found and not query and scheme in _LIBPQ_SCHEMES:
        found = [DEFAULT_SOCKET]  # ``postgresql:///db``: libpq dials its default socket
    file_url = not found and not query and path.startswith("/") and "://" not in path
    return found + query, file_url


def _hosts_from_values(*values: str, socket_values: bool = False) -> list[str]:
    """Every host the values name. ``socket_values``: a plain ``/``-prefixed value is a
    Unix socket the driver dials (libpq's ``host``), not a file path."""
    hosts: list[str] = []
    for raw in values:
        text = str(raw or "").strip()
        if not text:
            continue
        found: list[str] = []
        if text.startswith("/") and "://" not in text:
            # A filesystem path (sqlite database=/tmp/lab.db). Not a host,
            # even when a directory name looks like an address.
            if socket_values:
                hosts.append(text)
            continue
        if "://" in text:
            found, file_url = _url_hosts(text)
            if file_url:
                # Embedded file URL. Do not scan the path for addresses and
                # do not DNS the URL string (IDNA: label empty or too long).
                continue
        found.extend(_KEYWORD_HOST.findall(text))
        found.extend(_socket_host(value) for value in _KEYWORD_SOCKET.findall(text))
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
            if host == UNREADABLE_HOST or is_socket_target(host):
                hosts.append(host)
                continue
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
#: Told about each Python socket this context connects (see :func:`observe_dials`).
_DIAL_OBSERVER: contextvars.ContextVar[Any] = contextvars.ContextVar(
    "egress_dial_observer", default=None
)

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

        def _observe(sock) -> None:
            observer = _DIAL_OBSERVER.get()
            if observer is not None:
                observer(sock)

        def guarded_connect(self, address):
            policy = _DIAL_POLICY.get()
            if policy is not None:
                refusal = _refusal_for_target(_sockaddr_host(address), policy, real)
                if refusal:
                    raise EgressRefused(refusal)
            _observe(self)
            return real_connect(self, address)

        def guarded_connect_ex(self, address):
            policy = _DIAL_POLICY.get()
            if policy is not None:
                refusal = _refusal_for_target(_sockaddr_host(address), policy, real)
                if refusal:
                    raise EgressRefused(refusal)
            _observe(self)
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


@contextmanager
def observe_dials(observer: Any) -> Iterator[None]:
    """Call ``observer(sock)`` for every Python socket this context connects.

    A driver handshake has no connection object to interrupt yet: the tool
    deadline uses this to reach the socket of a connect that hangs on a peer
    that accepted and went silent (D281). Scoped like :func:`pinned_egress`.
    Nested scopes all observe: the outer observer is called first (D354: the
    connect-timeout bound runs inside the deadline's interruptible connect).
    """
    outer = _DIAL_OBSERVER.get()

    def chained(sock: Any) -> None:
        outer(sock)
        observer(sock)

    token = _DIAL_OBSERVER.set(observer if outer is None else chained)
    _install_guards()
    try:
        yield
    finally:
        _DIAL_OBSERVER.reset(token)
        _uninstall_guards()


def _cfg_dial_values(cfg: Any, extra: Sequence[str]) -> list[str]:
    values: list[str] = []
    if isinstance(cfg, dict):
        from services.dial_target import reconcile_dial_host

        # Drop a placeholder loopback host when the URI names the real target
        # before the values are classified. A loopback-only config is unchanged.
        reconcile_dial_host(cfg)
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
    values = _cfg_dial_values(cfg_map, extra)
    kind = str(db_type or cfg_map.get("type") or cfg_map.get("db_type") or "")
    assert_connector_dial_allowed(
        *values,
        workspace_id=str(cfg_map.get("workspace_id") or ""),
        db_type=kind,
        point=point,
        connector=_cfg_connector_name(cfg_map),
        aws_endpoint=aws_sdk_endpoint(cfg_map, kind),
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
    from src.ai.copilot.tool_deadline import waiting_on

    try:
        # A tool deadline that passes in here names this endpoint (D187).
        with waiting_on(cfg_map), pinned_egress(
            workspace_id=str(cfg_map.get("workspace_id") or ""), point=point
        ):
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
    try:
        target = urllib.parse.urlparse((url or "").strip())
        if target.scheme not in _ALLOWED_SCHEMES or not target.hostname:
            return False
        base = urllib.parse.urlparse((origin or "").strip())
        base_host = base.hostname
    except ValueError:  # a bracketed name urlparse rejects (security follow-up 7)
        return False
    if target.hostname != urllib.parse.unquote(target.hostname):
        return False  # urllib3 dials the decoded host; never follow an encoded one
    if base.scheme in _ALLOWED_SCHEMES and base_host:
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
                raise EgressRefused(_refusal_text(target, "redirect target"))
        return resp

    return _hook
