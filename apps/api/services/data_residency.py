"""Data residency: a workspace's configured region fences where its data may land.

**Whose region.** The region is read from the tenant record of the workspace a
job runs in (``services.tenant_store``), and from nowhere else. A request body,
a schedule payload or a tool argument can carry a ``data_region`` tag, but it
never moves the fence (audit H-4: naming the destination's own region used to
pass the check).

**When it is enforced.** When the workspace's tenant has a ``data_region``
(any region, ``us-east-1`` included: no region is implicitly allowed), or
when the operator sets ``DATAWRAP_RESIDENCY_STRICT=1``, which enforces the
deployment's ``DATAWRAP_DEFAULT_REGION`` for workspaces that set none.
Otherwise nothing is enforced and every function here answers "no refusal".

**What is judged: the place the driver sends the data** (security review H1).
The destination is resolved the way the engine resolves it (saved connector,
``extra`` overrides, dial-host reconciliation) and then:

=====================================  =====================================================
Destination                            What proves its region
=====================================  =====================================================
file export, local file, SQLite,       ``DATAWRAP_DEFAULT_REGION`` (the operator's statement
DuckDB; a server engine on loopback    of where this deployment runs). Nothing else.
S3 / DynamoDB, AWS endpoint            exactly ``connectors.aws_common.resolve_region(cfg)``,
                                       the region boto3 is given; when an endpoint URL is
                                       dialled, the region in that AWS hostname, which must
                                       agree with a ``host`` that names a region.
any host under a known provider        the label next to the provider suffix
suffix                                 (``<x>.<region>.rds.amazonaws.com``,
                                       ``s3.<region>.amazonaws.com``,
                                       ``<acct>.<region>[.<cloud>].snowflakecomputing.com``,
                                       ``<x>.<region>.<cloud>.confluent.cloud`` …). Every
                                       address field present (``host``, ``endpoint_url``,
                                       each host of the connection string and its
                                       ``host=`` parameters) must prove the same region.
anything else: an IP address, a        a region a **platform administrator** verified for the
custom domain, an S3-compatible        saved connector (``region_verification``, stamped by
endpoint, a bucket or project named    ``POST /connectors/saved/{id}/region-verification``),
without a host (GCS, BigQuery, ADLS),  and only while the connector still points where it
a SaaS API                             did when it was verified.
=====================================  =====================================================

A ``region`` / ``extra.region`` / ``extra.location`` typed on the destination is
**not** evidence: no driver sends data by it, and a workspace editor writes it.
It is read only to refuse a destination whose declared region contradicts its
verified one. A region-shaped label anywhere else in a hostname
(``db.eu-west-1.attacker.example``, the first label of
``eu-west-1.x.us-east-1.rds.amazonaws.com``) proves nothing.

A destination whose region none of these establishes is **refused** while a
region is enforced: an unknown location is not an in-region one.

**Where** (security review H2/H3).

* Every tracked run: ``src.transfer.job_scope.start_refusal``, called at the
  top of the engine's ``execute_tracked`` on the resolved request. A job is
  created once and run many times (resume, retry, a worker claim, the orphan
  sweep); each run is judged on the address it dials now.
* Job creation: :func:`enforce_request_residency` in
  ``src.transfer.job_submission.create_pending_job`` (REST, schedules,
  sync-now, retries, MCP, the Pilot), so nothing out of region is queued.
* The REST routes call :func:`residency_refusal` first so they answer 403
  before anything is queued; ``POST /query/export`` (which writes without a
  job) calls it for its connector and its local-file destination alike.

A tenant record that belongs to no workspace fences every workspace that has
no record of its own, on every one of these paths.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

from services.brand_env import getenv_brand

__all__ = [
    "DestinationRegion",
    "ResidencyRefused",
    "destination_region",
    "provider_region",
    "region_verification_address",
    "enforce_request_residency",
    "enforced_region",
    "job_data_region",
    "addressing_refusal_for",
    "residency_refusal",
]


class ResidencyRefused(ValueError):
    """A destination outside (or not provably inside) the workspace's region."""


@dataclass(frozen=True)
class DestinationRegion:
    """Where a destination is, and what says so (``region == ""``: unknown)."""

    region: str
    evidence: str


_UNKNOWN = DestinationRegion("", "")
#: ``enforced_region`` when tenant records with no workspace name different
#: regions: a fence is configured, and no destination is provably inside it.
_CONFLICTING_REGIONS = "(conflicting tenant regions)"

# us-east-1, eu-central-2, ap-southeast-3, us-gov-west-1, cn-north-1 …
_AWS_REGION = re.compile(
    r"(?:us|eu|ap|sa|ca|me|af|il|mx|cn)(?:-gov|-iso[a-z]?)?"
    r"-(?:north|south|east|west|central|northeast|northwest|southeast|southwest)-\d"
)
# A region-shaped label of another cloud: ``west-europe``, ``europe-west4``, ``us-central1``.
_REGION_SHAPED = re.compile(r"[a-z]{2,}[a-z0-9]*(?:-[a-z0-9]+)+|[a-z]+-?[a-z]+\d")
_CLOUDS = frozenset({"aws", "gcp", "azure"})
#: AWS service labels that sit between the region and ``amazonaws.com``
#: (``<name>.<id>.<region>.rds.amazonaws.com``).
_AWS_SERVICE_AFTER_REGION = frozenset(
    {"rds", "redshift", "redshift-serverless", "es", "docdb", "aoss", "neptune", "memorydb", "elb"}
)
#: Provider suffix -> where its hostnames carry the region (:func:`provider_region`).
_PROVIDER_SUFFIXES: tuple[tuple[str, str], ...] = (
    (".amazonaws.com.cn", "aws"),
    (".amazonaws.com", "aws"),
    (".snowflakecomputing.com", "snowflake"),
    (".confluent.cloud", "region.cloud"),
    (".clickhouse.cloud", "region.cloud"),
    (".found.io", "region.cloud"),
    (".cloud.es.io", "region.cloud"),
    (".elastic-cloud.com", "region.cloud"),
    (".neon.tech", "region.cloud"),
    (".pinecone.io", "region-cloud"),
    (".wasabisys.com", "region"),
    (".oraclecloud.com", "region"),
)
#: Drivers the AWS helpers dial (``connectors.aws_common``).
_AWS_NATIVE = frozenset({"s3", "dynamodb"})
#: Destinations written on this deployment's own disk (with
#: ``local_file_policy.local_disk_destination``, which knows a local DuckDB or
#: Iceberg warehouse from a remote one).
_LOCAL_KINDS = frozenset({"file_export", "file"})
_LOCAL_FORMATS = frozenset({"sqlite", "sqlite3"})
#: Engines that open a network connection to ``host``. For these a loopback
#: host is this deployment. For every other destination (a cloud or SaaS API,
#: an account locator) ``host`` is not dialled, and endpoint resolution fills
#: an empty one with ``localhost``: there a loopback host says nothing.
_HOST_DIALLED_DRIVERS = frozenset({
    "postgresql", "pgvector", "mysql", "sqlserver", "oracle", "mongodb", "redis", "kafka",
    "elasticsearch", "neo4j", "influxdb", "couchbase", "milvus", "qdrant", "weaviate", "sftp",
})
_HOST_DIALLED_FORMATS = frozenset({
    "clickhouse", "trino", "presto", "db2", "vertica", "teradata", "greenplum", "hive", "impala",
    "sybase", "informix", "singlestore", "mariadb", "cockroachdb", "tidb", "yugabytedb", "opensearch",
})
_HOSTNAME = re.compile(r"(?=.{1,253}$)[a-z0-9_]([a-z0-9_-]*[a-z0-9_])?(?:\.[a-z0-9_]([a-z0-9_-]*[a-z0-9_])?)*")
#: ``key=value`` connection strings (libpq, ODBC): the keys that name the server.
_DSN_HOST_KEY = re.compile(
    r"(?i)(?:^|[;\s])(?:host|hostaddr|server|data source|address|addr|network address|account)\s*=\s*([^;\s]+)"
)
#: URL query parameters that replace or add to the authority's host (libpq ``host=``).
_URL_HOST_PARAMS = ("host", "hostaddr", "server", "account")
#: URL query parameters that carry an address in another form (security review
#: round 2, H2): an ODBC connection string, a TNS descriptor / EZConnect / alias,
#: a libpq service-file name, an ODBC driver name (keywords injected into it
#: reach the connect string).
_URL_ADDRESS_PARAMS = ("odbc_connect", "dsn", "service", "driver")
#: ``(HOST=...)`` in an Oracle TNS descriptor.
_DESCRIPTOR_HOST = re.compile(r"(?i)\(\s*host\s*=\s*([^()\s]+)\s*\)")
_EZCONNECT = re.compile(r"(?://)?(?P<host>\[[^\]]+\]|[^:/\s]+)(?::\d+)?(?:/[^\s]*)?")
_PLAIN_DRIVER_NAME = re.compile(r"[A-Za-z0-9 ._+/()-]{1,128}")
#: Drivers whose URL (in ``host`` or the connection string) ``resolve_sql_endpoint``
#: resolves: the family it takes and its default port.
_SQL_URL_FAMILIES = {"postgresql": ("postgresql", 5432), "pgvector": ("postgresql", 5432), "mysql": ("mysql", 3306)}


def _norm(region: Any) -> str:
    return str(region or "").strip().lower()


def _strict() -> bool:
    return (getenv_brand("RESIDENCY_STRICT", "") or "").strip().lower() in ("1", "true", "yes", "on")


def enforced_region(workspace_id: str | None, *, host_tenant_id: str = "") -> str:
    """The region enforced for ``workspace_id``; ``""`` when residency is off.

    ``host_tenant_id`` is the tenant the request's custom domain resolved to
    (``TenantMiddleware``); it speaks for a request whose workspace has no
    tenant record of its own (a single-tenant deployment on its own domain).
    """
    from services.tenant_store import default_region, get_tenant, get_tenant_for_workspace, list_tenants

    tenant = get_tenant_for_workspace(str(workspace_id or "").strip())
    if tenant is None and host_tenant_id:
        candidate = get_tenant(host_tenant_id)
        if candidate is not None and (
            not workspace_id or not candidate.workspace_id or candidate.workspace_id == workspace_id
        ):
            tenant = candidate
    configured = _norm(tenant.data_region) if tenant is not None else ""
    if configured:
        return configured
    # Security review H3: a tenant record that belongs to no workspace (a
    # single-tenant deployment set up before tenants were workspace-scoped)
    # speaks for every workspace with no region of its own. Only a route knows
    # a request's host tenant; the engine, a schedule and a worker do not, and
    # must reach the same answer. Round 2 (M1): a workspace record with an
    # empty region inherits it too, so creating one never escapes the fence.
    unscoped = sorted({_norm(t.data_region) for t in list_tenants() if not t.workspace_id} - {""})
    if len(unscoped) == 1:
        return unscoped[0]
    if unscoped:
        return _CONFLICTING_REGIONS
    return _norm(default_region()) if _strict() else ""


def any_fence() -> bool:
    """Whether any workspace on this deployment has a data region enforced.

    A write that cannot name its workspace is refused while this is true
    (security review round 3, H4): with no workspace there is no fence to
    judge it against, and "no fence" must never be the answer by default.
    """
    from services.tenant_store import default_region, list_tenants

    if any(_norm(t.data_region) for t in list_tenants()):
        return True
    return bool(_strict() and _norm(default_region()))


def job_data_region(workspace_id: str | None, requested: str = "", *, host_tenant_id: str = "") -> str:
    """The ``data_region`` tag a job carries.

    The workspace's enforced region when there is one; the caller's tag (a
    label only) otherwise, defaulting to ``us-east-1`` as before.
    """
    enforced = enforced_region(workspace_id, host_tenant_id=host_tenant_id)
    if enforced and enforced != _CONFLICTING_REGIONS:
        return enforced
    return _norm(requested) or "us-east-1"


# --------------------------------------------------------------------------- addresses


def _value(endpoint: Any, name: str) -> Any:
    """A field as the driver reads it: ``extra`` overrides the endpoint's own
    (``resolve_connector_config`` does ``cfg.update(endpoint.extra)``)."""
    extra = getattr(endpoint, "extra", None)
    if isinstance(extra, dict) and name in extra:
        return extra[name]
    return getattr(endpoint, name, "")


def _hostname(text: str) -> str | None:
    """A plain ``host[:port]``, lower-cased; ``None`` for anything else.

    Strict on purpose: a value that is not obviously one hostname (userinfo, a
    path, a non-ASCII dot) is unreadable, and an unreadable address proves
    nothing.
    """
    host = str(text or "").strip().lower()
    if host.startswith("["):  # [::1] or [::1]:5432
        inner, bracket, port = host[1:].partition("]")
        if not bracket or (port and not port.removeprefix(":").isdigit()):
            return None
        return inner if _is_ip(inner) else None
    if host.count(":") == 1:
        host, port = host.split(":")
        if not port.isdigit():
            return None
    elif ":" in host:
        return host if _is_ip(host) else None
    if host.endswith("."):
        host = host[:-1]
    return host if _HOSTNAME.fullmatch(host) else None


def _is_ip(text: str) -> bool:
    import ipaddress

    try:
        ipaddress.ip_address(text)
    except ValueError:
        return False
    return True


def _is_loopback(host: str) -> bool:
    import ipaddress

    from services.dial_target import is_loopback_host

    if is_loopback_host(host):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _url_hosts(text: str) -> list[str] | None:
    """Every host a URL names: its authority (each member of a seed list) and
    any ``host=`` style query parameter. ``None`` when one cannot be read.

    Read by ``urlparse`` and by the drivers' own last-``@`` parser
    (``parse_url_authority``); a URL they read differently, or with an ``@``
    after the authority (``//h.eu/db@52.1.2.3/db``: urlparse ends the host at
    the first ``/``, the driver at the last ``@``), is unreadable (security
    review round 2, H1).
    """
    from connectors.url_authority import parse_url_authority
    from services.residency_addressing import url_query_pairs

    after = text.split("://", 1)[-1]
    if "@" in after[len(re.split(r"[/?#]", after, maxsplit=1)[0]):]:
        return None
    try:
        parsed = urlparse(text)
        dialled = parse_url_authority(text).host
    except ValueError:
        return None
    names: list[str | None] = []
    # ``user:pw@h1:27017,h2:27017`` — every member of a seed list.
    authority = parsed.netloc.rsplit("@", 1)[-1]
    for part in authority.split(","):
        if part.strip():
            names.append(_hostname(part))
    if authority and "," not in authority and _hostname(dialled) not in names:
        return None
    # The query as SQLAlchemy reads it: a ``#`` is part of it (security follow-up 7).
    for key, value in url_query_pairs(text):
        if key.lower() in _URL_HOST_PARAMS:
            names.extend(_hostname(part) for part in value.split(",") if part.strip())
        elif key.lower() in _URL_ADDRESS_PARAMS:
            found = _address_param_hosts(key.lower(), value)
            names.extend(found if found is not None else [None])
    if not names or any(name is None for name in names):
        return None
    return [name for name in names if name is not None]


def _descriptor_hosts(text: str) -> list[str] | None:
    """Every ``(HOST=...)`` of an Oracle TNS descriptor; ``None`` when one is unreadable."""
    names = [_hostname(value) for value in _DESCRIPTOR_HOST.findall(text)]
    if not names or any(name is None for name in names):
        return None
    return [name for name in names if name is not None]


def _address_param_hosts(key: str, value: str) -> list[str] | None:
    """The hosts an address-bearing URL parameter names: ``[]`` for a plain
    ODBC driver name, ``None`` when the value cannot be read."""
    text = str(value or "").strip()
    if key == "driver":
        return [] if _PLAIN_DRIVER_NAME.fullmatch(text.strip("{}")) else None
    if key == "odbc_connect":
        return _field_hosts(text)
    if key == "dsn":
        if text.startswith("("):
            return _descriptor_hosts(text)
        match = _EZCONNECT.fullmatch(text)
        if match and ("/" in text or ":" in text):
            host = _hostname(match.group("host").strip("[]"))
            return [host] if host else None
    return None  # an ODBC / TNS alias or a libpq service name: resolved from a file


def _field_hosts(raw: Any) -> list[str] | None:
    """The hosts one address field names; ``None`` when it cannot be read."""
    text = str(raw or "").strip()
    if text.lower().startswith("jdbc:"):
        text = text[5:]
    if "://" in text:
        return _url_hosts(text)
    if text.startswith("("):
        return _descriptor_hosts(text)
    keyed = _DSN_HOST_KEY.findall(text) if "=" in text else []
    if keyed:
        parts = [value.strip().removeprefix("tcp:").split(",", 1)[0] for value in keyed]
    elif "=" in text:
        return None
    else:
        parts = [part for part in text.split(",") if part.strip()]  # Kafka bootstrap list
    names = [_hostname(part) for part in parts]
    if not names or any(name is None for name in names):
        return None
    return [name for name in names if name is not None]


def _sql_driver_host(endpoint: Any) -> str:
    """The host ``resolve_sql_endpoint`` dials when ``host`` or the connection
    string holds a PostgreSQL / MySQL URL; ``""`` otherwise (H1)."""
    family = _SQL_URL_FAMILIES.get(_driver(_norm(getattr(endpoint, "format", ""))))
    if family is None:
        return ""
    from connectors.sql_dsn import looks_like_sql_url, resolve_sql_endpoint

    host = str(_value(endpoint, "host") or "")
    dsn = str(_value(endpoint, "connection_string") or "")
    if not (looks_like_sql_url(host, family=family[0]) or looks_like_sql_url(dsn, family=family[0])):
        return ""
    resolved = resolve_sql_endpoint(
        family=family[0], host=host, port=0, connection_string=dsn, default_port=family[1]
    )
    return str(resolved.get("host") or "")


def _addresses(endpoint: Any) -> tuple[list[str], bool]:
    """``(hosts, unreadable)`` over every address field that is set, plus the
    host the SQL driver's own resolver dials."""
    hosts: list[str] = []
    unreadable = False
    for name in ("host", "endpoint_url", "connection_string"):
        raw = _value(endpoint, name)
        if not str(raw or "").strip():
            continue
        found = _field_hosts(raw)
        if found is None:
            unreadable = True
        else:
            hosts.extend(found)
    dialled = _sql_driver_host(endpoint)
    if dialled:
        name = _hostname(dialled)
        unreadable = unreadable or name is None
        hosts.extend([name] if name else [])
    # Every allowlisted address key of the family, judged the way its driver
    # resolves it (review round 3, H1/H2: services.residency_addressing).
    from services.residency_addressing import address_hosts

    keyed, refused, _bucket = address_hosts(endpoint)
    return hosts + keyed, unreadable or refused


def _region_before_cloud(labels: list[str]) -> str:
    """``<name>.<region>.<cloud>``: the label before the cloud qualifier."""
    if len(labels) >= 3 and labels[-1] in _CLOUDS and _REGION_SHAPED.fullmatch(labels[-2]):
        return labels[-2]
    return ""


def provider_region(host: str) -> str:
    """The region a hostname proves, or ``""``.

    Only under a provider suffix this module knows, and only from the label
    that provider puts next to the suffix; never from an arbitrary label.
    """
    name = str(host or "").strip().lower().rstrip(".")
    if not _HOSTNAME.fullmatch(name):
        return ""
    for suffix, layout in _PROVIDER_SUFFIXES:
        if not name.endswith(suffix):
            continue
        labels = name[: -len(suffix)].split(".")
        last = labels[-1]
        if layout == "aws":
            if _AWS_REGION.fullmatch(last):
                return last  # s3.<region>, <bucket>.s3.<region>, dynamodb.<region>
            if last in _AWS_SERVICE_AFTER_REGION and len(labels) >= 2 and _AWS_REGION.fullmatch(labels[-2]):
                return labels[-2]
            return ""  # the global ``s3.amazonaws.com`` goes wherever the bucket lives
        if layout == "snowflake":
            if last == "privatelink":
                labels = labels[:-1]
                last = labels[-1] if labels else ""
            if len(labels) >= 2 and _AWS_REGION.fullmatch(last):
                return last  # <account>.<region>: the legacy AWS form
            return _region_before_cloud(labels)
        if layout == "region.cloud":
            return _region_before_cloud(labels)
        if layout == "region-cloud":  # <name>.svc.<region>-<cloud>
            region, _, cloud = last.rpartition("-")
            return region if cloud in _CLOUDS and len(labels) >= 2 and _REGION_SHAPED.fullmatch(region) else ""
        if layout == "region":
            return last if len(labels) >= 2 and _REGION_SHAPED.fullmatch(last) else ""
    return ""


def _driver(fmt: str) -> str:
    try:
        from src.transfer.connector_capabilities import resolve_driver_type

        return _norm(resolve_driver_type(fmt))
    except Exception:  # noqa: BLE001 - an unknown format is judged as itself
        return fmt


def _dials_host(fmt: str) -> bool:
    return fmt in _HOST_DIALLED_FORMATS or _driver(fmt) in _HOST_DIALLED_DRIVERS


def _snowflake_host(name: str) -> str:
    """The host the Snowflake driver dials for an account identifier."""
    from connectors.snowflake_conn import normalize_account

    account = normalize_account(name).lower()
    return account if _is_loopback(account) or not account else f"{account}.snowflakecomputing.com"


def _place(target: Any) -> tuple[str, ...]:
    """Where a connector or resolved endpoint sends data, for comparing the two.

    Its remote hosts when it names any (a port-only change is the same server,
    as for a host-key pin: ``connector_store._endpoint_moved``); otherwise the
    bucket / project / account it names.
    """
    hosts, _unreadable = _addresses(target)
    remote = sorted({host for host in hosts if not _is_loopback(host)})
    if remote:
        return ("hosts", *remote)
    return ("name", _norm(getattr(target, "database", "")))


def region_verification_address(connector: Any) -> str:
    """What a region verification is bound to: every addressing field (round 2, H4)."""
    from services.region_verification import verification_address

    return verification_address(connector)


def _declared_region(endpoint: Any) -> str:
    extra = getattr(endpoint, "extra", None)
    extra = extra if isinstance(extra, dict) else {}
    for value in (
        getattr(endpoint, "region", ""),
        extra.get("region"),
        extra.get("aws_region"),
        extra.get("location"),
    ):
        if _norm(value):
            return _norm(value)
    return ""


def _verified_region(endpoint: Any, workspace_id: str | None) -> DestinationRegion:
    """The region a platform administrator verified for the endpoint's saved connector.

    Read from the connector store by id, in the job's workspace: nothing a
    request carries can supply it. Honoured only while the connector's address
    is the one that was verified, and only for a run that dials that address.
    """
    connector_id = str(getattr(endpoint, "connector_id", "") or "").strip()
    if not connector_id:
        return _UNKNOWN
    try:
        from services.connector_store import get_connector

        connector = get_connector(connector_id, workspace_id=str(workspace_id or "") or None)
    except Exception:  # noqa: BLE001 - an unreadable store verifies nothing
        return _UNKNOWN
    owner = str(getattr(connector, "workspace_id", "") or "")
    if connector is None or (workspace_id and owner != str(workspace_id)):
        return _UNKNOWN
    stamp = getattr(connector, "region_verification", None) or {}
    region = _norm(stamp.get("region"))
    if not region or not stamp.get("by"):
        return _UNKNOWN
    from services.region_verification import address_unreadable, overridden_by_request

    try:
        moved = (
            stamp.get("address") != region_verification_address(connector)
            or _place(endpoint) != _place(connector)
            or address_unreadable(connector)
            or address_unreadable(endpoint)
            or overridden_by_request(endpoint, connector, workspace_id)
        )
    except Exception:  # noqa: BLE001 - a stamp that cannot be checked verifies nothing
        return _UNKNOWN
    if moved:
        return _UNKNOWN
    declared = _declared_region(endpoint)
    if declared and declared != region:
        return _UNKNOWN
    return DestinationRegion(region, "the region a platform administrator verified for this connector")


def _deployment_region() -> DestinationRegion:
    configured = _norm(getenv_brand("DEFAULT_REGION", ""))
    return DestinationRegion(configured, "this deployment's region") if configured else _UNKNOWN


def _aws_native_region(endpoint: Any, workspace_id: str | None) -> DestinationRegion:
    """S3 / DynamoDB: the endpoint and region ``connectors.aws_common`` gives boto3."""
    from connectors.aws_common import resolve_endpoint_url, resolve_region

    cfg = {name: _value(endpoint, name) for name in ("host", "port", "endpoint_url", "connection_string", "ssl")}
    signing = _norm(resolve_region(cfg))
    url = resolve_endpoint_url(cfg)
    if not url:
        # No endpoint override: boto3 dials the AWS endpoint of the region it is given.
        if _AWS_REGION.fullmatch(signing):
            return DestinationRegion(signing, "the region its driver connects to")
        return _UNKNOWN
    dialled = _hostname(urlparse(url).netloc.rsplit("@", 1)[-1]) or ""
    if dialled and _is_loopback(dialled):
        return _deployment_region()
    proven = provider_region(dialled)
    if not proven:
        return _verified_region(endpoint, workspace_id)  # an S3-compatible store, or the global endpoint
    host_field = str(cfg.get("host") or "").strip()
    if host_field and not _is_loopback(host_field.lower()) and signing != proven:
        return _UNKNOWN  # ``host`` names one region, the endpoint dialled another
    return DestinationRegion(proven, "its endpoint address")


def _on_this_deployments_disk(endpoint: Any, fmt: str) -> bool:
    if _norm(getattr(endpoint, "kind", "")) in _LOCAL_KINDS or fmt in _LOCAL_FORMATS:
        return True
    from services.local_file_policy import local_disk_destination

    return local_disk_destination(endpoint) is not None


def _judged(endpoint: Any, workspace_id: str | None) -> DestinationRegion:
    fmt = _norm(getattr(endpoint, "format", ""))
    if _on_this_deployments_disk(endpoint, fmt):
        return _deployment_region()
    if _driver(fmt) in _AWS_NATIVE:
        return _aws_native_region(endpoint, workspace_id)
    from services.residency_addressing import address_hosts

    if address_hosts(endpoint)[2]:
        # Data lands in a bucket no address places (an Iceberg warehouse).
        return _verified_region(endpoint, workspace_id)
    hosts, unreadable = _addresses(endpoint)
    if _driver(fmt) == "snowflake":
        hosts = [_snowflake_host(host) for host in hosts]
        hosts = [host for host in hosts if host]
    local = [host for host in hosts if _is_loopback(host)]
    if not _dials_host(fmt):
        hosts = [host for host in hosts if host not in local]  # a placeholder, not an address
    elif local:
        # A server on this deployment; a loopback next to a remote address proves nothing.
        return _deployment_region() if len(local) == len(hosts) and not unreadable else _UNKNOWN
    regions = {provider_region(host) for host in hosts}
    if hosts and not unreadable and len(regions) == 1 and "" not in regions:
        return DestinationRegion(regions.pop(), "its endpoint address")
    if regions - {""}:
        return _UNKNOWN  # some address names a region; not every address proves the same one
    return _verified_region(endpoint, workspace_id)


def _resolved(endpoint: Any, workspace_id: str | None) -> Any:
    """The endpoint as the engine will dial it.

    Always resolved, saved connector or not: resolution is where ``extra``
    overrides, the dial-host reconciliation and the saved connector's current
    address are applied, and the place judged must be the place dialled.
    """
    if _norm(getattr(endpoint, "kind", "")) in _LOCAL_KINDS:
        return endpoint
    try:
        from src.transfer.adapters import resolve_endpoint

        return resolve_endpoint(endpoint, workspace_id=str(workspace_id or "") or None)
    except Exception:  # noqa: BLE001 - an unresolvable connector is the caller's own error
        return endpoint


def destination_region(endpoint: Any, *, workspace_id: str | None = None) -> DestinationRegion:
    """The destination's region and the evidence for it (see the module docstring).

    ``workspace_id`` is the workspace the job runs in: its saved connectors are
    the only ones whose verified region can speak for the destination.
    """
    return _judged(_resolved(endpoint, workspace_id), workspace_id)


def addressing_refusal_for(
    workspace_id: str | None, endpoint: Any, *, host_tenant_id: str = ""
) -> str | None:
    """In a fenced workspace: a connection option, extra or override key not on
    the destination family's allowlist (``services.residency_addressing``), or
    ``None``. The routes answer it with a 422 naming the key."""
    if not enforced_region(workspace_id, host_tenant_id=host_tenant_id):
        return None
    if _norm(getattr(endpoint, "kind", "")) in _LOCAL_KINDS:
        return None
    from services.residency_addressing import addressing_refusal

    refusal = addressing_refusal(endpoint)  # the request: its fields and its extra overrides
    if refusal:
        return refusal
    saved = None
    connector_id = str(getattr(endpoint, "connector_id", "") or "").strip()
    if connector_id:
        try:
            from services.connector_store import get_connector

            saved = get_connector(connector_id, workspace_id=str(workspace_id or "") or None)
        except Exception:  # noqa: BLE001 - an unreadable store is judged by the resolved endpoint
            saved = None
    if saved is not None:
        refusal = addressing_refusal(saved, extras=True)
        if refusal:
            return refusal
    # The endpoint as the driver will be given it (its URL / DSN keys only:
    # a resolved endpoint's extra also holds the engine's own fields).
    return addressing_refusal(_resolved(endpoint, workspace_id), extras=False)


def residency_refusal(
    workspace_id: str | None, endpoint: Any, *, host_tenant_id: str = ""
) -> str | None:
    """Why this destination may not receive the workspace's data, or ``None``."""
    allowed = enforced_region(workspace_id, host_tenant_id=host_tenant_id)
    if not allowed:
        return None
    keyed = addressing_refusal_for(workspace_id, endpoint, host_tenant_id=host_tenant_id)
    if keyed:
        return keyed
    if allowed == _CONFLICTING_REGIONS:
        return (
            "Data residency violation: this deployment's tenant records with no workspace name "
            "more than one region, so no destination can be shown to be in the right one. "
            "Assign each tenant record to its workspace."
        )
    where = destination_region(endpoint, workspace_id=workspace_id)
    label = str(getattr(endpoint, "format", "") or getattr(endpoint, "kind", "") or "this").strip()
    if not where.region:
        return (
            f"Data residency violation: the region of the {label} destination cannot be "
            f"determined, and this workspace's data must stay in '{allowed}'. Use an endpoint "
            "address that names its provider region, or ask a platform administrator to "
            "verify the saved connector's region."
        )
    if where.region != allowed:
        return (
            f"Data residency violation: destination region '{where.region}' "
            f"(from {where.evidence}) does not match workspace region '{allowed}'."
        )
    return None


def enforce_request_residency(request: Any) -> None:
    """Refuse a transfer whose destination is outside its workspace's region.

    Called with saved connectors already resolved. Also stamps the job's
    ``data_region`` with the enforced region, whatever tag the caller sent.
    """
    workspace_id = str(getattr(request, "workspace_id", "") or "")
    allowed = enforced_region(workspace_id)
    if not allowed:
        return
    refusal = residency_refusal(workspace_id, request.destination)
    if refusal:
        raise ResidencyRefused(refusal)
    request.data_region = allowed
