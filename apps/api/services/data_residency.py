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

**What is judged.** The destination's region, established in this order:

1. the destination's own address: a hostname label that is a cloud region
   (``s3.eu-west-1.amazonaws.com``, ``db.x.us-west-2.rds.amazonaws.com``,
   ``xy123.us-east-1.snowflakecomputing.com``, ``pkc-1.us-east-1.aws.confluent.cloud``),
   read from ``host``, ``endpoint_url`` and the connection string; for S3 and
   DynamoDB, whose ``host`` field is the region itself, that value;
2. failing that, the region declared on the destination (``region``, or
   ``location`` / ``region`` in its ``extra``): an operator's statement, used
   only when the address shows nothing, never against it;
3. for a destination on this deployment's own disk (a file export, a local
   SQLite / DuckDB file), the deployment region, when the operator has set
   ``DATAWRAP_DEFAULT_REGION``.

A destination whose region none of these establishes is **refused** while a
region is enforced: an unknown location is not an in-region one.

**Where.** :func:`enforce_request_residency` runs in
``src.transfer.job_submission.create_pending_job``, the one place every job is
created (REST, schedules, sync-now, retries, MCP, the Pilot), after saved
connectors are resolved. The REST routes call :func:`residency_refusal` first
so they can answer 403 before anything is queued.
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
    "enforce_request_residency",
    "enforced_region",
    "job_data_region",
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

# us-east-1, eu-central-2, ap-southeast-3, us-gov-west-1, cn-north-1 …
_AWS_REGION = re.compile(
    r"(?:us|eu|ap|sa|ca|me|af|il|mx|cn)(?:-gov|-iso[a-z]?)?"
    r"-(?:north|south|east|west|central|northeast|northwest|southeast|southwest)-\d"
)
# A region-shaped label of another cloud next to its cloud qualifier:
# ``west-europe.azure``, ``europe-west4.gcp``, ``us-central1.gcp``.
_REGION_SHAPED = re.compile(r"[a-z]{2,}[a-z0-9]*(?:-[a-z0-9]+)+|[a-z]+-?[a-z]+\d")
_CLOUDS = ("aws", "gcp", "azure")
_LOOPBACK = frozenset({"localhost", "127.0.0.1", "::1", "host.docker.internal"})
#: Drivers whose ``host`` field is the region name itself (``connectors.aws_common``).
_HOST_IS_REGION = frozenset({"s3", "dynamodb"})
#: Destinations written on this deployment's own disk.
_LOCAL_KINDS = frozenset({"file_export", "file"})
_LOCAL_FORMATS = frozenset({"sqlite", "duckdb"})


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
    from services.tenant_store import default_region, get_tenant, get_tenant_for_workspace

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
    return _norm(default_region()) if _strict() else ""


def job_data_region(workspace_id: str | None, requested: str = "", *, host_tenant_id: str = "") -> str:
    """The ``data_region`` tag a job carries.

    The workspace's enforced region when there is one; the caller's tag (a
    label only) otherwise, defaulting to ``us-east-1`` as before.
    """
    return enforced_region(workspace_id, host_tenant_id=host_tenant_id) or _norm(requested) or "us-east-1"


def _hostnames(endpoint: Any) -> list[str]:
    """Every hostname the destination's address names (host, endpoint URL, URL-style DSN)."""
    names: list[str] = []
    for raw in (
        getattr(endpoint, "host", ""),
        getattr(endpoint, "endpoint_url", ""),
        getattr(endpoint, "connection_string", ""),
    ):
        text = str(raw or "").strip()
        if not text:
            continue
        if "://" in text:
            try:
                netloc = urlparse(text).netloc
            except ValueError:
                netloc = ""
            # ``user:pw@h1:27017,h2:27017`` — every member of a seed list.
            for part in netloc.rsplit("@", 1)[-1].split(","):
                host = part.strip().rsplit(":", 1)[0] if part.count(":") == 1 else part.strip()
                if host:
                    names.append(host.strip("[]").lower())
        else:
            names.append(text.split("/", 1)[0].rsplit(":", 1)[0].lower())
    return names


def _region_in_hostname(host: str) -> str:
    """The cloud region a hostname's own labels name, or ``""``."""
    labels = [label for label in host.split(".") if label]
    if host == "s3.amazonaws.com" or host.endswith(".s3.amazonaws.com"):
        # The global S3 endpoint redirects to wherever the bucket lives.
        return ""
    for index, label in enumerate(labels):
        if _AWS_REGION.fullmatch(label):
            return label
        if label in _CLOUDS and index and _REGION_SHAPED.fullmatch(labels[index - 1]):
            return labels[index - 1]
        for cloud in _CLOUDS:  # ``us-east-1-aws`` (one label)
            if label.endswith(f"-{cloud}") and _REGION_SHAPED.fullmatch(label[: -len(cloud) - 1]):
                return label[: -len(cloud) - 1]
    return ""


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


def destination_region(endpoint: Any) -> DestinationRegion:
    """The destination's region and the evidence for it (see the module docstring)."""
    fmt = _norm(getattr(endpoint, "format", ""))
    kind = _norm(getattr(endpoint, "kind", ""))
    hosts = _hostnames(endpoint)

    found = {region for region in map(_region_in_hostname, hosts) if region}
    if len(found) > 1:
        return _UNKNOWN  # an address that names two regions proves neither
    if found:
        return DestinationRegion(found.pop(), "its endpoint address")

    host_field = _norm(getattr(endpoint, "host", ""))
    if (
        fmt in _HOST_IS_REGION
        and host_field
        and "." not in host_field
        and "/" not in host_field
        and ":" not in host_field
        and host_field not in _LOOPBACK
        and not getattr(endpoint, "endpoint_url", "")
    ):
        return DestinationRegion(host_field, "its region setting")

    declared = _declared_region(endpoint)
    if declared:
        return DestinationRegion(declared, "the region declared on the destination")

    on_this_deployment = (
        kind in _LOCAL_KINDS
        or fmt in _LOCAL_FORMATS
        or (bool(hosts) and all(h in _LOOPBACK for h in hosts))
    )
    if on_this_deployment:
        configured = _norm(getenv_brand("DEFAULT_REGION", ""))
        if configured:
            return DestinationRegion(configured, "this deployment's region")
    return _UNKNOWN


def _resolved(endpoint: Any, workspace_id: str | None) -> Any:
    """The endpoint with its saved connector expanded (a route judges it before the engine does)."""
    if not str(getattr(endpoint, "connector_id", "") or "").strip():
        return endpoint
    try:
        from src.transfer.adapters import resolve_endpoint

        return resolve_endpoint(endpoint, workspace_id=str(workspace_id or "") or None)
    except Exception:  # noqa: BLE001 - an unresolvable connector is the caller's own error
        return endpoint


def residency_refusal(
    workspace_id: str | None, endpoint: Any, *, host_tenant_id: str = ""
) -> str | None:
    """Why this destination may not receive the workspace's data, or ``None``."""
    allowed = enforced_region(workspace_id, host_tenant_id=host_tenant_id)
    if not allowed:
        return None
    where = destination_region(_resolved(endpoint, workspace_id))
    label = str(getattr(endpoint, "format", "") or getattr(endpoint, "kind", "") or "this").strip()
    if not where.region:
        return (
            f"Data residency violation: the region of the {label} destination cannot be "
            f"determined, and this workspace's data must stay in '{allowed}'. Use a regional "
            "endpoint address, or set the destination's region."
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
