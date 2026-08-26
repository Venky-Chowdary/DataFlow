"""Shared helpers for SaaS source connectors (Salesforce, HubSpot, Stripe, etc.)."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, NoReturn

import requests
from services.error_handling import RetryBudget, with_retry
from services.value_serializer import cell_to_string

from connectors.base import ReadBatch


@dataclass(frozen=True)
class SaasDescribeGate:
    """Outcome of live schema probe before Map bind (HubSpot/SF class)."""

    ok: bool
    error: str = ""
    fields: list[Any] | None = None
    warning: str = ""


def merge_saas_live_types(
    live_types: dict[str, str],
    target_cols: list[str],
    *,
    studio_types: dict[str, Any] | None = None,
    product: str = "SaaS",
) -> tuple[dict[str, str], str | None]:
    """Live∩Studio coverage gate after a successful (possibly partial) Describe.

    Stripe/Shopify catalog merges refuse Map ``VARCHAR`` invent when a mapped
    column is absent from documented carriers. CRM Meta/Describe must do the
    same: HTTP 200 with a non-empty but incomplete field list must not soft-bind
    missing mapped targets (empty→null / overflow invent on reverse-ETL write).

    Returns ``(merged, None)`` when every mapped column has a live carrier or a
    Studio-typed destination carrier. Otherwise ``(partial, error)``.
    """
    live = live_types if isinstance(live_types, dict) else {}
    live_by_lower: dict[str, tuple[str, str]] = {}
    for key, typ in live.items():
        name = str(key or "").strip()
        carrier = str(typ or "").strip()
        if not name or not carrier:
            continue
        live_by_lower.setdefault(name.lower(), (name, carrier))

    studio = studio_types if isinstance(studio_types, dict) else {}
    studio_l = {
        str(k).lower(): str(v).strip()
        for k, v in studio.items()
        if k and str(v or "").strip()
    }

    merged: dict[str, str] = {}
    missing: list[str] = []
    for col in target_cols:
        if not col:
            continue
        hit = live_by_lower.get(str(col).lower())
        if hit:
            merged[col] = hit[1]
            continue
        st = studio_l.get(str(col).lower())
        if st:
            merged[col] = st
            continue
        missing.append(col)
    if missing:
        sample = ", ".join(repr(c) for c in missing[:12])
        more = f" (+{len(missing) - 12} more)" if len(missing) > 12 else ""
        return merged, (
            f"{product} live schema is missing mapped field(s) {sample}{more} — "
            "refuse Map VARCHAR invent (empty→null / overflow risk). Remap to "
            "fields present on the live object or provide Studio "
            "destination_column_types for every mapped column."
        )
    return merged, None


def resolve_saas_live_or_map_dest_types(
    target_cols: list[str],
    mappings: list[dict],
    column_types: dict[str, str] | None = None,
    *,
    live_carriers: dict[str, str] | None = None,
    live_schema_present: bool = False,
    studio_types: dict[str, Any] | None = None,
    logical_types: list[str] | None = None,
    product: str = "SaaS",
    default: str = "VARCHAR",
) -> dict[str, str]:
    """Live/Studio fail-closed carriers; Map only when neither was supplied.

    When Describe/Meta was probed (``live_schema_present``) or Studio typed any
    destination carriers, never soft-fill gaps with Map ``VARCHAR`` — return
    covered carriers only (parity with write-path ``merge_saas_live_types``).
    Map invent is allowed only for offline / create-new paths with no live
    schema and no Studio types.
    """
    studio = studio_types if isinstance(studio_types, dict) and studio_types else None
    if live_schema_present or studio is not None:
        merged, _err = merge_saas_live_types(
            live_carriers if isinstance(live_carriers, dict) else {},
            list(target_cols or []),
            studio_types=studio,
            product=product,
        )
        return merged
    from connectors.writer_common import resolve_mapping_dest_types

    return resolve_mapping_dest_types(
        target_cols,
        mappings,
        column_types or {},
        logical_types=logical_types,
        live_types=None,
        default=default,
    )


def gate_saas_describe(
    *,
    product: str,
    object_name: str,
    fields: list[Any] | None,
    exc: BaseException | None,
    target_cols: list[str],
    studio_types: dict[str, Any] | None,
    allow_empty_fields: bool = False,
) -> SaasDescribeGate:
    """Fail-closed when live schema is unavailable without Studio-typed Map.

    Returns ``fields=None`` when the caller should fall back to Studio carriers
    (never Map VARCHAR invent). ``allow_empty_fields`` is for probes that may
    honestly return zero definitions (e.g. Shopify metafields on a bare object).
    """
    studio_live = isinstance(studio_types, dict) and bool(target_cols) and all(
        str(studio_types.get(c) or "").strip() for c in target_cols if c
    )
    if exc is not None:
        if is_auth_error(exc if isinstance(exc, Exception) else Exception(str(exc))):
            return SaasDescribeGate(
                ok=False,
                error=(
                    f"{product} schema Describe auth failed: {exc} — "
                    "refuse Map VARCHAR bind (empty→null invent risk)."
                ),
            )
        if not studio_live:
            return SaasDescribeGate(
                ok=False,
                error=(
                    f"{product} schema Describe unavailable ({exc}) and Studio "
                    "did not type all mapped fields — refuse Map VARCHAR bind "
                    f"(empty→null invent risk) for {object_name!r}."
                ),
            )
        return SaasDescribeGate(
            ok=True,
            fields=None,
            warning=(
                f"{product} schema Describe unavailable ({exc}); using "
                "Studio-typed carriers only for this write"
            ),
        )
    if fields is not None and len(fields) == 0 and not allow_empty_fields:
        if not studio_live:
            return SaasDescribeGate(
                ok=False,
                error=(
                    f"{product} schema Describe returned no fields for "
                    f"{object_name!r} — refuse Map VARCHAR bind "
                    "(empty→null invent risk)."
                ),
            )
        return SaasDescribeGate(
            ok=True,
            fields=None,
            warning=(
                f"{product} schema Describe returned no fields for "
                f"{object_name!r}; using Studio-typed carriers only"
            ),
        )
    if fields is None and not allow_empty_fields:
        if not studio_live:
            return SaasDescribeGate(
                ok=False,
                error=(
                    f"{product} schema Describe returned no usable fields for "
                    f"{object_name!r} — refuse Map VARCHAR bind "
                    "(empty→null invent risk)."
                ),
            )
        return SaasDescribeGate(
            ok=True,
            fields=None,
            warning=(
                f"{product} schema Describe missing for {object_name!r}; "
                "using Studio-typed carriers only"
            ),
        )
    return SaasDescribeGate(ok=True, fields=fields)


def base_url(host: str, default: str) -> str:
    host = (host or default).strip()
    if not host:
        host = default
    if "://" not in host:
        host = f"https://{host}"
    return host.rstrip("/")


def token(  # nosec B107
    api_key: str = "",
    connection_string: str = "",
    username: str = "",
    password: str = "",
) -> str:
    """Extract a bearer token from the first non-empty credential field."""
    for value in (api_key, connection_string):
        v = (value or "").strip()
        if v:
            return v
    if username and password:
        return f"{username.strip()}:{password.strip()}"
    return ""


def saas_record_id(value: Any) -> str | None:
    """Dest-canonical present record id, or None when absent.

    ``if val`` dropped integer ``0``. Reader-null sentinels became URL ids.
    ``str(True)`` invented ``True`` so dest ``true`` missed upsert identity.
    """
    from services.value_serializer import present_cell_text

    return present_cell_text(value)


def request(
    *,
    method: str,
    url: str,
    token: str = "",
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    data: dict[str, Any] | None = None,
    timeout: float = 30.0,
    retry_budget: RetryBudget | None = None,
    auth_header: str = "Authorization",
    auth_scheme: str = "Bearer",
) -> requests.Response:
    """Make an HTTP request with retriable transient handling (429 / 5xx / timeouts)."""
    h = dict(headers or {})
    if token and auth_header:
        h.setdefault(auth_header, f"{auth_scheme} {token}".strip())
    h.setdefault("Accept", "application/json")
    h.setdefault("User-Agent", "Datawrap/1.0")

    def _call() -> requests.Response:
        resp = requests.request(
            method=method,
            url=url,
            headers=h,
            params=params,
            json=data,
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp

    return with_retry(_call, budget=retry_budget or RetryBudget())


def is_auth_error(exc: Exception) -> bool:
    """True for HTTP 401/403-class failures — callers must not swallow these."""
    text = str(exc).lower()
    if "401" in text or "403" in text:
        return True
    if "unauthorized" in text or "forbidden" in text:
        return True
    status = getattr(getattr(exc, "response", None), "status_code", None)
    return status in {401, 403}


def humanize_http_error(exc: Exception, driver: str) -> str:
    text = str(exc).lower()
    if "401" in text or "unauthorized" in text:
        return f"{driver.title()} authentication failed. Check your API token/key and that it is active."
    if "403" in text or "forbidden" in text:
        return f"{driver.title()} permission denied. The token does not have the required scopes/permissions for this object."
    if "404" in text or "not found" in text:
        return f"{driver.title()} resource not found. Check the object/table name and API endpoint."
    if "429" in text or "rate limit" in text:
        return f"{driver.title()} rate limit hit. Please wait and try again."
    if "timeout" in text or "timed out" in text:
        return "Connection timed out. Check the host/URL and network."
    if "connection" in text and "refused" in text:
        return "Could not reach the API host. Check the URL and network."
    if re.search(r"no module named|cannot import", text):
        return "The requests library is not installed in this environment."
    return f"{driver.title()} API error: {exc}"


def extract_records(records: list[dict[str, Any]]) -> ReadBatch:
    if not records:
        return ReadBatch(headers=[], rows=[], offset=0, total_rows=0)
    # Union keys across the page — late fields on later records must not vanish.
    headers: list[str] = []
    seen: set[str] = set()
    for rec in records:
        for key in rec.keys():
            if key == "attributes":
                continue
            if key not in seen:
                seen.add(key)
                headers.append(key)
    rows = [
        [cell_to_string(r.get(h), preserve_sql_null=True) for h in headers]
        for r in records
    ]
    return ReadBatch(headers=headers, rows=rows, offset=0, total_rows=None)


def object_name(cfg: dict[str, Any], default: str) -> str:
    return (
        (cfg.get("table") or "").strip()
        or (cfg.get("database") or "").strip()
        or default
    )


def write_not_supported(**_kwargs: Any) -> NoReturn:
    """Placeholder writer for source-only SaaS connectors."""
    raise RuntimeError("This SaaS connector is currently source-only.")


# Keep an alias under the canonical writer name so the registry stays consistent.
write_mapped_rows = write_not_supported
