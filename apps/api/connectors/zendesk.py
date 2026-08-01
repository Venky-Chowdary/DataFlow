"""Zendesk REST API source connector.

Wraps the brand-aware generic REST core with Zendesk defaults:
- Cursor pagination via ``next_page`` / ``per_page``
- Bearer token auth
"""

from __future__ import annotations

import base64
from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch, base_url, request, token

DEFAULT_HOST = "zendesk.com"


def test_zendesk(
    *,
    connector_type: str = "zendesk",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Zendesk connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def _auth_from_cfg(cfg: dict[str, Any]) -> tuple[str, str]:
    """Return (scheme, credential) for Support API calls."""
    cred = token(
        str(cfg.get("api_key") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("username") or ""),
        str(cfg.get("password") or ""),
    )
    if not cred:
        return ("", "")
    if ":" in cred:
        return ("Basic", base64.b64encode(cred.encode("utf-8")).decode("ascii"))
    return ("Bearer", cred)


def describe_fields(cfg: dict[str, Any], object_type: str = "") -> list[dict[str, Any]]:
    """Return Zendesk field definitions for tickets / users / organizations.

    Hits ``ticket_fields`` / ``user_fields`` / ``organization_fields`` so write
    quarantine can emit VARCHAR(65536)/DECIMAL carriers from live schema
    (Zendesk Help Center custom-field type limits).
    """
    obj = (object_type or str(cfg.get("table") or cfg.get("database") or "tickets")).strip().lower()
    if obj in {"ticket", "tickets"}:
        path = "ticket_fields"
        key = "ticket_fields"
    elif obj in {"user", "users"}:
        path = "user_fields"
        key = "user_fields"
    elif obj in {"organization", "organizations", "org", "orgs"}:
        path = "organization_fields"
        key = "organization_fields"
    else:
        return []

    host = str(cfg.get("host") or cfg.get("subdomain") or "")
    scheme, access = _auth_from_cfg(cfg)
    if not access or not host:
        return []
    url = f"{base_url(host, DEFAULT_HOST)}/api/v2/{path}.json"
    fields: list[dict[str, Any]] = []
    next_url: str | None = url
    seen: set[str] = set()
    while next_url:
        if next_url in seen:
            break
        seen.add(next_url)
        resp = request(
            method="GET",
            url=next_url,
            token=access,
            auth_scheme=scheme,
            timeout=30,
        )
        body = resp.json() if hasattr(resp, "json") else {}
        for f in body.get(key) or []:
            if not isinstance(f, dict):
                continue
            title = str(f.get("title") or f.get("raw_title") or "").strip()
            # Custom fields are addressed by id in ticket payloads; system
            # fields use ``type`` as the JSON key (subject, description, …).
            type_name = str(f.get("type") or "").strip().lower()
            fid = f.get("id")
            name = type_name if f.get("removable") is False and type_name else (
                str(fid) if fid is not None else title
            )
            # Prefer human title / type for Map columns that use field titles.
            fields.append(
                {
                    "id": fid,
                    "name": name,
                    "title": title,
                    "type": type_name,
                    "active": bool(f.get("active", True)),
                    "regexp_for_validation": f.get("regexp_for_validation") or "",
                }
            )
            if title and title.lower() != str(name).lower():
                fields.append(
                    {
                        "id": fid,
                        "name": title,
                        "title": title,
                        "type": type_name,
                        "active": bool(f.get("active", True)),
                        "regexp_for_validation": f.get("regexp_for_validation") or "",
                    }
                )
        next_page = body.get("next_page")
        next_url = str(next_page) if next_page else None
    return fields


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read Zendesk objects (tickets, users, organizations, etc.) as a row matrix."""
    resolved_cfg = {**cfg, "type": cfg.get("type") or "zendesk"}
    return rest_api.read_object(
        cfg=resolved_cfg,
        object=object,
        limit=limit,
        offset=offset,
        **kwargs,
    )
