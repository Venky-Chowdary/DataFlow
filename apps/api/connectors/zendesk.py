"""Zendesk REST API source connector.

Wraps the brand-aware generic REST core with Zendesk defaults:
- Cursor pagination via ``next_page`` / ``per_page``
- Bearer token auth
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch


def test_zendesk(
    *,
    connector_type: str = "zendesk",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Zendesk connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


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
