"""Notion REST API source connector.

Wraps the brand-aware generic REST core with Notion defaults:
- Cursor pagination via ``start_cursor`` / ``next_cursor``
- ``Notion-Version`` header
- ``results`` data path
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch


def test_notion(
    *,
    connector_type: str = "notion",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Notion connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read Notion objects (databases, pages, etc.) as a row matrix."""
    resolved_cfg = {**cfg, "type": cfg.get("type") or "notion"}
    return rest_api.read_object(
        cfg=resolved_cfg,
        object=object,
        limit=limit,
        offset=offset,
        **kwargs,
    )
