"""Airtable REST API source connector.

Wraps the brand-aware generic REST core with Airtable defaults:
- Cursor pagination via ``offset`` / ``pageSize``
- ``records`` data path
- Bearer token auth
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch


def test_airtable(
    *,
    connector_type: str = "airtable",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Airtable connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read an Airtable base/table as a row matrix."""
    resolved_cfg = {**cfg, "type": cfg.get("type") or "airtable"}
    return rest_api.read_object(
        cfg=resolved_cfg,
        object=object,
        limit=limit,
        offset=offset,
        **kwargs,
    )
