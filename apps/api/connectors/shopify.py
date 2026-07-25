"""Shopify Admin REST API source connector.

Wraps the brand-aware generic REST core with Shopify defaults:
- Link-header pagination
- ``X-Shopify-Access-Token`` auth header
- ``.json`` path suffix
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch


def test_shopify(
    *,
    connector_type: str = "shopify",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Shopify connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def read_object(
    *,
    cfg: dict[str, Any],
    object: str = "",
    limit: int = 100,
    offset: int = 0,
    **kwargs: Any,
) -> ReadBatch:
    """Read Shopify objects (products, orders, customers, etc.) as a row matrix."""
    resolved_cfg = {**cfg, "type": cfg.get("type") or "shopify"}
    return rest_api.read_object(
        cfg=resolved_cfg,
        object=object,
        limit=limit,
        offset=offset,
        **kwargs,
    )
