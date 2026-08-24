"""Shopify Admin REST API source connector.

Wraps the brand-aware generic REST core with Shopify defaults:
- Link-header pagination
- ``X-Shopify-Access-Token`` auth header
- ``.json`` path suffix
"""

from __future__ import annotations

from typing import Any

from connectors import rest_api
from connectors.saas_common import ReadBatch, base_url, request, token

DEFAULT_HOST = "myshopify.com"
API_VERSION = "2024-04"

_OWNER_TYPE: dict[str, str] = {
    "customers": "CUSTOMER",
    "customer": "CUSTOMER",
    "products": "PRODUCT",
    "product": "PRODUCT",
    "orders": "ORDER",
    "order": "ORDER",
    "variants": "PRODUCTVARIANT",
    "variant": "PRODUCTVARIANT",
    "product_variants": "PRODUCTVARIANT",
}


def test_shopify(
    *,
    connector_type: str = "shopify",
    **kwargs: Any,
) -> tuple[bool, str]:
    """Probe Shopify connectivity with a lightweight request."""
    return rest_api.test_connection(type=connector_type, **kwargs)


def describe_metafield_definitions(
    cfg: dict[str, Any],
    object_type: str = "",
) -> list[dict[str, Any]]:
    """Live metafield definitions via Admin GraphQL.

    Returns ``[{namespace, key, type, validations}]`` for reverse-ETL quarantine
    carriers (single_line_text_field max, number_decimal, …).

    Auth/scope failures (HTTP 401/403 or GraphQL ACCESS_DENIED) propagate so
    writers can fail-closed. Other probe misses soft-return ``[]`` / partial
    pages — core Admin carriers still resolve without metafield defs.
    """
    from connectors.saas_common import is_auth_error

    obj = (object_type or str(cfg.get("table") or cfg.get("database") or "customers")).strip()
    owner = _OWNER_TYPE.get(obj.lower())
    if not owner:
        return []
    access = token(
        str(cfg.get("api_key") or ""),
        str(cfg.get("connection_string") or ""),
        str(cfg.get("username") or ""),
        str(cfg.get("password") or ""),
    )
    host = str(cfg.get("host") or cfg.get("shop") or "")
    if not access or not host:
        return []
    shop = base_url(host, DEFAULT_HOST).rstrip("/")
    url = f"{shop}/admin/api/{API_VERSION}/graphql.json"
    query = """
    query MetafieldDefs($ownerType: MetafieldOwnerType!, $cursor: String) {
      metafieldDefinitions(first: 100, ownerType: $ownerType, after: $cursor) {
        pageInfo { hasNextPage endCursor }
        edges {
          node {
            namespace
            key
            type { name }
            validations { name value }
          }
        }
      }
    }
    """
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    try:
        while True:
            payload = {
                "query": query,
                "variables": {"ownerType": owner, "cursor": cursor},
            }
            resp = request(
                method="POST",
                url=url,
                token="",
                headers={"X-Shopify-Access-Token": access},
                data=payload,
                timeout=30,
            )
            body = resp.json() if hasattr(resp, "json") else {}
            gql_errors = body.get("errors")
            if gql_errors:
                msg = str(gql_errors)
                low = msg.lower()
                if any(
                    k in low
                    for k in (
                        "access",
                        "unauthorized",
                        "forbidden",
                        "401",
                        "403",
                        "access_denied",
                    )
                ):
                    raise RuntimeError(
                        f"Shopify metafield Describe auth/scope failed: {msg[:400]}"
                    )
                break
            block = ((body.get("data") or {}).get("metafieldDefinitions")) or {}
            for edge in block.get("edges") or []:
                node = (edge or {}).get("node") or {}
                typ = node.get("type") or {}
                typ_name = typ.get("name") if isinstance(typ, dict) else typ
                key = str(node.get("key") or "")
                ns = str(node.get("namespace") or "")
                sig = f"{ns}.{key}"
                if not key or sig in seen:
                    continue
                seen.add(sig)
                out.append(
                    {
                        "namespace": ns,
                        "key": key,
                        "type": str(typ_name or ""),
                        "validations": list(node.get("validations") or []),
                    }
                )
            page = block.get("pageInfo") or {}
            if not page.get("hasNextPage"):
                break
            cursor = page.get("endCursor")
            if not cursor or cursor in seen:
                break
            seen.add(str(cursor))
    except Exception as exc:
        if is_auth_error(exc):
            raise
        if "auth/scope failed" in str(exc).lower():
            raise
        return out
    return out


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
