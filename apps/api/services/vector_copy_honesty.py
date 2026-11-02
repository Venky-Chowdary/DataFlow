"""Operator honesty for vector identity COPY.

Identity COPY for Qdrant / Milvus / Weaviate / Pinecone / pgvector is
TRANSFER_READY (the algorithm exists and dest COUNT is the proof). It is
**not** a customer-tenant PRODUCTION_SKU:

* Desktop-lab bind (``:6333`` / ``:19530`` / ``:8080`` / loopback Postgres)
  is a named fixture, not a sold tenant.
* Identity routes (milvus→milvus, …) are absent from ``PRODUCTION_SKU``.
* CDC / retry delivery remains **at-least-once upsert by PK**. Snapshot
  overwrite is dest-exclusive (drop + upsert + dest COUNT) and idempotent
  for that run — that is not platform exactly-once and not LSN-guarded CDC.
"""

from __future__ import annotations

from typing import Any

_LOOPBACK_HOSTS = frozenset({
    "127.0.0.1",
    "localhost",
    "::1",
    "0.0.0.0",
    "ip6-localhost",
})

_DESKTOP_LAB_PORTS = {
    "qdrant": 6333,
    "milvus": 19530,
    "weaviate": 8080,
    "pgvector": 5432,
    "pinecone": None,
}

VECTOR_IDENTITY_COPY_ENGINES = frozenset({
    "qdrant",
    "milvus",
    "weaviate",
    "pinecone",
    "pgvector",
})

# Identity COPY pairs are TRANSFER_READY, not committed PRODUCTION_SKU routes.
VECTOR_IDENTITY_COPY_SKU_ROUTES: tuple[tuple[str, str, str, str], ...] = (
    ("database", "qdrant", "database", "qdrant"),
    ("database", "milvus", "database", "milvus"),
    ("database", "weaviate", "database", "weaviate"),
    ("database", "pinecone", "database", "pinecone"),
    ("database", "pgvector", "database", "pgvector"),
)


def _authority_host_port(cfg: dict[str, Any]) -> tuple[str, int | None]:
    host = str(cfg.get("host") or "").strip().lower()
    port_raw = cfg.get("port")
    port: int | None
    try:
        port = int(port_raw) if port_raw not in (None, "") else None
    except (TypeError, ValueError):
        port = None
    cs = str(cfg.get("connection_string") or cfg.get("dsn") or "").strip()
    blob = cs or host
    if "://" in blob or "@" in blob:
        from connectors.url_authority import parse_url_authority

        parsed = parse_url_authority(blob)
        if parsed.host:
            host = str(parsed.host).strip().lower()
        if parsed.port:
            port = int(parsed.port)
    host = host.replace("localhost", "127.0.0.1")
    if host.startswith("https://") or host.startswith("http://"):
        host = host.split("://", 1)[1]
    host = host.split("/", 1)[0].split(":", 1)[0]
    return host, port


def is_desktop_lab_endpoint(cfg: dict[str, Any] | None, engine: str = "") -> bool:
    """True when the bind is loopback (desktop-lab fixture, not a customer tenant)."""
    if not isinstance(cfg, dict):
        return False
    host, port = _authority_host_port(cfg)
    if host in _LOOPBACK_HOSTS or host.startswith("127."):
        return True
    expected = _DESKTOP_LAB_PORTS.get((engine or "").strip().lower())
    if expected and port == expected and (not host or host in _LOOPBACK_HOSTS):
        return True
    return False


def vector_identity_copy_honesty(
    *,
    engine: str,
    cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Dest-summary stamps. Identity COPY is never sold as PRODUCTION_SKU."""
    name = (engine or "").strip().lower()
    desktop = is_desktop_lab_endpoint(cfg or {}, name)
    return {
        "production_sku": False,
        "desktop_lab_endpoint": desktop,
        "delivery_class": "at_least_once_upsert",
        "cdc_exactly_once_claimed": False,
        "snapshot_copy_idempotent": True,
        "sku_honesty": (
            "desktop_lab_not_customer_tenant"
            if desktop
            else "identity_copy_not_production_sku"
        ),
    }


def vector_identity_cdc_proof_clause() -> str:
    return (
        "CDC remains at-least-once upsert by PK (not exactly-once, not "
        "_df_lsn-guarded). Desktop-lab vector endpoints are not a "
        "customer-tenant PRODUCTION_SKU."
    )
