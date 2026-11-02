"""Vector identity COPY is TRANSFER_READY, never a customer-tenant PRODUCTION_SKU."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.vector_copy_honesty import (  # noqa: E402
    VECTOR_IDENTITY_COPY_SKU_ROUTES,
    is_desktop_lab_endpoint,
    vector_identity_copy_honesty,
)
from src.transfer.connector_capabilities import TRANSFER_READY_CATALOG_IDS  # noqa: E402
from src.transfer.registry import PRODUCTION_SKU  # noqa: E402


def test_identity_vector_routes_are_not_production_sku():
    sku = set(PRODUCTION_SKU)
    missing = [route for route in VECTOR_IDENTITY_COPY_SKU_ROUTES if route in sku]
    assert not missing, (
        "Identity vector COPY is TRANSFER_READY, not PRODUCTION_SKU: "
        f"{missing}"
    )


def test_vector_engines_remain_transfer_ready_catalog():
    for engine in ("qdrant", "milvus", "weaviate", "pinecone", "pgvector"):
        assert engine in TRANSFER_READY_CATALOG_IDS


def test_desktop_lab_loopback_is_not_customer_tenant():
    assert is_desktop_lab_endpoint({"host": "127.0.0.1", "port": 19530}, "milvus")
    assert is_desktop_lab_endpoint({"host": "localhost", "port": 8080}, "weaviate")
    assert is_desktop_lab_endpoint({"host": "127.0.0.1", "port": 6333}, "qdrant")
    assert not is_desktop_lab_endpoint(
        {"host": "in01-xxx.zillizcloud.com", "port": 443}, "milvus"
    )


def test_honesty_stamps_refuse_sku_and_exactly_once():
    stamp = vector_identity_copy_honesty(
        engine="milvus",
        cfg={"host": "127.0.0.1", "port": 19530},
    )
    assert stamp["production_sku"] is False
    assert stamp["desktop_lab_endpoint"] is True
    assert stamp["cdc_exactly_once_claimed"] is False
    assert stamp["delivery_class"] == "at_least_once_upsert"
    assert stamp["snapshot_copy_idempotent"] is True
    assert stamp["sku_honesty"] == "desktop_lab_not_customer_tenant"

    cloud = vector_identity_copy_honesty(
        engine="weaviate",
        cfg={"host": "weaviate.example.com", "port": 443},
    )
    assert cloud["production_sku"] is False
    assert cloud["desktop_lab_endpoint"] is False
    assert cloud["sku_honesty"] == "identity_copy_not_production_sku"


def test_write_dest_sku_routes_are_not_identity_copy():
    """postgresql→qdrant is a write SKU; qdrant→qdrant identity is not."""
    assert ("database", "postgresql", "database", "qdrant") in PRODUCTION_SKU
    assert ("database", "postgresql", "database", "weaviate") in PRODUCTION_SKU
    assert ("file", "csv", "database", "milvus") in PRODUCTION_SKU
    assert ("database", "qdrant", "database", "qdrant") not in PRODUCTION_SKU
