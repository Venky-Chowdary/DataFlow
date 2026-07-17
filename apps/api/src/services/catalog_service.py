"""Compatibility shim: canonical implementation now lives in services.catalog_service."""
from __future__ import annotations

from services.catalog_service import (
    _CATALOG_PATH,
    _enriched_connectors,
    catalog_summary,
    catalog_training_docs,
    get_connector_by_id,
    load_catalog,
    search_catalog,
)

__all__ = ['_CATALOG_PATH', 'load_catalog', '_enriched_connectors', 'search_catalog', 'catalog_summary', 'get_connector_by_id', 'catalog_training_docs']
