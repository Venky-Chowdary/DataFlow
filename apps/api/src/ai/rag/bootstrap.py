"""Helpers for bootstrapping the local vector store."""

from __future__ import annotations

import os
from typing import Any

from .document_ingestion import DataTransferDocumentIngestion
from .vector_store import DataTransferVectorStore, get_vector_store


def rebuild_vector_store() -> dict[str, Any]:
    """Rebuild the local vector store from the built-in knowledge base."""
    persist_dir = os.environ.get("DATAFLOW_VECTOR_STORE_DIR")
    store = get_vector_store() if not persist_dir else DataTransferVectorStore(persist_dir=persist_dir)
    ingestion = DataTransferDocumentIngestion()
    ingestion.vector_store = store
    result = ingestion.ingest_knowledge_base()
    return {
        **result,
        "documents": store.document_count,
        "persist_dir": store.persist_dir,
        "backend": store.backend,
    }
