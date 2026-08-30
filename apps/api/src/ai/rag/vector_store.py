"""
Datawrap — Vector Store

Local vector storage using ChromaDB with in-memory fallback.
"""

from __future__ import annotations

import json
import logging
import math
import os
from services.brand_env import getenv_brand
import uuid
from dataclasses import dataclass
from typing import Optional

from services.value_serializer import json_default

from .embedding_service import get_embedding_service

try:
    import numpy as np
except ImportError:  # slim API hosts — the pure-Python path below still answers
    np = None  # type: ignore[assignment]

_vector_store: Optional["DataTransferVectorStore"] = None

DEFAULT_PERSIST_DIR = getenv_brand("VECTOR_STORE_DIR",
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "data", "vector_store"),
)


@dataclass
class VectorDocument:
    """A document stored in the vector store."""
    id: str
    text: str
    metadata: dict
    score: float = 0.0


class DataTransferVectorStore:
    """Vector store with ChromaDB backend and in-memory fallback."""

    COLLECTION_NAME = "datatransfer_knowledge"

    def __init__(self, persist_dir: str | None = None):
        self.persist_dir = persist_dir or DEFAULT_PERSIST_DIR
        self._backend = "memory"
        self._collection = None
        self._memory_docs: list[dict] = []
        # Cosine over the whole store runs per analyzed column, so the corpus is
        # kept pre-normalized: a search is then one dot product per document
        # (one matrix product under numpy) instead of re-deriving both norms
        # 2 x n x dim times per query.
        self._unit_rows: list[list[float]] = []
        self._unit_matrix = None
        self._embedding_service = get_embedding_service()
        self._init_backend()

    @staticmethod
    def _unit(vector) -> list[float]:
        values = [float(v) for v in vector]
        norm = math.sqrt(sum(v * v for v in values))
        if norm == 0.0:
            return values
        return [v / norm for v in values]

    def _index_unit_rows(self) -> None:
        """Rebuild the normalized corpus matrix after documents change."""
        if np is None or not self._unit_rows:
            self._unit_matrix = None
            return
        width = len(self._unit_rows[0])
        if any(len(row) != width for row in self._unit_rows):
            # Mixed dimensions (backend switched mid-process): fall back to the
            # per-row path, which skips mismatched documents instead of failing.
            self._unit_matrix = None
            return
        self._unit_matrix = np.asarray(self._unit_rows, dtype=float)

    def _init_backend(self):
        os.makedirs(self.persist_dir, exist_ok=True)
        backend = getenv_brand("VECTOR_STORE_BACKEND", "memory").lower()
        if backend == "chromadb":
            try:
                import chromadb
                client = chromadb.PersistentClient(path=self.persist_dir)
                self._collection = client.get_or_create_collection(
                    name=self.COLLECTION_NAME,
                    metadata={"hnsw:space": "cosine"},
                )
                self._backend = "chromadb"
            except ImportError:
                self._backend = "memory"
        else:
            self._backend = "memory"

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def document_count(self) -> int:
        if self._backend == "chromadb":
            return self._collection.count()
        return len(self._memory_docs)

    def add_documents(
        self,
        texts: list[str],
        metadatas: list[dict] | None = None,
        ids: list[str] | None = None,
    ) -> list[str]:
        """Add documents to the vector store."""
        if not texts:
            return []

        if metadatas is None:
            metadatas = [{}] * len(texts)
        if ids is None:
            ids = [str(uuid.uuid4()) for _ in texts]

        # Sanitize metadata for ChromaDB (only str/int/float/bool)
        clean_metas = []
        for meta in metadatas:
            clean = {}
            for k, v in meta.items():
                if isinstance(v, (str, int, float, bool)):
                    clean[k] = v
                elif isinstance(v, list):
                    clean[k] = json.dumps(v, default=json_default)
                else:
                    clean[k] = str(v)
            clean_metas.append(clean)

        embeddings = self._embedding_service.embed(texts)
        embedding_rows = (
            embeddings.tolist() if hasattr(embeddings, "tolist") else list(embeddings)
        )

        if self._backend == "chromadb":
            self._collection.upsert(
                ids=ids,
                documents=texts,
                metadatas=clean_metas,
                embeddings=embedding_rows,
            )
        else:
            # Upsert by id, matching the ChromaDB branch. Appending blindly let
            # a second ingest duplicate the whole knowledge base: a long-lived
            # API had grown 349 documents into 20190, and retrieval cost — which
            # is linear in the corpus — grew with it.
            position = {doc["id"]: i for i, doc in enumerate(self._memory_docs)}
            for i, text in enumerate(texts):
                record = {
                    "id": ids[i],
                    "text": text,
                    "metadata": clean_metas[i],
                    "embedding": embedding_rows[i],
                }
                unit = self._unit(embedding_rows[i])
                existing = position.get(ids[i])
                if existing is None:
                    position[ids[i]] = len(self._memory_docs)
                    self._memory_docs.append(record)
                    self._unit_rows.append(unit)
                else:
                    self._memory_docs[existing] = record
                    self._unit_rows[existing] = unit
            self._index_unit_rows()

        return ids

    def search(
        self,
        query: str,
        n_results: int = 5,
        filter_metadata: dict | None = None,
    ) -> list[VectorDocument]:
        """Search for similar documents."""
        if self.document_count == 0:
            return []

        if self._backend == "chromadb":
            where = filter_metadata if filter_metadata else None
            results = self._collection.query(
                query_texts=[query],
                n_results=min(n_results, self.document_count),
                where=where,
            )
            docs = []
            if results and results["documents"]:
                for i, text in enumerate(results["documents"][0]):
                    meta = results["metadatas"][0][i] if results["metadatas"] else {}
                    dist = results["distances"][0][i] if results["distances"] else 0
                    doc_id = results["ids"][0][i] if results["ids"] else str(i)
                    docs.append(VectorDocument(
                        id=doc_id,
                        text=text,
                        metadata=meta,
                        score=1.0 - dist,
                    ))
            return docs

        # Memory fallback
        query_unit = self._unit(self._embedding_service.embed_single(query))
        keep = [
            i
            for i, doc in enumerate(self._memory_docs)
            if not filter_metadata
            or all(doc["metadata"].get(k) == v for k, v in filter_metadata.items())
        ]
        if not keep:
            return []

        if self._unit_matrix is not None and len(query_unit) == self._unit_matrix.shape[1]:
            sims = self._unit_matrix[keep] @ np.asarray(query_unit, dtype=float)
            scores = [float(s) for s in sims]
        else:
            scores = []
            for i in keep:
                row = self._unit_rows[i]
                scores.append(
                    sum(x * y for x, y in zip(row, query_unit))
                    if len(row) == len(query_unit)
                    else 0.0
                )

        scored = [
            VectorDocument(
                id=self._memory_docs[i]["id"],
                text=self._memory_docs[i]["text"],
                metadata=self._memory_docs[i]["metadata"],
                score=score,
            )
            for i, score in zip(keep, scores)
        ]
        scored.sort(key=lambda d: d.score, reverse=True)
        return scored[:n_results]

    def delete_all(self):
        """Clear all documents."""
        if self._backend == "chromadb":
            import chromadb
            client = chromadb.PersistentClient(path=self.persist_dir)
            try:
                client.delete_collection(self.COLLECTION_NAME)
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
            self._collection = client.get_or_create_collection(
                name=self.COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
        else:
            self._memory_docs.clear()
            self._unit_rows.clear()
            self._index_unit_rows()


def get_vector_store() -> DataTransferVectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = DataTransferVectorStore()
    return _vector_store
