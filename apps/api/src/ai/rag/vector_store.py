"""
DataTransfer.space — Vector Store

Local vector storage using ChromaDB with in-memory fallback.
"""

from __future__ import annotations
import json
import os
import uuid
from dataclasses import dataclass
from typing import Optional


from .embedding_service import get_embedding_service

_vector_store: Optional["DataTransferVectorStore"] = None

DEFAULT_PERSIST_DIR = os.environ.get(
    "DATAFLOW_VECTOR_STORE_DIR",
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
        self._embedding_service = get_embedding_service()
        self._init_backend()

    def _init_backend(self):
        os.makedirs(self.persist_dir, exist_ok=True)
        backend = os.environ.get("DATAFLOW_VECTOR_STORE_BACKEND", "memory").lower()
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
                    clean[k] = json.dumps(v)
                else:
                    clean[k] = str(v)
            clean_metas.append(clean)

        embeddings = self._embedding_service.embed(texts)

        if self._backend == "chromadb":
            self._collection.upsert(
                ids=ids,
                documents=texts,
                metadatas=clean_metas,
                embeddings=embeddings.tolist(),
            )
        else:
            for i, text in enumerate(texts):
                self._memory_docs.append({
                    "id": ids[i],
                    "text": text,
                    "metadata": clean_metas[i],
                    "embedding": embeddings[i],
                })

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
        query_emb = self._embedding_service.embed_single(query)
        scored = []
        for doc in self._memory_docs:
            if filter_metadata:
                match = all(
                    doc["metadata"].get(k) == v for k, v in filter_metadata.items()
                )
                if not match:
                    continue
            sim = self._embedding_service.similarity(query_emb, doc["embedding"])
            scored.append(VectorDocument(
                id=doc["id"],
                text=doc["text"],
                metadata=doc["metadata"],
                score=sim,
            ))

        scored.sort(key=lambda d: d.score, reverse=True)
        return scored[:n_results]

    def delete_all(self):
        """Clear all documents."""
        if self._backend == "chromadb":
            import chromadb
            client = chromadb.PersistentClient(path=self.persist_dir)
            try:
                client.delete_collection(self.COLLECTION_NAME)
            except Exception:
                pass
            self._collection = client.get_or_create_collection(
                name=self.COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
        else:
            self._memory_docs.clear()


def get_vector_store() -> DataTransferVectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = DataTransferVectorStore()
    return _vector_store
