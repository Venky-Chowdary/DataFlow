"""Semantic chunking and embedding service for RAG/vector destinations.

Implements research-backed chunking defaults (256–1024 tokens, 10–20% overlap,
structure-aware splitting where available) and batched embedding with both
managed (OpenAI) and local (sentence-transformers) backends. Embedding outputs
are cached by content hash to avoid re-embedding unchanged rows.
"""

from __future__ import annotations

import hashlib
import os
from functools import lru_cache
from typing import Any, Callable, Protocol


class Embedder(Protocol):
    """Minimal interface for an embedding model."""

    def embed(self, texts: list[str]) -> list[list[float]]:
        ...

    @property
    def dimension(self) -> int:
        ...


class _SentenceTransformerEmbedder:
    """Local embedding backend using sentence-transformers (lazy-loaded)."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2"):
        self.model_name = model_name
        self._model: Any = None
        self._dimension: int | None = None

    def _load(self) -> Any:
        if self._model is None:
            from sentence_transformers import SentenceTransformer

            self._model = SentenceTransformer(self.model_name)
            dim_method = getattr(self._model, "get_embedding_dimension", None) or getattr(self._model, "get_sentence_embedding_dimension")
            self._dimension = int(dim_method())
        return self._model

    @property
    def dimension(self) -> int:
        if self._dimension is None:
            self._load()
        return self._dimension or 384

    def embed(self, texts: list[str]) -> list[list[float]]:
        model = self._load()
        if not texts:
            return []
        return [list(v) for v in model.encode(texts, show_progress_bar=False, convert_to_numpy=True)]


class _OpenAIEmbedder:
    """Managed OpenAI embedding backend (text-embedding-3-small by default)."""

    def __init__(self, model_name: str = "text-embedding-3-small", api_key: str = ""):
        self.model_name = model_name
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY", "")
        self._dimension: int | None = None

    @property
    def dimension(self) -> int:
        # Known dimensions for OpenAI embedding models.
        return {
            "text-embedding-3-small": 1536,
            "text-embedding-3-large": 3072,
            "text-embedding-ada-002": 1536,
        }.get(self.model_name, 1536)

    def embed(self, texts: list[str]) -> list[list[float]]:
        if not self.api_key:
            raise RuntimeError("OpenAI API key is not configured")
        try:
            import openai
        except ImportError as exc:
            raise RuntimeError("OpenAI package not installed") from exc

        client = openai.OpenAI(api_key=self.api_key)
        # OpenAI batch limit is 2048; cap locally to avoid large payloads.
        all_embeddings: list[list[float]] = []
        batch_size = 128
        for i in range(0, len(texts), batch_size):
            batch = texts[i : i + batch_size]
            response = client.embeddings.create(input=batch, model=self.model_name)
            all_embeddings.extend([list(e.embedding) for e in response.data])
        return all_embeddings


class _HashEmbedder:
    """Deterministic content-hash vectors for offline/CI proofs.

    Not a semantic model — same text always yields the same float vector.
    Used when ``model`` is ``hash/...`` so cache layers can be proven without
    network downloads or API keys.
    """

    def __init__(self, dimension: int = 32):
        self._dimension = max(8, int(dimension))

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, texts: list[str]) -> list[list[float]]:
        out: list[list[float]] = []
        for text in texts:
            digest = hashlib.sha256(text.encode("utf-8")).digest()
            # Expand digest if dimension > 32.
            raw = digest
            while len(raw) < self._dimension:
                raw += hashlib.sha256(raw).digest()
            out.append([(b / 127.5) - 1.0 for b in raw[: self._dimension]])
        return out


@lru_cache(maxsize=8)
def _get_embedder(name: str | None = None) -> Embedder:
    """Return a cached embedder instance by model name or env default."""
    default = os.environ.get("DATAFLOW_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2")
    model_name = name or default

    if model_name.startswith("openai/"):
        return _OpenAIEmbedder(model_name=model_name.split("/", 1)[1])
    if model_name.startswith("hash/") or model_name.startswith("deterministic/"):
        # Optional suffix is dimension, e.g. hash/32
        suffix = model_name.split("/", 1)[1]
        dim = 32
        if suffix.isdigit():
            dim = int(suffix)
        return _HashEmbedder(dimension=dim)
    if model_name.startswith("sentence-transformers/"):
        try:
            import sentence_transformers  # noqa: F401
        except ImportError as exc:
            raise ModuleNotFoundError(
                f"sentence_transformers is required for embedding model {model_name!r}; "
                "install sentence-transformers or set DATAFLOW_EMBEDDING_MODEL=hash/32"
            ) from exc
        return _SentenceTransformerEmbedder(model_name=model_name.split("/", 1)[1])
    if model_name in {"text-embedding-3-small", "text-embedding-3-large", "text-embedding-ada-002"}:
        return _OpenAIEmbedder(model_name=model_name)
    try:
        import sentence_transformers  # noqa: F401
    except ImportError as exc:
        raise ModuleNotFoundError(
            f"sentence_transformers is required for embedding model {model_name!r}; "
            "install sentence-transformers or set DATAFLOW_EMBEDDING_MODEL=hash/32"
        ) from exc
    return _SentenceTransformerEmbedder(model_name=model_name)


def chunk_text(
    text: str,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    split_on: Callable[[str], list[str]] | None = None,
) -> list[str]:
    """Split text into overlapping chunks.

    By default we split on paragraph boundaries, then recursively on sentences,
    then on whitespace. This respects document structure better than purely
    character-based splitting.
    """
    if not text or not text.strip():
        return []

    def _paragraphs(t: str) -> list[str]:
        return [p.strip() for p in t.split("\n\n") if p.strip()]

    def _sentences(t: str) -> list[str]:
        import re

        parts = re.split(r"(?<=[.!?])\s+", t)
        return [p.strip() for p in parts if p.strip()]

    def _split(t: str) -> list[str]:
        if split_on:
            return [p.strip() for p in split_on(t) if p.strip()]
        paragraphs = _paragraphs(t)
        if all(len(p) <= chunk_size for p in paragraphs):
            return paragraphs
        out: list[str] = []
        for p in paragraphs:
            if len(p) <= chunk_size:
                out.append(p)
                continue
            for s in _sentences(p):
                if len(s) <= chunk_size:
                    out.append(s)
                else:
                    out.extend(_fixed_chunks(s, chunk_size, chunk_overlap))
        return out

    def _fixed_chunks(t: str, size: int, overlap: int) -> list[str]:
        step = max(1, size - overlap)
        chunks = []
        start = 0
        while start < len(t):
            chunks.append(t[start : start + size].strip())
            start += step
        return [c for c in chunks if c]

    chunks = _split(text)
    merged: list[str] = []
    current = ""
    for c in chunks:
        if len(current) + len(c) + 1 <= chunk_size:
            current = f"{current}\n\n{c}".strip() if current else c
        else:
            if current:
                merged.append(current)
            current = c
    if current:
        merged.append(current)
    return merged


# In-process L1 cache for repeated identical content within a process.
_EMBEDDING_CACHE: dict[str, list[float]] = {}


def _cache_key(text: str, model: str | None) -> str:
    return hashlib.sha256(f"{model or 'default'}:{text}".encode("utf-8")).hexdigest()


def clear_memory_cache() -> int:
    """Drop the process-local L1 embedding cache. Returns prior size."""
    n = len(_EMBEDDING_CACHE)
    _EMBEDDING_CACHE.clear()
    return n


def embed(
    texts: list[str],
    model: str | None = None,
    use_cache: bool = True,
    *,
    durable: bool | None = None,
) -> list[list[float]]:
    """Return embeddings for a list of texts, using the configured model.

    Caching layers (when ``use_cache`` is True):
      1. Process-local dict (L1)
      2. SQLite durable store (L2) when ``durable`` is True (default from env)
    """
    if not texts:
        return []

    from services.embedding_cache import (
        durable_cache_enabled_by_default,
        get_cached,
        put_cached,
    )

    use_durable = durable_cache_enabled_by_default() if durable is None else bool(durable)
    embedder = _get_embedder(model)
    if not use_cache:
        return embedder.embed(texts)

    results: list[list[float] | None] = [None] * len(texts)
    missing_l1: list[tuple[int, str, str]] = []  # idx, text, key
    for i, text in enumerate(texts):
        key = _cache_key(text, model)
        if key in _EMBEDDING_CACHE:
            results[i] = list(_EMBEDDING_CACHE[key])
        else:
            missing_l1.append((i, text, key))

    if missing_l1 and use_durable:
        durable_hits = get_cached([key for _, _, key in missing_l1])
        still_missing: list[tuple[int, str, str]] = []
        for i, text, key in missing_l1:
            if key in durable_hits:
                vector = list(durable_hits[key])
                _EMBEDDING_CACHE[key] = vector
                results[i] = vector
            else:
                still_missing.append((i, text, key))
        missing_l1 = still_missing

    if missing_l1:
        embedded = embedder.embed([text for _, text, _ in missing_l1])
        to_persist: list[tuple[str, str, list[float]]] = []
        for (i, text, key), vector in zip(missing_l1, embedded):
            _EMBEDDING_CACHE[key] = vector
            results[i] = vector
            if use_durable:
                to_persist.append((key, model or "default", vector))
        if to_persist:
            put_cached(to_persist)

    out: list[list[float]] = []
    for v in results:
        if v is None:
            raise RuntimeError("embedding cache produced incomplete results")
        out.append(v)
    return out


def vectorize_records(
    records: list[dict[str, Any]],
    *,
    content_column: str | None = None,
    embedding_column: str | None = None,
    metadata_columns: list[str] | None = None,
    exclude_pii_columns: list[str] | None = None,
    model: str | None = None,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    skip_chunking: bool = False,
    durable_embedding_cache: bool | None = None,
) -> list[dict[str, Any]]:
    """Expand records into vector rows: id, content, embedding, metadata, source_id, chunk_index.

    If ``embedding_column`` is supplied and contains a numeric vector or string
    representation, it is used directly. Otherwise the ``content_column`` is
    chunked and embedded — unless ``skip_chunking`` is True or the record carries
    ``_df_prechunked`` (document chunking already produced RAG-sized rows).

    If ``content_column`` is not supplied, the first textual column with a
    ``text`` semantic type or the longest string value is used.

    ``exclude_pii_columns`` are stripped from metadata and rejected as content
    (fail-closed — never embed or store excluded PII in the vector store).
    """
    from services.document_chunking import PRECHUNKED_FLAG

    exclude = {str(c) for c in (exclude_pii_columns or []) if c}
    if content_column and content_column in exclude:
        raise ValueError(
            f"content_column '{content_column}' is excluded as PII — "
            "choose a non-PII text column or remove it from exclude_pii_columns"
        )

    rows: list[dict[str, Any]] = []
    for record in records:
        rec = {k: v for k, v in record.items() if v is not None}
        prechunked = skip_chunking or str(rec.get(PRECHUNKED_FLAG) or "") in {"1", "true", "True"}

        content = ""
        if content_column and content_column in rec:
            content = str(rec[content_column])
        else:
            # Prefer columns flagged as text/embedding content by heuristic.
            # Never auto-pick an excluded PII column as embed content.
            candidates = [
                k for k, v in rec.items()
                if isinstance(v, str)
                and len(v) > 20
                and k != PRECHUNKED_FLAG
                and k not in exclude
            ]
            if candidates:
                content = max((str(rec[c]) for c in candidates), key=len)
            elif "content" in rec and "content" not in exclude:
                content = str(rec["content"])

        embedding = None
        if embedding_column and embedding_column in rec:
            raw = rec[embedding_column]
            if isinstance(raw, list):
                embedding = [float(x) for x in raw]
            elif isinstance(raw, str):
                try:
                    import json

                    embedding = [float(x) for x in json.loads(raw)]
                except Exception:
                    pass

        source_id = str(rec.get("id", rec.get("_id", rec.get("source_id", ""))))
        metadata = rec.copy()
        if content_column and content_column in metadata:
            del metadata[content_column]
        if embedding_column and embedding_column in metadata:
            del metadata[embedding_column]
        metadata.pop(PRECHUNKED_FLAG, None)
        for col in exclude:
            metadata.pop(col, None)
        if metadata_columns:
            allowed = set(metadata_columns) - exclude
            metadata = {k: v for k, v in metadata.items() if k in allowed}

        existing_chunk_index = 0
        try:
            existing_chunk_index = int(rec.get("chunk_index") or 0)
        except (TypeError, ValueError):
            existing_chunk_index = 0

        if embedding:
            rows.append({
                "id": hashlib.sha256(f"{source_id}:{existing_chunk_index}:{content}".encode()).hexdigest()[:32],
                "content": content[:4000],
                "embedding": embedding,
                "metadata": metadata,
                "source_id": source_id,
                "chunk_index": existing_chunk_index,
            })
        elif content and prechunked:
            vectors = embed([content], model=model, durable=durable_embedding_cache)
            vector = vectors[0] if vectors else None
            rows.append({
                "id": hashlib.sha256(f"{source_id}:{existing_chunk_index}:{content}".encode()).hexdigest()[:32],
                "content": content[:4000],
                "embedding": vector,
                "metadata": metadata,
                "source_id": source_id,
                "chunk_index": existing_chunk_index,
            })
        elif content:
            chunks = chunk_text(content, chunk_size=chunk_size, chunk_overlap=chunk_overlap)
            if not chunks:
                chunks = [content]
            embeddings = embed(chunks, model=model, durable=durable_embedding_cache)
            for idx, (chunk, vector) in enumerate(zip(chunks, embeddings)):
                rows.append({
                    "id": hashlib.sha256(f"{source_id}:{idx}:{chunk}".encode()).hexdigest()[:32],
                    "content": chunk[:4000],
                    "embedding": vector,
                    "metadata": metadata,
                    "source_id": source_id,
                    "chunk_index": idx,
                })
        else:
            # No content and no embedding: still index metadata as a sparse row.
            rows.append({
                "id": hashlib.sha256(f"{source_id}:0:metadata".encode()).hexdigest()[:32],
                "content": "",
                "embedding": None,
                "metadata": metadata,
                "source_id": source_id,
                "chunk_index": 0,
            })
    return rows
