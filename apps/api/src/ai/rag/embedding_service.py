"""
Datawrap — Embedding Service

Semantic embedding generation with graceful fallback.
Uses sentence-transformers when available, falls back to TF-IDF.
"""

from __future__ import annotations

import hashlib
import logging
import math
import os
from services.brand_env import getenv_brand
import re
from typing import Optional

try:
    import numpy as np
except ImportError:  # slim API hosts — hashed TF-IDF still boots
    np = None  # type: ignore[assignment]

_logger = logging.getLogger(__name__)
_embedding_service: Optional["DataTransferEmbeddingService"] = None
Vec = list[float]


class DataTransferEmbeddingService:
    """Generate semantic embeddings for text."""

    MODEL_NAME = "all-MiniLM-L6-v2"
    EMBEDDING_DIM = 384

    def __init__(self):
        self._model = None
        self._backend = "fallback"
        self._vocab: dict[str, int] = {}
        self._idf: dict[str, float] = {}
        self._init_backend()

    def _init_backend(self):
        prefer = (getenv_brand("EMBEDDING_BACKEND") or "").strip().lower()
        if prefer in {"tfidf", "fallback", "off", "none"}:
            self._backend = "tfidf_fallback"
            return
        try:
            # Never let Pilot/chat wait on HuggingFace hub retries (can take minutes).
            os.environ.setdefault("HF_HUB_DISABLE_TELEMETRY", "1")
            from sentence_transformers import SentenceTransformer

            try:
                self._model = SentenceTransformer(self.MODEL_NAME, local_files_only=True)
            except Exception:
                allow_dl = (getenv_brand("ALLOW_EMBEDDING_DOWNLOAD") or "").lower() in {
                    "1",
                    "true",
                    "on",
                    "yes",
                }
                if not allow_dl:
                    raise
                self._model = SentenceTransformer(self.MODEL_NAME)
            self._backend = "sentence_transformers"
        except Exception as exc:
            _logger.info("Embedding backend falling back to TF-IDF (%s)", exc)
            self._backend = "tfidf_fallback"

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def dimension(self) -> int:
        if self._backend == "sentence_transformers":
            return self._model.get_sentence_embedding_dimension()
        return self.EMBEDDING_DIM

    def embed(self, texts: list[str]):
        """Embed a list of texts into vectors."""
        if not texts:
            return [] if np is None else np.array([])

        if self._backend == "sentence_transformers":
            return self._model.encode(texts, normalize_embeddings=True)

        rows = [self._tfidf_embed(t) for t in texts]
        return rows if np is None else np.array(rows)

    def embed_single(self, text: str):
        """Embed a single text."""
        return self.embed([text])[0]

    def similarity(self, vec1, vec2) -> float:
        """Cosine similarity between two vectors."""
        a = list(vec1)
        b = list(vec2)
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm1 = math.sqrt(sum(x * x for x in a))
        norm2 = math.sqrt(sum(y * y for y in b))
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return float(dot / (norm1 * norm2))

    def _tokenize(self, text: str) -> list[str]:
        text = text.lower()
        tokens = re.findall(r"[a-z0-9]+", text)
        return tokens

    def _tfidf_embed(self, text: str) -> Vec:
        """Fallback TF-IDF + hash embedding (no numpy required)."""
        tokens = self._tokenize(text)
        vec = [0.0] * self.EMBEDDING_DIM

        for token in tokens:
            h = int(hashlib.md5(token.encode(), usedforsecurity=False).hexdigest(), 16)
            idx = h % self.EMBEDDING_DIM
            sign = 1 if (h >> 8) % 2 == 0 else -1
            weight = 1.0 + math.log1p(tokens.count(token))
            vec[idx] += sign * weight

        # Add character n-gram features for abbreviations like "amt", "cust"
        for n in (2, 3):
            for i in range(len(text) - n + 1):
                ngram = text[i:i + n]
                h = int(hashlib.sha256(ngram.encode()).hexdigest(), 16)
                idx = h % self.EMBEDDING_DIM
                vec[idx] += 0.5

        norm = math.sqrt(sum(x * x for x in vec))
        if norm > 0:
            vec = [x / norm for x in vec]
        return vec


def get_embedding_service() -> DataTransferEmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = DataTransferEmbeddingService()
    return _embedding_service
