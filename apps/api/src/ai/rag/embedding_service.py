"""
DataTransfer.space — Embedding Service

Semantic embedding generation with graceful fallback.
Uses sentence-transformers when available, falls back to TF-IDF.
"""

from __future__ import annotations
import hashlib
import math
import re
from typing import Optional

import numpy as np

_embedding_service: Optional["DataTransferEmbeddingService"] = None


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
        try:
            from sentence_transformers import SentenceTransformer
            self._model = SentenceTransformer(self.MODEL_NAME)
            self._backend = "sentence_transformers"
        except (ImportError, OSError, RuntimeError):
            self._backend = "tfidf_fallback"

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def dimension(self) -> int:
        if self._backend == "sentence_transformers":
            return self._model.get_sentence_embedding_dimension()
        return self.EMBEDDING_DIM

    def embed(self, texts: list[str]) -> np.ndarray:
        """Embed a list of texts into vectors."""
        if not texts:
            return np.array([])

        if self._backend == "sentence_transformers":
            return self._model.encode(texts, normalize_embeddings=True)

        return np.array([self._tfidf_embed(t) for t in texts])

    def embed_single(self, text: str) -> np.ndarray:
        """Embed a single text."""
        return self.embed([text])[0]

    def similarity(self, vec1: np.ndarray, vec2: np.ndarray) -> float:
        """Cosine similarity between two vectors."""
        dot = np.dot(vec1, vec2)
        norm1 = np.linalg.norm(vec1)
        norm2 = np.linalg.norm(vec2)
        if norm1 == 0 or norm2 == 0:
            return 0.0
        return float(dot / (norm1 * norm2))

    def _tokenize(self, text: str) -> list[str]:
        text = text.lower()
        tokens = re.findall(r"[a-z0-9]+", text)
        return tokens

    def _tfidf_embed(self, text: str) -> np.ndarray:
        """Fallback TF-IDF + hash embedding."""
        tokens = self._tokenize(text)
        vec = np.zeros(self.EMBEDDING_DIM)

        for token in tokens:
            h = int(hashlib.md5(token.encode()).hexdigest(), 16)
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

        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return vec


def get_embedding_service() -> DataTransferEmbeddingService:
    global _embedding_service
    if _embedding_service is None:
        _embedding_service = DataTransferEmbeddingService()
    return _embedding_service
