"""Dual encoder: hashed n-grams in, cosine in a learned space.

This is the open-English brain. Two towers share a hashed embedding table
and have their own projection:

* query tower  ``W_q @ mean(E[features(question)])``
* gold tower   ``W_p @ mean(E[features(gold_question)])``

Training is symmetric InfoNCE (van den Oord et al.; Karpukhin et al. DPR):
in a batch of ``(query, gold)`` pairs the matching gold is the positive
and every other gold in the batch is a negative. That is what makes
``gotta have logical wal`` sit next to ``do I need wal_level logical``
and sit far from ``how do I cook rice``.

No PyTorch. Gradients are the textbook ones for cosine + cross-entropy,
implemented in NumPy so CI and air-gapped hosts can retrain in seconds.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np

from .tokens import HASH_SIZE, hashed_features

DIM = 32
TEMPERATURE = 0.07
DEFAULT_SEED = 7


def _l2_normalize(matrix: np.ndarray, eps: float = 1e-8) -> tuple[np.ndarray, np.ndarray]:
    norms = np.linalg.norm(matrix, axis=1, keepdims=True)
    return matrix / (norms + eps), norms


def _softmax(logits: np.ndarray) -> np.ndarray:
    shifted = logits - logits.max(axis=1, keepdims=True)
    exp = np.exp(shifted)
    return exp / (exp.sum(axis=1, keepdims=True) + 1e-8)


def _d_l2_normalize(unit: np.ndarray, norms: np.ndarray, d_unit: np.ndarray) -> np.ndarray:
    """Jacobian of ``u = v / ||v||`` applied to an upstream gradient."""
    projected = d_unit - unit * np.sum(d_unit * unit, axis=1, keepdims=True)
    return projected / (norms + 1e-8)


@dataclass
class DualEncoder:
    """Learned query/gold towers over a shared hashed feature table."""

    embeddings: np.ndarray
    query_proj: np.ndarray
    gold_proj: np.ndarray
    dim: int = DIM
    hash_size: int = HASH_SIZE

    @classmethod
    def init(cls, *, dim: int = DIM, hash_size: int = HASH_SIZE, seed: int = DEFAULT_SEED) -> DualEncoder:
        rng = np.random.default_rng(seed)
        scale = 0.05
        eye = np.eye(dim, dtype=np.float64)
        return cls(
            embeddings=rng.normal(0.0, scale, (hash_size, dim)).astype(np.float64),
            query_proj=eye + rng.normal(0.0, 0.01, (dim, dim)),
            gold_proj=eye + rng.normal(0.0, 0.01, (dim, dim)),
            dim=dim,
            hash_size=hash_size,
        )

    def _pooled(self, text: str) -> np.ndarray:
        feats = hashed_features(text)
        if not feats:
            return np.zeros(self.dim, dtype=np.float64)
        return self.embeddings[np.asarray(feats, dtype=np.int64)].mean(axis=0)

    def _project(self, pooled: np.ndarray, proj: np.ndarray) -> np.ndarray:
        vector = proj @ pooled
        norm = float(np.linalg.norm(vector))
        if norm <= 1e-8:
            return np.zeros(self.dim, dtype=np.float64)
        return vector / norm

    def encode_query(self, text: str) -> np.ndarray:
        return self._project(self._pooled(text), self.query_proj)

    def encode_gold(self, text: str) -> np.ndarray:
        return self._project(self._pooled(text), self.gold_proj)

    def cosine(self, query: str, gold: str) -> float:
        left = self.encode_query(query)
        right = self.encode_gold(gold)
        return float(left @ right)

    def encode_queries(self, texts: list[str]) -> np.ndarray:
        return np.stack([self.encode_query(t) for t in texts], axis=0)

    def encode_golds(self, texts: list[str]) -> np.ndarray:
        return np.stack([self.encode_gold(t) for t in texts], axis=0)

    def _batch_pooled(self, texts: list[str]) -> tuple[np.ndarray, list[list[int]]]:
        feats: list[list[int]] = [hashed_features(t) for t in texts]
        pooled = np.zeros((len(texts), self.dim), dtype=np.float64)
        for i, buckets in enumerate(feats):
            if buckets:
                pooled[i] = self.embeddings[np.asarray(buckets, dtype=np.int64)].mean(axis=0)
        return pooled, feats

    def train_infonce(
        self,
        pairs: list[tuple[str, str]],
        *,
        epochs: int = 8,
        batch_size: int = 16,
        lr: float = 0.06,
        temperature: float = TEMPERATURE,
        seed: int = DEFAULT_SEED,
    ) -> list[float]:
        """Symmetric in-batch InfoNCE. Returns mean loss per epoch."""
        if not pairs:
            return []
        rng = np.random.default_rng(seed)
        history: list[float] = []
        work = list(pairs)
        for _epoch in range(epochs):
            rng.shuffle(work)
            total = 0.0
            steps = 0
            for start in range(0, len(work), batch_size):
                batch = work[start : start + batch_size]
                if len(batch) < 2:
                    continue
                total += self._step(batch, lr=lr, temperature=temperature)
                steps += 1
            history.append(total / max(steps, 1))
        return history

    def _step(
        self,
        batch: list[tuple[str, str]],
        *,
        lr: float,
        temperature: float,
    ) -> float:
        queries = [q for q, _ in batch]
        golds = [g for _, g in batch]
        h_q, feats_q = self._batch_pooled(queries)
        h_p, feats_p = self._batch_pooled(golds)
        v_q = h_q @ self.query_proj.T
        v_p = h_p @ self.gold_proj.T
        u_q, n_q = _l2_normalize(v_q)
        u_p, n_p = _l2_normalize(v_p)

        logits = (u_q @ u_p.T) / temperature
        labels = np.arange(len(batch))
        row = _softmax(logits)
        col = _softmax(logits.T)
        loss_row = -np.log(np.clip(row[labels, labels], 1e-12, 1.0)).mean()
        loss_col = -np.log(np.clip(col[labels, labels], 1e-12, 1.0)).mean()

        d_row = row / len(batch)
        d_row[labels, labels] -= 1.0 / len(batch)
        d_col = col / len(batch)
        d_col[labels, labels] -= 1.0 / len(batch)
        d_logits = d_row + d_col.T

        d_u_q = (d_logits @ u_p) / temperature
        d_u_p = (d_logits.T @ u_q) / temperature
        d_v_q = _d_l2_normalize(u_q, n_q, d_u_q)
        d_v_p = _d_l2_normalize(u_p, n_p, d_u_p)

        self.query_proj -= lr * (d_v_q.T @ h_q)
        self.gold_proj -= lr * (d_v_p.T @ h_p)
        d_h_q = d_v_q @ self.query_proj
        d_h_p = d_v_p @ self.gold_proj
        self._scatter_embedding_grad(feats_q, d_h_q, lr)
        self._scatter_embedding_grad(feats_p, d_h_p, lr)
        return float(loss_row + loss_col)

    def _scatter_embedding_grad(
        self,
        feats: list[list[int]],
        d_pooled: np.ndarray,
        lr: float,
    ) -> None:
        for i, buckets in enumerate(feats):
            if not buckets:
                continue
            grad = (d_pooled[i] / len(buckets)) * lr
            idx = np.asarray(buckets, dtype=np.int64)
            self.embeddings[idx] -= grad


def nearest_gold(
    encoder: DualEncoder,
    query: str,
    golds: list[str],
    gold_vectors: np.ndarray | None = None,
) -> tuple[str, float]:
    """Return ``(gold, cosine)`` for the nearest canonical question."""
    if not golds:
        return "", -1.0
    q = encoder.encode_query(query)
    matrix = gold_vectors if gold_vectors is not None else encoder.encode_golds(golds)
    scores = matrix @ q
    index = int(np.argmax(scores))
    return golds[index], float(scores[index])
