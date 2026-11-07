"""Trained intent/act router for Datawrap Pilot.

A linear softmax classifier over hashed word unigrams, word bigrams and
character 3–5-grams (the FastText recipe, Joulin et al. 2017). It runs in
well under a millisecond, needs no downloaded weights, and — unlike a keyword
table — keeps firing when the operator misspells, reorders, or paraphrases.

It abstains. Below a confidence floor or a top-two margin the router returns
``None`` and the caller falls through to the existing routing. The router is
a *pre-retrieval* signal that decides who should answer a turn; it is never
the security boundary for a mutation or a tenant check.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .intent_labels import ACTS

VERSION = 1
ARTIFACT_NAME = "pilot_intent_v1.npz"
HASH_SIZE = 1 << 15
_HASH_PERSON = b"df-intent1"
_WORD_RE = re.compile(r"[a-z0-9_']+")
_NGRAM_MIN = 3
_NGRAM_MAX = 5

DEFAULT_MIN_CONFIDENCE = 0.55
DEFAULT_MIN_MARGIN = 0.20


def default_artifact_path() -> Path:
    return Path(__file__).resolve().parent / "artifacts" / ARTIFACT_NAME


def _bucket(feature: str) -> int:
    digest = hashlib.blake2b(feature.encode("utf-8"), digest_size=8, person=_HASH_PERSON).digest()
    return int.from_bytes(digest, "little") % HASH_SIZE


def features(text: str) -> list[int]:
    """Hashed feature ids for one turn (duplicates kept; the vector counts them)."""
    words = _WORD_RE.findall((text or "").lower())
    ids: list[int] = []
    for i, w in enumerate(words):
        ids.append(_bucket(f"w:{w}"))
        if i + 1 < len(words):
            ids.append(_bucket(f"b:{w}_{words[i + 1]}"))
        marked = f"<{w}>"
        for n in range(_NGRAM_MIN, _NGRAM_MAX + 1):
            for j in range(0, len(marked) - n + 1):
                ids.append(_bucket(f"c:{marked[j : j + n]}"))
    if words:
        ids.append(_bucket(f"first:{words[0]}"))
        ids.append(_bucket(f"len:{min(len(words), 12)}"))
    return ids


def vectorize(texts: list[str]) -> np.ndarray:
    """L2-normalised count vectors, shape ``(n, HASH_SIZE)``."""
    X: np.ndarray = np.zeros((len(texts), HASH_SIZE), dtype=np.float32)
    for row, text in enumerate(texts):
        ids = features(text)
        if not ids:
            continue
        np.add.at(X[row], ids, 1.0)
        X[row] = np.log1p(X[row])
        norm = float(np.linalg.norm(X[row]))
        if norm > 0:
            X[row] /= norm
    return X


@dataclass(frozen=True)
class Routing:
    act: str
    confidence: float
    margin: float
    runner_up: str


@dataclass
class IntentRouter:
    weights: np.ndarray  # (HASH_SIZE, n_acts)
    bias: np.ndarray  # (n_acts,)
    acts: tuple[str, ...] = ACTS
    version: int = VERSION

    def probabilities(self, text: str) -> np.ndarray:
        x = vectorize([text])[0]
        logits = x @ self.weights + self.bias
        logits -= logits.max()
        p = np.exp(logits)
        out: np.ndarray = p / p.sum()
        return out

    def route(
        self,
        text: str,
        *,
        min_confidence: float = DEFAULT_MIN_CONFIDENCE,
        min_margin: float = DEFAULT_MIN_MARGIN,
    ) -> Routing | None:
        """Best act, or ``None`` when the router is not sure enough to speak."""
        if not (text or "").strip():
            return None
        p = self.probabilities(text)
        order = np.argsort(-p)
        top, second = int(order[0]), int(order[1])
        conf, margin = float(p[top]), float(p[top] - p[second])
        if conf < min_confidence or margin < min_margin:
            return None
        return Routing(self.acts[top], conf, margin, self.acts[second])

    def save(self, path: Path | None = None) -> Path:
        dest = path or default_artifact_path()
        dest.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(
            dest,
            version=np.asarray([self.version], dtype=np.int32),
            hash_size=np.asarray([HASH_SIZE], dtype=np.int32),
            weights=self.weights.astype(np.float32),
            bias=self.bias.astype(np.float32),
            acts=np.asarray(self.acts, dtype=object),
        )
        return dest


def train(
    examples: list[tuple[str, str]],
    *,
    epochs: int = 300,
    lr: float = 20.0,
    l2: float = 1e-5,
    seed: int = 7,
) -> IntentRouter:
    """Full-batch softmax regression with L2, deterministic for a seed."""
    rng = np.random.default_rng(seed)
    acts = tuple(ACTS)
    index = {a: i for i, a in enumerate(acts)}
    X = vectorize([t for t, _ in examples])
    y = np.asarray([index[a] for _, a in examples], dtype=np.int64)
    n, k = len(examples), len(acts)
    Y: np.ndarray = np.zeros((n, k), dtype=np.float32)
    Y[np.arange(n), y] = 1.0
    # Balance classes so a 30-seed act does not out-vote an 18-seed act.
    counts = np.bincount(y, minlength=k).astype(np.float32)
    sample_w = (n / (k * counts))[y][:, None]
    W = (rng.standard_normal((HASH_SIZE, k)) * 0.01).astype(np.float32)
    b: np.ndarray = np.zeros(k, dtype=np.float32)
    for _ in range(epochs):
        logits = X @ W + b
        logits -= logits.max(axis=1, keepdims=True)
        P = np.exp(logits)
        P /= P.sum(axis=1, keepdims=True)
        G = (P - Y) * sample_w / n
        W -= lr * (X.T @ G + l2 * W)
        b -= lr * G.sum(axis=0)
    return IntentRouter(weights=W, bias=b, acts=acts)


def load_router(path: Path | None = None) -> IntentRouter:
    src = path or default_artifact_path()
    with np.load(src, allow_pickle=True) as z:
        if int(z["hash_size"][0]) != HASH_SIZE:
            raise ValueError("intent artifact was built with a different feature space")
        acts = tuple(str(a) for a in z["acts"].tolist())
        return IntentRouter(
            weights=z["weights"].astype(np.float32),
            bias=z["bias"].astype(np.float32),
            acts=acts,
            version=int(z["version"][0]),
        )


_ROUTER: IntentRouter | None = None
_ROUTER_FAILED = False


def get_router() -> IntentRouter | None:
    """Process-wide router, or ``None`` when the artifact is missing/corrupt."""
    global _ROUTER, _ROUTER_FAILED
    if _ROUTER is None and not _ROUTER_FAILED:
        try:
            _ROUTER = load_router()
        except (OSError, ValueError, KeyError):
            _ROUTER_FAILED = True
    return _ROUTER


def route_intent(text: str) -> Routing | None:
    router = get_router()
    return router.route(text) if router is not None else None
