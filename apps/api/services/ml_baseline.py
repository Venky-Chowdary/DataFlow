"""Deterministic char n-gram baseline for the optional automap boost.

Why this exists: the baseline used to ship as a ``pickle`` of a scikit-learn
model, i.e. an arbitrary-code-execution artifact loaded inside the transfer
engine. A mapping *boost* is not worth that blast radius, and the pickle was
also unloadable in practice (the class was serialized as ``__main__.…``).

This module loads a plain JSON artifact (a target-name vocabulary) and scores
candidates with a self-contained character n-gram TF-IDF cosine — same shape of
signal as the old ``char_wb`` TF-IDF, no pickle, no sklearn at import time, and
byte-for-byte deterministic across processes and Python versions.
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from pathlib import Path

_NGRAM_MIN = 2
_NGRAM_MAX = 4
ARTIFACT_SCHEMA_VERSION = 1


def normalize_name(name: str) -> str:
    s = (name or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _ngrams(text: str) -> Counter[str]:
    # ``char_wb``-style: pad each word so boundaries carry signal.
    grams: Counter[str] = Counter()
    for word in (w for w in normalize_name(text).split("_") if w):
        padded = f" {word} "
        for n in range(_NGRAM_MIN, _NGRAM_MAX + 1):
            for i in range(len(padded) - n + 1):
                grams[padded[i : i + n]] += 1
    return grams


class CharNgramBaseline:
    """Cosine nearest-neighbour over a fixed target-name vocabulary."""

    def __init__(self, targets: list[str]):
        self.targets = [t for t in dict.fromkeys(normalize_name(t) for t in targets) if t]
        self._df: Counter[str] = Counter()
        docs: list[Counter[str]] = []
        for target in self.targets:
            grams = _ngrams(target)
            docs.append(grams)
            self._df.update(grams.keys())
        self._n_docs = max(1, len(docs))
        self._vectors = [self._weight(doc) for doc in docs]

    def _idf(self, gram: str) -> float:
        # Smoothed idf (sklearn convention) so unseen grams stay finite.
        return math.log((1 + self._n_docs) / (1 + self._df.get(gram, 0))) + 1.0

    def _weight(self, grams: Counter[str]) -> dict[str, float]:
        vec = {g: (1.0 + math.log(c)) * self._idf(g) for g, c in grams.items()}
        norm = math.sqrt(sum(v * v for v in vec.values()))
        if norm == 0.0:
            return {}
        return {g: v / norm for g, v in vec.items()}

    def predict_target(self, source: str) -> tuple[str, float]:
        if not self.targets:
            return "", 0.0
        query = self._weight(_ngrams(source))
        if not query:
            return "", 0.0
        best_target, best_score = "", 0.0
        for target, vec in zip(self.targets, self._vectors, strict=True):
            if len(query) > len(vec):
                score = sum(w * query.get(g, 0.0) for g, w in vec.items())
            else:
                score = sum(w * vec.get(g, 0.0) for g, w in query.items())
            if score > best_score:
                best_target, best_score = target, score
        return best_target, best_score


def load_baseline(path: Path) -> CharNgramBaseline | None:
    """Load the JSON vocabulary artifact, or ``None`` when absent/invalid."""
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path.name}: expected a JSON object artifact")
    version = int(payload.get("schema_version") or 0)
    if version != ARTIFACT_SCHEMA_VERSION:
        raise ValueError(
            f"{path.name}: unsupported schema_version {version} "
            f"(expected {ARTIFACT_SCHEMA_VERSION})"
        )
    targets = payload.get("targets")
    if not isinstance(targets, list) or not all(isinstance(t, str) for t in targets):
        raise ValueError(f"{path.name}: 'targets' must be a list of strings")
    return CharNgramBaseline(targets)
