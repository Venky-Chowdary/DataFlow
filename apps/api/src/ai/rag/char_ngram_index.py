"""Character n-gram retrieval — the recall the word index cannot reach.

BM25 matches whole normalized terms, so it is blind to everything that changes a
word's spelling: ``reconcile`` vs ``reconciliation``, ``dedupe`` vs ``deduped``,
``postgress`` typed for ``postgres``, ``full-refresh`` typed for
``full_refresh``. The stemmer in ``lexical_index`` closes a handful of English
suffixes and nothing else, and the shipped alias table is a fixed list of
connector nicknames.

A character n-gram vector space closes that gap without a model download, which
matters because this product must answer on an air-gapped host: the sentence
transformer is an optional extra and the hashed fallback embedding it degrades to
scores every passage 0.2–0.4 against every question, which is no signal at all.

This is a lexical retriever, not a semantic one — it will not connect *bad rows*
to *quarantine* (that is ``query_analysis``'s job). What it does is make the
matching robust to how a word is spelled, and it is fused with BM25 rather than
replacing it, because on exact terminology BM25 ranks better.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable
from dataclasses import dataclass

# 4 is the usual sweet spot for English retrieval n-grams: long enough that a
# gram is not shared by every word, short enough to survive a suffix change.
NGRAM_SIZE = 4

# Grams shared by more than this share of passages describe the corpus, not the
# question, and only add noise to the cosine.
MAX_DOC_SHARE = 0.55

_NON_WORD = re.compile(r"[^a-z0-9]+")


def _grams(text: str, n: int = NGRAM_SIZE) -> list[str]:
    """Character n-grams over word-boundary-padded text.

    Padding with a single space on each side of every word keeps prefix and
    suffix grams distinct from interior ones, so ``_dedu`` (word start) does not
    collide with the ``dedu`` inside an unrelated word.
    """
    flat = _NON_WORD.sub(" ", str(text or "").lower()).strip()
    if not flat:
        return []
    out: list[str] = []
    for word in flat.split():
        padded = f" {word} "
        if len(padded) <= n:
            out.append(padded)
            continue
        out.extend(padded[i : i + n] for i in range(len(padded) - n + 1))
    return out


@dataclass(frozen=True)
class NgramHit:
    id: str
    score: float


class CharNgramIndex:
    """Cosine similarity over IDF-weighted character n-gram vectors."""

    def __init__(self, documents: Iterable[tuple[str, str]], n: int = NGRAM_SIZE) -> None:
        self._n = n
        self._ids: list[str] = []
        raw_counts: list[dict[str, int]] = []
        doc_freq: dict[str, int] = {}

        for doc_id, text in documents:
            counts: dict[str, int] = {}
            for gram in _grams(text, n):
                counts[gram] = counts.get(gram, 0) + 1
            self._ids.append(doc_id)
            raw_counts.append(counts)
            for gram in counts:
                doc_freq[gram] = doc_freq.get(gram, 0) + 1

        self._count = len(self._ids)
        ceiling = max(1, int(self._count * MAX_DOC_SHARE))
        self._idf = {
            gram: math.log((self._count + 1) / (df + 0.5))
            for gram, df in doc_freq.items()
            if df <= ceiling
        }
        self._vectors: list[dict[str, float]] = [
            self._weight(counts) for counts in raw_counts
        ]

    @property
    def document_count(self) -> int:
        return self._count

    def _weight(self, counts: dict[str, int]) -> dict[str, float]:
        """Sub-linear term frequency times IDF, L2-normalized."""
        vec: dict[str, float] = {}
        for gram, tf in counts.items():
            idf = self._idf.get(gram)
            if not idf:
                continue
            vec[gram] = (1.0 + math.log(tf)) * idf
        norm = math.sqrt(sum(v * v for v in vec.values()))
        if norm <= 0:
            return {}
        return {gram: v / norm for gram, v in vec.items()}

    def search(self, query: str, limit: int = 10) -> list[NgramHit]:
        counts: dict[str, int] = {}
        for gram in _grams(query, self._n):
            counts[gram] = counts.get(gram, 0) + 1
        qvec = self._weight(counts)
        if not qvec:
            return []
        hits: list[NgramHit] = []
        for doc_id, dvec in zip(self._ids, self._vectors):
            if not dvec:
                continue
            # Iterate the shorter side; query vectors are far smaller.
            score = sum(w * dvec.get(gram, 0.0) for gram, w in qvec.items())
            if score > 0:
                hits.append(NgramHit(id=doc_id, score=score))
        hits.sort(key=lambda h: h.score, reverse=True)
        return hits[:limit]
