"""Reciprocal Rank Fusion — how two retrievers become one ranking.

Blending retriever *scores* requires them to be on a comparable scale, and they
are not: BM25 is an unbounded sum of IDF terms while a cosine is bounded by 1.
Normalizing either one per query makes the blend depend on how good the best hit
happened to be, which is exactly the instability that makes a tuned weight stop
working when the corpus grows.

Reciprocal Rank Fusion (Cormack, Clarke & Buettcher, SIGIR 2009) combines
*ranks* instead, so it needs no per-retriever calibration and no tuning beyond
the single ``k`` damping constant. It is what Elasticsearch, Vespa and Weaviate
use for hybrid search, and it is the right default here: a passage both
retrievers rank highly outranks one that only the stronger retriever liked.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

# The constant from the original paper. It damps the contribution of top ranks so
# a single retriever's first place cannot dominate agreement between retrievers.
RRF_K = 60.0


@dataclass(frozen=True)
class FusedHit:
    """One passage with its fused score and where each retriever ranked it."""

    id: str
    score: float
    ranks: dict[str, int]

    def rank_in(self, retriever: str) -> int | None:
        return self.ranks.get(retriever)


def reciprocal_rank_fusion(
    rankings: Sequence[tuple[str, Sequence[str]]],
    *,
    weights: dict[str, float] | None = None,
    k: float = RRF_K,
    limit: int = 10,
) -> list[FusedHit]:
    """Fuse named rankings of passage ids into one ranking.

    ``rankings`` is a sequence of ``(retriever_name, ordered_ids)``. Weights let
    a retriever count for more without reintroducing score calibration — the
    contribution stays a function of rank only.
    """
    weights = weights or {}
    scores: dict[str, float] = {}
    ranks: dict[str, dict[str, int]] = {}

    for name, ordered in rankings:
        weight = float(weights.get(name, 1.0))
        if weight <= 0:
            continue
        for position, doc_id in enumerate(ordered, start=1):
            scores[doc_id] = scores.get(doc_id, 0.0) + weight / (k + position)
            ranks.setdefault(doc_id, {})[name] = position

    fused = [
        FusedHit(id=doc_id, score=score, ranks=ranks.get(doc_id, {}))
        for doc_id, score in scores.items()
    ]
    # Ties break toward the passage the most retrievers found, then toward the
    # better single rank — both are more informative than dictionary order.
    fused.sort(
        key=lambda h: (h.score, len(h.ranks), -min(h.ranks.values(), default=10**6)),
        reverse=True,
    )
    return fused[:limit]
