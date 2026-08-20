"""Product documentation retrieval — the evidence behind every product answer.

The knowledge base held semantic patterns, synonyms, industry schemas and type
conversions: column-mapping material with no sentence about quarantine, CDC setup,
preflight gates or proof. So "what does quarantine mean" retrieved column noise at
0.3 similarity and an answer was narrated over it. This module makes the shipped
operator help (``help_corpus.json``, generated from ``apps/web/src/lib/helpDocs.ts``)
the retrievable corpus for those questions, and returns citations with every hit so
an answer can be checked against the article it came from.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from .lexical_index import Bm25Index, LexicalHit, content_terms

HELP_CORPUS_PATH = Path(__file__).with_name("help_corpus.json")

# A passage must cover this share of the question's informative terms before it is
# offered as evidence. Below it, the honest answer is "the documentation does not
# cover this" — never a fluent paragraph built from the best of the noise.
GROUNDING_FLOOR = 0.34

# BM25 alone ranks a passing mention in a short FAQ above the section written about
# the feature, because length normalization rewards brevity. A heading that names the
# question's terms is the strongest signal an operator's own eye uses, so ranking
# blends it in — and down-weights hits that cover little of the question.
TITLE_WEIGHT = 3.0


@dataclass(frozen=True)
class ProductDocChunk:
    """One help-article section, the unit that is retrieved and cited."""

    id: str
    doc_id: str
    doc_slug: str
    doc_title: str
    category: str
    section_id: str
    section_title: str
    text: str

    @property
    def citation(self) -> str:
        return f"{self.doc_title} → {self.section_title}"

    @property
    def href(self) -> str:
        return f"#/help/{self.doc_slug}"


@dataclass(frozen=True)
class ProductDocHit:
    """A retrieved section with the evidence measure that admitted it."""

    chunk: ProductDocChunk
    score: float
    grounding: float
    matched_terms: tuple[str, ...]

    def as_source(self) -> dict[str, object]:
        return {
            "title": self.chunk.citation,
            "doc": self.chunk.doc_title,
            "section": self.chunk.section_title,
            "href": f"{self.chunk.href}#{self.chunk.section_id}",
            "text": self.chunk.text,
            "score": round(self.score, 4),
            "grounding": round(self.grounding, 4),
            "matched_terms": list(self.matched_terms),
            "type": "product_doc",
        }


@lru_cache(maxsize=1)
def load_product_doc_chunks() -> tuple[ProductDocChunk, ...]:
    """Load the generated help corpus; an absent corpus yields no evidence, not a crash."""
    if not HELP_CORPUS_PATH.exists():
        return ()
    with HELP_CORPUS_PATH.open(encoding="utf-8") as fh:
        payload = json.load(fh)
    chunks: list[ProductDocChunk] = []
    for raw in payload.get("chunks", []):
        text = str(raw.get("text") or "").strip()
        if not text:
            continue
        chunks.append(
            ProductDocChunk(
                id=str(raw.get("id") or ""),
                doc_id=str(raw.get("doc_id") or ""),
                doc_slug=str(raw.get("doc_slug") or ""),
                doc_title=str(raw.get("doc_title") or ""),
                category=str(raw.get("category") or ""),
                section_id=str(raw.get("section_id") or ""),
                section_title=str(raw.get("section_title") or ""),
                text=text,
            )
        )
    return tuple(chunks)


@lru_cache(maxsize=1)
def _index() -> tuple[Bm25Index, dict[str, ProductDocChunk]]:
    chunks = load_product_doc_chunks()
    by_id = {c.id: c for c in chunks}
    return Bm25Index([(c.id, f"{c.doc_title}\n{c.text}") for c in chunks]), by_id


def _title_coverage(chunk: ProductDocChunk, terms: Sequence[str]) -> float:
    """Share of the question's terms named in the article and section headings."""
    if not terms:
        return 0.0
    heading = set(content_terms(f"{chunk.doc_title} {chunk.section_title}"))
    return sum(1 for t in terms if t in heading) / len(terms)


def product_doc_search(
    query: str,
    limit: int = 5,
    grounding_floor: float = GROUNDING_FLOOR,
) -> list[ProductDocHit]:
    """Documentation sections that actually cover the question, best first."""
    index, by_id = _index()
    terms = content_terms(query)
    hits: list[LexicalHit] = index.search(query, limit=max(limit * 4, 12))
    ranked: list[tuple[float, ProductDocHit]] = []
    for hit in hits:
        chunk = by_id.get(hit.id)
        if chunk is None or hit.grounding < grounding_floor:
            continue
        rank = hit.score * (0.4 + 0.6 * hit.grounding)
        rank += TITLE_WEIGHT * _title_coverage(chunk, terms)
        ranked.append(
            (
                rank,
                ProductDocHit(
                    chunk=chunk,
                    score=hit.score,
                    grounding=hit.grounding,
                    matched_terms=hit.matched_terms,
                ),
            )
        )
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return [hit for _, hit in ranked[:limit]]


@lru_cache(maxsize=1)
def corpus_vocabulary() -> frozenset[str]:
    """Every content term the shipped documentation actually uses."""
    terms: set[str] = set()
    for chunk in load_product_doc_chunks():
        terms.update(content_terms(f"{chunk.doc_title} {chunk.section_title} {chunk.text}"))
    return frozenset(terms)


def names_product_subject(query: str) -> bool:
    """Whether the question names anything the product documentation talks about.

    Keyword intent scoring fired on contentless phrases like "how do i", so
    "how do I cook rice" scored as product help and got a transfer blurb.
    """
    return bool(set(content_terms(query)) & corpus_vocabulary())


def nearest_articles(query: str, limit: int = 3) -> list[str]:
    """Closest article titles for a question nothing covers — a lead, not an answer."""
    index, by_id = _index()
    titles: list[str] = []
    for hit in index.search(query, limit=limit * 4):
        chunk = by_id.get(hit.id)
        if chunk is None:
            continue
        if chunk.doc_title not in titles:
            titles.append(chunk.doc_title)
        if len(titles) >= limit:
            break
    return titles


def compose_documented_answer(hits: Sequence[ProductDocHit], max_sections: int = 2) -> str:
    """Answer text quoted from the cited sections, so wording never drifts from the docs."""
    parts: list[str] = []
    for hit in hits[:max_sections]:
        chunk = hit.chunk
        body = chunk.text
        if body.startswith(chunk.section_title):
            body = body[len(chunk.section_title):].strip()
        parts.append(f"**{chunk.section_title}** — {body}")
    cited = " · ".join(hit.chunk.citation for hit in hits[:max_sections])
    if cited:
        parts.append(f"Source: {cited} (Help)")
    return "\n\n".join(parts)


def product_doc_documents() -> tuple[list[str], list[dict], list[str]]:
    """Help sections as vector-store documents so semantic search can reach them too."""
    texts: list[str] = []
    metas: list[dict] = []
    ids: list[str] = []
    for chunk in load_product_doc_chunks():
        texts.append(f"{chunk.doc_title}. {chunk.text}")
        metas.append(
            {
                "type": "product_doc",
                "doc_id": chunk.doc_id,
                "doc_title": chunk.doc_title,
                "doc_slug": chunk.doc_slug,
                "section_id": chunk.section_id,
                "section_title": chunk.section_title,
                "category": chunk.category,
            }
        )
        ids.append(f"doc_{chunk.id}")
    return texts, metas, ids
