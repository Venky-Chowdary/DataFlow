"""Okapi BM25 lexical retrieval with an explainable grounding measure.

The hashed TF-IDF fallback embedding scores every passage 0.2–0.4 against every
question, so a product question retrieved column-semantics noise and the
generator narrated over it. Ranking prose needs term statistics, not a 384-slot
hash: BM25 is the standard for that, and it needs no model download on an air-gapped
host.

Ranking alone still cannot say *whether* a passage answers the question, so each
hit also carries ``grounding`` — the share of the question's informative terms
(IDF-weighted) the passage actually covers. That is the number a floor can be set
on and the number an operator can be shown, which is what keeps an answer from
being composed out of passages that merely ranked first.
"""

from __future__ import annotations

import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

K1 = 1.2
B = 0.75

_TOKEN_RE = re.compile(r"[a-z0-9_]+")

# Operators type the short name; the documentation spells the product name. Without
# these, "CDC from Postgres" matched no passage that says PostgreSQL.
ALIASES = {
    "postgres": "postgresql",
    "pg": "postgresql",
    "psql": "postgresql",
    "mongo": "mongodb",
    "mssql": "sqlserver",
    "sql_server": "sqlserver",
    "bq": "bigquery",
    "snowflke": "snowflake",
    "docs": "documentation",
    "doc": "documentation",
    "auth": "authentication",
    "perms": "permissions",
    "perm": "permission",
    "config": "configuration",
    "creds": "credentials",
}


_SIBILANT_TAILS = ("s", "x", "z", "ch", "sh")


def _stem(token: str) -> str:
    """Strip the few English suffixes that split a term from its own documentation.

    ``-es`` only loses the ``e`` after a sibilant (``batches`` → ``batch``); elsewhere
    it drops the ``s`` alone, so ``tables`` and ``table`` reach the same term instead
    of stemming to ``tabl`` and ``table``.
    """
    for suffix in ("ies", "ing", "ed", "es", "s"):
        if len(token) > len(suffix) + 3 and token.endswith(suffix):
            if suffix == "ies":
                return f"{token[:-3]}y"
            if suffix == "es":
                stem = token[:-2]
                return stem if stem.endswith(_SIBILANT_TAILS) else token[:-1]
            return token[: -len(suffix)]
    return token


def normalize(token: str) -> str:
    """Canonical index/query form of one token."""
    return ALIASES.get(token, _stem(token))


# Deliberately small: only words that carry no retrieval signal in operator
# questions. Domain words ("job", "run", "map") stay — they are the subject here.
_STOPWORD_WORDS = """
    a an the and or but if then than that this these those there here
    i me my we our you your it its is are was were be been being am
    do does did doing done have has had having
    can could should would will shall may might must
    of in on at to for from by with without into onto about as
    what which who whom whose when where why how
    not no nor so too very just also only
    please tell show explain mean means help
    """.split()
STOPWORDS = frozenset(_STOPWORD_WORDS) | frozenset(normalize(w) for w in _STOPWORD_WORDS)


def tokenize(text: str) -> list[str]:
    """Normalized word tokens, snake_case preserved so ``full_refresh`` stays one term."""
    return [normalize(t) for t in _TOKEN_RE.findall(str(text or "").lower())]


def content_terms(text: str) -> list[str]:
    """Question terms that carry retrieval signal, in order, deduplicated."""
    seen: set[str] = set()
    out: list[str] = []
    for token in tokenize(text):
        if token in STOPWORDS or len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out


@dataclass(frozen=True)
class LexicalHit:
    """One ranked passage with the evidence measure a floor can be set on."""

    id: str
    score: float
    grounding: float
    matched_terms: tuple[str, ...]


class Bm25Index:
    """Immutable BM25 index over a fixed passage set."""

    def __init__(self, documents: Iterable[tuple[str, str]]) -> None:
        self._ids: list[str] = []
        self._term_freq: list[dict[str, int]] = []
        self._lengths: list[int] = []
        doc_freq: dict[str, int] = {}

        for doc_id, text in documents:
            tokens = [t for t in tokenize(text) if t not in STOPWORDS]
            freq: dict[str, int] = {}
            for token in tokens:
                freq[token] = freq.get(token, 0) + 1
            self._ids.append(doc_id)
            self._term_freq.append(freq)
            self._lengths.append(len(tokens))
            for term in freq:
                doc_freq[term] = doc_freq.get(term, 0) + 1

        self._count = len(self._ids)
        self._avg_len = (sum(self._lengths) / self._count) if self._count else 0.0
        self._idf = {
            term: math.log(1.0 + (self._count - df + 0.5) / (df + 0.5))
            for term, df in doc_freq.items()
        }

    @property
    def document_count(self) -> int:
        return self._count

    def idf(self, term: str) -> float:
        """IDF of a term, 0.0 for terms the corpus never uses."""
        return self._idf.get(term, 0.0)

    def search(self, query: str, limit: int = 5) -> list[LexicalHit]:
        """Passages ranked by BM25, each carrying its grounding share."""
        terms = content_terms(query)
        if not self._count or not terms:
            return []
        # An unknown term still counts against grounding: a question about a word
        # the documentation never uses is exactly the case that must not look
        # answered. Give it the IDF a term seen once would earn.
        unseen_idf = math.log(1.0 + (self._count + 0.5) / 1.5)
        query_mass = sum(self.idf(t) or unseen_idf for t in terms)
        if query_mass <= 0:
            return []

        hits: list[LexicalHit] = []
        for i, doc_id in enumerate(self._ids):
            freq = self._term_freq[i]
            length = self._lengths[i] or 1
            score = 0.0
            matched: list[str] = []
            covered = 0.0
            for term in terms:
                tf = freq.get(term, 0)
                if not tf:
                    continue
                idf = self.idf(term)
                denom = tf + K1 * (1.0 - B + B * length / (self._avg_len or 1.0))
                score += idf * (tf * (K1 + 1.0)) / denom
                covered += idf or unseen_idf
                matched.append(term)
            if not matched:
                continue
            hits.append(
                LexicalHit(
                    id=doc_id,
                    score=score,
                    grounding=min(covered / query_mass, 1.0),
                    matched_terms=tuple(matched),
                )
            )

        hits.sort(key=lambda h: (h.score, h.grounding), reverse=True)
        return hits[:limit]


def build_index(documents: Sequence[tuple[str, str]]) -> Bm25Index:
    return Bm25Index(documents)
