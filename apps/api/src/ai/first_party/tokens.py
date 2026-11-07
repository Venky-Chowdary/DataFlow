"""Hashed word + character n-gram features — the FastText tokenizer.

Bojanowski et al. (TACL 2017) showed that a word is not an atomic id: its
character n-grams still fire when the operator types ``wal_lvl``, ``binlogs``,
or ``quarantined``. That is the open-English problem this package exists to
solve. We do **not** download a word table; every feature is hashed into a
fixed number of buckets so training and serve stay air-gapped.

The hash is keyed (BLAKE2b person-string) so it is stable across processes
and Python ``hash()`` randomization. Bucket collisions are expected and are
what the learned projection later untangles.
"""

from __future__ import annotations

import hashlib
import re

HASH_SIZE = 2048
CHAR_NGRAM_MIN = 3
CHAR_NGRAM_MAX = 5
# Person-string pins the feature space so a retrained checkpoint stays aligned
# with the tokenizer that built it.
_HASH_PERSON = b"df-fp-v1"

_WORD_RE = re.compile(r"[a-z0-9_]+")


def word_tokens(text: str) -> list[str]:
    """Lowercased word tokens, including ``snake_case`` identifiers."""
    return _WORD_RE.findall((text or "").lower())


def char_ngrams(word: str) -> list[str]:
    """Character n-grams of one word, including boundary marks.

    FastText wraps a word as ``<word>`` so the edges of a short identifier
    (``<cdc>``, ``<g3>``) still produce features when the word is shorter
    than the n-gram window.
    """
    if not word:
        return []
    marked = f"<{word}>"
    out: list[str] = []
    for n in range(CHAR_NGRAM_MIN, CHAR_NGRAM_MAX + 1):
        if len(marked) < n:
            continue
        for i in range(0, len(marked) - n + 1):
            out.append(marked[i : i + n])
    return out


def feature_id(feature: str) -> int:
    """Deterministic bucket in ``[0, HASH_SIZE)``."""
    digest = hashlib.blake2b(
        feature.encode("utf-8"),
        digest_size=8,
        person=_HASH_PERSON,
    ).digest()
    return int.from_bytes(digest, "little") % HASH_SIZE


def hashed_features(text: str) -> list[int]:
    """Word buckets plus every character n-gram of those words.

    Empty text returns no features; the encoder maps that to the zero vector
    rather than inventing a mean over an empty slice.
    """
    ids: list[int] = []
    for word in word_tokens(text):
        ids.append(feature_id(f"w:{word}"))
        for ngram in char_ngrams(word):
            ids.append(feature_id(f"c:{ngram}"))
    return ids


# Closed fluency words the pointer-generator may emit without copying.
# None of these name a warehouse, a CDC delivery guarantee, dbt, or SSH.
GLUE_WORDS: tuple[str, ...] = (
    "a",
    "an",
    "and",
    "behavior",
    "by",
    "default",
    "does",
    "documented",
    "for",
    "here",
    "in",
    "is",
    "it",
    "on",
    "product",
    "short",
    "the",
    "this",
    "what",
)


def is_glue_token(token: str) -> bool:
    return (token or "").lower() in GLUE_WORDS


# Closed conversational words the first-party seq2seq may generate.
# Pointer-gen GLUE is prefix-only; dialogue needs yes/today/confirm/pipelines.
# Still no warehouse name, dbt, ssh, exactly, or once.
DIALOGUE_GLUE: tuple[str, ...] = GLUE_WORDS + (
    "yes",
    "no",
    "i",
    "am",
    "you",
    "your",
    "we",
    "me",
    "my",
    "today",
    "utc",
    "keep",
    "workspace",
    "datawrap",
    "pilot",
    "personal",
    "calendar",
    "not",
    "have",
    "has",
    "are",
    "will",
    "can",
    "do",
    "pipelines",
    "pipeline",
    "parked",
    "approval",
    "approves",
    "approve",
    "enabled",
    "due",
    "run",
    "running",
    "fire",
    "tick",
    "until",
    "someone",
    "those",
    "they",
    "them",
    "these",
    "that",
    "none",
    "nothing",
    "yet",
    "so",
    "if",
    "or",
    "then",
    "scheduled",
    "schedules",
    "cadence",
    "create",
    "one",
    "after",
    "transfer",
    "see",
    "saved",
    "connectors",
    "connector",
    "recent",
    "jobs",
    "job",
    "failed",
    "need",
    "look",
    "of",
    "paste",
    "host",
    "url",
    "connection",
    "stage",
    "confirm",
    "already",
    "name",
    "engine",
    "want",
    "walk",
    "fields",
    "source",
    "destination",
    "propose",
    "sync",
    "mode",
    "writes",
    "accept",
    "hello",
    "hey",
    "hi",
    "to",
    "from",
    "with",
    "at",
    "as",
    "up",
    "read",
    "live",
    "compose",
    "evidence",
    "general",
    "chatbot",
    "and",
)
