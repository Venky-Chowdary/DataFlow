"""Typo tolerance for the workspace vocabulary operators type by hand.

Operators type ``conenctors?``, ``shcedules`` and ``list tabels on Demo Orders``.
Each of those reached no workspace tool at all and came back as "That is outside
what the Datawrap documentation covers", which reads as though the product does
not know what a connector is.

The correction is deliberately narrow, because the same tokens are also table and
column names and rewriting one of those would answer a question the operator did
not ask:

* only whole words of at least five characters,
* only within one Damerau-Levenshtein edit (which is what makes a transposition
  like ``shcedules`` reachable, unlike plain Levenshtein where it costs two),
* only when exactly one vocabulary word is that close, and
* only words whose near neighbours are not themselves plausible table names —
  ``contracts`` is left out on purpose, because ``contacts`` is one edit away and
  is a table in half the warehouses in the world.

The caller applies this as a *fallback*: it retries planning with the corrected
text and keeps the result only when the correction turns a documentation answer
into a real workspace read. A correction can therefore never change the meaning
of a question the router already understood.
"""

from __future__ import annotations

import re

# Nouns the router keys on. Excludes anything whose one-edit neighbourhood
# contains a common table name (see module docstring on ``contracts``).
WORKSPACE_VOCABULARY: frozenset[str] = frozenset(
    {
        "briefing",
        "collections",
        "column",
        "columns",
        "connection",
        "connections",
        "connector",
        "connectors",
        "database",
        "databases",
        "dataset",
        "datasets",
        "mapping",
        "mappings",
        "pipeline",
        "pipelines",
        "postgres",
        "postgresql",
        "preflight",
        "quarantine",
        "quarantined",
        "reconcile",
        "schedule",
        "schedules",
        "schema",
        "schemas",
        "snowflake",
        "table",
        "tables",
        "transfer",
        "transfers",
        "validate",
        "validation",
        "workspace",
    }
)

_MIN_LENGTH = 5
_WORD = re.compile(r"[A-Za-z]+")


def _within_one_edit(a: str, b: str) -> bool:
    """Damerau-Levenshtein distance of at most one, including transpositions."""
    if a == b:
        return True
    la, lb = len(a), len(b)
    if abs(la - lb) > 1:
        return False
    if la == lb:
        diffs = [i for i in range(la) if a[i] != b[i]]
        if len(diffs) == 1:
            return True
        if len(diffs) == 2:
            i, j = diffs
            return j == i + 1 and a[i] == b[j] and a[j] == b[i]
        return False
    # One insertion or deletion: walk the longer word looking for a single skip.
    longer, shorter = (a, b) if la > lb else (b, a)
    i = j = 0
    skipped = False
    while i < len(longer) and j < len(shorter):
        if longer[i] == shorter[j]:
            i += 1
            j += 1
            continue
        if skipped:
            return False
        skipped = True
        i += 1
    return True


def correct_word(word: str) -> str:
    """The vocabulary word this token is a typo of, or the token unchanged."""
    token = word.lower()
    if len(token) < _MIN_LENGTH or token in WORKSPACE_VOCABULARY:
        return word
    matches = [v for v in WORKSPACE_VOCABULARY if _within_one_edit(token, v)]
    if not matches:
        return word
    # A singular and its plural are one noun, and both sit one edit from a typo
    # like ``pipelins``. Requiring a single candidate rejected exactly the words
    # the vocabulary holds in both forms, so collapse that pair first and keep
    # the form whose length the operator actually typed.
    if len({m.rstrip("s") for m in matches}) != 1:
        return word
    # Keep the operator's plurality: ``pipelins`` is a plural and routes to the
    # pipeline *list*, while the singular reaches the documentation instead.
    plural = token.endswith("s")
    return min(
        matches,
        key=lambda m: (m.endswith("s") != plural, abs(len(m) - len(token))),
    )


def correct_workspace_typos(message: str) -> str:
    """Rewrite near-miss workspace nouns, leaving every other word alone."""
    if not message:
        return message
    return _WORD.sub(lambda m: correct_word(m.group(0)), message)


def corrected_tokens(message: str) -> dict[str, str]:
    """The misspelled words in this message mapped to their vocabulary form."""
    out: dict[str, str] = {}
    for match in _WORD.finditer(message or ""):
        word = match.group(0)
        fixed = correct_word(word)
        if fixed.lower() != word.lower():
            out[word.lower()] = fixed
    return out
