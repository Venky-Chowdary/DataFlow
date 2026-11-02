"""Spelling repair against the product's own vocabulary.

``wat is gaet 8 reconcilation`` retrieved nothing because three of its four
content words exist in no passage; the char-n-gram retriever can absorb one
slip, not three. Rather than list misspellings, this module treats the product's
own vocabulary as the dictionary: a word the corpus never uses is snapped to the
single *product term* within a small Damerau-Levenshtein distance, weighted by
how often the corpus uses it. Product terms are the words the documentation puts
in its article and section titles plus the workspace nouns the tool router keys
on — not the corpus's ordinary English, which is far too small a sample of
English to tell ``users`` from a typo of ``uses``.

Guard-rails, because the same tokens are also table and column names:

* only lower-case alphabetic words of at least four letters (a capitalised
  ``Orders`` or a ``snake_case`` identifier is left alone),
* only words the corpus does not already contain,
* four-letter words only by transposition (``gaet``), five and up within one
  edit, eight and up within two,
* only when one candidate is clearly best — ties are left unchanged.

A handful of question words too short for the edit-distance rule (``wat``,
``hw``) are repaired from a tiny fixed table.
"""

from __future__ import annotations

import re
from collections import Counter
from functools import lru_cache

_WORD = re.compile(r"[A-Za-z]+")
_MIN_LENGTH = 4
_LONG_WORD = 8
_MIN_CORPUS_PASSAGES = 2

# Heading words that are ordinary English, never a product term to snap to.
_GENERIC: frozenset[str] = frozenset(
    """
    what when where which whose does how why who with without from into onto over
    under after before this that these those there their them then than your yours
    mine ours have has had will would should could can cannot must need needs want
    wants make makes made take takes give gives get gets goes went come comes know
    knows think thing things some same such only also very more most many much each
    every other another about again still just even ever never always often once
    here back down away well like long last first next both between through during
    while until since because being been were done doing used uses using user users
    mean means meant keep keeps kept look looks looking show shows tell tells says
    said step steps tips guide help faq part parts list lists view views open opens
    name names type types text line lines page pages card cards role roles rule
    rules date dates time times true false none null yes not
    """.split()
)

_FUNCTION_WORDS: dict[str, str] = {
    "wat": "what",
    "wht": "what",
    "wut": "what",
    "hw": "how",
    "hwo": "how",
    "wich": "which",
    "whcih": "which",
    "teh": "the",
    "thier": "their",
    "plz": "please",
    "pls": "please",
    "thx": "thanks",
    "cud": "could",
    "shud": "should",
    "wud": "would",
    "abt": "about",
    "frm": "from",
}


def damerau_levenshtein(a: str, b: str, *, cap: int) -> int:
    """Edit distance with adjacent transposition, stopping early above ``cap``."""
    if a == b:
        return 0
    la, lb = len(a), len(b)
    if abs(la - lb) > cap:
        return cap + 1
    prev2: list[int] = []
    prev = list(range(lb + 1))
    for i in range(1, la + 1):
        cur = [i] + [0] * lb
        row_min = i
        for j in range(1, lb + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            best = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                best = min(best, prev2[j - 2] + 1)
            cur[j] = best
            row_min = min(row_min, best)
        if row_min > cap:
            return cap + 1
        prev2, prev = prev, cur
    return prev[lb]


@lru_cache(maxsize=1)
def _vocabulary() -> tuple[frozenset[str], dict[str, int], dict[str, tuple[str, ...]]]:
    """Every corpus word; product terms with passage counts, bucketed by first letter."""
    from ..copilot.spelling import WORKSPACE_VOCABULARY
    from .product_docs import load_product_doc_chunks

    counts: Counter[str] = Counter()
    headings: set[str] = set(WORKSPACE_VOCABULARY)
    for chunk in load_product_doc_chunks():
        title = f"{chunk.doc_title} {chunk.section_title}"
        counts.update({w.lower() for w in _WORD.findall(f"{title} {chunk.text}") if len(w) >= 3})
        headings.update(w.lower() for w in _WORD.findall(title) if len(w) >= _MIN_LENGTH)
    terms = {
        w: max(counts.get(w, 0), _MIN_CORPUS_PASSAGES)
        for w in headings
        if w not in _GENERIC
    }
    buckets: dict[str, list[str]] = {}
    for w in terms:
        buckets.setdefault(w[0], []).append(w)
    return frozenset(counts), terms, {k: tuple(v) for k, v in buckets.items()}


def _inflection_of_known(low: str, known: frozenset[str]) -> bool:
    """``products`` is not a typo of ``product``; ``passing`` is not one of ``pausing``."""
    stems = {low[:-1], low[:-2], low + "s", low + "e", low[:-1] + "e"}
    if low.endswith("ing"):
        stems.update({low[:-3], low[:-3] + "e"})
    if low.endswith("ed"):
        stems.update({low[:-2], low[:-1]})
    if low.endswith("ies"):
        stems.add(low[:-3] + "y")
    return any(len(s) >= 3 and s in known for s in stems)


def correct_word(word: str) -> str:
    """The product spelling of ``word``, or ``word`` itself when unsure."""
    low = word.lower()
    if low in _FUNCTION_WORDS:
        return _FUNCTION_WORDS[low]
    if len(word) < _MIN_LENGTH or not word.islower():
        return word
    known, counts, buckets = _vocabulary()
    if low in known or _inflection_of_known(low, known):
        return word
    cap = 2 if len(low) >= _LONG_WORD else 1
    candidates: list[tuple[int, int, str]] = []
    # A typo rarely touches the first letter; checking that bucket plus the one
    # reachable by a transposition/deletion of it keeps this cheap.
    firsts = {low[0]}
    if len(low) > 1:
        firsts.add(low[1])
    for first in firsts:
        for cand in buckets.get(first, ()):
            if abs(len(cand) - len(low)) > cap:
                continue
            d = damerau_levenshtein(low, cand, cap=cap)
            if d > cap:
                continue
            if len(low) == _MIN_LENGTH and sorted(low) != sorted(cand):
                continue
            candidates.append((d, -counts[cand], cand))
    if not candidates:
        return word
    candidates.sort()
    best = candidates[0]
    if len(candidates) > 1:
        second = candidates[1]
        if second[0] == best[0] and -second[1] * 2 > -best[1]:
            return word
    return best[2]


def correct_to_corpus(text: str) -> str:
    """Repair the misspelled content words of a question."""
    if not text:
        return text
    return _WORD.sub(lambda m: correct_word(m.group(0)), text)
