"""Composing the answer — selecting sentences, not pasting sections.

The previous composer took the first three sentences of the top two sections and
concatenated them. That is not an answer to a question; it is the top of two
articles. Asked "which sync mode should I pick for a nightly load" it opened with
whatever sentence the sync-mode section happens to begin with, which may be about
something else entirely, and it could not use a third section even when that was
where the answer lived.

Answer composition here is extractive selection over the whole retrieved set:

1. **Split** every retrieved section into sentences, keeping the gate/step lines
   the help corpus writes as bare lines rather than dropping them.
2. **Score** each sentence on the question's terms — the operator's own words
   weigh more than the expansion vocabulary — plus a bonus for the shape of
   answer the question asked for. A "what is" question wants the sentence that
   defines the term; a "how do I" question wants the imperative step.
3. **Select** with maximal-marginal-relevance so three sections that all restate
   the same definition contribute it once, and the remaining slots go to
   sentences that add something.
4. **Cite** every section an included sentence came from, so each claim is
   traceable to a page the operator can open.

Extraction is deliberate: the sentences are the shipped documentation's own, so
the answer cannot assert anything the documentation does not. When a cloud model
is configured it may rewrite this draft, but the rewrite is checked against the
same evidence and discarded if it drifts (see ``generator._narrate``).
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass

from .lexical_index import content_terms
from .query_analysis import QueryAnalysis

# How much an expansion term counts relative to a word the operator typed.
EXPANSION_WEIGHT = 0.45

# Redundancy penalty for maximal-marginal-relevance selection. 0.7 keeps the
# answer from restating one definition three times while still allowing a second
# sentence that elaborates it.
MMR_LAMBDA = 0.7

MAX_SENTENCES = 6
MAX_CITED_SECTIONS = 3

# Content terms in a typical help sentence. Sentences longer than this have
# their term matches discounted proportionally, the way BM25 discounts a long
# document: a raw match count lets a 400-character sentence that lists every
# permission in the product outrank the one sentence that answers the question,
# purely by covering more ground. Shorter sentences are never *rewarded* —
# normalization is one-sided so a three-word fragment cannot win on density.
LENGTH_PIVOT = 18

# What one item of a list is worth when the question matched the heading the
# list sits under, scaled by how much of the question that heading carries.
LIST_ITEM_CREDIT = 2.6

# What a sentence is worth on its heading alone, when the heading restates the
# whole question. Below the credit one typed word earns, so a sentence that
# answers in the operator's own words always outranks one vouched for by the
# title above it.
HEADING_CREDIT = 0.9

# The heading has to carry *every* anchor term of the question before it can
# speak for a sentence that carries none. A partial overlap is how "which
# engines can I connect to" reached "Procedure: connect Cursor" — one word of
# two, and a section about MCP answered a question about database engines.
HEADING_ANCHOR = 1.0

# A list answers a question as a whole or not at all, so once one item is
# selected its siblings come with it. Nine is the number of preflight gates —
# the longest enumeration the shipped documentation contains.
MAX_LIST_ITEMS = 9

_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+(?=[A-Z(`*\d])")
_GATE_LINE = re.compile(r"^G\d+\b")
_LIST_LINE = re.compile(r"^(?:\d+[.)]\s|[-*•]\s|\*\*[A-Z][^*]{0,40}\*\*\s*(?:—|-|:))")
_SKIP_PREFIX = ("Where:", "Tip:", "Note:", "Example:")
_UI_CAPTION = re.compile(
    r"^(Validate|Map|Job Theater|System|Operations|Transfer|Pilot|Connectors|"
    r"Schedules|Proofs|Evidence) — ",
)
_CODE_LINE = re.compile(r"^\s*(?:\{|\[|GET |POST |PUT |PATCH |DELETE |curl |\$ )")
_IMPERATIVE = re.compile(
    r"^(?:Open|Click|Pick|Choose|Select|Set|Enter|Type|Paste|Copy|Add|Create|"
    r"Review|Return|Expand|Fix|Remediate|Use|Run|Press|Confirm|Approve|Sign|"
    r"Go|Navigate|Upload|Download|Export|Import|Enable|Disable|Start|Stop)\b"
)
_DEFINITIONAL = re.compile(
    r"^\s*(?:\*\*)?[A-Z][\w \-/()`*]{0,60}(?:\*\*)?\s+"
    r"(?:is|are|means|refers to|describes|holds|records|names)\b"
)

# Section titles whose sentences answer a given ask better than the corpus
# average. Derived from how the help corpus is written: definitions live in FAQ
# and "What is" cards, steps live in "Procedure:" sections.
_ASK_SECTION_BONUS: dict[str, tuple[tuple[str, float], ...]] = {
    "definition": (("what is", 2.4), ("what are", 2.4), ("core gate", 1.8), ("procedure:", -1.6)),
    "procedure": (("procedure:", 2.4), ("what is", -0.8), ("what are", -0.8)),
    "enumeration": (("mode", 1.6), ("core gate", 1.6), ("role", 1.2), ("procedure:", -1.0)),
    "diagnosis": (("phas", 1.2), ("checksum", 1.2), ("quarantine", 1.2), ("drift", 1.2)),
    "comparison": (("mode", 1.8), ("what is", 0.8)),
    "capability": (("what is", 1.0), ("support", 1.4)),
}


@dataclass(frozen=True)
class Candidate:
    """One sentence of evidence, with where it came from."""

    text: str
    section_title: str
    citation: str
    href: str
    order: int
    terms: frozenset[str]
    score: float = 0.0
    #: True when the sentence is one item of an enumerated list in its section
    #: (``G1 Source readable — …``, ``1. Open Connectors``). List items are
    #: selected as a group, because half a list is not an answer.
    list_item: bool = False


def _split_annotated(text: str, section_title: str = "") -> list[tuple[str, bool]]:
    """Sentences of one help section, each flagged as a list item or not."""
    body = (text or "").strip()
    title = (section_title or "").strip()
    if title and (body == title or body.startswith((f"{title}\n", f"{title} "))):
        body = body[len(title) :].strip()

    out: list[tuple[str, bool]] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line or line.startswith(_SKIP_PREFIX) or _CODE_LINE.match(line):
            continue
        if _UI_CAPTION.match(line):
            continue
        if _GATE_LINE.match(line):
            out.append((line if line.endswith((".", "!", "?")) else f"{line}.", True))
            continue
        listed = bool(_LIST_LINE.match(line))
        if not any(ch in line for ch in ".!?"):
            # A bare line is only content when it reads as a statement; headings
            # and captions are not.
            if ":" not in line or "—" in line:
                continue
        for piece in _SENTENCE_SPLIT.split(line):
            piece = piece.strip()
            if not piece or piece.startswith(_SKIP_PREFIX) or piece.endswith(":"):
                continue
            if len(piece) < 12:
                continue
            out.append(
                (piece if piece.endswith((".", "!", "?")) else f"{piece}.", listed)
            )
            listed = False
    return out


def split_sentences(text: str, section_title: str = "") -> list[str]:
    """Readable sentences from one help section, keeping the bare gate lines.

    The corpus writes G1–G9 as their own lines with no terminator, and those
    lines are the answer to every gate question, so a naive sentence splitter
    throws away the most useful content in the file.
    """
    return [sentence for sentence, _ in _split_annotated(text, section_title)]


def _section_bonus(ask: str, section_title: str) -> float:
    title = (section_title or "").strip().lower()
    return sum(
        weight for needle, weight in _ASK_SECTION_BONUS.get(ask, ()) if needle in title
    )


def _heading_match(heading: str, typed: set[str]) -> float:
    """Share of the question's own terms the section's heading path carries."""
    if not typed:
        return 0.0
    words = set(content_terms(heading or ""))
    if not words:
        return 0.0
    return len(words & typed) / len(typed)


def _length_norm(term_count: int) -> float:
    """One-sided pivoted length normalization for a sentence's match count."""
    if term_count <= LENGTH_PIVOT:
        return 1.0
    return LENGTH_PIVOT / term_count


def _shape_bonus(ask: str, sentence: str) -> float:
    if ask == "procedure" and _IMPERATIVE.match(sentence):
        return 1.4
    if ask == "definition" and _DEFINITIONAL.match(sentence):
        return 2.0
    if ask == "enumeration" and ("·" in sentence or sentence.count(",") >= 2):
        return 0.9
    return 0.0


def build_candidates(
    analysis: QueryAnalysis,
    sections: Sequence[tuple[str, str, str, str]],
) -> list[Candidate]:
    """Score every sentence in the retrieved sections against the question.

    ``sections`` is ``(section_title, citation, href, text)`` in fused rank
    order; earlier sections carry a small rank prior so a tie resolves toward
    the passage retrieval preferred.
    """
    typed = set(analysis.terms)
    expanded = set(analysis.expansions) - typed
    candidates: list[Candidate] = []
    order = 0
    for rank, (section_title, citation, href, text) in enumerate(sections):
        rank_prior = 1.0 / (1.0 + rank)
        heading_match = _heading_match(citation or section_title, typed)
        for sentence, is_list_item in _split_annotated(text, section_title):
            terms = frozenset(content_terms(sentence))
            typed_hits = len(terms & typed)
            expanded_hits = len(terms & expanded)
            # A list item rarely repeats the word that names the list. "G3
            # Schema contract — source and target schemas are compatible" scores
            # nothing against "what are the preflight gates", so all nine gates
            # were dropped and the answer talked around them. The heading the
            # question did match is what vouches for its items.
            listed_credit = (
                LIST_ITEM_CREDIT * heading_match if is_list_item else 0.0
            )
            match = typed_hits + EXPANSION_WEIGHT * expanded_hits
            if match or listed_credit:
                score = (
                    match * _length_norm(len(terms))
                    + listed_credit
                    + _section_bonus(analysis.ask, section_title)
                    + _shape_bonus(analysis.ask, sentence)
                    + 0.8 * rank_prior
                )
            elif heading_match >= HEADING_ANCHOR:
                # The same argument as for list items, one level weaker. "Do
                # you have webhooks" is answered by "Subscribe to job.completed,
                # job.failed and pipeline.quarantine_threshold events", which
                # names the events instead of the feature — ``webhook`` appears
                # only in the heading above it. Scored on its own words that
                # sentence is worth nothing, so the answer came from an
                # unrelated section. The ask-shape and section priors are left
                # out on purpose: they describe how well a sentence fits the
                # question's shape, and there is no fit to grade when the
                # sentence matched none of it.
                score = HEADING_CREDIT + 0.8 * rank_prior
            else:
                continue
            candidates.append(
                Candidate(
                    text=sentence,
                    section_title=section_title,
                    citation=citation,
                    href=href,
                    order=order,
                    terms=terms,
                    score=score,
                    list_item=is_list_item,
                )
            )
            order += 1
    candidates.sort(key=lambda c: (c.score, -c.order), reverse=True)
    return candidates


def select_sentences(
    candidates: Sequence[Candidate],
    limit: int = MAX_SENTENCES,
    mmr_lambda: float = MMR_LAMBDA,
) -> list[Candidate]:
    """Maximal-marginal-relevance selection: relevant, then non-redundant."""
    chosen: list[Candidate] = []
    pool = list(candidates)
    if not pool:
        return []
    best = max(pool, key=lambda c: c.score)
    chosen.append(best)
    pool.remove(best)

    top = best.score or 1.0
    while pool and len(chosen) < limit:
        def marginal(cand: Candidate) -> float:
            overlap = max(
                (
                    len(cand.terms & other.terms) / max(len(cand.terms | other.terms), 1)
                    for other in chosen
                ),
                default=0.0,
            )
            return mmr_lambda * (cand.score / top) - (1.0 - mmr_lambda) * overlap

        pick = max(pool, key=marginal)
        if marginal(pick) <= 0:
            break
        chosen.append(pick)
        pool.remove(pick)
    chosen = _complete_lists(chosen, candidates)
    # Lead with the sentence that answers, then read in source order. Sorting
    # purely by source order opened "what is quarantine" with "Open Operations →
    # Jobs → Quarantine on the run", because that section ranked first — an
    # answer has to start with the answer.
    # A list item is never hoisted: pulling G5 to the front leaves the gates
    # reading G5, G1, G2, G3 …
    leadable = [c for c in chosen if not c.list_item]
    if not leadable:
        chosen.sort(key=lambda c: c.order)
        return chosen
    lead = max(leadable, key=lambda c: c.score)
    rest = sorted((c for c in chosen if c is not lead), key=lambda c: c.order)
    return [lead, *rest]


def _complete_lists(
    chosen: list[Candidate],
    candidates: Sequence[Candidate],
) -> list[Candidate]:
    """Add the siblings of any selected list item.

    Maximal-marginal-relevance is built to suppress near-duplicates, and the
    items of one list look like near-duplicates to it — they share a heading, a
    shape and half their words. Left to itself it returned "G1" and "G4" and
    called that the gate list. A list is one unit of evidence.
    """
    sections_with_items = {c.section_title for c in chosen if c.list_item}
    if not sections_with_items:
        return chosen
    picked = {c.order for c in chosen}
    out = list(chosen)
    for section in sections_with_items:
        siblings = [
            c
            for c in candidates
            if c.list_item
            and c.section_title == section
            and c.order not in picked
        ]
        already = sum(
            1 for c in chosen if c.list_item and c.section_title == section
        )
        for cand in siblings[: max(0, MAX_LIST_ITEMS - already)]:
            out.append(cand)
            picked.add(cand.order)
    return out


def compose_answer(
    analysis: QueryAnalysis,
    sections: Sequence[tuple[str, str, str, str]],
    *,
    partial_caveat: str = "",
    limit: int = MAX_SENTENCES,
) -> str:
    """The spoken answer: selected documentation sentences, then its citations.

    ``partial_caveat`` names what the question asked for that the documentation
    does not cover. It is stated after the answer rather than instead of it —
    an operator can act on "here is what is documented, this part is not" and
    cannot act on a refusal.
    """
    candidates = build_candidates(analysis, sections)
    chosen = select_sentences(candidates, limit=limit)
    if not chosen:
        return ""

    body = " ".join(c.text for c in chosen)
    parts = [body]
    if partial_caveat:
        parts.append(partial_caveat)

    cited: list[str] = []
    for cand in chosen:
        if cand.citation and cand.citation not in cited:
            cited.append(cand.citation)
    if cited:
        parts.append("Source: " + " · ".join(cited[:MAX_CITED_SECTIONS]) + " (Help)")
    return "\n\n".join(parts)
