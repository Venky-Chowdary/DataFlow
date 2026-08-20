"""Canonical checks that an LLM rewrite still rests on the evidence we gave it.

A provider returning HTTP 200 is not evidence. Both narration paths — RAG
documentation answers and Pilot's answer polish — must reject prose that dropped
the facts it was asked to restate, otherwise a provider that ignores the prompt
(or a compromised/misconfigured endpoint) silently becomes the source of truth.
"""

from __future__ import annotations

import re

from .lexical_index import content_terms

# Overlap floor between a rewrite's terms and the evidence terms. Low enough that
# genuine paraphrase survives, high enough that unrelated prose does not.
TERM_OVERLAP_FLOOR = 0.25

# Facts a rewrite may never lose: numbers, IDs, and quoted identifiers. These are
# what an operator acts on, so a dropped or altered one is a wrong answer.
_NUMBER_RE = re.compile(r"\d[\d,._]*\d|\d")
_IDENTIFIER_RE = re.compile(r"`([^`]{1,120})`|\b(pf_[A-Za-z0-9_-]+|[0-9a-f]{24})\b")

# Writing "three jobs" for 3 is a legitimate rewrite of a small count, so accept the
# spelled form; anything larger is an operator figure and must survive as digits.
_SPELLED = (
    "zero one two three four five six seven eight nine ten eleven twelve "
    "thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty"
).split()


def _number_forms(fact: str) -> set[str]:
    """Every spelling of a number that still means the same figure."""
    forms = {fact, fact.replace(",", "")}
    digits = fact.replace(",", "")
    if digits.isdigit():
        value = int(digits)
        forms.add(f"{value:,}")
        if value < len(_SPELLED):
            forms.add(_SPELLED[value])
    return forms


def retains_evidence(
    answer: str,
    matched_terms: set[str],
    context_terms: set[str],
    floor: float = TERM_OVERLAP_FLOOR,
) -> bool:
    """Whether a narration still talks about the passages it was given."""
    answer_terms = set(content_terms(answer))
    if matched_terms & answer_terms:
        return True
    if not answer_terms or not context_terms:
        return False
    return len(answer_terms & context_terms) / len(answer_terms) >= floor


def names_identifier(text: str) -> bool:
    """Whether the text names a workspace object by ID or backticked name.

    An operator pasting `pf_...`, a job ID or a table in backticks is asking about
    their own data, which the shipped documentation cannot vouch for by wording.
    """
    return bool(_IDENTIFIER_RE.search(text or ""))


def _facts(text: str) -> list[set[str]]:
    """Each fact as the set of spellings that count as keeping it."""
    facts = [_number_forms(m.group(0)) for m in _NUMBER_RE.finditer(text)]
    for match in _IDENTIFIER_RE.finditer(text):
        identifier = (match.group(1) or match.group(2) or "").strip().lower()
        if identifier:
            facts.append({identifier})
    return [f for f in facts if any(f)]


def keeps_draft_facts(draft: str, rewrite: str, floor: float = TERM_OVERLAP_FLOOR) -> bool:
    """Whether a polished answer still carries the draft's facts and subject.

    Guards the Pilot polish step: the draft is already grounded in real tool
    output, so a rewrite that lost its counts, IDs or subject is a regression
    even when the provider call succeeded.
    """
    draft_body = (draft or "").strip()
    body = (rewrite or "").strip()
    if not draft_body or not body:
        return False
    lowered = body.lower()
    if any(not any(form in lowered for form in fact) for fact in _facts(draft_body)):
        return False
    draft_terms = set(content_terms(draft_body))
    if not draft_terms:
        return True
    kept = len(draft_terms & set(content_terms(body))) / len(draft_terms)
    return kept >= floor
