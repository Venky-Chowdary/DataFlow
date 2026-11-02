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
# snake_case product identifiers (sync modes, flags, counters) are what an
# operator types into a form or a manifest, so "Full Refresh Overwrite" for
# full_refresh_overwrite is a lost fact unless it keeps the words in order.
_SNAKE_RE = re.compile(r"\b[a-z][a-z0-9]*(?:_[a-z0-9]+)+\b")

# A sentence that denies something. A rewrite may only deny what the draft
# denied: "You cannot export a schedule as YAML" was narrated from a draft that
# explained how to export one.
_NEGATION_RE = re.compile(
    r"\b(?:not|no|never|cannot|can't|cant|won't|wont|don't|dont|doesn't|doesnt|"
    r"isn't|isnt|aren't|arent|unable|without|unsupported|impossible|"
    r"rather than|instead of)\b",
    re.I,
)
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_CLAUSE_RE = re.compile(r"[.;:!?\n]|\s[\u2014\u2013-]\s")

# Trailing chat filler a provider appends after the answer. It carries no
# evidence, and an enterprise operator did not ask to be asked.
_FILLER_RE = re.compile(
    r"(?:would you like|do you (?:have|want|need)|if you (?:need|have|want)|"
    r"feel free to|let me know|is there anything else|happy to help|"
    r"hope this helps|i'm here to help)",
    re.I,
)

# Writing "three jobs" for 3 is a legitimate rewrite of a small count, so accept the
# spelled form; anything larger is an operator figure and must survive as digits.
_SPELLED = (
    "zero one two three four five six seven eight nine ten eleven twelve "
    "thirteen fourteen fifteen sixteen seventeen eighteen nineteen twenty"
).split()


def _number_forms(fact: str) -> set[str]:
    """Every spelling of a number that still means the same figure.

    The exact token always counts. Canonical / spelled / US-grouped forms are
    added only when ``decimal_wire_value`` binds — the same parser the write
    path uses. Auto-ambiguous ``1,234`` / ``1.234`` therefore stay as written;
    a rewrite that drops the grouping mark invents a US or EU reading Auto
    itself refuses, so that rewrite is a lost fact.
    """
    from services.transform_engine import decimal_wire_value

    forms = {fact}
    parsed = decimal_wire_value(fact)
    if parsed is None:
        return forms
    if parsed == parsed.to_integral_value():
        value = int(parsed)
        forms.add(str(value))
        forms.add(f"{value:,}")
        if 0 <= value < len(_SPELLED):
            forms.add(_SPELLED[value])
        return forms
    wire = format(parsed, "f").rstrip("0").rstrip(".")
    if wire:
        forms.add(wire)
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
    for snake in {m.group(0) for m in _SNAKE_RE.finditer(text)}:
        facts.append({snake, snake.replace("_", " "), snake.replace("_", "-")})
    return [f for f in facts if any(f)]


def _sentences(text: str) -> list[str]:
    return [s.strip() for s in _SENTENCE_RE.split(text or "") if s.strip()]


def contradicts_draft(draft: str, rewrite: str) -> bool:
    """Whether a rewrite denies something the draft did not deny.

    A denial's scope is the clause from its cue to the clause end. Every
    denied rewrite scope must share a content term with a denied draft scope,
    so a paraphrase survives ("is not dropped" for "rather than dropped")
    while a fresh denial about a new subject does not ("cannot export a
    schedule" from a draft whose only denial was "does not include
    credentials").
    """
    denied_scopes, affirmed = _clause_terms(draft)
    denied = set().union(*denied_scopes) if denied_scopes else set()
    for scope in _clause_terms(rewrite)[0]:
        if not scope or scope & denied:
            continue
        # A denial the draft never made is a contradiction when the draft
        # positively asserted (nearly) the same words in one clause — "cannot
        # export a schedule as YAML" against "Export a schedule as YAML from
        # the detail drawer". A denial that only restates the question
        # ("scheduled runs do not skip preflight") is left to the term floor.
        if any(len(scope & clause) >= _DENIAL_MATCH * len(scope) for clause in affirmed):
            return True
    return False


# Share of a denied scope's terms one affirmative draft clause must contain
# before the denial is read as inverting that clause.
_DENIAL_MATCH = 0.75


def _clause_terms(text: str) -> tuple[list[set[str]], list[set[str]]]:
    """Per clause: the scope of its denial, or its terms when it affirms.

    A denial's scope runs from its cue to the clause end. One scope per
    clause: a second cue inside it ("no row is lost without notice")
    qualifies the first denial, it does not start one.
    """
    denied: list[set[str]] = []
    affirmed: list[set[str]] = []
    for clause in _CLAUSE_RE.split(text or ""):
        cue = _NEGATION_RE.search(clause)
        terms = set(content_terms(clause[cue.end():] if cue else clause))
        if not terms:
            continue
        (denied if cue else affirmed).append(terms)
    return denied, affirmed


def strip_chat_filler(text: str) -> str:
    """Drop trailing offer/ask sentences a provider appended to the answer."""
    body = (text or "").strip()
    sentences = _sentences(body)
    dropped: str | None = None
    while len(sentences) > 1 and _FILLER_RE.search(sentences[-1]):
        dropped = sentences.pop()
    if dropped is None:
        return body
    # Cut at the first dropped sentence so the draft's own line breaks and
    # bullet layout survive in what is kept.
    return body[: body.rfind(dropped)].rstrip()


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
