"""Product documentation retrieval — the evidence behind every product answer.

The knowledge base held semantic patterns, synonyms, industry schemas and type
conversions: column-mapping material with no sentence about quarantine, CDC setup,
preflight gates or proof. So "what does quarantine mean" retrieved column noise at
0.3 similarity and an answer was narrated over it. This module makes the shipped
operator help (``help_corpus.json``, generated from ``apps/web/src/lib/helpDocs.ts``)
plus passages generated from the product's own enforcing modules
(``product_facts``) the retrievable corpus for those questions, and returns
citations with every hit so an answer can be checked against its source.

Retrieval runs in four stages, each owned by its own module so the failure can be
attributed:

``query_analysis``   understands the question — what kind of answer it wants, its
                     content terms with discourse filler removed, and the
                     documentation's vocabulary for what the operator said.
``Bm25Index`` +
``CharNgramIndex``   two retrievers over the same passages: term statistics for
                     exact terminology, character n-grams for spelling and
                     morphology the stemmer cannot reach.
``fusion``           Reciprocal Rank Fusion into one ranking, so neither
                     retriever's score scale has to be calibrated.
``evidence_policy``  whether the fused set can answer, as ``answer`` / ``partial``
                     / ``refuse``.

The previous single absolute floor — one passage covering 55% of the question's
IDF mass — refused 14 of 30 ordinary operator questions on this corpus, including
"what happens to bad rows" and "how do I connect to BigQuery". See
``evidence_policy`` for why that measure was the wrong one.
"""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass, replace
from functools import lru_cache
from pathlib import Path

from .char_ngram_index import CharNgramIndex
from .evidence_policy import EvidenceVerdict, assess_evidence
from .fusion import reciprocal_rank_fusion
from .lexical_index import Bm25Index, content_terms
from .query_analysis import (
    CUSTOM_SLOT_NAME_RE,
    INCREMENTAL_UPDATED_AT_RE,
    PAUSE_CDC_RE,
    QueryAnalysis,
    analyze_query,
    distinctive_procedure_terms,
)

HELP_CORPUS_PATH = Path(__file__).with_name("help_corpus.json")

# Retained for callers that still read it. Per-hit term coverage is reported on
# every hit as ``grounding``, but it is no longer what decides answerability:
# coverage of a *single* passage falls as a question gets longer, so it refused
# questions the corpus plainly answers. ``evidence_policy`` owns that decision.
GROUNDING_FLOOR = 0.2

# BM25 alone ranks a passing mention in a short FAQ above the section written about
# the feature, because length normalization rewards brevity. A heading that names the
# question's terms is the strongest signal an operator's own eye uses, so ranking
# blends it in — and down-weights hits that cover little of the question.
TITLE_WEIGHT = 3.0

# How many passages the fusion stage considers before the evidence policy judges
# them. Wider than the answer needs: a question answered by the third-ranked
# section is ordinary, and the policy measures coverage over the whole set.
FUSION_CANDIDATES = 24

# BM25 over the operator's own words leads the fusion. The expansion vocabulary
# and the n-gram retriever are there for recall, not precision, so they carry
# less than half the vote each — enough to rescue a question whose wording missed
# the corpus, never enough to outvote the words the operator actually typed.
RETRIEVER_WEIGHTS = {
    "bm25_typed": 1.0,
    "bm25_expanded": 0.4,
    "char_ngram": 0.45,
}

# Scale the normalized fused relevance onto the same range the heading and
# section-intent priors use, so relevance stays the dominant term and the priors
# stay adjustments rather than overrides.
FUSED_RANK_SCALE = 10.0

# How relevance splits between rank position and score magnitude. Rank fusion is
# robust but, at the standard ``k=60`` on a corpus this small, nearly flat; the
# normalized BM25 score is discriminative but sensitive to one long passage
# repeating a term. Half of each: the rank list decides the shortlist, the score
# decides the order within it.
RRF_WEIGHT = 0.45

# How the evidence window trades relevance for coverage of the question. Both
# are expressed against the normalized rank score, so 1.0 would let a passage
# that covers the whole question outrank the best-ranked one outright. The
# novelty bonus is shared across the question's terms; the redundancy penalty
# is the share of a passage's matches that the window already has, so a passage
# that only restates what is already there is close to worthless.
COVERAGE_NOVELTY = 1.0
COVERAGE_REDUNDANCY = 0.6

# Definitional asks ("what is quarantine") must not lose to a long Procedure
# section just because the procedure also says the word. FAQ / core-gate
# headings are the section an operator's eye would open first.
_DEFINITIONAL_QUERY = re.compile(
    r"\b(?:what\s+(?:is|are|does)|what'?s|mean(?:ing)?)\b",
    re.I,
)
_SKIP_HELP_LINE = ("Where:", "Tip:", "Note:")
_SENTENCE_SPLIT = re.compile(r"(?<=[.!?])\s+")
_GATE_LINE = re.compile(r"^G\d+\b")
_STEP_HEADING = re.compile(
    r"^(Open the |Look for |Review |Return to |Click |Fix and |Remediate |"
    r"Use this path|Copy the |Paste into |Expand the |Add the )",
    re.I,
)
_UI_CAPTION = re.compile(
    r"^(Validate|Map|Job Theater|System|Operations|Transfer|Pilot|Connectors) — ",
)


@dataclass(frozen=True)
class ProductDocChunk:
    """One retrievable section — a help article's, or one generated from a module.

    ``source_module`` is set only on generated passages. A generated passage has
    no Help page to open, so it cites the module that makes it true instead; that
    keeps every claim checkable without shipping a second copy of the rule as
    prose.
    """

    id: str
    doc_id: str
    doc_slug: str
    doc_title: str
    category: str
    section_id: str
    section_title: str
    text: str
    source_module: str = ""

    @property
    def generated(self) -> bool:
        return bool(self.source_module)

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
        source: dict[str, object] = {
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
        if self.chunk.source_module:
            source["source_module"] = self.chunk.source_module
        return source


@lru_cache(maxsize=1)
def load_help_corpus_chunks() -> tuple[ProductDocChunk, ...]:
    """The shipped help articles; an absent corpus yields no evidence, not a crash."""
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


# A section longer than this is retrieved as its steps rather than whole. The
# median help section is ~370 characters; the procedures run to 4,500. BM25
# divides a document's term weight by its length, so the longest and most
# instructive passages in the corpus were systematically the hardest to reach:
# "what is the difference between jobs and pipelines" is answered verbatim by
# one line of "Procedure: create a pipeline", and that section did not make the
# top five for a question naming both of its subjects.
MAX_SECTION_CHARS = 1200

# Every procedure in the corpus closes each step with a ``Where:`` breadcrumb,
# which is what makes the step boundaries real structure rather than a guess.
# Two steps is the minimum that makes splitting meaningful.
MIN_STEPS_TO_SPLIT = 2

# A section with fewer lines than this is one paragraph, and cutting a paragraph
# in the middle would leave a passage that does not read as a statement.
MIN_LINES_TO_SPLIT = 4

_STEP_TRAILER = ("Where:", "Tip:", "Note:")


def _split_procedure(text: str) -> list[tuple[str, str]]:
    """One long procedure as ``(step_title, step_text)``, or ``[]`` if not one.

    A step runs from its title line to its ``Where:`` breadcrumb, taking any
    ``Tip:`` that trails the breadcrumb with it. Everything before the first
    step is the section's own preamble and is returned with an empty title.
    """
    lines = [line.rstrip() for line in (text or "").splitlines()]
    if sum(1 for line in lines if line.strip().startswith("Where:")) < MIN_STEPS_TO_SPLIT:
        return []

    steps: list[list[str]] = [[]]
    closed = False
    for line in lines:
        stripped = line.strip()
        trailer = stripped.startswith(_STEP_TRAILER)
        if closed and stripped and not trailer:
            steps.append([])
            closed = False
        steps[-1].append(line)
        if stripped.startswith("Where:"):
            closed = True

    out: list[tuple[str, str]] = []
    for index, block in enumerate(steps):
        body = "\n".join(block).strip()
        if not body:
            continue
        # The preamble keeps the section's own title; a step is named by its
        # first line, which is how the corpus writes step headings.
        title = "" if index == 0 else body.splitlines()[0].strip()
        out.append((title, body))
    return out if len(out) > MIN_STEPS_TO_SPLIT else []


def _split_paragraphs(text: str) -> list[tuple[str, str]]:
    """A long reference section as passages of whole lines under the char limit.

    The splitter only knew how to divide a *procedure*, by its ``Where:``
    breadcrumbs. A reference grid — one fact per line, which is how the
    generated sections are written — stayed one long passage and paid the same
    length penalty: "do you preserve column order" ranked a CSV tutorial that
    says "in order" four times above the section that states the rule. Lines are
    the corpus's own unit here, so a group of them is a real boundary rather
    than a guess, and no line is ever cut in half.
    """
    lines = [line.rstrip() for line in (text or "").splitlines() if line.strip()]
    if len(lines) < MIN_LINES_TO_SPLIT:
        return []
    groups: list[list[str]] = [[]]
    width = 0
    for line in lines:
        if groups[-1] and width + len(line) > MAX_SECTION_CHARS:
            groups.append([])
            width = 0
        groups[-1].append(line)
        width += len(line)
    return [("", "\n".join(group)) for group in groups if group]


def _as_step_chunks(chunk: ProductDocChunk) -> list[ProductDocChunk]:
    """``chunk`` split into its steps when it is long enough to need it."""
    if len(chunk.text) <= MAX_SECTION_CHARS:
        return [chunk]
    steps = _split_procedure(chunk.text) or _split_paragraphs(chunk.text)
    if not steps:
        return [chunk]
    out: list[ProductDocChunk] = []
    for index, (title, body) in enumerate(steps):
        section_title = (
            chunk.section_title if not title else f"{chunk.section_title} → {title}"
        )
        out.append(
            replace(
                chunk,
                id=f"{chunk.id}#s{index}",
                section_id=f"{chunk.section_id}-s{index}",
                section_title=section_title,
                text=body,
            )
        )
    return out


@lru_cache(maxsize=1)
def load_generated_chunks() -> tuple[ProductDocChunk, ...]:
    """Passages generated from the product's own enforcing modules."""
    from .product_facts import FACT_DOC_SLUG, generated_sections

    chunks: list[ProductDocChunk] = []
    for index, section in enumerate(generated_sections()):
        chunks.append(
            ProductDocChunk(
                id=f"fact-{index}-{section.section_id}",
                doc_id=FACT_DOC_SLUG,
                doc_slug=FACT_DOC_SLUG,
                doc_title=section.doc_title,
                category=section.category,
                section_id=section.section_id,
                section_title=section.section_title,
                text=section.text,
                source_module=section.source_module,
            )
        )
    return tuple(chunks)


@lru_cache(maxsize=1)
def load_product_doc_chunks() -> tuple[ProductDocChunk, ...]:
    """Every documented section: shipped help first, generated facts after.

    Sections as the documentation declares them. This is what the corpus is
    *about* — ``evidence_policy`` reads its headings to decide which subjects
    the product documents — so it stays one entry per authored section even
    when retrieval indexes that section as several passages.
    """
    return load_help_corpus_chunks() + load_generated_chunks()


@lru_cache(maxsize=1)
def retrieval_passages() -> tuple[ProductDocChunk, ...]:
    """The same corpus at retrieval granularity, long procedures split by step."""
    out: list[ProductDocChunk] = []
    for chunk in load_product_doc_chunks():
        out.extend(_as_step_chunks(chunk))
    return tuple(out)


@lru_cache(maxsize=1)
def _index() -> tuple[Bm25Index, dict[str, ProductDocChunk]]:
    chunks = retrieval_passages()
    by_id = {c.id: c for c in chunks}
    return Bm25Index([(c.id, f"{c.doc_title}\n{c.text}") for c in chunks]), by_id


@lru_cache(maxsize=1)
def _ngram_index() -> CharNgramIndex:
    """Character n-gram index over the same passages, headings included.

    Headings are repeated into the indexed text because a heading names the
    subject, and an n-gram match on the subject is the signal worth most here.
    """
    return CharNgramIndex(
        [
            (c.id, f"{c.doc_title} {c.section_title} {c.doc_title} {c.section_title}\n{c.text}")
            for c in retrieval_passages()
        ]
    )


def _covers(term: str, heading: set[str]) -> bool:
    """Whether a heading names this term, allowing for a morphological tail.

    ``connect`` and ``connectors`` are the same subject to an operator, but the
    stemmer only closes a fixed set of English suffixes, so exact term equality
    scored the connector article 0 for "how do I connect to BigQuery". A shared
    prefix of at least four characters is the same allowance the n-gram
    retriever makes, applied to the heading prior.
    """
    if term in heading:
        return True
    if len(term) < 4:
        return False
    return any(
        h.startswith(term) or term.startswith(h)
        for h in heading
        if len(h) >= 4
    )


def _title_coverage(chunk: ProductDocChunk, terms: Sequence[str]) -> float:
    """Share of the question's terms named in the article and section headings."""
    if not terms:
        return 0.0
    heading = set(content_terms(f"{chunk.doc_title} {chunk.section_title}"))
    return sum(1 for t in terms if _covers(t, heading)) / len(terms)


_DEFINITIONAL_TITLE = (
    "what is",
    "what are",
    "what a ",
    "what the",
    "what each",
    "what happens",
)

#: Headings that publish a number, for the ask that wants one.
_COUNTING_TITLE = (
    "how many",
    "how much",
)

#: Headings that publish a list, for the ask that wants the members.
_LISTING_TITLE = (
    "which ",
    "what each",
)

#: Swept over an 18-phrasing cardinality set and the 158-case answer audit.
#: The knee is 2.0, where "how many sources can you connect to" flips from a
#: preflight gate description to "30 sources and 30 destinations"; 2.0 through
#: 9.0 are indistinguishable on both sets, so this sits above the knee with
#: margin, and the audit is unchanged at 118 leading across the whole sweep.
_COUNTING_TITLE_BONUS = 3.2

#: And what it costs a heading that publishes a number when the question did
#: not ask for one. Swept over the 158-case answer audit and a five-phrasing
#: listing set: 0.0–2.0 leave the audit at 121 and "which engines can I
#: connect to" still opening on four totals; 4.5 is the knee (122 leading,
#: engines listing flips); 6.0 is where "what engines do you support" flips
#: too; 6.0 through 12.0 are indistinguishable on the audit. Sits above the
#: knee with the same margin the on-ask bonus uses.
_COUNTING_TITLE_OFF_ASK = 6.0

#: The generated role matrix, whose every chunk is one role crossed with the
#: verb list for that role. It therefore holds the vocabulary of nearly any
#: operator question — "read the audit log", "start transfers", "cancel, retry
#: and resume jobs" are all in it — while answering only one question about
#: them: who is allowed to. Its heading also begins "What each", so it drew the
#: definitional prior on top of that overlap. Measured, it led "show me the
#: audit log", "start the transfer" and "how do I cancel a running transfer",
#: and half its sentences are the negative form, so the last of those was
#: answered with "a viewer cannot … cancel, retry and resume jobs".
_ROLE_MATRIX_DOC = "roles & permissions"

#: What the role matrix is the right answer to. Naming a role is enough, as is
#: asking who may do something; ``role`` and ``permission`` are kept as bare
#: words because an operator who types either is asking about authorization.
_PERMISSION_QUESTION = re.compile(
    # Bare ``who``: in this product's vocabulary a question about a person is a
    # question about authorization. Requiring a modal after it missed "can I
    # limit who sees a connector", which is the matrix's question asked without
    # the word "can" next to the word "who".
    r"\bwho\b"
    r"|\b(?:permission|permissions|rbac|authoriz|unauthoriz|forbidden|"
    r"role|roles)\b"
    # A named role is the matrix's subject, so naming one is the question.
    # ``operator`` is not in that list: this documentation calls its reader an
    # operator on every page, so the bare word says nothing about authorization
    # and only counts next to a permission verb.
    r"|\b(?:viewer|editor|admin|owner)s?\b"
    r"|\boperators?\s+(?:can|cannot|can't|may|need)\b"
    r"|\b(?:allowed|permitted)\s+to\b"
    r"|\baccess\s+(?:level|control)\b",
    re.I,
)

#: A permission question wants it, so it keeps the prior a definitional heading
#: earns. Anything else has to outrank it on its own terms.
#:
#: The penalty was swept against the 158-case answer audit and the operator
#: questions above. -3.0 was not enough to move "show me the audit log" off the
#: role list; -4.5 was the knee where it flipped to "Settings → Audit Logs lists
#: mapping decisions, job runs, quarantine events…"; -4.5 through -9.0 were
#: indistinguishable on both sets, so this sits above the knee with margin.
_ROLE_MATRIX_ON_ASK = 3.2
_ROLE_MATRIX_OFF_ASK = -6.0

#: The nine named cards. An enumeration ask used to pay them +3.0 whenever
#: their heading was on subject. "Which destinations can I write to" is on
#: subject for "Core gates (before write)" because the heading says ``write``,
#: so the answer opened G1–G3 instead of naming a destination. Same pattern
#: as the role matrix: the cards keep the prior when the question is about
#: the gates, and pay the off-ask cost otherwise.
_GATE_QUESTION = re.compile(r"\bgates?\b|\bpreflight\b", re.I)
_CORE_GATES_ON_ASK = 3.0
_CORE_GATES_OFF_ASK = 6.0

#: The UTC policy section and the TIMESTAMP-range section share ``timestamp``.
#: A question about where timestamps are stored wants UTC / offset label;
#: a question about 2038 wants the range. Same pattern as the gate cards.
_TIMEZONE_QUESTION = re.compile(
    r"\b(?:timezone|time\s*zones?|utc|offset\s+label)\b",
    re.I,
)
_TIMESTAMP_RANGE_TITLE = ("range", "instant carrier")
_TIMEZONE_TITLE_ON_ASK = 3.2
_TIMEZONE_TITLE_OFF_ASK = 3.2

#: Verbs and container nouns that appear on almost every procedure heading.
#: A heading that only shares these with the question is weakly on-subject —
#: "Procedure: connect Cursor" for "how do I connect a postgres database",
#: "Open Job Theater" / "Procedure: create a pipeline" for "can I schedule
#: a pipeline every night". The +3.0 procedure prior is withheld unless the
#: heading also names a distinctive term (postgres, cadence, pause, api).
#: When the question has no distinctive term ("how do I add a connector"),
#: the prior stays as it was.
def _section_intent_bonus(
    chunk: ProductDocChunk,
    analysis: QueryAnalysis,
) -> float:
    """Prefer the section shape the question asked for, when it is on subject.

    Two things were wrong with judging this on the heading's form alone. It fired
    off subject — "What is Datawrap?" outranked "What the row ledger proves" for
    *"what is a row ledger"* purely for being phrased as a definition. And it
    always penalized ``Procedure:`` sections, including for "how do I schedule a
    pipeline every hour", where the procedure *is* the answer; there was no ask
    classifier to consult, so a definition prior was applied to every question.

    The form of a heading is therefore worth nothing unless the heading names a
    subject the question is about, and what it is worth depends on the ask.
    """
    from .evidence_policy import is_subject_term

    # Decided before the on-subject gate, because the role matrix is *always*
    # nominally on subject: it names every verb in the product, so whatever the
    # operator asked about, its heading covers it.
    if (chunk.doc_title or "").strip().lower() == _ROLE_MATRIX_DOC:
        if _PERMISSION_QUESTION.search(analysis.text):
            return _ROLE_MATRIX_ON_ASK
        return _ROLE_MATRIX_OFF_ASK

    title = (chunk.section_title or "").strip().lower()
    counting = title.startswith(_COUNTING_TITLE)
    listing = title.startswith(_LISTING_TITLE)
    named_gate = bool(re.search(r"^what is g[1-9]\b", title))
    core_gates = "core gates" in title
    heading = set(content_terms(f"{chunk.doc_title} {chunk.section_title}"))
    on_subject = any(
        is_subject_term(term) and _covers(term, heading)
        for term in analysis.search_terms
    )
    numbered_gate_ask = bool(
        re.search(r"\bgate\s*[1-9]\b|\bg[1-9]\b", analysis.text, re.I)
    )
    if not on_subject:
        # The gate cards still have to pay the off-ask cost when they ranked
        # here on a body word. "Which destinations can I write to" does not
        # name a gate, so "Core gates (before write)" is not on subject — and
        # without a penalty it still wins the ranking, because G2 says
        # "Destination write access". G1 says "source connects", which is how
        # "how many sources can you connect to" opened on a gate card.
        if named_gate and not numbered_gate_ask:
            return -_CORE_GATES_OFF_ASK
        if listing and "gate" in title and analysis.ask == "count":
            return -_CORE_GATES_OFF_ASK
        if core_gates and not _GATE_QUESTION.search(analysis.text):
            return -_CORE_GATES_OFF_ASK
        return 0.0

    is_procedure = title.startswith("procedure:")
    definitional = title.startswith(_DEFINITIONAL_TITLE)
    ask = analysis.ask

    bonus = 0.0
    if ask == "count":
        # A count is published under a heading that asks for one. Left in the
        # definitional family below, a count ask paid "Core gates (before
        # write)" the +3.0 its nine named cards earn for a definition — and G1
        # reads "Source readable", so "how many sources can you connect to" led
        # with a gate description while the passage that states "30 sources and
        # 30 destinations" was not even in the evidence window.
        if counting:
            bonus += _COUNTING_TITLE_BONUS
        if is_procedure:
            bonus -= 2.8
        if named_gate:
            bonus -= _CORE_GATES_OFF_ASK
        return bonus
    # A counting heading on any other ask is the wrong container, and a heading
    # that has to name four inventories to say how many there are of each
    # overlaps nearly every question about any of them. Asked "which engines
    # can I connect to", "How many sync modes, roles, engines and formats there
    # are" outranked "Which engines you can connect" — 2.89 to 2.55 — and the
    # answer opened with four totals instead of naming one engine.
    #
    # Measured twice. When this penalty was first tried, a listing question
    # phrased "which engines can I connect to" was still classified a
    # procedure, and at -3.0 and -6.0 it moved neither the cardinality set nor
    # the audit; the conclusion recorded then was that it was unnecessary. It
    # was the ask that was wrong, not the penalty.
    if counting:
        bonus -= _COUNTING_TITLE_OFF_ASK
    if ask == "procedure":
        if is_procedure:
            # Weak overlap on ``connect`` / ``pipeline`` / ``schedule`` is how
            # MCP, Job Theater and the create-pipeline wizard beat the section
            # written about the distinctive object (PostgreSQL, cadence, pause).
            distinctive = distinctive_procedure_terms(analysis)
            if not distinctive or any(_covers(term, heading) for term in distinctive):
                bonus += 3.0
        if definitional:
            bonus -= 1.2
    elif ask == "diagnosis":
        # "Procedure: fix a blocked gate" is what an operator with a red gate
        # wants, not the definition of a gate.
        if is_procedure:
            bonus += 1.6
    elif ask == "consequence":
        # The outcome is in a fidelity or proof passage. A procedure that
        # names the subject is how "what happens if the destination count
        # does not match" opened on a delete-polarity caption.
        if is_procedure:
            bonus -= 1.6
    elif ask == "enumeration":
        if listing:
            bonus += 2.4
        if definitional:
            bonus += 1.5
        if is_procedure:
            bonus -= 1.5
    else:
        # definition · comparison · capability · other
        if definitional:
            bonus += 3.2
        if is_procedure:
            bonus -= 2.8
    # A numbered G-card is the answer to "what is G3" and the wrong
    # container for the set. ``explain the preflight gates`` is classified
    # definition on ``explain`` unless the inventory rule fires, so the
    # penalty cannot live only on the enumeration branch.
    gate_set_ask = bool(
        re.search(r"\bgates\b", analysis.text, re.I)
        and not re.search(r"\bgate\s*[1-9]\b|\bg[1-9]\b", analysis.text, re.I)
    )
    if named_gate and gate_set_ask:
        bonus -= 3.2
    if listing and gate_set_ask and ask != "enumeration":
        bonus += 2.4
    custom_roles_ask = bool(re.search(r"\bcustom\s+roles?\b", analysis.text, re.I))
    if "custom role" in title:
        bonus += 3.2 if custom_roles_ask else -3.2
    slack_connector_ask = bool(
        re.search(
            r"\bis\s+slack\s+a\s+connector\b"
            r"|\bslack\s+(?:as\s+a\s+)?(?:source|destination|connector)\b",
            analysis.text,
            re.I,
        )
    )
    if "slack a connector" in title:
        bonus += 3.2 if slack_connector_ask else -3.2
    pause_cdc_ask = bool(PAUSE_CDC_RE.search(analysis.text))
    if "pause cdc" in title or "pausing cdc" in title:
        bonus += 6.0 if pause_cdc_ask else -3.2
    if pause_cdc_ask and (
        "replication slot is" in title or "delete a cdc schedule" in title
    ):
        bonus -= 6.0
    if pause_cdc_ask and re.search(r"\bdrop\b", analysis.text, re.I):
        if "drop the replication slot" in title:
            bonus += 2.4
    generic_pause = bool(re.search(r"\bpause\b", analysis.text, re.I)) and not pause_cdc_ask
    if "pause a schedule" in title:
        bonus += 3.2 if generic_pause else (-3.2 if pause_cdc_ask else 0.0)
    salesforce_connect_ask = bool(
        re.search(
            r"connect\s+salesforce|salesforce\s+connection|set\s+up\s+salesforce",
            analysis.text,
            re.I,
        )
    )
    if "connect salesforce" in title:
        bonus += 3.2 if salesforce_connect_ask else -3.2
    if "sql server cdc" in title:
        bonus += 3.2 if re.search(r"\bsql\s+server\s+cdc\b", analysis.text, re.I) else -3.2
    if "change tracking" in title:
        bonus += 3.2 if re.search(r"\bchange\s+tracking\b", analysis.text, re.I) else -3.2
    if "snapshot hands off" in title or "snapshot handoff" in title:
        if re.search(r"\bhandoff\b|\bhand\s+off\b", analysis.text, re.I):
            bonus += 3.2
    slot_fill_ask = bool(
        re.search(
            r"\bslot\s+fills?\b|\bwal\s+fills?\b|\bmax_replication_slots\b"
            r"|\breplication\s+slot\s+fills?\b",
            analysis.text,
            re.I,
        )
    )
    if slot_fill_ask:
        if "max_replication_slots" in title:
            bonus += 6.0
        if title.startswith("what a replication slot"):
            bonus -= 6.0
    fabric_ask = bool(re.search(r"\bfabric\b|\bonelake\b", analysis.text, re.I))
    if "microsoft fabric" in title:
        bonus += 6.0 if fabric_ask else -3.2
    if fabric_ask and "teams as a destination" in title:
        bonus -= 6.0
    if "oracle xstream" in title:
        bonus += 6.0 if re.search(r"\bxstream\b", analysis.text, re.I) else -3.2
    if re.search(r"\bxstream\b", analysis.text, re.I) and "logminer" in title:
        bonus -= 6.0
    if "always on" in title:
        bonus += 6.0 if re.search(r"\balways\s+on\b|\bavailability\s+group", analysis.text, re.I) else -3.2
    if "managed identity" in title:
        bonus += 6.0 if re.search(r"\bmanaged\s+identity\b", analysis.text, re.I) else -3.2
    if "heartbeat interval" in title:
        bonus += 6.0 if re.search(r"\bheartbeat\s+interval\b|\bset\s+a\s+cdc\s+heartbeat\b", analysis.text, re.I) else -3.2
    if "cdc fetch size" in title:
        bonus += 6.0 if re.search(r"\bfetch\s+size\b|\bcdc\s+batch\s+size\b", analysis.text, re.I) else -3.2
    if "skip deletes" in title:
        bonus += 6.0 if re.search(r"\bskip\s+deletes?\b|\bignore\s+deletes?\b", analysis.text, re.I) else -3.2
    if "cosmos db" in title:
        bonus += 6.0 if re.search(r"\bcosmos\b", analysis.text, re.I) else -3.2
    if "event hubs" in title:
        bonus += 6.0 if re.search(r"\bevent\s+hubs?\b", analysis.text, re.I) else -3.2
    if "service bus" in title:
        bonus += 6.0 if re.search(r"\bservice\s+bus\b", analysis.text, re.I) else -3.2
    cloud_sql_ask = bool(re.search(r"\bcloud\s+sql\b", analysis.text, re.I))
    if "cloud sql" in title:
        bonus += 6.0 if cloud_sql_ask else -3.2
    if cloud_sql_ask and title == "do you support sql server":
        bonus -= 6.0
    azure_sql_ask = bool(re.search(r"\bazure\s+sql\b", analysis.text, re.I))
    if "azure sql" in title:
        bonus += 6.0 if azure_sql_ask else -3.2
    if azure_sql_ask and "azure synapse" in title:
        bonus -= 6.0
    gcs_ask = bool(
        re.search(
            r"\bgcs\b|\bgoogle\s+cloud\s+storage\b",
            analysis.text,
            re.I,
        )
    )
    if "gcs as a destination" in title:
        bonus += 6.0 if gcs_ask else -3.2
    if gcs_ask and "glue catalog" in title:
        bonus -= 6.0
    if "pub/sub" in title or "pubsub" in title:
        bonus += 6.0 if re.search(r"\bpub[\s/-]?sub\b", analysis.text, re.I) else -3.2
    if "cloud spanner" in title:
        bonus += 6.0 if re.search(r"\bspanner\b", analysis.text, re.I) else -3.2
    if re.search(r"\bspanner\b", analysis.text, re.I) and "dbt cloud" in title:
        bonus -= 6.0
    vertex_ask = bool(re.search(r"\bvertex\s+ai\b", analysis.text, re.I))
    if "vertex ai" in title:
        bonus += 6.0 if vertex_ask else -3.2
    if vertex_ask and (
        "chatgpt" in title or "third-party llm" in title or "hybrid" in title
    ):
        bonus -= 6.0
    if "sharepoint" in title:
        bonus += 6.0 if re.search(r"\bsharepoint\b", analysis.text, re.I) else -3.2
    if re.search(r"\bsharepoint\b", analysis.text, re.I) and "bigquery as a destination" in title:
        bonus -= 6.0
    if "dynamics 365" in title:
        bonus += 6.0 if re.search(r"\bdynamics\s*365\b|\bdataverse\b", analysis.text, re.I) else -3.2
    if re.search(r"\bdynamics\s*365\b|\bdataverse\b", analysis.text, re.I) and "dynamic tables" in title:
        bonus -= 6.0
    if "purview" in title:
        bonus += 6.0 if re.search(r"\bpurview\b", analysis.text, re.I) else -3.2
    if re.search(r"\bpurview\b", analysis.text, re.I) and "teams as a destination" in title:
        bonus -= 6.0
    if "excel online" in title:
        bonus += 6.0 if re.search(r"\bexcel\s+online\b|\bexcel\s+365\b", analysis.text, re.I) else -3.2
    iam_ask = bool(
        re.search(
            r"\biam\s+role\b|\bsts:assumerole\b|\bassume\s+an?\s+aws\s+iam\s+role\b",
            analysis.text,
            re.I,
        )
    )
    if "aws iam role" in title:
        bonus += 6.0 if iam_ask else -3.2
    if iam_ask and (
        "privatelink" in title or title.startswith("how many roles")
    ):
        bonus -= 6.0
    if CUSTOM_SLOT_NAME_RE.search(analysis.text):
        if "replication slot name" in title:
            bonus += 6.0
        if title.startswith("what a replication slot") or "max_replication_slots" in title:
            bonus -= 6.0
    scd2_ask = bool(re.search(r"\bscd\s*(?:type\s*)?2\b", analysis.text, re.I))
    if "scd type 2" in title:
        bonus += 6.0 if scd2_ask else -3.2
    if scd2_ask and title.endswith("scd1"):
        bonus -= 6.0
    if INCREMENTAL_UPDATED_AT_RE.search(analysis.text):
        if "incremental by updated_at" in title:
            bonus += 6.0
        if "difference between incremental and upsert" in title:
            bonus -= 6.0
    if "blue-green" in title or "blue green" in title:
        bonus += 6.0 if re.search(r"\bblue[\s-]green\b|\bzero[\s-]downtime\s+cutover\b", analysis.text, re.I) else -3.2
    azure_pg_ask = bool(re.search(r"\bazure\s+database\s+for\s+postgresql\b", analysis.text, re.I))
    if "azure database for postgresql" in title:
        bonus += 6.0 if azure_pg_ask else -3.2
    if azure_pg_ask and "cloud sql" in title:
        bonus -= 6.0
    azure_my_ask = bool(re.search(r"\bazure\s+database\s+for\s+mysql\b", analysis.text, re.I))
    if "azure database for mysql" in title:
        bonus += 6.0 if azure_my_ask else -3.2
    if azure_my_ask and "cloud sql" in title:
        bonus -= 6.0
    cloud_sql_mssql_ask = bool(re.search(r"\bcloud\s+sql\s+for\s+sql\s+server\b", analysis.text, re.I))
    if "cloud sql for sql server" in title:
        bonus += 6.0 if cloud_sql_mssql_ask else -3.2
    if cloud_sql_mssql_ask and title == "do you support cloud sql":
        bonus -= 6.0
    if "onedrive" in title:
        bonus += 6.0 if re.search(r"\bonedrive\b", analysis.text, re.I) else -3.2
    if re.search(r"\bonedrive\b", analysis.text, re.I) and "bigquery as a destination" in title:
        bonus -= 6.0
    looker_studio_ask = bool(re.search(r"\blooker\s+studio\b", analysis.text, re.I))
    if "looker studio" in title:
        bonus += 6.0 if looker_studio_ask else -3.2
    looker_ask = bool(re.search(r"\blooker\b", analysis.text, re.I)) and not looker_studio_ask
    if title == "do you support looker as a destination":
        bonus += 6.0 if looker_ask else -3.2
    if (looker_ask or looker_studio_ask) and "bigquery as a destination" in title:
        bonus -= 6.0
    if "bigquery omni" in title:
        bonus += 6.0 if re.search(r"\bbigquery\s+omni\b", analysis.text, re.I) else -3.2
    if re.search(r"\bbigquery\s+omni\b", analysis.text, re.I) and title == "do you support bigquery as a destination":
        bonus -= 6.0
    if "google sheets" in title:
        bonus += 6.0 if re.search(r"\bgoogle\s+sheets\b", analysis.text, re.I) else -3.2
    if re.search(r"\bgoogle\s+sheets\b", analysis.text, re.I) and "pub/sub" in title:
        bonus -= 6.0
    if "alloydb" in title:
        bonus += 6.0 if re.search(r"\balloydb\b", analysis.text, re.I) else -3.2
    if "microsoft graph" in title:
        bonus += 6.0 if re.search(r"\bgraph\b", analysis.text, re.I) else -3.2
    if re.search(r"\bmicrosoft\s+graph\b|\bms\s+graph\b", analysis.text, re.I) and "teams as a destination" in title:
        bonus -= 6.0
    if "azure openai" in title:
        bonus += 6.0 if re.search(r"\bazure\s+openai\b", analysis.text, re.I) else -3.2
    if "azure data explorer" in title or title.endswith("kusto"):
        bonus += 6.0 if re.search(r"\bdata\s+explorer\b|\bkusto\b", analysis.text, re.I) else -3.2
    if re.search(r"\bdata\s+explorer\b|\bkusto\b", analysis.text, re.I) and "azure data factory" in title:
        bonus -= 6.0
    if "event grid" in title:
        bonus += 6.0 if re.search(r"\bevent\s+grid\b", analysis.text, re.I) else -3.2
    if re.search(r"\bevent\s+grid\b", analysis.text, re.I) and "event hubs" in title:
        bonus -= 6.0
    if "google cloud dataflow" in title:
        bonus += 6.0 if re.search(r"\bdataflow\b", analysis.text, re.I) else -3.2
    if re.search(r"\bgoogle\s+(?:cloud\s+)?dataflow\b|\bcloud\s+dataflow\b", analysis.text, re.I) and "pub/sub" in title:
        bonus -= 6.0
    if "power platform" in title:
        bonus += 6.0 if re.search(r"\bpower\s+platform\b", analysis.text, re.I) else -3.2
    if re.search(r"\bpower\s+platform\b", analysis.text, re.I) and "power bi" in title:
        bonus -= 6.0
    if "conditional access" in title:
        bonus += 6.0 if re.search(r"\bconditional\s+access\b", analysis.text, re.I) else -3.2
    if re.search(r"\bconditional\s+access\b", analysis.text, re.I) and "azure synapse" in title:
        bonus -= 6.0
    if "cloud composer" in title:
        bonus += 6.0 if re.search(r"\bcloud\s+composer\b", analysis.text, re.I) else -3.2
    if re.search(r"\bcloud\s+composer\b", analysis.text, re.I) and "azure sql" in title:
        bonus -= 6.0
    if "azure data factory" in title:
        bonus += 6.0 if re.search(r"\bdata\s+factory\b|\badf\b", analysis.text, re.I) else -3.2
    if "adls as a destination" in title:
        bonus += 6.0 if re.search(r"\badls\b", analysis.text, re.I) else -3.2
    if "power bi" in title:
        bonus += 6.0 if re.search(r"\bpower\s*bi\b", analysis.text, re.I) else -3.2
    privatelink_ask = bool(re.search(r"\bprivate\s+link\b|\bprivatelink\b", analysis.text, re.I))
    if "privatelink" in title or "private service connect" in title:
        bonus += 6.0 if privatelink_ask else 0.0
    if privatelink_ask and "job theater" in title:
        bonus -= 6.0
    engine_set_ask = bool(
        re.search(r"\bengines?\b", analysis.text, re.I)
        and re.search(r"\bconnect", analysis.text, re.I)
        and not privatelink_ask
        and not re.search(r"\bdebezium\b|\bflink\b|\bkafka\s+connect\b", analysis.text, re.I)
    )
    if "which engines you can connect" in title:
        bonus += 8.0 if engine_set_ask else 0.0
    if engine_set_ask and (
        "debezium" in title or "privatelink" in title or "private key" in title
        or title.startswith("how many sources")
        or title.startswith("procedure: connect")
    ):
        bonus -= 8.0
    if core_gates:
        if _GATE_QUESTION.search(analysis.text):
            bonus += _CORE_GATES_ON_ASK
        else:
            bonus -= _CORE_GATES_OFF_ASK
    timezone_title = "utc" in title or "time zone" in title
    range_title = any(needle in title for needle in _TIMESTAMP_RANGE_TITLE)
    if timezone_title and _TIMEZONE_QUESTION.search(analysis.text):
        bonus += _TIMEZONE_TITLE_ON_ASK
    if range_title and _TIMEZONE_QUESTION.search(analysis.text):
        bonus -= _TIMEZONE_TITLE_OFF_ASK
    return bonus


def spoken_doc_excerpt(text: str, section_title: str = "", *, max_sentences: int = 3) -> str:
    """First definition sentences plus named G1–G9 cards — not Where:/Tip: steps."""
    body = (text or "").strip()
    title = (section_title or "").strip()
    if title and (
        body == title
        or body.startswith(title + "\n")
        or body.startswith(title + " ")
    ):
        body = body[len(title):].strip()
    sentences: list[str] = []
    gates: list[str] = []
    for raw in body.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line.startswith(_SKIP_HELP_LINE):
            continue
        if line.startswith("{") or line.startswith(("GET ", "POST ", "PUT ", "DELETE ")):
            continue
        if _UI_CAPTION.match(line):
            continue
        if _GATE_LINE.match(line):
            gates.append(line.rstrip("."))
            continue
        if not any(ch in line for ch in ".!?"):
            if _STEP_HEADING.match(line) or ":" not in line:
                continue
            # Screenshot captions: "Validate — core gate cards …"
            if "—" in line:
                continue
        for piece in _SENTENCE_SPLIT.split(line):
            piece = piece.strip()
            if not piece or piece.startswith(_SKIP_HELP_LINE):
                continue
            if piece.endswith(":"):
                continue
            if not piece.endswith((".", "!", "?")):
                piece += "."
            sentences.append(piece)
            if len(sentences) >= max_sentences and not gates:
                break
        if len(sentences) >= max_sentences and not gates:
            break
    out = " ".join(sentences[:max_sentences]).strip()
    if gates:
        gate_bit = " ".join(
            g if g.endswith((".", "!", "?")) else f"{g}."
            for g in gates[:9]
        )
        out = f"{out} {gate_bit}".strip()
    return out


@dataclass(frozen=True)
class ProductAnswer:
    """The retrieval result and the answerability decision made on it."""

    query: str
    analysis: QueryAnalysis
    hits: tuple[ProductDocHit, ...]
    verdict: EvidenceVerdict

    @property
    def answerable(self) -> bool:
        return self.verdict.answerable and bool(self.hits)

    @property
    def partial(self) -> bool:
        return self.answerable and self.verdict.partial

    @property
    def sources(self) -> list[dict[str, object]]:
        return [hit.as_source() for hit in self.hits]

    @property
    def caveat(self) -> str:
        """The sentence that names what the documentation does not cover.

        Only rendered for a partial answer. Saying it after the answer is what
        lets an operator act: a refusal tells them nothing, and a silent partial
        answer lets them believe the missing part was covered.
        """
        if not self.partial:
            return ""
        missing = [t for t in self.verdict.uncovered_subjects if len(t) > 2][:4]
        if not missing:
            return ""
        listed = ", ".join(f"“{t}”" for t in missing)
        return (
            f"The documentation I can cite does not cover {listed}, so that part "
            f"is not answered here — ask me to read your live workspace if it is "
            f"a question about your own jobs or tables."
        )


def _select_covering(
    ranked: Sequence[tuple[float, ProductDocHit]],
    limit: int,
    typed_terms: Sequence[str],
) -> list[ProductDocHit]:
    """Fill the evidence window to cover the question, not to repeat its best match.

    Ranking scores each passage on its own, but answerability is judged on what
    the window covers *jointly* — so taking the top ``limit`` by rank optimizes
    a different objective than the one the evidence policy then measures.
    "How do I move data from Postgres to Snowflake without losing decimal
    precision" asks two things; every one of the five best-ranked passages
    answered the route half, and the precision half never entered the window
    even though the corpus states it.

    Greedy selection with a novelty bonus and a redundancy penalty, the same
    maximal-marginal-relevance shape the sentence composer uses, one level up.
    """
    if len(ranked) <= limit:
        return [hit for _, hit in ranked]
    wanted = set(typed_terms)
    if not wanted:
        return [hit for _, hit in ranked[:limit]]

    top = max((score for score, _ in ranked), default=0.0) or 1.0
    pool = list(ranked)
    chosen: list[ProductDocHit] = []
    covered: set[str] = set()
    while pool and len(chosen) < limit:
        def value(pair: tuple[float, ProductDocHit]) -> float:
            score, hit = pair
            matched = set(hit.matched_terms) & wanted
            if not matched:
                return score / top
            fresh = matched - covered
            return (
                score / top
                + COVERAGE_NOVELTY * len(fresh) / len(wanted)
                - COVERAGE_REDUNDANCY * len(matched & covered) / len(matched)
            )

        pick = max(pool, key=value)
        pool.remove(pick)
        chosen.append(pick[1])
        covered |= set(pick[1].matched_terms) & wanted
    return chosen


def _rank_hits(
    analysis: QueryAnalysis,
    limit: int,
    grounding_floor: float,
) -> list[ProductDocHit]:
    """Fuse three rankings, then apply the heading/intent priors.

    The operator's own words and the loose expansion vocabulary are run as
    *separate* BM25 rankings rather than concatenated into one query.
    Concatenating them let the expansion outvote the question: "how do I connect
    to BigQuery" expands to include ``destination``, which the preflight-gates
    section uses heavily, and that section then outranked the connector article.

    Rank position and score magnitude are both used, because each is blind
    where the other sees. Reciprocal Rank Fusion alone, with the standard
    ``k=60``, is far too flat for a 66-passage corpus: rank 1 scores 1/61 and
    rank 10 scores 1/70, so after normalization every candidate landed between
    8.0 and 10.0 and the heading and intent priors decided the whole ranking.
    Measured on "what is change data capture", five unrelated sections sat
    within 3% of each other. Blending in the min-max normalized BM25 score
    restores the discrimination that the rank transform threw away, while RRF
    keeps the merge robust to one retriever's scale.
    """
    index, by_id = _index()
    depth = max(FUSION_CANDIDATES, limit * 3)

    # An exact phrase expansion is the operator's own words in the corpus's
    # spelling, so it leads the ranking with them.
    anchor_terms = list(analysis.anchor_terms)
    typed_terms = list(analysis.terms)
    typed_query = " ".join(anchor_terms) or analysis.text
    expansion_query = " ".join(analysis.loose_expansions)

    typed = index.search(typed_query, limit=depth)
    expanded = index.search(expansion_query, limit=depth) if expansion_query else []
    ngram = _ngram_index().search(analysis.expanded_text or analysis.text, limit=depth)

    # Grounding and matched terms are reported against the operator's own words:
    # a loose expansion term the passage happens to use is not something the
    # operator asked about, and reporting it as matched would misstate the
    # evidence. This is a separate search from the ranking one for that reason.
    reported = index.search(" ".join(typed_terms) or analysis.text, limit=depth)
    grounding = {hit.id: hit.grounding for hit in reported}
    matched = {hit.id: hit.matched_terms for hit in reported}
    bm25_score = {hit.id: hit.score for hit in typed}
    bm25_best = max(bm25_score.values(), default=0.0) or 1.0

    rankings = [("bm25_typed", [h.id for h in typed])]
    if expanded:
        rankings.append(("bm25_expanded", [h.id for h in expanded]))
    if ngram:
        rankings.append(("char_ngram", [h.id for h in ngram]))

    fused = reciprocal_rank_fusion(
        rankings,
        weights=RETRIEVER_WEIGHTS,
        limit=depth,
    )
    if not fused:
        return []

    # Normalize the fused score so the heading and intent priors keep the weight
    # they were calibrated with, whatever absolute values fusion produced.
    best = max(h.score for h in fused) or 1.0

    ranked: list[tuple[float, ProductDocHit]] = []
    for fused_hit in fused:
        chunk = by_id.get(fused_hit.id)
        if chunk is None:
            continue
        hit_grounding = grounding.get(fused_hit.id, 0.0)
        if hit_grounding < grounding_floor and fused_hit.rank_in("bm25_typed") is None:
            # No overlap with anything the operator typed: a match on the
            # expansion vocabulary or a spelling coincidence, not evidence.
            continue
        relevance = (
            RRF_WEIGHT * (fused_hit.score / best)
            + (1.0 - RRF_WEIGHT) * (bm25_score.get(fused_hit.id, 0.0) / bm25_best)
        )
        rank = FUSED_RANK_SCALE * relevance
        rank += TITLE_WEIGHT * _title_coverage(chunk, anchor_terms)
        rank += _section_intent_bonus(chunk, analysis)
        ranked.append(
            (
                rank,
                ProductDocHit(
                    chunk=chunk,
                    score=bm25_score.get(fused_hit.id, 0.0),
                    grounding=hit_grounding,
                    matched_terms=matched.get(fused_hit.id, ()),
                ),
            )
        )
    ranked.sort(key=lambda pair: pair[0], reverse=True)
    return _select_covering(ranked, limit, typed_terms)


def retrieve_product_answer(
    query: str,
    limit: int = 5,
    grounding_floor: float = GROUNDING_FLOOR,
) -> ProductAnswer:
    """Retrieve for one question and decide whether the result can answer it.

    This is the entry point callers should use: it exposes the verdict, so a
    question the documentation half covers can be answered with a caveat instead
    of refused.
    """
    analysis = analyze_query(query)
    hits = _rank_hits(analysis, limit=limit, grounding_floor=grounding_floor)
    # Headings are evidence of coverage too. Judging on body text alone reported
    # "column" as uncovered for a question answered out of three sections of
    # "Semantic column mapping", because the sections say "field" and "edge" in
    # their prose and put the word in the title.
    verdict = assess_evidence(
        analysis,
        [
            f"{hit.chunk.doc_title} {hit.chunk.section_title} {hit.chunk.text}"
            for hit in hits
        ],
        in_vocabulary=lambda term: _index()[0].idf(term) > 0,
    )
    if not verdict.answerable:
        hits = []
    return ProductAnswer(
        query=query,
        analysis=analysis,
        hits=tuple(hits),
        verdict=verdict,
    )


def product_doc_search(
    query: str,
    limit: int = 5,
    grounding_floor: float = GROUNDING_FLOOR,
) -> list[ProductDocHit]:
    """Documentation sections that actually cover the question, best first.

    Empty when the evidence policy refuses the question, so a caller that only
    checks for emptiness still fails closed on an off-subject ask.
    """
    return list(retrieve_product_answer(query, limit=limit, grounding_floor=grounding_floor).hits)


@lru_cache(maxsize=1)
def corpus_vocabulary() -> frozenset[str]:
    """Every content term the shipped documentation actually uses."""
    terms: set[str] = set()
    for chunk in load_product_doc_chunks():
        terms.update(content_terms(f"{chunk.doc_title} {chunk.section_title} {chunk.text}"))
    return frozenset(terms)


def names_product_subject(query: str) -> bool:
    """Whether the question names a subject this product documents.

    Intersecting the question with the whole corpus *vocabulary* was too loose in
    one direction: every passage contains ordinary English, so "what is the
    capital of France" named a product subject because one passage happens to
    use the word ``capital``. The test that holds is whether the question names
    something the documentation has a heading about, or one of the product's own
    enum values — after the operator's words are translated into the
    documentation's vocabulary.
    """
    from .evidence_policy import is_subject_term

    analysis = analyze_query(query)
    return any(is_subject_term(term) for term in analysis.search_terms)


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
    """Spoken answer quoted from the cited sections — definition, not the procedure dump.

    Section-lead form, used where no question is available to select against.
    Prefer :func:`compose_product_answer`, which selects the sentences that
    answer the question across the whole retrieved set.
    """
    parts: list[str] = []
    for hit in hits[:max_sections]:
        chunk = hit.chunk
        body = spoken_doc_excerpt(chunk.text, chunk.section_title)
        if not body:
            continue
        parts.append(f"**{chunk.section_title}** — {body}")
    cited = " · ".join(hit.chunk.citation for hit in hits[:max_sections])
    if cited:
        parts.append(f"Source: {cited} (Help)")
    return "\n\n".join(parts)


def compose_product_answer(answer: ProductAnswer) -> str:
    """The answer to *this* question, selected across every retrieved section.

    Falls back to the section-lead form when sentence selection finds nothing —
    a short section whose only sentence was filtered as a caption still has to
    produce an answer rather than an empty bubble.
    """
    from .answer_composer import compose_answer

    if not answer.hits:
        return ""
    sections = [
        (
            hit.chunk.section_title,
            hit.chunk.citation,
            f"{hit.chunk.href}#{hit.chunk.section_id}",
            hit.chunk.text,
        )
        for hit in answer.hits
    ]
    composed = compose_answer(
        answer.analysis,
        sections,
        scores=[hit.score for hit in answer.hits],
        idf=_index()[0].idf,
        partial_caveat=answer.caveat,
    )
    if composed:
        try:
            from src.ai.first_party.engine import narrate_answer

            evidence = " ".join(chunk_text for *_, chunk_text in sections)
            narrated = narrate_answer(answer.analysis.text, evidence, composed)
            if narrated:
                return narrated
        except Exception:
            pass
        return composed
    legacy = compose_documented_answer(list(answer.hits))
    if legacy and answer.caveat:
        return f"{legacy}\n\n{answer.caveat}"
    return legacy


def product_doc_documents() -> tuple[list[str], list[dict], list[str]]:
    """Help sections as vector-store documents so semantic search can reach them too."""
    texts: list[str] = []
    metas: list[dict] = []
    ids: list[str] = []
    for chunk in retrieval_passages():
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
                "source_module": chunk.source_module,
            }
        )
        ids.append(f"doc_{chunk.id}")
    return texts, metas, ids
