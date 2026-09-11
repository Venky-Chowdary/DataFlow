"""Whether the retrieved passages are enough to answer — the decision that was wrong.

The previous rule was a single absolute threshold: one passage had to cover 55%
of the question's IDF-weighted terms or the whole question was refused. That
measures the wrong thing twice over.

*It judges one passage at a time.* Real answers are assembled from several
sections — "which sync mode for a nightly load" is answered by the sync-mode
list plus the schedule cadence section, and neither covers the question alone.

*It scales with question length.* A six-word question can clear 55%; the same
question asked in a full sentence cannot, because every extra word raises the
denominator. Measured on the shipped corpus this refused 14 of 30 ordinary
operator questions, including "what happens to bad rows" and "how do I connect
to BigQuery" — both squarely documented.

What actually distinguishes an answerable question is not how much of it one
passage repeats, but whether it **names a subject the documentation is about**
and whether the retrieved set **covers that subject**. So the decision here rests
on two independent signals:

``anchor``    at least one of the question's terms (after expansion) is a subject
              the documentation has a heading for, and the retrieved passages
              cover it. This is what keeps an off-subject question refused:
              "what is the capital of France" does name a corpus term
              (``capital`` appears in one passage) but no heading is *about*
              capitals, so there is no anchor and the question is refused.

``coverage``  the share of the question's content terms the retrieved set
              accounts for. Below a floor the question is mostly about things
              the documentation never mentions.

Together they give three outcomes rather than two. The missing middle is what
made the old behaviour feel evasive: "how do I see why a job was slow" is a
question about ``job``, which is documented, plus ``slow``, which is not. The
honest response is the job-phase material *and* a sentence saying duration is not
covered — not a blanket refusal.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from functools import lru_cache

from .lexical_index import content_terms, normalize
from .query_analysis import GENERIC_QUESTION_WORDS, QueryAnalysis

#: Share of the question's content terms the retrieved set must account for
#: before the answer is offered without a caveat.
COVERAGE_FLOOR = 0.6

#: Below this the question is mostly about undocumented things, and naming one
#: documented subject is not enough to answer it. "write me a poem about the
#: sea" anchors on ``write`` — a real heading term — and is refused here.
PARTIAL_FLOOR = 0.34

#: A ``snake_case`` word an operator types is the name of one of their own
#: objects, not a concept. "What semantic patterns match subscriber_id column
#: naming" is about ``subscriber_id``; the documentation describes semantic
#: roles in general and cannot vouch for that column, so narrating the general
#: material as the answer would be exactly the invented answer this product
#: refuses to give. When the documentation does use the name — every sync mode
#: is a ``snake_case`` identifier — it is covered and nothing changes.
_MIN_OBJECT_NAME_LEN = 5

# Heading words that name no subject. A heading term is the strongest available
# signal that the documentation is *about* something, but headings also contain
# ordinary English. Without this subtraction "write me a poem" anchors on
# ``write`` and "get me a beer" anchors on ``get``.
_NON_SUBJECT_HEADING_WORDS = frozenset(
    normalize(w)
    for w in """
    add create open run runs get use used see work works write receive
    first next last before after related optional different same other
    guide guides tips tip example examples checklist procedure step steps
    question questions asked frequently concept concepts page surface
    support supported honest exact label bind hand bridge off
    live native advanced ready order fix inspect configure manage
    """.split()
)


@lru_cache(maxsize=1)
def product_subjects() -> frozenset[str]:
    """Terms the shipped documentation has a heading about.

    Derived from the corpus rather than hand-listed, so it grows when the corpus
    does. Article and section titles are what the documentation declares itself
    to be about, which is a far better answerability signal than IDF: the core
    subjects of this product (``connector``, ``gate``, ``pipeline``) appear in
    most passages and therefore have *low* IDF, while noise words like ``tips``
    and ``first`` have high IDF.
    """
    from .product_docs import load_product_doc_chunks

    terms: set[str] = set()
    for chunk in load_product_doc_chunks():
        terms.update(content_terms(f"{chunk.doc_title} {chunk.section_title}"))
    return frozenset(terms - _NON_SUBJECT_HEADING_WORDS - GENERIC_QUESTION_WORDS)


@lru_cache(maxsize=1)
def subject_aliases() -> frozenset[str]:
    """Subjects the product has, spelled the way an operator would type them.

    A heading says "Sync modes (exact product labels)"; an operator says
    ``upsert`` or ``scd2``. These are the product's own enum values and gate
    names — shipped behaviour, not authored prose — so anchoring on them is
    still anchoring on something the product documents by doing it.
    """
    extra: set[str] = set()
    try:
        from services.sync_cursor import CANONICAL_SYNC_MODES

        for mode in CANONICAL_SYNC_MODES:
            extra.update(content_terms(mode.replace("_", " ")))
            extra.add(normalize(mode))
    except Exception:
        pass
    try:
        from services.schedule_store import SCHEMA_POLICIES

        for policy in SCHEMA_POLICIES:
            extra.update(content_terms(policy.replace("_", " ")))
    except Exception:
        pass
    try:
        from services.rbac import role_names

        extra.update(normalize(r) for r in role_names())
    except Exception:
        extra.update({"viewer", "editor", "operator", "admin"})
    return frozenset(extra - GENERIC_QUESTION_WORDS - _NON_SUBJECT_HEADING_WORDS)


def is_subject_term(term: str) -> bool:
    """Whether one term names something this product documents."""
    return term in product_subjects() or term in subject_aliases()


def names_own_object(term: str) -> bool:
    """Whether a term is the ``snake_case`` name of one of the operator's objects."""
    return "_" in term and len(term) >= _MIN_OBJECT_NAME_LEN


@dataclass(frozen=True)
class EvidenceVerdict:
    """The answerability decision, with the numbers it was made on."""

    #: ``answer`` · ``partial`` · ``refuse``
    outcome: str
    coverage: float
    anchors: tuple[str, ...] = ()
    covered_anchors: tuple[str, ...] = ()
    uncovered_terms: tuple[str, ...] = ()
    #: The subset of ``uncovered_terms`` that names something this product
    #: documents elsewhere. Only these are worth telling the operator about:
    #: that the documentation does not contain the word "low" from "why is my
    #: mapping confidence low" is true and completely uninformative.
    uncovered_subjects: tuple[str, ...] = ()
    reason: str = ""
    subjects_in_question: tuple[str, ...] = field(default=())

    @property
    def answerable(self) -> bool:
        return self.outcome in {"answer", "partial"}

    @property
    def partial(self) -> bool:
        return self.outcome == "partial"

    def as_dict(self) -> dict[str, object]:
        return {
            "outcome": self.outcome,
            "coverage": round(self.coverage, 4),
            "anchors": list(self.anchors),
            "covered_anchors": list(self.covered_anchors),
            "uncovered_terms": list(self.uncovered_terms),
            "uncovered_subjects": list(self.uncovered_subjects),
            "reason": self.reason,
        }


def assess_evidence(
    analysis: QueryAnalysis,
    passage_texts: Sequence[str],
    *,
    coverage_floor: float = COVERAGE_FLOOR,
    partial_floor: float = PARTIAL_FLOOR,
) -> EvidenceVerdict:
    """Decide whether ``passage_texts`` can answer ``analysis``.

    ``passage_texts`` is the fused top-k, judged as one body of evidence — an
    answer composed from three sections is a normal answer, not a stretch.
    """
    question_terms = tuple(analysis.terms)
    if not question_terms:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=0.0,
            reason="The question carries no searchable terms.",
        )

    covered: set[str] = set()
    for text in passage_texts:
        covered.update(content_terms(text))

    subjects = tuple(t for t in analysis.search_terms if is_subject_term(t))
    anchors = tuple(dict.fromkeys(subjects))
    covered_anchors = tuple(a for a in anchors if a in covered)

    # Coverage is measured on what the operator actually typed. Expansion terms
    # widen retrieval; letting them also satisfy coverage would mean the
    # expansion table could talk itself into an answer.
    hit = [t for t in question_terms if t in covered]
    uncovered = tuple(t for t in question_terms if t not in covered)
    uncovered_subjects = tuple(t for t in uncovered if is_subject_term(t))
    coverage = len(hit) / len(question_terms)

    if not covered_anchors:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                "The question names no subject the documentation has a section about."
                if not anchors
                else "The retrieved sections do not cover the subject the question names."
            ),
        )

    if coverage < partial_floor:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"Only {coverage:.0%} of the question is covered by the documentation "
                f"— too little to answer without guessing."
            ),
        )

    unknown_objects = tuple(t for t in uncovered if names_own_object(t))
    if unknown_objects:
        return EvidenceVerdict(
            outcome="refuse",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"The question is about {', '.join(unknown_objects)}, which the "
                f"documentation does not name — that needs a live read, not an article."
            ),
        )

    # A partial answer is one that leaves part of the *subject* unanswered.
    # Keying this on any uncovered term made almost every answer partial, since
    # an ordinary question carries modifiers ("nightly", "low", "2am") that no
    # passage repeats and that nothing is missing without.
    if coverage < coverage_floor or uncovered_subjects:
        return EvidenceVerdict(
            outcome="partial",
            coverage=coverage,
            anchors=anchors,
            covered_anchors=covered_anchors,
            uncovered_terms=uncovered,
            uncovered_subjects=uncovered_subjects,
            subjects_in_question=subjects,
            reason=(
                f"Documented for {', '.join(covered_anchors)}; "
                f"not covered: {', '.join(uncovered_subjects)}."
                if uncovered_subjects
                else f"Partial coverage ({coverage:.0%})."
            ),
        )

    return EvidenceVerdict(
        outcome="answer",
        coverage=coverage,
        anchors=anchors,
        covered_anchors=covered_anchors,
        subjects_in_question=subjects,
        reason=f"Documentation covers {coverage:.0%} of the question.",
    )
