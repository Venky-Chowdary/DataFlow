"""Anchor coverage may outrank the coverage ratio, but not without a bound.

Refusing on the ratio alone declined questions the corpus answers outright:
"how do you handle very large decimals" is one part subject and three parts
framing, so it covered 33% and was refused even though ``decimal`` — its only
subject — was fully covered. Letting anchor coverage win fixed that and opened
a hole: "unmapped nonsense query zzqq" names ``query``, which is a documented
subject, and its remaining words are nonsense. It started answering.

The line between the two is not the ratio. It is whether the leftover words are
English this documentation uses at all.
"""

from __future__ import annotations

import pytest

from src.ai.rag.evidence_policy import PARTIAL_FLOOR, assess_evidence
from src.ai.rag.product_docs import retrieve_product_answer
from src.ai.rag.query_analysis import analyze_query


def test_framing_words_the_corpus_uses_do_not_block_an_answer() -> None:
    verdict = retrieve_product_answer("how do you handle very large decimals").verdict
    assert verdict.outcome in {"answer", "partial"}, verdict.reason


@pytest.mark.parametrize(
    "question",
    [
        "unmapped nonsense query zzqq",
        "what semantic patterns match subscriber_id column naming",
        "how do I cook rice",
    ],
)
def test_words_the_corpus_has_never_used_keep_the_refusal(question: str) -> None:
    verdict = retrieve_product_answer(question).verdict
    assert verdict.outcome == "refuse", f"{question!r}: {verdict.reason}"


def test_the_bound_is_the_whole_corpus_not_the_retrieved_passages() -> None:
    """A word absent from the top-k but present in the corpus is not foreign.

    Keying on the passages would make the bound depend on what ranked, which is
    the thing the evidence policy exists to second-guess.
    """
    analysis = analyze_query("how do you handle truncated unmapped nonsense decimals")
    passages = ["A decimal column is created as the NUMERIC type on postgresql."]
    baseline = assess_evidence(analysis, passages)
    assert baseline.coverage < 0.5, "the escape is only reached below the floor"

    everything_known = assess_evidence(
        analysis, passages, in_vocabulary=lambda _term: True
    )
    nothing_known = assess_evidence(
        analysis, passages, in_vocabulary=lambda term: term == "decimal"
    )
    assert everything_known.outcome in {"answer", "partial"}
    assert nothing_known.outcome == "refuse"


def test_an_uncovered_expansion_does_not_veto_the_escape() -> None:
    """The escape asks about the subjects the operator typed, not every anchor.

    Coverage is deliberately measured on typed words so the expansion table
    cannot talk a question into an answer. Requiring every *anchor* to be
    covered let it talk one out of an answer instead, which is the same defect
    facing the other way: "truncated unmapped nonsense decimals" expands
    ``truncated`` to the ``overwrite`` sync mode, a passage about decimals does
    not mention overwriting, and a question whose own subject was fully covered
    was refused over a word nobody typed.
    """
    analysis = analyze_query("how do you handle truncated unmapped nonsense decimals")
    passages = ["A decimal column is created as the NUMERIC type on postgresql."]
    verdict = assess_evidence(analysis, passages, in_vocabulary=lambda _term: True)
    assert verdict.coverage < PARTIAL_FLOOR, "the escape is only reached below the floor"

    expansion_anchors = set(verdict.anchors) - set(analysis.terms)
    assert expansion_anchors - set(verdict.covered_anchors), (
        "fixture no longer has an uncovered expansion anchor; "
        f"anchors={verdict.anchors} covered={verdict.covered_anchors}"
    )
    assert not verdict.uncovered_subjects
    assert verdict.outcome in {"answer", "partial"}, verdict.reason


def test_omitting_the_vocabulary_leaves_the_policy_as_it_was() -> None:
    analysis = analyze_query("how do you handle very large decimals")
    passages = ["A decimal column is created as NUMERIC on postgresql."]
    assert assess_evidence(analysis, passages).outcome in {"answer", "partial"}


# --------------------------------------------------------------------------
# A concept the documentation spells differently
# --------------------------------------------------------------------------
#
# The bound above is what keeps nonsense refused. Read literally it also
# refuses a real question asked in a real synonym: "explain aggregation" and
# "what does a breakdown show me" scored 0% against a passage that defines a
# grouped aggregate, because ``aggregation`` does not stem to ``aggregat`` and
# ``breakdown`` written as one word shares no token with "break down". A word
# the corpus has never used cannot be absent from a passage *informatively* —
# its absence is guaranteed — so counting it as a gap measures the spelling,
# not the evidence.


def test_a_synonym_a_phrase_rule_names_is_not_a_gap_in_its_own_answer() -> None:
    analysis = analyze_query("explain aggregation")
    assert "aggregation" in analysis.terms, analysis.terms
    passages = [
        "A grouped aggregate is one measure per distinct value of a column: "
        "group by splits the rows of a table into groups on one column."
    ]
    verdict = assess_evidence(
        analysis, passages, in_vocabulary=lambda term: term != "aggregation"
    )
    assert verdict.outcome in {"answer", "partial"}, verdict.reason
    assert "aggregation" not in verdict.uncovered_terms


def test_the_credit_reaches_only_the_words_inside_the_matched_phrase() -> None:
    """The rule spoke for ``aggregation``. It has nothing to say about ``zzqq``.

    Crediting every typed word once any phrase rule fires is how the expansion
    table would talk itself into an answer, which is what coverage is measured
    on typed words to prevent.
    """
    analysis = analyze_query("aggregation zzqqxx nonsense")
    passages = [
        "A grouped aggregate is one measure per distinct value of a column."
    ]
    verdict = assess_evidence(
        analysis, passages, in_vocabulary=lambda term: term == "group"
    )
    assert verdict.outcome == "refuse", verdict.reason
    assert "zzqqxx" in verdict.uncovered_terms


def test_a_phrase_rule_whose_meaning_is_absent_earns_nothing() -> None:
    """The credit is the rule's reading being borne out by the evidence.

    Firing is the rule's claim; its targets appearing in the retrieved set is
    the evidence for that claim. Without them the passage is about something
    else and the operator's word is a genuine gap.
    """
    analysis = analyze_query("explain aggregation")
    passages = ["Settings → Audit Logs lists mapping decisions and job runs."]
    verdict = assess_evidence(
        analysis, passages, in_vocabulary=lambda term: term != "aggregation"
    )
    assert verdict.outcome == "refuse", verdict.reason


def test_a_loose_expansion_still_cannot_buy_coverage() -> None:
    """Only the phrase tier is trusted; the single-word guesses are not.

    ``slow`` pointing at ``phase`` is a useful guess for recall and no evidence
    at all, and the distinction already exists in ``QueryAnalysis``. This holds
    the policy to the same tier boundary retrieval uses.
    """
    analysis = analyze_query("why is my transfer slow")
    assert analysis.loose_expansions, "fixture no longer has a loose expansion"
    loose = set(analysis.loose_expansions)
    passages = [" ".join(sorted(loose))]
    verdict = assess_evidence(
        analysis, passages, in_vocabulary=lambda term: term not in {"slow"}
    )
    assert "slow" in verdict.uncovered_terms, verdict


def test_phrase_evidence_reports_the_words_it_ate_and_what_they_mean() -> None:
    from src.ai.rag.query_analysis import phrase_evidence

    fired = dict(
        (consumed, targets)
        for consumed, targets in phrase_evidence("what is change data capture")
    )
    assert fired, "the CDC phrase rule no longer fires"
    consumed = {term for words in fired for term in words}
    # Stemmed, because the terms are the ones coverage is measured on.
    assert {"chang", "data", "captur"} <= consumed, consumed
    assert not phrase_evidence("what is a connector")
