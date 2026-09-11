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

from src.ai.rag.evidence_policy import assess_evidence
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


def test_omitting_the_vocabulary_leaves_the_policy_as_it_was() -> None:
    analysis = analyze_query("how do you handle very large decimals")
    passages = ["A decimal column is created as NUMERIC on postgresql."]
    assert assess_evidence(analysis, passages).outcome in {"answer", "partial"}
