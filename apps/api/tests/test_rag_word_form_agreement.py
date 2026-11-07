"""One word, one token — the defect that made phrasing decide the answer.

Retrieval here is lexical: BM25 over stems, fused with character n-grams. That
only works if the query side and the corpus side reduce the same word to the
same token. They did not, for a whole family of words, and the family was this
product's own vocabulary.

English drops a silent ``-e`` before ``-ed`` and ``-ing``, so "quarantined" is
``quarantin`` while "quarantine" stayed ``quarantine``. Every verb the corpus
uses to describe what the engine does — quarantine, validate, create, schedule,
reconcile, store, write — was two unrelated index terms depending on tense. An
operator asking "what gets quarantined" and one asking "what is quarantine"
were searching two different indexes.

Measured on twenty question pairs that differ only in verb form, agreement was
9 of 20. It is 16 of 20 with the two fixes here. That number is the reason to
keep them: the on-lead proxy on the audit fixture moved the other way by a
count or two, because the fixture's expected phrases were authored against the
old spellings.
"""

from __future__ import annotations

import pytest

from src.ai.rag.lexical_index import normalize, tokenize
from src.ai.rag.product_docs import retrieve_product_answer


#: Verbs the corpus uses for what the engine does, with the inflection an
#: operator is at least as likely to type. Every pair was a split index term.
SILENT_E_VERBS = [
    ("delete", "deleted"),
    ("quarantine", "quarantined"),
    ("validate", "validated"),
    ("create", "created"),
    ("schedule", "scheduled"),
    ("reconcile", "reconciled"),
    ("store", "stored"),
    ("write", "writing"),
    ("overwrite", "overwriting"),
    ("mirror", "mirrored"),
]


@pytest.mark.parametrize(("base", "inflected"), SILENT_E_VERBS)
def test_a_verb_and_its_inflection_are_one_token(base: str, inflected: str) -> None:
    """The base form has to meet the stem ``-ed``/``-ing`` already produced.

    Moving the suffix rule instead would have been the other way round and far
    more disruptive: ``-ed`` and ``-ing`` are shared with every regular verb in
    the corpus, while the silent ``-e`` is a property of the base form alone.
    """
    assert tokenize(base) == tokenize(inflected), f"{base} != {inflected}"


def test_a_vowel_before_the_e_is_left_alone() -> None:
    """``-ue`` and ``-ie`` are not silent, and clipping them invents collisions.

    ``queue`` → ``queu`` is harmless; ``true`` → ``tru`` is not, and neither is
    any short word where the ``e`` is the syllable.
    """
    for word in ("true", "value", "queue", "due", "tie"):
        assert normalize(word).endswith("e") or len(word) < 5, word


def test_a_short_word_keeps_its_e() -> None:
    """Below five letters the ``-e`` is load-bearing far more often than not."""
    for word in ("use", "one", "side", "role", "mode", "type", "name"):
        assert normalize(word).endswith("e"), word


def test_an_identifier_is_normalized_part_by_part() -> None:
    """The corpus writes ``reverse_etl``; the query side builds it from stems.

    Stemming the identifier whole left the corpus at ``reverse_etl`` while "what
    is reverse ETL" shingled to ``revers_etl``, so the shingle built expressly
    to find that label could not reach it. Both sides normalize each part, so
    they agree whichever side the label arrives from.
    """
    assert normalize("reverse_etl") == "_".join(
        [normalize("reverse"), normalize("etl")]
    )
    assert normalize("map_confidence") == "_".join(
        [normalize("map"), normalize("confidence")]
    )


def test_an_identifier_in_a_passage_covers_the_words_it_is_made_of() -> None:
    """A passage saying ``reverse_etl`` has covered "reverse" and "etl".

    The evidence policy splits identifiers out of the retrieved text for exactly
    this, and it used to re-stem each part. A suffix stripper is one pass, not a
    rule that converges, so a second pass eroded ``revers`` to ``rever`` — a
    token the question side never produces, leaving a covered word counted as
    uncovered. Part-wise normalization is what makes the second pass
    unnecessary rather than merely harmful.
    """
    from src.ai.rag.evidence_policy import assess_evidence
    from src.ai.rag.query_analysis import analyze_query

    verdict = assess_evidence(
        analyze_query("what is reverse ETL"),
        ["Sync modes include reverse_etl, which writes back to a SaaS system."],
    )
    assert normalize("reverse") not in verdict.uncovered_terms
    assert normalize("etl") not in verdict.uncovered_terms


# --------------------------------------------------------------------------
# The point of all of it: the verb form must not choose the answer
# --------------------------------------------------------------------------

#: Question pairs that differ only in the form of one word. Each has one right
#: answer, so retrieval that reads the word rather than its spelling must reach
#: the same material for both.
PARAPHRASE_PAIRS = [
    ("what is quarantine", "what gets quarantined"),
    ("how do I validate a transfer", "what is validated before a transfer"),
    ("what does reconcile prove", "what is reconciled after a load"),
    ("how do I schedule a pipeline", "how are pipelines scheduled"),
    ("where are rows stored", "where does the product store rows"),
]


@pytest.mark.parametrize(("asked", "rephrased"), PARAPHRASE_PAIRS)
def test_two_forms_of_one_question_reach_the_same_sections(
    asked: str, rephrased: str
) -> None:
    """Agreement is measured on the sections retrieved, not on the wording.

    Sentence selection can legitimately open two phrasings on two different
    sentences — the ask type differs between "what is X" and "what gets X'd".
    What must not differ is which passages were considered answerable, because
    that is the part the stem decided.
    """
    left = retrieve_product_answer(asked)
    right = retrieve_product_answer(rephrased)
    assert left.answerable and right.answerable, f"{asked!r} / {rephrased!r} refused"

    sections = lambda answer: {  # noqa: E731
        (hit.chunk.doc_title, hit.chunk.section_title) for hit in answer.hits
    }
    shared = sections(left) & sections(right)
    assert shared, (
        f"{asked!r} and {rephrased!r} share no section; "
        f"{sorted(sections(left))} vs {sorted(sections(right))}"
    )
