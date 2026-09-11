"""Sentence selection was scoring words, not evidence.

Three defects shared one cause: ``build_candidates`` counted how many of the
question's words a sentence contained and nothing else. So the rare word that
made the question specific counted the same as the common one that did not, a
sentence mentioning both halves of a phrase counted as having said the phrase,
and a caption about the documentation's own screenshots counted as product
content.

Each contract here names the answer the pilot gave before the signal existed.
"""

from __future__ import annotations

from src.ai.rag.answer_composer import (
    PHRASE_CREDIT,
    build_candidates,
    split_sentences,
    term_weights,
)
from src.ai.rag.lexical_index import (
    adjacent_shingles,
    content_terms,
    identifier_shingles,
)
from src.ai.rag.query_analysis import analyze_query


def _lead(question: str, sections: list[tuple[str, str, str, str]], **kw) -> str:
    candidates = build_candidates(analyze_query(question), sections, **kw)
    assert candidates, f"no candidate sentence for {question!r}"
    return candidates[0].text


# --------------------------------------------------------------------------
# Term specificity
# --------------------------------------------------------------------------

def test_a_rare_word_outweighs_a_common_one() -> None:
    """"What about generated columns" tied three sentences at exactly 1.80.

    One contained ``generated`` — a word in a single passage of the corpus —
    and the others contained ``column``, which is in dozens. The tie broke on
    document order, so the answer opened on NOT NULL.
    """
    text = (
        "NOT NULL is one of those carried aspects, so a required column stays "
        "required on the destination.\n"
        "Views, triggers and generated expressions are certified as "
        "unsupported rather than quietly omitted.\n"
    )
    sections = [("What a create-new carries", "Help", "", text)]
    idf = {"generat": 4.0, "column": 0.5}.get

    flat = _lead("what about generated columns", sections)
    weighted = _lead(
        "what about generated columns",
        sections,
        idf=lambda t: idf(t) or 0.0,
    )
    assert flat.startswith("NOT NULL")
    assert weighted.startswith("Views, triggers and generated expressions")


def test_weights_average_one_so_the_other_constants_still_hold() -> None:
    weights = term_weights(("a", "b", "c"), {"a": 1.0, "b": 2.0, "c": 3.0}.get)
    assert round(sum(weights.values()) / len(weights), 6) == 1.0


def test_a_word_the_corpus_never_uses_keeps_the_average_weight() -> None:
    """Zero IDF means unseen, which is distinctive — not free to miss."""
    weights = term_weights(("known", "unseen"), {"known": 2.0, "unseen": 0.0}.get)
    assert weights["unseen"] == 1.0


def test_no_idf_supplied_leaves_scoring_unweighted() -> None:
    assert term_weights(("a", "b"), None) == {}


# --------------------------------------------------------------------------
# Phrases, and only real ones
# --------------------------------------------------------------------------

def test_the_exact_phrase_beats_the_same_words_scattered() -> None:
    """"Do you preserve column order" was answered by a CSV tutorial line.

    "5 columns: order_id, customer_email, order_amt" has both words and
    neither meaning; the sentence that states the rule has to enumerate all 26
    certificate aspects, so on unigrams alone it lost on length.
    """
    text = (
        "Source is sample-orders.csv with 5 rows and 5 columns: order_id, "
        "customer_email, order_amt, order_date, status.\n"
        "Every migration certificate accounts for each of these aspects, "
        "recording it as carried or unsupported: primary key, unique, foreign "
        "key, check, not null, default, identity sequence, generated, "
        "collation, encoding, decimal, charset, index, partitioning, "
        "tablespace, clustering, comment, view, trigger, column order "
        "(column_order), name case.\n"
    )
    sections = [("Every aspect a certificate answers for", "Help", "", text)]
    assert _lead("do you preserve column order", sections).startswith(
        "Every migration certificate"
    )


def test_filler_breaks_a_phrase_instead_of_being_squeezed_out() -> None:
    """"Semantic mapping with confidence" is not the phrase "mapping confidence".

    Shingling the sentence after filler removal invented it, and a feature-list
    sentence then outranked the section that explains a low confidence score.
    """
    assert "map_confidence" in identifier_shingles(["map", "confidence"])
    assert "map_confidence" not in adjacent_shingles("semantic mapping with confidence")
    assert "map_confidence" in adjacent_shingles("why is my mapping confidence low")


def test_the_phrase_matches_either_spelling_of_an_aspect() -> None:
    """The corpus writes both, so the credit reads both.

    Prose reaches the phrase by adjacency; the identifier is already one token,
    which is why the composer credits a phrase found in either set.
    """
    assert "column_order" in adjacent_shingles("preserve column order")
    assert "column_order" in content_terms("the column_order aspect")


def test_phrase_credit_survives_a_long_sentence() -> None:
    """Length normalization discounts incidental matches; a phrase is not one."""
    short = [("S", "Help", "", "Column order is recorded on the certificate.\n")]
    assert PHRASE_CREDIT > 0
    candidates = build_candidates(analyze_query("do you preserve column order"), short)
    assert candidates[0].score >= PHRASE_CREDIT


# --------------------------------------------------------------------------
# Captions are not content
# --------------------------------------------------------------------------

def test_a_sentence_about_the_screenshots_is_not_an_answer() -> None:
    """It contains the phrase the operator typed and nothing they can act on.

    "How do I create a recurring sync" opened with "Screenshots are from the
    live Create recurring sync form and pipeline detail drawer — not Jobs".
    """
    line = (
        "Follow this exact path in the signed-in workspace. Screenshots are "
        "from the live **Create recurring sync** form and pipeline detail "
        "drawer — not Jobs."
    )
    sentences = split_sentences(line)
    assert any("Follow this exact path" in s for s in sentences)
    assert not [s for s in sentences if "creenshot" in s]


def test_marker_captions_go_too() -> None:
    kept = split_sentences(
        "Follow all five steps in order. Markers on each screenshot match the "
        "live product UI."
    )
    assert kept == ["Follow all five steps in order."]
