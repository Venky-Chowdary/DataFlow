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
    RELEVANCE_FLOOR,
    Candidate,
    build_candidates,
    select_sentences,
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


# --------------------------------------------------------------------------
# An example is not the subject
# --------------------------------------------------------------------------

def test_a_term_matched_only_inside_a_code_literal_is_not_a_match() -> None:
    """The demo file is called orders; the sentence is not about column order.

    Asked "do you preserve column order" the pilot answered correctly and then
    appended three sentences of the CSV tutorial, each of which had matched
    ``order`` inside ``sample-orders.csv``, ``order_id`` or ``public.orders``.
    """
    literal = [
        (
            "Example transfer used in this guide",
            "Transfer Studio guide → Example transfer",
            "#/help/transfer-studio",
            "**Destination:** **File Export** → **CSV** → "
            "`exports/sample-orders.csv`.\n",
        )
    ]
    assert not build_candidates(analyze_query("do you preserve column order"), literal)


def test_the_same_term_in_prose_still_matches() -> None:
    """Only the literal is discounted — a sentence that says it keeps its credit."""
    prose = [
        (
            "Every aspect a migration certificate answers for",
            "Type fidelity → Aspects",
            "#/help/product-facts",
            "Column order is recorded on the certificate as carried or "
            "unsupported.\n",
        )
    ]
    candidates = build_candidates(analyze_query("do you preserve column order"), prose)
    assert candidates and candidates[0].score > 0


def test_a_sentence_naming_the_subject_then_showing_it_keeps_full_credit() -> None:
    """Prose and literal in one sentence scores as the prose alone would."""
    question = "which sync mode replaces the destination"
    with_literal = [
        ("S", "Help", "", "Sync mode `full_refresh_overwrite` replaces the destination.\n")
    ]
    without = [
        ("S", "Help", "", "Sync mode full_refresh_overwrite replaces the destination.\n")
    ]
    scored = build_candidates(analyze_query(question), with_literal)[0].score
    plain = build_candidates(analyze_query(question), without)[0].score
    # ``sync``, ``mode``, ``replaces`` and ``destination`` are all prose in both.
    assert scored == plain


# --------------------------------------------------------------------------
# The answer stops when it is answered
# --------------------------------------------------------------------------

def test_a_sentence_far_weaker_than_the_lead_is_left_out() -> None:
    """Relevance never ended selection — only the sentence budget did."""
    lead = Candidate(
        text="Quarantine is the rule that no row disappears silently.",
        section_title="What happens to bad rows",
        citation="Quarantine → Bad rows",
        href="#/help/product-facts",
        order=0,
        terms=frozenset({"quarantine", "row"}),
        score=10.0,
    )
    weak = Candidate(
        text="Wait until the file chip shows the row and column counts.",
        section_title="Procedure: run the sample-orders transfer",
        citation="Transfer Studio → Procedure",
        href="#/help/transfer-studio",
        order=1,
        terms=frozenset({"row", "column", "count"}),
        score=10.0 * RELEVANCE_FLOOR - 0.1,
    )
    assert select_sentences([lead, weak]) == [lead]


def test_a_sentence_just_over_the_floor_is_kept() -> None:
    """The floor sits in a measured gap, so it has to be a boundary, not a wall."""
    lead = Candidate(
        text="Quarantine is the rule that no row disappears silently.",
        section_title="What happens to bad rows",
        citation="Quarantine → Bad rows",
        href="#/help/product-facts",
        order=0,
        terms=frozenset({"quarantine", "row"}),
        score=10.0,
    )
    support = Candidate(
        text="Quarantined rows can be exported as CSV and replayed.",
        section_title="What happens to bad rows",
        citation="Quarantine → Bad rows",
        href="#/help/product-facts",
        order=1,
        terms=frozenset({"quarantine", "export", "replay"}),
        score=10.0 * RELEVANCE_FLOOR + 0.1,
    )
    assert select_sentences([lead, support]) == [lead, support]


def _gate(order: int, text: str, terms: set[str], score: float, vouched: bool) -> Candidate:
    return Candidate(
        text=text,
        section_title="Core gates (before write)",
        citation="Preflight gates → Core gates",
        href="#/help/preflight",
        order=order,
        terms=frozenset(terms),
        score=score,
        list_item=True,
        list_vouched=vouched,
    )


def test_a_vouched_list_item_is_exempt_from_the_floor() -> None:
    """A list the question asked for is admitted whole.

    G1 scores nothing against "what are the preflight gates" — the heading the
    question matched is what earns it — so a relevance floor measured against
    the lead would drop the gates the question asked to see.
    """
    lead = _gate(8, "G9 Data integrity — encoding, required nulls.", {"data", "integrity"}, 10.0, True)
    sibling = _gate(0, "G1 Source readable — source connects.", {"source", "readable"}, 0.2, True)
    assert select_sentences([lead, sibling]) == [sibling, lead]


def test_an_unvouched_list_item_has_to_clear_the_floor_like_any_other() -> None:
    """Being a list item is not itself a reason to be in the answer.

    The exemption exists because a heading the question matched speaks for
    items that repeat none of its words. With no such heading there is nothing
    vouching for the item, and the blanket exemption let any list in any
    retrieved passage past the floor: asked "can I limit who sees a connector",
    the four connector maturity labels were appended to an answer about roles.
    """
    lead = _gate(8, "G9 Data integrity — encoding, required nulls.", {"data", "integrity"}, 10.0, False)
    sibling = _gate(0, "G1 Source readable — source connects.", {"source", "readable"}, 0.2, False)
    assert select_sentences([lead, sibling]) == [lead]


def test_an_unvouched_list_item_does_not_drag_in_its_siblings() -> None:
    """List completion is for a list that was admitted as a unit."""
    picked = _gate(0, "**Beta** — works with known limits.", {"beta", "work", "limit"}, 3.0, False)
    sibling = _gate(1, "**Planned** — catalog presence only.", {"planned", "catalog"}, 0.1, False)
    assert select_sentences([picked, sibling]) == [picked]


def test_an_enumeration_ask_vouches_for_the_list_it_asked_for() -> None:
    """"What are the preflight gates" is a request for a list, so it gets one."""
    sections = [
        (
            "Core gates (before write)",
            "Preflight gates → Core gates",
            "#/help/preflight",
            "Core gates (before write)\nG1 Source readable — source connects\n"
            "G2 Destination write access — destination is reachable",
        ),
    ]
    candidates = build_candidates(analyze_query("what are the preflight gates"), sections)
    listed = [c for c in candidates if c.list_item]
    assert listed and all(c.list_vouched for c in listed)


def test_a_list_the_question_only_brushes_is_not_vouched_for() -> None:
    """Reaching a list is not the same as asking for one.

    A capability question whose heading overlap is one term of four gets the
    items it genuinely matched and no free pass for their siblings. Under the
    blanket exemption this list arrived whole, which is how four connector
    maturity labels ended up under an answer about roles.
    """
    sections = [
        (
            "Honest transfer-ready labels",
            "Connectors → Honest transfer-ready labels",
            "#/help/connectors",
            "Honest transfer-ready labels\n**Beta** — works with known limits.\n"
            "**Planned** — catalog presence only; do not schedule yet.",
        ),
    ]
    candidates = build_candidates(
        analyze_query("can I schedule a beta connector for production"), sections
    )
    listed = [c for c in candidates if c.list_item]
    assert listed, "expected the labels to be recognised as list items"
    assert not any(c.list_vouched for c in listed)
