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
    LEAD_SUBJECT_FLOOR,
    PHRASE_CREDIT,
    RELEVANCE_FLOOR,
    Candidate,
    _shape_bonus,
    build_candidates,
    select_sentences,
    split_sentences,
    subject_words,
    term_weights,
)
from src.ai.rag.lexical_index import (
    adjacent_shingles,
    content_terms,
    identifier_shingles,
    normalize,
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
    label = normalize("map_confidence")
    assert label in identifier_shingles([normalize("map"), normalize("confidence")])
    assert label not in adjacent_shingles("semantic mapping with confidence")
    assert label in adjacent_shingles("why is my mapping confidence low")


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

def test_an_inventory_lead_that_ends_in_a_colon_is_kept() -> None:
    """Live tool prose was "You have **2 saved connector(s)**:".

    The splitter dropped every colon-terminated piece so the inventory
    sentence never became the lead and the first bullet did.
    """
    kept = split_sentences("You have **2 saved connector(s)**:\n• **Demo Orders**")
    assert kept
    assert kept[0].startswith("You have **2 saved connector(s)**")


def test_a_heading_colon_is_still_dropped() -> None:
    """Fill: / Where: captions are not sentences."""
    kept = split_sentences("Fill:\n1. Pipeline name")
    assert not any(s.startswith("Fill") for s in kept)


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


def test_an_unvouched_list_item_earns_no_list_credit() -> None:
    """Partial heading overlap is not a voucher, so it cannot pay list credit.

    "**Destination write → Query**" matched ``write`` in "Write modes
    (Advanced)" and collected ``LIST_ITEM_CREDIT * 0.5``. That was enough to
    outrank "Destinations a transfer can write to, 37 of them" for the
    question that heading answers.
    """
    from src.ai.rag.answer_composer import LIST_ITEM_CREDIT, PHRASE_CREDIT

    sections = [
        (
            "Which destinations you can write to",
            "Connections & engines → Which destinations you can write to",
            "#/help/dest",
            "Which destinations you can write to\n"
            "Destinations a transfer can write to, 37 of them.",
        ),
        (
            "Write modes (Advanced)",
            "Transfer Studio guide → Write modes (Advanced)",
            "#/help/write",
            "Write modes (Advanced)\n"
            "**Destination write → Query** — one INSERT/MERGE/UPDATE.",
        ),
    ]
    candidates = build_candidates(
        analyze_query("which destinations can I write to"), sections
    )
    dest = next(c for c in candidates if c.text.startswith("Destinations a transfer"))
    caption = next(c for c in candidates if "Destination write" in c.text)
    assert dest.score > caption.score, (dest.score, caption.score)
    assert not caption.list_vouched
    # The credit is the thing we took away; if it comes back, this assertion
    # is how we notice before the lead flips.
    assert LIST_ITEM_CREDIT > PHRASE_CREDIT


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


# --------------------------------------------------------------------------
# The lead names the subject
# --------------------------------------------------------------------------

def _idf(known: dict[str, float]):
    """Corpus IDF for the words under test; anything else is average."""
    return lambda term: known.get(term, 1.0)


def _opening(question: str, sections: list[tuple[str, str, str, str]], **kw) -> str:
    """The sentence the operator actually reads first."""
    chosen = select_sentences(build_candidates(analyze_query(question), sections, **kw))
    assert chosen, f"no sentence selected for {question!r}"
    return chosen[0].text


_MODE_SECTION = ("What each sync mode does", "Sync modes → What each sync mode does", "#/help/sync-modes")


def test_the_lead_names_what_the_question_is_about() -> None:
    """"What does mirror mode do to deleted rows" opened on upsert.

    Scores are a sum, so the upsert sentence won on ``mode``, ``rows`` and its
    repeated ``keys`` while never saying ``mirror`` — the one word that made
    the question specific. The operator asked about deletes and read a
    paragraph about inserts and updates.
    """
    text = (
        "What each sync mode does\n"
        "Upsert mode writes rows key-idempotently: new keys insert, known keys "
        "update, and no rows are removed in this mode.\n"
        "Mirror makes the destination match the source.\n"
    )
    opening = _opening(
        "what does mirror mode do to deleted rows",
        [(*_MODE_SECTION, text)],
        idf=_idf({"mirror": 4.2, "mode": 1.0, "rows": 0.9, "delet": 2.0}),
    )
    assert opening.startswith("Mirror makes the destination match the source")


def test_among_subject_naming_sentences_the_score_still_decides() -> None:
    """This reorders the shortlist; it does not replace the ranking."""
    text = (
        "What each sync mode does\n"
        "Mirror is one of the five modes.\n"
        "Mirror mode deletes rows at the destination that the source no longer has.\n"
    )
    opening = _opening(
        "what does mirror mode do to deleted rows",
        [(*_MODE_SECTION, text)],
        idf=_idf({"mirror": 4.2, "mode": 1.0, "rows": 0.9, "delet": 2.0}),
    )
    assert opening.startswith("Mirror mode deletes rows at the destination")


def test_nothing_naming_the_subject_leaves_the_ranking_alone() -> None:
    """The rule never asks for a lead that was not retrieved.

    The anchor is the rarest word the operator typed *that some retrieved
    sentence uses*, so a passage answering entirely in its own vocabulary still
    leads with its best sentence instead of with whatever says the rarest word.
    """
    text = (
        "What each sync mode does\n"
        "Upsert mode writes rows key-idempotently: new keys insert, known keys "
        "update, and no rows are removed in this mode.\n"
        "Append mode adds rows and removes nothing.\n"
    )
    opening = _opening(
        "what does mirror mode do to deleted rows",
        [(*_MODE_SECTION, text)],
        idf=_idf({"mirror": 4.2, "mode": 1.0, "rows": 0.9, "delet": 2.0}),
    )
    assert opening.startswith("Upsert mode writes rows key-idempotently")


def test_a_list_the_question_asked_for_still_leads() -> None:
    """"What are the preflight gates" opens on G1, not on prose about gates.

    Preferring a subject-naming prose sentence unconditionally is how this
    stopped happening: the section heading says ``gates`` and no single gate
    says ``preflight``, so every item scored as naming nothing.
    """
    sections = [
        (
            "Core gates (before write)",
            "Preflight gates → Core gates",
            "#/help/preflight",
            "Core gates (before write)\n"
            "G1 Source readable — the source connector connects and the table reads.\n"
            "G2 Destination write access — the destination is reachable and writable.\n",
        ),
    ]
    assert _opening("what are the preflight gates", sections).startswith("G1 Source readable")


def test_a_count_question_leads_on_the_sentence_holding_the_number() -> None:
    """"How many" has one shape of answer, and it is not a tour of the page.

    Measured over eighteen cardinality phrasings, 3 led with a number before
    the count ask existed and 12 do now. The sentence's own shape decides:
    a heading about engines cannot make a sentence without a number into the
    answer to "how many".
    """
    sections = [
        (
            "Which engines you can connect",
            "Connections & engines → Which engines you can connect",
            "#/help/connectors",
            "Which engines you can connect\n"
            "You add a connector under Connectors and Test it before use.\n"
            "There are 46 transfer-ready connector drivers today.\n",
        ),
    ]
    assert _opening("how many connectors do you support", sections).startswith(
        "There are 46 transfer-ready"
    )


def test_the_number_counts_wherever_the_sentence_puts_it() -> None:
    """The shape test searches; it does not anchor at the start of the sentence.

    A definition and an imperative are anchored — they are recognised by how the
    sentence opens. A count is not: "Every role is one of viewer, operator,
    editor or admin" answers "how many roles are there" with its number in the
    middle, and requiring it first withheld the credit from every sentence that
    actually answered.
    """
    mid = "Exactly nine preflight gates run before any production write."
    tail = "Preflight runs before any production write, across nine gates."
    for sentence in (mid, tail):
        assert _shape_bonus("count", sentence) > 0, sentence
    assert _shape_bonus("count", "Preflight runs before any production write.") == 0


def test_a_subject_naming_sentence_far_below_the_top_does_not_lead() -> None:
    """Naming the subject says a sentence is on topic, not that it answers.

    Without a floor the rarest word decided the lead wherever it appeared, so
    "how do you handle booleans across databases" opened on a sidebar caption
    and "how does semantic column mapping decide a type" on a sentence about the
    CDC snapshot handoff. Both scored a fraction of the sentence that answered.
    """
    text = (
        "What each sync mode does\n"
        "Deleted rows are removed at the destination in this mode, so a mode "
        "that deletes rows leaves the destination holding exactly the rows the "
        "source holds, and rows deleted at the source are deleted here too.\n"
        "The sidebar groups every guide under Platform, Operations and System, "
        "and the mirror page sits beside the schedule page, the team page, the "
        "audit page and the settings page in the second of those three groups "
        "rather than in the first or the last one, which is worth knowing "
        "before you go looking for it in the navigation.\n"
    )
    opening = _opening(
        "what does mirror mode do to deleted rows",
        [(*_MODE_SECTION, text)],
        idf=_idf({"mirror": 4.2, "mode": 1.0, "rows": 0.9, "delet": 2.0}),
    )
    assert opening.startswith("Deleted rows are removed at the destination")


def test_the_floor_is_a_share_of_the_top_score_not_an_absolute() -> None:
    """Scores are unnormalized sums, so only a ratio is comparable across questions.

    A one-word question and a nine-word one produce scores an order of magnitude
    apart; any absolute bar would be permissive for one and prohibitive for the
    other.
    """
    assert 0.0 < LEAD_SUBJECT_FLOOR < 1.0


def test_the_rarest_word_the_question_typed_is_its_subject() -> None:
    """``subject_words`` reads the anchor off corpus IDF, not off word order."""
    analysis = analyze_query("what does mirror mode do to deleted rows")
    available = frozenset({"mirror", "mode", "rows", "delet"})
    subject = subject_words(analysis, available, _idf({"mirror": 4.2, "delet": 2.0}))
    assert "mirror" in subject
    assert "mode" not in subject and "rows" not in subject


def test_a_phrase_expansion_counts_as_the_subject_named_in_the_corpus_spelling() -> None:
    """"What is change data capture" is answered by sentences saying ``cdc``.

    The corpus never spells the phrase out, so on the typed anchor alone the
    answer left the sync-mode passage to find a sentence using ``capture``.
    """
    analysis = analyze_query("what is change data capture")
    assert "cdc" in analysis.phrase_expansions
    subject = subject_words(analysis, frozenset(analysis.terms), _idf({}))
    assert "cdc" in subject


def test_a_loose_expansion_is_recall_not_subject() -> None:
    """Loose expansions are a guess for recall, and often a rarer guess.

    "How do I see the logs" expands to ``theater``, which the corpus uses in
    far fewer places than ``logs``. Counted as the subject it would decide the
    lead, so the answer would open on whichever sentence names the page rather
    than on the one that says where the log is.
    """
    analysis = analyze_query("how do I see the logs")
    assert "theater" in analysis.loose_expansions
    subject = subject_words(
        analysis,
        frozenset({*analysis.terms, "theater"}),
        _idf({"logs": 2.0, "theater": 5.0}),
    )
    assert subject == frozenset({"logs"})


def test_words_as_rare_as_the_rarest_all_name_the_subject() -> None:
    """A gap too small to mean anything must not decide the lead.

    "What does mirror mode do to deleted rows" scores ``delet`` at 2.60 and
    ``mirror`` at 2.48. On the single rarest word the sentence defining mirror
    mode was not *about* mirror mode, and the answer opened with the definition
    of CDC, which says "deletes".
    """
    analysis = analyze_query("what does mirror mode do to deleted rows")
    subject = subject_words(
        analysis,
        frozenset(analysis.terms),
        _idf({"delet": 2.60, "mirror": 2.48, "mode": 1.62, "rows": 1.42}),
    )
    assert {"delet", "mirror"} <= subject
    # The band is a band, not an amnesty: the ordinary words of the question
    # still do not speak for its subject.
    assert "mode" not in subject
    assert "rows" not in subject


def test_the_band_widens_on_a_statistic_and_never_on_its_absence() -> None:
    """A word the corpus never uses cannot be banded against.

    Scaling zero by anything is zero, so every typed word would clear the bar
    and the subject test would stop discriminating entirely. The head-last
    tiebreak still decides that case, because English puts the head last.
    """
    analysis = analyze_query("how is this different from writing ETL scripts")
    subject = subject_words(
        analysis, frozenset(analysis.terms), _idf(dict.fromkeys(analysis.terms, 0.0))
    )
    assert "script" in subject
    assert "writ" not in subject


def test_an_exact_tie_no_longer_hides_the_head_of_the_noun_phrase() -> None:
    """Tied words are indistinguishable, so the score decides among them.

    "How is this different from writing ETL scripts" scores ``writ``, ``etl``
    and ``script`` at exactly 4.19 each. Taking one made the question about
    *writing*, and the answer opened on "writing one into an instant column"
    from the timestamp passage. Admitting all three does not bring that back:
    ``script`` is still the subject, and among sentences that name a subject
    word the ranking still chooses.
    """
    analysis = analyze_query("how is this different from writing ETL scripts")
    subject = subject_words(
        analysis, frozenset(analysis.terms), _idf(dict.fromkeys(analysis.terms, 4.19))
    )
    assert {"writ", "etl", "script"} <= subject


def test_no_corpus_statistic_means_no_subject_rule() -> None:
    """Without IDF there is no basis for calling one typed word the subject."""
    analysis = analyze_query("what does mirror mode do to deleted rows")
    assert subject_words(analysis, frozenset(analysis.terms), None) == frozenset()
