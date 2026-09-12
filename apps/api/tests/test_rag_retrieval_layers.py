"""Unit contracts for the stages between an operator's words and an answer.

Each stage here was added because the pipeline failed without it, and each of
these tests names the question that failed. They are unit-level on purpose: the
end-to-end floors live in ``test_pilot_answer_audit_eval.py``, and a floor tells
you the engine regressed without telling you where.

Reading order matches the pipeline:

    query analysis → retrieval passages → ranking / evidence window
                   → evidence policy → answer composition
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.rag.answer_composer import (  # noqa: E402
    LENGTH_PIVOT,
    Candidate,
    build_candidates,
    compose_answer,
    select_sentences,
    split_sentences,
)
from src.ai.rag.evidence_policy import (  # noqa: E402
    assess_evidence,
    is_subject_term,
    names_own_object,
)
from src.ai.rag.lexical_index import identifier_shingles, normalize  # noqa: E402
from src.ai.rag.product_docs import (  # noqa: E402
    MAX_SECTION_CHARS,
    ProductDocHit,
    _CORE_GATES_OFF_ASK,
    _COUNTING_TITLE_BONUS,
    _COUNTING_TITLE_OFF_ASK,
    _ROLE_MATRIX_DOC,
    _section_intent_bonus,
    _select_covering,
    _split_procedure,
    compose_product_answer,
    load_product_doc_chunks,
    retrieval_passages,
    retrieve_product_answer,
)
from src.ai.rag.query_analysis import (  # noqa: E402
    GENERIC_QUESTION_WORDS,
    analyze_query,
    classify_ask,
    expand_terms_tiered,
)


# --- term normalization -----------------------------------------------------

def test_a_doubled_consonant_before_ing_or_ed_is_undone():
    """"Mapping" reached ``mapp`` while every passage saying "map" reached
    ``map``, so a question about mapping confidence covered a third of its own
    terms against the section that answers it."""
    for inflected, base in (
        ("mapping", "map"),
        ("running", "run"),
        ("logging", "log"),
        ("dropped", "drop"),
        ("planned", "plan"),
        ("committed", "commit"),
    ):
        assert normalize(inflected) == normalize(base) == base


def test_undoubling_does_not_eat_a_letter_the_base_word_owns():
    """``install`` and ``process`` are doubled in the base word, and ``add`` is
    too short to give a letter away."""
    for word in ("install", "installing", "process", "processing", "fill", "filled"):
        assert not normalize(word).endswith(("nstal", "roces", "fil"))
    assert normalize("add") == "add"


# --- query analysis ---------------------------------------------------------

def test_ask_type_is_read_from_the_question_not_guessed():
    assert classify_ask("what is quarantine") == "definition"
    assert classify_ask("how do I add a connector") == "procedure"
    assert classify_ask("why did my transfer fail") == "diagnosis"
    assert classify_ask("do you support CDC") == "capability"
    assert classify_ask("which sync modes are there") == "enumeration"
    assert classify_ask("full overwrite vs mirror") == "comparison"
    assert classify_ask("how many connectors do you support") == "count"


def test_a_cardinality_question_is_not_the_enumeration_it_looks_like():
    """A count wants a number; the list is a different answer to a different ask.

    "How many connectors do you support" matched ``capability`` on "do you
    support" and was answered with "Open Platform → Connectors" and the four
    transfer-readiness labels — navigation and no number, while every count the
    product publishes sat two sections over. It is listed before the other
    patterns because these phrasings also match ``capability``, ``procedure``
    and ``enumeration``, and the shape they ask for is the narrowest.
    """
    for question in (
        "how many connectors do you support",
        "how many preflight gates are there",
        "how much data can it move",
        "what is the number of sync modes",
        "what is the total of quarantined rows",
        "how big can a decimal be",
    ):
        assert classify_ask(question) == "count", question

    # Asking to be shown the list is still an enumeration, and asking how to do
    # something is still a procedure, however many things it involves.
    assert classify_ask("which sync modes are there") == "enumeration"
    assert classify_ask("how do I add a connector") == "procedure"


# --- a listing question, whatever verb the operator hung it on ---------------
#
# The verb an operator reaches for when asking to be told what there is belongs
# to another ask. "What sync modes do you *support*" was a ``capability`` and
# opened with the definition of a sync mode without naming one; "which engines
# can I *connect* to" was a ``procedure`` and opened with the steps for
# connecting Cursor to the MCP server.

LISTING_QUESTIONS = [
    "what sync modes do you support",
    "which sync modes do you support",
    "which engines can I connect to",
    "what connectors do you support",
    "what file formats do you support",
    "which destinations can I write to",
    "what schema change policies are there",
    "what validation modes are there",
    "what roles are there",
    "which destinations can I write to",
    "what destinations do you support",
    "who can run transfers",
    # Singular, but ``which`` opens it: picking one member out of a set is
    # answered from the same list as the whole set.
    "which preflight gate blocks a lossy type change",
    "which gate blocks a lossy change",
]

#: Questions the listing rule must not claim, and what they actually ask.
NOT_LISTING = [
    # A definition. The old rule accepted a singular head noun, so "what is a
    # sync mode" was an enumeration — a definition answered with a list.
    ("what is a sync mode", "definition"),
    ("what is a preflight gate", "definition"),
    # A recommendation. The noun sits between the interrogative and the verb,
    # which the comparison rule did not allow, so the listing rule took both
    # and answered "which should I use" with all nine modes.
    ("which mode should I pick for a nightly load", "comparison"),
    ("which sync mode is better for a big table", "comparison"),
    ("which connector do you recommend for postgres", "comparison"),
    # Steps, not an inventory.
    ("how do I list my connectors", "procedure"),
    ("how do I connect a postgres database", "procedure"),
    # A number, not the names.
    ("how many roles are there", "count"),
    ("how many sync modes are there", "count"),
    # Something is wrong, which is a different question from what exists.
    ("what gates failed on my transfer", "diagnosis"),
    ("why did my gates fail", "diagnosis"),
    ("what happens to a timestamp without timezone", "consequence"),
    ("what happens if the destination count does not match", "consequence"),
]


@pytest.mark.parametrize("question", LISTING_QUESTIONS)
def test_a_request_to_be_told_what_there_is_is_an_enumeration(question):
    assert classify_ask(question) == "enumeration", question


@pytest.mark.parametrize(("question", "ask"), NOT_LISTING)
def test_the_listing_rule_claims_only_listing_questions(question, ask):
    assert classify_ask(question) == ask, question


def test_a_comparison_frame_carries_no_subject_signal():
    """"Difference" and "between" are the frame, not the subject.

    Left in, they retrieved the row-ledger section for "what is the difference
    between jobs and pipelines", because it says "the difference is reported as
    unaccounted rows" — a lexical match on the one word in the question that
    means nothing.
    """
    for word in ("difference", "between", "compare", "comparison", "versus"):
        assert normalize(word) in GENERIC_QUESTION_WORDS

    terms = analyze_query("what is the difference between jobs and pipelines").terms
    assert set(terms) == {normalize("jobs"), normalize("pipeline")}


def test_expansion_is_tiered_so_a_phrase_is_trusted_and_a_word_is_not():
    """An exact multi-word phrase is the operator's own words in the corpus's
    spelling; a single-term synonym is a recall guess and must weigh less."""
    phrase, loose = expand_terms_tiered(
        "what is change data capture", ("change", "data", "capture")
    )
    assert "cdc" in phrase
    assert "cdc" not in loose


def test_inflected_expansion_keys_actually_fire():
    """The expansion table is keyed on surface forms; terms arrive stemmed.

    Around twenty-five entries — ``gates``, ``permissions``, ``rejected`` — could
    never match anything until the lookup normalized its own keys.
    """
    analysis = analyze_query("who has permissions to run transfers")
    assert "role" in analysis.expansions


def test_generic_words_still_trigger_expansion_but_never_anchor():
    """"Bad" is useless as an anchor and useful as a pointer at "quarantine"."""
    analysis = analyze_query("what happens to bad rows")
    assert "bad" not in analysis.terms
    assert normalize("quarantine") in analysis.expansions


def test_query_side_shingles_recover_the_corpus_own_labels():
    """The corpus writes ``reverse_etl``; an operator writes "reverse ETL".

    Splitting identifiers in the index instead was measured worse: it put
    ``write`` and ``etl`` into every sync-mode passage and broke "how is this
    different from writing ETL scripts".
    """
    # Both sides are stemmed, so the assertion is that they agree rather than
    # that they are spelled any particular way.
    assert identifier_shingles((normalize("reverse"), normalize("etl"))) == [
        normalize("reverse_etl")
    ]
    assert (
        normalize("reverse_etl")
        in analyze_query("what is reverse ETL").phrase_expansions
    )
    # A term that is already an identifier is not re-joined.
    assert identifier_shingles((normalize("reverse_etl"), "mode")) == []


# --- retrieval passages -----------------------------------------------------

def test_long_procedures_are_indexed_by_step():
    """BM25 divides term weight by document length, so the corpus's longest and
    most instructive passages were the hardest to reach."""
    sections = load_product_doc_chunks()
    passages = retrieval_passages()
    assert len(passages) > len(sections)

    long_sections = [c for c in sections if len(c.text) > MAX_SECTION_CHARS]
    assert long_sections, "fixture assumes the corpus still has long procedures"

    steps = [
        c
        for c in passages
        if c.section_title.startswith("Procedure: create a pipeline →")
    ]
    assert len(steps) >= 4
    assert all(len(c.text) <= len(c.text) for c in steps)
    assert len({c.id for c in passages}) == len(passages), "passage ids must be unique"


def test_the_section_list_stays_unsplit():
    """``evidence_policy`` reads these headings to decide what the product
    documents. Step titles are not subjects — "scroll", "footer", "tick" are
    not things an operator can ask about."""
    assert all("→" not in c.section_title for c in load_product_doc_chunks())
    assert not is_subject_term("scroll")
    assert not is_subject_term("footer")


def test_a_section_without_step_structure_is_left_whole():
    assert _split_procedure("One sentence. Another sentence.") == []
    assert _split_procedure("Do a thing\nWhere: Somewhere") == []


# --- the evidence window ----------------------------------------------------

def _hit(section: str, matched: tuple[str, ...]) -> ProductDocHit:
    chunk = next(iter(retrieval_passages()))
    return ProductDocHit(chunk=chunk, score=1.0, grounding=1.0, matched_terms=matched)


def test_the_evidence_window_covers_the_question_not_its_best_match():
    """Ranking scores a passage alone; answerability is judged on what the
    window covers jointly. Taking the top k by rank optimizes the wrong thing."""
    ranked = [
        (10.0, _hit("a", ("postgresql", "snowflake"))),
        (9.8, _hit("b", ("postgresql", "snowflake"))),
        (9.6, _hit("c", ("postgresql", "snowflake"))),
        (4.0, _hit("d", ("decimal", "precision"))),
    ]
    chosen = _select_covering(ranked, 2, ["postgresql", "snowflake", "decimal", "precision"])
    covered: set[str] = set()
    for hit in chosen:
        covered |= set(hit.matched_terms)
    assert covered == {"postgresql", "snowflake", "decimal", "precision"}


def test_a_two_part_question_retrieves_both_parts():
    answer = retrieve_product_answer(
        "how do I move data from Postgres to Snowflake without losing decimal precision"
    )
    terms = {t for hit in answer.hits for t in hit.matched_terms}
    assert {"postgresql", "snowflake"} & terms
    assert {"decimal", "precision"} & terms


# --- heading and intent priors ----------------------------------------------

def _role_matrix_chunk():
    """One chunk of the generated role×verb matrix."""
    for chunk in retrieval_passages():
        if (chunk.doc_title or "").strip().lower() == _ROLE_MATRIX_DOC:
            return chunk
    raise AssertionError(f"no passage from {_ROLE_MATRIX_DOC!r} in the corpus")


def _lead(question: str) -> str:
    return " ".join(
        (compose_product_answer(retrieve_product_answer(question)) or "").split()
    )


def test_the_role_matrix_is_demoted_for_a_question_that_is_not_about_roles():
    """It lists every verb in the product, so it overlaps almost any question.

    Measured: it led "show me the audit log", "start the transfer" and "how do
    I cancel a running transfer" — the last of those with the sentence saying a
    *viewer* cannot cancel, which is not merely off topic but the opposite of
    what the operator asked for.
    """
    bonus = _section_intent_bonus(_role_matrix_chunk(), analyze_query("start the transfer"))
    assert bonus < 0


def test_the_role_matrix_is_promoted_for_a_question_that_is_about_roles():
    bonus = _section_intent_bonus(_role_matrix_chunk(), analyze_query("what can a viewer do"))
    assert bonus > 0


def test_who_alone_marks_a_question_as_being_about_permission():
    """"can I limit who sees a connector" is the matrix's question asked
    without the word "can" next to the word "who" — requiring a modal there
    cost this case, measured on the answer audit."""
    bonus = _section_intent_bonus(
        _role_matrix_chunk(), analyze_query("can I limit who sees a connector")
    )
    assert bonus > 0


def test_the_bare_word_operator_is_not_a_permission_question():
    """This documentation calls its reader an operator on every page, so the
    bare word cannot be the signal — otherwise the matrix wins everything
    again by the same route it won before."""
    bonus = _section_intent_bonus(
        _role_matrix_chunk(),
        analyze_query("what does the operator see when a gate blocks"),
    )
    assert bonus < 0


def test_a_different_section_that_merely_says_role_is_not_demoted():
    """``Semantic roles`` is about detected column roles, not authorization.
    The rule keys on the article, not on the presence of the word."""
    semantic = next(
        (c for c in retrieval_passages() if (c.section_title or "") == "Semantic roles"),
        None,
    )
    assert semantic is not None
    assert (semantic.doc_title or "").strip().lower() != _ROLE_MATRIX_DOC


def test_an_operator_asking_where_the_audit_log_is_is_told_where():
    answer = _lead("show me the audit log")
    assert "audit log" in answer.lower()
    # Not the role list, which was the measured answer before.
    assert "rotate their own password" not in answer


def test_a_permission_question_is_still_answered_from_the_role_matrix():
    answer = _lead("what can a viewer do")
    assert "viewer" in answer.lower()


# --- evidence policy --------------------------------------------------------

def test_documentation_that_covers_the_subject_answers():
    analysis = analyze_query("what is quarantine")
    verdict = assess_evidence(
        analysis,
        ["Quarantine holds rows the destination refused. No row disappears silently."],
    )
    assert verdict.outcome == "answer"
    assert verdict.answerable


def test_an_uncovered_subject_makes_the_answer_partial_not_a_refusal():
    """An operator can act on "here is what is documented, this part is not";
    they can do nothing with a refusal."""
    analysis = analyze_query("how do I schedule a pipeline with a data contract")
    verdict = assess_evidence(
        analysis,
        ["Pipelines run on a cadence. Open Operations then Pipelines to schedule one."],
    )
    assert verdict.outcome == "partial"
    assert "contract" in verdict.uncovered_subjects


def test_an_ordinary_modifier_does_not_make_an_answer_partial():
    """Keying partiality on any uncovered term made almost every answer partial
    and filled the product with caveats about "low", "night" and "2am"."""
    analysis = analyze_query("why is my mapping confidence low")
    verdict = assess_evidence(
        analysis,
        [
            "Each Map edge shows a confidence percentage. Review anything below "
            "your threshold and click Accept risk for intentional lossy casts."
        ],
    )
    assert verdict.outcome == "answer"
    assert not verdict.uncovered_subjects


def test_an_object_the_documentation_does_not_name_is_refused():
    """On-vocabulary, no documentation. This is the honesty contract: the
    question needs a live read of the operator's workspace, not an article."""
    assert names_own_object("subscriber_id")
    assert not names_own_object("rows")
    analysis = analyze_query("what semantic patterns match subscriber_id column naming")
    verdict = assess_evidence(
        analysis,
        ["Semantic column mapping pairs source fields to destination names."],
    )
    assert verdict.outcome == "refuse"
    assert not verdict.answerable


def test_an_identifier_in_the_corpus_covers_its_parts():
    """A passage that says ``reverse_etl`` has covered "reverse" and "etl"."""
    analysis = analyze_query("what is reverse etl")
    verdict = assess_evidence(
        analysis,
        ["Sync mode reverse_etl pushes warehouse rows back into an operational system."],
    )
    assert verdict.answerable


# --- answer composition -----------------------------------------------------

_GATES = (
    "Core gates (before write)",
    "Preflight gates explained → Core gates (before write)",
    "#/help/gates",
    "Core gates (before write)\n"
    "G1 Source readable — source connects and rows can be read.\n"
    "G2 Destination writable — destination accepts a write.\n"
    "G3 Schema contract — source and target schemas are compatible.\n"
    "G4 Type fidelity — every cast is lossless or explicitly accepted.\n"
    "G5 Sample dry-run — sample rows pass the same transforms writers use.",
)


def test_bare_gate_lines_survive_sentence_splitting():
    """The corpus writes G1–G9 as lines with no terminator, and those lines are
    the answer to every gate question."""
    sentences = split_sentences(_GATES[3], _GATES[0])
    assert any(s.startswith("G1 ") for s in sentences)
    assert any(s.startswith("G5 ") for s in sentences)


def test_a_list_is_answered_whole_or_not_at_all():
    """Maximal-marginal-relevance suppresses near-duplicates, and the items of
    one list look like near-duplicates to it — it returned G1 and G4 and called
    that the gate list."""
    analysis = analyze_query("what are the preflight gates")
    answer = compose_answer(analysis, [_GATES])
    for gate in ("G1 ", "G2 ", "G3 ", "G4 ", "G5 "):
        assert gate in answer, f"{gate} missing from:\n{answer}"


def test_a_heading_that_restates_the_question_vouches_for_its_prose():
    """"Do you have webhooks" is answered by the sentence naming the events; the
    word ``webhook`` is only in the heading above it, so scored on its own words
    that sentence was worth nothing and an unrelated section answered."""
    section = (
        "Webhooks",
        "API reference → Webhooks",
        "#/help/api",
        "Webhooks\nSubscribe to job.completed, job.failed, and "
        "pipeline.quarantine_threshold events.",
    )
    analysis = analyze_query("do you have webhooks")
    candidates = build_candidates(analysis, [section])
    assert candidates, "a heading-matched section must be able to contribute"
    assert "job.completed" in candidates[0].text


def test_a_heading_that_half_matches_does_not_vouch():
    """One word of two is how "which engines can I connect to" reached
    "Procedure: connect Cursor" and answered about MCP."""
    section = (
        "Procedure: connect Cursor",
        "MCP Server for agents → Procedure: connect Cursor",
        "#/help/mcp",
        "Procedure: connect Cursor\nExpand the Cursor integration card.",
    )
    analysis = analyze_query("which engines can I connect to")
    assert not build_candidates(analysis, [section])


def test_a_long_sentence_does_not_win_on_breadth_alone():
    """A raw match count let a 400-character sentence listing every permission
    outrank the one sentence that answers."""
    long_text = " ".join(f"word{i}" for i in range(LENGTH_PIVOT * 4))
    section = (
        "Roles",
        "Roles & permissions → What each role can do",
        "#/help/roles",
        f"An editor can build transfers and run them.\n"
        f"A role grants a transfer to a role and a transfer to a role {long_text}.",
    )
    analysis = analyze_query("what can an editor do")
    chosen = build_candidates(analysis, [section])
    assert chosen[0].text.startswith("An editor can build transfers")


def test_the_answer_leads_with_the_answer_then_reads_in_order():
    lead = Candidate(
        text="Quarantine holds rows the destination refused.",
        section_title="What is quarantine",
        citation="Quarantine → What is quarantine",
        href="#/help/q",
        order=5,
        terms=frozenset({"quarantine", "row"}),
        score=9.0,
    )
    first = Candidate(
        text="Open Operations then Jobs then Quarantine on the run.",
        section_title="Procedure: inspect quarantine",
        citation="Quarantine → Procedure",
        href="#/help/q",
        order=0,
        terms=frozenset({"operation", "job"}),
        # Comfortably over ``RELEVANCE_FLOOR`` of the lead. This test is about
        # the order the chosen sentences are read in, not about which ones
        # qualify; the floor has its own coverage in
        # ``test_rag_sentence_selection_signals``.
        score=4.5,
    )
    chosen = select_sentences([lead, first])
    assert chosen[0] is lead
    assert chosen[1] is first


def test_every_composed_answer_cites_where_it_came_from():
    analysis = analyze_query("what are the preflight gates")
    answer = compose_answer(analysis, [_GATES])
    assert answer.rstrip().endswith("(Help)")
    assert "Preflight gates explained" in answer



# --- a count is published under a heading that asks for one ------------------
#
# ``count`` had no branch of its own, so it fell into the definitional family
# and paid "Core gates (before write)" the +3.0 its nine named cards earn for a
# definition. G1 reads "Source readable", which was enough for "how many
# sources can you connect to" to lead with a gate description while the passage
# that states "30 sources and 30 destinations" was not even in the evidence
# window.


def _counting_chunk():
    """The generated passage whose heading asks how many there are."""
    for chunk in retrieval_passages():
        if (chunk.section_title or "").strip().lower().startswith("how many"):
            return chunk
    raise AssertionError("no counting passage in the corpus")


def _gates_chunk():
    for chunk in retrieval_passages():
        if "core gates" in (chunk.section_title or "").strip().lower():
            return chunk
    raise AssertionError("no core-gates passage in the corpus")


def test_a_count_question_prefers_the_heading_that_publishes_a_number():
    question = analyze_query("how many connectors do you support")
    assert question.ask == "count", question.ask
    assert _section_intent_bonus(_counting_chunk(), question) == pytest.approx(
        _COUNTING_TITLE_BONUS
    )


def test_a_count_question_no_longer_pays_the_gate_list_a_definition_bonus():
    """The measured defect, stated as the prior that caused it."""
    question = analyze_query("how many sources can you connect to")
    assert question.ask == "count", question.ask
    assert _section_intent_bonus(_gates_chunk(), question) <= 0.0


def test_the_gate_list_keeps_its_bonus_for_the_asks_that_earned_it():
    """Enumeration and definition are what the nine named cards answer."""
    gates = _gates_chunk()
    for question in ("what are the preflight gates", "what is a preflight gate"):
        analysis = analyze_query(question)
        assert analysis.ask in {"enumeration", "definition"}, analysis.ask
        assert _section_intent_bonus(gates, analysis) > 0.0, question


def test_the_gate_list_pays_the_off_ask_cost_for_a_destination_question():
    """G2 says "Destination write access"; that is not a list of destinations."""
    analysis = analyze_query("which destinations can I write to")
    assert analysis.ask == "enumeration"
    assert _section_intent_bonus(_gates_chunk(), analysis) == pytest.approx(
        -_CORE_GATES_OFF_ASK
    )


def test_a_counting_heading_pays_when_the_question_did_not_ask_for_a_number():
    """The inventory-count heading names every noun, so it overlaps any of them."""
    analysis = analyze_query("which engines can I connect to")
    assert analysis.ask == "enumeration"
    chunk = None
    for candidate in retrieval_passages():
        title = (candidate.section_title or "").strip().lower()
        if title.startswith("how many") and "engines" in title:
            chunk = candidate
            break
    assert chunk is not None
    assert _section_intent_bonus(chunk, analysis) == pytest.approx(
        -_COUNTING_TITLE_OFF_ASK
    )


@pytest.mark.parametrize(
    "question",
    [
        "how many sources can you connect to",
        "how many file formats can you read",
        "how many connectors do you support",
        "how many connectors are live",
        "how many sync modes are there",
        "how many roles are there",
        "how many destinations do you support",
    ],
)
def test_a_cardinality_question_leads_with_a_number(question):
    """On the served evidence window, which is four passages, not the default.

    Reaching the passage is not enough for a count: the number has to be in the
    sentence the operator reads first.
    """
    body = " ".join(
        (compose_product_answer(retrieve_product_answer(question, limit=4)) or "").split()
    )
    lead = body.split(". ")[0]
    assert re.search(r"\b\d[\d,]*\b", lead), lead[:220]

# --- what counts as answering "how many" -------------------------------------


def test_a_spelled_number_used_adverbially_is_not_a_count():
    """"In one transaction" is a manner, not a quantity.

    Measured: asked "how many destinations do you support", the count shape
    bonus went to "commit the applied rows and the watermark that records them
    in one transaction", which then led the answer.
    """
    from src.ai.rag.answer_composer import _CARDINAL

    for manner in (
        "committed in one transaction",
        "applied as one atomic unit",
        "one measure per distinct value of a column",
        "kept in one place",
    ):
        assert not _CARDINAL.search(manner), manner


def test_a_spelled_number_quantifying_something_still_counts():
    """The bonus has to survive for the sentences that do answer a count."""
    from src.ai.rag.answer_composer import _CARDINAL

    for quantity in (
        "There are two ways to capture changes",
        "Preflight runs nine core gates",
        "five sync modes require a cursor field",
        "46 connectors are live and transfer-ready",
        "there are 30 sources and 30 destinations",
    ):
        assert _CARDINAL.search(quantity), quantity
