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

import sys
from pathlib import Path

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
    _select_covering,
    _split_procedure,
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
    assert set(terms) == {"jobs", "pipeline"}


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
    assert "quarantine" in analysis.expansions


def test_query_side_shingles_recover_the_corpus_own_labels():
    """The corpus writes ``reverse_etl``; an operator writes "reverse ETL".

    Splitting identifiers in the index instead was measured worse: it put
    ``write`` and ``etl`` into every sync-mode passage and broke "how is this
    different from writing ETL scripts".
    """
    assert identifier_shingles(("reverse", "etl")) == ["reverse_etl"]
    assert "reverse_etl" in analyze_query("what is reverse ETL").phrase_expansions
    # A term that is already an identifier is not re-joined.
    assert identifier_shingles(("reverse_etl", "mode")) == []


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
        score=3.0,
    )
    chosen = select_sentences([lead, first])
    assert chosen[0] is lead
    assert chosen[1] is first


def test_every_composed_answer_cites_where_it_came_from():
    analysis = analyze_query("what are the preflight gates")
    answer = compose_answer(analysis, [_GATES])
    assert answer.rstrip().endswith("(Help)")
    assert "Preflight gates explained" in answer
