"""The type space had to become retrievable, and it had to stay generated.

Type fidelity is this product's core claim, and it was the subject Pilot could
say least about: 12 of 25 ordinary questions about nulls, timezones, arrays,
booleans, binary, floats and decimals were refused outright, because the type
space is documented only in the modules that enforce it.

These contracts hold the two halves of the fix. The passages must be *read out
of* ``type_system`` / ``timezone_policy`` / ``schema_fidelity`` rather than
restated beside them, so the answer cannot drift from the engine. And the
subject model must recognise a logical type or a fidelity aspect as something
the product documents, since no heading word does.
"""

from __future__ import annotations

import pytest

from src.ai.rag.evidence_policy import is_subject_term, product_subjects
from src.ai.rag.product_docs import (
    load_product_doc_chunks,
    names_product_subject,
    retrieval_passages,
    retrieve_product_answer,
)
from src.ai.rag.product_facts import generated_sections
from src.ai.rag.query_analysis import normalize


def _section(title: str):
    for section in generated_sections():
        if section.section_title == title:
            return section
    raise AssertionError(
        f"{title!r} not generated; have {[s.section_title for s in generated_sections()]}"
    )


# --------------------------------------------------------------------------
# Generated, not restated
# --------------------------------------------------------------------------

def test_type_carriers_come_from_the_ddl_table() -> None:
    """The carrier grid is read from the table the DDL builder renders from."""
    from services.type_system import DDL_TYPES

    text = _section("Destination type for each logical type").text
    for logical in ("boolean", "binary", "json", "decimal"):
        for engine in ("postgresql", "mysql", "snowflake", "bigquery"):
            ddl = (DDL_TYPES.get(engine) or {}).get(logical)
            assert ddl, f"{engine}/{logical} missing from DDL_TYPES"
            assert ddl in text, f"{engine} {logical} carrier {ddl!r} not in the passage"


def test_the_mysql_timestamp_window_is_the_one_the_policy_enforces() -> None:
    from services.timezone_policy import MYSQL_TIMESTAMP_RANGE_TEXT

    assert MYSQL_TIMESTAMP_RANGE_TEXT in _section(
        "Timestamp range and instant carriers"
    ).text


def test_every_certified_aspect_is_named() -> None:
    """A reader has to be able to check the list against the list."""
    from services.schema_fidelity import REQUIRED_ASPECTS

    text = _section("Every aspect a migration certificate answers for").text
    for aspect in REQUIRED_ASPECTS:
        assert aspect.replace("_", " ") in text, f"{aspect} unaccounted for"


def test_multiword_aspects_carry_the_identifier_too() -> None:
    """A question's exact phrase shingles to the identifier, and must match it.

    "Column order" reaches ``column_order``, which only a passage spelling the
    aspect the way the certificate does can answer; rendered as prose alone,
    the question was answered by a CSV tutorial that says "in order".
    """
    text = _section("Every aspect a migration certificate answers for").text
    assert "column order (column_order)" in text
    assert "not null (not_null)" in text


def test_the_certificate_states_what_each_status_means() -> None:
    """Naming an aspect is not answering for it.

    The roll lists 26 aspects and the prose classifies about half, so
    "what happens to partitioning" had nothing to answer from until the
    lead-in named the four statuses in the same sentence as the roll.
    """
    text = _section("Every aspect a migration certificate answers for").text
    for status in ("carried", "unsupported", "skipped", "unknown"):
        assert status in text


def test_the_roll_and_the_prose_are_separate_sections() -> None:
    """A 26-name enumeration inside the prose passage cost that passage its rank.

    The roll matched every aspect question, crowded out the sentence that
    answered, and lengthened the passage enough that BM25 dropped it behind a
    CSV tutorial.
    """
    titles = {s.section_title for s in generated_sections()}
    assert "What a create-new carries" in titles
    assert "Every aspect a migration certificate answers for" in titles
    assert "primary_key" not in _section("What a create-new carries").text


def test_the_concept_and_the_range_are_separate_sections() -> None:
    """One section per question an operator asks.

    Held together, a passage about the 2038 bound was retrieved as the answer
    to "how do you handle timezones".
    """
    titles = {s.section_title for s in generated_sections()}
    assert "Timestamps and time zones" in titles
    assert "Timestamp range and instant carriers" in titles


# --------------------------------------------------------------------------
# The subject model recognises the type space
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "term", ["boolean", "array", "binary", "decimal", "json", "uuid", "datetime"]
)
def test_logical_types_are_subjects(term: str) -> None:
    """No heading word names a type, so heading-derived subjects missed them all."""
    assert is_subject_term(normalize(term))


@pytest.mark.parametrize("term", ["null", "default", "collation", "encoding", "charset"])
def test_fidelity_aspects_are_subjects(term: str) -> None:
    assert is_subject_term(normalize(term))


def test_off_subject_words_are_still_not_subjects() -> None:
    """Widening the subject model must not make everything answerable."""
    for term in ("rice", "capital", "poem", "weather"):
        assert not is_subject_term(normalize(term))
    assert not names_product_subject("how do I cook rice")
    assert not names_product_subject("what is the capital of France")


def test_timezone_written_as_one_word_reaches_the_policy() -> None:
    """The heading says "time" and "zones"; operators type "timezone"."""
    assert names_product_subject("how do you handle timezones")


# --------------------------------------------------------------------------
# End to end: the questions that were refused
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "question,expected",
    [
        ("how do you handle booleans across databases", "BOOLEAN"),
        ("how do you handle binary blobs", "BYTEA"),
        ("what happens to dates before 1970", "2038"),
        ("how do you handle timezones", "offset label"),
        ("what happens to null values", "NOT NULL"),
        ("what character encoding do you use", "Unicode"),
        ("do you preserve column order", "column_order"),
        ("what happens to partitioning", "partitioning"),
    ],
)
def test_type_questions_are_answered_from_the_generated_passages(
    question: str, expected: str
) -> None:
    answer = retrieve_product_answer(question)
    assert answer.verdict.outcome in {"answer", "partial"}, answer.verdict.reason
    joined = " ".join(hit.chunk.text for hit in answer.hits)
    assert expected in joined, f"{expected!r} not in retrieved evidence for {question!r}"


# --------------------------------------------------------------------------
# Passage granularity
# --------------------------------------------------------------------------

def test_long_reference_sections_are_split_into_passages() -> None:
    """A reference grid paid the same length penalty a long procedure did.

    The splitter only knew how to divide a procedure by its ``Where:``
    breadcrumbs, so "do you preserve column order" ranked a CSV tutorial that
    says "in order" above the section that states the rule.
    """
    from src.ai.rag.product_docs import MAX_SECTION_CHARS

    assert len(retrieval_passages()) > len(load_product_doc_chunks())
    long_passages = [
        p for p in retrieval_passages() if len(p.text) > MAX_SECTION_CHARS * 1.5
    ]
    assert not long_passages, [p.section_title for p in long_passages]


def test_splitting_never_cuts_a_line_in_half() -> None:
    """Lines are the corpus's unit; a half sentence is not a passage."""
    for chunk in load_product_doc_chunks():
        lines = {line.strip() for line in chunk.text.splitlines() if line.strip()}
        if not lines:
            continue
        from src.ai.rag.product_docs import _as_step_chunks

        for passage in _as_step_chunks(chunk):
            for line in passage.text.splitlines():
                if line.strip():
                    assert line.strip() in lines


def test_headings_still_describe_the_corpus() -> None:
    """``product_subjects`` reads authored sections, which stay unsplit."""
    assert "quarantine" in product_subjects()
    assert "connector" in product_subjects()
