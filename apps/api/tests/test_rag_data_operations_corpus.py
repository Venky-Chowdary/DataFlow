"""The data-engineering questions Pilot had no passage for, and the honesty bar.

Type fidelity became retrievable first; delivery semantics, resume points,
throughput, capture mode, delete polarity and lineage grain did not, and they
are the questions a data engineer evaluating a transfer product asks before any
of them. Measured on the shipped corpus, "what is the delivery guarantee",
"where does a re-run start from", "do you read the WAL or poll", "how do you
handle soft deletes" and "can I get row level lineage" were all refused
outright — not answered badly, refused — because the rules live only in the
modules that enforce them and nothing in the documentation had a heading about
them.

These contracts hold two things. Each passage must be **read out of** its
enforcing module, so the answer cannot drift from the engine the way a
hand-written paragraph does. And each passage must carry the *honest* form of
the claim: at-least-once by default, idempotent rather than exactly-once,
lineage at run grain rather than per row, a tile count that is not a driver
count. Those are the sentences a competitor's marketing page gets wrong, and
generating them from the constants is how they stay right here.
"""

from __future__ import annotations

import pytest

from src.ai.rag.product_docs import retrieve_product_answer
from src.ai.rag.product_facts import generated_sections


def _section(title: str):
    for section in generated_sections():
        if section.section_title == title:
            return section
    raise AssertionError(
        f"{title!r} not generated; have {[s.section_title for s in generated_sections()]}"
    )


# --------------------------------------------------------------------------
# Generated from the enforcing module, not restated beside it
# --------------------------------------------------------------------------

def test_the_delivery_guarantee_is_the_constant_the_engine_enforces() -> None:
    """The one number a CDC evaluation turns on is not retyped here."""
    from services.cdc_effectively_once import DELIVERY_DEFAULT

    text = _section("Delivery guarantee and duplicate changes").text
    assert DELIVERY_DEFAULT in text


def test_exactly_once_is_not_claimed_while_the_module_does_not_claim_it() -> None:
    """The passage says what the flag says, so the two cannot diverge.

    A hand-written paragraph is how "exactly once" ends up in documentation for
    a platform that delivers at-least-once. If the engine ever earns the
    stronger claim the flag moves and this assertion moves with it.
    """
    from services.cdc_effectively_once import EXACTLY_ONCE_CLAIMED

    text = _section("Delivery guarantee and duplicate changes").text
    if EXACTLY_ONCE_CLAIMED:
        pytest.skip("the platform now claims exactly-once; the passage should too")
    assert "Exactly-once is not claimed platform-wide" in text


def test_the_guard_column_is_named_so_the_claim_is_checkable() -> None:
    """"Idempotent" is a word; ``_df_lsn`` is a thing an operator can go look at."""
    text = _section("Delivery guarantee and duplicate changes").text
    assert "_df_lsn" in text
    assert "not exactly-once delivery" in text


def test_an_append_only_destination_is_told_it_cannot_be_guarded() -> None:
    """No row to overwrite means no position guard, and silence there is the bug.

    A destination with nothing to key on cannot make a redelivery harmless, and
    saying so is the difference between at-least-once and quiet duplication.
    """
    from services.cdc_effectively_once import APPEND_ONLY_SINKS_EFFECTIVELY_ONCE

    if APPEND_ONLY_SINKS_EFFECTIVELY_ONCE:
        pytest.skip("append-only sinks now carry a guard")
    text = _section("Delivery guarantee and duplicate changes").text
    assert "appends a duplicate row" in text


def test_the_delivery_passage_does_not_spell_out_change_data_capture() -> None:
    """Measured: spelling the phrase out cost a different question its answer.

    "What is change data capture" is a definitional question, and a passage
    containing the exact phrase three times became its strongest lexical match
    — so the audit answered a request for the definition with the delivery
    guarantee, and dropped from 151 on-target cases to 150. The passage is
    about a CDC route and says ``CDC``.
    """
    text = _section("Delivery guarantee and duplicate changes").text
    assert "change data capture" not in text.lower()
    assert "CDC" in text


def test_every_tombstone_name_the_engine_accepts_is_listed() -> None:
    """Delete polarity is an exact set, and a reader has to be able to check it."""
    from services.tombstone import TOMBSTONE_COLUMNS

    text = _section("Deletes, tombstones and soft deletes").text
    for column in TOMBSTONE_COLUMNS:
        assert f"`{column}`" in text, f"{column} not listed as a tombstone"


def test_the_lookalikes_are_listed_as_excluded_not_omitted() -> None:
    """``deleted_by`` is the trap; leaving it unmentioned is how an operator falls in.

    A column recording *who* deleted a row is not a column saying the row is
    deleted, and an operator auditing their own schema needs the exclusion
    stated rather than inferred from an absence.
    """
    from services.tombstone import TOMBSTONE_LOOKALIKES

    text = _section("Deletes, tombstones and soft deletes").text
    assert "deliberately excluded" in text
    for column in TOMBSTONE_LOOKALIKES:
        assert f"`{column}`" in text, f"{column} not named as an excluded lookalike"


def test_a_liveness_flag_is_documented_as_left_alone() -> None:
    """Reading ``is_active`` as a deletion inverts the table — the worst failure here."""
    text = _section("Deletes, tombstones and soft deletes").text
    assert "`is_active`" in text
    assert "inversion of the table" in text


def test_the_soft_delete_mirror_column_is_the_engine_constant() -> None:
    from services.mirror_engine import SOFT_DELETE_COLUMN

    text = _section("Deletes, tombstones and soft deletes").text
    assert f"`{SOFT_DELETE_COLUMN}`" in text


def test_lineage_says_what_grain_it_is_and_is_not() -> None:
    """The refusal was replaced with an honest two-part answer, not with a claim.

    Row-level lineage is the thing every catalog vendor implies and few have. It
    is not what this product emits, and the passage has to say the grain it does
    emit *and* name the artifacts that answer the per-row question instead.
    """
    text = " ".join(_section("Lineage grain and what is traceable").text.split())
    assert "not a per-row graph" in text
    assert "quarantined row" in text
    assert "is not something this product claims" in text


def test_the_driver_list_agrees_with_the_count_the_product_reports() -> None:
    """One source of truth for "how many connectors are live".

    Deriving the names from catalog tiles listed 31 while ``catalog_summary``
    reported 46 ``unique_drivers`` — the same overclaim-shaped inconsistency in
    reverse, and a passage that disagrees with the dashboard beside it.
    """
    from services.catalog_service import catalog_summary

    summary = catalog_summary()
    text = _section("Which engines you can connect").text
    assert f"{summary['unique_drivers']} of them" in text
    for driver in summary["unique_driver_types"]:
        assert driver in text, f"{driver} missing from the transfer-ready list"


def test_the_tile_count_is_stated_as_tiles_and_the_planned_share_with_it() -> None:
    """A catalog count is not a capability claim unless the planned share is beside it."""
    from services.catalog_service import catalog_summary

    summary = catalog_summary()
    text = _section("How many connectors are transfer-ready").text
    assert f"{summary['catalog_tile_total']} connector tiles in total" in text
    assert f"{summary['planned']} are planned" in text
    assert "the overclaim this product refuses to make" in text


def test_the_counts_are_their_own_section_under_their_own_question() -> None:
    """"How many connectors" carries one term, and it is not ``engines``.

    Held inside "Which engines you can connect" the counts were unreachable:
    the question retrieved the Connectors page tour instead and answered a
    cardinality question with navigation. Expanding ``connector`` onto
    ``engine`` was measured first and was worse — it put the engine vocabulary
    into every connector question and cost the audit a case. One section per
    question an operator asks is the rule this corpus already follows.
    """
    titles = {section.section_title for section in generated_sections()}
    assert "Which engines you can connect" in titles
    assert "How many connectors are transfer-ready" in titles


def test_the_count_section_stays_short_enough_to_rank_for_its_own_question() -> None:
    """BM25 length normalization decides whether the answer is retrievable at all.

    The first draft stated the counts, the honesty caveat, the planned share,
    all 46 driver names and the per-side split. It ranked fourth of five for
    "how many connectors do you support" — below three sections that say
    nothing about counts — and the Pilot retrieves four, so it was cut off.
    """
    text = _section("How many connectors are transfer-ready").text
    assert len(text.split()) < 120, f"{len(text.split())} words is back over the bar"


def test_the_capture_downgrade_is_documented_where_the_module_allows_it() -> None:
    """One cause degrades and the rest fail closed; the passage follows the module.

    Substituting polling for log capture loses deletes. That is allowed only
    when the server was never configured to emit a log, and the passage states
    the refusals from the same flags the engine gates on.
    """
    from services.cdc_capability import (
        CAUSE_PRIVILEGE,
        CAUSE_SERVER_NOT_CONFIGURED,
        CAUSE_SLOT_QUOTA,
        LogCaptureRefusal,
    )

    def fails_closed(cause: str) -> bool:
        return LogCaptureRefusal(cause=cause, detail="", remedy="").fail_closed

    text = " ".join(
        _section("Log capture, polling, and the snapshot handoff").text.split()
    )
    assert "cannot see a DELETE" in text
    if not fails_closed(CAUSE_SERVER_NOT_CONFIGURED):
        assert "never configured to emit a change log" in text
    if fails_closed(CAUSE_SLOT_QUOTA) and fails_closed(CAUSE_PRIVILEGE):
        assert "fails closed with the remedy" in text


def test_the_snapshot_handoff_is_described_as_position_based() -> None:
    """"Is the initial load consistent with the stream" has one correct answer."""
    text = _section("Log capture, polling, and the snapshot handoff").text
    assert "known log position" in text


def test_resume_distinguishes_the_two_markers_it_keeps() -> None:
    """A snapshot's last key and a stream's watermark are different things.

    Conflating them is how a resume rewinds a stream to a table position. The
    passage has to name both and say the first is cleared at the handoff.
    """
    text = _section("Resume points, checkpoints and watermarks").text
    assert "last primary key" in text
    assert "high water mark" in text
    assert "cleared when the stream takes over" in text


def test_resume_grain_is_stated_as_the_chunk_not_the_row() -> None:
    """Per-chunk durability is *why* the write has to be idempotent — say both."""
    text = " ".join(_section("Resume points, checkpoints and watermarks").text.split())
    assert "durable per committed chunk, not per row" in text
    assert "idempotent" in text


def test_throughput_is_described_as_bounded_concurrency() -> None:
    from services.parallel_chunks import ChunkDispatcher  # noqa: F401

    text = _section("Chunking, concurrency and throttled throughput").text
    assert "never in one statement" in text
    assert "ascending order" in text


def test_the_throughput_heading_does_not_say_large_table() -> None:
    """Measured: a heading about "a large table" hijacked a question about decimals.

    Answerability and ranking both read heading words, so a heading is a claim
    about which questions the section is for. "How do you handle very large
    decimals" is a type question, and it opened on this passage until the
    heading named the mechanism instead of the adjective.
    """
    title = _section("Chunking, concurrency and throttled throughput").section_title
    assert "large" not in title.lower()


# --------------------------------------------------------------------------
# The questions that were refused are answered end to end
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("what is the delivery guarantee", "at-least-once"),
        ("do you do exactly once delivery", "at-least-once"),
        ("what happens if the same change is delivered twice", "_df_lsn"),
        ("where does a re-run start from", "chunk"),
        ("how is the high water mark stored", "water mark"),
        ("do you read the WAL or poll for changes", "write-ahead log"),
        ("is the initial load consistent with the stream", "log position"),
        ("how do you handle soft deletes", "tombstone"),
        ("what counts as a deleted row", "tombstone"),
        ("can I get row level lineage", "per-row graph"),
        ("how do you throttle a large table", "chunks"),
    ],
)
def test_a_data_engineering_question_is_answered_rather_than_refused(
    question: str, expected: str
) -> None:
    """Named-fixture floor. Every one of these was refused before the passages existed."""
    answer = retrieve_product_answer(question)
    assert answer.answerable, f"{question!r} refused: {answer.verdict.reason}"
    joined = " ".join(hit.chunk.text for hit in answer.hits)
    assert expected in joined, f"{question!r} did not reach {expected!r}"


@pytest.mark.parametrize(
    "question",
    [
        "how many connectors do you support",
        "how many connectors are live",
        "how many connectors are there",
        "how many engines are transfer ready",
    ],
)
def test_a_cardinality_question_is_answered_with_the_number(question: str) -> None:
    """Named-fixture floor, on the composed answer rather than on the passages.

    Reaching the passage is not enough for a count: the number has to be in the
    sentence the operator reads. The Pilot retrieves four passages, so this also
    holds the count section inside that window.
    """
    from services.catalog_service import catalog_summary
    from src.ai.rag.product_docs import compose_product_answer

    body = compose_product_answer(retrieve_product_answer(question, limit=4))
    assert str(catalog_summary()["unique_drivers"]) in body.split(".")[0], body[:200]
