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
    text = _section("How many connectors, sources and destinations are transfer-ready").text
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
    assert "How many connectors, sources and destinations are transfer-ready" in titles


def test_the_count_section_stays_short_enough_to_rank_for_its_own_question() -> None:
    """BM25 length normalization decides whether the answer is retrievable at all.

    The first draft stated the counts, the honesty caveat, the planned share,
    all 46 driver names and the per-side split. It ranked fourth of five for
    "how many connectors do you support" — below three sections that say
    nothing about counts — and the Pilot retrieves four, so it was cut off.
    """
    text = _section("How many connectors, sources and destinations are transfer-ready").text
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


# --------------------------------------------------------------------------
# Grouped aggregates: the operation Pilot could run but not explain
# --------------------------------------------------------------------------
#
# "break down orders by region on Demo Orders" plans a real ``GROUP BY``
# against the live table, and "what does group by do" was refused as outside
# the documentation. Measured both before and after the routing fix that
# stopped the same phrasing being *mistaken* for an aggregation, so the refusal
# was never a regression — the corpus simply said nothing about the one
# analytical operation the product performs.

_AGGREGATE_SECTION = "What a grouped aggregate is and what it guarantees"


def test_every_metric_the_passage_offers_is_one_the_engine_runs() -> None:
    """The passage cannot advertise a measure the aggregation tool lacks.

    Read out of ``_METRICS`` rather than retyped beside it: a hand-written list
    is how documentation ends up offering a median the engine cannot compute.
    """
    from src.ai.copilot.aggregate_tools import _METRICS
    from src.ai.rag.product_facts import _METRIC_ENGLISH

    text = _section(_AGGREGATE_SECTION).text
    for name, english in _METRIC_ENGLISH.items():
        if name in _METRICS:
            assert english in text, f"{name} is runnable but unlisted"
        else:
            assert english not in text, f"{name} is offered but not runnable"


def test_the_group_cap_is_the_constant_the_tool_enforces() -> None:
    """An operator planning around the cap is told the number that applies."""
    from src.ai.copilot.aggregate_tools import _DEFAULT_GROUP_LIMIT, _MAX_GROUP_LIMIT

    text = _section(_AGGREGATE_SECTION).text
    assert str(_DEFAULT_GROUP_LIMIT) in text
    assert str(_MAX_GROUP_LIMIT) in text


def test_the_null_group_rule_is_stated_rather_than_left_to_be_discovered() -> None:
    """A total that quietly drops rows is the failure this product forbids.

    The same no-silent-loss rule quarantine states for rows, said for groups,
    because it is the one thing an operator cannot verify by looking at the
    result.
    """
    text = _section(_AGGREGATE_SECTION).text.lower()
    assert "null" in text
    assert "filtered away" in text or "rather than" in text


def test_the_aggregate_is_documented_as_exact_and_not_sampled() -> None:
    """Pushed down to the engine, so the number is not an extrapolation."""
    text = _section(_AGGREGATE_SECTION).text.lower()
    assert "exact" in text
    assert "sample" in text


def test_the_definition_opens_in_the_shape_a_definition_is_scored_in() -> None:
    """Sentence selection pays the definition bonus on subject-then-copula.

    The first draft opened "A grouped aggregate *answers* one question per…",
    which is not that shape, so the null-group paragraph — which happens to
    read "A group whose value *is* NULL is reported…" — collected the
    definition credit and led the answer to "what is group by" with the null
    rule instead of the definition.
    """
    from src.ai.rag.answer_composer import _DEFINITIONAL, split_sentences

    lead = split_sentences(_section(_AGGREGATE_SECTION).text)[0]
    assert _DEFINITIONAL.match(lead), lead


@pytest.mark.parametrize(
    "question",
    [
        "what does group by do",
        "what is group by",
        "what is a group by clause",
        "what is a grouped aggregate",
        "what is an aggregate",
        "explain aggregation",
        "what does a breakdown show me",
        "does a breakdown include null values",
        "is the count exact or a sample",
        "can pilot average a column",
    ],
)
def test_an_aggregation_question_leads_with_the_aggregation_passage(
    question: str,
) -> None:
    """Named-fixture floor. Every one of these was refused outright before."""
    from src.ai.rag.product_docs import compose_product_answer

    answer = retrieve_product_answer(question, limit=4)
    assert answer.answerable, f"{question!r} refused: {answer.verdict.reason}"
    body = compose_product_answer(answer)
    assert body.strip(), f"{question!r} composed nothing"
    assert _AGGREGATE_SECTION in body, body[:300]


# --------------------------------------------------------------------------
# The corpus enumerates; now it also counts
# --------------------------------------------------------------------------

_INVENTORY_SECTION = "How many sync modes, roles, engines and formats there are"


def test_every_count_is_read_from_the_registry_that_enforces_it() -> None:
    """A retyped number is how documentation falls behind the product."""
    from services.rbac import role_names
    from services.sync_cursor import CANONICAL_SYNC_MODES
    from src.ai.rag.product_facts import _SYNC_MODE_BEHAVIOUR

    text = _section(_INVENTORY_SECTION).text
    modes = [m for m in CANONICAL_SYNC_MODES if _SYNC_MODE_BEHAVIOUR.get(m)]
    assert f"{len(modes)} sync modes" in text
    assert f"{len(list(role_names()))} roles" in text


def test_the_counts_keep_the_tile_caveat_off_the_driver_number() -> None:
    """A catalog tile is not a transfer-ready driver, and this section says so."""
    text = _section(_INVENTORY_SECTION).text.lower()
    assert "catalog tile is not a transfer-ready driver" in text


def test_the_counting_section_is_one_sentence_of_answer() -> None:
    """A second sentence of provenance took the lead away from the numbers.

    Measured: with "Each number is counted from the registry the engine
    dispatches on…" in the passage, "what are the roles in this product" opened
    with that sentence instead of the one naming the roles, and the audit lost
    a case to it. Provenance belongs in ``source_module``, which is what the
    citation renders.
    """
    section = _section(_INVENTORY_SECTION)
    assert len(section.text.split()) < 60, section.text
    assert "counted from the registry" not in section.text
    assert section.source_module


def test_counting_them_inside_the_listing_sections_was_the_worse_option() -> None:
    """The sections that enumerate must not also open with a total.

    A count sentence at the front of the sync-mode grid took a slot in the
    six-sentence answer, and "what is change data capture" — whose answer is
    one of those paragraphs — stopped mentioning CDC at all.
    """
    for title in ("What each sync mode does", "What each role can do"):
        text = _section(title).text
        assert not text.lstrip().startswith("There are"), title


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("how many sync modes are there", "sync modes"),
        ("what is the number of sync modes", "sync modes"),
        ("how many roles are there", "roles"),
    ],
)
def test_a_cardinality_question_is_led_by_the_count(
    question: str, expected: str
) -> None:
    """Named-fixture floor. Each of these opened with a definition before."""
    import re

    from src.ai.rag.product_docs import compose_product_answer

    body = " ".join(
        (compose_product_answer(retrieve_product_answer(question, limit=4)) or "").split()
    )
    lead = body.split(". ")[0]
    assert re.search(r"\b\d[\d,]*\b", lead), lead[:200]
    assert expected in lead, lead[:200]


def test_change_data_capture_still_leads_with_the_capture_answer() -> None:
    """The case the in-place counts cost, kept as the guard that it did."""
    from src.ai.rag.product_docs import compose_product_answer

    body = " ".join(
        (compose_product_answer(retrieve_product_answer("what is change data capture", limit=4)) or "").split()
    )
    lead = body.split(". ")[0].lower()
    assert "cdc" in lead or "change data capture" in lead, lead[:200]
    assert "log" in lead, lead[:200]


# --------------------------------------------------------------------------
# One enum, one shape: nine parallel lines that score the same way
# --------------------------------------------------------------------------

def test_every_line_of_the_sync_mode_grid_is_written_as_a_definition() -> None:
    """A definition ask pays for definitional shape, so the grid must be uniform.

    When ``mirror`` was the only line opening with a copula it collected that
    bonus for questions about every *other* mode: asked "what is SCD type 2"
    the answer led with "Sync mode mirror is upsert plus deletion", and asked
    "what does mirror mode do to deleted rows" — before the grid was made
    uniform — the answer led with whichever other line the arbitrary part of
    the ranking preferred. Uniform shape hands the decision back to the words
    the operator typed.
    """
    from src.ai.rag.answer_composer import _DEFINITIONAL, split_sentences

    section = _section("What each sync mode does")
    lines = [
        line
        for line in split_sentences(section.text, section.section_title)
        if line.startswith("Sync mode ")
    ]
    assert len(lines) >= 8, lines
    off_shape = [line for line in lines if not _DEFINITIONAL.match(line)]
    assert not off_shape, off_shape


@pytest.mark.parametrize(
    ("question", "expected"),
    [
        ("what is SCD type 2", "scd2"),
        ("what does mirror mode do to deleted rows", "mirror"),
        ("what is upsert mode", "upsert"),
        ("what does cdc mode do", "cdc"),
    ],
)
def test_a_question_about_one_mode_leads_with_that_mode(
    question: str, expected: str
) -> None:
    """Named-fixture floor: the lead names the mode that was asked about."""
    from src.ai.rag.product_docs import compose_product_answer

    body = " ".join(
        (compose_product_answer(retrieve_product_answer(question, limit=4)) or "").split()
    )
    assert body, question
    assert expected in body.split(". ")[0].lower(), body[:220]


# --------------------------------------------------------------------------
# Destinations and schema policies: lists the corpus already knew, unpublished
# --------------------------------------------------------------------------

def test_the_destination_list_is_read_from_the_capability_registry() -> None:
    from src.transfer.connector_capabilities import dest_live_driver_types

    dests = dest_live_driver_types()
    text = _section("Which destinations you can write to").text
    assert str(len(dests)) in text
    for name in dests[:5]:
        assert name in text


@pytest.mark.parametrize(
    "question",
    [
        "which destinations can I write to",
        "what destinations do you support",
        "which destinations do you support",
    ],
)
def test_a_destination_listing_opens_on_the_destinations(question: str) -> None:
    from src.ai.rag.product_docs import compose_product_answer

    body = " ".join(
        (compose_product_answer(retrieve_product_answer(question, limit=4)) or "").split()
    )
    lead = body.split(". ")[0]
    assert "destination" in lead.lower(), lead[:200]
    assert "full append" not in lead.lower(), lead[:200]
    assert "g1" not in lead.lower(), lead[:200]


@pytest.mark.parametrize(
    ("question", "needles"),
    [
        ("who can run transfers", ("operator", "editor", "admin")),
        ("what roles can approve a risky mapping", ("editor", "admin")),
        ("what is change data capture", ("cdc", "log")),
        ("is CDC exactly once", ("at-least-once", "exactly-once")),
        (
            "what happens to a timestamp without timezone",
            ("wall clock", "utc_invented_from_naive"),
        ),
        (
            "what happens to a character the destination cannot store",
            ("quarantined", "unsupported"),
        ),
        (
            "what happens if a numeric overflows the destination type",
            ("preflight finding", "lossy"),
        ),
        ("can I keep my pipelines in git", ("yaml", "git")),
        ("can I filter rows before they are written", ("filter", "transform")),
        (
            "what is the difference between append and overwrite",
            ("insert-only", "replaces the destination"),
        ),
        (
            "can I schedule a pipeline to run every night at 2am",
            ("cadence", "recurring"),
        ),
        (
            "can I schedule a transfer every hour",
            ("cadence", "hourly", "cron"),
        ),
        (
            "how do I pause a schedule",
            ("pause", "activate", "pipelines"),
        ),
        (
            "how do I connect a postgres database",
            ("new connection", "postgresql"),
        ),
        (
            "how do I use the API",
            ("/api/v1", "endpoint"),
        ),
        (
            "how do I export proof for an auditor",
            ("archive", "checksum"),
        ),
        (
            "my connector test passed but the transfer failed, why",
            ("does **not** skip preflight", "validate still runs"),
        ),
        (
            "what do I do when validate is blocked",
            ("suggested fixes", "accept risk", "remap", "blocked gate"),
        ),
        (
            "do you have webhooks",
            ("webhook",),
        ),
        (
            "what timezone are timestamps stored in",
            ("utc", "offset label"),
        ),
        (
            "what are the preflight gates",
            ("g1", "g9"),
        ),
        (
            "explain the preflight gates",
            ("g1", "g9"),
        ),
        ("what is type_locked", ("type_locked", "type change")),
        ("what is standing authority", ("unattended", "authorize")),
        ("what is G3", ("g3", "schema contract")),
        ("who is allowed to start a transfer", ("editor", "admin")),
        (
            "if I run the same CDC change twice is it safe",
            ("_df_lsn", "at-least-once"),
        ),
        ("where do bad rows end up", ("quarantine",)),
        ("can I turn off a nightly pipeline", ("pause", "pipelines")),
        ("how do I cancel a running transfer", ("cancel", "job")),
        (
            "how are you different from airbyte",
            ("semantic mapping", "quarantine", "checksum"),
        ),
        (
            "can I query my warehouse from here",
            ("query playground", "read-only"),
        ),
        (
            "can pipelines run while nobody is watching",
            ("unattended", "standing authority"),
        ),
        (
            "how do you hand off from snapshot to the CDC stream",
            ("log position", "handoff"),
        ),
        (
            "do you read the WAL or just poll",
            ("wal", "poll"),
        ),
        (
            "what happens to a delete in CDC",
            ("tombstone", "hard delete"),
        ),
        (
            "can a viewer start a transfer",
            ("viewer",),
        ),
        (
            "what is a create-new mapping",
            ("create-new",),
        ),
        (
            "how do I resume if the job crashes between snapshot and stream",
            ("handoff", "boundary"),
        ),
        (
            "how do I export a schedule as YAML",
            ("detail drawer",),
        ),
        (
            "how do I stop a type change from being applied",
            ("type_locked",),
        ),
    ],
)
def test_the_lead_names_the_outcome_the_question_asked_for(
    question: str, needles: tuple[str, ...]
) -> None:
    """Named-fixture floor for the consequence / permission / CDC cluster."""
    from src.ai.rag.product_docs import compose_product_answer

    body = " ".join(
        (compose_product_answer(retrieve_product_answer(question, limit=4)) or "").split()
    )
    lead = body.split(". ")[0].lower()
    missing = [n for n in needles if n.lower() not in lead]
    assert not missing, f"{missing} not in {lead[:240]}"


def test_the_schema_change_policies_are_named_in_the_lead() -> None:
    from src.ai.rag.product_docs import compose_product_answer
    from services.schedule_store import SCHEMA_POLICIES

    body = " ".join(
        (
            compose_product_answer(
                retrieve_product_answer("what schema change policies are there", limit=4)
            )
            or ""
        ).split()
    )
    lead = body.split(". ")[0]
    for name in SCHEMA_POLICIES:
        assert name in lead, lead[:220]


def test_the_how_to_passages_are_generated() -> None:
    """One short section per procedure the audit still buried in a wizard step."""
    titles = {section.section_title for section in generated_sections()}
    for title in (
        "Procedure: set a nightly, hourly or cron pipeline cadence",
        "Procedure: pause a schedule",
        "Procedure: connect a PostgreSQL database",
        "Procedure: call the /api/v1 endpoints",
        "Procedure: export checksum proof for an auditor",
        "Procedure: Test passed does not skip preflight",
        "How many destinations do you support",
        "Procedure: remap or Accept risk when Validate is blocked",
        "Webhooks",
        "What is the difference between append and overwrite",
        "Which preflight gates run before a write",
        "What type_locked rejects",
        "Procedure: stop a type change with type_locked",
        "Can pipelines run unattended while nobody is watching",
        "Who is allowed to start a transfer",
        "Procedure: cancel a running transfer",
        "What Query Playground is",
        "How Datawrap differs from Airbyte and Fivetran",
        "Where bad rows end up",
        "How the snapshot hands off to the CDC stream",
        "What happens to a delete in CDC",
        "What a create-new mapping is",
        "Procedure: export a schedule as YAML",
    ):
        assert title in titles, title
