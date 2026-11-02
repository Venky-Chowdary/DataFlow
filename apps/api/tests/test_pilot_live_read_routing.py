"""A request phrased as a question is still a request to read a live table.

Two shapes were reaching the documentation instead of the operator's workspace,
and both produced answers that had nothing to do with what was asked.

"What tables are on Audit SQLite" is "list tables on Audit SQLite" phrased as a
question. The imperative routed; the question did not, and the turn answered
with a passage about how a datetime column is created on each engine.

"Break down orders by region on Audit SQLite" is "count orders by region on
Audit SQLite" with a different verb. The latter routed because ``orders on`` is
a business object next to its scope; in the former the aggregation verb and the
``by`` clause pushed them apart, and the turn answered with a passage about
resuming an initial snapshot from the last primary key read.

What separates a read from a documentation question in both cases is the scope:
a preposition followed by something of the operator's. "What tables does the
product support" names nothing of theirs and stays a documentation question.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATAFLOW_PILOT_ENGINE", "local")

import pytest

from src.ai.copilot.tools import _looks_like_live_data_fetch, infer_tools_from_message


def _is_read(question: str) -> bool:
    return _looks_like_live_data_fetch(question.lower())


def _tools(question: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(question)]


#: An inventory or schema read asked as a question rather than ordered.
DECLARATIVE_READS = [
    "what tables are on Audit SQLite",
    "what tables are in Audit SQLite",
    "which tables live on Audit SQLite",
    "what objects are on Audit SQLite",
    "what collections are in mongo_prod",
    "what columns does orders have on Audit SQLite",
    "what columns are in the orders table on Audit SQLite",
    "which fields exist in customers on the warehouse",
]

#: A grouped aggregate over one of the operator's own tables.
GROUPED_READS = [
    "break down orders by region on Audit SQLite",
    "break orders down by region on Audit SQLite",
    "group orders by region on Audit SQLite",
    "count orders by region on Audit SQLite",
    "sum amount by status in orders on Audit SQLite",
    "average amount by region in orders on Audit SQLite",
    "roll up revenue by month on the warehouse",
    "segment customers by country on Audit SQLite",
]

#: The same vocabulary with nothing of the operator's in it. Each is answered
#: from the documentation, and each is a phrasing that the rules above would
#: catch if they keyed on the verb or the noun instead of the scope.
DOCUMENTATION = [
    "what is a table",
    "what tables does the product support",
    "what columns does a quarantine record have",
    "what does group by do",
    "how do you break down a large table",
    "how do I group rows before writing them",
    "what is a region",
    "which sync modes exist",
    "what fields are in an audit event",
    "what columns are in a decimal mapping",
    "how many connectors do you support",
    "what is reverse ETL",
]


@pytest.mark.parametrize("question", DECLARATIVE_READS)
def test_an_inventory_read_asked_as_a_question_is_still_a_read(question):
    assert _is_read(question), f"{question!r} went to the documentation"


@pytest.mark.parametrize("question", GROUPED_READS)
def test_a_grouped_aggregate_over_a_named_table_is_a_read(question):
    assert _is_read(question), f"{question!r} went to the documentation"


@pytest.mark.parametrize("question", DOCUMENTATION)
def test_the_same_words_without_a_scope_stay_a_documentation_question(question):
    assert not _is_read(question), f"{question!r} was taken for a live read"


def test_the_scope_is_what_decides_and_not_the_noun():
    """The pair that makes the rule legible: one word of scope apart."""
    assert _is_read("what tables are on Audit SQLite")
    assert not _is_read("what tables are supported")


def test_an_article_after_the_preposition_is_not_a_named_object():
    """"in an audit event" names a documented shape, not the operator's store.

    Without this guard the declarative rule fired on "what fields are in an
    audit event", which the documentation answers.
    """
    assert not _is_read("what fields are in an audit event")
    assert _is_read("what fields are in audit_events on Audit SQLite")


def test_a_grouped_aggregate_needs_both_the_by_clause_and_the_scope():
    assert _is_read("group orders by region on Audit SQLite")
    # A ``by`` clause with nothing of the operator's.
    assert not _is_read("what does it mean to group rows by key")
    # A scope with no grouping is covered by the plain object rules, not this
    # one; the point here is that the grouping rule does not fire without both.
    assert not _is_read("how do you break down a large table")


# --- a question about the operator's last run has to fetch the run ----------
#
# Recognising a request as a read of the workspace and then reaching no tool is
# worse than either: "prove the row counts matched on my last transfer" was
# taken for a live read, planned nothing, and was answered by asking which
# table and connector the operator meant.

#: Everything an operator asks *about* their last run. The verb varies and the
#: noun for the run varies; what they have in common is the run.
LAST_RUN_REQUESTS = [
    "prove the row counts matched on my last transfer",
    "give me the proof for my last transfer",
    "was my last transfer complete",
    "show the checksum for my last job",
    "did my latest sync finish",
    "what happened in our most recent load",
    "how many rows did the last transfer write",
]


@pytest.mark.parametrize("question", LAST_RUN_REQUESTS)
def test_a_question_about_the_last_run_fetches_the_run(question):
    assert "list_jobs" in _tools(question), (
        f"{question!r} planned {_tools(question)} — no way to reach the run"
    )


def test_a_recognised_read_never_plans_nothing():
    """The failure mode this closes, stated as the rule it broke."""
    for question in LAST_RUN_REQUESTS:
        if _is_read(question):
            assert _tools(question), f"{question!r} is a read that plans no tool"


def test_reconcile_asked_in_general_stays_a_documentation_question():
    """No run is named, so there is nothing to fetch — explain the proof."""
    assert "list_jobs" not in _tools("show me the reconcile proof")
    assert "list_jobs" not in _tools("what does checksum MATCH prove")


# --- the verb needs something to group ---------------------------------------
#
# Teaching the aggregation parser the grouping verbs an operator actually types
# ("break down orders by region", "roll up revenue by month") made it also
# claim the question *about* the clause: "what does group by do" parsed as an
# aggregation and answered a documentation question with a connector error —
# the same way "what does quarantine mean" once did. A verb followed directly
# by ``by`` is not a verb at all, it is the name of the SQL clause.
#
# Parsing only, so these belong here rather than beside the live-Postgres
# aggregate matrix, which skips whole-module when the server is unreachable.

CLAUSE_QUESTIONS = [
    "what does group by do",
    "what is group by",
    "what is a group by clause",
    "explain group by",
    "how does group by work",
    "what does grouped by mean",
]

# The operation named rather than commanded. Measured over HTTP: each of these
# routed to the aggregator, which had nothing to aggregate and answered
# "Connector not found" over the top of the documentation that states the rule
# they asked about. "Does a breakdown include null values" was worse than a bare
# miss — the measure tail is read as a column name, so it planned a count over
# a table called ``include null values``.
ABOUT_THE_OPERATION = [
    "does a breakdown include null values",
    "is the count exact or a sample",
    "is the average exact",
    "can the count be wrong",
    "does the aggregate include nulls",
    "what does an aggregate do",
]

# The same measure words with the operator pointing at data. A scope clause is
# what separates a topic from a request, so these must still reach the tool.
SCOPED_REQUESTS = [
    "does the count include nulls on Demo Orders",
    "count of orders by status",
    "the count of orders by status on Demo Orders",
    "average amount on Demo Orders",
    "sum revenue by month on Demo Orders",
    "count rows in orders",
]

GROUPING_VERBS = [
    ("break down orders by region on Demo Orders", "region"),
    ("breakdown of orders by status on Demo Orders", "status"),
    ("bucket orders by region on Demo Orders", "region"),
    ("segment orders by region on Demo Orders", "region"),
    ("split orders by region on Demo Orders", "region"),
    ("tally orders by region on Demo Orders", "region"),
    ("roll up orders by region on Demo Orders", "region"),
]


@pytest.mark.parametrize("question", CLAUSE_QUESTIONS)
def test_a_question_about_the_clause_is_not_parsed_as_an_aggregation(question):
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    assert parse_aggregation_request(question) is None, question


@pytest.mark.parametrize("question", CLAUSE_QUESTIONS)
def test_a_question_about_the_clause_is_not_a_live_read(question):
    assert not _is_read(question), f"{question!r} classified as a live read"


@pytest.mark.parametrize(("question", "group_by"), GROUPING_VERBS)
def test_a_grouping_verb_with_an_object_still_parses(question, group_by):
    """The guard is a lookahead on ``by``, not a retreat from the vocabulary."""
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    parsed = parse_aggregation_request(question)
    assert parsed is not None, question
    assert parsed.metric == "count", parsed
    assert parsed.group_by == group_by, parsed



# --- the copula may sit between the object and its scope ---------------------

COPULA_READS = [
    "what tables are on Demo Orders",
    "which collections are in Demo Orders",
    "what objects exist on Demo Orders",
    "which tables live in Demo Orders",
]


@pytest.mark.parametrize("question", COPULA_READS)
def test_a_copula_between_the_object_and_its_scope_still_reads_the_table(question):
    """"What tables are on X" is "tables on X" asked as a question.

    Without it the alternatives had to enumerate every way of saying this —
    "do we have", "exist", "are there", "are available" — and plain "are" is
    the one they missed, so the turn answered with how a datetime column is
    created on each engine.
    """
    assert "list_connector_objects" in _tools(question), (
        f"{question!r} planned {_tools(question)}"
    )


@pytest.mark.parametrize("question", ABOUT_THE_OPERATION)
def test_a_question_about_the_measure_is_not_a_request_for_it(question):
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    assert parse_aggregation_request(question) is None, question


@pytest.mark.parametrize("question", SCOPED_REQUESTS)
def test_a_measure_with_a_scope_still_reaches_the_aggregator(question):
    """The guard is the absence of data to point at, not the measure word."""
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    assert parse_aggregation_request(question) is not None, question


def test_the_guard_does_not_swallow_a_plain_request_that_reads_the_same_way():
    """"Show me the count" has the same determiner and is a real request.

    Its own slot is empty, which the parser has always answered with a
    follow-up rather than a refusal, and that path must stay reachable.
    """
    from src.ai.copilot.aggregate_tools import _asks_about_the_operation

    assert not _asks_about_the_operation("show me the count")
    assert not _asks_about_the_operation("count of orders by status")
    assert _asks_about_the_operation("is the count exact or a sample")


# --- a question about the catalog is not a table to count --------------------
#
# Measured over HTTP: "how many file formats can you read" and "how many
# sources can you connect to" planned a count over tables named `file formats
# can` and `sources can`. No error reached the operator, because the
# documentation answer led, but every such turn ran a doomed lookup first and
# the same defect *did* surface as a validation error for phrasings where the
# documentation did not lead.

CATALOG_COUNTS = [
    "how many file formats can you read",
    "how many sources can you connect to",
    "how many destinations do you support",
    "how many roles are there",
    "how many gates run before a write",
    "how many engines can it use",
    "how many warehouses do you support",
    "how many sync modes are there",
]

# The same nouns with a connector named. The operator really does mean a table
# of that name, so the rejection must not reach them.
NAMED_ON_A_CONNECTOR = [
    ("count sources on Demo Orders", "sources"),
    ("count modes on Demo Orders", "modes"),
    ("count orders by region on Demo Orders", "orders"),
]


@pytest.mark.parametrize("question", CATALOG_COUNTS)
def test_a_count_of_the_products_own_inventory_is_not_an_aggregation(question):
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    assert parse_aggregation_request(question) is None, question


@pytest.mark.parametrize(("question", "table"), NAMED_ON_A_CONNECTOR)
def test_the_same_noun_on_a_named_connector_still_aggregates(question, table):
    from src.ai.copilot.aggregate_tools import parse_aggregation_request

    parsed = parse_aggregation_request(question)
    assert parsed is not None, question
    assert parsed.table == table, parsed
    assert parsed.connector_name == "Demo Orders", parsed


def test_a_modal_is_never_part_of_a_name():
    """"File formats can" is a fragment of the question, not an identifier."""
    from src.ai.copilot.aggregate_tools import _STOP_TOKENS, _trim_filler

    assert {"can", "could", "will", "would", "should"} <= _STOP_TOKENS
    assert _trim_filler("file formats can") == "file formats"
    assert _trim_filler("sources can") == "sources"


def test_the_head_of_the_noun_phrase_is_checked_and_not_only_the_first_word():
    """"File formats" is a question about formats; ``file`` is not a platform noun."""
    from src.ai.copilot.aggregate_tools import _PLATFORM_NOUNS, parse_aggregation_request

    assert "formats" in _PLATFORM_NOUNS
    assert "file" not in _PLATFORM_NOUNS
    assert parse_aggregation_request("count file formats") is None


def test_the_operators_own_warehouse_is_a_scope_and_not_the_catalog():
    """Only the plural belongs to the catalog.

    Rejecting the singular too cost "how many datasets in the warehouse" its
    live read: with ``in the warehouse`` read as a catalog question there was
    nothing left to count. The definite singular names the operator's store.
    """
    from src.ai.copilot.aggregate_tools import _PLATFORM_NOUNS, parse_aggregation_request

    assert "warehouses" in _PLATFORM_NOUNS
    assert "warehouse" not in _PLATFORM_NOUNS
    assert parse_aggregation_request("how many warehouses do you support") is None
    scoped = parse_aggregation_request("how many datasets in the warehouse")
    assert scoped is not None
    assert scoped.table == "warehouse", scoped
