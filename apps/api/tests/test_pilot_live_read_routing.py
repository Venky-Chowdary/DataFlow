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

from src.ai.copilot.tools import _looks_like_live_data_fetch


def _is_read(question: str) -> bool:
    return _looks_like_live_data_fetch(question.lower())


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
