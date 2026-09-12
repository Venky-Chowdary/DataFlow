"""A table switch names the table last, and a pronoun names nothing.

``_extract_edit_table`` read the first identifier after "what about" as a new
table. That is right for "what about the invoices table" and wrong for every
question whose subject is a noun phrase: "what about generated columns" dropped
the noun and introspected a table called ``generated``, so the first question
an operator asked after a count came back as a lookup error.

Measured before the fix, in one session on a seeded SQLite fixture:

    how many rows in orders  -> 12 rows
    what about generated columns -> Could not read the schema of 'generated'
    what about case sensitivity  -> Could not read the schema of 'case'
    what about it                -> Could not read the schema of 'it'
"""

from __future__ import annotations

import pytest

from src.ai.copilot.followup import _extract_edit_table, resolve_followup
from src.ai.copilot.working_memory import PilotFocus


@pytest.mark.parametrize(
    "message,table",
    [
        ("same for products", "products"),
        ("now do orders", "orders"),
        ("what about the invoices table", "invoices"),
        ("switch to customers", "customers"),
        ("how about products", "products"),
        # An edit clause may follow the name — the name is still last.
        ("same for products by region", "products"),
        ("now do orders instead", "orders"),
        ("what about orders in Prod Mongo", "orders"),
    ],
)
def test_a_table_switch_still_parses(message: str, table: str) -> None:
    assert _extract_edit_table(message) == table


@pytest.mark.parametrize(
    "message",
    [
        "what about generated columns",
        "what about case sensitivity",
        "what about column order",
        "what about name collisions",
    ],
)
def test_a_trailing_noun_means_the_token_was_a_modifier(message: str) -> None:
    assert _extract_edit_table(message) == ""


@pytest.mark.parametrize("message", ["what about it", "same for that", "now do this"])
def test_a_pronoun_is_not_a_table_name(message: str) -> None:
    """It points at the remembered table; it does not name a new one."""
    assert _extract_edit_table(message) == ""


def test_a_question_about_an_aspect_does_not_edit_the_remembered_query() -> None:
    """With nothing left to edit, routing gets the turn back."""
    focus = PilotFocus(
        connector_name="Audit SQLite",
        table="orders",
        metric="count",
        tool="aggregate_data",
    )
    assert resolve_followup("what about generated columns", focus) is None


def test_a_real_ellipsis_still_edits_the_remembered_query() -> None:
    focus = PilotFocus(
        connector_name="Audit SQLite",
        table="orders",
        metric="count",
        tool="aggregate_data",
    )
    req = resolve_followup("and by region", focus)
    assert req is not None
    assert req.table == "orders"
    assert req.group_by == "region"
