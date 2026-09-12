"""A leading "where" is sometimes SQL and sometimes English.

``where region = east`` is a predicate over the rows the operator just sampled.
``where do rejected rows go and can I replay them`` is a question about
quarantine. Both were routed by the opening word alone, so with a sampled table
in focus the second one came back as

    I couldn't complete that lookup:
    • Provide a column to filter on.

The operator asked a documented question and was handed a form to fill in for a
table they had not mentioned.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.followup import (  # noqa: E402
    asks_its_own_question,
    looks_like_followup,
    opens_a_row_predicate,
)
from src.ai.copilot.pilot_agent import DataPilotAgent  # noqa: E402
from src.ai.copilot.working_memory import PilotFocus, get_working_memory  # noqa: E402

SAMPLE_COLUMNS = ("id", "customer", "region", "amount", "status")

PREDICATES = (
    "where region = east",
    "where amount > 100",
    "where status is pending",
    "filter where status is paid",
    "filter where email is null",
    "where customer like acme%",
    "where amount between 10 and 20",
    "where amount is greater than 100",
    "where status is not paid",
)

QUESTIONS = (
    "where do rejected rows go and can I replay them",
    "where does my data land",
    "where do I find the quarantined rows",
    "where can I see the preflight gates",
    "where is the reconcile proof stored",
    "where are the job logs",
    "where should I put the connector secrets",
    "where did the schema drift warning come from",
)


def test_every_predicate_is_read_as_a_predicate() -> None:
    for message in PREDICATES:
        assert opens_a_row_predicate(message, SAMPLE_COLUMNS), message


def test_every_question_is_read_as_a_question() -> None:
    for message in QUESTIONS:
        assert not opens_a_row_predicate(message, SAMPLE_COLUMNS), message


def test_a_bare_imperative_still_reaches_the_tool_that_can_ask() -> None:
    """"Filter" on its own is under-specified, and being asked which column is
    the right answer to it — unlike a sentence, which has to look like one."""
    for message in ("filter", "filter it", "filter this result"):
        assert opens_a_row_predicate(message, SAMPLE_COLUMNS), message


def test_the_stored_columns_settle_what_no_shape_can() -> None:
    """"Where region east" has no operator and no keyword; it has a column."""
    assert opens_a_row_predicate("where region east apac", SAMPLE_COLUMNS)
    assert not opens_a_row_predicate("where region east apac", ())


def test_a_pronoun_after_its_own_antecedent_is_not_a_coreference() -> None:
    """The turn names the rows and then refers back to them inside itself.

    Any pronoun at all used to mean the turn leaned on remembered state, which
    made this look elliptical and handed it to the last sampled table.
    """
    assert asks_its_own_question("where do rejected rows go and can I replay them")
    assert not looks_like_followup(
        "where do rejected rows go and can I replay them",
        PilotFocus(connector_name="Audit SQLite", table="orders", result_id="pr_x"),
    )


def test_a_pronoun_with_nothing_before_it_still_points_at_the_last_turn() -> None:
    """"How many of them are pending" has no antecedent of its own."""
    focus = PilotFocus(connector_name="Audit SQLite", table="orders", result_id="pr_x")
    assert not asks_its_own_question("how many of them are pending")
    assert looks_like_followup("how many of them are pending", focus)


def test_a_documented_question_is_not_planned_as_a_filter() -> None:
    """End to end through the planner, with a sampled table remembered."""
    session = "where-predicate-or-question"
    get_working_memory().remember_focus(
        session,
        PilotFocus(
            connector_name="Audit SQLite",
            connector_type="sqlite",
            table="orders",
            columns=list(SAMPLE_COLUMNS),
            result_id="pr_test_filter_routing",
        ),
    )
    agent = DataPilotAgent()
    context = {"pilot_session_id": session}

    planned = agent._plan_with_memory(
        "where do rejected rows go and can I replay them", context
    )
    assert "filter_result" not in [name for name, _ in planned]

    # The predicate the same focus exists for still lands on the stored rows.
    planned = agent._plan_with_memory("where region = apac", context)
    assert "filter_result" in [name for name, _ in planned]
