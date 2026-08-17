"""DataPilot understands an ordinary transfer sentence — and its rules.

The failure this pins down: "transfer data from sql to postgres from table
users by following these rules: skip nulls, dedupe on email, upsert on id"
routed to nothing (or to ``recommend_sync_mode``), so the Pilot answered "I'm
not sure how to do that" for a request it is built to serve.

Two properties are asserted throughout:

* the route resolves from the *whole* sentence — rule clauses are parsed, not
  swallowed into a connector name, and the object may be named after the
  destination ("… to postgres from table users");
* a rule that cannot be applied is never dropped. It becomes a question and
  downgrades the plan to plan-only, so nothing is staged that would move rows
  the operator excluded.
"""

from __future__ import annotations

import pytest

from src.ai.copilot.tools import infer_tools_from_message, parse_transfer_intent
from src.ai.copilot.transfer_rules import filter_columns, parse_transfer_data_rules


def _tool_args(message: str, name: str) -> dict:
    for planned_name, args in infer_tools_from_message(message):
        if planned_name == name:
            return args
    raise AssertionError(
        f"{name} was not planned for {message!r}; got "
        f"{[n for n, _ in infer_tools_from_message(message)]}"
    )


# --------------------------------------------------------------------------
# The reported failure
# --------------------------------------------------------------------------


def test_owner_reported_sentence_resolves_a_route():
    intent = parse_transfer_intent("transfer data from sql to postgres from table users")
    assert intent is not None
    assert intent["source_table"] == "users"
    assert intent["source_connector_name"] == "sql"
    assert intent["dest_connector_name"] == "postgres"


def test_owner_reported_sentence_with_typo_still_resolves():
    intent = parse_transfer_intent("tranfer data from sql to postgres from table users")
    assert intent is not None
    assert intent["source_table"] == "users"


def test_owner_reported_sentence_stages_a_transfer_not_advice():
    names = [n for n, _ in infer_tools_from_message(
        "transfer data from sql to postgres from table users"
    )]
    assert names == ["start_transfer"]


def test_rule_bearing_sentence_keeps_the_route_and_asks_for_the_missing_detail():
    message = (
        "transfer data from sql to postgres from table users by following these "
        "rules: skip nulls, dedupe on email, upsert on id"
    )
    args = _tool_args(message, "plan_transfer")
    assert args["source_table"] == "users"
    assert args["dest_connector_name"] == "postgres"
    assert args["upsert_key"] == "id"
    assert args["dedupe_key"] == "email"
    questions = " ".join(args["rule_questions"]).lower()
    # Bare "skip nulls" has no column, and two different keys are two identities.
    assert "skip nulls" in questions
    assert "one key" in questions


def test_unapplied_rule_downgrades_to_plan_only():
    message = (
        "transfer users from Local PG 5433 to Warehouse by following these rules: "
        "skip nulls"
    )
    names = [n for n, _ in infer_tools_from_message(message)]
    assert "plan_transfer" in names
    assert "start_transfer" not in names


# --------------------------------------------------------------------------
# Phrasings operators actually use
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("message", "table", "src", "dst"),
    [
        (
            "move the users table from SQL Server to Postgres",
            "users",
            "SQL Server",
            "Postgres",
        ),
        (
            "transfer table users from sqlserver to postgres",
            "users",
            "sqlserver",
            "postgres",
        ),
        (
            "please move data into Warehouse from Local PG 5433 for table customers",
            "customers",
            "Local PG 5433",
            "Warehouse",
        ),
        (
            "load the events collection from Mongo Prod to Snowflake DW",
            "events",
            "Mongo Prod",
            "Snowflake DW",
        ),
        (
            "copy orders out of Local PG 5433 into Warehouse",
            "orders",
            "Local PG 5433",
            "Warehouse",
        ),
    ],
)
def test_common_phrasings_resolve(message: str, table: str, src: str, dst: str):
    intent = parse_transfer_intent(message)
    assert intent is not None, message
    assert intent["source_table"] == table
    assert intent["source_connector_name"] == src
    assert intent["dest_connector_name"] == dst


def test_rules_do_not_leak_into_the_destination_name():
    intent = parse_transfer_intent(
        "transfer table users from sqlserver to postgres, only rows where "
        "status='active', upsert on id"
    )
    assert intent is not None
    # The old grammar produced a connector called
    # "postgres, only rows where status='active', upsert on id".
    assert intent["dest_connector_name"] == "postgres"
    assert intent["source_filter"] == {
        "column": "status",
        "operator": "eq",
        "value": "active",
    }
    assert intent["upsert_key"] == "id"
    assert not intent.get("rule_questions")


def test_filter_and_limit_reach_the_staged_tool_call():
    args = _tool_args(
        "copy users from Local PG 5433 to Warehouse where signup_date >= 2024-01-01, "
        "first 100 rows",
        "start_transfer",
    )
    assert args["source_filter"] == {
        "column": "signup_date",
        "operator": "gte",
        "value": "2024-01-01",
    }
    assert args["limit"] == 100


def test_multiple_conditions_become_one_and_filter():
    _, rules = parse_transfer_data_rules(
        "move users from A to B where status = active, where country in (US, CA)"
    )
    assert rules.source_filter["and"] == [
        {"column": "status", "operator": "eq", "value": "active"},
        {"column": "country", "operator": "in", "value": ["US", "CA"]},
    ]
    assert filter_columns(rules.source_filter) == ["status", "country"]


def test_and_conditions_survive_a_value_list_comma():
    _, rules = parse_transfer_data_rules(
        "move users from A to B where status = active and country in (US, CA)"
    )
    assert rules.source_filter == {
        "and": [
            {"column": "status", "operator": "eq", "value": "active"},
            {"column": "country", "operator": "in", "value": ["US", "CA"]},
        ]
    }
    assert rules.questions == ()


def test_or_conditions_become_an_or_group():
    _, rules = parse_transfer_data_rules(
        "move users from A to B where status = active or status = trial"
    )
    assert rules.source_filter == {
        "or": [
            {"column": "status", "operator": "eq", "value": "active"},
            {"column": "status", "operator": "eq", "value": "trial"},
        ]
    }


@pytest.mark.parametrize(
    "clause",
    [
        # Mixed and/or has no unambiguous precedence in English.
        "where status = active and amount > 100 or vip = yes",
        # Not a comparison at all.
        "where the customer looks legit",
    ],
)
def test_unreadable_condition_is_never_partially_applied(clause: str):
    _, rules = parse_transfer_data_rules(f"move users from A to B {clause}")
    assert rules.source_filter == {}
    assert rules.blocking


def test_skip_nulls_in_a_named_column_is_applied_not_questioned():
    _, rules = parse_transfer_data_rules(
        "move users from A to B, skip nulls in email"
    )
    assert rules.source_filter == {"column": "email", "operator": "is_not_null"}
    assert rules.questions == ()


# --------------------------------------------------------------------------
# Fail closed: ambiguity is a question, never a guess
# --------------------------------------------------------------------------


def test_business_state_without_a_column_is_a_question():
    _, rules = parse_transfer_data_rules(
        "migrate users from A to B, keep only active rows"
    )
    assert rules.source_filter == {}
    assert rules.blocking
    assert "status" in " ".join(rules.questions)


def test_masking_is_named_as_a_studio_control():
    _, rules = parse_transfer_data_rules("move users from A to B and mask ssn")
    assert rules.blocking
    assert "Map" in " ".join(rules.questions)


def test_cadence_is_carried_as_a_cadence_not_dropped_into_the_route():
    # A cadence is now honoured by staging a schedule (see
    # test_pilot_schedule_nl.py), so it must survive parsing intact — and must
    # not be read as part of the destination connector's name.
    intent = parse_transfer_intent(
        "sync the orders collection from Mongo Prod into Snowflake DW nightly"
    )
    assert intent is not None
    assert intent["dest_connector_name"] == "Snowflake DW"
    assert intent["cadence"] == "nightly"


def test_all_tables_is_not_read_as_a_table_named_tables():
    intent = parse_transfer_intent(
        "migrate all tables from my sql server into postgres"
    )
    assert intent is not None
    assert intent["source_table"] == ""
    assert intent["all_tables"] is True
    assert intent["plan_only"] is True
    assert intent["dest_connector_name"] == "postgres"


# --------------------------------------------------------------------------
# Routing precedence
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "message",
    [
        "transfer data from sql to postgres from table users, upsert on id",
        "move the users table from SQL Server to Postgres as upsert",
        "migrate users from Local PG 5433 to Warehouse and dedupe on email",
    ],
)
def test_transfer_requests_do_not_route_to_inventory_or_advice(message: str):
    names = {n for n, _ in infer_tools_from_message(message)}
    assert names & {"start_transfer", "plan_transfer"}
    assert "list_jobs" not in names
    assert "recommend_sync_mode" not in names
    assert "introspect_connector_schema" not in names


def test_asking_for_a_schema_still_introspects():
    names = {n for n, _ in infer_tools_from_message(
        "show me the columns on users in Local PG 5433"
    )}
    assert "introspect_connector_schema" in names


def test_transfer_wording_alone_does_not_stage_a_mutation():
    # No object, no route — this must stay a question, not a staged run.
    assert parse_transfer_intent("can you transfer some data for me") is None
