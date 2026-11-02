"""Routing contracts for the operations an operator asks Pilot to perform.

Every case here is a phrasing that reached the wrong tool — or no tool at all —
and came back either as a documentation essay, an inventory listing, or "I'm
not sure how to do that". They are grouped by the *shape* of the ask rather
than by the tool, because each defect was a whole family: the planner had grown
one literal pattern per phrasing it had been shown, so the next phrasing of the
same request fell through.
"""

from __future__ import annotations

import sqlite3

import pytest

from src.ai.copilot.followup import (
    names_its_own_subject,
    resolve_table_coreference_tools,
)
from src.ai.copilot.tools import infer_tools_from_message
from src.ai.copilot.working_memory import PilotFocus


def _tools(message: str) -> list[str]:
    return [name for name, _ in infer_tools_from_message(message)]


def _args(message: str, tool: str) -> dict:
    for name, args in infer_tools_from_message(message):
        if name == tool:
            return args or {}
    raise AssertionError(f"{tool} not planned for {message!r}: {_tools(message)}")


# --------------------------------------------------------------------------
# "Do I have any X" — asking whether something exists at all
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "question,tool",
    [
        ("do I have any contracts", "list_contracts"),
        ("are there any contracts", "list_contracts"),
        ("have we got any data contracts", "list_contracts"),
        ("do I have any jobs", "list_jobs"),
        ("are there any failed jobs", "list_jobs"),
        ("do I have any schedules", "list_schedules"),
        ("do I have any pipelines", "list_schedules"),
        ("do I have any connectors", "list_connectors"),
    ],
)
def test_existence_questions_reach_the_inventory_tool(question: str, tool: str) -> None:
    """The listing patterns all keyed on an imperative verb or a possessive.

    "List my contracts" worked; "do I have any contracts" — the most natural
    way to ask — matched nothing and got the generic capability list back.
    """
    assert tool in _tools(question)


def test_defining_a_contract_is_not_an_inventory_ask() -> None:
    planned = _tools("what is a data contract")
    assert "explain_product" in planned
    assert "list_contracts" not in planned


# --------------------------------------------------------------------------
# Row previews — the quantity sits between the verb and the table
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "question,table,limit",
    [
        ("show me 3 rows from orders on Audit SQLite", "orders", 3),
        ("sample 3 rows from orders on Audit SQLite", "orders", 3),
        ("preview 5 rows of orders on Audit SQLite", "orders", 5),
        ("show me the first 3 rows of orders on Audit SQLite", "orders", 3),
        ("read the last 10 rows of orders", "orders", 10),
        ("fetch 100 records from users on pg", "users", 100),
        ("show me some rows from orders on Audit SQLite", "orders", 0),
        ("get a few records from orders on Audit SQLite", "orders", 0),
    ],
)
def test_row_preview_parses_table_and_honours_the_count(
    question: str, table: str, limit: int
) -> None:
    """"show me 3 rows from orders on Audit SQLite" sampled a table called ``3``.

    The count was read as the table name and the rest of the sentence as the
    connector, so the answer was "provide a simple table name".
    """
    args = _args(question, "sample_connector_object")
    assert args.get("table") == table
    assert int(args.get("limit") or 0) == limit


def test_row_preview_without_a_count_keeps_the_old_parse() -> None:
    """"show the data from countries" must still capture the table, not ``data``."""
    assert _args("show the data from countries", "sample_connector_object")["table"] == (
        "countries"
    )


def test_asking_for_tables_is_inventory_not_a_table_named_tables() -> None:
    assert "list_connector_objects" in _tools("get tables from Audit SQLite")


# --------------------------------------------------------------------------
# Destructive asks stay refused
# --------------------------------------------------------------------------

@pytest.mark.parametrize(
    "question",
    [
        "delete all my connectors",
        "drop the orders table",
        "remove the postgres connector",
        "delete my schedules",
    ],
)
def test_destructive_asks_do_not_reach_an_inventory_listing(question: str) -> None:
    """Answering "delete all my connectors" with the connector list reads as consent."""
    planned = _tools(question)
    assert "list_connectors" not in planned
    assert "list_schedules" not in planned


# --------------------------------------------------------------------------
# Remembered state must not capture a self-contained question
# --------------------------------------------------------------------------

def _focus() -> PilotFocus:
    return PilotFocus(
        connector_name="Audit SQLite",
        connector_id="c-1",
        table="orders",
        metric="count",
    )


def test_a_product_question_is_not_a_pointer_at_the_remembered_table() -> None:
    """"what schema change policies are there" is existential ``there``.

    The coreference layer runs ahead of ordinary routing, so it took the turn
    away from a correct documentation plan and introspected whichever table the
    operator had last counted.
    """
    assert resolve_table_coreference_tools(
        "what schema change policies are there", _focus()
    ) is None


def test_a_real_pointer_still_resolves_to_the_remembered_table() -> None:
    resolved = resolve_table_coreference_tools("show me the schema of that table", _focus())
    assert resolved and resolved[0][0] == "introspect_connector_schema"
    assert resolved[0][1]["table"] == "orders"


def test_platform_tools_carry_their_own_subject() -> None:
    assert names_its_own_subject([("list_jobs", {"limit": 10})])
    assert names_its_own_subject([("list_contracts", {})])


def test_documentation_fallbacks_do_not_count_as_their_own_subject() -> None:
    """"by region" and "how many rows" fall back to ``explain_product``.

    Those genuinely need the remembered subject, so ``explain_product`` must
    stay out of the own-subject set or every elliptical edit breaks.
    """
    assert not names_its_own_subject([("explain_product", {"query": "by region"})])
    assert not names_its_own_subject([])


# --------------------------------------------------------------------------
# One saved connector is not an ambiguous connector
# --------------------------------------------------------------------------

@pytest.fixture()
def one_saved_connector(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(tmp_path / "connectors.json"))
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    monkeypatch.setenv("DATAFLOW_SEED_DEMO", "0")
    db = tmp_path / "only.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, customer TEXT)")
        conn.execute("INSERT INTO orders (id, customer) VALUES (1, 'a')")
        conn.commit()

    from services.connector_store import create_connector

    return create_connector(
        {
            "name": "Only SQLite",
            "type": "sqlite",
            "role": "source",
            "database": str(db),
            "connection_string": str(db),
        }
    )


def test_the_only_connector_is_used_when_none_was_named(one_saved_connector) -> None:
    """Asking "which connector?" when there is exactly one is a dead-end."""
    from src.ai.copilot.schema_tools import _safe_connector

    conn, failure = _safe_connector(tool="schema")
    assert failure is None
    assert conn and conn.get("name") == "Only SQLite"


def test_a_name_that_matches_nothing_still_fails(one_saved_connector) -> None:
    """Auto-pick must not paper over a wrong name — that would answer about the
    wrong database."""
    from src.ai.copilot.schema_tools import _safe_connector

    conn, failure = _safe_connector(name="Warehouse That Is Not Saved", tool="schema")
    assert conn is None
    assert failure is not None and not failure.success


def test_a_data_turn_does_not_capture_the_next_two_questions(
    monkeypatch, tmp_path, one_saved_connector
) -> None:
    """The whole reason both guards exist, measured through the real agent.

    In one session: count rows, then ask a documentation question, then ask a
    platform question. Before the guards the second turn introspected ``orders``
    and the third counted it, so every question asked after a data question was
    answered about the wrong subject.
    """
    monkeypatch.setenv("DATAFLOW_PILOT_MEMORY_PATH", str(tmp_path / "memory.json"))
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")

    from src.ai.copilot.pilot_agent import DataPilotAgent

    agent = DataPilotAgent()
    ctx = {"pilot_session_id": "carryover-test"}

    first = agent.chat("count the rows in orders on Only SQLite", [], data_context=dict(ctx))
    assert "1" in (first.answer or "")

    second = agent.chat("what schema change policies are there", [], data_context=dict(ctx))
    assert "explain_product" in [t.get("name") for t in (second.tools_used or [])]
    assert "orders" not in (second.answer or "").lower()

    third = agent.chat("how many jobs ran today", [], data_context=dict(ctx))
    assert "list_jobs" in [t.get("name") for t in (third.tools_used or [])]

    # The elliptical edit still inherits the remembered subject.
    fourth = agent.chat("and by customer", [], data_context=dict(ctx))
    assert "customer" in (fourth.answer or "").lower()


def test_clarify_examples_never_name_a_table_the_workspace_lacks() -> None:
    """The example used to name a fixture table, telling operators to ask about
    something their workspace does not contain."""
    from src.ai.copilot.example_phrases import example_table_name

    assert example_table_name() == "your_table"
    assert example_table_name({"table": "orders"}) == "orders"
    assert example_table_name({"tables": [{"name": "payments"}]}) == "payments"
