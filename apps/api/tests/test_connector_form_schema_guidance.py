"""Pilot connect procedures name the fields the New connection form shows.

The field list is not hand-written: ``data/connector_form_schema.json`` is
exported from ``apps/web/src/lib/connectorFormConfig.ts`` (the form owner), so
these tests pin the contract between that export and the generated cards.
"""

from __future__ import annotations

import json

import pytest

from services.connector_form_schema import (
    SCHEMA_PATH,
    describe_connector_fields,
    get_connector_form,
    list_connector_form_types,
)
from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
from src.ai.rag.product_facts import _connect_engine_sections


def _answer(question: str) -> str:
    answer = retrieve_product_answer(question, limit=5)
    return " ".join((compose_product_answer(answer) or "").split()).lower()


def _card(engine_name: str) -> str:
    for section in _connect_engine_sections():
        if section.section_title == f"Procedure: connect a {engine_name} database":
            return section.text
    raise AssertionError(f"no procedure card for {engine_name}")


def test_export_is_present_and_covers_every_transfer_ready_engine() -> None:
    assert SCHEMA_PATH.exists(), "run `npx tsx scripts/export_connector_form_schema.ts` in apps/web"
    types = set(list_connector_form_types())
    for engine in ("postgresql", "sqlserver", "mysql", "oracle", "mongodb", "snowflake", "bigquery", "redis"):
        assert engine in types, engine


def test_export_holds_metadata_only() -> None:
    raw = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    allowed = {"key", "label", "type", "optional", "sensitive", "hint", "placeholder"}
    for entry in raw.values():
        for mode in entry["auth_modes"]:
            for field in mode["fields"]:
                assert set(field) <= allowed, field
                if field["sensitive"]:
                    assert "placeholder" not in field or not field["placeholder"].startswith("sk-")


@pytest.mark.parametrize(
    "engine,expected",
    [
        ("Snowflake", ("account host", "warehouse", "schema", "role", "programmatic access token", "key-pair")),
        ("BigQuery", ("service account json", "gcp project id", "dataset")),
        ("MongoDB", ("auth source", "27017", "connection string")),
        ("PostgreSQL", ("host", "5432", "database", "username", "password", "ssl")),
        ("MySQL", ("3306", "connection string")),
        ("SQL Server", ("1433",)),
        ("Redis", ("database index", "username (acl)")),
    ],
)
def test_procedure_card_names_engine_specific_fields(engine: str, expected: tuple[str, ...]) -> None:
    card = _card(engine).lower()
    assert card.startswith(f"click new connection and pick the {engine.lower()} driver")
    for token in expected:
        assert token in card, (token, card)
    assert "click test, and save" in card


def test_snowflake_setup_notes_come_from_the_ui_guide() -> None:
    titles = {s.section_title: s.text for s in _connect_engine_sections()}
    notes = titles["Snowflake connection setup notes"]
    assert "account identifier" in notes.lower()
    assert "250001" in notes
    assert "PostgreSQL connection setup notes" not in titles, "generic guide must not masquerade as engine notes"


def test_describe_fields_orders_required_optional_toggles() -> None:
    form = get_connector_form("snowflake")
    assert form is not None and form.default_mode is not None
    text = describe_connector_fields(form.default_mode)
    assert text.startswith("Enter Account host")
    assert text.index("Warehouse") < text.index("Role is optional")
    mongo = get_connector_form("mongodb")
    assert mongo is not None and mongo.default_mode is not None
    mongo_text = describe_connector_fields(mongo.default_mode)
    assert mongo_text.index("Auth source") < mongo_text.index("Enable Use TLS / SSL")
    assert "MongoDB Atlas" in mongo_text


@pytest.mark.parametrize(
    "question,tokens",
    [
        ("how do i connect to snowflake", ("warehouse", "account host")),
        ("what fields do i need to connect bigquery", ("service account", "project")),
        ("how do i connect to mongodb", ("auth source",)),
        ("which port does sql server use when i add a connection", ("1433",)),
    ],
)
def test_pilot_answers_name_the_fields(question: str, tokens: tuple[str, ...]) -> None:
    body = _answer(question)
    assert "new connection" in body, body
    for token in tokens:
        assert token in body, (token, body)
