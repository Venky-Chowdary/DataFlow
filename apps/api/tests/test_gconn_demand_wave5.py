"""Demand-wave 5: Airtable, ClickUp, Mailjet, SendGrid SDK sources.

Airtable exercises the body-token cursor paginator (response ``offset``
field fed back as the ``offset`` query param); ClickUp uses raw
``Authorization: pk_`` api_key auth with page-index pagination; Mailjet
uses api-key + secret basic auth over offset pagination; SendGrid is a
simple bearer full-refresh set. Same completeness bar as waves 1-4.
"""

from __future__ import annotations

import json
from pathlib import Path

from connectors.sdk import get_descriptor
from connectors.sdk.airtable import AirtableSource
from connectors.sdk.clickup import ClickUpSource
from connectors.sdk.mailjet import MailjetSource
from connectors.sdk.sendgrid import SendGridSource
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures"


def _headers(log_entry) -> dict[str, str]:
    return {key.lower(): value for key, value in log_entry.headers.items()}


# --------------------------------------------------------------- airtable ---


def test_airtable_manifest_contract() -> None:
    source = AirtableSource(
        {"api_key": "pat_synthetic", "base_id": "appXYZ", "table_id": "tblABC"}
    )
    records, tables = source.manifest.streams

    assert source._base_url == "https://api.airtable.com/"
    assert source.auth.headers == {"Authorization": "Bearer pat_synthetic"}
    assert records.path == "v0/{base_id}/{table_id}"
    assert records.paginator.type == "cursor"
    assert records.paginator.cursor_param == "offset"
    assert records.paginator.cursor_path == "offset"
    assert tables.path == "v0/meta/bases/{base_id}/tables"
    assert get_descriptor("airtable").catalog_ids == ("airtable",)


def test_airtable_records_follow_offset_token() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "airtable" / "records_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "records": [
            {"id": f"rec{i:04d}", "createdTime": "2024-01-01T00:00:00Z", "fields": {}}
            for i in range(1, 101)
        ],
        "offset": "itrtoken-page-two",
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v0/appXYZ/tblABC",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = AirtableSource(
            {
                "base_url": fixture.base_url,
                "api_key": "pat_synthetic",
                "base_id": "appXYZ",
                "table_id": "tblABC",
            }
        )
        records = [r for batch in source.read("records") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "recFixturePageTwo"
    assert len(fixture.request_log) == 2
    assert "offset=itrtoken-page-two" in fixture.request_log[1].target


# ---------------------------------------------------------------- clickup ---


def test_clickup_manifest_contract() -> None:
    source = ClickUpSource({"api_key": "pk_synthetic", "list_id": "9001"})
    tasks, teams = source.manifest.streams

    assert source._base_url == "https://api.clickup.com/api/v2/"
    # Raw pk_ token — no Bearer prefix.
    assert source.auth.headers == {"Authorization": "pk_synthetic"}
    assert tasks.path == "list/{list_id}/task"
    assert tasks.paginator.type == "page"
    assert get_descriptor("clickup").catalog_ids == ("clickup",)


def test_clickup_tasks_page_pagination() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "clickup" / "tasks_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "tasks": [
            {"id": f"task-{i:04d}", "name": f"task {i}", "date_created": "1"}
            for i in range(1, 101)
        ],
        "last_page": False,
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/list/9001/task",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = ClickUpSource(
            {
                "base_url": fixture.base_url,
                "api_key": "pk_synthetic",
                "list_id": "9001",
            }
        )
        records = [r for batch in source.read("tasks") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "fixture-task-p2"
    assert len(fixture.request_log) == 2
    assert _headers(fixture.request_log[0])["authorization"] == "pk_synthetic"


# ---------------------------------------------------------------- mailjet ---


def test_mailjet_manifest_contract() -> None:
    source = MailjetSource({"api_key": "pub_synthetic", "secret_key": "sec_synthetic"})
    contacts, messages = source.manifest.streams

    assert source._base_url == "https://api.mailjet.com/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (contacts, messages):
        assert stream.paginator.type == "offset"
        assert stream.records_path == "Data"
    assert get_descriptor("mailjet").catalog_ids == ("mailjet",)


def test_mailjet_contacts_offset_pagination() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "mailjet" / "contacts_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "Count": 100,
        "Total": 101,
        "Data": [
            {"ID": i, "Email": f"user{i}@example.com"}
            for i in range(1, 101)
        ],
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v3/REST/contact",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = MailjetSource(
            {
                "base_url": fixture.base_url,
                "api_key": "pub_synthetic",
                "secret_key": "sec_synthetic",
            }
        )
        records = [r for batch in source.read("contacts") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["ID"] == 9001
    assert len(fixture.request_log) == 2
    assert "offset=100" in fixture.request_log[1].target
    assert _headers(fixture.request_log[0])["authorization"].startswith("Basic ")


# --------------------------------------------------------------- sendgrid ---


def test_sendgrid_manifest_contract() -> None:
    source = SendGridSource({"api_key": "SG.synthetic"})
    lists, segments, templates = source.manifest.streams

    assert source._base_url == "https://api.sendgrid.com/"
    assert source.auth.headers == {"Authorization": "Bearer SG.synthetic"}
    assert lists.path == "v3/contactdb/lists"
    assert segments.path == "v3/contactdb/segments"
    assert templates.path == "v3/templates"
    assert get_descriptor("sendgrid").catalog_ids == ("sendgrid",)


def test_sendgrid_lists_full_refresh() -> None:
    body = json.loads(
        (_FIXTURE_DIR / "sendgrid" / "lists.json").read_text(encoding="utf-8")
    )
    with FixtureServer() as fixture:
        fixture.add_route(
            "/v3/contactdb/lists",
            responses=[FixtureResponse(body=body)],
        )
        source = SendGridSource(
            {"base_url": fixture.base_url, "api_key": "SG.synthetic"}
        )
        records = [r for batch in source.read("lists") for r in batch.records]

    assert len(records) == 1
    assert records[0]["id"] == 5001
    assert len(fixture.request_log) == 1
    assert _headers(fixture.request_log[0])["authorization"] == "Bearer SG.synthetic"
