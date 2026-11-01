"""Demand-wave 3 declarative SDK sources: manifest contracts + fixture replays.

Six more high-demand sources (freshservice, close, todoist, typeform,
notion, zoho_crm) ship as honest ``beta`` — certified on synthetic
fixtures, live API runs earn the live flag later. Covers basic-auth
key-as-username (freshservice, close), bearer (todoist, typeform, notion),
POST + request-body cursor pagination (notion), path templating
(typeform responses, notion database_rows) and the OAuth2-refresh auth
bundle (zoho_crm — the token-exchange path itself needs a real tenant).
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from connectors.sdk import get_descriptor
from connectors.sdk.close import CloseSource
from connectors.sdk.freshservice import FreshserviceSource
from connectors.sdk.notion import NotionSource
from connectors.sdk.todoist import TodoistSource
from connectors.sdk.typeform import TypeformSource
from connectors.sdk.zoho_crm import ZohoCrmSource
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures"


def _full_page(count: int, make) -> list[dict]:
    return [make(i) for i in range(1, count + 1)]


def _headers(log_entry) -> dict[str, str]:
    return {key.lower(): value for key, value in log_entry.headers.items()}


def _query(log_entry) -> dict[str, list[str]]:
    return parse_qs(urlsplit(log_entry.target).query)


def _body(log_entry) -> dict:
    return json.loads(log_entry.body or "{}")


# --------------------------------------------------------- freshservice ----


def test_freshservice_manifest_contract() -> None:
    source = FreshserviceSource({"domain": "acme", "api_key": "fs-synthetic"})
    tickets, requesters, groups = source.manifest.streams

    assert source._base_url == "https://acme.freshservice.com/api/v2/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (tickets, requesters, groups):
        assert stream.paginator.type == "page"
        assert stream.paginator.page_param == "page"
        assert stream.paginator.start_index == 1
    assert tickets.cursor is not None
    assert tickets.cursor.request_param == "updated_since"
    assert requesters.cursor is None and groups.cursor is None
    assert get_descriptor("freshservice").catalog_ids == ("freshservice",)


def test_freshservice_tickets_replay_two_pages() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "freshservice" / "tickets_page_2.json").read_text(
            encoding="utf-8"
        )
    )
    page_one = {
        "tickets": _full_page(
            100,
            lambda i: {"id": i, "subject": f"ticket {i}", "updated_at": "2024-01-01T00:00:00Z"},
        )
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/api/v2/tickets",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = FreshserviceSource(
            {"base_url": fixture.base_url + "/api/v2", "domain": "acme", "api_key": "fs-synthetic"}
        )
        records = [r for batch in source.read("tickets") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == 902
    assert len(fixture.request_log) == 2
    assert _headers(fixture.request_log[0])["authorization"].startswith("Basic ")
    assert _query(fixture.request_log[0])["page"] == ["1"]
    assert _query(fixture.request_log[1])["page"] == ["2"]


# ----------------------------------------------------------------- close ----


def test_close_manifest_contract() -> None:
    source = CloseSource({"api_key": "close-synthetic"})
    leads, opportunities, tasks = source.manifest.streams

    assert source._base_url == "https://api.close.com/api/v1/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (leads, opportunities, tasks):
        assert stream.paginator.type == "offset"
        assert stream.paginator.offset_param == "_skip"
        assert stream.paginator.page_size_param == "_limit"
    assert leads.cursor is not None
    assert leads.cursor.request_param == "date_updated__gte"
    assert get_descriptor("close").catalog_ids == ("close",)


def test_close_leads_replay_two_pages_with_skip() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "close" / "leads_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "has_more": True,
        "data": _full_page(
            100,
            lambda i: {
                "id": f"lead_{i:05d}",
                "display_name": f"Lead {i}",
                "date_updated": "2024-01-01T00:00:00+00:00",
            },
        ),
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/lead/",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = CloseSource({"base_url": fixture.base_url, "api_key": "close-synthetic"})
        records = [r for batch in source.read("leads") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "lead_fixture_p2"
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["_skip"] == ["100"]


# --------------------------------------------------------------- todoist ----


def test_todoist_manifest_contract() -> None:
    source = TodoistSource({"api_key": "td-synthetic"})
    tasks, projects, sections = source.manifest.streams

    assert source._base_url == "https://api.todoist.com/rest/v2/"
    assert source.auth.headers == {"Authorization": "Bearer td-synthetic"}
    for stream in (tasks, projects, sections):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "cursor"
        assert stream.paginator.cursor_path == "next_cursor"
        assert stream.records_path == "results"
    # REST v2 has no updated-since filter — honest full-refresh descriptor.
    assert get_descriptor("todoist").sync_modes == ("full_refresh",)


def test_todoist_tasks_follow_next_cursor() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "todoist" / "tasks_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "results": _full_page(
            200,
            lambda i: {"id": str(i), "content": f"task {i}", "is_completed": False},
        ),
        "next_cursor": "cursor-page-2",
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/tasks",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = TodoistSource({"base_url": fixture.base_url, "api_key": "td-synthetic"})
        records = [r for batch in source.read("tasks") for r in batch.records]

    assert len(records) == 201
    assert records[-1]["id"] == "2995104339"
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["cursor"] == ["cursor-page-2"]


# --------------------------------------------------------------- typeform ---


def test_typeform_manifest_contract() -> None:
    source = TypeformSource({"api_key": "tf-synthetic", "form_id": "f1"})
    forms, responses = source.manifest.streams

    assert source._base_url == "https://api.typeform.com/"
    assert source.auth.headers == {"Authorization": "Bearer tf-synthetic"}
    assert forms.paginator.type == "page" and responses.paginator.type == "page"
    assert responses.path == "forms/{form_id}/responses"
    assert responses.cursor is not None
    assert responses.cursor.request_param == "since"
    assert responses.request_params["sort"] == "submitted_at,asc"
    assert get_descriptor("typeform").catalog_ids == ("typeform",)


def test_typeform_forms_replay_and_responses_bind_form_id() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "typeform" / "forms_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "total_items": 201,
        "page_count": 2,
        "items": _full_page(200, lambda i: {"id": f"form{i:04d}", "title": f"F{i}"}),
    }
    resp_page = {
        "total_items": 1,
        "page_count": 1,
        "items": [{"token": "tok1", "submitted_at": "2024-01-02T00:00:00Z"}],
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/forms",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        fixture.add_route(
            "/forms/f1/responses",
            responses=[FixtureResponse(body=resp_page)],
        )
        source = TypeformSource(
            {"base_url": fixture.base_url, "api_key": "tf-synthetic", "form_id": "f1"}
        )
        forms = [r for batch in source.read("forms") for r in batch.records]
        responses = [r for batch in source.read("responses") for r in batch.records]

    assert len(forms) == 201 and forms[-1]["id"] == "fixtureform02"
    assert len(responses) == 1 and responses[0]["token"] == "tok1"
    # The {form_id} path template bound to the configured form.
    assert any(r.target.startswith("/forms/f1/responses") for r in fixture.request_log)


# ----------------------------------------------------------------- notion ---


def test_notion_manifest_contract() -> None:
    source = NotionSource({"api_key": "ntn-synthetic", "database_id": "db1"})
    pages, databases, database_rows = source.manifest.streams

    assert source._base_url == "https://api.notion.com/v1/"
    assert source.auth.headers == {"Authorization": "Bearer ntn-synthetic"}
    for stream in (pages, databases, database_rows):
        assert stream.method == "POST"
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_location == "body"
        assert stream.paginator.cursor_param == "start_cursor"
        assert stream.request_headers["Notion-Version"] == "2022-06-28"
    assert pages.request_body_template["filter"]["value"] == "page"
    assert databases.request_body_template["filter"]["value"] == "database"
    assert database_rows.path == "databases/{database_id}/query"
    assert get_descriptor("notion").catalog_ids == ("notion",)


def test_notion_pages_send_start_cursor_in_body() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "notion" / "pages_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "object": "list",
        "results": _full_page(
            100,
            lambda i: {
                "object": "page",
                "id": f"page-{i:04d}",
                "created_time": "2024-01-01T00:00:00.000Z",
                "last_edited_time": "2024-01-01T00:00:00.000Z",
                "archived": False,
            },
        ),
        "next_cursor": "eyJza2lwIjoyfQ==",
        "has_more": True,
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/search",
            method="POST",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = NotionSource(
            {"base_url": fixture.base_url, "api_key": "ntn-synthetic"}
        )
        records = [r for batch in source.read("pages") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "59833787-2cf9-4fdf-8782-e53db20768a5"
    assert len(fixture.request_log) == 2
    first_body = _body(fixture.request_log[0])
    second_body = _body(fixture.request_log[1])
    assert "start_cursor" not in first_body
    assert first_body["filter"]["value"] == "page"
    assert second_body["start_cursor"] == "eyJza2lwIjoyfQ=="
    assert _headers(fixture.request_log[0])["notion-version"] == "2022-06-28"


# --------------------------------------------------------------- zoho_crm ---


def test_zoho_crm_manifest_contract() -> None:
    source = ZohoCrmSource(
        {
            "client_id": "cid",
            "client_secret": "csec",
            "refresh_token": "rt",
            "access_token": "zoho-synthetic-token",
        }
    )
    leads, contacts, accounts, deals = source.manifest.streams

    assert source._base_url == "https://www.zohoapis.com/crm/v2/"
    # An access_token the tenant already held is used directly — no token
    # round-trip needed to construct the connector (refresh path is live-only).
    assert source.auth.headers == {"Authorization": "Bearer zoho-synthetic-token"}
    for stream in (leads, contacts, accounts, deals):
        assert stream.paginator.type == "page"
        assert stream.paginator.page_param == "page"
        assert stream.records_path == "data"
    assert get_descriptor("zoho_crm").catalog_ids == ("zoho_crm",)


def test_zoho_crm_leads_replay_two_pages() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "zoho_crm" / "leads_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "data": _full_page(
            200,
            lambda i: {
                "id": f"529023900000000{i:04d}",
                "Last_Name": f"Lead{i}",
                "Modified_Time": "2024-01-01T00:00:00+00:00",
            },
        ),
        "info": {"per_page": 200, "count": 200, "page": 1, "more_records": True},
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/Leads",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = ZohoCrmSource(
            {
                "base_url": fixture.base_url,
                "client_id": "cid",
                "client_secret": "csec",
                "refresh_token": "rt",
                "access_token": "zoho-synthetic-token",
            }
        )
        records = [r for batch in source.read("leads") for r in batch.records]

    assert len(records) == 201
    assert records[-1]["id"] == "5290239000000503001"
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["page"] == ["2"]
    assert _headers(fixture.request_log[0])["authorization"] == "Bearer zoho-synthetic-token"
