"""Demand-wave declarative SDK sources: manifest contracts + fixture replays.

Six new high-demand SaaS sources (gitlab, mailchimp, pipedrive, klaviyo,
servicenow, freshdesk) ship as honest ``beta`` — certified on synthetic
fixtures, live API runs earn the live flag later. Each test pins the manifest
contract (auth mode, paginator, cursor semantics) and replays a two-page read
through the fixture server so pagination/auth wiring is proven, not assumed.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from connectors.sdk import get_descriptor
from connectors.sdk.freshdesk import FreshdeskSource
from connectors.sdk.gitlab import GitLabSource
from connectors.sdk.klaviyo import KlaviyoSource
from connectors.sdk.mailchimp import MailchimpSource
from connectors.sdk.pipedrive import PipedriveSource
from connectors.sdk.servicenow import ServiceNowSource
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures"


def _full_page(count: int, make) -> list[dict]:
    return [make(i) for i in range(1, count + 1)]


# ---------------------------------------------------------------- gitlab ----


def test_gitlab_manifest_declares_private_token_link_pagination_and_cursors() -> None:
    source = GitLabSource({"api_key": "synthetic-token"})
    projects, issues, merge_requests = source.manifest.streams

    assert source._base_url == "https://gitlab.com/api/v4/"
    assert source.auth.headers == {"PRIVATE-TOKEN": "synthetic-token"}
    for stream in (projects, issues, merge_requests):
        assert stream.paginator.type == "link_header"
        assert stream.paginator.page_size_param == "per_page"
        assert stream.cursor is not None
        assert stream.cursor.field == "updated_at"
        assert stream.cursor.request_param == "updated_after"
        assert stream.cursor.lookback_s >= 1

    descriptor = get_descriptor("gitlab")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("gitlab",)


def test_gitlab_issues_follow_link_header_and_send_updated_after() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "gitlab" / "issues_page_2.json").read_text(encoding="utf-8")
    )
    page_one = [
        {
            "id": 9010 + i,
            "iid": 30 + i,
            "project_id": 55,
            "title": f"fixture issue {i}",
            "state": "opened",
            "created_at": "2024-01-01T00:00:00.000Z",
            "updated_at": "2024-01-02T00:00:00.000Z",
        }
        for i in (1, 2)
    ]
    link = "</issues?per_page=100&page=2>; rel=\"next\""

    with FixtureServer() as fixture:
        fixture.add_route(
            "/issues",
            responses=[
                FixtureResponse(body=page_one, headers={"Link": link}),
                FixtureResponse(body=page_two),
            ],
        )
        source = GitLabSource(
            {"base_url": fixture.base_url, "api_key": "synthetic-token"}
        )
        records = [
            record
            for batch in source.read("issues")
            for record in batch.records
        ]

    assert [record["id"] for record in records] == [9011, 9012, 9012]
    assert len(fixture.request_log) == 2
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    assert headers["private-token"] == "synthetic-token"


# -------------------------------------------------------------- mailchimp ---


def test_mailchimp_manifest_declares_basic_auth_offset_pagination_and_cursors() -> None:
    source = MailchimpSource({"dc": "us21", "api_key": "key-us21"})
    lists, campaigns, templates = source.manifest.streams

    assert source._base_url == "https://us21.api.mailchimp.com/3.0/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (lists, campaigns, templates):
        assert stream.paginator.type == "offset"
        assert stream.paginator.offset_param == "offset"
        assert stream.paginator.page_size_param == "count"
        assert stream.cursor is not None
        assert stream.cursor.request_param.startswith("since_")

    descriptor = get_descriptor("mailchimp")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("mailchimp",)


def test_mailchimp_lists_page_through_offset_and_key_becomes_password() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "mailchimp" / "lists_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "lists": _full_page(
            100,
            lambda i: {
                "id": f"list-{i}",
                "name": f"fixture list {i}",
                "date_created": "2024-01-01T00:00:00+00:00",
                "stats": {"member_count": i, "unsubscribe_count": 0},
            },
        ),
        "total_items": 101,
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/lists",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = MailchimpSource(
            {
                "base_url": fixture.base_url,
                "dc": "us21",
                "api_key": "key-us21",
            }
        )
        records = [
            record
            for batch in source.read("lists")
            for record in batch.records
        ]

    assert len(records) == 101
    assert records[-1]["id"] == "list-c"
    assert len(fixture.request_log) == 2
    second_query = parse_qs(urlsplit(fixture.request_log[1].target).query)
    assert second_query["offset"] == ["100"]
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    import base64

    decoded = base64.b64decode(headers["authorization"][6:]).decode()
    assert decoded == "datawrap:key-us21"


# -------------------------------------------------------------- pipedrive ---


def test_pipedrive_manifest_declares_query_token_and_next_start_pagination() -> None:
    source = PipedriveSource({"domain": "acme", "api_key": "synthetic-token"})
    deals, persons, organizations, activities = source.manifest.streams

    assert source._base_url == "https://acme.pipedrive.com/api/v1/"
    assert source.auth.params == {"api_token": "synthetic-token"}
    for stream in (deals, persons, organizations, activities):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "start"
        assert stream.paginator.cursor_path == "additional_data.pagination.next_start"
        # Pipedrive v1 has no updated-since filter on entity endpoints —
        # honest full-refresh only.
        assert stream.cursor is None

    descriptor = get_descriptor("pipedrive")
    assert descriptor is not None
    assert descriptor.sync_modes == ("full_refresh",)


def test_pipedrive_deals_follow_next_start_cursor() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "pipedrive" / "deals_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "success": True,
        "data": _full_page(
            100,
            lambda i: {
                "id": i,
                "title": f"fixture deal {i}",
                "value": 100.0,
                "currency": "USD",
                "status": "open",
                "stage_id": 1,
                "add_time": "2024-01-01 10:00:00",
                "update_time": "2024-01-01 10:00:00",
            },
        ),
        "additional_data": {
            "pagination": {
                "start": 0,
                "limit": 100,
                "more_items_in_collection": True,
                "next_start": 100,
            }
        },
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/deals",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = PipedriveSource(
            {
                "base_url": fixture.base_url,
                "domain": "acme",
                "api_key": "synthetic-token",
            }
        )
        records = [
            record
            for batch in source.read("deals")
            for record in batch.records
        ]

    assert len(records) == 101
    assert records[-1]["id"] == 101
    assert len(fixture.request_log) == 2
    second_query = parse_qs(urlsplit(fixture.request_log[1].target).query)
    assert second_query["start"] == ["100"]
    assert second_query["api_token"] == ["synthetic-token"]


# ---------------------------------------------------------------- klaviyo ---


def test_klaviyo_manifest_declares_api_key_header_and_next_link_cursor() -> None:
    source = KlaviyoSource({"api_key": "synthetic-key"})
    lists, profiles, events, campaigns = source.manifest.streams

    assert source._base_url == "https://a.klaviyo.com/api/"
    assert source.auth.headers["Klaviyo-API-Key"] == "synthetic-key"
    for stream in (lists, profiles, events, campaigns):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "page[cursor]"
        assert stream.paginator.cursor_path == "links.next"
        assert stream.request_headers["revision"] == "2024-10-15"
    assert profiles.cursor is not None
    assert profiles.cursor.request_template.startswith("greater-than(updated,")
    assert events.cursor is not None
    assert events.cursor.request_template.startswith("greater-than(timestamp,")

    descriptor = get_descriptor("klaviyo")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("klaviyo",)


def test_klaviyo_lists_pass_next_link_back_as_page_cursor() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "klaviyo" / "lists_page_2.json").read_text(encoding="utf-8")
    )

    with FixtureServer() as fixture:
        next_link = f"{fixture.base_url}/lists?page[cursor]=cursor-token-2"
        page_one = {
            "data": [
                {
                    "id": f"list-{i}",
                    "type": "list",
                    "attributes": {
                        "name": f"fixture list {i}",
                        "created": "2024-01-01T00:00:00+00:00",
                        "updated": "2024-01-01T00:00:00+00:00",
                    },
                }
                for i in ("a", "b")
            ],
            "links": {"self": f"{fixture.base_url}/lists", "next": next_link},
        }
        fixture.add_route(
            "/lists",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = KlaviyoSource(
            {"base_url": fixture.base_url, "api_key": "synthetic-key"}
        )
        records = [
            record
            for batch in source.read("lists")
            for record in batch.records
        ]

    assert [record["id"] for record in records] == ["list-a", "list-b", "list-c"]
    assert len(fixture.request_log) == 2
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    assert headers["klaviyo-api-key"] == "synthetic-key"


# ------------------------------------------------------------- servicenow ---


def test_servicenow_manifest_declares_basic_auth_sysparm_pagination() -> None:
    source = ServiceNowSource(
        {"instance": "dev123", "username": "etl", "password": "synthetic"}
    )
    incident, change_request, sys_user = source.manifest.streams

    assert source._base_url == "https://dev123.service-now.com/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (incident, change_request, sys_user):
        assert stream.paginator.type == "offset"
        assert stream.paginator.offset_param == "sysparm_offset"
        assert stream.paginator.page_size_param == "sysparm_limit"
        assert stream.cursor is not None
        assert stream.cursor.request_param == "sysparm_query"
        assert "sys_updated_on>=" in stream.cursor.request_template
        assert stream.cursor.lookback_s >= 60

    descriptor = get_descriptor("servicenow")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("servicenow",)


def test_servicenow_incident_pages_via_sysparm_offset() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "servicenow" / "incident_page_2.json").read_text(
            encoding="utf-8"
        )
    )
    page_one = {
        "result": _full_page(
            100,
            lambda i: {
                "sys_id": f"inc-{i}",
                "number": f"INC0010{i:03d}",
                "short_description": f"fixture incident {i}",
                "state": "1",
                "priority": "2",
                "sys_created_on": "2024-01-01 08:00:00",
                "sys_updated_on": "2024-01-01 08:00:00",
            },
        )
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/api/now/table/incident",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = ServiceNowSource(
            {
                "base_url": fixture.base_url,
                "instance": "dev123",
                "username": "etl",
                "password": "synthetic",
            }
        )
        records = [
            record
            for batch in source.read("incident")
            for record in batch.records
        ]

    assert len(records) == 101
    assert records[-1]["sys_id"] == "inc-101"
    assert len(fixture.request_log) == 2
    second_query = parse_qs(urlsplit(fixture.request_log[1].target).query)
    assert second_query["sysparm_offset"] == ["100"]
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    import base64

    decoded = base64.b64decode(headers["authorization"][6:]).decode()
    assert decoded == "etl:synthetic"


# -------------------------------------------------------------- freshdesk ---


def test_freshdesk_manifest_declares_api_key_basic_auth_page_pagination() -> None:
    source = FreshdeskSource({"domain": "acme", "api_key": "synthetic-key"})
    tickets, contacts, companies = source.manifest.streams

    assert source._base_url == "https://acme.freshdesk.com/api/v2/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (tickets, contacts, companies):
        assert stream.paginator.type == "page"
        assert stream.paginator.page_param == "page"
        assert stream.paginator.page_size_param == "per_page"
    assert tickets.cursor is not None
    assert tickets.cursor.field == "updated_at"
    assert tickets.cursor.request_param == "updated_since"
    assert tickets.cursor.lookback_s >= 60

    descriptor = get_descriptor("freshdesk")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("freshdesk",)


def test_freshdesk_tickets_page_and_send_api_key_as_username() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "freshdesk" / "tickets_page_2.json").read_text(
            encoding="utf-8"
        )
    )
    page_one = _full_page(
        100,
        lambda i: {
            "id": i,
            "subject": f"fixture ticket {i}",
            "status": 2,
            "priority": 1,
            "requester_id": 50 + i,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        },
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/tickets",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = FreshdeskSource(
            {
                "base_url": fixture.base_url,
                "domain": "acme",
                "api_key": "synthetic-key",
            }
        )
        records = [
            record
            for batch in source.read("tickets")
            for record in batch.records
        ]

    assert len(records) == 101
    assert records[-1]["id"] == 101
    assert len(fixture.request_log) == 2
    second_query = parse_qs(urlsplit(fixture.request_log[1].target).query)
    assert second_query["page"] == ["2"]
    assert second_query["per_page"] == ["100"]
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    import base64

    decoded = base64.b64decode(headers["authorization"][6:]).decode()
    # Freshdesk convention: API key is the basic-auth username, password is X.
    assert decoded == "synthetic-key:X"
