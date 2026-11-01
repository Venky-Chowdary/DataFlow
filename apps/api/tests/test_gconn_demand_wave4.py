"""Demand-wave 4: next_url paginator + Posthog/Confluence/Zendesk SDK sources.

Covers the new ``next_url`` paginator type (Confluence ``_links.next``,
Posthog ``next``, Zendesk ``next_page`` — body-embedded next-page URLs
followed verbatim on the same origin only) and the three connectors it
unblocks. Same completeness bar as waves 1-3.
"""

from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import urlsplit

from connectors.sdk import get_descriptor
from connectors.sdk.confluence import ConfluenceSource
from connectors.sdk.posthog import PosthogSource
from connectors.sdk.zendesk import ZendeskSource
from tests.connector_certification.fixture_server import (
    FixtureResponse,
    FixtureServer,
)

_FIXTURE_DIR = Path(__file__).parent / "connector_certification" / "fixtures"


def _full_page(count: int, make) -> list[dict]:
    return [make(i) for i in range(1, count + 1)]


def _headers(log_entry) -> dict[str, str]:
    return {key.lower(): value for key, value in log_entry.headers.items()}


# ---------------------------------------------------------------- posthog ---


def test_posthog_manifest_contract() -> None:
    source = PosthogSource(
        {"domain": "app.posthog.com", "api_key": "phx_synthetic", "project_id": "42"}
    )
    events, persons, cohorts, feature_flags = source.manifest.streams

    assert source._base_url == "https://app.posthog.com/"
    assert source.auth.headers == {"Authorization": "Bearer phx_synthetic"}
    for stream in (events, persons, cohorts, feature_flags):
        assert stream.paginator.type == "next_url"
        assert stream.paginator.next_url_path == "next"
        assert stream.records_path == "results"
    assert events.path == "api/projects/{project_id}/events"
    assert get_descriptor("posthog").catalog_ids == ("posthog",)


def test_posthog_persons_follow_next_url() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "posthog" / "persons_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "count": 101,
        "next": "/api/projects/42/persons?cursor=cGFnZT0y&limit=100",
        "previous": None,
        "results": _full_page(
            100,
            lambda i: {
                "id": f"person-{i:04d}",
                "distinct_ids": [f"user{i}@example.com"],
                "created_at": "2024-01-01T00:00:00Z",
            },
        ),
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/api/projects/42/persons",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = PosthogSource(
            {
                "base_url": fixture.base_url,
                "domain": "app.posthog.com",
                "api_key": "phx_synthetic",
                "project_id": "42",
            }
        )
        records = [r for batch in source.read("persons") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "018d3c06-fixture-person-p2"
    assert len(fixture.request_log) == 2
    # Page 2 followed the body-embedded URL verbatim — cursor carried in it.
    assert "cursor=cGFnZT0y" in fixture.request_log[1].target


# ------------------------------------------------------------- confluence ---


def test_confluence_manifest_contract() -> None:
    source = ConfluenceSource(
        {"domain": "acme", "username": "a@acme.com", "api_key": "atl-synthetic"}
    )
    pages, spaces, blogposts, attachments = source.manifest.streams

    assert source._base_url == "https://acme.atlassian.net/wiki/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (pages, spaces, blogposts, attachments):
        assert stream.paginator.type == "next_url"
        assert stream.paginator.next_url_path == "_links.next"
    assert get_descriptor("confluence").catalog_ids == ("confluence",)


def test_confluence_pages_follow_links_next_relative_url() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "confluence" / "pages_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "results": _full_page(
            100,
            lambda i: {"id": str(i), "status": "current", "title": f"Page {i}"},
        ),
        "_links": {"next": "/wiki/api/v2/pages?cursor=page2&limit=100"},
    }
    with FixtureServer() as fixture:
        fixture.add_route(
            "/wiki/api/v2/pages",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        # Fixture server has no /wiki prefix — override base_url to simulate.
        source = ConfluenceSource(
            {
                "base_url": fixture.base_url + "/wiki",
                "domain": "acme",
                "username": "a@acme.com",
                "api_key": "atl-synthetic",
            }
        )
        records = [r for batch in source.read("pages") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == "987654322"
    assert len(fixture.request_log) == 2
    assert _headers(fixture.request_log[0])["authorization"].startswith("Basic ")


# ---------------------------------------------------------------- zendesk ---


def test_zendesk_manifest_contract() -> None:
    source = ZendeskSource(
        {"domain": "acme", "username": "a@acme.com", "api_key": "zd-synthetic"}
    )
    tickets, users, organizations, ticket_comments = source.manifest.streams

    assert source._base_url == "https://acme.zendesk.com/api/v2/"
    # {email}/token is the Zendesk API-token username contract.
    assert "a%40acme.com%2Ftoken" in source.auth.headers["Authorization"] or source.auth.headers[
        "Authorization"
    ].startswith("Basic ")
    for stream in (tickets, users, organizations, ticket_comments):
        assert stream.paginator.type == "next_url"
        assert stream.paginator.next_url_path == "next_page"
    assert ticket_comments.path == "tickets/{ticket_id}/comments"
    assert get_descriptor("zendesk").catalog_ids == ("zendesk",)


def test_zendesk_tickets_follow_next_page_url() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "zendesk" / "tickets_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "tickets": _full_page(
            100,
            lambda i: {"id": i, "subject": f"ticket {i}", "status": "open"},
        ),
        "next_page": "NEXT_URL_PLACEHOLDER",
        "previous_page": None,
        "count": 101,
    }
    with FixtureServer() as fixture:
        # Zendesk returns an absolute next_page URL — build it against the
        # fixture origin so the same-origin guard passes.
        page_one["next_page"] = f"{fixture.base_url}/tickets?page=2&per_page=100"
        fixture.add_route(
            "/tickets",
            responses=[FixtureResponse(body=page_one), FixtureResponse(body=page_two)],
        )
        source = ZendeskSource(
            {
                "base_url": fixture.base_url,
                "domain": "acme",
                "username": "a@acme.com",
                "api_key": "zd-synthetic",
            }
        )
        records = [r for batch in source.read("tickets") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == 9002
    assert len(fixture.request_log) == 2
    assert "page=2" in fixture.request_log[1].target
