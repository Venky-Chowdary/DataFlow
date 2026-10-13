from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from connectors.sdk import get_descriptor
from connectors.sdk.intercom import IntercomSource
from tests.connector_certification.fixture_server import FixtureResponse, FixtureServer

_FIXTURE_DIR = (
    Path(__file__).parent / "connector_certification" / "fixtures" / "intercom"
)


def test_intercom_manifest_declares_versioned_auth_and_sync_contracts() -> None:
    source = IntercomSource({"access_token": "synthetic-token"})
    contacts, conversations = source.manifest.streams

    assert source._base_url == "https://api.intercom.io/"
    assert source.auth.headers["Authorization"] == "Bearer synthetic-token"
    assert contacts.path == "contacts"
    assert contacts.paginator.page_size == 150
    assert contacts.paginator.page_size_param == "per_page"
    assert contacts.paginator.cursor_path == "pages.next.starting_after"
    assert contacts.request_headers["Intercom-Version"] == "2.11"
    assert conversations.path == "conversations/search"
    assert conversations.method == "POST"
    assert conversations.cursor is not None
    assert conversations.cursor.field == "updated_at"
    assert conversations.cursor.request_param == "query.value"
    assert conversations.cursor.request_location == "body"
    assert conversations.cursor.format == "epoch_s"
    assert conversations.cursor.lookback_s == 1
    assert conversations.paginator.cursor_location == "body"
    assert conversations.paginator.cursor_param == "pagination.starting_after"
    assert conversations.paginator.cursor_path == "pages.next.starting_after"
    assert conversations.paginator.page_size == 150
    assert conversations.paginator.page_size_param == ""
    assert conversations.request_headers["Intercom-Version"] == "2.11"

    descriptor = get_descriptor("intercom")
    assert descriptor is not None
    assert "full_refresh" in descriptor.sync_modes
    assert "incremental" in descriptor.sync_modes
    assert (
        "certified as SDK source on synthetic fixtures; not yet wired into the transfer engine"
        in descriptor.description
    )


def test_intercom_contacts_follow_starting_after_with_required_headers() -> None:
    page_one = json.loads(
        (_FIXTURE_DIR / "contacts_page_1.json").read_text(encoding="utf-8")
    )
    page_two = json.loads(
        (_FIXTURE_DIR / "contacts_page_2.json").read_text(encoding="utf-8")
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/contacts",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = IntercomSource(
            {
                "base_url": fixture.base_url,
                "access_token": "synthetic-token",
            }
        )
        batches = list(source.read("contacts"))

    records = [record for batch in batches for record in batch.records]
    assert [record["id"] for record in records] == ["4001", "4002", "4003"]
    assert len(fixture.request_log) == 2
    assert all(request.method == "GET" for request in fixture.request_log)
    assert [
        parse_qs(urlsplit(request.target).query)
        for request in fixture.request_log
    ] == [
        {"per_page": ["150"]},
        {"per_page": ["150"], "starting_after": ["synthetic-contact-page-two"]},
    ]
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    assert headers["authorization"] == "Bearer synthetic-token"
    assert headers["intercom-version"] == "2.11"


def test_intercom_conversation_search_uses_post_cursor_body_and_overlap() -> None:
    page_one = json.loads(
        (_FIXTURE_DIR / "conversations_page_1.json").read_text(encoding="utf-8")
    )
    page_two = json.loads(
        (_FIXTURE_DIR / "conversations_page_2.json").read_text(encoding="utf-8")
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/conversations/search",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
            method="POST",
        )
        source = IntercomSource(
            {
                "base_url": fixture.base_url,
                "access_token": "synthetic-token",
            }
        )
        batches = list(source.read("conversations"))

    records = [record for batch in batches for record in batch.records]
    assert [record["id"] for record in records] == ["3001", "3002", "3003", "3004"]
    assert len(fixture.request_log) == 2
    first_body = json.loads(fixture.request_log[0].body)
    second_body = json.loads(fixture.request_log[1].body)
    assert first_body["query"] == {
        "field": "updated_at",
        "operator": ">",
        "value": 0,
    }
    assert first_body["pagination"] == {"per_page": 150}
    assert second_body["pagination"] == {
        "per_page": 150,
        "starting_after": "synthetic-conversation-page-two",
    }
    headers = {
        key.lower(): value for key, value in fixture.request_log[0].headers.items()
    }
    assert fixture.request_log[0].method == "POST"
    assert headers["authorization"] == "Bearer synthetic-token"
    assert headers["intercom-version"] == "2.11"


def test_intercom_incremental_conversation_query_is_epoch_seconds_with_overlap() -> None:
    with FixtureServer() as fixture:
        fixture.add_route(
            "/conversations/search",
            FixtureResponse(
                body={
                    "conversations": [
                        {
                            "id": "3005",
                            "updated_at": 1767226000,
                            "created_at": 1767225900,
                            "title": "Synthetic Incremental Conversation",
                            "state": "open",
                        }
                    ]
                }
            ),
            method="POST",
        )
        source = IntercomSource(
            {
                "base_url": fixture.base_url,
                "access_token": "synthetic-token",
            }
        )
        list(source.read("conversations", state={"cursor": 1767226000}))

    request_body = json.loads(fixture.request_log[0].body)
    assert request_body["query"]["value"] == 1767225999
    assert fixture.request_log[0].method == "POST"
