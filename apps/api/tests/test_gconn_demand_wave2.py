"""Demand-wave 2 declarative SDK sources: manifest contracts + fixture replays.

Six more high-demand SaaS/e-commerce sources (slack, asana, calendly,
woocommerce, bigcommerce, brevo) ship as honest ``beta`` — certified on
synthetic fixtures, live API runs earn the live flag later. Each test pins
the manifest contract (auth mode, paginator, cursor semantics) and replays a
two-page read through the fixture server, including the ``{config}``
request_params templating that slack/asana/calendly rely on.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

from connectors.sdk import get_descriptor
from connectors.sdk.asana import AsanaSource
from connectors.sdk.bigcommerce import BigCommerceSource
from connectors.sdk.brevo import BrevoSource
from connectors.sdk.calendly import CalendlySource
from connectors.sdk.declarative.errors import ManifestError
from connectors.sdk.slack import SlackSource
from connectors.sdk.woocommerce import WooCommerceSource
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


# ----------------------------------------------------------------- slack ----


def test_slack_manifest_declares_bearer_cursor_pagination_and_oldest_cursor() -> None:
    source = SlackSource({"api_key": "xoxb-synthetic"})
    users, channels, messages = source.manifest.streams

    assert source._base_url == "https://slack.com/api/"
    assert source.auth.headers == {"Authorization": "Bearer xoxb-synthetic"}
    for stream in (users, channels, messages):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "cursor"
        assert stream.paginator.cursor_path == "response_metadata.next_cursor"
    # Only conversations.history has a bounded lookback window; users and
    # channels re-read in full.
    assert users.cursor is None and channels.cursor is None
    assert messages.cursor is not None
    assert messages.cursor.field == "ts"
    assert messages.cursor.request_param == "oldest"
    assert messages.cursor.lookback_s >= 1
    assert messages.request_params["channel"] == "{channel}"

    descriptor = get_descriptor("slack")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("slack",)


def test_slack_users_follow_next_cursor_and_messages_send_channel() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "slack" / "users_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "ok": True,
        "members": _full_page(
            200,
            lambda i: {
                "id": f"U{i:06d}",
                "name": f"fixture.user.{i}",
                "is_bot": False,
                "deleted": False,
                "updated": 1712340000.0 + i,
            },
        ),
        "response_metadata": {"next_cursor": "dGVzdDpjdXJzb3Iy"},
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/users.list",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = SlackSource({"base_url": fixture.base_url, "api_key": "xoxb-synthetic"})
        records = [r for batch in source.read("users") for r in batch.records]

    assert len(records) == 201
    assert records[-1]["id"] == "U0PAGE2"
    assert len(fixture.request_log) == 2
    assert _headers(fixture.request_log[0])["authorization"] == "Bearer xoxb-synthetic"
    assert _query(fixture.request_log[1])["cursor"] == ["dGVzdDpjdXJzb3Iy"]

    # Missing channel config must fail fast (no silent full-workspace crawl).
    with FixtureServer() as fixture:
        source = SlackSource({"base_url": fixture.base_url, "api_key": "xoxb-synthetic"})
        with pytest.raises(ManifestError):
            list(source.read("messages"))
    assert fixture.request_log == []

    with FixtureServer() as fixture:
        fixture.add_route(
            "/conversations.history",
            responses=[FixtureResponse(body={"ok": True, "messages": [], "response_metadata": {"next_cursor": ""}})],
        )
        source = SlackSource(
            {"base_url": fixture.base_url, "api_key": "xoxb-synthetic", "channel": "C0FIXTURE"}
        )
        list(source.read("messages"))
    assert _query(fixture.request_log[0])["channel"] == ["C0FIXTURE"]


# ----------------------------------------------------------------- asana ----


def test_asana_manifest_declares_bearer_offset_token_pagination_and_workspace() -> None:
    source = AsanaSource({"api_key": "asana-synthetic", "workspace": "ws-1"})
    workspaces, users, projects, tasks = source.manifest.streams

    assert source._base_url == "https://app.asana.com/api/1.0/"
    assert source.auth.headers == {"Authorization": "Bearer asana-synthetic"}
    for stream in (workspaces, users, projects, tasks):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "offset"
        assert stream.paginator.cursor_path == "next_page.offset"
        assert stream.records_path == "data"
    assert tasks.cursor is not None
    assert tasks.cursor.field == "modified_at"
    assert tasks.cursor.request_param == "modified_since"
    for stream in (users, projects, tasks):
        assert stream.request_params["workspace"] == "{workspace}"

    descriptor = get_descriptor("asana")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("asana",)


def test_asana_tasks_send_workspace_and_modified_since() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "asana" / "tasks_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "data": _full_page(
            100,
            lambda i: {
                "gid": f"110{i:03d}",
                "name": f"fixture task {i}",
                "completed": False,
                "modified_at": "2024-01-02T00:00:00.000Z",
                "resource_type": "task",
            },
        ),
        "next_page": {"offset": "opaque-offset-token-2"},
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/tasks",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = AsanaSource(
            {"base_url": fixture.base_url, "api_key": "asana-synthetic", "workspace": "ws-9"}
        )
        records = [r for batch in source.read("tasks") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["gid"] == "1200PAGE2"
    assert len(fixture.request_log) == 2
    first = _query(fixture.request_log[0])
    assert first["workspace"] == ["ws-9"]
    assert _query(fixture.request_log[1])["offset"] == ["opaque-offset-token-2"]
    assert _headers(fixture.request_log[0])["authorization"] == "Bearer asana-synthetic"


# -------------------------------------------------------------- calendly ----


def test_calendly_manifest_declares_page_token_pagination_and_min_start_time() -> None:
    source = CalendlySource(
        {"api_key": "cal-synthetic", "user_uri": "https://api.calendly.com/users/U1"}
    )
    event_types, scheduled_events = source.manifest.streams

    assert source._base_url == "https://api.calendly.com/"
    assert source.auth.headers == {"Authorization": "Bearer cal-synthetic"}
    for stream in (event_types, scheduled_events):
        assert stream.paginator.type == "cursor"
        assert stream.paginator.cursor_param == "page_token"
        assert stream.paginator.cursor_path == "pagination.next_page_token"
        assert stream.records_path == "collection"
        assert stream.request_params["user"] == "{user_uri}"
    assert scheduled_events.cursor is not None
    assert scheduled_events.cursor.request_param == "min_start_time"
    assert scheduled_events.cursor.lookback_s >= 60

    descriptor = get_descriptor("calendly")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("calendly",)


def test_calendly_events_send_user_uri_unencoded_and_follow_page_token() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "calendly" / "scheduled_events_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "collection": _full_page(
            100,
            lambda i: {
                "uri": f"https://api.calendly.com/scheduled_events/EVT-{i:04d}",
                "name": f"fixture event {i}",
                "status": "active",
                "start_time": "2024-01-01T10:00:00.000Z",
                "end_time": "2024-01-01T10:30:00.000Z",
                "created_at": "2024-01-01T00:00:00.000Z",
            },
        ),
        "pagination": {"count": 100, "next_page_token": "tok-page-2"},
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/scheduled_events",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = CalendlySource(
            {
                "base_url": fixture.base_url,
                "api_key": "cal-synthetic",
                "user_uri": "https://api.calendly.com/users/U-ME",
            }
        )
        records = [r for batch in source.read("scheduled_events") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["uri"].endswith("EVT-PAGE2")
    assert len(fixture.request_log) == 2
    # The user URI must arrive decoded once (not double-encoded) in the query.
    first = _query(fixture.request_log[0])
    assert first["user"] == ["https://api.calendly.com/users/U-ME"]
    assert _query(fixture.request_log[1])["page_token"] == ["tok-page-2"]


# ------------------------------------------------------------ woocommerce ---


def test_woocommerce_manifest_declares_basic_auth_page_pagination_and_modified_after() -> None:
    source = WooCommerceSource(
        {"domain": "shop.example.com", "username": "ck_x", "password": "cs_y"}
    )
    orders, products, customers = source.manifest.streams

    assert source._base_url == "https://shop.example.com/wp-json/wc/v3/"
    assert source.auth.headers["Authorization"].startswith("Basic ")
    for stream in (orders, products, customers):
        assert stream.paginator.type == "page"
        assert stream.paginator.page_param == "page"
        assert stream.paginator.page_size_param == "per_page"
        assert stream.paginator.start_index == 1
    assert orders.cursor is not None and products.cursor is not None
    assert orders.cursor.request_param == "modified_after"
    # v3 customers supports the same modified_after filter as orders/products.
    assert customers.cursor is not None
    assert customers.cursor.request_param == "modified_after"

    descriptor = get_descriptor("woocommerce")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("woocommerce",)


def test_woocommerce_orders_page_through_and_send_consumer_credentials() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "woocommerce" / "orders_page_2.json").read_text(encoding="utf-8")
    )
    page_one = _full_page(
        100,
        lambda i: {
            "id": 4100 + i,
            "number": str(4100 + i),
            "status": "processing",
            "total": "10.00",
            "currency": "USD",
            "date_created": "2024-01-01T00:00:00",
            "date_modified": "2024-01-01T01:00:00",
        },
    )

    with FixtureServer() as fixture:
        fixture.add_route(
            "/orders",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = WooCommerceSource(
            {
                "base_url": fixture.base_url,
                "domain": "shop.example.com",
                "username": "ck_consumer",
                "password": "cs_secret",
            }
        )
        records = [r for batch in source.read("orders") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == 4202
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["page"] == ["2"]
    decoded = base64.b64decode(_headers(fixture.request_log[0])["authorization"][6:]).decode()
    assert decoded == "ck_consumer:cs_secret"


# ------------------------------------------------------------ bigcommerce ---


def test_bigcommerce_manifest_declares_x_auth_token_and_min_filters() -> None:
    source = BigCommerceSource({"store_hash": "abc123", "api_key": "bc-synthetic"})
    products, customers, orders = source.manifest.streams

    assert source._base_url == "https://api.bigcommerce.com/stores/abc123/"
    assert source.auth.headers == {"X-Auth-Token": "bc-synthetic"}
    for stream in (products, customers, orders):
        assert stream.paginator.type == "page"
        assert stream.paginator.page_param == "page"
        assert stream.paginator.page_size_param == "limit"
        assert stream.cursor is not None
        assert stream.cursor.field == "date_modified"
    assert products.cursor.request_param == "date_modified:min"
    assert orders.cursor.request_param == "min_date_modified"
    assert orders.path == "v2/orders" and products.path == "v3/catalog/products"

    descriptor = get_descriptor("bigcommerce")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("bigcommerce",)


def test_bigcommerce_products_page_through_and_send_x_auth_token() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "bigcommerce" / "products_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "data": _full_page(
            250,
            lambda i: {
                "id": 8000 + i,
                "name": f"fixture product {i}",
                "sku": f"FIX-{i}",
                "price": 9.99,
                "date_created": "2024-01-01T00:00:00+00:00",
                "date_modified": "2024-01-01T01:00:00+00:00",
            },
        ),
        "meta": {"pagination": {"total": 251, "count": 250, "current_page": 1, "total_pages": 2}},
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/v3/catalog/products",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = BigCommerceSource(
            {
                "base_url": fixture.base_url + "/",
                "store_hash": "abc123",
                "api_key": "bc-synthetic",
            }
        )
        # base_url override replaces the manifest host entirely (fixture server).
        records = [r for batch in source.read("products") for r in batch.records]

    assert len(records) == 251
    assert records[-1]["id"] == 8102
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["page"] == ["2"]
    assert _headers(fixture.request_log[0])["x-auth-token"] == "bc-synthetic"


# ----------------------------------------------------------------- brevo ----


def test_brevo_manifest_declares_api_key_offset_pagination_and_modified_since() -> None:
    source = BrevoSource({"api_key": "brevo-synthetic"})
    contacts, lists, campaigns = source.manifest.streams

    assert source._base_url == "https://api.brevo.com/v3/"
    assert source.auth.headers == {"api-key": "brevo-synthetic"}
    for stream in (contacts, lists, campaigns):
        assert stream.paginator.type == "offset"
        assert stream.paginator.offset_param == "offset"
        assert stream.paginator.page_size_param == "limit"
    assert contacts.cursor is not None
    assert contacts.cursor.field == "modifiedAt"
    assert contacts.cursor.request_param == "modifiedSince"
    assert lists.cursor is None and campaigns.cursor is None

    descriptor = get_descriptor("brevo")
    assert descriptor is not None
    assert descriptor.catalog_ids == ("brevo",)


def test_brevo_contacts_page_through_offset_and_send_api_key_header() -> None:
    page_two = json.loads(
        (_FIXTURE_DIR / "brevo" / "contacts_page_2.json").read_text(encoding="utf-8")
    )
    page_one = {
        "contacts": _full_page(
            100,
            lambda i: {
                "id": 6900 + i,
                "email": f"fixture.{i}@example.com",
                "emailBlacklisted": False,
                "smsBlacklisted": False,
                "createdAt": "2024-01-01T00:00:00.000Z",
                "modifiedAt": "2024-01-01T01:00:00.000Z",
            },
        ),
        "count": 101,
    }

    with FixtureServer() as fixture:
        fixture.add_route(
            "/contacts",
            responses=[
                FixtureResponse(body=page_one),
                FixtureResponse(body=page_two),
            ],
        )
        source = BrevoSource({"base_url": fixture.base_url, "api_key": "brevo-synthetic"})
        records = [r for batch in source.read("contacts") for r in batch.records]

    assert len(records) == 101
    assert records[-1]["id"] == 7002
    assert len(fixture.request_log) == 2
    assert _query(fixture.request_log[1])["offset"] == ["100"]
    assert _headers(fixture.request_log[0])["api-key"] == "brevo-synthetic"
