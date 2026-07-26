"""Unit tests for new brand-specific REST/SaaS source connectors."""

from __future__ import annotations

from typing import Any
from unittest import mock

from connectors.airtable import read_object as airtable_read
from connectors.notion import read_object as notion_read
from connectors.rest_api import _build_headers, _get_url, _resolve_config
from connectors.shopify import read_object as shopify_read
from connectors.zendesk import read_object as zendesk_read
from src.transfer.connector_capabilities import enrich_catalog_entry


def _make_response(json_data: Any, headers: dict | None = None, status: int = 200):
    m = mock.MagicMock()
    m.status_code = status
    m.headers = headers or {}
    m.json.return_value = json_data
    return m


def test_shopify_base_url_and_auth():
    cfg = _resolve_config(
        {"type": "shopify", "api_key": "shpat_xxx", "table": "products", "extra": {"shop": "demo"}}
    )
    assert cfg["host"] == "https://demo.myshopify.com/admin/api/2024-04"
    assert cfg["object_path"] == "products"
    assert cfg.get("path_suffix") == ".json"
    assert cfg.get("auth_header") == "X-Shopify-Access-Token"
    assert _build_headers(cfg).get("X-Shopify-Access-Token") == "shpat_xxx"
    assert _get_url(cfg, {}) == "https://demo.myshopify.com/admin/api/2024-04/products.json"


def test_shopify_read_object():
    payload = {"products": [{"id": 1, "title": "A"}, {"id": 2, "title": "B"}]}
    with mock.patch("connectors.rest_api.requests.request", return_value=_make_response(payload)):
        batch = shopify_read(
            cfg={"api_key": "shpat_xxx", "table": "products", "extra": {"shop": "demo"}},
            limit=2,
        )
    assert batch.headers == ["id", "title"]
    assert batch.rows == [["1", "A"], ["2", "B"]]


def test_zendesk_cursor_pagination():
    calls: list[Any] = []

    def _respond(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            body = {
                "tickets": [{"id": 1, "subject": "A"}],
                "next_page": "https://demo.zendesk.com/api/v2/tickets.json?per_page=1&page=2",
            }
        else:
            body = {"tickets": [{"id": 2, "subject": "B"}]}
        return _make_response(body)

    with mock.patch("connectors.rest_api.requests.request", side_effect=_respond):
        batch = zendesk_read(
            cfg={"api_key": "token", "table": "tickets", "database": "demo"},
            limit=2,
        )
    assert batch.rows == [["1", "A"], ["2", "B"]]
    assert len(calls) == 2


def test_notion_cursor_pagination():
    calls: list[Any] = []

    def _respond(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            body = {"results": [{"id": "p1"}], "has_more": True, "next_cursor": "cur2"}
        else:
            body = {"results": [{"id": "p2"}], "has_more": False, "next_cursor": None}
        return _make_response(body)

    with mock.patch("connectors.rest_api.requests.request", side_effect=_respond):
        batch = notion_read(cfg={"api_key": "secret_xxx", "table": "databases"}, limit=2)
    assert batch.rows == [["p1"], ["p2"]]
    second_params = calls[1][1].get("params")
    assert second_params["start_cursor"] == "cur2"
    assert second_params["page_size"] == 1


def test_airtable_cursor_pagination():
    calls: list[Any] = []

    def _respond(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) == 1:
            body = {
                "records": [{"id": "r1", "fields": {"Name": "A"}}],
                "offset": "off2",
            }
        else:
            body = {"records": [{"id": "r2", "fields": {"Name": "B"}}]}
        return _make_response(body)

    with mock.patch("connectors.rest_api.requests.request", side_effect=_respond):
        batch = airtable_read(
            cfg={"api_key": "pat_xxx", "table": "appXXX/tblYYY"},
            limit=2,
        )
    assert batch.headers == ["id", "fields.Name"]
    assert batch.rows == [["r1", "A"], ["r2", "B"]]
    second_params = calls[1][1].get("params")
    assert second_params["offset"] == "off2"
    assert second_params["pageSize"] == 1


def test_source_only_saas_tiers():
    """No dedicated SaaS brand should remain source-only once a reverse-ETL writer ships."""
    for brand in ():
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live"}
        )
        assert row["transfer_ready"] is False, brand
        assert row["certification_tier"] == "source_only", brand
        assert row["effective_status"] == "live", brand


def test_transfer_ready_saas_tiers():
    """HubSpot/Salesforce certified; Stripe/Shopify/Airtable/Zendesk/Notion Planned."""
    certified = {"hubspot", "salesforce"}
    planned = {"stripe", "shopify", "airtable", "zendesk", "notion"}
    for brand in certified:
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live"}
        )
        assert row["transfer_ready"] is True, brand
        assert row["certification_tier"] == "certified", brand
        assert row["effective_status"] == "live", brand
    for brand in planned:
        row = enrich_catalog_entry(
            {"id": brand, "name": brand.title(), "category": "saas", "status": "live"}
        )
        assert row["transfer_ready"] is False, brand
        assert row["certification_tier"] == "planned", brand
        assert row["effective_status"] == "planned", brand
