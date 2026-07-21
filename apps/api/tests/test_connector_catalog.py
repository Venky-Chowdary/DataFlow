"""Tests for connector catalog API (certified + roadmap tiles)."""

from services.connector_catalog import list_catalog


def test_catalog_has_600_plus_entries() -> None:
    data = list_catalog(limit=1)
    assert data["catalog_total"] >= 600


def test_catalog_search_salesforce() -> None:
    data = list_catalog(q="salesforce", limit=20)
    assert data["total"] >= 1
    assert any("salesforce" in c["name"].lower() for c in data["connectors"])


def test_catalog_category_filter() -> None:
    data = list_catalog(category="logistics", limit=50)
    assert data["total"] >= 5
    assert all(c["category"] == "logistics" for c in data["connectors"])
