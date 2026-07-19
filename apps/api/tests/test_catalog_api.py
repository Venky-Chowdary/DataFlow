"""Catalog service — category, role, and search filters."""

from __future__ import annotations

import importlib.util
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
_CATALOG_PATH = _API_ROOT / "src" / "services" / "catalog_service.py"


def _load_catalog_service():
    spec = importlib.util.spec_from_file_location("catalog_service", _CATALOG_PATH)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod


_catalog = _load_catalog_service()


def test_catalog_summary():
    data = _catalog.catalog_summary()
    assert data["total"] >= 600
    assert data.get("unique_drivers", data.get("transfer_live", 0)) >= 8
    assert data.get("unique_drivers", 999) < 80
    assert data["categories"] >= 8


def test_search_database_category():
    data = _catalog.search_catalog("", "all", "database", "", 20)
    assert data["filtered"] > 0
    for c in data["connectors"]:
        assert c["category"] == "database"


def test_search_role_source_suggested():
    data = _catalog.search_catalog("", "source", "", "", 16)
    assert len(data.get("suggested", [])) > 0


def test_search_status_live():
    data = _catalog.search_catalog("", "all", "", "live", 30)
    for c in data["connectors"]:
        assert c["status"] == "live"


def test_search_status_live_count():
    """Live *tiles* may be many aliases; unique drivers stay bounded."""
    from services.catalog_service import catalog_summary

    summary = catalog_summary()
    unique = int(summary.get("unique_drivers") or summary.get("transfer_live") or 0)
    assert unique >= 8
    assert unique < 80
    data = _catalog.search_catalog("", "all", "", "live", 1000)
    # Tile filter can return aliases — just ensure the endpoint works.
    assert data["filtered"] >= unique


def test_search_query():
    data = _catalog.search_catalog("postgres", "all", "", "", 10)
    assert data["filtered"] > 0
    blob = " ".join(c["name"].lower() + c["id"].lower() for c in data["connectors"])
    assert "postgres" in blob


def test_catalog_training_docs_do_not_overclaim_planned_connectors():
    docs = _catalog.catalog_training_docs()
    planned = next(d for d in docs if d["metadata"].get("status") == "planned")
    assert "catalog discovery" in planned["text"]
    assert "route live transfers only when" in planned["text"]


def test_catalog_summary_survives_broken_module_spec(monkeypatch):
    """A broken/missing DBAPI package must not crash the catalog endpoint."""
    original = importlib.util.find_spec

    def fake_find_spec(name, *args, **kwargs):
        if name and "snowflake" in name:
            raise ModuleNotFoundError(f"No module named {name!r}")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    data = _catalog.catalog_summary()
    assert data["total"] >= 600
