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
    assert data["live"] > 0
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
