"""Sold SKU routes must validate with drivers present — never invent green."""

from __future__ import annotations

from services.sku_honesty import (
    classify_production_sku,
    classify_sku_route,
    sku_honesty_summary,
)
from src.transfer.registry import PRODUCTION_SKU, validate_transfer


def test_sold_routes_all_validate_and_have_drivers() -> None:
    rows = classify_production_sku()
    summary = sku_honesty_summary(rows)
    assert summary["production_sku_claimed"] == len(PRODUCTION_SKU)
    assert (
        summary["production_sku_sold"]
        + summary["production_sku_driver_missing"]
        + summary["production_sku_refused"]
        == len(PRODUCTION_SKU)
    )
    sold = [r for r in rows if r["status"] == "sold"]
    assert sold, "at least the core OLTP/file SKU routes must be sold on this host"
    for row in sold:
        assert row["validate_ok"] is True, row
        assert row["driver_gap"] is None, row
        ok, msg = validate_transfer(
            row["source_kind"], row["source_format"], row["dest_kind"], row["dest_format"]
        )
        assert ok, f"{row['route']} sold but validate_transfer refused: {msg}"
        assert "Planned" not in msg


def test_driver_missing_is_not_sold() -> None:
    rows = classify_production_sku()
    for row in rows:
        if row["status"] == "driver_missing":
            assert row["sold"] is False
            assert row["driver_gap"]


def test_refused_is_not_sold() -> None:
    rows = classify_production_sku()
    for row in rows:
        if row["status"] == "refused":
            assert row["sold"] is False
            assert row["validate_ok"] is False


def test_classify_core_pg_mysql_is_sold() -> None:
    row = classify_sku_route(("database", "postgresql", "database", "mysql"))
    assert row["status"] == "sold"
    assert row["sold"] is True


def test_core_driver_missing_is_gap_not_refused(monkeypatch) -> None:
    """psycopg2/pymysql missing is driver_missing, not a refused SKU."""
    import services.sku_honesty as sh

    monkeypatch.setattr(sh, "driver_available", lambda *a, **k: False)
    row = sh.classify_sku_route(("database", "postgresql", "database", "mysql"))
    assert row["status"] == "driver_missing"
    assert row["sold"] is False
    assert row["driver_gap"]
    assert "postgresql" in row["driver_gap"] or "mysql" in row["driver_gap"]
