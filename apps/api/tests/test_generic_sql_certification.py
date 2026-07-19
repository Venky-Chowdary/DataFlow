"""Sprint D: Oracle / SQL Server certification honesty."""

from __future__ import annotations

import pytest

from src.transfer.connector_capabilities import (
    _generic_sql_brand_certified,
    driver_available,
    enrich_catalog_entry,
)


@pytest.mark.parametrize("brand", ["oracle", "sql_server", "mssql"])
def test_generic_sql_brand_certification_matches_dbapi(brand: str) -> None:
    certified = _generic_sql_brand_certified(brand)
    # Must agree with driver_available for the dialect.
    expect = driver_available("generic_sql", "oracle" if "oracle" in brand else "sqlserver")
    assert certified is expect
    row = enrich_catalog_entry(
        {"id": brand if brand != "mssql" else "sql_server", "name": brand, "category": "database", "status": "live", "description": ""}
    )
    if expect:
        assert row["transfer_ready"] is True
        assert row["certification_tier"] == "certified"
    else:
        assert row["transfer_ready"] is False
        assert row["certification_tier"] == "planned"
