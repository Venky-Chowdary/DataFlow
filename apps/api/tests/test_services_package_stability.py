"""Dual services package import stability — proves both namespaces resolve."""

from __future__ import annotations


def test_canonical_services_is_apps_api_services():
    import services
    import services.catalog_service as cat

    assert "apps/api/services" in str(services.__file__).replace("\\", "/")
    assert hasattr(cat, "catalog_summary")
    summary = cat.catalog_summary()
    assert summary["total"] >= 600
    assert int(summary.get("unique_drivers") or 0) >= 8


def test_src_services_reexports_catalog():
    from src.services import catalog_service as shim

    assert hasattr(shim, "catalog_summary")
    # Same function object when shim re-exports canonical.
    from services import catalog_service as canonical

    assert shim.catalog_summary is canonical.catalog_summary or (
        shim.catalog_summary()["total"] == canonical.catalog_summary()["total"]
    )
