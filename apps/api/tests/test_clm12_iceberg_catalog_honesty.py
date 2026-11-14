"""CLM-12: Iceberg never guesses a Glue catalog, and its copy names what ships.

Any s3:// or gs:// warehouse without a catalog_type was routed to the Glue
catalog (Planned, untested in CI) silently. Marketing said "SQL and REST
catalogs" while the connect form offers filesystem and REST.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from connectors.iceberg_catalog import IcebergCatalogNotChosen, _infer_catalog_type

REPO = Path(__file__).resolve().parents[3]


@pytest.mark.parametrize(
    ("warehouse", "region"),
    [("s3://lake/wh", ""), ("gs://lake/wh", ""), ("gcs://lake/wh", ""), ("", "eu-west-1")],
)
def test_an_inferred_glue_catalog_is_refused_with_the_choices(warehouse: str, region: str) -> None:
    with pytest.raises(IcebergCatalogNotChosen) as exc:
        _infer_catalog_type("", region, warehouse, {})
    text = str(exc.value)
    assert "catalog_type" in text and "rest" in text and "sql" in text
    assert "Glue is Planned" in text


def test_explicit_catalogs_still_resolve() -> None:
    assert _infer_catalog_type("", "", "s3://lake/wh", {"catalog_type": "glue"}) == "glue"
    assert _infer_catalog_type("", "", "s3://lake/wh", {"catalog_type": "rest"}) == "rest"
    assert _infer_catalog_type("https://catalog.example/v1", "eu-west-1", "s3://lake/wh", {}) == "rest"
    assert _infer_catalog_type("sqlite:///cat.db", "", "s3://lake/wh", {}) == "sql"
    assert _infer_catalog_type("", "", "/tmp/w5clm_wh", {}) == "filesystem"


def test_marketing_names_the_catalogs_the_product_offers() -> None:
    text = (REPO / "apps" / "web" / "src" / "lib" / "provenEvidence.ts").read_text(encoding="utf-8")
    assert "Iceberg works with SQL and REST catalogs" not in text
    assert "REST catalog" in text
