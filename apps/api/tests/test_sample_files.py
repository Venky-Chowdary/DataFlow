"""Automated tests for all bundled sample data files."""

from __future__ import annotations

from pathlib import Path

import pytest

from services.file_parser import store_upload
from services.mapping_pipeline import run_mapping_pipeline

FIXTURES = Path(__file__).resolve().parent / "fixtures"

SAMPLE_FILES = [
    "sample_payments.csv",
    "sample_logistics.csv",
    "sample_retail.csv",
    "sample_synonyms.csv",
    "sample_hr.json",
    "sample_payments.tsv",
]

WAREHOUSE_TARGETS = [
    "customer_id",
    "payment_amount",
    "transaction_date",
    "account_number",
    "currency_code",
    "reference_number",
    "status",
    "description",
]


@pytest.mark.parametrize("filename", SAMPLE_FILES)
def test_sample_file_upload_and_schema(filename: str) -> None:
    path = FIXTURES / filename
    assert path.exists(), f"Missing fixture: {path}"
    content = path.read_bytes()
    record = store_upload(filename, content)
    assert record["row_count"] >= 1, f"{filename} should have rows"
    assert len(record["columns"]) >= 1, f"{filename} should infer columns"
    assert record["file_id"]
    for col in record["columns"]:
        assert col["name"]
        assert col["inferred_type"]


@pytest.mark.parametrize("filename", ["sample_logistics.csv", "sample_payments.csv", "sample_retail.csv"])
def test_semantic_mapping_on_samples(filename: str) -> None:
    path = FIXTURES / filename
    record = store_upload(filename, path.read_bytes())
    source_cols = [c["name"] for c in record["columns"]]
    result = run_mapping_pipeline(
        source_cols,
        WAREHOUSE_TARGETS,
        source_schemas=record["columns"],
        target_schemas=[{"name": t, "inferred_type": "VARCHAR", "samples": []} for t in WAREHOUSE_TARGETS],
        file_format=record["format"],
    )
    assert len(result["mappings"]) >= 1
    assert result["semantic_analysis"]
    mapped_sources = {m["source"] for m in result["mappings"]}
    assert mapped_sources.issubset(set(source_cols))


def test_logistics_amount_column_maps_to_payment_amount() -> None:
    path = FIXTURES / "sample_logistics.csv"
    record = store_upload("sample_logistics.csv", path.read_bytes())
    result = run_mapping_pipeline(
        [c["name"] for c in record["columns"]],
        WAREHOUSE_TARGETS + ["origin_city", "destination_city", "tracking_number", "shipment_weight_kg"],
        source_schemas=record["columns"],
        file_format="csv",
    )
    by_source = {m["source"]: m for m in result["mappings"]}
    assert by_source["AMT"]["target"] == "payment_amount"
    assert by_source["AMT"]["confidence"] >= 0.85


def test_transform_dry_run_zero_errors_on_samples() -> None:
    from services.transform_engine import dry_run_sample, infer_transform

    for filename in SAMPLE_FILES:
        path = FIXTURES / filename
        record = store_upload(filename, path.read_bytes())
        headers = [c["name"] for c in record["columns"]]
        preview = record.get("preview_rows") or []
        if not preview:
            continue
        mappings = [
            {
                "source": c["name"],
                "target": c["name"].lower(),
                "transform": infer_transform(c["name"], c["name"].lower(), c["inferred_type"]),
            }
            for c in record["columns"]
        ]
        col_types = {c["name"]: c["inferred_type"] for c in record["columns"]}
        passed, errors = dry_run_sample(
            headers=headers,
            sample_rows=preview,
            mappings=mappings,
            column_types=col_types,
        )
        assert passed, f"{filename} transform dry-run failed: {errors}"
