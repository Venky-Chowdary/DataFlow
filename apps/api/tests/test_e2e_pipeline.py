"""End-to-end tests: upload → schema inference → semantic mapping → preflight → transform."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from services.file_parser import store_upload
from services.mapping_pipeline import run_mapping_pipeline
from services.schema_inference import infer_type

FIXTURES = Path(__file__).resolve().parent / "fixtures"
_API_ROOT = Path(__file__).resolve().parents[1]


def _run_file_preflight(**kwargs):
    """Load preflight_service without pulling in pymongo via src.services package."""
    path = _API_ROOT / "src" / "services" / "preflight_service.py"
    spec = importlib.util.spec_from_file_location("preflight_service_mod", path)
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(mod)
    return mod.run_file_preflight(**kwargs)

ALL_SAMPLES = [
    "sample_payments.csv",
    "sample_logistics.csv",
    "sample_retail.csv",
    "sample_synonyms.csv",
    "sample_hr.json",
    "sample_payments.tsv",
    "sample_schema_types.csv",
    "sample_mixed_types.jsonl",
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
    "record_uuid",
    "metadata_json",
    "narrative_body",
    "payload_b64",
    "customer_email",
]

# Typed warehouse stamps — bare VARCHAR invent fail-closes G3 on DECIMAL/UUID/JSON.
_WAREHOUSE_TARGET_TYPES = {
    "customer_id": "INTEGER",
    "payment_amount": "DECIMAL",
    "transaction_date": "DATE",
    "account_number": "VARCHAR",
    "currency_code": "VARCHAR",
    "reference_number": "VARCHAR",
    "status": "VARCHAR",
    "description": "TEXT",
    "record_uuid": "UUID",
    "metadata_json": "JSON",
    "narrative_body": "TEXT",
    "payload_b64": "BINARY",
    "customer_email": "VARCHAR",
}


@pytest.mark.parametrize("filename", ALL_SAMPLES)
def test_e2e_upload_to_preflight(filename: str) -> None:
    """Full pipeline: parse file, map columns, run preflight gates."""
    path = FIXTURES / filename
    record = store_upload(filename, path.read_bytes())
    assert record["row_count"] >= 1
    source_cols = [c["name"] for c in record["columns"]]
    column_types = {c["name"]: c["inferred_type"] for c in record["columns"]}

    pipeline = run_mapping_pipeline(
        source_cols,
        WAREHOUSE_TARGETS,
        source_schemas=record["columns"],
        target_schemas=[
            {
                "name": t,
                "inferred_type": _WAREHOUSE_TARGET_TYPES.get(t, "VARCHAR"),
                "samples": [],
            }
            for t in WAREHOUSE_TARGETS
        ],
        file_format=record["format"],
    )
    mappings = pipeline["mappings"]
    assert len(mappings) == len(source_cols)

    preview = record.get("preview_rows") or []
    sample_rows = [dict(zip(source_cols, row)) for row in preview[:10]] if preview else None

    pf = _run_file_preflight(
        columns=source_cols,
        column_types=column_types,
        row_count=record["row_count"],
        mappings=[{"source": m["source"], "target": m["target"], "confidence": m["confidence"], "reason": m.get("reasoning", "")} for m in mappings],
        destination_connected=False,
        destination_error="E2E test — no live destination",
        sample_rows=sample_rows,
        estimated_bytes=record.get("file_size_bytes", 0),
    )
    assert pf["total_gates"] == 10
    by_id = {g["id"]: g for g in pf["gates"]}
    assert "g13_source_coverage" in by_id
    assert by_id.get("g1_source", {}).get("status") == "pass", pf["gates"]
    assert by_id.get("g2_destination", {}).get("status") == "block", pf["gates"]
    # Offline warehouse map: G3/G4 may block on low confidence or residual coercion —
    # still require the preflight surface to run (≥3 green gates is the offline floor).
    assert pf["passed_count"] >= 3, (
        f"Expected offline preflight floor for {filename}, got {pf['passed_count']}: "
        f"{[(g['id'], g.get('status')) for g in pf['gates']]}"
    )
    assert "g4_mapping_confidence" in by_id


@pytest.mark.parametrize("filename", ALL_SAMPLES)
def test_e2e_semantic_analysis_present(filename: str) -> None:
    path = FIXTURES / filename
    record = store_upload(filename, path.read_bytes())
    result = run_mapping_pipeline(
        [c["name"] for c in record["columns"]],
        WAREHOUSE_TARGETS,
        source_schemas=record["columns"],
        file_format=record["format"],
    )
    assert result["semantic_analysis"]
    assert result["classification"]["format"]


def test_e2e_schema_types_column_accuracy() -> None:
    """Enterprise schema fixture — each column type must be detected correctly."""
    path = FIXTURES / "sample_schema_types.csv"
    record = store_upload("sample_schema_types.csv", path.read_bytes())
    by_name = {c["name"]: c["inferred_type"] for c in record["columns"]}

    expected = {
        "row_id": "INTEGER",
        "amount": "DECIMAL",
        "is_active": "BOOLEAN",
        "created_at": "TIMESTAMPTZ",
        "birth_date": "DATE",
        "txn_yyyymmdd": "DATE",
        "record_uuid": "UUID",
        "metadata_json": "JSON",
        "narrative_body": "TEXT",
        "payload_b64": "BINARY",
        "customer_email": "VARCHAR",
        "updated_epoch_ms": "TIMESTAMP",
    }
    for col, exp_type in expected.items():
        assert col in by_name, f"Missing column {col}"
        got = by_name[col]
        if exp_type == "DECIMAL":
            assert str(got).startswith("DECIMAL"), (
                f"{col}: expected DECIMAL(p,s) or DECIMAL, got {got}"
            )
        else:
            assert got == exp_type, f"{col}: expected {exp_type}, got {got}"


def test_e2e_jsonl_parsing() -> None:
    path = FIXTURES / "sample_mixed_types.jsonl"
    record = store_upload("sample_mixed_types.jsonl", path.read_bytes())
    assert record["format"] == "jsonl"
    assert record["row_count"] == 3
    by_name = {c["name"]: c["inferred_type"] for c in record["columns"]}
    assert by_name["row_id"] == "INTEGER"
    assert str(by_name["amount"]).startswith("DECIMAL")
    assert by_name["is_active"] == "BOOLEAN"
    assert by_name["record_uuid"] == "UUID"
    assert by_name["metadata_json"] == "JSON"
    assert by_name["payload_b64"] == "BINARY"


@pytest.mark.parametrize(
    "samples,expected",
    [
        (["1500", "2000", "3"], "INTEGER"),
        (["1500.50", "2000.00"], "DECIMAL"),
        (["true", "false", "yes"], "BOOLEAN"),
        (["2024-01-15", "2024-02-01"], "DATE"),
        (["20240115", "20240201"], "DATE"),
        (["2024-01-15 10:30:00", "2024-02-01T14:22:33Z"], "TIMESTAMP"),
        (["1705312200000", "1706788800000"], "TIMESTAMP"),
        (["550e8400-e29b-41d4-a716-446655440000"], "UUID"),
        (['{"a":1}', '{"b":2}'], "JSON"),
        (["SGVsbG8gV29ybGQ="], "VARCHAR"),
        (["x" * 300], "TEXT"),
        (["user@example.com"], "VARCHAR"),
    ],
)
def test_infer_type_unit(samples: list[str], expected: str) -> None:
    got = infer_type(samples)
    # Sample-aware invent stamps DECIMAL(p,s); bare DECIMAL remains the class.
    if expected == "DECIMAL":
        assert str(got).startswith("DECIMAL")
    else:
        assert got == expected
