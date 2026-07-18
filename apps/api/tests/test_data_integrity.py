"""Production-grade data integrity tests — critical financial and type safety scenarios."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.data_integrity import run_integrity_audit  # noqa: E402
from services.transform_engine import apply_transform  # noqa: E402

# ── Transform edge cases (no silent magnitude loss) ──────────────────────────

@pytest.mark.parametrize(
    "raw,transform,expected",
    [
        ("$10,000.00", "decimal", "10000.00"),
        ("(1,234.56)", "decimal", "-1234.56"),
        ("1,234.56-", "decimal", "-1234.56"),
        ("50%", "decimal", "50"),
        ("1.5e3", "decimal", "1500"),
        ("1.5E+3", "integer", 1500),
        ("  42  ", "integer", 42),
        ("true", "boolean", True),
        ("N", "boolean", False),
    ],
)
def test_transform_parses_critical_formats(raw: str, transform: str, expected):
    value, err = apply_transform(raw, transform)
    assert err is None, f"Failed to parse {raw!r}: {err}"
    assert str(value) == str(expected) or value == expected


def test_transform_rejects_invalid_decimal():
    _, err = apply_transform("not_a_number", "decimal")
    assert err is not None


# ── Financial precision integrity ────────────────────────────────────────────

def test_integrity_blocks_unparseable_financial():
    rows = [
        {"amount": "$10,000.00"},
        {"amount": "not_money"},
        {"amount": "$5,000.00"},
    ]
    mappings = [{"source": "amount", "target": "amount", "confidence": 0.95, "transform": "decimal"}]
    report = run_integrity_audit(
        source_columns=["amount"],
        mappings=mappings,
        source_schemas=[{"name": "amount", "inferred_type": "DECIMAL"}],
        sample_rows=rows,
        validation_mode="strict",
    )
    assert report["blocks_transfer"] is True
    assert any("unparseable" in i.lower() or "financial" in i.lower() for i in report["issues"])


def test_integrity_passes_clean_financial_data():
    rows = [{"amount": "$10,000.00"}, {"amount": "$5,000.00"}, {"amount": "$2,499.00"}]
    mappings = [{"source": "amount", "target": "amount", "confidence": 0.95, "transform": "decimal"}]
    report = run_integrity_audit(
        source_columns=["amount"],
        mappings=mappings,
        source_schemas=[{"name": "amount", "inferred_type": "DECIMAL"}],
        sample_rows=rows,
        validation_mode="strict",
    )
    financial = next((c for c in report["checks"] if c["check"] == "financial_precision"), None)
    assert financial is not None
    assert financial["passed"] is True


def test_integrity_passes_locale_currency_formats():
    rows = [
        {"amount": "€1.000.000,89"},
        {"amount": "1 000 000.89"},
        {"amount": "(1,234.56)"},
        {"amount": "USD 1 000,00"},
    ]
    mappings = [{"source": "amount", "target": "amount", "confidence": 0.95, "transform": "decimal"}]
    report = run_integrity_audit(
        source_columns=["amount"],
        mappings=mappings,
        source_schemas=[{"name": "amount", "inferred_type": "DECIMAL"}],
        sample_rows=rows,
        validation_mode="strict",
    )
    financial = next((c for c in report["checks"] if c["check"] == "financial_precision"), None)
    assert financial is not None
    assert financial["passed"] is True
    transform = next((c for c in report["checks"] if c["check"] == "transform_dry_run"), None)
    assert transform is None or transform["passed"] is True


# ── Required field null checks ─────────────────────────────────────────────

def test_integrity_blocks_nulls_on_required_id():
    rows = [{"customer_id": ""}, {"customer_id": "C001"}, {"customer_id": ""}]
    mappings = [{"source": "customer_id", "target": "customer_id", "confidence": 0.95}]
    report = run_integrity_audit(
        source_columns=["customer_id"],
        mappings=mappings,
        sample_rows=rows,
        validation_mode="strict",
    )
    null_check = next((c for c in report["checks"] if c["check"] == "required_nulls"), None)
    assert null_check is not None
    assert null_check["blocks_transfer"] is True


# ── Duplicate key detection ──────────────────────────────────────────────────

def test_integrity_blocks_duplicate_primary_keys():
    rows = [
        {"order_id": "ORD-1"},
        {"order_id": "ORD-1"},
        {"order_id": "ORD-2"},
    ]
    mappings = [{"source": "order_id", "target": "order_id", "confidence": 0.99}]
    report = run_integrity_audit(
        source_columns=["order_id"],
        mappings=mappings,
        sample_rows=rows,
        validation_mode="strict",
    )
    dup_check = next((c for c in report["checks"] if c["check"] == "duplicate_keys"), None)
    assert dup_check is not None
    assert dup_check["blocks_transfer"] is True


# ── Coercion safety ──────────────────────────────────────────────────────────

def test_integrity_blocks_lossy_coercion():
    mappings = [{"source": "notes", "target": "age", "confidence": 0.6}]
    report = run_integrity_audit(
        source_columns=["notes"],
        target_columns=["age"],
        mappings=mappings,
        source_schemas=[{"name": "notes", "inferred_type": "VARCHAR"}],
        target_schemas=[{"name": "age", "inferred_type": "INTEGER"}],
        sample_rows=[{"notes": "hello"}, {"notes": "world"}],
        validation_mode="strict",
    )
    coercion = next((c for c in report["checks"] if c["check"] == "coercion_safety"), None)
    assert coercion is not None
    assert coercion["blocks_transfer"] is True


# ── Mapping confidence ───────────────────────────────────────────────────────

def test_integrity_blocks_low_confidence_in_strict_mode():
    mappings = [{"source": "AMT", "target": "amount", "confidence": 0.55}]
    report = run_integrity_audit(
        source_columns=["AMT"],
        mappings=mappings,
        sample_rows=[{"AMT": "100"}],
        validation_mode="strict",
    )
    conf = next((c for c in report["checks"] if c["check"] == "mapping_confidence"), None)
    assert conf is not None
    assert conf["blocks_transfer"] is True


def test_integrity_balanced_mode_allows_moderate_confidence():
    mappings = [{"source": "AMT", "target": "amount", "confidence": 0.80}]
    report = run_integrity_audit(
        source_columns=["AMT"],
        mappings=mappings,
        sample_rows=[{"AMT": "100"}],
        validation_mode="balanced",
    )
    conf = next((c for c in report["checks"] if c["check"] == "mapping_confidence"), None)
    assert conf is not None
    assert conf["passed"] is True


# ── Full audit structure ─────────────────────────────────────────────────────

def test_integrity_audit_returns_structured_report():
    report = run_integrity_audit(
        source_columns=["id", "amount"],
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "amount", "target": "amount", "confidence": 0.95, "transform": "decimal"},
        ],
        source_schemas=[
            {"name": "id", "inferred_type": "VARCHAR"},
            {"name": "amount", "inferred_type": "DECIMAL"},
        ],
        sample_rows=[
            {"id": "1", "amount": "$100.00"},
            {"id": "2", "amount": "$200.00"},
        ],
        validation_mode="strict",
    )
    assert "checks" in report
    assert report["checks_run"] >= 5
    assert "summary" in report
    assert isinstance(report["passed"], bool)


# ── Encoding / format-control characters ─────────────────────────────────────

def test_encoding_blocks_strict_mode():
    zwsp = "hello\u200bworld"
    report = run_integrity_audit(
        source_columns=["title"],
        mappings=[{"source": "title", "target": "title", "confidence": 0.99}],
        sample_rows=[{"title": zwsp}],
        validation_mode="strict",
    )
    enc = next((c for c in report["checks"] if c["check"] == "encoding_anomalies"), None)
    assert enc is not None
    assert enc["blocks_transfer"] is True
    assert report["blocks_transfer"] is True
    assert any("format-control" in str(i).lower() for i in enc["issues"])


def test_encoding_warns_balanced_mode():
    zwsp = "hello\u200bworld"
    report = run_integrity_audit(
        source_columns=["title"],
        mappings=[{"source": "title", "target": "title", "confidence": 0.99}],
        sample_rows=[{"title": zwsp}],
        validation_mode="balanced",
    )
    enc = next((c for c in report["checks"] if c["check"] == "encoding_anomalies"), None)
    assert enc is not None
    assert enc["blocks_transfer"] is False
    assert enc["passed"] is True
    assert enc["warnings"]
    assert "format-control" in " ".join(enc["warnings"]).lower()


def test_strip_controls_clears_encoding_block():
    zwsp = "hello\u200bworld"
    cleaned, err = apply_transform(zwsp, "strip_controls")
    assert err is None
    assert cleaned == "hello world" or cleaned == "helloworld" or "\u200b" not in str(cleaned)

    report = run_integrity_audit(
        source_columns=["title"],
        mappings=[{"source": "title", "target": "title", "confidence": 0.99, "transform": "strip_controls"}],
        sample_rows=[{"title": zwsp}],
        validation_mode="strict",
    )
    enc = next((c for c in report["checks"] if c["check"] == "encoding_anomalies"), None)
    assert enc is not None
    assert enc["blocks_transfer"] is False
    assert not enc["issues"]
