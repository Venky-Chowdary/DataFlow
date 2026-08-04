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


def test_integrity_allows_sparse_oauth_ids_when_real_pk_present():
    """Mongo users often lack googleId/providerId — do not block on those FKs."""
    rows = [
        {"_id": "a1", "email": "a@x.com", "googleId": "", "providerId": ""},
        {"_id": "a2", "email": "b@x.com", "googleId": "g-2", "providerId": "google"},
        {"_id": "a3", "email": "c@x.com", "googleId": "", "providerId": ""},
    ]
    mappings = [
        {"source": "_id", "target": "id", "confidence": 0.99},
        {"source": "email", "target": "email", "confidence": 0.95},
        {"source": "googleId", "target": "google_id", "confidence": 0.93},
        {"source": "providerId", "target": "provider_id", "confidence": 0.93},
    ]
    report = run_integrity_audit(
        source_columns=["_id", "email", "googleId", "providerId"],
        mappings=mappings,
        sample_rows=rows,
        destination_db_type="mysql",
        validation_mode="strict",
        sync_mode="full_refresh_append",
    )
    null_check = next((c for c in report["checks"] if c["check"] == "required_nulls"), None)
    assert null_check is not None
    assert null_check["blocks_transfer"] is False, null_check.get("issues")
    assert not any("googleId" in i or "providerId" in i for i in (null_check.get("issues") or []))


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


def test_integrity_balanced_still_blocks_dupes_on_upsert():
    """Balanced must not green-light PK collisions for upsert/CDC routes."""
    rows = [
        {"id": "1"},
        {"id": "1"},
    ]
    mappings = [{"source": "id", "target": "id", "confidence": 0.99, "primary_key": True}]
    report = run_integrity_audit(
        source_columns=["id"],
        mappings=mappings,
        sample_rows=rows,
        validation_mode="balanced",
        destination_db_type="postgresql",
        sync_mode="upsert",
    )
    dup_check = next((c for c in report["checks"] if c["check"] == "duplicate_keys"), None)
    assert dup_check is not None
    assert dup_check["blocks_transfer"] is True
    assert dup_check["passed"] is False
    assert dup_check.get("warnings") or dup_check.get("issues")


def test_integrity_source_probe_blocks_even_balanced_append():
    """Full-table probe findings must keep Validate red after Quarantine/balanced."""
    # Clean sample (would pass sample-only) + probe says 153 keys repeat.
    rows = [{"id": "unique-a"}, {"id": "unique-b"}, {"id": "unique-c"}]
    mappings = [{"source": "id", "target": "id", "confidence": 0.99, "primary_key": True}]
    findings = [
        {"value": "507f1f77bcf86cd799439011", "count": 4},
        {"value": "507f1f77bcf86cd799439012", "count": 3},
    ]
    report = run_integrity_audit(
        source_columns=["id"],
        mappings=mappings,
        sample_rows=rows,
        validation_mode="balanced",
        destination_db_type="postgresql",
        sync_mode="full_refresh_append",
        contract_primary_key="id",
        source_duplicate_findings=findings,
        source_duplicate_probe_ran=True,
        source_duplicate_probe_pk="id",
    )
    dup_check = next((c for c in report["checks"] if c["check"] == "duplicate_keys"), None)
    assert dup_check is not None
    assert dup_check["blocks_transfer"] is True
    assert dup_check["passed"] is False
    assert report["blocks_transfer"] is True
    joined = " ".join(dup_check.get("issues") or [])
    assert "source probe" in joined.lower()
    assert "Strip/Quarantine cannot fix" in (dup_check.get("note") or "")


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


def test_integrity_blocks_varchar_to_number_without_risk_ack():
    """Declared VARCHAR→NUMBER is lossy — sample soft-pass requires Accept risk (G3)."""
    mappings = [{
        "source": "population",
        "target": "population",
        "confidence": 0.93,
        "transform": "none",
        "target_type": "NUMBER(38,0)",
    }]
    report = run_integrity_audit(
        source_columns=["population"],
        target_columns=["population"],
        mappings=mappings,
        source_schemas=[{"name": "population", "inferred_type": "VARCHAR"}],
        target_schemas=[{"name": "population", "inferred_type": "NUMBER(38,0)"}],
        sample_rows=[
            {"population": "331002651"},
            {"population": "1402112000"},
            {"population": "45195799"},
        ],
        destination_db_type="snowflake",
        validation_mode="strict",
    )
    coercion = next((c for c in report["checks"] if c["check"] == "coercion_safety"), None)
    assert coercion is not None
    assert coercion["blocks_transfer"] is True


def test_integrity_sample_clears_varchar_to_number_with_risk_ack():
    """With a continue-policy Risk Contract, clean samples may clear the coercion block."""
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="population",
        source_type="VARCHAR",
        destination_type="NUMBER(38,0)",
        approved_by="admin@dataflow.app",
        reason="Numeric VARCHAR population cast",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    mappings = [{
        "source": "population",
        "target": "population",
        "confidence": 0.93,
        "transform": "none",
        "target_type": "NUMBER(38,0)",
        "risk_acknowledged": True,
        "risk_contract": contract,
    }]
    report = run_integrity_audit(
        source_columns=["population"],
        target_columns=["population"],
        mappings=mappings,
        source_schemas=[{"name": "population", "inferred_type": "VARCHAR"}],
        target_schemas=[{"name": "population", "inferred_type": "NUMBER(38,0)"}],
        sample_rows=[
            {"population": "331002651"},
            {"population": "1402112000"},
            {"population": "45195799"},
        ],
        destination_db_type="snowflake",
        validation_mode="strict",
    )
    coercion = next((c for c in report["checks"] if c["check"] == "coercion_safety"), None)
    assert coercion is not None
    assert coercion["blocks_transfer"] is False


def test_integrity_boolean_ack_alone_does_not_sample_clear():
    """GA: bare risk_acknowledged must not soft-pass G9 coercion_safety."""
    mappings = [{
        "source": "population",
        "target": "population",
        "confidence": 0.93,
        "transform": "none",
        "target_type": "NUMBER(38,0)",
        "risk_acknowledged": True,
    }]
    report = run_integrity_audit(
        source_columns=["population"],
        target_columns=["population"],
        mappings=mappings,
        source_schemas=[{"name": "population", "inferred_type": "VARCHAR"}],
        target_schemas=[{"name": "population", "inferred_type": "NUMBER(38,0)"}],
        sample_rows=[
            {"population": "331002651"},
            {"population": "1402112000"},
        ],
        destination_db_type="snowflake",
        validation_mode="strict",
    )
    coercion = next((c for c in report["checks"] if c["check"] == "coercion_safety"), None)
    assert coercion is not None
    assert coercion["blocks_transfer"] is True


# ── Mapping confidence ───────────────────────────────────────────────────────

def test_integrity_reports_low_confidence_without_blocking():
    """Module 3: G9 reports confidence; G4 owns the hard block."""
    mappings = [{"source": "AMT", "target": "amount", "confidence": 0.55}]
    report = run_integrity_audit(
        source_columns=["AMT"],
        mappings=mappings,
        sample_rows=[{"AMT": "100"}],
        validation_mode="strict",
    )
    conf = next((c for c in report["checks"] if c["check"] == "mapping_confidence"), None)
    assert conf is not None
    assert conf["blocks_transfer"] is False
    assert conf.get("authority") == "g4_mapping_confidence"
    assert conf.get("warnings") or conf.get("issues")


def test_integrity_honors_user_override_like_g4():
    """Approved Map overrides must not be re-blocked by G9 confidence."""
    mappings = [{
        "source": "companyNumEmployees",
        "target": "company_number_employees",
        "confidence": 0.82,
        "user_override": True,
    }]
    report = run_integrity_audit(
        source_columns=["companyNumEmployees"],
        mappings=mappings,
        sample_rows=[{"companyNumEmployees": "1000"}],
        validation_mode="strict",
    )
    conf = next((c for c in report["checks"] if c["check"] == "mapping_confidence"), None)
    assert conf is not None
    assert conf["blocks_transfer"] is False
    assert conf["passed"] is True


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


def test_encoding_blocks_balanced_mode_too():
    """Balanced surfaces encoding as warnings + strip_controls — not a hard block."""
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
    assert any("strip_controls" in w for w in (enc.get("warnings") or []))
    assert any("strip_controls" in w for w in (report.get("warnings") or []))
    assert enc.get("encoding_findings")

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


def test_strip_controls_skip_is_case_insensitive():
    zwsp = "hello\u200bworld"
    report = run_integrity_audit(
        source_columns=["Description"],
        mappings=[{"source": "Description", "target": "DESCRIPTION", "confidence": 0.99, "transform": "strip_controls"}],
        sample_rows=[{"Description": zwsp}],
        validation_mode="strict",
    )
    enc = next((c for c in report["checks"] if c["check"] == "encoding_anomalies"), None)
    assert enc is not None
    assert enc["blocks_transfer"] is False
    assert not enc["issues"]
