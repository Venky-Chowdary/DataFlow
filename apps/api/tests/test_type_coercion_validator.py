"""Type coercion validator tests."""

from services.type_coercion_validator import (
    coerce_blocks_transfer,
    validate_mapping_coercions,
)


def test_no_issue_for_same_logical_type():
    issues = validate_mapping_coercions(
        [{"source": "id", "target": "user_id", "confidence": 0.95}],
        source_types={"id": "INTEGER"},
        target_types={"user_id": "BIGINT"},
    )
    assert issues == []


def test_lossy_coercion_blocks_when_low_confidence():
    issues = validate_mapping_coercions(
        [{"source": "note", "target": "amount", "confidence": 0.6}],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
    )
    assert any(i.get("lossy") for i in issues)
    assert coerce_blocks_transfer(issues) is True


def test_lossy_coercion_blocks_when_high_confidence():
    """Under strict, lossy coercions always block — confidence must not green-light write failures."""
    issues = validate_mapping_coercions(
        [{"source": "note", "target": "amount", "confidence": 0.95}],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
        validation_mode="strict",
    )
    assert issues
    assert issues[0]["severity"] == "block"
    assert coerce_blocks_transfer(issues) is True


def test_lossy_coercion_blocks_under_balanced_without_risk_contract():
    """Balanced must not soft-green VARCHAR→NUMBER — Risk Contract required (data-rule matrix)."""
    issues = validate_mapping_coercions(
        [{"source": "note", "target": "amount", "confidence": 0.95}],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
        validation_mode="balanced",
    )
    assert issues
    assert issues[0]["severity"] == "block"
    assert coerce_blocks_transfer(issues) is True


def test_strict_boolean_ack_alone_still_blocks_lossy():
    """GA: risk_acknowledged without Risk Contract must not soften strict blocks."""
    issues = validate_mapping_coercions(
        [{
            "source": "note",
            "target": "amount",
            "confidence": 0.95,
            "risk_acknowledged": True,
        }],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
        validation_mode="strict",
    )
    assert issues
    assert issues[0]["severity"] == "block"


def test_strict_clearing_risk_contract_softens_to_warn():
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="note",
        source_type="VARCHAR",
        destination_type="INTEGER",
        approved_by="admin@dataflow.app",
        reason="Cast with quarantine",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    issues = validate_mapping_coercions(
        [{
            "source": "note",
            "target": "amount",
            "confidence": 0.95,
            "risk_acknowledged": True,
            "risk_contract": contract,
        }],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
        validation_mode="strict",
    )
    assert issues
    assert issues[0]["severity"] == "warn"


def test_type_locked_blocks_any_logical_type_change():
    """When target type is locked, any logical type change is a hard blocker."""
    issues = validate_mapping_coercions(
        [{"source": "id", "target": "id", "confidence": 0.99}],
        source_types={"id": "INTEGER"},
        target_types={"id": "VARCHAR"},
        schema_policy="type_locked",
    )
    assert any(i["severity"] == "block" for i in issues)


def test_type_locked_allows_same_logical_type():
    issues = validate_mapping_coercions(
        [{"source": "id", "target": "user_id", "confidence": 0.6}],
        source_types={"id": "INTEGER"},
        target_types={"user_id": "BIGINT"},
        schema_policy="type_locked",
    )
    assert issues == []


def test_folded_oracle_source_type_is_not_defaulted_to_varchar():
    issues = validate_mapping_coercions(
        [
            {"source": "id", "target": "id", "confidence": 0.99, "transform": "integer"},
            {
                "source": "amount",
                "target": "amount",
                "confidence": 0.99,
                "transform": "decimal",
            },
        ],
        source_types={"ID": "DECIMAL(38,0)", "AMOUNT": "DECIMAL(18,2)"},
        target_types={"id": "NUMERIC(38,0)", "amount": "NUMERIC(18,2)"},
        dest_db_type="postgresql",
        dest_table_exists=False,
    )
    assert issues == [], issues
