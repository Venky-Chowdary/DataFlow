"""Type coercion validator tests."""

from services.type_coercion_validator import coerce_blocks_transfer, validate_mapping_coercions


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


def test_lossy_coercion_warns_when_high_confidence():
    issues = validate_mapping_coercions(
        [{"source": "note", "target": "amount", "confidence": 0.95}],
        source_types={"note": "VARCHAR"},
        target_types={"amount": "INTEGER"},
    )
    assert issues
    assert issues[0]["severity"] == "warn"
    assert coerce_blocks_transfer(issues) is False


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
