"""D148 — the plan asks for a contract only when a value is finer than milliseconds.

BSON date and Elasticsearch date keep milliseconds. A TIMESTAMPTZ(6) column
whose measured values are whole milliseconds is not a fidelity collapse.
A value with a microsecond is, and the plan names the two exits: accept the
truncation, or keep the digits in a string. JSONB onto a document object is
the same document and is never a collapse.
"""

from __future__ import annotations

from services.mapping_proof import mapping_fidelity
from services.type_coercion import is_lossy_coercion, is_precision_collapse_coercion


def _fid(src: str, tgt: str, pop=None, **extra):
    mapping = {
        "source": "c",
        "target": "c",
        "source_type": src,
        "target_type": tgt,
        "samples": pop or [],
        **extra,
    }
    return mapping_fidelity(
        mapping, destination_db_type="mongodb", dest_table_exists=False, population=pop
    )


def test_whole_milliseconds_need_no_contract():
    verdict = _fid("TIMESTAMPTZ(6)", "date", ["2024-01-01T00:00:00.123+00:00"])
    assert verdict["verdict"] == "preserve"
    assert verdict["requires_risk_contract"] is False
    assert is_lossy_coercion(
        "TIMESTAMPTZ(6)", "date", dest_db="mongodb",
        population=["2024-01-01T00:00:00.123+00:00"], source_db="postgresql",
    ) is False


def test_a_microsecond_requires_the_contract_and_names_both_exits():
    verdict = _fid("TIMESTAMPTZ(6)", "date", ["2024-01-01T00:00:00.123456+00:00"])
    assert verdict["verdict"] == "lossy_cast"
    assert verdict["requires_risk_contract"] is True
    assert "milliseconds" in str(verdict["reason"])
    assert "string" in str(verdict["reason"])
    assert is_precision_collapse_coercion(
        "TIMESTAMPTZ(6)", "date", dest_db="mongodb",
        population=["2024-01-01T00:00:00.123456+00:00"],
    ) is True


def test_a_signed_continue_contract_clears_the_millisecond_narrowing():
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="ts",
        source_type="TIMESTAMPTZ(6)",
        destination_type="date",
        approved_by="risk-approver@example.com",
        reason="Accept millisecond truncation",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
    ).to_dict()
    verdict = _fid(
        "TIMESTAMPTZ(6)", "date", ["2024-01-01T00:00:00.123456+00:00"],
        risk_contract=contract,
    )
    assert verdict["requires_risk_contract"] is False


def test_a_decimal_measure_is_not_a_card_number():
    """D295: an Oracle BINARY_DOUBLE ratio can Luhn-check once the point is stripped."""
    from services.pii_guard import pii_findings

    for value in ("1.4285714285714286", "2.5714285714285716", "99999999999.999999"):
        assert "pci_pan" not in pii_findings(value), value


def test_jsonb_onto_a_document_object_is_not_a_collapse():
    for tgt in ("OBJECT", "object"):
        verdict = _fid("JSONB", tgt)
        assert verdict["verdict"] == "preserve", verdict
        assert verdict["requires_risk_contract"] is False
        assert is_lossy_coercion("JSONB", tgt, dest_db="mongodb", source_db="postgresql") is False
        assert is_precision_collapse_coercion("JSONB", tgt, dest_db="mongodb") is False
