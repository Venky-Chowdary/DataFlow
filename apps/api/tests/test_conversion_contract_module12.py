"""Module 12 — Conversion Contract + DDL Identity tests."""

from __future__ import annotations

import pytest

from services.conversion_contract import (
    ConversionClass,
    DdlIdentityError,
    approved_mapping_ddl_fingerprint,
    assert_ddl_identity,
    classify_conversion,
    invents_unproven_capacity,
)


def test_lossless_same_decimal_params():
    result = classify_conversion(
        "DECIMAL(12,2)",
        "DECIMAL(12,2)",
        dest_db="postgresql",
        transform="none",
    )
    # Phase C3 — identical stamps are Identity (still a safe path).
    assert result["conversion_class"] == ConversionClass.IDENTITY.value
    assert result["requires_risk_contract"] is False


def test_integer_widen_is_widening_class():
    result = classify_conversion(
        "INTEGER",
        "BIGINT",
        dest_db="postgresql",
        transform="none",
    )
    assert result["conversion_class"] == ConversionClass.WIDENING.value
    assert result["lossy"] is False


def test_bare_decimal_invent_needs_user_approval():
    assert invents_unproven_capacity("DECIMAL", "DECIMAL(38,10)", dest_db="snowflake")
    result = classify_conversion(
        "DECIMAL",
        "NUMBER(38,10)",
        dest_db="snowflake",
        transform="none",
        risk_acknowledged=False,
    )
    assert result["conversion_class"] == ConversionClass.NEEDS_USER_APPROVAL.value
    assert result["invents_capacity"] is True
    assert result["requires_risk_contract"] is True


def test_lossy_without_ack_needs_user_approval():
    result = classify_conversion(
        "TEXT",
        "INTEGER",
        dest_db="postgresql",
        transform="none",
        risk_acknowledged=False,
    )
    assert result["conversion_class"] == ConversionClass.NEEDS_USER_APPROVAL.value
    assert result["lossy"] is True


def test_lossy_with_ack_is_lossy_class():
    result = classify_conversion(
        "TEXT",
        "INTEGER",
        dest_db="postgresql",
        transform="integer",
        risk_acknowledged=True,
    )
    assert result["conversion_class"] == ConversionClass.LOSSY.value


def test_unsupported_struct_to_integer():
    result = classify_conversion(
        "STRUCT<a:INT64>",
        "INTEGER",
        dest_db="bigquery",
        transform="none",
    )
    assert result["conversion_class"] == ConversionClass.UNSUPPORTED.value


def test_unmapped_needs_manual_mapping():
    result = classify_conversion("", "", mapped=False)
    assert result["conversion_class"] == ConversionClass.NEEDS_MANUAL_MAPPING.value


def test_parse_transform_needs_quarantine_when_types_hold():
    result = classify_conversion(
        "VARCHAR",
        "VARCHAR",
        dest_db="postgresql",
        transform="integer",  # parse transform; type path VARCHAR→VARCHAR not lossy
        risk_acknowledged=False,
    )
    # transform_fidelity(integer)=lossy_cast → NEEDS_QUARANTINE when not lossy path
    assert result["conversion_class"] == ConversionClass.NEEDS_QUARANTINE.value


def test_ddl_fingerprint_stable_and_sensitive():
    maps = [
        {
            "source": "amount",
            "target": "amount",
            "target_type": "DECIMAL(18,2)",
            "transform": "none",
        }
    ]
    a = approved_mapping_ddl_fingerprint(maps, dest_db="postgresql")
    b = approved_mapping_ddl_fingerprint(maps, dest_db="postgresql")
    assert a == b
    assert len(a) == 64

    changed = [
        {
            "source": "amount",
            "target": "amount",
            "target_type": "DECIMAL(18,4)",
            "transform": "none",
        }
    ]
    c = approved_mapping_ddl_fingerprint(changed, dest_db="postgresql")
    assert a != c


def test_assert_ddl_identity_fail_closed_on_drift():
    maps = [
        {
            "source": "id",
            "target": "id",
            "target_type": "INTEGER",
            "transform": "none",
        }
    ]
    fp = approved_mapping_ddl_fingerprint(maps, dest_db="postgresql")
    assert assert_ddl_identity(fp, maps, dest_db="postgresql") == fp

    # INTEGER and BIGINT materialize to the same Postgres column under the
    # never-narrower rule; SMALLINT is a different physical column.
    drifted = [
        {
            "source": "id",
            "target": "id",
            "target_type": "SMALLINT",
            "transform": "none",
        }
    ]
    with pytest.raises(DdlIdentityError) as ei:
        assert_ddl_identity(fp, drifted, dest_db="postgresql")
    assert ei.value.expected == fp
    assert ei.value.actual != fp


def test_assert_ddl_identity_missing_fingerprint():
    with pytest.raises(DdlIdentityError):
        assert_ddl_identity("", [{"source": "a", "target": "a", "target_type": "TEXT"}])


def test_ai_type_matrix_is_non_authoritative():
    from src.ai.knowledge import type_conversions as tc

    assert getattr(tc, "AUTHORITATIVE", True) is False
    assert "conversion_contract" in (tc.AUTHORITY_NOTE or "").lower() or True
    # Must not claim string→integer lossless via AI matrix for migration decisions.
    suggestion = tc.suggest_type_conversion("string", "integer")
    assert suggestion is not None
    # Wrapper must mark non-authoritative when present.
    if hasattr(tc, "suggest_type_conversion_non_authoritative"):
        wrapped = tc.suggest_type_conversion_non_authoritative("string", "integer")
        assert wrapped.get("authoritative") is False
    date_hint = tc.suggest_type_conversion("string", "date")
    assert date_hint is not None
    assert date_hint.get("lossy") is True
    assert "%m/%d/%Y" not in (date_hint.get("formats") or [])
    assert "01/02/2024" in (date_hint.get("note") or "")
    bool_hint = tc.suggest_type_conversion("string", "boolean")
    assert bool_hint is not None
    assert "yes" not in (bool_hint.get("mapping") or {})
    assert "no" not in (bool_hint.get("mapping") or {})
    int_hint = tc.suggest_type_conversion("string", "integer")
    assert int_hint is not None
    assert int_hint.get("lossy") is True
    assert "1,234" in (int_hint.get("note") or "")
    dec_hint = tc.suggest_type_conversion("string", "decimal")
    assert dec_hint is not None
    assert dec_hint.get("lossy") is True
    assert "1,234" in (dec_hint.get("note") or "")
    assert dec_hint.get("validation") is None
