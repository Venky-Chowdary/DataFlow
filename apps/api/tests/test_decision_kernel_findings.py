"""Canonical Validation Finding model — Decision Kernel (Validate SSOT)."""

from __future__ import annotations

from services.coercion_probe import analyze_coercion
from services.decision_kernel import (
    FailureClass,
    classify_transform_failure,
    findings_from_coercion_report,
    findings_from_population_fit,
    merge_validation_findings,
    rank_suggested_target_type,
)
from services.transform_resolver import resolve_transform


def test_fractional_to_int_classifies_as_precision_loss():
    fc = classify_transform_failure(
        "Invalid integer: '94.5'",
        source_type="FLOAT",
        target_type="INT",
        source_value="94.5",
    )
    assert fc is FailureClass.FRACTIONAL_PRECISION_LOSS
    # Gate prose alone (no source_value kwarg) must still recover the fraction.
    assert (
        classify_transform_failure("Invalid integer: '94.5'")
        is FailureClass.FRACTIONAL_PRECISION_LOSS
    )


def test_empty_datetime_classifies_as_nullability_policy():
    fc = classify_transform_failure(
        "Empty value cannot coerce to datetime",
        source_type="TEXT",
        target_type="DATETIME",
        source_value="",
    )
    assert fc is FailureClass.EMPTY_VALUE_NOT_NULLABLE


def test_rank_suggests_double_before_longtext_for_float_to_int():
    suggested = rank_suggested_target_type(
        source_type="FLOAT",
        target_type="INT",
        dest_db="mysql",
        failure_class=FailureClass.FRACTIONAL_PRECISION_LOSS,
        failure_examples=["94.5", "96.77777777777777"],
    )
    upper = suggested.upper()
    assert "DOUBLE" in upper or "FLOAT" in upper or "DECIMAL" in upper
    assert "LONGTEXT" not in upper
    assert upper not in {"TEXT", "VARCHAR", "STRING", "LONGTEXT"}


def test_empty_value_does_not_suggest_text_widen():
    suggested = rank_suggested_target_type(
        source_type="TEXT",
        target_type="DATETIME",
        dest_db="mysql",
        failure_class=FailureClass.EMPTY_VALUE_NOT_NULLABLE,
        failure_examples=[""],
    )
    assert suggested == ""


def test_coercion_probe_suggests_numeric_widen_for_ats_score():
    report = analyze_coercion(
        sample_rows=[
            {"metadata_atsScore": "90"},
            {"metadata_atsScore": "94.5"},
            {"metadata_atsScore": "96.77777777777777"},
        ],
        mappings=[
            {
                "source": "metadata_atsScore",
                "target": "metadata_ats_score",
                "create_new": True,
                "target_type": "INT",
            }
        ],
        source_types={"metadata_atsScore": "FLOAT"},
        dest_types={},
        dest_db_type="mysql",
        table_exists=False,
    )
    col = report["by_source"]["metadata_atsScore"]
    assert col["severity"] == "block"
    assert col["failure_class"] == FailureClass.FRACTIONAL_PRECISION_LOSS.value
    suggested = (col.get("suggested_target_type") or "").upper()
    assert "DOUBLE" in suggested or "FLOAT" in suggested or "DECIMAL" in suggested
    assert "LONGTEXT" not in suggested


def test_widen_to_longtext_clears_stale_integer_transform_without_contract():
    """Operator Widen to LONGTEXT must not keep cast_integer (Validate lie)."""
    transform = resolve_transform(
        {
            "source": "metadata_atsScore",
            "target": "metadata_ats_score",
            "target_type": "LONGTEXT",
            "transform": "cast_integer",
            "user_override": True,
            "create_new": True,
        },
        column_types={"metadata_atsScore": "FLOAT"},
    )
    assert transform == "none"


def test_cast_and_continue_keeps_integer_on_text_stamp():
    from services.migration_risk_contract import create_migration_risk_contract

    rc = create_migration_risk_contract(
        column="amt",
        source_type="TEXT",
        destination_type="TEXT",
        approved_by="test@dataflow.app",
        reason="TEXT stamp with intentional integer cast under CAST_AND_CONTINUE",
        execution_policy="CAST_AND_CONTINUE",
        transform="integer",
    ).to_dict()
    transform = resolve_transform(
        {
            "source": "amt",
            "target": "amt",
            "target_type": "TEXT",
            "transform": "integer",
            "user_override": True,
            "risk_contract": rc,
        },
        column_types={"amt": "TEXT"},
    )
    assert transform == "integer"


def test_findings_from_coercion_report_stamps_canonical_ssot():
    report = analyze_coercion(
        sample_rows=[
            {"metadata_atsScore": "90"},
            {"metadata_atsScore": "94.5"},
        ],
        mappings=[
            {
                "source": "metadata_atsScore",
                "target": "metadata_ats_score",
                "create_new": True,
                "target_type": "INT",
            }
        ],
        source_types={"metadata_atsScore": "FLOAT"},
        dest_types={},
        dest_db_type="mysql",
        table_exists=False,
    )
    findings = findings_from_coercion_report(report, dest_db="mysql")
    assert findings, "blocking coercion must emit canonical findings"
    top = findings[0]
    assert top["failure_class"] == FailureClass.FRACTIONAL_PRECISION_LOSS.value
    assert top["schema_version"] == "validation_finding_v1"
    assert top["blocking"] is True
    suggested = (top.get("suggested_target_type") or "").upper()
    assert "DOUBLE" in suggested or "FLOAT" in suggested or "DECIMAL" in suggested
    assert "LONGTEXT" not in suggested


def test_population_fit_overflow_classifies_and_widens_dest_number():
    """flights-1m class: 'does not fit NUMBER(9,6)' is OVERFLOW, not a cast."""
    assert (
        classify_transform_failure(
            "1 value(s) in 'DEP_TIME' do not fit DEP_TIME NUMBER(9,6) "
            "(first at row 293) — decimal does not fit NUMBER(9,6)",
            target_type="NUMBER(9,6)",
            source_value="7.9166665",
        )
        is FailureClass.OVERFLOW
    )
    assert (
        classify_transform_failure(
            "value exceeds NUMBER(9,6)",
            target_type="NUMBER(9,6)",
        )
        is FailureClass.OVERFLOW
    )
    suggested = rank_suggested_target_type(
        source_type="NUMBER(9,6)",
        target_type="NUMBER(9,6)",
        dest_db="snowflake",
        failure_class=FailureClass.OVERFLOW,
        failure_examples=["7.9166665"],
    )
    assert suggested == "NUMBER(10,7)"


def test_rank_does_not_invent_widen_for_auto_ambiguous_decimal():
    """Auto ``1.234`` has no write-path bind — do not stamp a dest widen."""
    suggested = rank_suggested_target_type(
        source_type="NUMBER(9,6)",
        target_type="NUMBER(9,6)",
        dest_db="snowflake",
        failure_class=FailureClass.OVERFLOW,
        failure_examples=["1.234"],
    )
    assert suggested != "NUMBER(10,7)"
    assert "NUMBER(8," not in (suggested or "")


def test_varchar_and_integer_do_not_fit_classify_to_the_right_class():
    assert (
        classify_transform_failure(
            "1 value(s) in 'code' do not fit code VARCHAR(8) (first at row 39)",
            target_type="VARCHAR(8)",
            source_value="XXXXXXXXXXXXXXXXXXXX",
        )
        is FailureClass.LENGTH_OVERFLOW
    )
    assert (
        classify_transform_failure(
            "1 value(s) in 'qty' do not fit qty INTEGER (first at row 39)",
            target_type="INTEGER",
            source_value="99999999999",
        )
        is FailureClass.OVERFLOW
    )


def test_findings_from_population_fit_stamps_dest_widen():
    report = {
        "evidence": "exact",
        "findings": [
            {
                "source": "DEP_TIME",
                "target": "DEP_TIME",
                "target_type": "NUMBER(9,6)",
                "unfit_rows": 1,
                "example_rows": [293],
                "example_values": ["7.9166665"],
                "aborts_job": True,
                "reason": (
                    "1 value(s) in 'DEP_TIME' do not fit DEP_TIME NUMBER(9,6) "
                    "(first at row 293)"
                ),
                "suggested_target_type": "NUMBER(10,7)",
                "suggested_fix": (
                    "Open Map → widen DEP_TIME to NUMBER(10,7) "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently truncate."
                ),
            }
        ],
    }
    findings = findings_from_population_fit(report, dest_db="snowflake")
    assert len(findings) == 1
    top = findings[0]
    assert top["failure_class"] == FailureClass.OVERFLOW.value
    assert top["suggested_target_type"] == "NUMBER(10,7)"
    assert top["row_number"] == 293
    assert "g3f_population_fit" in top["gate_ids"]
    assert "NUMBER(10,7)" in top["recommended_action"]
    assert "truncate" in top["recommended_action"].lower()


def test_fractional_into_zero_scale_number_is_not_overflow():
    assert (
        classify_transform_failure(
            "fractional value 22.433332 is not an integer for NUMBER(38,0)",
            target_type="NUMBER(38,0)",
            source_value="22.433332",
        )
        is FailureClass.FRACTIONAL_PRECISION_LOSS
    )
    suggested = rank_suggested_target_type(
        source_type="DECIMAL(13,8)",
        target_type="INT",
        dest_db="mysql",
        failure_class=FailureClass.FRACTIONAL_PRECISION_LOSS,
        failure_examples=["22.433332"],
    )
    assert "DOUBLE" in suggested.upper() or "FLOAT" in suggested.upper()
    assert "LONGTEXT" not in suggested.upper()


def test_integer_range_overflow_ranks_bigint_not_text():
    suggested = rank_suggested_target_type(
        source_type="BIGINT",
        target_type="SMALLINT",
        dest_db="postgresql",
        failure_class=FailureClass.OVERFLOW,
        failure_examples=["40000"],
    )
    assert suggested == "BIGINT"


def test_merge_prefers_population_fit_widen_over_preview_coercion():
    coercion = [
        {
            "source_column": "DEP_TIME",
            "target_column": "DEP_TIME",
            "failure_class": FailureClass.UNKNOWN.value,
            "suggested_target_type": "VARCHAR",
            "gate_ids": ["g3_coercion"],
        }
    ]
    population = [
        {
            "source_column": "DEP_TIME",
            "target_column": "DEP_TIME",
            "failure_class": FailureClass.OVERFLOW.value,
            "suggested_target_type": "NUMBER(10,7)",
            "gate_ids": ["g3f_population_fit"],
        }
    ]
    merged = merge_validation_findings(coercion, population)
    assert len(merged) == 1
    assert merged[0]["suggested_target_type"] == "NUMBER(10,7)"
    assert merged[0]["failure_class"] == FailureClass.OVERFLOW.value


def test_enum_domain_do_not_fit_classifies_as_cast_not_overflow():
    assert (
        classify_transform_failure(
            "1 value(s) in 'status' do not fit status ENUM('active','inactive') "
            "(first at row 3) — value not in ENUM domain — refuse invent: 'late'",
            target_type="ENUM('active','inactive')",
            source_value="late",
        )
        is FailureClass.TYPE_CAST_FAILURE
    )
    report = {
        "evidence": "exact",
        "findings": [
            {
                "source": "status",
                "target": "status",
                "target_type": "ENUM('active','inactive')",
                "unfit_rows": 1,
                "example_rows": [3],
                "example_values": ["late"],
                "reason": (
                    "1 value(s) in 'status' do not fit status "
                    "ENUM('active','inactive') (first at row 3) — "
                    "value not in ENUM domain — refuse invent: 'late'"
                ),
                "suggested_target_type": "ENUM('active','inactive','late')",
                "suggested_fix": (
                    "Open Map → widen status to ENUM('active','inactive','late') "
                    "(or ALTER the destination) → re-Validate. "
                    "Do not silently store '' / drop SET members."
                ),
            }
        ],
    }
    findings = findings_from_population_fit(report, dest_db="mysql")
    assert findings[0]["failure_class"] == FailureClass.TYPE_CAST_FAILURE.value
    assert findings[0]["suggested_target_type"] == "ENUM('active','inactive','late')"
    assert "VARCHAR" not in findings[0]["suggested_target_type"]
    assert "silently store" in findings[0]["recommended_action"]


def test_interval_family_mismatch_classifies_as_cast():
    assert (
        classify_transform_failure(
            "interval family mismatch wire=ym dest=ds — YEAR-MONTH ↔ DAY-SECOND collapse",
            target_type="INTERVAL DAY TO SECOND",
            source_value="P1Y2M",
        )
        is FailureClass.TYPE_CAST_FAILURE
    )


def test_year_out_of_range_classifies_as_cast_not_overflow():
    assert (
        classify_transform_failure(
            "YEAR 1899 outside 0 or 1901–2155 — refuse invent (MySQL would store 0000)",
            target_type="YEAR",
            source_value="1899",
        )
        is FailureClass.TYPE_CAST_FAILURE
    )


def test_binary_length_classifies_as_length_overflow():
    assert (
        classify_transform_failure(
            "1 value(s) in 'blob' do not fit blob BINARY(2) — binary length 4 exceeds BINARY(2)",
            target_type="BINARY(2)",
            source_value="YWJjZA==",
        )
        is FailureClass.LENGTH_OVERFLOW
    )
