"""A bounded destination carrier is decided before the write, not at row 431.

The defect these cover: a 1M-row CSV mapped ``DECIMAL(12,9) → NUMBER(11,8)``
passed Validate on 25 preview rows and then failed the load with zero rows
committed, because 27 values deeper in the file cannot fit the carrier. Fit was
decidable the whole time — nothing sampled it.

Every assertion here uses the same predicates the writers use
(``fits_decimal`` / ``fits_varchar`` / ``fits_integer``), so a dialect change in
the write path cannot silently disagree with what Validate proved.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from services.population_fit_scan import (
    EVIDENCE_EXACT,
    EVIDENCE_PARTIAL,
    EVIDENCE_SAMPLED,
    EVIDENCE_UNMEASURED,
    GATE_ID,
    bounded_targets,
    build_population_fit_gate,
    scan_population_fit,
)

# The production mapping, verbatim: a signed continue-policy contract is absent,
# so the resolved action for a fit failure is fail-closed.
NARROWING = [{"source": "arr_time", "target": "arr_time", "target_type": "NUMBER(11,8)"}]
SOURCE_TYPES = {"arr_time": "DECIMAL(12,9)"}


def _rows(count: int, *, unfit_at: tuple[int, ...] = ()) -> list[dict[str, object]]:
    """``count`` fitting rows, with an over-precision value at each 1-based index."""
    out: list[dict[str, object]] = []
    for i in range(1, count + 1):
        value = "9999.99999999" if i in unfit_at else "12.34567890"
        out.append({"arr_time": value})
    return out


def _scan(rows, **kw):
    params = {
        "dest_types": {"arr_time": "NUMBER(11,8)"},
        "source_types": SOURCE_TYPES,
        "dest_db": "snowflake",
        "dialect_label": "snowflake",
        "job_error_policy": "fail",
        "rows_are_population": True,
    }
    params.update(kw)
    return scan_population_fit(rows, NARROWING, **params)


def test_late_unfit_row_is_found_when_the_preview_is_clean() -> None:
    """The exact production shape: clean head, unfit value far down the file."""
    rows = _rows(1_000, unfit_at=(431,))

    preview = _scan(rows[:25], rows_are_population=False, rows_total=1_000)
    assert preview.findings == ()
    assert preview.evidence == EVIDENCE_SAMPLED
    assert build_population_fit_gate(preview)["status"] == "warn"
    assert "not population-proven" in build_population_fit_gate(preview)["message"]

    full = _scan(rows, rows_total=1_000)
    assert full.evidence == EVIDENCE_EXACT
    assert [f.unfit_rows for f in full.findings] == [1]
    assert full.findings[0].example_rows == (431,)
    assert full.findings[0].example_values == ("9999.99999999",)


def test_fail_closed_policy_blocks_execute_with_the_offending_rows() -> None:
    report = _scan(_rows(600, unfit_at=(431, 433, 599)), rows_total=600)
    gate = build_population_fit_gate(report)

    assert gate["id"] == GATE_ID
    assert gate["status"] == "block"
    assert "3 value(s)" in gate["message"]
    assert "all 600 source row(s)" in gate["message"]
    assert gate["details"]["findings"][0]["example_rows"] == [431, 433, 599]
    assert "Widen the destination column" in gate["details"]["corrective_action"]


def _signed_contract(execution_policy: str) -> dict[str, object]:
    """A verifiable contract — the scan resolves policy through the same SSOT the
    writer uses, so an unsigned or tampered body must not grant anything."""
    from services.migration_risk_contract import sign_risk_contract

    body: dict[str, object] = {
        "risk_id": "fidelity_collapse",
        "severity": "high",
        "root_cause": "fidelity_collapse",
        "column": "arr_time",
        "source_type": "DECIMAL(12,9)",
        "destination_type": "NUMBER(11,8)",
        "transform": None,
        "rows_sampled": 25,
        "estimated_rows": 600,
        "expected_failure_pct": 0.3,
        "expected_precision_loss": True,
        "expected_truncation": False,
        "expected_nulls": False,
        "execution_policy": execution_policy,
        "quarantine_policy": "DLQ",
        "retry_policy": "NONE",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "admin@dataflow.app",
        "approved_at": "2026-08-17T00:00:00Z",
        "reason": "Declared fidelity collapse accepted for this load",
        "target": "arr_time",
    }
    body["signature"] = sign_risk_contract(body)
    return body


def test_signed_continue_policy_forecasts_quarantine_instead_of_blocking() -> None:
    """A signed continue-policy contract is authority to hold rows out, not to
    discover them at write time — the count is stated up front."""
    mapping = {
        "source": "arr_time",
        "target": "arr_time",
        "target_type": "NUMBER(11,8)",
        "risk_contract": _signed_contract("QUARANTINE_ROW"),
    }
    report = scan_population_fit(
        _rows(600, unfit_at=(431, 433)),
        [mapping],
        dest_types={"arr_time": "NUMBER(11,8)"},
        source_types=SOURCE_TYPES,
        dest_db="snowflake",
        job_error_policy="quarantine",
        rows_total=600,
        rows_are_population=True,
    )
    gate = build_population_fit_gate(report)

    assert report.aborting_findings == ()
    assert gate["status"] == "warn"
    assert "held out in quarantine" in gate["message"]
    assert "2 row(s)" in gate["message"]


def test_signed_fail_job_contract_still_blocks() -> None:
    mapping = {
        "source": "arr_time",
        "target": "arr_time",
        "target_type": "NUMBER(11,8)",
        "risk_contract": _signed_contract("FAIL_JOB"),
    }
    report = scan_population_fit(
        _rows(600, unfit_at=(431,)),
        [mapping],
        dest_types={"arr_time": "NUMBER(11,8)"},
        source_types=SOURCE_TYPES,
        dest_db="snowflake",
        job_error_policy="quarantine",
        rows_total=600,
        rows_are_population=True,
    )
    assert build_population_fit_gate(report)["status"] == "block"


def test_tampered_contract_cannot_downgrade_a_finding_to_a_warning() -> None:
    contract = dict(_signed_contract("QUARANTINE_ROW"))
    contract["execution_policy"] = "SKIP_ROW"  # body changed after signing
    mapping = {
        "source": "arr_time",
        "target": "arr_time",
        "target_type": "NUMBER(11,8)",
        "risk_contract": contract,
    }
    report = scan_population_fit(
        _rows(600, unfit_at=(431,)),
        [mapping],
        dest_types={"arr_time": "NUMBER(11,8)"},
        source_types=SOURCE_TYPES,
        dest_db="snowflake",
        job_error_policy="quarantine",
        rows_total=600,
        rows_are_population=True,
    )
    assert build_population_fit_gate(report)["status"] == "block"


def test_clean_population_scan_passes_and_says_it_scanned_everything() -> None:
    report = _scan(_rows(5_000), rows_total=5_000)
    gate = build_population_fit_gate(report)

    assert report.evidence == EVIDENCE_EXACT
    assert report.findings == ()
    assert gate["status"] == "pass"
    assert "5000 source row(s)" in gate["message"]


def test_no_rows_is_unmeasured_never_proof_of_fit() -> None:
    report = _scan([], rows_total=1_000_000)
    gate = build_population_fit_gate(report)

    assert report.evidence == EVIDENCE_UNMEASURED
    assert report.scanned_population is False
    assert gate["status"] == "warn"
    assert "unmeasured" in gate["message"]


def test_budget_truncation_downgrades_the_caller_population_claim() -> None:
    report = _scan(_rows(50, unfit_at=(49,)), rows_total=50, budget=10)

    assert report.evidence == EVIDENCE_PARTIAL
    assert report.rows_scanned == 10
    assert report.findings == ()  # the unfit row was never reached
    gate = build_population_fit_gate(report)
    assert gate["status"] == "warn"
    assert "10 of 50" in gate["message"]
    assert "budget" in gate["message"]


def test_time_budget_stops_a_million_row_walk_and_stays_partial() -> None:
    """Studio 1M CSV must not sit in GET /preflight for minutes.

    The predicates stay the write path's. The deadline is honesty: remaining
    rows are unproven, never silently treated as exact.
    """
    import time

    ticks: list[int] = []

    def _slow_rows():
        for i in range(1, 50_001):
            time.sleep(0.002)
            yield {"arr_time": "12.34567890"}

    report = _scan(
        _slow_rows(),
        rows_total=1_000_000,
        deadline_monotonic=time.monotonic() + 0.03,
        on_progress=ticks.append,
    )
    assert report.evidence == EVIDENCE_PARTIAL
    assert report.truncated_reason == "time"
    assert 0 < report.rows_scanned < 50_000
    assert ticks, "worker must heartbeat so GET can show live rows"
    assert report.duration_ms >= 20
    gate = build_population_fit_gate(report)
    assert gate["status"] == "warn"
    assert "time budget" in gate["message"]
    assert gate["duration_ms"] == report.duration_ms


def test_narrowing_integral_digits_are_scanned_even_when_scale_widens() -> None:
    """``DECIMAL(9,4) → NUMBER(11,8)`` widens scale but leaves only 3 integral
    digits, so it can still overflow — declaration does not decide it."""
    targets, _, safe = bounded_targets(
        [{"source": "c", "target": "c", "target_type": "NUMBER(11,8)"}],
        source_types={"c": "DECIMAL(9,4)"},
        dest_db="snowflake",
    )
    assert safe == ()
    assert [t.target for t in targets] == ["c"]


def test_widening_and_identical_declarations_need_no_value_scan() -> None:
    for source_type, target_type in (
        ("DECIMAL(11,8)", "NUMBER(11,8)"),
        ("DECIMAL(9,7)", "NUMBER(11,8)"),
        ("VARCHAR(50)", "VARCHAR(200)"),
        ("VARCHAR(50)", "VARCHAR(50)"),
        ("SMALLINT", "BIGINT"),
    ):
        targets, undecidable, safe = bounded_targets(
            [{"source": "c", "target": "c", "target_type": target_type}],
            source_types={"c": source_type},
            dest_db="postgresql",
            source_kind="database",
            source_format="postgresql",
        )
        assert targets == (), f"{source_type} → {target_type} should not be scanned"
        assert safe == ("c",), f"{source_type} → {target_type} should be declared safe"
        assert undecidable == ()


def test_a_declaration_safe_plan_reads_no_rows_at_all() -> None:
    """Laziness is load-bearing: an ordinary transfer must not pay for a scan."""

    def exploding_rows():
        raise AssertionError("the scan must not read rows it does not need")
        yield  # pragma: no cover

    report = scan_population_fit(
        exploding_rows(),
        [{"source": "c", "target": "c", "target_type": "NUMBER(11,8)"}],
        source_types={"c": "NUMBER(11,8)"},
        dest_db="snowflake",
        source_kind="database",
        source_format="snowflake",
        rows_are_population=True,
    )
    assert report.targets == ()
    assert build_population_fit_gate(report)["status"] == "pass"


def test_unbounded_source_declaration_is_still_scanned() -> None:
    """A bare DECIMAL/TEXT source decides nothing — unknown never means safe."""
    targets, _, safe = bounded_targets(
        [{"source": "c", "target": "c", "target_type": "NUMBER(11,8)"}],
        source_types={"c": "DECIMAL"},
        dest_db="snowflake",
    )
    assert safe == ()
    assert [t.target for t in targets] == ["c"]


def test_undecidable_target_carrier_is_reported_not_assumed_safe() -> None:
    targets, undecidable, safe = bounded_targets(
        [{"source": "c", "target": "c", "target_type": "VARIANT"}],
        source_types={"c": "VARCHAR(10)"},
        dest_db="snowflake",
    )
    assert targets == ()
    assert undecidable == ("c",)
    assert safe == ()


def test_nulls_and_blanks_are_a_nullability_question_not_a_fit_question() -> None:
    rows = [{"arr_time": None}, {"arr_time": ""}, {"arr_time": "   "}]
    report = _scan(rows, rows_total=3)

    assert report.findings == ()
    assert report.evidence == EVIDENCE_EXACT


@pytest.mark.parametrize(
    "value,fits",
    [
        ("999.99999999", True),  # exactly 11 digits, 8 of them fractional
        ("9999.99999999", False),  # integral digits exceed precision - scale
        ("-9999.99999999", False),
        ("123.456789012", False),  # scale exceeds 8 on Snowflake
        ("-123.456789012", False),
        ("999.99999999e-3", False),  # exponent pushes scale past 8
        ("123.12345678", True),  # exact fit at the boundary
        ("-123.12345678", True),
        (Decimal("123.12345678"), True),
        (Decimal("1234.12345678"), False),
        (0, True),
    ],
)
def test_snowflake_number_boundaries_match_the_writer(value, fits: bool) -> None:
    report = _scan([{"arr_time": value}], rows_total=1)
    assert (report.findings == ()) is fits, f"{value!r} on NUMBER(11,8)"


def test_postgres_numeric_rounds_scale_where_the_writer_rounds() -> None:
    """Postgres rounds to scale on write, so extra scale is not an overflow —
    the scan must not invent a failure the writer would never raise."""
    report = scan_population_fit(
        [{"c": "123.123456789012"}],
        [{"source": "c", "target": "c", "target_type": "NUMERIC(11,8)"}],
        source_types={"c": "DECIMAL"},
        dest_db="postgresql",
        dialect_label="postgresql",
        job_error_policy="fail",
        rows_total=1,
        rows_are_population=True,
    )
    assert report.findings == ()

    overflow = scan_population_fit(
        [{"c": "1234.12345678"}],
        [{"source": "c", "target": "c", "target_type": "NUMERIC(11,8)"}],
        source_types={"c": "DECIMAL"},
        dest_db="postgresql",
        job_error_policy="fail",
        rows_total=1,
        rows_are_population=True,
    )
    assert [f.unfit_rows for f in overflow.findings] == [1]


def test_mysql_decimal_overflow_is_found() -> None:
    report = scan_population_fit(
        [{"c": "12.3"}, {"c": "12345678.9"}],
        [{"source": "c", "target": "c", "target_type": "DECIMAL(6,2)"}],
        source_types={"c": "DECIMAL"},
        dest_db="mysql",
        dialect_label="mysql",
        job_error_policy="fail",
        rows_total=2,
        rows_are_population=True,
    )
    assert [f.unfit_rows for f in report.findings] == [1]
    assert report.findings[0].example_rows == (2,)


def test_bounded_string_overflow_is_found_per_dialect() -> None:
    for dest_db in ("snowflake", "postgresql", "mysql"):
        report = scan_population_fit(
            [{"c": "ok"}, {"c": "x" * 300}],
            [{"source": "c", "target": "c", "target_type": "VARCHAR(255)"}],
            source_types={"c": "TEXT"},
            dest_db=dest_db,
            dialect_label=dest_db,
            job_error_policy="fail",
            rows_total=2,
            rows_are_population=True,
        )
        assert [f.unfit_rows for f in report.findings] == [1], dest_db
        assert report.findings[0].example_rows == (2,), dest_db


def test_integer_overflow_is_found() -> None:
    report = scan_population_fit(
        [{"c": 32_767}, {"c": 40_000}, {"c": -40_000}],
        [{"source": "c", "target": "c", "target_type": "SMALLINT"}],
        source_types={"c": "BIGINT"},
        dest_db="postgresql",
        job_error_policy="fail",
        rows_total=3,
        rows_are_population=True,
    )
    assert [f.unfit_rows for f in report.findings] == [2]
    assert report.findings[0].example_rows == (2, 3)
    assert report.findings[0].suggested_target_type == "BIGINT"
    assert "BIGINT" in (report.findings[0].suggested_fix or "")


@pytest.mark.parametrize(
    "dest_db,int_type",
    [
        ("mysql", "INT"),
        ("postgresql", "INTEGER"),
        ("snowflake", "NUMBER(38,0)"),
        ("sqlserver", "INT"),
        ("oracle", "NUMBER(10,0)"),
    ],
)
def test_a_decimal_population_into_an_integer_carrier_blocks_before_the_write(
    dest_db: str, int_type: str
) -> None:
    """The 1M-row MySQL abort, decided at Validate for every connector family.

    ``ARR_TIME`` holds fractional hours; the destination column is an integer.
    The write refuses every fractional cell ("Invalid integer: '22.433332'"),
    so this must never reach Execute — and the finding must say *fractional*,
    not "overflow", because widening the carrier is the fix, not a bigger int.
    """
    rows = [
        {"arr_time": "1000"},
        {"arr_time": "22.433332"},
        {"arr_time": "22.05"},
        {"arr_time": "21.833334"},
        {"arr_time": "1130"},
    ]
    report = scan_population_fit(
        rows,
        [{"source": "arr_time", "target": "ARR_TIME", "target_type": int_type}],
        source_types={"arr_time": "DECIMAL(13,8)"},
        dest_db=dest_db,
        dialect_label=dest_db,
        job_error_policy="fail",
        rows_total=len(rows),
        rows_are_population=True,
    )
    assert [f.unfit_rows for f in report.findings] == [3], dest_db
    assert report.findings[0].example_rows == (2, 3, 4), dest_db
    assert "fractional" in report.findings[0].unfit_reason, dest_db
    suggested = (report.findings[0].suggested_target_type or "").upper()
    assert suggested in {"DOUBLE", "FLOAT", "FLOAT64", "DECIMAL", "NUMERIC"}, dest_db
    assert "INT" not in suggested.replace("FLOAT", ""), dest_db

    gate = build_population_fit_gate(report)
    assert gate["status"] == "block", dest_db
    assert "3 value(s)" in gate["message"], dest_db


def test_zero_scale_number_sees_locale_money_the_write_path_binds() -> None:
    """$1,234.56 / €1.234,56 are fractional; Auto 1,234 and $1,234 are not."""
    from services.transform_engine import is_fractional_wire_value

    assert is_fractional_wire_value("$1,234.56") is True
    assert is_fractional_wire_value("€1.234,56") is True
    assert is_fractional_wire_value("$1,234") is False
    assert is_fractional_wire_value("1,234") is False
    blocked = scan_population_fit(
        [{"c": "$1,234.56"}, {"c": "€1.234,56"}, {"c": "1000"}],
        [{"source": "c", "target": "c", "target_type": "NUMBER(38,0)"}],
        source_types={"c": "DECIMAL"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_total=3,
        rows_are_population=True,
    )
    assert [f.unfit_rows for f in blocked.findings] == [2]
    assert "fractional" in blocked.findings[0].unfit_reason
    whole = scan_population_fit(
        [{"c": "2000.00"}, {"c": "1000"}, {"c": "1e3"}],
        [{"source": "c", "target": "c", "target_type": "NUMBER(38,0)"}],
        source_types={"c": "DECIMAL"},
        dest_db="snowflake",
        dialect_label="snowflake",
        job_error_policy="fail",
        rows_total=3,
        rows_are_population=True,
    )
    assert whole.findings == ()


def test_an_integral_population_still_lands_in_an_integer_carrier() -> None:
    """Integral decimals and scientific notation are what the writer accepts."""
    report = scan_population_fit(
        [{"c": "1000"}, {"c": "22.0"}, {"c": "1e3"}, {"c": 2200}],
        [{"source": "c", "target": "c", "target_type": "INT"}],
        source_types={"c": "DECIMAL(13,8)"},
        dest_db="mysql",
        job_error_policy="fail",
        rows_total=4,
        rows_are_population=True,
    )
    assert report.findings == ()
    assert build_population_fit_gate(report)["status"] == "pass"


def test_a_signed_continue_contract_forecasts_the_held_out_fractional_rows() -> None:
    """An approved lossy policy holds the rows out — it never truncates them."""
    contract = _signed_contract("QUARANTINE_ROW")
    contract["column"] = "arr_time"
    contract["target"] = "arr_time"
    contract["source_type"] = "DECIMAL(13,8)"
    contract["destination_type"] = "INT"
    from services.migration_risk_contract import sign_risk_contract

    contract.pop("signature", None)
    contract["signature"] = sign_risk_contract(contract)

    report = scan_population_fit(
        [{"arr_time": "1000"}, {"arr_time": "22.433332"}],
        [
            {
                "source": "arr_time",
                "target": "arr_time",
                "target_type": "INT",
                "risk_contract": contract,
            }
        ],
        source_types={"arr_time": "DECIMAL(13,8)"},
        dest_db="mysql",
        job_error_policy="quarantine",
        rows_total=2,
        rows_are_population=True,
    )
    gate = build_population_fit_gate(report)
    assert gate["status"] == "warn"
    assert "1 row(s) will be held out" in gate["message"]


def test_intentionally_omitted_column_is_not_scanned() -> None:
    targets, _, _ = bounded_targets(
        [
            {
                "source": "arr_time",
                "target": "arr_time",
                "target_type": "NUMBER(11,8)",
                "intentional_omit": True,
            }
        ],
        source_types=SOURCE_TYPES,
        dest_db="snowflake",
    )
    assert targets == ()


def test_report_payload_carries_the_evidence_for_the_ui() -> None:
    payload = _scan(_rows(100, unfit_at=(7,)), rows_total=100).to_dict()

    assert payload["evidence"] == EVIDENCE_EXACT
    assert payload["scanned_population"] is True
    assert payload["rows_scanned"] == 100
    assert payload["unfit_rows"] == 1
    assert payload["bounded_columns"][0]["target_type"] == "NUMBER(11,8)"
    finding = payload["findings"][0]
    assert finding["example_rows"] == [7]
    assert "do not fit" in finding["reason"]
