from decimal import Decimal

from services.reconciliation import reconcile


def test_reconcile_pass():
    r = reconcile(source_rows=10, target_rows=10, source_checksum="abc", target_checksum="abc")
    assert r.passed
    assert "checksums match" in r.message
    d = r.to_dict()
    assert d["phase"] == "post_write_verified"
    assert d["post_write_pending"] is False


def test_canonicalize_ieee_float_matches_decimal_sink():
    """Excel IEEE residue must not false-fail Gate-8 vs DECIMAL 106.6."""
    from services.reconciliation import normalize_cell, sample_compare_rows

    assert normalize_cell(106.60000000000001) == "106.6"
    assert normalize_cell("106.60000000000001") == "106.6"
    assert normalize_cell("106.6") == "106.6"

    cmp = sample_compare_rows(
        [{"id": "1", "Total": 106.60000000000001}],
        [{"id": "1", "total": "106.6"}],
        [{"source": "Total", "target": "total"}],
        sort_key="id",
    )
    assert cmp["passed"] is True
    assert cmp["compared"] >= 1


def test_stamp_writer_ack_phase():
    from services.reconciliation import stamp_post_write_phase

    stamped = stamp_post_write_phase({
        "passed": True,
        "message": "Transfer verified by writer: 10 rows written (read-back verifier not available)",
        "source_rows": 10,
        "target_rows": 10,
        "source_checksum": "abc",
        "target_checksum": "",
    })
    assert stamped["phase"] == "post_write_writer_ack"
    assert stamped["post_write_pending"] is False
    assert stamped["assurance_level"] == "writer_ack"


def test_stamp_file_object_export_unproven_not_writer_ack():
    """File/object export message must not false-green as writer_ack / verified."""
    from services.reconciliation import stamp_post_write_phase

    stamped = stamp_post_write_phase({
        "passed": True,
        "unproven": True,
        "skipped_readback": True,
        "message": (
            "File/object export wrote successfully — Gate-8 cell fidelity "
            "unproven (no destination read-back). Writer checksum present (abc123…) — count/bytes only."
        ),
        "source_rows": 1,
        "target_rows": 1,
        "checksum": "abc123checksum",
    })
    assert stamped["phase"] == "post_write_skipped"
    assert stamped["assurance_level"] == "none"
    assert stamped["coverage"] == "none"
    assert stamped["unproven"] is True
    assert stamped["migration_proven"] is False


def test_reconcile_row_mismatch():
    r = reconcile(source_rows=10, target_rows=9, source_checksum="abc", target_checksum="abc")
    assert not r.passed
    assert "mismatch" in r.message.lower()


def test_reconcile_allows_quarantined_rows():
    r = reconcile(
        source_rows=10,
        target_rows=8,
        source_checksum="abc",
        target_checksum="abc",
        rejected_rows=2,
    )
    assert r.passed
    assert r.rejected_rows == 2


def test_reconcile_accounts_skipped_rows():
    # 10 source rows, 2 stale CDC redeliveries skipped, 1 rejected -> 7 expected.
    r = reconcile(
        source_rows=10,
        target_rows=7,
        source_checksum="abc",
        target_checksum="abc",
        rejected_rows=1,
        rows_skipped=2,
    )
    assert r.passed
    assert r.rows_skipped == 2


def test_reconcile_fails_sample_mismatch():
    r = reconcile(
        source_rows=2,
        target_rows=2,
        source_checksum="abc",
        target_checksum="abc",
        sample_compare={
            "passed": False,
            "mismatches": [{"row": "0", "source": "id", "target": "id", "source_value": "1", "target_value": "2"}],
        },
    )
    assert not r.passed
    assert "read-back" in r.message.lower()


def test_reconcile_fails_checksum_mismatch_strict():
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        strict_checksum=True,
    )
    assert not r.passed
    assert "checksum" in r.message.lower()


def test_reconcile_refuses_unverified_checksum_mismatch_balanced():
    """Balanced must not soft-pass checksum drift without sample proof."""
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        strict_checksum=False,
    )
    assert not r.passed
    assert "compared>0" in r.message or "sample" in r.message.lower()


def test_reconcile_balanced_fails_even_with_key_aligned_sample():
    """GA: sample diagnostics never green-pass a checksum mismatch."""
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        strict_checksum=False,
        sample_compare={"passed": True, "compared": 5, "mismatches": []},
    )
    assert not r.passed
    assert "checksum mismatch" in r.message.lower()
    assert "diagnostic" in r.message.lower() or "cannot override" in r.message.lower()
    stamped = r.to_dict()
    assert stamped["phase"] == "post_write_failed"
    assert stamped.get("checksum_match") is False
    assert (stamped.get("sample_compare") or {}).get("compared") == 5


def test_stamp_fails_when_checksums_diverge_even_if_sample_ok():
    """GA: diverging digests force failed — sample cannot soft-verify."""
    from services.reconciliation import stamp_post_write_phase

    out = stamp_post_write_phase(
        {
            "passed": True,
            "source_checksum": "aaa",
            "target_checksum": "bbb",
            "message": "Transfer completed successfully",
            "sample_compare": {"passed": True, "compared": 3, "mismatches": []},
        }
    )
    assert out["passed"] is False
    assert out["phase"] == "post_write_failed"
    assert out.get("coverage") == "none"


def test_stamp_full_checksum_coverage_when_checksums_match():
    from services.reconciliation import stamp_post_write_phase

    out = stamp_post_write_phase(
        {
            "passed": True,
            "source_checksum": "aaa",
            "target_checksum": "aaa",
            "message": "ok",
            "sample_compare": {"passed": True, "compared": 3},
        }
    )
    assert out["phase"] == "post_write_verified"
    assert out.get("coverage") == "full_checksum"


def test_aggregate_checksum_order_independent():
    from services.reconciliation import aggregate_checksum

    rows_a = [{"id": "1", "amt": "10"}, {"id": "2", "amt": "20"}]
    rows_b = [{"id": "2", "amt": "20"}, {"id": "1", "amt": "10"}]
    assert aggregate_checksum(rows_a, ["id", "amt"]) == aggregate_checksum(rows_b, ["id", "amt"])


def test_sample_compare_rows_detects_mismatch():
    from services.reconciliation import sample_compare_rows

    result = sample_compare_rows(
        [{"id": "1", "name": "Alice"}],
        [{"id": "2", "name": "Alice"}],
        [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        rows_are_paired=True,
    )
    assert not result["passed"]
    assert result["mismatches"]


def test_build_reconciliation_proof_scores_exact_key_fidelity():
    from services.reconciliation import build_reconciliation_proof

    source_records = [
        {"id": "1", "amount": "10.00"},
        {"id": "2", "amount": "20.00"},
    ]
    target_records = [
        {"id": "1", "amount": "10.00"},
        {"id": "2", "amount": "20.00"},
    ]
    proof = build_reconciliation_proof(
        source_records,
        target_records,
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        primary_key="id",
    )
    assert proof["passed"] is True
    assert proof["matched_key_count"] == 2
    assert proof["row_fidelity_score"] >= 0.95


def test_build_reconciliation_proof_detects_missing_keys():
    from services.reconciliation import build_reconciliation_proof

    source_records = [
        {"id": "1", "amount": "10.00"},
        {"id": "2", "amount": "20.00"},
    ]
    target_records = [
        {"id": "1", "amount": "10.00"},
    ]
    proof = build_reconciliation_proof(
        source_records,
        target_records,
        [{"source": "id", "target": "id"}, {"source": "amount", "target": "amount"}],
        primary_key="id",
    )
    assert proof["passed"] is False
    assert proof["missing_key_count"] == 1


def test_normalize_cell_equates_decimal_representations():
    from services.reconciliation import normalize_cell

    assert normalize_cell("9.5") == normalize_cell("9.5000000000")
    assert normalize_cell(Decimal("9.5")) == normalize_cell("9.5000000000")
    assert normalize_cell("1000") == normalize_cell("1E+3")
    assert normalize_cell("0.000") == "0"


def test_normalize_cell_uuid_case_fold_with_ddl():
    """PG UUID read-back is lowercase; source wire may be upper — Gate-8 must match."""
    from services.reconciliation import normalize_cell

    upper = "A1B2C3D4-E5F6-7890-ABCD-EF1234567890"
    lower = upper.lower()
    assert normalize_cell(upper, ddl_type="UUID") == lower
    assert normalize_cell(lower, ddl_type="UUID") == lower
    assert normalize_cell(upper, ddl_type="UNIQUEIDENTIFIER") == lower
    # Braces / 32-hex also canonicalize (SQL Server / .NET wire forms).
    assert normalize_cell("{" + upper + "}", ddl_type="UUID") == lower
    assert normalize_cell(upper.replace("-", ""), ddl_type="GUID") == lower
    # Without UUID DDL, preserve case (not every 8-4-4-4-12 string is a UUID column).
    assert normalize_cell(upper) == upper


def test_normalize_cell_preserves_booleans_and_text():
    from services.reconciliation import normalize_cell

    assert normalize_cell(True) == "1"
    assert normalize_cell(False) == "0"
    assert normalize_cell("hello") == "hello"
    # SQL/Dynamo NULL is distinct from empty string.
    assert normalize_cell(None) == "\x00NULL\x00"
    assert normalize_cell(None) != normalize_cell("")
    # Status enums must not collide with true/false (false 100% fidelity).
    assert normalize_cell("active") == "active"
    assert normalize_cell("enabled") == "enabled"
    assert normalize_cell("inactive") == "inactive"
    assert normalize_cell("disabled") == "disabled"
    assert normalize_cell("true") == "1"
    # Informal yes is not a canonical boolean wire token — keep literal.
    assert normalize_cell("yes") == "yes"
    assert normalize_cell("true") == "1"
    assert normalize_cell("false") == "0"


def test_oracle_empty_string_equates_null_write_location():
    """HVR write-location: Oracle '' ≡ NULL; Postgres keeps them distinct."""
    from services.reconciliation import (
        destination_empty_string_is_null,
        fingerprint_for_reconcile,
        normalize_cell,
    )

    assert destination_empty_string_is_null("oracle")
    assert destination_empty_string_is_null("oracledb")
    assert not destination_empty_string_is_null("postgresql")
    assert not destination_empty_string_is_null("mysql")
    assert not destination_empty_string_is_null("")

    null_fp = normalize_cell(None, engine="oracle")
    assert normalize_cell("", engine="oracle") == null_fp
    assert fingerprint_for_reconcile("", engine="oracle") == null_fp
    assert fingerprint_for_reconcile(None, engine="oracle") == null_fp

    # Postgres / default: empty string remains a real value.
    assert normalize_cell("", engine="postgresql") != normalize_cell(None, engine="postgresql")
    assert normalize_cell("") != normalize_cell(None)

    # Non-empty Oracle strings still fingerprint as text.
    assert normalize_cell("x", engine="oracle") == "x"

def test_normalize_cell_equates_offset_datetime_to_utc_instant():
    """Wire may keep +05:30; checksum must match destination UTC datetime objects."""
    from datetime import datetime, timezone, timedelta

    from services.reconciliation import normalize_cell

    wire = "2024-06-01T12:00:00+05:30"
    readback = datetime(2024, 6, 1, 2, 30, tzinfo=timezone(timedelta(hours=-4)))
    assert normalize_cell(wire) == "2024-06-01T06:30:00"
    assert normalize_cell(wire) == normalize_cell(readback)
    assert normalize_cell("2024-12-31T23:59:59Z") == normalize_cell(
        datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    )


def test_normalize_cell_utc_wall_clock_equates_aware_and_ntz():
    """UTC components match across TIMESTAMPTZ sources and NTZ sinks; offsets do not."""
    from datetime import datetime, timezone, timedelta

    from services.reconciliation import normalize_cell

    naive = datetime(2024, 6, 1, 12, 0, 0)
    aware_utc = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    offset_local = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone(timedelta(hours=5, minutes=30)))
    assert normalize_cell(naive) == "2024-06-01T12:00:00"
    assert normalize_cell(aware_utc) == "2024-06-01T12:00:00"
    assert normalize_cell(naive) == normalize_cell(aware_utc)
    assert normalize_cell(offset_local) == "2024-06-01T06:30:00"
    assert normalize_cell(naive) != normalize_cell(offset_local)
    assert normalize_cell("2024-06-01T12:00:00") == normalize_cell(naive)
    assert normalize_cell("2024-06-01T12:00:00Z") == normalize_cell(aware_utc)


def test_reconcile_extra_rows_checksum_mismatch_always_fails():
    """Incomparable append is dest-before delta, not a checksum. Sample never upgrades it.

    Without dest-before the delta is unverified (fail). Overwrite-shaped
    checksum mismatch (equal row counts) still fails even with a sample.
    """
    r = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        strict_checksum=True,
    )
    assert not r.passed
    assert "unverified" in r.message.lower()
    assert "checksum mismatch" not in r.message.lower()

    r2 = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        sample_compare={"passed": True, "compared": 0, "mismatches": []},
    )
    assert not r2.passed

    r3 = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        target_rows_before=5,
        sample_compare={"passed": True, "compared": 10, "mismatches": []},
    )
    assert r3.passed is True
    stamped = r3.to_dict()
    assert stamped["passed"] is True
    assert stamped["assurance_level"] == "row_count"
    assert stamped["migration_proven"] is False
    assert stamped["checksum_match"] is False

    overwrite = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=False,
        sample_compare={"passed": True, "compared": 10, "mismatches": []},
    )
    assert not overwrite.passed
    assert "checksum mismatch" in overwrite.message.lower()
    assert "cannot override" in overwrite.message.lower() or "diagnostic" in overwrite.message.lower()


def test_sample_compare_aligns_renamed_primary_key():
    from services.reconciliation import sample_compare_rows

    source = [{"rec_id": "1", "compensation": "9.50", "active": "true"}]
    target = [{"id": 1, "pay_amount": "9.5", "is_active": True}]
    mappings = [
        {"source": "rec_id", "target": "id", "transform": "integer"},
        {"source": "compensation", "target": "pay_amount", "transform": "decimal"},
        {"source": "active", "target": "is_active", "transform": "boolean"},
    ]
    result = sample_compare_rows(
        source,
        target,
        mappings,
        target_columns=["id", "pay_amount", "is_active"],
        sort_key="id",
    )
    assert result["compared"] > 0
    assert result["passed"] is True
    assert result["mismatches"] == []


def test_sample_compare_skips_columns_absent_from_readback():
    """Missing read-back keys must not invent NULL mismatches (old [:20] bug)."""
    from services.reconciliation import sample_compare_rows

    source = [{
        "id": "1",
        "flag": "false",
        "late_col": "x",
    }]
    # Simulate truncated SELECT that omitted late_col.
    target = [{"id": 1, "flag": 0}]
    mappings = [
        {"source": "id", "target": "id", "transform": "integer"},
        {"source": "flag", "target": "flag", "transform": "boolean"},
        {"source": "late_col", "target": "late_col", "transform": "none"},
    ]
    result = sample_compare_rows(
        source,
        target,
        mappings,
        target_columns=["id", "flag", "late_col"],
        sort_key="id",
    )
    assert result["passed"] is True
    assert all(m.get("target") != "late_col" for m in result["mismatches"])


def test_mysql_boolean_false_binds_as_zero():
    import pytest
    from connectors.mysql_writer import _to_mysql_value

    assert _to_mysql_value(False, "BOOLEAN") == 0
    assert _to_mysql_value(True, "BOOLEAN") == 1
    # Mongo cell_to_string wire form
    assert _to_mysql_value("false", "BOOLEAN") == 0
    assert _to_mysql_value("true", "TINYINT") == 1
    assert _to_mysql_value("0", "BOOLEAN") == 0
    with pytest.raises(ValueError, match="refuse silent NULL invent"):
        _to_mysql_value("", "JSON")
    assert _to_mysql_value({"a": 1}, "JSON") == '{"a":1}'
    assert _to_mysql_value("not-json", "JSON") == '"not-json"'
