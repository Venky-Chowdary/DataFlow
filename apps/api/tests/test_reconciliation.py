from decimal import Decimal

from services.reconciliation import reconcile


def test_reconcile_pass():
    r = reconcile(source_rows=10, target_rows=10, source_checksum="abc", target_checksum="abc")
    assert r.passed
    assert "100%" in r.message


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


def test_reconcile_allows_checksum_mismatch_balanced():
    r = reconcile(
        source_rows=10,
        target_rows=10,
        source_checksum="abc",
        target_checksum="xyz",
        strict_checksum=False,
    )
    assert r.passed


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
    assert normalize_cell("yes") == "1"
    assert normalize_cell("false") == "0"

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


def test_reconcile_extra_rows_requires_sample_proof():
    """Gate-8 soft-pass under allow_extra_rows must fail-closed without sample proof."""
    # No sample_compare → fail
    r = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        strict_checksum=True,
    )
    assert not r.passed
    assert "sample" in r.message.lower()

    # Empty compared=0 → fail
    r2 = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        sample_compare={"passed": True, "compared": 0, "mismatches": []},
    )
    assert not r2.passed

    # Real sample proof → pass
    r3 = reconcile(
        source_rows=10,
        target_rows=15,
        source_checksum="abc",
        target_checksum="xyz",
        allow_extra_rows=True,
        sample_compare={"passed": True, "compared": 10, "mismatches": []},
    )
    assert r3.passed
    assert "sample" in r3.message.lower()


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
