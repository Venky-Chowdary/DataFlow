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
    assert normalize_cell(None) == ""
