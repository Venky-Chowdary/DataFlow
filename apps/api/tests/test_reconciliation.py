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


def test_sample_compare_rows_detects_mismatch():
    from services.reconciliation import sample_compare_rows

    result = sample_compare_rows(
        [{"id": "1", "name": "Alice"}],
        [{"id": "2", "name": "Alice"}],
        [{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
    )
    assert not result["passed"]
    assert result["mismatches"]
