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
