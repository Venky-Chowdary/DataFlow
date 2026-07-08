"""Writer row-mapping resilience tests."""

from connectors.writer_common import build_mapped_rows


def test_quarantine_policy_skips_bad_rows():
    mapped, errors = build_mapped_rows(
        headers=["AMT", "CUST_ID"],
        data_rows=[["10.50", "C1"], ["not-a-number", "C2"], ["20.00", "C3"]],
        mappings=[
            {"source": "AMT", "target": "payment_amount", "transform": "decimal"},
            {"source": "CUST_ID", "target": "customer_id", "transform": "trim"},
        ],
        target_cols=["payment_amount", "customer_id"],
        column_types={"AMT": "DECIMAL", "CUST_ID": "TEXT"},
        error_policy="quarantine",
    )
    assert mapped == [("10.50", "C1"), ("20.00", "C3")]
    assert errors and "row 2" in errors[0]


def test_coerce_null_policy_preserves_row_count():
    mapped, errors = build_mapped_rows(
        headers=["is_active"],
        data_rows=[["true"], ["maybe"]],
        mappings=[{"source": "is_active", "target": "is_active", "transform": "boolean"}],
        target_cols=["is_active"],
        column_types={"is_active": "BOOLEAN"},
        error_policy="coerce_null",
    )
    assert mapped == [(True,), (None,)]
    assert errors
