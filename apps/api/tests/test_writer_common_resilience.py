"""Writer row-mapping resilience tests."""

from decimal import Decimal

from connectors.writer_common import (
    build_mapped_rows,
    resolve_target_columns,
    sanitize_identifier,
)


def test_sanitize_identifier_preserves_mongodb_id():
    # MongoDB's primary key must survive normalization.
    assert sanitize_identifier("_id") == "_id"
    # Trailing underscores are kept: stripping them made "_id_" and "_id"
    # the same destination column.
    assert sanitize_identifier("_id_") == "_id_"
    assert sanitize_identifier("customer_id") == "customer_id"
    assert sanitize_identifier("NAME") == "name"


def test_resolve_target_columns_preserves_mongodb_id():
    mappings = [{"source": "_id", "target": "_id"}, {"source": "name", "target": "name"}]
    target_cols, _ = resolve_target_columns(mappings, column_types={"_id": "VARCHAR", "name": "VARCHAR"})
    assert target_cols == ["_id", "name"]


def test_quarantine_policy_holds_out_bad_rows():
    """Quarantine must not invent NULL in the primary table for a bad cell."""
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
    # A decimal transform binds an exact Decimal, as a boolean binds a bool:
    # the driver never re-parses text, so scale (10.50) survives the write.
    assert mapped == [(Decimal("10.50"), "C1"), (Decimal("20.00"), "C3")]
    assert [str(cell) for cell, _ in mapped] == ["10.50", "20.00"]
    assert errors and "row 2" in errors[0]


def test_coerce_null_policy_preserves_row_count():
    from connectors.writer_common import build_mapped_rows_with_details

    mapped, errors, details = build_mapped_rows_with_details(
        headers=["is_active"],
        data_rows=[["true"], ["maybe"]],
        mappings=[{"source": "is_active", "target": "is_active", "transform": "boolean"}],
        target_cols=["is_active"],
        column_types={"is_active": "BOOLEAN"},
        error_policy="coerce_null",
        allow_job_coerce_null=True,
    )
    assert mapped == [(True,), (None,)]
    assert errors
    assert details and details[0]["policy"] == "coerce_null"
