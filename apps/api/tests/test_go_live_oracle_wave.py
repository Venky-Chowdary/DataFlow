"""Oracle case, NUMBER-vs-BOOLEAN, and folded CREATE identifiers.

DEF-B-009 not-null half, DEF-B-011, DEF-B-008. Unit proof only — not a live
Oracle retest.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import oracle as oracle_dialect

from connectors.generic_sql import _build_table_for_write
from services.data_profiler import merge_profiler_schema
from services.decimal_observe import (
    _carrier_for_cursor_column,
    cursor_declared_carriers,
)
from services.decision_kernel.type_invent import (
    refuse_boolean_invent_from_numeric_source,
)
from services.expectations_engine import expect_column_not_null
from services.schema_inference import infer_type


class _Dialect:
    def __init__(self, name: str) -> None:
        self.name = name


class _Engine:
    def __init__(self, name: str) -> None:
        self.dialect = _Dialect(name)


class _OracleNumber:
    """oracledb.DbType shape: class name hides the carrier, ``name`` does not."""

    name = "DB_TYPE_NUMBER"


_OracleNumber.__name__ = "DbType"


def test_uppercase_oracle_keys_are_not_null_failures() -> None:
    rows = [{"ID": 1, "ORDER_ID": 10, "AMOUNT": "1.25"}]
    ident = expect_column_not_null(rows, "id")
    order = expect_column_not_null(rows, "order_id")
    assert ident.passed is True
    assert ident.failing_count == 0
    assert order.passed is True
    assert order.failing_count == 0


def test_declared_number_is_not_relabelled_boolean() -> None:
    # A file with no declared width may still promote flag-shaped 0/1.
    assert infer_type(["0", "1"], field_name="is_active") == "BOOLEAN"
    kept = merge_profiler_schema(
        {"is_active": "DECIMAL(1,0)", "wide": "DECIMAL(5,0)"},
        {"is_active": "BOOLEAN", "wide": "BOOLEAN"},
    )
    assert kept["is_active"] == "DECIMAL(1,0)"
    assert kept["wide"] == "DECIMAL(5,0)"
    promoted = merge_profiler_schema(
        {"is_active": "VARCHAR"},
        {"is_active": "BOOLEAN"},
    )
    assert promoted["is_active"] == "BOOLEAN"


def test_oracle_number_type_object_is_decimal_not_boolean() -> None:
    code = _OracleNumber()
    assert _carrier_for_cursor_column(("is_active", code, None, None, 1, 0)) == "DECIMAL"
    declared = cursor_declared_carriers(
        ["is_active"],
        [("is_active", code, None, None, 1, 0)],
    )
    assert declared["is_active"] == "DECIMAL(1,0)"


def test_create_new_does_not_stamp_boolean_on_a_declared_number() -> None:
    stamped = refuse_boolean_invent_from_numeric_source(
        "DECIMAL(1,0)", "BOOLEAN", "postgresql"
    )
    assert "BOOL" not in stamped.upper()
    tiny = refuse_boolean_invent_from_numeric_source(
        "TINYINT(1)", "BOOLEAN", "mysql"
    )
    assert tiny == "BOOLEAN"


def test_folding_dialect_create_quotes_uppercase_physical_names() -> None:
    table = _build_table_for_write(
        _Engine("oracle"),
        "qa6b_ow_pg",
        "sales",
        ["id", "is_active"],
        {"id": "INTEGER", "is_active": "INTEGER"},
        db_type="oracle",
        conflict_columns=["id"],
    )
    ddl = str(
        sa.schema.CreateTable(table).compile(dialect=oracle_dialect.dialect())
    )
    assert '"ID"' in ddl
    assert '"IS_ACTIVE"' in ddl
    assert '"QA6B_OW_PG"' in ddl
    assert '"id"' not in ddl
    assert table.c.id.key == "id"
    assert table.c.id.name == "ID"

    kept = _build_table_for_write(
        _Engine("postgresql"),
        "qa6b_ow_pg",
        "sales",
        ["id", "is_active"],
        {"id": "INTEGER", "is_active": "INTEGER"},
        db_type="postgresql",
        conflict_columns=["id"],
    )
    assert kept.c.id.name == "id"


def test_parquet_export_refuses_a_zero_column_file() -> None:
    from connectors.object_store_materialize import materialize_object_store_export

    result = materialize_object_store_export(
        key="exports/orders.parquet",
        headers=["id"],
        data_rows=[["1"]],
        mappings=[],
        target_cols=[],
        column_types={"id": "INTEGER"},
        dest_types={},
        error_policy="quarantine",
        dest_kind="s3",
        dialect_label="S3",
        spill_max_size=1024,
    )
    assert result.export is None
    assert result.abort_error
    assert "0-column" in result.abort_error
