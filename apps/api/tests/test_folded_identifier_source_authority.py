"""Folded-catalog identifiers must not degrade source type authority.

Oracle/DB2/Snowflake report ``AMOUNT`` while the same read hands rows back as
``amount``. Every miss on that lookup used to fall back to a sampled guess or
``TEXT``, which is how a live ``NUMBER(12,2)`` reached Validate as
``DECIMAL(8,4)`` and create-new materialised a text column.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import oracle

from connectors.generic_sql import _logical_type_from_sa, _tz_safe_projection
from services.column_case import column_type_or_none, header_index, lookup_column, lookup_row_value
from services.source_schema_authority import reconcile_source_types
from src.transfer.engine import _rekey_to_read_columns


def test_oracle_number_keeps_scale_not_integer():
    # oracle.NUMBER subclasses both Numeric and Integer — integer-first lost the scale.
    assert _logical_type_from_sa(oracle.NUMBER(precision=12, scale=2)) == "DECIMAL(12,2)"
    assert _logical_type_from_sa(oracle.NUMBER(precision=10, scale=0)) == "integer"


def test_lookup_column_is_case_tolerant_but_refuses_ambiguity():
    assert lookup_column({"AMOUNT": "DECIMAL(12,2)"}, "amount") == "DECIMAL(12,2)"
    assert lookup_column({"id": "INT", "ID": "TEXT"}, "Id") is None
    assert column_type_or_none({"AMOUNT": "   "}, "amount") is None
    assert column_type_or_none({"amount": "DECIMAL(12,2)"}, "amount") == "DECIMAL(12,2)"


def test_lookup_row_value_finds_folded_oracle_keys():
    row = {"ID": "1", "AMOUNT": "1000.00", "CODE": "USD"}
    assert lookup_row_value(row, "amount", "") == "1000.00"
    assert lookup_row_value(row, "id") == "1"
    assert lookup_row_value(row, "missing", "SENTINEL") == "SENTINEL"
    assert lookup_row_value({"id": "1", "ID": "2"}, "Id", "SENTINEL") == "SENTINEL"
    assert header_index(["ID", "AMOUNT", "CODE"], "amount") == 1
    assert header_index(["id", "ID"], "Id") is None


def test_reconcile_keeps_declared_spelling_for_folded_live_key():
    merged, _drift = reconcile_source_types(
        {"amount": "DECIMAL(8,4)"}, {"AMOUNT": "DECIMAL(12,2)"}
    )
    assert merged == {"amount": "DECIMAL(12,2)"}


def test_rekey_to_read_columns_matches_row_spelling():
    schema = {"AMOUNT": "DECIMAL(12,2)", "TS_TZ": "TIMESTAMP_TZ"}
    assert _rekey_to_read_columns(schema, ["amount", "ts_tz"]) == {
        "amount": "DECIMAL(12,2)",
        "ts_tz": "TIMESTAMP_TZ",
    }


def test_rekey_leaves_ambiguous_case_collision_alone():
    schema = {"id": "INTEGER", "ID": "TEXT"}
    assert _rekey_to_read_columns(schema, ["Id"]) == schema


def test_oracle_tz_columns_are_rendered_server_side():
    meta = sa.MetaData()
    table = sa.Table(
        "t",
        meta,
        sa.Column("ts_tz", oracle.TIMESTAMP(timezone=True)),
        sa.Column("ts_naive", oracle.TIMESTAMP()),
        sa.Column("n", sa.Integer()),
    )
    cfg = {"type": "oracle"}
    projected = _tz_safe_projection(cfg, list(table.c))
    sql = str(sa.select(*projected).compile(dialect=oracle.dialect()))
    # python-oracledb drops the offset on TIMESTAMP WITH TIME ZONE, so it must
    # be rendered by the server; naive columns stay untouched.
    assert sql.startswith("SELECT to_char(t.ts_tz")
    assert "to_char(t.ts_naive" not in sql
    assert [getattr(c, "name", None) for c in projected] == ["ts_tz", "ts_naive", "n"]


def test_non_oracle_projection_is_unchanged():
    meta = sa.MetaData()
    table = sa.Table("t", meta, sa.Column("ts", sa.DateTime(timezone=True)))
    cols = list(table.c)
    assert _tz_safe_projection({"type": "postgresql"}, cols) == cols
