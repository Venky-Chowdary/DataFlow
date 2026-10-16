"""MySQL create-new must not emit a DEFAULT the column type rejects."""

from __future__ import annotations

from services.schema_fidelity import mysql_default_is_legal, resolve_create_fidelity_plan


def test_integer_key_rejects_empty_string_and_clock_defaults():
    assert mysql_default_is_legal("''", "BIGINT") is False
    assert mysql_default_is_legal("CURRENT_TIMESTAMP", "INT") is False
    assert mysql_default_is_legal("0", "BIGINT") is True
    assert mysql_default_is_legal("'open'", "VARCHAR(32)") is True
    assert mysql_default_is_legal("'0000-00-00 00:00:00'", "DATETIME") is False
    assert mysql_default_is_legal("CURRENT_TIMESTAMP", "DATETIME") is True


def test_mysql_create_plan_omits_invalid_order_id_default():
    plan = resolve_create_fidelity_plan(
        source_schema_catalog={
            "dialect": "mysql",
            "columns": ["order_id", "status"],
            "column_types": {"order_id": "bigint", "status": "varchar(32)"},
            "nullable": {"order_id": False, "status": True},
            "defaults": {"order_id": "''", "status": "'open'"},
            "primary_key": ["order_id"],
        },
        mappings=[
            {"source": "order_id", "target": "order_id"},
            {"source": "status", "target": "status"},
        ],
        target_columns=["order_id", "status"],
        target_types=["BIGINT", "VARCHAR(32)"],
        dest_dialect="mysql",
    )
    order_suffixes = " ".join(plan.column_suffixes.get("order_id") or [])
    status_suffixes = " ".join(plan.column_suffixes.get("status") or [])
    assert "DEFAULT" not in order_suffixes.upper()
    assert "DEFAULT" in status_suffixes.upper()
    dropped = [
        item
        for item in plan.report.items
        if item.aspect == "default" and item.name == "order_id"
    ]
    assert dropped and dropped[0].status == "unsupported"
