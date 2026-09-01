"""G21 control totals — opt-in independent SUM, fail closed.

A sample SUM is not proof. Undeclared columns are a skip, not a green pass.
SQLite INTEGER SUM is exact; float SUM is unproven.
"""

from __future__ import annotations

import sqlite3
from decimal import Decimal
from pathlib import Path

from services.control_totals import (
    GATE_ID,
    build_control_totals_gate,
    build_control_totals_report,
    decimal_from_sql_sum,
    independent_column_sum,
    is_money_logical_type,
    mapping_asks_control_total,
    verify_control_totals,
)


def test_undeclared_decimal_is_not_a_control_total() -> None:
    assert is_money_logical_type("MONEY") is True
    assert is_money_logical_type("DECIMAL(12,2)") is False
    assert mapping_asks_control_total(
        {"source": "amount", "target": "amount", "target_type": "DECIMAL(12,2)"}
    ) is False
    report = build_control_totals_report(
        mappings=[{"source": "amount", "target": "amount", "target_type": "DECIMAL"}],
        phase="execute",
    )
    gate = build_control_totals_gate(report, phase="execute")
    assert report["declared"] is False
    assert gate["status"] == "skip"
    assert gate["id"] == GATE_ID


def test_money_type_still_needs_the_flag() -> None:
    assert mapping_asks_control_total(
        {"source": "bal", "target": "bal", "target_type": "MONEY"}
    ) is False
    assert mapping_asks_control_total(
        {"source": "bal", "target": "bal", "target_type": "MONEY", "control_total": True}
    ) is True
    assert mapping_asks_control_total(
        {"source": "bal", "target": "bal", "target_type": "MONEY", "control_total": False}
    ) is False


def test_validate_skips_declared_control_total() -> None:
    report, gate = verify_control_totals(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "none",
            }
        ],
        phase="validate",
    )
    assert report["declared"] is True
    assert gate["status"] == "skip"
    assert "post-write" in gate["message"].lower() or "sample" in gate["message"].lower()


def test_sample_is_not_proof() -> None:
    report = build_control_totals_report(
        mappings=[{"source": "amount", "target": "amount", "control_total": True}],
        phase="execute",
        sample_only=True,
        source_sums={"amount": {"available": True, "sum": "30.50"}},
        dest_sums={"amount": {"available": True, "sum": "30.50"}},
    )
    gate = build_control_totals_gate(report, phase="execute")
    assert gate["status"] == "block"
    assert report["columns"][0]["proven"] is False


def test_transformed_amount_is_unproven() -> None:
    report = build_control_totals_report(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "currency",
            }
        ],
        phase="execute",
        source_sums={"amount": {"available": True, "sum": "30"}},
        dest_sums={"amount": {"available": True, "sum": "30"}},
    )
    gate = build_control_totals_gate(report, phase="execute")
    assert gate["status"] == "block"
    assert "identity" in report["columns"][0]["reason"]


def test_decimal_bind_is_identity_for_control_total() -> None:
    report = build_control_totals_report(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "decimal",
            }
        ],
        phase="execute",
        source_sums={"amount": {"available": True, "sum": "30.50"}},
        dest_sums={"amount": {"available": True, "sum": "30.50"}},
    )
    gate = build_control_totals_gate(report, phase="execute")
    assert gate["status"] == "pass"


def test_quarantine_without_amount_sum_is_unproven() -> None:
    report = build_control_totals_report(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "none",
            }
        ],
        phase="execute",
        rejected_rows=2,
        source_sums={"amount": {"available": True, "sum": "30"}},
        dest_sums={"amount": {"available": True, "sum": "20"}},
    )
    gate = build_control_totals_gate(report, phase="execute")
    assert gate["status"] == "block"
    assert "quarantine" in report["columns"][0]["reason"].lower()


def test_float_sum_is_unproven() -> None:
    assert decimal_from_sql_sum(30.5) is None
    assert decimal_from_sql_sum(Decimal("30.50")) == Decimal("30.50")
    assert decimal_from_sql_sum("30.50") == Decimal("30.50")
    assert decimal_from_sql_sum(30) == Decimal(30)


def _sqlite_cfg(path: Path, *statements: str) -> dict[str, str]:
    with sqlite3.connect(path) as conn:
        for stmt in statements:
            conn.execute(stmt)
        conn.commit()
    return {"type": "sqlite", "database": str(path)}


def test_sqlite_independent_sum_match(tmp_path: Path) -> None:
    src = _sqlite_cfg(
        tmp_path / "src.db",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "INSERT INTO ledger (id, amount) VALUES (1, 1000), (2, 2050)",
    )
    dst = _sqlite_cfg(
        tmp_path / "dst.db",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "INSERT INTO ledger (id, amount) VALUES (1, 1000), (2, 2050)",
    )
    src_sum = independent_column_sum(
        "sqlite", src, schema="", table="ledger", column="amount"
    )
    dst_sum = independent_column_sum(
        "sqlite", dst, schema="", table="ledger", column="amount"
    )
    assert src_sum["available"] is True
    assert dst_sum["available"] is True
    assert Decimal(src_sum["sum"]) == Decimal("3050")
    assert Decimal(dst_sum["sum"]) == Decimal(dst_sum["sum"])
    report, gate = verify_control_totals(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "none",
            }
        ],
        source_db_type="sqlite",
        source_cfg=src,
        source_table="ledger",
        dest_db_type="sqlite",
        dest_cfg=dst,
        dest_table="ledger",
        phase="execute",
    )
    assert gate["status"] == "pass"
    assert report["columns"][0]["proven"] is True
    assert report["columns"][0]["matched"] is True


def test_sqlite_independent_sum_mismatch_fails(tmp_path: Path) -> None:
    src = _sqlite_cfg(
        tmp_path / "src.db",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "INSERT INTO ledger (id, amount) VALUES (1, 1000), (2, 2050)",
    )
    dst = _sqlite_cfg(
        tmp_path / "dst.db",
        "CREATE TABLE ledger (id INTEGER PRIMARY KEY, amount INTEGER NOT NULL)",
        "INSERT INTO ledger (id, amount) VALUES (1, 1000), (2, 2049)",
    )
    report, gate = verify_control_totals(
        mappings=[
            {
                "source": "amount",
                "target": "amount",
                "control_total": True,
                "transform": "none",
            }
        ],
        source_db_type="sqlite",
        source_cfg=src,
        source_table="ledger",
        dest_db_type="sqlite",
        dest_cfg=dst,
        dest_table="ledger",
        phase="execute",
    )
    assert gate["status"] == "block"
    assert report["any_mismatch"] is True
    # Same cardinality, different ledger — the bank-examiner case.
    assert Decimal(report["columns"][0]["source_sum"]) == Decimal("3050")
    assert Decimal(report["columns"][0]["dest_sum"]) == Decimal("3049")


def test_mapping_item_keeps_control_total() -> None:
    from src.routers.preflight_router import MappingItem

    item = MappingItem(
        source="amount",
        target="amount",
        control_total=True,
    )
    dumped = item.model_dump()
    assert dumped["control_total"] is True
