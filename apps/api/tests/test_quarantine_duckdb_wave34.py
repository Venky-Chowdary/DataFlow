"""Wave 34: quarantine source_values dual-stamp + DuckDB MERGE upsert."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_write_quarantine_dual_stamps_source_values_via_mappings():
    from connectors.writer_common import apply_write_quarantine_matrix

    details: list[dict] = []
    rows = [
        ("1", "10.5"),
        ("2", "999999999999999999999"),
    ]
    mappings = [
        {"source": "user_id", "target": "id"},
        {"source": "amt", "target": "amount"},
    ]
    out = apply_write_quarantine_matrix(
        rows,
        ["id", "amount"],
        ["INTEGER", "DECIMAL(10,2)"],
        details,
        "quarantine",
        dialect_label="postgres",
        mappings=mappings,
    )
    assert len(out) == 1
    assert details
    d = details[0]
    assert d["values"]["id"] == "2"
    assert d["source_values"]["user_id"] == "2"
    assert d["source_values"]["amt"] == d["values"]["amount"]


def test_transform_quarantine_stamps_source_values():
    from connectors.writer_common import build_mapped_rows_with_details

    rows, errs, details = build_mapped_rows_with_details(
        headers=["age", "name"],
        data_rows=[["not-int", "Ada"]],
        mappings=[
            {"source": "age", "target": "age", "target_type": "integer"},
            {"source": "name", "target": "name"},
        ],
        target_cols=["age", "name"],
        column_types={"age": "string", "name": "string"},
        error_policy="quarantine",
        dest_types={"age": "INTEGER", "name": "VARCHAR"},
    )
    assert details
    assert details[0].get("source_values", {}).get("age") == "not-int"
    assert details[0].get("source_values", {}).get("name") == "Ada"


def test_duckdb_capability_upsert_honest():
    from services.connector_capability_registry import CAPABILITY_REGISTRY

    cap = CAPABILITY_REGISTRY["duckdb"]
    assert cap["supports_upsert"] is True
    assert cap["supports_merge"] is True
    assert cap.get("supports_lsn_guard") is True


def test_duckdb_merge_sql_null_safe():
    from connectors.generic_sql import _duckdb_merge_upsert

    executed: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            executed.append(str(getattr(stmt, "text", stmt)).upper())

    import sqlalchemy as sa

    table = sa.table("orders", sa.column("id"), sa.column("amount"))
    n = _duckdb_merge_upsert(
        _Conn(),
        table,
        [{"id": 1, "amount": 10}, {"id": None, "amount": 2}],
        ["id"],
        ["id", "amount"],
        ["amount"],
    )
    assert n == 2
    blob = " ".join(executed)
    assert "MERGE INTO" in blob
    assert "IS NULL" in blob
    assert "CREATE TEMP TABLE" in blob


def test_upsert_batch_duckdb_prefers_merge_then_delete_insert():
    import sqlalchemy as sa

    from connectors.generic_sql import _upsert_batch

    table = sa.table("t", sa.column("id"), sa.column("v"))
    calls: list[str] = []

    class _Conn:
        def execute(self, stmt, params=None):  # noqa: ANN001
            text = str(getattr(stmt, "text", stmt))
            calls.append(text)
            if "MERGE" in text.upper():
                raise sa.exc.SQLAlchemyError("merge unavailable")
            result = MagicMock()
            result.rowcount = 1
            return result

        def rollback(self) -> None:
            calls.append("ROLLBACK")

    with patch(
        "connectors.generic_sql._delete_by_keys",
        side_effect=lambda *a, **k: calls.append("DELETE_KEYS"),
    ):
        n = _upsert_batch(
            _Conn(),
            table,
            [{"id": 1, "v": "a"}],
            ["id"],
            ["id", "v"],
            "duckdb",
        )
    assert n == 1
    assert any("MERGE" in c.upper() for c in calls)
    assert "ROLLBACK" in calls
    assert "DELETE_KEYS" in calls
