"""Pilot live query / sample tools + NL routing."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from src.ai.copilot.query_tools import (  # noqa: E402
    _SAFE_IDENT,
    _analyze_rows,
    _sample_sql,
    run_connector_query,
    sample_connector_object,
)
from src.ai.copilot.tools import infer_tools_from_message  # noqa: E402
from src.routers.query_router import _is_safe_sql  # noqa: E402


def test_sample_sql_dialects() -> None:
    assert "LIMIT 10" in _sample_sql("orders", "postgresql", 10)
    assert "`orders`" in _sample_sql("orders", "mysql", 5)
    assert "TOP 5" in _sample_sql("orders", "mssql", 5)


def test_safe_sql_blocks_destructive() -> None:
    assert _is_safe_sql("SELECT 1")
    assert not _is_safe_sql("DELETE FROM orders")
    assert not _is_safe_sql("DROP TABLE orders")
    assert not _is_safe_sql("SELECT * INTO copy FROM orders")


def test_analyze_rows_profiles() -> None:
    rows = [{"id": 1, "email": "a@b.com"}, {"id": 2, "email": None}]
    profile = _analyze_rows(rows, ["id", "email"])
    assert profile["row_count_sampled"] == 2
    by_col = {c["column"]: c for c in profile["columns"]}
    assert by_col["email"]["nulls"] == 1
    assert by_col["id"]["non_null"] == 2


def test_nl_routes_sample_and_sql() -> None:
    planned = infer_tools_from_message("sample airports on Local Postgres")
    names = [n for n, _ in planned]
    assert "sample_connector_object" in names
    args = dict(planned)["sample_connector_object"]
    assert args["table"] == "airports"
    assert "local postgres" in args["connector_name"].lower()

    planned_q = infer_tools_from_message("SELECT id, name FROM airports LIMIT 5 on Local Postgres")
    assert "run_query" in [n for n, _ in planned_q]
    qargs = dict(planned_q)["run_query"]
    assert "SELECT" in qargs["query"].upper()


def test_run_query_rejects_missing_connector() -> None:
    result = run_connector_query(
        connector_name="Missing Connector XYZ",
        query="DROP TABLE users",
    )
    assert result.success is False
    err = (result.error or "").lower()
    assert "not found" in err or "matched" in err or "read-only" in err


def test_sample_requires_safe_table_name() -> None:
    result = sample_connector_object(table="orders; drop table x", connector_name="x")
    assert result.success is False
    assert not _SAFE_IDENT.match("orders; drop table x")


def test_sample_executes_via_query_router() -> None:
    fake_conn = {
        "id": "c1",
        "name": "Local Postgres",
        "type": "postgresql",
    }
    fake_saved = MagicMock()
    with patch("src.ai.copilot.query_tools._safe_connector", return_value=(fake_conn, None)):
        with patch("services.connector_store.get_connector", return_value=fake_saved):
            with patch(
                "src.routers.query_router._run_query",
                return_value=(
                    [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                    ["id", "name"],
                    {"id": "INTEGER", "name": "VARCHAR"},
                    False,
                ),
            ):
                result = sample_connector_object(
                    connector_name="Local Postgres",
                    table="airports",
                    limit=10,
                    analyze=True,
                )
    assert result.success is True
    assert result.output["row_count"] == 2
    assert result.output["analysis"]["column_count"] == 2
