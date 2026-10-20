"""X-122: dedupe winner checks reported 'unsupported_destination' on
DuckDB because the stamped engine type was the driver key 'generic_sql'
(duckdb has no driver entry — it runs on the shared SQLAlchemy driver).
The stamp now carries the catalog type.
"""

from __future__ import annotations

from unittest.mock import patch

from src.transfer import reconcile_step as rs


def _ctx(engine: str) -> dict:
    return {
        "cfg": {"type": engine, "path": "/tmp/x.duckdb"},
        "schema": "",
        "table_name": "out",
        "engine": engine,
    }


def _summary() -> dict:
    return {
        "shape_dedupe": [{
            "step": 1, "keys": ["grp"], "keep": "last",
            "order_by": "amt", "order_dir": "desc",
            "rows_in": 3, "rows_out": 1, "losers": 2,
        }],
        "shape_output_columns": ["grp", "amt"],
        "rows_written": 1,
    }


def test_duckdb_not_unsupported_destination():
    checks = rs._run_dedupe_gate_checks({}, _ctx("duckdb"), _summary(), "full_refresh_overwrite")
    uniq = [c for c in checks if c.get("check") == "winner_uniqueness"]
    assert uniq
    assert uniq[0]["status"] != "unverified" or uniq[0].get("reason") != "unsupported_destination"


def test_duckdb_dialect_reaches_count(monkeypatch=None):
    seen = {}

    def fake_count(dialect, cfg, endpoint=None, table=None, columns=None):
        seen["dialect"] = dialect
        return 0

    with patch.object(rs, "_count_dest_key_duplicates", fake_count), \
         patch.object(rs, "_dedupe_winner_ordering_check", lambda *a, **k: {"status": "unverified", "reason": "no source"}):
        checks = rs._run_dedupe_gate_checks({}, _ctx("duckdb"), _summary(), "full_refresh_overwrite")
    assert seen["dialect"] == "duckdb"
    uniq = [c for c in checks if c.get("check") == "winner_uniqueness"][0]
    assert uniq["status"] == "passed"


def test_ctx_engine_uses_catalog_type():
    from src.transfer.connector_capabilities import resolve_driver_type

    driver = resolve_driver_type("duckdb")
    assert driver == "generic_sql"  # the mapping that broke the check
    assert rs._dedupe_ctx_engine("duckdb", "", driver) == "duckdb"
    assert rs._dedupe_ctx_engine("", "duckdb", driver) == "duckdb"
    assert rs._dedupe_ctx_engine("sqlserver", "", "sqlserver") == "sqlserver"
