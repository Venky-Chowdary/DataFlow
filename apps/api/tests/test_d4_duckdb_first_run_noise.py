"""D4 LOW: first-run DuckDB introspection on a missing table must not
dump a CatalogException traceback — "does not exist" is an expected
answer, reported as a clean one-line error with ``not_found`` set.

Run against a real fresh DuckDB file (no mocks)."""

from __future__ import annotations

import logging
import os
import tempfile


def _duckdb_cfg(path: str) -> dict:
    return {
        "type": "duckdb",
        "connection_string": f"duckdb:///{path}",
        "database": path,
        "schema": "main",
    }


def test_first_run_missing_table_returns_not_found_no_traceback(caplog):
    pytest = __import__("pytest")
    pytest.importorskip("duckdb")
    pytest.importorskip("duckdb_sqlalchemy")
    from connectors.generic_sql import introspect_table_schema

    with tempfile.TemporaryDirectory() as d:
        cfg = _duckdb_cfg(os.path.join(d, "fresh.duckdb"))
        with caplog.at_level(logging.WARNING):
            result = introspect_table_schema(cfg, "never_created")

    assert result["ok"] is False
    assert result.get("not_found") is True
    assert "does not exist" in result["error"]
    warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
    assert warnings == [], (
        "a missing table is an expected answer — no warning/traceback "
        f"may be logged, got: {[r.getMessage() for r in warnings]}"
    )


def test_not_found_classifier_matches_duckdb_catalog_exception():
    pytest = __import__("pytest")
    pytest.importorskip("duckdb")
    pytest.importorskip("duckdb_sqlalchemy")
    import sqlalchemy as sa
    from connectors.generic_sql import _is_not_found_error

    engine = sa.create_engine("duckdb:///:memory:")
    try:
        with engine.connect() as conn:
            try:
                conn.execute(sa.text("SELECT * FROM missing_table"))
            except Exception as exc:
                assert _is_not_found_error(exc) is True
            else:
                raise AssertionError("expected CatalogException")
    finally:
        engine.dispose()


def test_not_found_classifier_rejects_unrelated_errors():
    from connectors.generic_sql import _is_not_found_error

    assert _is_not_found_error(ValueError("conversion failed")) is False
    assert _is_not_found_error(RuntimeError("connection refused")) is False
    assert _is_not_found_error(Exception("timeout")) is False


def test_not_found_classifier_walks_wrapped_causes():
    import sqlalchemy as sa
    from connectors.generic_sql import _is_not_found_error

    class _FakeCatalog(Exception):
        pass

    _FakeCatalog.__name__ = "CatalogException"
    wrapped = sa.exc.ProgrammingError("SELECT 1", {}, Exception())
    wrapped.__cause__ = _FakeCatalog("Table with name x does not exist")
    assert _is_not_found_error(wrapped) is True
