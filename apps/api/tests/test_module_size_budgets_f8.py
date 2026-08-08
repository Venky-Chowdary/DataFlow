"""Phase F8 — god-module freeze budgets + facades."""

from __future__ import annotations

from connectors.merge_registry import MERGE_STRATEGIES, merge_strategy_for
from connectors.writer_common_api import WriteResult, transform_error_policy
from services.reconciliation_api import FingerprintAccumulator, canonical_checksum


def test_module_size_budgets_script_ok():
    from scripts.check_module_size_budgets import main

    assert main() == 0


def test_merge_registry_covers_core_dialects():
    for d in ("postgresql", "mysql", "sqlserver", "oracle", "snowflake", "bigquery"):
        strat = merge_strategy_for(d)
        assert strat.get("strategy")
        assert strat.get("at_least_once") is True
    assert "postgresql" in MERGE_STRATEGIES


def test_facades_export_callable_surfaces():
    assert WriteResult is not None
    assert callable(transform_error_policy)
    assert FingerprintAccumulator is not None
    assert callable(canonical_checksum)
