"""The replay-safety verdict must match what the writers actually do.

``classify_replay_safety`` tells the engine whether an interrupted write may be
re-sent, and tells the operator the same thing in the UI. If it claims a
destination keeps a committed-chunk ledger when the writer does not, two things
break at once: ``with_retry`` re-sends an ambiguous batch and silently
duplicates rows, and the Replay Safety card reassures the operator while it
happens.

An earlier version of the registry listed destination *names*, which drifted the
moment a driver was re-routed — Snowflake, BigQuery and SQLite were all claimed
without a ledger behind them. These tests close that hole from both sides:
every module claimed must really call the ledger helpers, and every SQL
destination's claim must match the module it actually dispatches to.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from services.replay_safety import (
    _LEDGER_WRITER_MODULES,
    classify_replay_safety,
    destination_has_chunk_ledger,
    writer_module_for,
)

CONNECTORS_DIR = Path(__file__).resolve().parents[1] / "connectors"

# Calling any of these means the module participates in the ledger protocol.
_LEDGER_CALLS = {
    "ensure_raw_write_ledger",
    "raw_chunk_rows_written",
    "mark_raw_chunk_committed",
    "ensure_sqlalchemy_write_ledger",
    "sqlalchemy_chunk_rows_written",
    "mark_sqlalchemy_chunk_committed",
}


def _module_path(dotted: str) -> Path:
    return CONNECTORS_DIR / f"{dotted.split('.')[-1]}.py"


def _names_used(path: Path) -> set[str]:
    """Every identifier referenced in a module, including via delegation."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    return {
        node.id for node in ast.walk(tree) if isinstance(node, ast.Name)
    } | {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    } | {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }


def _delegates_to(path: Path) -> set[str]:
    """Connector modules this module imports ``write_mapped_rows`` from."""
    tree = ast.parse(path.read_text(encoding="utf-8"))
    targets: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith(
            "connectors."
        ):
            if any(a.name == "write_mapped_rows" for a in node.names):
                targets.add(node.module)
    return targets


def _has_ledger_support(dotted: str, _seen: frozenset[str] = frozenset()) -> bool:
    """Whether a writer module uses the ledger directly or via delegation."""
    if dotted in _seen:
        return False
    path = _module_path(dotted)
    if not path.exists():
        return False
    if _names_used(path) & _LEDGER_CALLS:
        return True
    return any(
        _has_ledger_support(target, _seen | {dotted})
        for target in _delegates_to(path)
    )


class TestLedgerClaimsMatchWriters:
    """Every module the registry vouches for must really keep a ledger."""

    @pytest.mark.parametrize("module", sorted(_LEDGER_WRITER_MODULES))
    def test_claimed_module_calls_the_ledger_helpers(self, module: str) -> None:
        assert _module_path(module).exists(), f"{module} does not exist"
        assert _has_ledger_support(module), (
            f"{module} is listed in _LEDGER_WRITER_MODULES but never calls the "
            "ledger helpers. classify_replay_safety would tell the engine an "
            "interrupted write is safe to re-send, duplicating rows."
        )

    def test_unclaimed_sql_writers_really_lack_a_ledger(self) -> None:
        """The reverse gap: a writer that gained a ledger but no registry entry.

        Under-claiming is safe but wasteful — it costs the destination its retry
        budget. Failing here is a prompt to add the module, not a bug report.
        """
        gained = {
            f"connectors.{path.stem}"
            for path in CONNECTORS_DIR.glob("*_writer.py")
            if f"connectors.{path.stem}" not in _LEDGER_WRITER_MODULES
            and _has_ledger_support(f"connectors.{path.stem}")
        }
        assert not gained, (
            f"{sorted(gained)} now keep a chunk ledger but are missing from "
            "_LEDGER_WRITER_MODULES, so their writes needlessly lose the retry "
            "budget they have earned."
        )


class TestVerdictMatchesDispatch:
    """The verdict must follow the module a destination actually dispatches to."""

    @pytest.mark.parametrize(
        "dest",
        [
            "postgresql",
            "redshift",
            "cockroachdb",
            "mysql",
            "sqlserver",
            "mssql",
            "oracle",
            "sqlite",
            "duckdb",
            "clickhouse",
            "databricks",
            "trino",
            "athena",
            "vertica",
            "teradata",
            "db2",
            "synapse",
            "generic_sql",
        ],
    )
    def test_sql_destinations_are_ledger_backed(self, dest: str) -> None:
        module = writer_module_for(dest)
        assert module, f"{dest} does not resolve to a writer module"
        assert destination_has_chunk_ledger(dest) is (
            module in _LEDGER_WRITER_MODULES
        )
        assert destination_has_chunk_ledger(dest), (
            f"{dest} dispatches to {module}, which has no chunk ledger. An "
            "insert-mode retry there can duplicate rows."
        )

    @pytest.mark.parametrize("dest", ["bigquery", "s3", "gcs", "adls", "kafka", "iceberg"])
    def test_append_only_destinations_do_not_claim_a_ledger(self, dest: str) -> None:
        """BigQuery load jobs and object stores have no cross-statement transaction.

        A ledger row that commits while its data write rolls back would make the
        retry *skip* a chunk that never landed — losing rows, which is strictly
        worse than duplicating them. These destinations must stay honest.
        """
        assert not destination_has_chunk_ledger(dest)

    def test_unknown_destination_does_not_claim_a_ledger(self) -> None:
        assert writer_module_for("not_a_real_destination") == ""
        assert not destination_has_chunk_ledger("not_a_real_destination")
        assert not destination_has_chunk_ledger("")


class TestClassificationUsesRealRouting:
    def test_insert_to_ledger_destination_is_replay_safe(self) -> None:
        verdict = classify_replay_safety(
            dest_type="oracle", write_mode="insert", job_id="job-1"
        )
        assert verdict.safe
        assert verdict.mechanism == "chunk_ledger"

    def test_insert_to_append_only_destination_is_not_replay_safe(self) -> None:
        verdict = classify_replay_safety(
            dest_type="bigquery", write_mode="insert", job_id="job-1"
        )
        assert not verdict.safe
        assert verdict.duplicate_risk
        assert verdict.mechanism == "none"

    def test_ledger_needs_a_job_id_to_be_credited(self) -> None:
        verdict = classify_replay_safety(
            dest_type="postgresql", write_mode="insert", job_id=""
        )
        assert not verdict.safe
        assert "job id" in verdict.reason

    def test_upsert_beats_ledger_on_any_destination(self) -> None:
        verdict = classify_replay_safety(
            dest_type="bigquery",
            write_mode="upsert",
            conflict_columns=["id"],
            job_id="job-1",
        )
        assert verdict.safe
        assert verdict.mechanism == "idempotent_upsert"
