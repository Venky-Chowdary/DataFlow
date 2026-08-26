"""The ladder's row budget must be enforced before a population is resident.

A 10M-row destination peaked at ~15 GB RSS in the verification step: the
readers ran ``fetchall()`` and only then did ``run_five_layer_verification``
decline the work. Declining after the allocation is not declining.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest

from services.verification_ladder import (
    MAX_LADDER_ROWS,
    PopulationTooLarge,
    read_sqlite_rows,
)
from src.transfer.reconcile_step import _ladder_declined, _maybe_attach_verification_ladder
from src.transfer.models import EndpointConfig

BUDGET = 500


def _sqlite_with(tmp_path: Path, rows: int) -> str:
    path = str(tmp_path / "pop.db")
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        conn.executemany(
            "INSERT INTO t (id, v) VALUES (?, ?)", [(i, f"v{i}") for i in range(rows)]
        )
    return path


def test_reader_stops_at_the_budget_instead_of_materializing(tmp_path: Path) -> None:
    path = _sqlite_with(tmp_path, 2000)
    with pytest.raises(PopulationTooLarge) as excinfo:
        read_sqlite_rows(database=path, table="t", max_rows=BUDGET)
    # It stopped one row past the budget, not at the 2000-row population.
    assert excinfo.value.budget == BUDGET
    assert excinfo.value.rows_read == BUDGET + 1


def test_reader_returns_everything_under_the_budget(tmp_path: Path) -> None:
    path = _sqlite_with(tmp_path, 120)
    rows = read_sqlite_rows(database=path, table="t", max_rows=BUDGET)
    assert len(rows) == 120
    assert rows[0] == {"id": "0", "v": "v0"}


def test_column_projection_survives_batched_reads(tmp_path: Path) -> None:
    path = _sqlite_with(tmp_path, 30)
    rows = read_sqlite_rows(database=path, table="t", columns=["v"], max_rows=BUDGET)
    assert rows[3] == {"v": "v3"}


def test_declining_keeps_gate8_and_names_the_budget() -> None:
    report = {"passed": True, "checksum_match": True, "assurance_level": "full_checksum"}
    out = _ladder_declined(report, 10_000_000, 250_000)
    ladder = out["verification_ladder"]
    assert ladder["skipped"] is True
    assert ladder["population_proof"] is False
    assert ladder["population_checksum_proof"] is True
    assert "10000000" in ladder["reason"] and "250000" in ladder["reason"]
    # Gate-8's own verdict is untouched — only localization was declined.
    assert out["passed"] is True
    assert out["assurance_level"] == "full_checksum"


def test_oversized_run_never_reaches_the_readers(monkeypatch: Any) -> None:
    """The count we already have is enough to decline — no read is attempted."""

    def explode(**_: Any) -> list[dict[str, Any]]:
        raise AssertionError("reader must not run for an oversized population")

    monkeypatch.setattr("services.verification_ladder.read_postgres_rows", explode)
    monkeypatch.setattr("services.verification_ladder.read_sqlite_rows", explode)

    report = {"passed": True, "source_rows": MAX_LADDER_ROWS + 1, "target_rows": 1}
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=EndpointConfig(kind="database", format="postgresql", table="t"),
        source_endpoint=None,
        records=[],
        columns=["id"],
        dest_summary={},
        mappings=[{"source": "id", "target": "id"}],
        validation_mode="strict",
    )
    assert out["verification_ladder"]["skipped"] is True


def test_oversized_mysql_still_names_the_decline() -> None:
    """A dest the in-memory ladder does not read must not return a bare Gate-8 report."""
    report = {"passed": True, "source_rows": MAX_LADDER_ROWS + 1, "target_rows": 1}
    out = _maybe_attach_verification_ladder(
        report,
        endpoint=EndpointConfig(kind="database", format="mysql", table="t"),
        source_endpoint=None,
        records=[],
        columns=["id"],
        dest_summary={},
        mappings=[{"source": "id", "target": "id"}],
        validation_mode="strict",
    )
    assert out["verification_ladder"]["skipped"] is True
    assert out["passed"] is True
