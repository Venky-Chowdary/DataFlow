"""Source-side duplicate-key probe in preflight."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest


@pytest.fixture
def sqlite_dupes_connector(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[str, str]:
    """Create a saved SQLite connector and a table with duplicate ``id`` values."""
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE_BACKEND", "file")
    store_path = tmp_path / "connectors.json"
    monkeypatch.setenv("DATAFLOW_CONNECTOR_STORE", str(store_path))

    db_path = tmp_path / "dupes.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.executemany(
        "INSERT INTO jobs VALUES (?, ?)",
        [("a", "A"), ("b", "B"), ("a", "A2"), ("c", "C"), ("b", "B2")],
    )
    conn.commit()
    conn.close()

    from services.connector_store import create_connector

    saved = create_connector(
        {
            "name": "SQLite Dupes",
            "type": "sqlite",
            "role": "source",
            "connection_string": f"sqlite:///{db_path}",
            "ssl": False,
        }
    )
    return str(saved.id), "jobs"


def test_source_duplicate_probe_sqlite(sqlite_dupes_connector: tuple[str, str]) -> None:
    """The probe returns the duplicate keys and their counts."""
    connector_id, table = sqlite_dupes_connector
    from services.source_duplicate_probe import probe_source_duplicate_keys

    result = probe_source_duplicate_keys(
        source_connector_id=connector_id,
        source_table=table,
        primary_key="id",
    )
    assert len(result) == 2
    values = {r["value"]: r["count"] for r in result}
    assert values.get("a") == 2
    assert values.get("b") == 2


def test_preflight_blocks_on_source_duplicate_keys(
    sqlite_dupes_connector: tuple[str, str],
) -> None:
    """A database source with duplicate identity keys must fail G9 before Run."""
    connector_id, table = sqlite_dupes_connector
    from services.preflight_service import run_file_preflight

    sample_rows: list[dict[str, Any]] = [{"id": "c", "name": "C"}]
    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": None},
        {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
    ]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=5,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        source_connected=True,
        source_kind="database",
        source_format="sqlite",
        sync_mode="append",
        sample_rows=sample_rows,
        destination_db_type="postgresql",
        source_connector_id=connector_id,
        source_table=table,
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        validation_mode="strict",
    )
    gate_status = {g["id"]: g for g in result["gates"]}
    assert gate_status["g9_data_integrity"]["status"] == "block"
    g9_issues = gate_status["g9_data_integrity"].get("details", {}).get("issues", [])
    assert any("duplicate key values from source probe" in str(i) for i in g9_issues)
    assert result["passed"] is False
