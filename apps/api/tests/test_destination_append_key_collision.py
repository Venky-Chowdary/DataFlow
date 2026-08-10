"""Append into an existing keyed destination must block at Validate, not Execute.

Regression: a real-service matrix run had ``postgresql->postgresql`` and
``sqlite->postgresql`` append routes pass all 13 gates and then abort at write
with ``duplicate key value violates unique constraint``. The collision was
knowable before the write — the destination already stored those keys.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

import pytest


def _seed_destination(tmp_path: Path, rows: list[tuple[str, str]]) -> str:
    db_path = tmp_path / "dest.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    conn.executemany("INSERT INTO jobs VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return str(db_path)


def _dest_cfg(db_path: str) -> dict[str, Any]:
    return {"type": "sqlite", "connection_string": f"sqlite:///{db_path}"}


def _run(
    *,
    db_path: str,
    sample_rows: list[dict[str, Any]],
    sync_mode: str,
    destination_pk_columns: list[str],
    destination_table_exists: bool = True,
) -> dict[str, Any]:
    from services.preflight_service import run_file_preflight

    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99, "transform": None},
        {"source": "name", "target": "name", "confidence": 0.99, "transform": None},
    ]
    result: dict[str, Any] = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=len(sample_rows),
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode=sync_mode,
        sample_rows=sample_rows,
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=destination_table_exists,
        destination_pk_columns=destination_pk_columns,
        destination_config=_dest_cfg(db_path),
        destination_column_types={"id": "TEXT", "name": "TEXT"},
        validation_mode="strict",
    )
    return result


def _gate(result: dict[str, Any], gate_id: str) -> dict[str, Any]:
    return {g["id"]: g for g in result["gates"]}[gate_id]


def test_append_blocks_when_destination_already_holds_the_key(tmp_path: Path) -> None:
    db_path = _seed_destination(tmp_path, [("a", "A"), ("b", "B")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "a", "name": "A2"}, {"id": "z", "name": "Z"}],
        sync_mode="append",
        destination_pk_columns=["id"],
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "block", g6
    assert "existing destination key" in g6["message"]
    assert "upsert/merge" in g6["message"]
    assert result["passed"] is False


def test_append_passes_when_batch_keys_are_new(tmp_path: Path) -> None:
    db_path = _seed_destination(tmp_path, [("a", "A"), ("b", "B")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "y", "name": "Y"}, {"id": "z", "name": "Z"}],
        sync_mode="append",
        destination_pk_columns=["id"],
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] != "block", g6


def test_no_enforced_destination_key_leaves_append_alone(tmp_path: Path) -> None:
    """A destination without a PK legally accepts repeated values."""
    db_path = _seed_destination(tmp_path, [("a", "A")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "a", "name": "A2"}],
        sync_mode="append",
        destination_pk_columns=[],
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] != "block", g6


def test_create_new_destination_is_not_probed(tmp_path: Path) -> None:
    db_path = _seed_destination(tmp_path, [("a", "A")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "a", "name": "A2"}],
        sync_mode="append",
        destination_pk_columns=["id"],
        destination_table_exists=False,
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] != "block", g6


@pytest.mark.parametrize("sync_mode", ["upsert", "merge", "full_refresh_overwrite"])
def test_key_resolving_sync_modes_are_exempt(tmp_path: Path, sync_mode: str) -> None:
    """Upsert/merge/overwrite define what happens to a colliding key."""
    db_path = _seed_destination(tmp_path, [("a", "A"), ("b", "B")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "a", "name": "A2"}],
        sync_mode=sync_mode,
        destination_pk_columns=["id"],
    )
    g6 = _gate(result, "g6_target_ddl")
    msg = str(g6.get("message") or "")
    assert "existing destination key" not in msg, g6


def test_composite_destination_key_is_not_treated_as_single_column(
    tmp_path: Path,
) -> None:
    """A composite PK tolerates a repeated first column — never invent a blocker."""
    from services.destination_key_collision_probe import destination_enforces_key

    assert destination_enforces_key("id", destination_pk_columns=["id", "region"]) is False
    assert destination_enforces_key("id", destination_pk_columns=["id"]) is True
    assert (
        destination_enforces_key(
            "email", destination_unique_keys=[{"columns": ["email"]}]
        )
        is True
    )


def test_unreachable_destination_is_a_skip_not_a_pass() -> None:
    """A probe that could not run must never be stamped as proof of no collision."""
    from services.destination_key_collision_probe import (
        probe_destination_key_collisions,
    )

    res = probe_destination_key_collisions(
        destination_config={"type": "sqlite", "connection_string": "sqlite:///:memory:"},
        destination_db_type="sqlite",
        destination_table="does_not_exist",
        key_column="id",
        values=["a"],
    )
    assert res.status == "error"
    assert res.findings == []
    assert res.ran is False
