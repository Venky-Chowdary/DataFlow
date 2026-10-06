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
    resume: bool = False,
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
        resume=resume,
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


def test_append_blocks_a_repeated_key_when_the_table_does_not_enforce_it(
    tmp_path: Path,
) -> None:
    """No unique constraint must not make a second copy of the same key legal."""
    db_path = tmp_path / "heap.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?)", ("a", "A"))
    conn.commit()
    conn.close()
    result = _run(
        db_path=str(db_path),
        sample_rows=[{"id": "a", "name": "A2"}],
        sync_mode="append",
        destination_pk_columns=[],
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "block", g6
    assert "second copy" in g6["message"]
    assert "upsert/merge" in g6["message"]
    assert result["passed"] is False


def test_append_blocks_the_same_mapped_row_when_there_is_no_key(tmp_path: Path) -> None:
    db_path = tmp_path / "notes.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE notes (body TEXT, city TEXT)")
    conn.execute("INSERT INTO notes VALUES (?, ?)", ("hello", "london"))
    conn.commit()
    conn.close()
    from services.preflight_service import run_file_preflight

    mappings = [
        {"source": "body", "target": "body", "confidence": 0.99, "transform": None},
        {"source": "city", "target": "city", "confidence": 0.99, "transform": None},
    ]
    result = run_file_preflight(
        columns=["body", "city"],
        column_types={"body": "VARCHAR", "city": "VARCHAR"},
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=[{"body": "hello", "city": "london"}],
        destination_db_type="sqlite",
        destination_table="notes",
        destination_table_exists=True,
        destination_pk_columns=[],
        destination_config=_dest_cfg(str(db_path)),
        destination_column_types={"body": "TEXT", "city": "TEXT"},
        validation_mode="strict",
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "block", g6
    assert result["passed"] is False

    fresh = run_file_preflight(
        columns=["body", "city"],
        column_types={"body": "VARCHAR", "city": "VARCHAR"},
        row_count=1,
        mappings=mappings,
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=[{"body": "hello", "city": "paris"}],
        destination_db_type="sqlite",
        destination_table="notes",
        destination_table_exists=True,
        destination_pk_columns=[],
        destination_config=_dest_cfg(str(db_path)),
        destination_column_types={"body": "TEXT", "city": "TEXT"},
        validation_mode="strict",
    )
    fresh_gate = _gate(fresh, "g6_target_ddl")
    assert fresh_gate["status"] == "pass", fresh_gate
    assert "no collision" in fresh_gate["message"]


def test_composite_key_blocks_only_the_whole_pair(tmp_path: Path) -> None:
    db_path = tmp_path / "lines.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "CREATE TABLE lines (id TEXT, region TEXT, qty INT, PRIMARY KEY (id, region))"
    )
    conn.execute("INSERT INTO lines VALUES (?, ?, ?)", ("a", "us", 1))
    conn.commit()
    conn.close()
    from services.preflight_service import run_file_preflight

    mappings = [
        {"source": "id", "target": "id", "confidence": 0.99},
        {"source": "region", "target": "region", "confidence": 0.99},
        {"source": "qty", "target": "qty", "confidence": 0.99},
    ]

    def run(rows: list[dict[str, Any]]) -> dict[str, Any]:
        return run_file_preflight(
            columns=["id", "region", "qty"],
            column_types={"id": "VARCHAR", "region": "VARCHAR", "qty": "INTEGER"},
            row_count=len(rows),
            mappings=mappings,
            destination_connected=True,
            destination_can_create=True,
            destination_can_write=True,
            source_connected=True,
            source_kind="file",
            source_format="csv",
            sync_mode="append",
            sample_rows=rows,
            destination_db_type="sqlite",
            destination_table="lines",
            destination_table_exists=True,
            destination_pk_columns=["id", "region"],
            destination_config=_dest_cfg(str(db_path)),
            destination_column_types={"id": "TEXT", "region": "TEXT", "qty": "INT"},
            validation_mode="strict",
        )

    other_region = run([{"id": "a", "region": "eu", "qty": 2}])
    assert _gate(other_region, "g6_target_ddl")["status"] != "block", other_region
    same_pair = run([{"id": "a", "region": "us", "qty": 9}])
    blocked = _gate(same_pair, "g6_target_ddl")
    assert blocked["status"] == "block", blocked
    assert same_pair["passed"] is False


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


def test_resumed_append_applies_the_overlap_instead_of_blocking(tmp_path: Path) -> None:
    """Resume re-delivers the interrupted batch; the writer resolves it on the key.

    Blocking here strands a half-loaded destination with no forward path — the
    operator cannot append (keys collide) and did not ask for overwrite.
    """
    db_path = _seed_destination(tmp_path, [("a", "A"), ("b", "B")])
    result = _run(
        db_path=db_path,
        sample_rows=[{"id": "a", "name": "A2"}, {"id": "z", "name": "Z"}],
        sync_mode="append",
        destination_pk_columns=["id"],
        resume=True,
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "pass", g6
    assert "Resume re-delivery overlaps" in g6["message"]
    details = g6.get("details") or {}
    assert details.get("delivery") == "at_least_once_idempotent_apply", g6
    assert details.get("rule_id") == "g6_target_ddl.append_key_collision_resume", g6


def test_unenforced_probe_that_cannot_run_is_not_a_clean_pass(tmp_path: Path) -> None:
    """A heap with no readable destination must not pass as 'uniqueness not required'."""
    result = _run(
        db_path=str(tmp_path / "empty.db"),
        sample_rows=[{"id": "a", "name": "A"}],
        sync_mode="full_refresh_append",
        destination_pk_columns=[],
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "warn", g6
    assert "did not run" in g6["message"]
    assert "uniqueness not required" not in g6["message"]


def test_resume_of_an_unenforced_heap_still_blocks(tmp_path: Path) -> None:
    """Resume is idempotent only when the destination rejects a second copy.

    A heap stores the re-delivered batch again. Overlap is not a forward path.
    """
    db_path = tmp_path / "heap.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?)", ("a", "A"))
    conn.commit()
    conn.close()
    result = _run(
        db_path=str(db_path),
        sample_rows=[{"id": "a", "name": "A"}],
        sync_mode="append",
        destination_pk_columns=[],
        resume=True,
    )
    g6 = _gate(result, "g6_target_ddl")
    assert g6["status"] == "block", g6
    assert "second copy" in g6["message"]
    assert result["passed"] is False


def test_resume_flag_is_recorded_on_the_probe_result(tmp_path: Path) -> None:
    """The probe still runs on resume — the overlap is evidence, not a skip."""
    from services.destination_key_collision_probe import probe_append_key_collisions

    db_path = _seed_destination(tmp_path, [("a", "A")])
    res = probe_append_key_collisions(
        mappings=[{"source": "id", "target": "id"}, {"source": "name", "target": "name"}],
        source_columns=["id", "name"],
        sample_rows=[{"id": "a", "name": "A2"}],
        sync_mode="append",
        dest_kind="database",
        validation_mode="strict",
        destination_config=_dest_cfg(db_path),
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        destination_unique_keys=None,
        contract_primary_key="id",
        resume=True,
    )
    assert res is not None
    assert res.status == "ran"
    assert res.findings, "overlap must stay visible as evidence"
    assert res.idempotent_apply is True
