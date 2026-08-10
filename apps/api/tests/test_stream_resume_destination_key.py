"""A resumed append writes through the destination's own key.

Failure-injection regression: SIGKILL mid-append left 800 rows in a keyed
PostgreSQL table; the resume then had no stream contract, fell back to a plain
INSERT, and preflight blocked on the unavoidable duplicate-key abort — the run
could neither finish nor be replayed. Resolving the destination catalog key
turns that into an idempotent apply (at-least-once read, key-resolved write).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from transfer.stream import _destination_key_for_resume

MAPPINGS = [
    {"source": "id", "target": "id"},
    {"source": "name", "target": "name"},
]


def _dest(tmp_path: Path, ddl: str) -> tuple[str, dict[str, str]]:
    db_path = tmp_path / "dest.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(ddl)
    conn.commit()
    conn.close()
    return str(db_path), {
        "type": "sqlite",
        "connection_string": f"sqlite:///{db_path}",
    }


def test_resolves_single_column_destination_primary_key(tmp_path: Path) -> None:
    _, cfg = _dest(tmp_path, "CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    assert _destination_key_for_resume("sqlite", cfg, "jobs", MAPPINGS) == (
        ["id"],
        ["id"],
    )


def test_keyless_destination_yields_no_key(tmp_path: Path) -> None:
    """No enforced key means no ON CONFLICT target — never invent one."""
    _, cfg = _dest(tmp_path, "CREATE TABLE jobs (id TEXT, name TEXT)")
    assert _destination_key_for_resume("sqlite", cfg, "jobs", MAPPINGS) == ([], [])


def test_unmapped_key_column_yields_no_key(tmp_path: Path) -> None:
    """A key the mapping never writes cannot resolve rows — refuse to guess."""
    _, cfg = _dest(tmp_path, "CREATE TABLE jobs (tenant TEXT PRIMARY KEY, name TEXT)")
    assert _destination_key_for_resume("sqlite", cfg, "jobs", MAPPINGS) == ([], [])


def test_composite_destination_key_is_resolved_in_full(tmp_path: Path) -> None:
    _, cfg = _dest(
        tmp_path,
        "CREATE TABLE jobs (id TEXT, name TEXT, PRIMARY KEY (id, name))",
    )
    src, tgt = _destination_key_for_resume("sqlite", cfg, "jobs", MAPPINGS)
    assert sorted(src) == ["id", "name"]
    assert sorted(tgt) == ["id", "name"]


def test_missing_table_is_not_a_key(tmp_path: Path) -> None:
    _, cfg = _dest(tmp_path, "CREATE TABLE jobs (id TEXT PRIMARY KEY)")
    assert _destination_key_for_resume("sqlite", cfg, "absent", MAPPINGS) == ([], [])
