"""Constraints, indexes, nullability and defaults compared against real catalogs.

Every case runs against a live SQLite catalog rather than a stubbed inspector:
the whole value of this module is that it reads what the database actually
stored, so a mock would prove nothing.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from services.migration_certificate import physical_state_findings
from services.physical_state_diff import (
    ASPECTS,
    compare_physical_state,
    read_physical_state,
    verify_physical_state,
)

FULL = """
CREATE TABLE {name} (
  id INTEGER PRIMARY KEY,
  code TEXT NOT NULL UNIQUE,
  parent_id INTEGER REFERENCES parent(id),
  note TEXT DEFAULT 'n'
)
"""
BARE = "CREATE TABLE {name} (id INTEGER, code TEXT, parent_id INTEGER, note TEXT)"


def _db(tmp_path: Path, *statements: str) -> dict[str, str]:
    path = str(tmp_path / "cat.db")
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        for stmt in statements:
            conn.execute(stmt)
    return {"type": "sqlite", "database": path}


def _verify(cfg: dict[str, str], src: str, dest: str) -> dict:
    return verify_physical_state(
        source_db_type="sqlite",
        source_cfg=cfg,
        source_table=src,
        dest_db_type="sqlite",
        dest_cfg=cfg,
        dest_table=dest,
    )


def test_faithful_copy_verifies_every_aspect(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        "CREATE INDEX ix_src ON src (note)",
        FULL.format(name="dst"),
        "CREATE INDEX ix_dst ON dst (note)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is True
    assert result["absent"] == []
    assert set(result["aspects"]) == set(ASPECTS)
    assert {a["status"] for a in result["aspects"].values()} == {"carried"}


def test_dropped_constraints_are_reported_absent(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        "CREATE INDEX ix_src ON src (note)",
        BARE.format(name="dst"),
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is False
    assert set(result["absent"]) == set(ASPECTS)
    assert result["aspects"]["primary_key"]["missing"] == ["id"]
    assert result["aspects"]["foreign_keys"]["missing"] == ["parent_id->parent->id"]
    assert result["aspects"]["not_null"]["missing"] == ["code"]
    assert result["aspects"]["defaults"]["missing"] == ["note"]


def test_missing_primary_key_alone_fails(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, code TEXT)",
        "CREATE TABLE dst (id INTEGER, code TEXT)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["absent"] == ["primary_key"]
    assert result["aspects"]["indexes"]["status"] == "carried"


def test_extra_destination_index_does_not_fail(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE src (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE TABLE dst (id INTEGER PRIMARY KEY, note TEXT)",
        "CREATE INDEX ix_dst ON dst (note)",
    )
    result = _verify(cfg, "src", "dst")
    assert result["verified"] is True
    assert result["aspects"]["indexes"]["extra"] == ["note"]


def test_absent_destination_table_is_unreadable_not_carried(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="src"))
    result = _verify(cfg, "src", "nope")
    assert result["verified"] is False
    assert "not found" in result["reason"]
    assert not result.get("aspects")


def test_case_folded_table_and_columns_match(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        'CREATE TABLE "SRC" (ID INTEGER PRIMARY KEY, Code TEXT NOT NULL)',
        'CREATE TABLE "dst" (id INTEGER PRIMARY KEY, code TEXT NOT NULL)',
    )
    result = _verify(cfg, "src", "DST")
    assert result["verified"] is True


def test_unreadable_source_is_not_a_pass(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="dst"))
    result = verify_physical_state(
        source_db_type="sqlite",
        source_cfg={"type": "sqlite", "database": str(tmp_path / "missing.db")},
        source_table="src",
        dest_db_type="sqlite",
        dest_cfg=cfg,
        dest_table="dst",
    )
    assert result["verified"] is False


def test_read_state_reports_the_stored_facts(tmp_path: Path) -> None:
    cfg = _db(tmp_path, FULL.format(name="src"), "CREATE INDEX ix_src ON src (note)")
    state = read_physical_state("sqlite", cfg, table="src")
    assert state.found and state.readable
    assert state.primary_key == ("id",)
    assert ("code",) in state.unique_constraints
    assert state.not_null >= {"code"}
    assert "note" in state.defaults


def test_compare_is_symmetric_about_direction(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        FULL.format(name="src"),
        BARE.format(name="dst"),
    )
    src = read_physical_state("sqlite", cfg, table="src")
    dst = read_physical_state("sqlite", cfg, table="dst")
    forward = compare_physical_state(src, dst)
    backward = compare_physical_state(dst, src)
    assert forward["verified"] is False
    # Nothing is missing when the destination is the richer table.
    assert backward["absent"] == []


def test_certificate_carries_schema_object_findings() -> None:
    recon = {
        "physical_state": {
            "schema_objects": {
                "verified": False,
                "absent": ["primary_key"],
                "aspects": {"primary_key": {"status": "absent", "missing": ["id"]}},
            }
        }
    }
    findings = physical_state_findings(recon)
    assert findings["schema_objects"]["absent"] == ["primary_key"]
    assert findings["schema_objects"]["verified"] is False


def test_certificate_marks_missing_comparison_unverified() -> None:
    findings = physical_state_findings({})
    assert findings["schema_objects"]["verified"] is False
    assert "not compared" in findings["schema_objects"]["reason"]
