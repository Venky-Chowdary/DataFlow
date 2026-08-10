"""Destination orphan proof, against a live SQLite catalog and real rows.

The interesting case is the one every load-speed playbook creates: the
destination table was built without the source's foreign key, so the engine
accepted child rows whose parent never arrived. A catalog diff sees a missing
FK; only a scan sees the broken rows.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from services.destination_ri_probe import verify_destination_referential_integrity
from services.migration_certificate import (
    _referential_blockers,
    physical_state_findings,
)

SOURCE_FK = [
    {
        "constrained_columns": ["parent_id"],
        "referred_table": "parent",
        "referred_columns": ["id"],
    }
]


def _db(tmp_path: Path, *statements: str) -> dict[str, str]:
    path = str(tmp_path / "ri.db")
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO parent (id) VALUES (1), (2)")
        for stmt in statements:
            conn.execute(stmt)
    return {"type": "sqlite", "database": path}


def _probe(cfg: dict[str, str], table: str = "child", **kw: Any) -> dict[str, Any]:
    return verify_destination_referential_integrity(
        "sqlite", cfg, table=table, foreign_keys=SOURCE_FK, **kw
    )


def test_unenforced_fk_with_intact_rows_is_scanned_and_clean(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)",
        "INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 2), (3, NULL)",
    )
    result = _probe(cfg)
    assert result["verified"] is True
    assert result["relations"][0]["status"] == "scanned"
    assert result["orphan_rows"] == 0


def test_orphan_rows_are_counted_and_exampled(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)",
        "INSERT INTO child (id, parent_id) VALUES (1, 1), (2, 99), (3, 404)",
    )
    result = _probe(cfg)
    assert result["verified"] is False
    assert result["orphan_rows"] == 2
    rel = result["relations"][0]
    assert rel["status"] == "scanned"
    assert sorted(rel["examples"]) == ["404", "99"]
    assert result["orphan_relations"] == ["parent_id->parent"]


def test_enforced_fk_needs_no_scan(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, "
        "parent_id INTEGER REFERENCES parent(id))",
        "INSERT INTO child (id, parent_id) VALUES (1, 1)",
    )
    result = _probe(cfg)
    assert result["verified"] is True
    assert result["relations"][0]["status"] == "enforced"


def test_missing_parent_table_is_unavailable_never_clean(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path, "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)"
    )
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="child",
        foreign_keys=[
            {
                "constrained_columns": ["parent_id"],
                "referred_table": "never_migrated",
                "referred_columns": ["id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "unavailable"
    assert "absent from destination" in result["relations"][0]["reason"]
    assert result["unavailable_relations"] == ["parent_id->never_migrated"]


def test_composite_fk_is_unavailable_not_silently_passed(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER)",
    )
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="child",
        foreign_keys=[
            {
                "constrained_columns": ["a", "b"],
                "referred_table": "parent",
                "referred_columns": ["id", "id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["reason"] == (
        "composite foreign keys are not scanned"
    )


def test_missing_destination_table_reports_reason(tmp_path: Path) -> None:
    cfg = _db(tmp_path)
    result = _probe(cfg, table="nope")
    assert result["verified"] is False
    assert "not found in destination catalog" in result["reason"]


def test_case_folded_names_resolve_to_the_stored_spelling(tmp_path: Path) -> None:
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, Parent_Id INTEGER)",
        "INSERT INTO child (id, Parent_Id) VALUES (1, 77)",
    )
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="CHILD",
        foreign_keys=[
            {
                "constrained_columns": ["PARENT_ID"],
                "referred_table": "PARENT",
                "referred_columns": ["ID"],
            }
        ],
    )
    assert result["relations"][0]["status"] == "scanned"
    assert result["orphan_rows"] == 1


def test_orphans_block_the_certificate_verdict() -> None:
    findings = physical_state_findings(
        {
            "physical_state": {
                "referential_integrity": {
                    "verified": False,
                    "orphan_rows": 2,
                    "orphan_relations": ["parent_id->parent"],
                    "relations": [
                        {
                            "columns": ["parent_id"],
                            "referred_table": "parent",
                            "status": "scanned",
                            "available": True,
                            "orphan_count": 2,
                        }
                    ],
                }
            }
        }
    )
    blockers = _referential_blockers(findings)
    assert blockers and "no parent" in blockers[0]
    assert "2 child row(s)" in blockers[0]


def test_clean_scan_raises_no_blocker() -> None:
    findings = physical_state_findings(
        {"physical_state": {"referential_integrity": {"verified": True}}}
    )
    assert _referential_blockers(findings) == []


def test_missing_ri_evidence_is_reported_not_assumed() -> None:
    referential = physical_state_findings({})["referential_integrity"]
    assert referential["verified"] is False
    assert "not scanned" in referential["reason"]
