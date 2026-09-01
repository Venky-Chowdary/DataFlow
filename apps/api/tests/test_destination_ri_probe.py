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


COMPOSITE_FK = [
    {
        "constrained_columns": ["tenant_id", "order_no"],
        "referred_table": "orders",
        "referred_columns": ["tenant_id", "order_no"],
    }
]


def _composite_db(tmp_path: Path, *child_rows: str) -> dict[str, str]:
    path = str(tmp_path / "composite.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, "
            "PRIMARY KEY (tenant_id, order_no))"
        )
        conn.execute("INSERT INTO orders VALUES (1, 100), (1, 101), (2, 100)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, "
            "order_no INTEGER)"
        )
        for row in child_rows:
            conn.execute(f"INSERT INTO child (id, tenant_id, order_no) VALUES {row}")
    return {"type": "sqlite", "database": path}


def _composite_probe(cfg: dict[str, str]) -> dict[str, Any]:
    return verify_destination_referential_integrity(
        "sqlite", cfg, table="child", foreign_keys=COMPOSITE_FK
    )


def test_composite_fk_scans_the_whole_tuple_not_one_column(tmp_path: Path) -> None:
    """(2, 101) is an orphan even though 2 and 101 each exist on their own."""
    cfg = _composite_db(tmp_path, "(1, 1, 100)", "(2, 2, 101)")
    result = _composite_probe(cfg)
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "scanned"
    assert result["orphan_rows"] == 1
    assert result["relations"][0]["examples"] == ["2+101"]


def test_composite_fk_with_intact_tuples_is_clean(tmp_path: Path) -> None:
    cfg = _composite_db(tmp_path, "(1, 1, 100)", "(2, 1, 101)", "(3, 2, 100)")
    result = _composite_probe(cfg)
    assert result["verified"] is True
    assert result["orphan_rows"] == 0


def test_composite_fk_partial_null_is_unconstrained_not_orphan(tmp_path: Path) -> None:
    """MATCH SIMPLE: any NULL component means the key imposes no constraint."""
    cfg = _composite_db(tmp_path, "(1, NULL, 999)", "(2, 9, NULL)")
    result = _composite_probe(cfg)
    assert result["verified"] is True
    assert result["orphan_rows"] == 0


def test_self_referential_composite_is_aliased_and_scanned(tmp_path: Path) -> None:
    """Hierarchy tables join the same relation twice — parent must be aliased."""
    path = str(tmp_path / "self_ref.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE emp (org INTEGER, emp_id INTEGER, mgr_org INTEGER, "
            "mgr_id INTEGER, PRIMARY KEY (org, emp_id))"
        )
        conn.execute("INSERT INTO emp VALUES (1, 1, NULL, NULL)")
        conn.execute("INSERT INTO emp VALUES (1, 2, 1, 1)")
        conn.execute("INSERT INTO emp VALUES (1, 3, 1, 99)")
    result = verify_destination_referential_integrity(
        "sqlite",
        {"type": "sqlite", "database": path},
        table="emp",
        foreign_keys=[
            {
                "constrained_columns": ["mgr_org", "mgr_id"],
                "referred_table": "emp",
                "referred_columns": ["org", "emp_id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "scanned"
    assert result["orphan_rows"] == 1
    assert result["relations"][0]["examples"] == ["1+99"]


def test_mismatched_column_counts_are_unavailable(tmp_path: Path) -> None:
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
                "referred_columns": ["id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["reason"] == (
        "relationship has no usable column pairing"
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


def test_g22_skip_when_no_relationships() -> None:
    from services.destination_ri_probe import build_dest_ri_gate, referential_integrity_proven

    gate = build_dest_ri_gate(
        {"verified": False, "asked": False, "relations": []},
        has_relationships=False,
    )
    assert gate["status"] == "skip"
    assert referential_integrity_proven({"verified": True, "relations": []}) is False


def test_g22_blocks_orphans_and_unproven() -> None:
    from services.destination_ri_probe import (
        apply_dest_ri_to_reconcile,
        build_dest_ri_gate,
        referential_integrity_proven,
    )

    orphans = {
        "verified": False,
        "asked": True,
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
    assert referential_integrity_proven(orphans) is False
    gate = build_dest_ri_gate(orphans, has_relationships=True)
    assert gate["status"] == "block"
    stamped = apply_dest_ri_to_reconcile(
        {"passed": True, "message": "checksums match"},
        evidence=orphans,
        has_relationships=True,
    )
    assert stamped["passed"] is False

    clean = {
        "verified": True,
        "asked": True,
        "orphan_rows": 0,
        "relations": [
            {
                "columns": ["parent_id"],
                "referred_table": "parent",
                "status": "scanned",
                "available": True,
                "orphan_count": 0,
            }
        ],
    }
    assert referential_integrity_proven(clean) is True
    assert build_dest_ri_gate(clean, has_relationships=True)["status"] == "pass"
