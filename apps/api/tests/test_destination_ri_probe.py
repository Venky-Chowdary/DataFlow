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

from services.destination_ri_probe import (
    _same_relationship,
    relationship_identity,
    verify_destination_referential_integrity,
)
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


def test_relationship_identity_is_the_column_pairs() -> None:
    """Order in the DDL is not a different promise. A different parent column is."""
    forward = relationship_identity(["a", "b"], "public.parent", ["x", "y"])
    reversed_pairs = relationship_identity(["b", "a"], "parent", ["y", "x"])
    other_parent_column = relationship_identity(["a", "b"], "parent", ["y", "x"])
    other_schema = relationship_identity(["a", "b"], "archive.parent", ["x", "y"])
    assert forward is not None and reversed_pairs is not None
    assert _same_relationship(forward, reversed_pairs) is True
    assert other_parent_column is not None
    assert _same_relationship(forward, other_parent_column) is False
    assert other_schema is not None
    assert _same_relationship(forward, other_schema) is False


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


def test_enforced_fk_on_a_different_parent_column_is_scanned(tmp_path: Path) -> None:
    """Same child column and parent table, different parent column, is not enforced.

    ``parent_id → parent.legacy_id`` does not prove ``parent_id → parent.id``.
    The row 10 is a real legacy key and an orphan of id.
    """
    path = str(tmp_path / "wrong_parent_col.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE parent (id INTEGER PRIMARY KEY, legacy_id INTEGER UNIQUE)"
        )
        conn.execute("INSERT INTO parent (id, legacy_id) VALUES (1, 10), (2, 20)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, "
            "parent_id INTEGER REFERENCES parent(legacy_id))"
        )
        conn.execute("INSERT INTO child (id, parent_id) VALUES (1, 10)")
    result = verify_destination_referential_integrity(
        "sqlite",
        {"type": "sqlite", "database": path},
        table="child",
        foreign_keys=SOURCE_FK,
    )
    assert result["relations"][0]["status"] == "scanned"
    assert result["orphan_rows"] == 1
    assert result["verified"] is False


def test_reversed_composite_pairs_still_count_as_enforced(tmp_path: Path) -> None:
    """(b, a) → (y, x) is the same FK as (a, b) → (x, y)."""
    path = str(tmp_path / "pair_order.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE parent (x INTEGER, y INTEGER, PRIMARY KEY (x, y))"
        )
        conn.execute("INSERT INTO parent (x, y) VALUES (1, 2)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, "
            "FOREIGN KEY (a, b) REFERENCES parent (x, y))"
        )
        conn.execute("INSERT INTO child (id, a, b) VALUES (1, 1, 2)")
    result = verify_destination_referential_integrity(
        "sqlite",
        {"type": "sqlite", "database": path},
        table="child",
        foreign_keys=[
            {
                "constrained_columns": ["b", "a"],
                "referred_table": "public.parent",
                "referred_columns": ["y", "x"],
            }
        ],
    )
    assert result["verified"] is True
    assert result["relations"][0]["status"] == "enforced"


def test_two_parent_aliases_are_not_scanned_as_clean(tmp_path: Path) -> None:
    """A local customers row must not prove a payload that also names real_parent."""
    path = str(tmp_path / "two_parents.db")
    with sqlite3.connect(path) as conn:
        conn.execute("CREATE TABLE customers (id INTEGER PRIMARY KEY)")
        conn.execute("INSERT INTO customers (id) VALUES (10)")
        conn.execute("CREATE TABLE real_parent (id INTEGER PRIMARY KEY)")
        conn.execute(
            "CREATE TABLE orders (id INTEGER PRIMARY KEY, customer_id INTEGER)"
        )
        conn.execute("INSERT INTO orders (id, customer_id) VALUES (1, 10)")
    result = verify_destination_referential_integrity(
        "sqlite",
        {"type": "sqlite", "database": path},
        table="orders",
        foreign_keys=[
            {
                "columns": ["customer_id"],
                "referenced_table": "customers",
                "referenced_columns": ["id"],
                "referred_table": "real_parent",
                "referred_columns": ["id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "unavailable"
    assert result["orphan_rows"] == 0
    reason = result["relations"][0]["reason"]
    assert "customers" in reason
    assert "real_parent" in reason


def test_parent_named_in_another_schema_is_not_the_local_table(tmp_path: Path) -> None:
    """A local ``parent`` does not prove ``sales.parent``. The scan must not borrow it."""
    cfg = _db(
        tmp_path,
        "CREATE TABLE child (id INTEGER PRIMARY KEY, parent_id INTEGER)",
        "INSERT INTO child (id, parent_id) VALUES (1, 1)",
    )
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="child",
        foreign_keys=[
            {
                "constrained_columns": ["parent_id"],
                "referred_schema": "sales",
                "referred_table": "parent",
                "referred_columns": ["id"],
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "unavailable"
    assert "sales.parent" in result["relations"][0]["reason"]


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
    """Unreported match is MATCH SIMPLE: any NULL component imposes no constraint."""
    cfg = _composite_db(tmp_path, "(1, NULL, 999)", "(2, 9, NULL)")
    result = _composite_probe(cfg)
    assert result["verified"] is True
    assert result["orphan_rows"] == 0
    assert result["relations"][0]["match"] == "simple"


def test_match_full_partial_null_is_an_orphan_even_when_the_constraint_exists(
    tmp_path: Path,
) -> None:
    """A stored MATCH SIMPLE foreign key does not prove a MATCH FULL source rule.

    SQLite lists the constraint and has no match type. The old scan skipped
    those rows. (NULL, 999) and (9, NULL) are orphans under MATCH FULL.
    (NULL, NULL) is allowed.
    """
    path = str(tmp_path / "full.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, "
            "PRIMARY KEY (tenant_id, order_no))"
        )
        conn.execute("INSERT INTO orders VALUES (1, 100)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, "
            "order_no INTEGER, FOREIGN KEY (tenant_id, order_no) "
            "REFERENCES orders (tenant_id, order_no))"
        )
        conn.execute("INSERT INTO child VALUES (1, NULL, 999)")
        conn.execute("INSERT INTO child VALUES (2, 9, NULL)")
        conn.execute("INSERT INTO child VALUES (3, NULL, NULL)")
        conn.execute("INSERT INTO child VALUES (4, 1, 100)")
    result = verify_destination_referential_integrity(
        "sqlite",
        {"type": "sqlite", "database": path},
        table="child",
        foreign_keys=[
            {
                "constrained_columns": ["tenant_id", "order_no"],
                "referred_table": "orders",
                "referred_columns": ["tenant_id", "order_no"],
                "match": "FULL",
            }
        ],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "scanned"
    assert result["relations"][0]["match"] == "full"
    assert result["orphan_rows"] == 2
    assert set(result["relations"][0]["examples"]) == {"+999", "9+"}


def test_match_partial_is_not_a_completed_scan(tmp_path: Path) -> None:
    cfg = _composite_db(tmp_path, "(1, 1, 100)")
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="child",
        foreign_keys=[{**COMPOSITE_FK[0], "match": "PARTIAL"}],
    )
    assert result["verified"] is False
    assert result["relations"][0]["status"] == "unavailable"
    assert result["orphan_rows"] == 0
    assert "MATCH PARTIAL" in result["relations"][0]["reason"]
    assert "not a completed scan" in result["relations"][0]["reason"]


def test_two_match_spellings_do_not_scan(tmp_path: Path) -> None:
    cfg = _composite_db(tmp_path, "(1, 1, 100)")
    result = verify_destination_referential_integrity(
        "sqlite",
        cfg,
        table="child",
        foreign_keys=[
            {
                **COMPOSITE_FK[0],
                "match": "FULL",
                "options": {"match": "SIMPLE"},
            }
        ],
    )
    assert result["verified"] is False
    assert "two match types" in result["relations"][0]["reason"]


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

    unproven = {
        "verified": False,
        "asked": True,
        "orphan_rows": 0,
        "unavailable_relations": ["customer_id->customers"],
        "reason": "parent scan did not run",
        "relations": [
            {
                "columns": ["customer_id"],
                "referred_table": "customers",
                "status": "unavailable",
                "available": False,
                "orphan_count": 0,
            }
        ],
    }
    unproven_gate = build_dest_ri_gate(unproven, has_relationships=True)
    assert unproven_gate["status"] == "warn", unproven_gate
    kept = apply_dest_ri_to_reconcile(
        {"passed": True, "message": "checksums match"},
        evidence=unproven,
        has_relationships=True,
    )
    assert kept["passed"] is True
    assert "unproven" in kept["message"]
