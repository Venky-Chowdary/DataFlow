"""Composite FK preflight uses the same MATCH SIMPLE tuple scan as dest RI."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from services.population_orphan_probe import probe_population_fk_orphans
from services.sample_orphan_probe import probe_sample_fk_orphans


def _db(tmp_path: Path) -> str:
    path = str(tmp_path / "fk.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, "
            "PRIMARY KEY (tenant_id, order_no))"
        )
        conn.execute("INSERT INTO orders VALUES (1, 100), (1, 101), (2, 100)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)"
        )
        conn.execute("INSERT INTO child VALUES (1, 1, 100)")
        conn.execute("INSERT INTO child VALUES (2, 2, 101)")  # orphan tuple
        conn.execute("INSERT INTO child VALUES (3, NULL, 999)")  # MATCH SIMPLE unconstrained
    return path


COMPOSITE_FK = [
    {
        "columns": ["tenant_id", "order_no"],
        "referenced_table": "orders",
        "referenced_columns": ["tenant_id", "order_no"],
    }
]


def test_population_composite_counts_whole_tuple_orphan(tmp_path: Path):
    path = _db(tmp_path)
    report = probe_population_fk_orphans(
        child_table="child",
        mappings=[
            {"source": "tenant_id", "target": "tenant_id"},
            {"source": "order_no", "target": "order_no"},
        ],
        foreign_keys=COMPOSITE_FK,
        source_config={"type": "sqlite", "database": path},
        validation_mode="strict",
    )
    assert report["ran"] is True
    assert report["complete"] is True
    assert report["population_proof"] is False
    assert report["orphan_count"] == 1
    assert report["findings"][0]["code"] == "fk_orphan_in_population"
    assert "2+101" in report["findings"][0]["message"]


def test_population_composite_clean_tuples_are_proven(tmp_path: Path):
    path = str(tmp_path / "clean.db")
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE orders (tenant_id INTEGER, order_no INTEGER, "
            "PRIMARY KEY (tenant_id, order_no))"
        )
        conn.execute("INSERT INTO orders VALUES (1, 100), (1, 101)")
        conn.execute(
            "CREATE TABLE child (id INTEGER PRIMARY KEY, tenant_id INTEGER, order_no INTEGER)"
        )
        conn.execute("INSERT INTO child VALUES (1, 1, 100), (2, 1, 101), (3, NULL, 1)")
    report = probe_population_fk_orphans(
        child_table="child",
        mappings=[],
        foreign_keys=COMPOSITE_FK,
        source_config={"type": "sqlite", "database": path},
        validation_mode="strict",
    )
    assert report["complete"] is True
    assert report["orphan_count"] == 0
    assert report["population_proof"] is True
    assert report["findings"] == []


def test_sample_composite_detects_orphan_tuple(tmp_path: Path):
    path = _db(tmp_path)
    report = probe_sample_fk_orphans(
        sample_rows=[
            {"tenant_id": 1, "order_no": 100},
            {"tenant_id": 2, "order_no": 101},
            {"tenant_id": None, "order_no": 999},
        ],
        mappings=[],
        foreign_keys=COMPOSITE_FK,
        source_config={"type": "sqlite", "database": path},
        validation_mode="strict",
    )
    assert report["ran"] is True
    assert report["population_proof"] is False
    assert report["orphan_count"] == 1
    assert report["checked_values"] == 2
    assert report["findings"][0]["code"] == "fk_orphan_in_sample"
    assert any("2+101" in (ex or "") for ex in report["checks"][0].get("orphan_examples") or [])
