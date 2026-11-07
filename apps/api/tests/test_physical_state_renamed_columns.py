"""Gate-8 catalog compare follows the run's column mappings.

QA (client fixture, PG ``orders.id`` → MySQL ``orders_target.order_id``): the
key was carried, yet the compare read raw names and called the primary key
and NOT NULL absent, which withheld the migration-proven claim.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

from services.physical_state_diff import (
    CatalogIndex,
    PhysicalState,
    compare_physical_state,
    verify_physical_state,
)
from src.transfer.reconcile_step import _source_to_dest_column_map


def _state(**kw) -> PhysicalState:
    return PhysicalState(found=True, readable=True, **kw)


def test_renamed_primary_key_and_not_null_are_carried() -> None:
    src = _state(primary_key=("id",), not_null=frozenset({"id"}))
    dst = _state(primary_key=("order_id",), not_null=frozenset({"order_id"}))
    assert compare_physical_state(src, dst)["aspects"]["primary_key"]["status"] == "absent"
    out = compare_physical_state(src, dst, column_map={"id": "order_id"})
    assert out["aspects"]["primary_key"]["status"] == "carried"
    assert out["aspects"]["not_null"]["status"] == "carried"
    assert out["absent"] == []


def test_rename_still_reports_a_key_that_was_not_built() -> None:
    src = _state(primary_key=("id",))
    dst = _state()
    out = compare_physical_state(src, dst, column_map={"id": "order_id"})
    assert out["aspects"]["primary_key"]["status"] == "absent"
    assert out["aspects"]["primary_key"]["missing"] == ["order_id"]


def test_rename_covers_unique_index_and_default_columns() -> None:
    ix = CatalogIndex(unique=False, keys=("email desc",), predicate="", nulls_not_distinct=False)
    src = _state(
        unique_constraints=frozenset({("email",)}),
        indexes=frozenset({ix}),
        defaults=frozenset({("status", "'new'")}),
    )
    dst = _state(
        unique_constraints=frozenset({("contact_email",)}),
        indexes=frozenset({ix._replace(keys=("contact_email desc",))}),
        defaults=frozenset({("state", "'new'")}),
    )
    out = compare_physical_state(
        src, dst, column_map={"email": "contact_email", "status": "state"}
    )
    for aspect in ("unique_constraints", "indexes", "defaults"):
        assert out["aspects"][aspect]["status"] == "carried", aspect


def test_column_map_ignores_intentional_omits() -> None:
    mappings = [
        {"source": "id", "target": "order_id"},
        {"source": "note", "target": "memo", "omit": True, "intentional_omit": True},
    ]
    cmap = _source_to_dest_column_map(mappings)
    assert cmap == {"id": "order_id"}


def test_live_sqlite_catalogs_with_renamed_key(tmp_path: Path) -> None:
    src_db = tmp_path / "src.db"
    dst_db = tmp_path / "dst.db"
    with sqlite3.connect(src_db) as c:
        c.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, item TEXT NOT NULL)")
    with sqlite3.connect(dst_db) as c:
        c.execute("CREATE TABLE orders_target (order_id INTEGER PRIMARY KEY, product TEXT NOT NULL)")
    common = dict(source_db_type="sqlite", dest_db_type="sqlite", source_table="orders",
                  dest_table="orders_target")
    raw = verify_physical_state(
        source_cfg={"type": "sqlite", "database": str(src_db)},
        dest_cfg={"type": "sqlite", "database": str(dst_db)},
        **common,
    )
    assert "primary_key" in raw["absent"]
    mapped = verify_physical_state(
        source_cfg={"type": "sqlite", "database": str(src_db)},
        dest_cfg={"type": "sqlite", "database": str(dst_db)},
        column_map={"id": "order_id", "item": "product"},
        **common,
    )
    assert mapped["aspects"]["primary_key"]["status"] == "carried"
    assert mapped["aspects"]["not_null"]["status"] == "carried"
