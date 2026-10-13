"""Go-live register: scheduler clock, append keys, inventory, referential integrity.

These are the shared algorithms behind the no-go snapshot. Each test names the
defect it locks.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, timezone
from pathlib import Path

import sqlalchemy as sa


def test_parse_ts_compares_naive_text_and_bson_datetime() -> None:
    """DEF-B-031 / DEF-B2-011: one naive timestamp must not abort the beat."""
    from services.schedule_store import _parse_ts

    naive = _parse_ts("2026-10-08T04:00:00")
    assert naive is not None and naive.tzinfo is not None
    as_datetime = _parse_ts(datetime(2026, 10, 8, 4, 0, 0))
    assert as_datetime == naive
    zulu = _parse_ts("2026-10-08T04:00:00Z")
    assert zulu == naive
    # The beat clock is aware. This subtraction is what killed every cron.
    delta = datetime.now(timezone.utc) - naive
    assert delta.total_seconds() > 0


def test_append_create_new_does_not_declare_the_source_key() -> None:
    """DEF-B2-001: full_refresh_append create-new must not add PRIMARY KEY(id)."""
    from connectors.generic_sql import _build_table_for_write
    from services.schema_fidelity import resolve_create_fidelity_plan

    columns = ["id", "qty", "status"]
    types = {"id": "INTEGER", "qty": "INTEGER", "status": "VARCHAR(10)"}
    catalog = {
        "dialect": "postgresql",
        "columns": columns,
        "column_types": types,
        "nullable": {"id": False, "qty": True, "status": True},
        "primary_key": ["id"],
        "unique_keys": [["status"]],
    }
    plan = resolve_create_fidelity_plan(
        source_schema_catalog=catalog,
        mappings=[{"source": c, "target": c} for c in columns],
        target_columns=list(columns),
        target_types=[types[c] for c in columns],
        dest_dialect="mysql",
        carry_keys=False,
    )
    assert plan.primary_key == []
    assert plan.unique_constraints == []
    assert not any(
        str(clause).upper().startswith("PRIMARY KEY") for clause in plan.table_constraints
    )
    engine = sa.create_engine("sqlite://")
    table = _build_table_for_write(
        engine,
        "orders",
        None,
        list(columns),
        types,
        fidelity_plan=plan,
        conflict_columns=["id"],
        declare_key=False,
    )
    ddl = str(sa.schema.CreateTable(table).compile(engine))
    assert "PRIMARY KEY" not in ddl
    assert "UNIQUE" not in ddl
    assert "NOT NULL" in ddl


def test_enforced_append_collision_count_is_the_batch_not_the_sample(
    tmp_path: Path,
) -> None:
    """DEF-B2-001: '5 key value(s)' was the display sample, not the collision."""
    from services.preflight_service import run_file_preflight

    db_path = tmp_path / "keyed.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    rows = [(str(i), f"n{i}") for i in range(8)]
    conn.executemany("INSERT INTO jobs VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    sample = [{"id": str(i), "name": f"n{i}"} for i in range(8)]
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=len(sample),
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
        ],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=sample,
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        destination_config={"type": "sqlite", "connection_string": f"sqlite:///{db_path}"},
        destination_column_types={"id": "TEXT", "name": "TEXT"},
        validation_mode="strict",
    )
    g6 = {g["id"]: g for g in result["gates"]}["g6_target_ddl"]
    assert g6["status"] == "block", g6
    details = g6.get("details") or {}
    assert details.get("collision_count") == 8, g6
    assert len(details.get("sample_collisions") or []) <= 5
    assert "8" in g6["message"]
    assert result["passed"] is False


def test_execute_refuses_an_enforced_key_before_insert(tmp_path: Path) -> None:
    """DEF-B2-012: do not insert the first row when the live key already collides."""
    from services.destination_key_collision_probe import (
        refuse_enforced_append_before_write,
    )

    db_path = tmp_path / "live.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE jobs (id TEXT PRIMARY KEY, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES ('a', 'A')")
    conn.commit()
    conn.close()
    cfg = {"type": "sqlite", "connection_string": f"sqlite:///{db_path}"}
    refusal = refuse_enforced_append_before_write(
        destination_config=cfg,
        destination_db_type="sqlite",
        destination_table="jobs",
        headers=["id", "name"],
        data_rows=[["a", "A2"], ["b", "B"]],
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "name", "target": "name"},
        ],
    )
    assert refusal and "already stored" in refusal
    assert "1 key" in refusal

    heap = tmp_path / "heap.db"
    conn = sqlite3.connect(str(heap))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES ('a', 'A')")
    conn.commit()
    conn.close()
    assert (
        refuse_enforced_append_before_write(
            destination_config={"type": "sqlite", "connection_string": f"sqlite:///{heap}"},
            destination_db_type="sqlite",
            destination_table="jobs",
            headers=["id", "name"],
            data_rows=[["a", "A2"]],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "name", "target": "name"},
            ],
        )
        is None
    )


def test_planned_source_key_does_not_block_a_heap_append(tmp_path: Path) -> None:
    """DEF-B2-001: a plan that copied source id must not refuse a PK-less table."""
    from services.preflight_service import run_file_preflight

    heap = tmp_path / "heap.db"
    conn = sqlite3.connect(str(heap))
    conn.execute("CREATE TABLE jobs (id TEXT, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES ('a', 'A')")
    conn.commit()
    conn.close()
    result = run_file_preflight(
        columns=["id", "name"],
        column_types={"id": "VARCHAR", "name": "VARCHAR"},
        row_count=1,
        mappings=[
            {"source": "id", "target": "id", "confidence": 0.99},
            {"source": "name", "target": "name", "confidence": 0.99},
        ],
        destination_connected=True,
        destination_can_create=True,
        destination_can_write=True,
        source_connected=True,
        source_kind="file",
        source_format="csv",
        sync_mode="full_refresh_append",
        sample_rows=[{"id": "a", "name": "A2"}],
        destination_db_type="sqlite",
        destination_table="jobs",
        destination_table_exists=True,
        destination_pk_columns=["id"],
        destination_config={
            "type": "sqlite",
            "connection_string": f"sqlite:///{heap}",
        },
        destination_column_types={"id": "TEXT", "name": "TEXT"},
        validation_mode="strict",
    )
    g6 = {g["id"]: g for g in result["gates"]}["g6_target_ddl"]
    assert g6["status"] == "warn", g6
    assert (g6.get("details") or {}).get("key_enforced") is False
    assert result["passed"] is True


def test_scheduler_sleeps_until_the_next_due_instant(monkeypatch) -> None:
    """A fixed 60s poll fired 25s late, then 62s late. Sleep until next_run_at."""
    import services.schedule_store as store

    soon = datetime.now(timezone.utc) + timedelta(seconds=12)

    class _Sched:
        enabled = True
        next_run_at = soon.isoformat()

    monkeypatch.setattr(store, "_load_all", lambda: [_Sched()])
    wait = store.seconds_until_next_schedule()
    assert 1.0 <= wait <= 13.0
    monkeypatch.setattr(store, "_load_all", lambda: [])
    assert store.seconds_until_next_schedule() == 60.0


def test_object_store_create_keeps_pending_columns() -> None:
    """DEF-C-043: pending_dest_schema is create-new on an object, not zero columns."""
    from connectors.writer_common import resolve_target_columns

    mappings = [
        {
            "source": "sku",
            "target": "sku",
            "assignment_strategy": "pending_dest_schema",
            "target_type": "VARCHAR",
        }
    ]
    skipped, _types = resolve_target_columns(
        mappings, {"sku": "VARCHAR"}, preserve_case=True
    )
    assert skipped == []
    kept, types = resolve_target_columns(
        mappings, {"sku": "VARCHAR"}, preserve_case=True, table_exists=False
    )
    assert kept == ["sku"]
    assert types and types[0]
