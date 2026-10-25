"""D101 — upsert into an existing table without a unique key.

An upsert writer needs a unique constraint on the upsert columns. When the
destination table already exists without one, the writers used to create the
``uidx_…`` index silently at run time — undisclosed DDL on an operator table
that fails raw (`could not create unique index`) when duplicates exist.
Validate must inspect the destination, count duplicate keys, disclose the
planned DDL, and require explicit approval (`ddl_changes_acknowledged`).
"""
from __future__ import annotations

import sqlite3

import pytest



def _sqlite_dest(path, rows, unique=False):
    conn = sqlite3.connect(path)
    conn.execute(
        f"CREATE TABLE e (id INTEGER, v TEXT"
        f"{', PRIMARY KEY (id)' if unique else ''})"
    )
    conn.executemany("INSERT INTO e VALUES (?, ?)", rows)
    conn.commit()
    conn.close()
    return {"type": "sqlite", "database": str(path)}


def _preflight(dest_cfg, dest_exists=True, **kw):
    from services.preflight_service import run_file_preflight

    return run_file_preflight(
        columns=["id", "v"],
        column_types={"id": "integer", "v": "string"},
        row_count=None,
        mappings=[
            {"source": "id", "target": "id"},
            {"source": "v", "target": "v"},
        ],
        destination_connected=True,
        source_kind="database",
        source_format="sqlite",
        source_table="src_t",
        destination_table="e",
        destination_table_exists=dest_exists,
        destination_db_type="sqlite",
        destination_config=dest_cfg,
        sync_mode="incremental_upsert",
        stream_contracts=[
            {"selected": True, "primary_key": "id", "cursor_column": "v"}
        ],
        **kw,
    )


def _gate(result, gate_id="g_upsert_dest_key"):
    return next(
        (g for g in result.get("gates") or [] if g.get("id") == gate_id), None
    )


def test_preflight_blocks_upsert_existing_dest_without_unique_key_and_duplicates(
    tmp_path,
):
    dest = _sqlite_dest(
        tmp_path / "d.db", [(10, "a"), (10, "b"), (11, "c")], unique=False
    )
    res = _preflight(dest)
    gate = _gate(res)
    assert gate is not None, "D101 gate must run for upsert into existing table"
    assert gate["status"] == "block"
    details = gate.get("details") or {}
    assert details.get("duplicate_key_count") == 1
    assert details.get("duplicate_row_count") == 2
    assert any(
        s.get("values", {}).get("id") == "10"
        for s in details.get("duplicate_samples") or []
    )
    assert res["passed"] is False
    # Operator surface: the dedicated upsert_dest_ddl root cause with counts.
    rcs = res.get("root_causes") or []
    rc = next((r for r in rcs if r.get("kind") == "upsert_dest_ddl"), None)
    assert rc is not None, "expected an upsert_dest_ddl root cause"
    assert "duplicate" in (rc.get("summary") or "").lower()


def test_upsert_index_creation_disclosed_and_requires_approval(tmp_path):
    # Clean dest, no unique key: disclosed DDL + approval required, not silence.
    dest = _sqlite_dest(tmp_path / "d.db", [(10, "a"), (11, "c")], unique=False)
    res = _preflight(dest)
    gate = _gate(res)
    assert gate is not None
    assert gate["status"] == "block"
    details = gate.get("details") or {}
    assert details.get("requires_approval") is True
    ddl = details.get("planned_ddl") or ""
    assert "CREATE UNIQUE INDEX" in ddl
    assert "id" in ddl
    assert res["passed"] is False
    # The disclosed DDL is on the gate details and surfaced via root cause.
    rcs = res.get("root_causes") or []
    rc = next((r for r in rcs if r.get("kind") == "upsert_dest_ddl"), None)
    assert rc is not None
    assert "CREATE UNIQUE INDEX" in (rc.get("summary") or "")


def test_upsert_index_creation_passes_when_ddl_approved(tmp_path):
    dest = _sqlite_dest(tmp_path / "d.db", [(10, "a"), (11, "c")], unique=False)
    res = _preflight(dest, ddl_changes_acknowledged=True)
    gate = _gate(res)
    assert gate is not None
    assert gate["status"] == "pass"
    assert "approv" in (gate.get("message") or "").lower()
    # The D101 gate itself is satisfied — approval lifts its block, and no
    # upsert_dest_ddl root cause remains (other gates are out of this scope).
    assert not any(
        r.get("kind") == "upsert_dest_ddl"
        for r in res.get("root_causes") or []
    )


def test_upsert_existing_dest_with_unique_key_needs_no_ddl(tmp_path):
    dest = _sqlite_dest(tmp_path / "d.db", [(10, "a")], unique=True)
    res = _preflight(dest, destination_pk_columns=["id"])
    gate = _gate(res)
    assert gate is not None
    assert gate["status"] == "pass"
    assert not any(
        r.get("kind") == "upsert_dest_ddl"
        for r in res.get("root_causes") or []
    )


def test_upsert_new_dest_needs_no_ddl_gate(tmp_path):
    dest = _sqlite_dest(tmp_path / "d.db", [(10, "a")], unique=False)
    res = _preflight(dest, dest_exists=False)
    gate = _gate(res)
    assert gate is None, "no gate when DataFlow creates the table itself"


def test_dup_probe_unmeasured_not_clean_zero(tmp_path):
    from services.upsert_ddl_gate import count_dest_duplicate_keys

    # Missing table: ran=False — never invent a clean zero.
    cfg = {"type": "sqlite", "database": str(tmp_path / "empty.db")}
    sqlite3.connect(tmp_path / "empty.db").close()
    res = count_dest_duplicate_keys(cfg, "ghost", ["id"])
    assert res["ran"] is False
    assert res["duplicate_keys"] == 0
    # Missing key column on a real table: also unmeasured.
    dest = _sqlite_dest(tmp_path / "d.db", [(10, "a")])
    res = count_dest_duplicate_keys(dest, "e", ["nope"])
    assert res["ran"] is False
