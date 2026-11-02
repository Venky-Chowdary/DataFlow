"""Pilot Validate must probe an existing table with the connection it just opened.

DEF-B2-002 / DEF-C-027: Gate-2 said the destination table existed, and the
append collision check then warned that the connection or table was
unavailable. The Pilot inspects the saved connector and then called preflight
without that connection, so the probe could not run.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any

from services.secret_config import RedactedConfig


def _seed(db_path: Path, *, primary_key: bool) -> None:
    conn = sqlite3.connect(str(db_path))
    key = " PRIMARY KEY" if primary_key else ""
    conn.execute(f"CREATE TABLE jobs (id TEXT{key}, name TEXT)")
    conn.execute("INSERT INTO jobs VALUES (?, ?)", ("a", "A"))
    conn.commit()
    conn.close()


def _pilot(monkeypatch, db_path: Path, *, primary_key: bool) -> dict[str, Any]:
    cfg = RedactedConfig(
        {"type": "sqlite", "connection_string": f"sqlite:///{db_path}"}
    )

    def fake_inspect(**kwargs):
        assert kwargs.get("dest_table") == "jobs"
        return {
            "connected": True,
            "table_exists": True,
            "db_type": "sqlite",
            "can_write": True,
            "can_create_table": True,
            "column_types": {"id": "TEXT", "name": "TEXT"},
            "primary_key_columns": ["id"] if primary_key else [],
            "unique_keys": [{"columns": ["id"]}] if primary_key else [],
            "_probe_cfg": cfg,
        }

    monkeypatch.setattr(
        "services.preflight_service.inspect_destination_for_preflight", fake_inspect
    )
    monkeypatch.setattr(
        "services.preflight_service.run_transfer_policy_gates", lambda **_k: []
    )
    monkeypatch.setattr(
        "services.preflight_service.apply_policy_gates",
        lambda result, *_a, **_k: result,
    )
    from src.ai.copilot.transfer_tools import _run_preflight

    return _run_preflight(
        src_conn={"id": "src", "name": "src", "type": "postgresql"},
        dst_conn={"id": "dst", "name": "dst", "type": "sqlite"},
        src_table="sales",
        dst_table="jobs",
        src_rows=[
            {"name": "id", "inferred_type": "VARCHAR", "nullable": False},
            {"name": "name", "inferred_type": "VARCHAR", "nullable": True},
        ],
        sample_rows=[{"id": "a", "name": "A2"}],
        mappings=[
            {"source": "id", "target": "id", "confidence": 1.0},
            {"source": "name", "target": "name", "confidence": 1.0},
        ],
        mode="full_refresh_append",
        schema_policy="manual_review",
        validation_mode="balanced",
        src_db_type="postgresql",
        source_config={"type": "postgresql"},
        dest_db_type="sqlite",
        dest_exists=True,
        source_primary_key="id",
        source_kind="file",
        known_row_count=1,
        stream_contracts=[
            {
                "stream": "jobs",
                "selected": True,
                "primary_key": "id",
            }
        ],
    )


def _gate(result: dict[str, Any], gate_id: str) -> dict[str, Any]:
    return {g["id"]: g for g in result["gates"]}[gate_id]


def test_pilot_append_blocks_a_key_the_existing_table_already_holds(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "keyed.db"
    _seed(db_path, primary_key=True)
    monkeypatch.setattr(
        "services.preflight_run_store.STORE_PATH", tmp_path / "runs.jsonl"
    )
    result = _pilot(monkeypatch, db_path, primary_key=True)
    g6 = _gate(result, "g6_target_ddl")
    assert "unavailable" not in g6["message"]
    assert "did not run" not in g6["message"]
    assert g6["status"] == "block", g6
    assert "existing destination key" in g6["message"]


def test_pilot_append_warns_when_a_heap_already_holds_the_row(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "heap.db"
    _seed(db_path, primary_key=False)
    monkeypatch.setattr(
        "services.preflight_run_store.STORE_PATH", tmp_path / "runs.jsonl"
    )
    result = _pilot(monkeypatch, db_path, primary_key=False)
    g6 = _gate(result, "g6_target_ddl")
    assert "unavailable" not in g6["message"]
    assert "did not run" not in g6["message"]
    assert g6["status"] == "warn", g6
    assert "second copy" in g6["message"]
