"""D123 / D132 — Gate-8's verification ladder must read back what was written.

The ladder re-reads the source table, which is the *pre-image*: a column
transform (``upper``) or a CAST_AND_CONTINUE integer cast leaves the source
holding ``'xyz'`` / ``'001'`` while the destination holds ``'XYZ'`` / ``1``,
and a cast failure is quarantined, not landed. Comparing the raw re-read
failed every incremental rerun ("L4/L5 localized: source='xyz'
target='XYZ'"), and an incremental_append rerun into the occupied table
compared the run's window against every row earlier runs had appended.

Real SQLite → SQLite runs through the engine with preflight ON: two
incremental runs each, exact destination contents.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    fake = _FakeMongo()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _exec(path: Path, sql: str, rows) -> None:
    with sqlite3.connect(path) as conn:
        conn.executemany(sql, rows)


def _code_mapping(kind: str) -> dict:
    if kind == "upper":
        return {
            "source": "code", "target": "code", "target_type": "TEXT",
            "transform": "upper", "transform_override": True, "confidence": 0.99,
        }
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column="code", source_type="TEXT", destination_type="INTEGER",
        approved_by="risk@example.com", reason="codes are integers",
        execution_policy="CAST_AND_CONTINUE", transform="integer",
    ).to_dict()
    return {
        "source": "code", "target": "code", "target_type": "INTEGER",
        "transform": "integer", "confidence": 0.99,
        "risk_acknowledged": True, "risk_contract": contract,
    }


@pytest.mark.parametrize("kind", ["upper", "cast_and_continue"])
@pytest.mark.parametrize("mode", ["incremental_append", "incremental_deduped"])
def test_incremental_rerun_with_transform_verifies(tmp_path, isolated, mode, kind):
    src, dst = tmp_path / "src.db", tmp_path / "dst.db"
    with sqlite3.connect(src) as conn:
        conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, code TEXT, seq INTEGER NOT NULL)")
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)",
          [(1, "001", 1), (2, "002", 2), (3, "abc", 3), (4, "010", 4)])
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        mappings=[
            {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
            _code_mapping(kind),
            {"source": "seq", "target": "seq", "target_type": "INTEGER", "confidence": 1.0},
        ],
        sync_mode=mode,
        stream_contracts=[{
            "name": "t", "cursor_field": "seq", "cursor_semantics": "monotonic_sequence",
            "primary_key": ["id"], "sync_mode": mode, "selected": True,
        }],
        validation_mode="strict",
        triggered_by="risk@example.com",
    )

    def run():
        job_id = uuid.uuid4().hex[:24]
        isolated.update_job_status(job_id, "pending", transfer_request={})
        return UniversalTransferEngine().execute_tracked(request, job_id)

    first = run()
    assert first.success, first.error
    _exec(src, "INSERT INTO t VALUES (?, ?, ?)", [(5, "005", 5), (6, "xyz", 6)])
    second = run()
    assert second.success, second.error

    with sqlite3.connect(dst) as conn:
        got = conn.execute("SELECT id, code FROM t ORDER BY id").fetchall()
    if kind == "upper":
        assert got == [(1, "001"), (2, "002"), (3, "ABC"), (4, "010"), (5, "005"), (6, "XYZ")]
    else:
        # 'abc' and 'xyz' are cast failures: quarantined, never landed.
        assert got == [(1, 1), (2, 2), (4, 10), (5, 5)]
        assert int(second.reconciliation.get("rejected_rows") or 0) == 1
