"""D26 — a TEXT source into an existing typed destination column.

The mapper binds ``amount TEXT`` to the destination's own ``amount NUMERIC``
(never a phantom ``amount_text``) and grades the text -> typed parse as a
lossy pair, so an unsigned strict run is refused by design. The explicit,
signed mapping (cast + CAST_AND_CONTINUE) is the supported way through and
must load. SQLite -> SQLite through the engine with preflight ON.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path

import pytest

from services.semantic_mapper import map_columns


@pytest.mark.parametrize(
    ("name", "samples", "target_type"),
    [
        ("amount", ["12.50", "7"], "DECIMAL(12,2)"),
        ("qty", ["1", "2", "3"], "INTEGER"),
        ("created", ["2024-01-01", "2024-02-02"], "DATE"),
        ("flag", ["true", "false"], "BOOLEAN"),
    ],
)
def test_text_source_binds_to_its_typed_column_and_is_graded_not_forked(name, samples, target_type):
    out = map_columns(
        [name],
        [name],
        source_schemas=[{"name": name, "inferred_type": "TEXT", "samples": samples}],
        target_schemas=[{"name": name, "inferred_type": target_type}],
        destination_db_type="postgresql",
        destination_table_exists=True,
        source_db_type="postgresql",
    )
    row = out[0]
    assert row["target"] == name, row
    assert not row.get("create_new"), row
    assert row["requires_review"], row


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    fake = _Store()
    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    return fake


def _signed(col, source_type, dest_type, transform):
    from services.migration_risk_contract import create_migration_risk_contract

    contract = create_migration_risk_contract(
        column=col, source_type=source_type, destination_type=dest_type,
        approved_by="risk@example.com", reason="text holds clean values",
        execution_policy="CAST_AND_CONTINUE", transform=transform,
    ).to_dict()
    return {
        "source": col, "target": col, "source_type": source_type,
        "target_type": dest_type, "transform": transform, "confidence": 0.99,
        "risk_acknowledged": True, "risk_contract": contract,
    }


def _run(fake, src, dst, mappings):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    job_id = "d26" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(
        TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
            sync_mode="full_refresh_append",
            validation_mode="strict",
            mappings=mappings,
        ),
        job_id,
    )


def _seed(tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount TEXT, qty TEXT)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?)", [(1, "12.50", "3"), (2, "7", "4")])
    conn.commit()
    conn.close()
    conn = sqlite3.connect(dst)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, amount NUMERIC(12,2), qty INTEGER)")
    conn.commit()
    conn.close()
    return src, dst


def test_unsigned_text_to_typed_is_refused_not_silently_cast(tmp_path, isolated):
    src, dst = _seed(tmp_path)
    result = _run(isolated, src, dst, [])
    assert not result.success
    assert "fidelity" in (result.error or "").lower()
    assert sqlite3.connect(dst).execute("SELECT COUNT(*) FROM t").fetchone()[0] == 0


def test_signed_explicit_mapping_loads_text_into_typed_columns(tmp_path, isolated):
    src, dst = _seed(tmp_path)
    mappings = [
        {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER", "confidence": 0.99},
        _signed("amount", "TEXT", "NUMERIC(12,2)", "decimal"),
        _signed("qty", "TEXT", "INTEGER", "integer"),
    ]
    result = _run(isolated, src, dst, mappings)
    assert result.success, result.error
    rows = sqlite3.connect(dst).execute("SELECT id, amount, qty FROM t ORDER BY id").fetchall()
    assert rows == [(1, 12.5, 3), (2, 7, 4)]
