"""Reversibility gate: third adversarial review (C1, M3, M4, M6 and the gate Lows).

Real engine runs (SQLite files, in-process job store) wherever the finding was
confirmed on the engine; the rest at the function the finding names.
"""

from __future__ import annotations

import ast
import os
import sqlite3
import uuid
from pathlib import Path

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

API = Path(__file__).resolve().parent.parent


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


def _q(path: Path, sql: str):
    conn = sqlite3.connect(str(path))
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.fixture()
def fake(monkeypatch, tmp_path):
    import src.transfer.engine as engine_mod
    from services import audit_log
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields)
            return True

    store = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: store)
    monkeypatch.setattr(audit_log, "STORE_PATH", tmp_path / "audit.jsonl")
    return store


def _run(fake, request):
    from src.transfer.engine import UniversalTransferEngine

    job_id = "rev" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(request, job_id)


def _sqlite_request(src: Path, dst: Path, table: str = "people", **kw):
    from src.transfer.models import EndpointConfig, TransferRequest

    base = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table=table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table=table),
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": table, "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=True,
        compliance_acknowledged=True, acknowledgment_actor="owner@example.com",
        acknowledgment_reason="reviewed run of the reversibility gate",
    )
    base.update(kw)
    return TransferRequest(**base)


def _g24(result) -> bool:
    blob = str(result.error or "") + str(getattr(result, "error_details", "") or "")
    return "must be reversible" in blob or "g24" in blob


# ------------------------------------------------------------------- M6


def _gate_calls(tree: ast.AST) -> list[ast.Call]:
    return [n for n in ast.walk(tree) if isinstance(n, ast.Call)
            and isinstance(n.func, ast.Attribute) and n.func.attr == "_execute_policy_gates_for_request"]


def test_m6_only_the_streaming_path_hands_planned_to_the_millisecond_gate():
    """``planned=`` switches on g3_millisecond_fit; the in-memory and file paths never passed it before."""
    tree = ast.parse((API / "src/transfer/engine.py").read_text(encoding="utf-8"))
    keywords = [sorted(k.arg for k in call.keywords if k.arg) for call in _gate_calls(tree)]
    assert len(keywords) == 3
    assert sum("planned" in k for k in keywords) == 1, keywords
    assert all("planned" in k or "reversibility" in k for k in keywords), keywords


def test_m6_reversibility_inputs_do_not_run_the_millisecond_gate(monkeypatch, tmp_path):
    """Flag off: a datetime onto a millisecond carrier runs no source aggregate on the in-memory / file paths."""
    from services import millisecond_fit as mf
    from src.transfer.engine_preflight import _execute_policy_gates_for_request
    from src.transfer.models import EndpointConfig, TransferRequest

    db = tmp_path / "s.sqlite"
    _exec(db, "CREATE TABLE ev (id INTEGER PRIMARY KEY, at TIMESTAMP)")
    _exec(db, "INSERT INTO ev VALUES (1, '2026-01-01 10:00:00.123456')")
    calls: list[int] = []
    real = mf.source_sub_millisecond_counts
    monkeypatch.setattr(mf, "source_sub_millisecond_counts", lambda *a, **k: (calls.append(1), real(*a, **k))[1])
    maps = [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "at", "target": "at", "source_type": "TIMESTAMP(6)", "target_type": "date"}]
    types, dest_types = {"id": "INTEGER", "at": "TIMESTAMP(6)"}, {"id": "INTEGER", "at": "date"}
    inputs = (maps, types, dest_types, [{"id": 1, "at": "2026-01-01 10:00:00.123456"}], 1)

    def gates(require: bool, **kw):
        request = TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=str(db), table="ev"),
            destination=EndpointConfig(kind="database", format="elasticsearch", database="x", table="ev"),
            sync_mode="full_refresh_overwrite", require_reversibility=require)
        return [g["id"] for g in _execute_policy_gates_for_request(request, source_columns=["id", "at"], **kw)]

    baseline = gates(False)
    assert gates(False, reversibility=inputs) == baseline and calls == []
    # The gate still gets its inputs when the run must be reversible - and still no millisecond aggregate.
    assert gates(True, reversibility=inputs) == [*baseline, "g24_reversibility"] and calls == []
    # The streaming path's own ``planned`` keeps its millisecond answer (D148), unchanged.
    assert "g3_millisecond_fit" in gates(False, planned=inputs) and calls == [1]
