"""D388 — the reversibility gate and the reversibility report must agree.

QA (r55-r75): PG → SQL Server, no transforms, ``require_reversibility``: the run
was refused "not reversible: <table>.payload" with g24 details
``{verdict: partly_reversible, lossy_columns: []}`` while the job's
reversibility report said "reversible".

Root cause: the planner maps ``jsonb`` → ``NVARCHAR(MAX)`` with the cast
transform ``json``. At Execute the gate checks that transform on the design
sample, where PostgreSQL renders a jsonb value as ``{"k": 1}``; ``json`` writes
``{"k":1}``, so 29 of 30 values "would change" under a strict text compare and
the column stayed open. The job report has no sample, so it said reversible.
jsonb has no formatting to lose (PostgreSQL stores it parsed), so the gate and
the writer's per-row enforcement now compare documents (numbers by their
literal text); everything else keeps the strict text rule.

SQL Server is CI-only here. The Execute gate runs before the destination is
opened, so the SQL Server leg drives the real engine to its g24 decision; the
write leg (per-row enforcement while writing) runs PG → SQLite with the same
mapping the planner hands Execute for SQL Server.
"""

from __future__ import annotations

import json
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
from tests import value_fidelity_route as r  # noqa: E402


@pytest.fixture()
def store(tmp_path: Path, monkeypatch):
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    import services.sync_cursor as sc

    class _Jobs(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kw):
            self.jobs.setdefault(job_id, {}).update(fields or {})
            return True

    fake = _Jobs()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.setenv("DATAFLOW_PII_HASH_KEY", "d388-test-key")
    return fake


@pytest.fixture()
def gates(monkeypatch):
    """Every g24 decision Execute makes (the real gate; only observed)."""
    import services.reversibility_sources as sources

    seen: list[dict] = []
    real = sources.execute_gate

    def _observe(request, planned):
        gate = real(request, planned)
        if gate is not None:
            seen.append(gate)
        return gate

    monkeypatch.setattr(sources, "execute_gate", _observe)
    return seen


_DOCS = {10: {}, 11: {"s": "東京 ß 😀"}, 9: {"n": 99999999999.999999, "x": [1.50, None]}}


@pytest.fixture()
def source_table():
    pg = r.pg_conn()
    table = "d388_" + uuid.uuid4().hex[:8]
    cur = pg.cursor()
    cur.execute(f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, note TEXT, payload JSONB)")
    for i in range(1, 31):
        doc = None if i in (3, 30) else _DOCS.get(i, {"k": i, "nested": {"a": [i, "b"]}})
        cur.execute(
            f"INSERT INTO {table} VALUES (%s, %s, %s)",  # nosec B608
            (i, f"n{i}", None if doc is None else json.dumps(doc)),
        )
    try:
        yield table
    finally:
        cur.execute(f"DROP TABLE IF EXISTS {table}")  # nosec B608
        pg.close()


def _pg(table: str) -> EndpointConfig:
    c = r.PG
    return EndpointConfig(kind="database", format="postgresql", table=table, host=c["host"],
                          port=c["port"], database=c["database"], username=c["username"],
                          password=c["password"], schema="public")


def _run(store, request: TransferRequest):
    """``execute``: the job is created from the request as every route creates it."""
    result = UniversalTransferEngine().execute(request)
    return result.job_id, result


def test_sqlserver_execute_gate_passes_an_untransformed_jsonb_copy(store, gates, source_table):
    """AC-1 on the QA pair: Execute's g24 for PG → SQL Server (planner's own mappings)."""
    dest = EndpointConfig(kind="database", format="sqlserver", table=source_table, host="127.0.0.1",
                          port=1, database="ci", username="ci", password="ci", schema="dbo")
    _job, _result = _run(store, TransferRequest(
        source=_pg(source_table), destination=dest, sync_mode="full_refresh_overwrite",
        require_reversibility=True, validation_mode="strict",
    ))
    assert gates, "Execute made no g24 decision"
    gate = gates[-1]
    assert gate["status"] == "pass", gate
    assert gate["details"]["verdict"] == "reversible"


def _sqlserver_like_mappings(protect: str = "") -> list[dict]:
    """The mapping the planner hands Execute for jsonb → NVARCHAR(MAX), on a text destination."""
    payload = {"source": "payload", "target": "payload", "source_type": "JSONB",
               "target_type": "TEXT", "transform": "json", "confidence": 0.95}
    if protect:
        payload.update(transform=protect, transform_override=True)
    return [
        {"source": "id", "target": "id", "source_type": "BIGINT", "target_type": "INTEGER", "confidence": 0.95},
        {"source": "note", "target": "note", "source_type": "TEXT", "target_type": "TEXT", "confidence": 0.95},
        payload,
    ]


def test_require_reversibility_run_writes_and_report_agrees(store, gates, source_table, tmp_path):
    from services.reversibility_sources import job_report

    db = tmp_path / "d.sqlite"
    job_id, result = _run(store, TransferRequest(
        source=_pg(source_table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(db), table="t"),
        mappings=_sqlserver_like_mappings(), sync_mode="full_refresh_overwrite",
        require_reversibility=True, validation_mode="strict",
    ))
    assert result.success, result.error
    assert gates and gates[-1]["status"] == "pass", gates
    conn = sqlite3.connect(str(db))
    try:
        stored = dict(conn.execute("SELECT id, payload FROM t").fetchall())
    finally:
        conn.close()
    assert len(stored) == 30 and stored[3] is None and stored[30] is None
    for i, text in stored.items():
        if text is not None:  # every document lands as itself (enforced on every row while writing)
            assert json.loads(text) == json.loads(json.dumps(_DOCS.get(i, {"k": i, "nested": {"a": [i, "b"]}})))
    report = job_report(store.get_job(job_id) or {}, job_id)
    assert report["verdict"] == gates[-1]["details"]["verdict"] == "reversible"


def test_a_lossy_column_is_still_refused_and_listed(store, gates, source_table, tmp_path):
    """AC-2: the refusal names only the lossy column, in lossy_columns too."""
    db = tmp_path / "d.sqlite"
    _job, result = _run(store, TransferRequest(
        source=_pg(source_table),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(db), table="t"),
        mappings=_sqlserver_like_mappings("hash_pii"), sync_mode="full_refresh_overwrite",
        require_reversibility=True, validation_mode="strict",
    ))
    assert not result.success
    gate = gates[-1]
    assert gate["status"] == "block" and "payload" in gate["message"], gate
    assert "note" not in gate["message"]
    assert any("payload" in str(c) for c in gate["details"]["lossy_columns"]), gate
