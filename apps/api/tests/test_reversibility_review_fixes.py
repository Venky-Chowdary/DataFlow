"""Reversibility gate: regressions from the adversarial review of the Reversibility Proof.

Each test names the finding it pins (C1 row caps / filters, C2 recipe steps that
overwrite a column, C3 lossy type mappings the type system reads as widening, ...).
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATAFLOW_JOB_STORE", "memory")
os.environ.setdefault("DATAFLOW_DISABLE_OBJECT_STORE", "1")

from services.reversibility import (  # noqa: E402
    CONDITIONAL,
    INVERTIBLE,
    LOSSY,
    PARTLY,
    REVERSIBLE,
    analyze_table,
    classify_mapping,
)


def _m(source, target=None, transform="none", stype="", ttype=""):
    return {"source": source, "target": target or source, "transform": transform,
            "source_type": stype, "target_type": ttype}


_TYPED = [_m("id", stype="INTEGER", ttype="INTEGER"), _m("email", stype="TEXT", ttype="TEXT")]


def _row_changes(table):
    return [c for c in table["columns"] if c["reason_code"] == "row_change"]


# ------------------------------------------------- C1: row caps and filters


def test_c1_a_row_cap_is_a_lossy_row_change():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"], row_limit=2)
    [entry] = _row_changes(table)
    assert entry["invertibility"] == LOSSY and entry["transforms"] == ["limit"]
    assert entry["source"] is None and entry["target"] is None
    assert "2" in entry["reason"]
    assert table["verdict"] != REVERSIBLE


def test_c1_a_source_filter_is_a_lossy_row_change():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          source_filter={"column": "id", "op": "lte", "value": 3})
    [entry] = _row_changes(table)
    assert entry["invertibility"] == LOSSY and entry["transforms"] == ["source_filter"]
    assert table["verdict"] == PARTLY


def test_c1_no_cap_and_no_filter_add_nothing():
    table = analyze_table(table="people", mappings=_TYPED, source_columns=["id", "email"],
                          row_limit=0, source_filter={})
    assert not _row_changes(table) and table["verdict"] == REVERSIBLE


def test_c1_execute_request_with_a_limit_or_filter_blocks_g24():
    from services.reversibility_sources import execute_gate
    from src.transfer.models import EndpointConfig, TransferRequest

    def req(**kw):
        return TransferRequest(
            source=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
            destination=EndpointConfig(kind="database", format="sqlite", database=":memory:", table="t"),
            mappings=_TYPED, require_reversibility=True, **kw)

    planned = (_TYPED, {"id": "INTEGER", "email": "TEXT"}, {"id": "INTEGER", "email": "TEXT"})
    assert execute_gate(req(), planned)["status"] == "pass"
    capped = execute_gate(req(limit=2), planned)
    assert capped["status"] == "block" and "(rows)" in capped["message"]
    assert capped["details"]["lossy_columns"][0]["reason_code"] == "row_change"
    filtered = execute_gate(req(source_filter={"column": "id", "op": "lte", "value": 3}), planned)
    assert filtered["status"] == "block"


def test_c1_job_and_plan_reports_read_the_cap_and_filter():
    from services.reversibility_sources import job_report, plan_report

    job = {"transfer_request": {
        "source": {"format": "sqlite", "table": "t"}, "destination": {"format": "sqlite", "table": "t"},
        "column_types": {"id": "INTEGER"}, "mappings": [_m("id", ttype="INTEGER")],
        "limit": 5, "require_reversibility": True}}
    assert job_report(job, "j")["gate"]["status"] == "block"
    job["transfer_request"]["limit"] = 0
    job["transfer_request"]["source_filter"] = {"column": "id", "op": "eq", "value": 1}
    assert job_report(job, "j")["gate"]["status"] == "block"

    rev = SimpleNamespace(mappings=[_m("id")])
    plan = SimpleNamespace(
        id="p", source={"format": "sqlite", "table": "t"}, destination={"format": "sqlite", "table": "t"},
        source_columns=["id"], source_schema={"id": "INTEGER"}, target_schema={"id": "INTEGER"},
        sample_rows=[], policies={"require_reversibility": True, "row_limit": 10},
        active_revision=lambda: rev)
    report = plan_report(plan)
    assert report["gate"]["status"] == "block"
    assert any(c["reason_code"] == "row_change" for c in report["lossy_columns"])
    plan.policies = {"require_reversibility": True}
    assert plan_report(plan)["gate"]["status"] == "pass"


# ----------------------------------------------------- in-process engine e2e


def _exec(path: Path, sql: str, rows=None) -> None:
    conn = sqlite3.connect(str(path))
    try:
        with conn:
            conn.executemany(sql, rows) if rows is not None else conn.execute(sql)
    finally:
        conn.close()


@pytest.fixture()
def fake_store(monkeypatch):
    import src.transfer.engine as engine_mod
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Store(_FakeMongo):
        def update_job_fields(self, job_id, fields, **_kwargs):
            return super().update_job_fields(job_id, fields)

    fake = _Store()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    return fake


@pytest.fixture()
def people(tmp_path):
    src, dst = tmp_path / "src.sqlite", tmp_path / "dst.sqlite"
    _exec(src, "CREATE TABLE people (id INTEGER PRIMARY KEY, email TEXT)")
    _exec(src, "INSERT INTO people VALUES (?, ?)", [(i, f"u{i}@example.com") for i in range(1, 11)])
    return src, dst


def _execute(fake, src: Path, dst: Path, **over):
    from src.transfer.engine import UniversalTransferEngine
    from src.transfer.models import EndpointConfig, TransferRequest

    body = dict(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="people"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="people"),
        mappings=[dict(m, confidence=0.99) for m in _TYPED],
        sync_mode="full_refresh_overwrite",
        stream_contracts=[{"name": "people", "primary_key": ["id"], "selected": True}],
        validation_mode="strict", skip_preflight=False, require_reversibility=True,
        compliance_acknowledged=True,
    )
    body.update(over)
    job_id = "rev" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    return UniversalTransferEngine().execute_tracked(TransferRequest(**body), job_id)


def _dest_rows(dst: Path) -> int | None:
    if not dst.exists():
        return None
    conn = sqlite3.connect(str(dst))
    try:
        if not conn.execute("SELECT name FROM sqlite_master WHERE name='people'").fetchall():
            return None
        return conn.execute("SELECT count(*) FROM people").fetchone()[0]
    finally:
        conn.close()


def _blocked_by_g24(result) -> bool:
    return "g24_reversibility" in str(result.error_details or "")


def test_c1_engine_blocks_a_required_reversible_run_with_a_row_cap(people, fake_store):
    src, dst = people
    result = _execute(fake_store, src, dst, limit=2)
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None


def test_c1_engine_blocks_a_required_reversible_run_with_a_source_filter(people, fake_store):
    src, dst = people
    result = _execute(fake_store, src, dst, source_filter={"column": "id", "op": "lte", "value": 3})
    assert not result.success and _blocked_by_g24(result), result.error
    assert _dest_rows(dst) is None
