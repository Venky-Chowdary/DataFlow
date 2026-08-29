"""Named fixture: failing transform not_null persists to the same DLQ."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from services.transform_models import DataTest, TransformModel
from services.transform_runner import TransformRunner


@pytest.fixture()
def warehouse(tmp_path: Path) -> str:
    db = str(tmp_path / "wh.db")
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE orders (id INTEGER, email TEXT)")
    con.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(1, "a@example.com"), (2, None), (3, "c@example.com")],
    )
    con.commit()
    con.close()
    return db


def _model(**kw) -> TransformModel:
    return TransformModel(
        name="stg_orders",
        sql="SELECT id, email FROM {{ source('orders') }}",
        materialization="table",
        **kw,
    )


def test_failing_not_null_persists_dlq_and_accounting(warehouse, tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    runner = TransformRunner(
        {"type": "sqlite", "database": warehouse},
        dialect="sqlite",
        project_id="fixture-not-null",
        workspace_id="ws-fixture",
    )
    result = runner.run(
        [_model(tests=[DataTest(test_type="not_null", column="email")])]
    )
    payload = result.to_dict()
    ledger = payload["row_accounting"]
    assert result.status == "partial"
    assert ledger["tests_failed"] == 1
    assert ledger["rows_quarantined"] == 1
    assert ledger["models_run"] == 1
    events = dlq.list_dlq_events(job_id="xform-fixture-not-null")
    assert events
    assert events[0]["details"]["source"] == "transform"
    finding = events[0]["details"]["rejected_details"][0]
    assert finding["job_id"] == "xform-fixture-not-null"
    assert "not_null" in str(finding.get("expected_type") or finding.get("failure_reason") or "")


def test_warn_severity_does_not_persist_dlq(warehouse, tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    runner = TransformRunner(
        {"type": "sqlite", "database": warehouse},
        dialect="sqlite",
        project_id="fixture-warn",
    )
    result = runner.run(
        [_model(tests=[DataTest(test_type="not_null", column="email", severity="warn")])]
    )
    assert result.status == "success"
    assert result.row_accounting()["rows_quarantined"] == 0
    assert dlq.list_dlq_events(job_id="xform-fixture-warn") == []


def test_no_project_id_does_not_persist_undurable_quarantine(warehouse, tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
    result = runner.run(
        [_model(tests=[DataTest(test_type="not_null", column="email")])]
    )
    assert result.status == "partial"
    assert result.row_accounting()["rows_quarantined"] == 1
    assert dlq.list_dlq_events(job_id="xform-") == []
