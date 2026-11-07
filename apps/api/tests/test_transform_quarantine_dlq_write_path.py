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

    con = sqlite3.connect(warehouse)
    mart_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    nulls = con.execute("SELECT COUNT(*) FROM stg_orders WHERE email IS NULL").fetchone()[0]
    q_count = con.execute("SELECT COUNT(*) FROM stg_orders_df_quarantine").fetchone()[0]
    payload = con.execute("SELECT _df_payload FROM stg_orders_df_quarantine").fetchone()[0]
    con.close()
    assert mart_count == 2, "violating row must leave the mart (dest COUNT hold-out)"
    assert nulls == 0
    assert q_count == 1
    assert ledger["rows_written"] == 2
    assert ledger["rows_quarantined"] == 1
    assert mart_count + ledger["rows_quarantined"] == 3
    assert payload and ("2" in payload or "null" in payload.lower() or "__DF_SQL_NULL__" in payload)


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
    con = sqlite3.connect(warehouse)
    mart_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    nulls = con.execute("SELECT COUNT(*) FROM stg_orders WHERE email IS NULL").fetchone()[0]
    con.close()
    assert mart_count == 3
    assert nulls == 1


def test_no_project_id_does_not_persist_undurable_quarantine(warehouse, tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    runner = TransformRunner({"type": "sqlite", "database": warehouse}, dialect="sqlite")
    result = runner.run(
        [_model(tests=[DataTest(test_type="not_null", column="email")])]
    )
    assert result.status == "partial"
    assert result.row_accounting()["rows_quarantined"] == 0
    assert result.row_accounting()["tests_failed"] == 1
    assert dlq.list_dlq_events(job_id="xform-") == []
    con = sqlite3.connect(warehouse)
    mart_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    nulls = con.execute("SELECT COUNT(*) FROM stg_orders WHERE email IS NULL").fetchone()[0]
    con.close()
    assert mart_count == 3, "fail-closed: no project_id means no DELETE"
    assert nulls == 1
    assert any("project_id" in w for w in result.warnings)


def test_unique_holdout_deletes_every_row_in_duplicate_groups(warehouse, tmp_path, monkeypatch):
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    con = sqlite3.connect(warehouse)
    con.execute("DELETE FROM orders")
    con.executemany(
        "INSERT INTO orders VALUES (?, ?)",
        [(1, "a@example.com"), (2, "a@example.com"), (3, "c@example.com")],
    )
    con.commit()
    con.close()
    runner = TransformRunner(
        {"type": "sqlite", "database": warehouse},
        dialect="sqlite",
        project_id="fixture-unique",
        workspace_id="ws-fixture",
    )
    result = runner.run(
        [_model(tests=[DataTest(test_type="unique", column="email")])]
    )
    ledger = result.row_accounting()
    assert result.status == "partial"
    assert ledger["rows_quarantined"] == 2
    assert ledger["rows_written"] == 1
    con = sqlite3.connect(warehouse)
    mart = con.execute("SELECT id, email FROM stg_orders ORDER BY id").fetchall()
    q_count = con.execute("SELECT COUNT(*) FROM stg_orders_df_quarantine").fetchone()[0]
    remaining_a = con.execute(
        "SELECT COUNT(*) FROM stg_orders WHERE email = ?", ("a@example.com",)
    ).fetchone()[0]
    con.close()
    assert mart == [(3, "c@example.com")]
    assert remaining_a == 0
    assert q_count == 2


def test_dest_dlq_write_failure_does_not_delete_from_mart(warehouse, tmp_path, monkeypatch):
    import services.dest_quarantine as dest_q
    import services.quarantine_dlq as dlq

    monkeypatch.setattr(dlq, "DLQ_PATH", tmp_path / "q.jsonl")
    monkeypatch.setattr(
        dest_q,
        "write_dest_quarantine",
        lambda *a, **k: {"ok": False, "error": "injected dest DLQ failure", "rows_written": 0},
    )
    runner = TransformRunner(
        {"type": "sqlite", "database": warehouse},
        dialect="sqlite",
        project_id="fixture-fail-closed",
        workspace_id="ws-fixture",
    )
    result = runner.run(
        [_model(tests=[DataTest(test_type="not_null", column="email")])]
    )
    assert result.status == "partial"
    assert result.row_accounting()["rows_quarantined"] == 0
    con = sqlite3.connect(warehouse)
    mart_count = con.execute("SELECT COUNT(*) FROM stg_orders").fetchone()[0]
    nulls = con.execute("SELECT COUNT(*) FROM stg_orders WHERE email IS NULL").fetchone()[0]
    con.close()
    assert mart_count == 3
    assert nulls == 1
    assert any("Mart left intact" in w for w in result.warnings)
