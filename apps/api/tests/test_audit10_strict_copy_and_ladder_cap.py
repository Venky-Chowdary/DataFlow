"""Audit finding 10 (H8, small part) — strict same-engine copies and the ladder's row cut-off.

* A same-engine copy runs inside the engines, so the route's own proof is a
  row count. Under ``validation_mode="strict"`` the run passed on that count
  with ``assurance_level: none`` although the verification ladder had re-read
  and digested both populations. Strict now takes that digest as the verdict.
* The in-memory ladder stops at 250,000 rows. Past it the layers that did not
  run were simply absent; the cut-off is now a ``coverage_limit`` on the
  report, a qualifier on the verdict and a sentence in the message.

The in-engine pushdown digest that would lift the limit is a later change.
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
from services.verification_ladder import (  # noqa: E402
    MAX_LADDER_ROWS,
    attach_ladder_to_reconcile_report,
    ladder_coverage_limit,
    run_five_layer_verification,
)
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402
from src.transfer.reconcile_step import _ladder_declined  # noqa: E402


@pytest.fixture()
def isolated(tmp_path: Path, monkeypatch):
    import services.sync_cursor as sc
    from tests.test_property2_golden_path_never_blocked import _FakeMongo

    class _Fake(_FakeMongo):
        def update_job_fields(self, job_id, fields=None, **kw):
            self.jobs.setdefault(job_id, {}).update({**(fields or {}), **kw})
            return True

    fake = _Fake()
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: fake)
    monkeypatch.setattr(sc, "STORE_PATH", tmp_path / "sync_cursors.json")
    monkeypatch.setattr(sc, "_mongo_cursors", lambda: None)
    monkeypatch.delenv("DATAFLOW_ENGINE_DIGEST", raising=False)
    monkeypatch.delenv("DATAWRAP_ENGINE_DIGEST", raising=False)
    return fake


def _copy(fake, tmp_path: Path, validation_mode: str):
    src, dst = tmp_path / f"s_{validation_mode}.db", tmp_path / f"d_{validation_mode}.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, amt NUMERIC, ts TEXT)")
    conn.executemany(
        "INSERT INTO t VALUES (?, ?, ?, ?)",
        [(i, f"n{i}", i * 1.5, "2024-01-01 00:00:00") for i in range(1, 101)],
    )
    conn.commit()
    conn.close()
    job_id = "a10" + uuid.uuid4().hex[:12]
    fake.update_job_status(job_id, "pending", transfer_request={})
    request = TransferRequest(
        source=EndpointConfig(kind="database", format="sqlite", database=str(src), table="t"),
        destination=EndpointConfig(kind="database", format="sqlite", database=str(dst), table="t"),
        sync_mode="full_refresh_overwrite",
        skip_preflight=True,
        validation_mode=validation_mode,
    )
    return UniversalTransferEngine().execute_tracked(request, job_id)


def test_strict_same_engine_copy_is_not_passed_on_a_count_alone(tmp_path, isolated):
    result = _copy(isolated, tmp_path, "strict")

    assert result.success, result.error
    recon = result.reconciliation or {}
    # The route really was a count-only engine copy...
    assert "engine" in str(recon.get("message") or "").lower()
    # ...and strict did not settle for the count.
    assert recon["passed"] is True
    assert recon["assurance_level"] == "full_checksum"
    assert recon["checksum_match"] is True
    assert recon["source_checksum"] == recon["target_checksum"] != ""
    assert recon["verification_verdict"] == "verified"
    assert recon.get("post_write_pending") is False
    assert "full_checksum" in recon["message"]
    assert "DATAFLOW_ENGINE_DIGEST" not in recon["message"]


def test_the_ladder_cut_off_is_a_coverage_limit_of_the_verdict():
    report = {
        "passed": True, "message": "Row fidelity verified.", "assurance_level": "full_checksum",
        "checksum_match": True, "verification_verdict": "verified",
    }

    out = _ladder_declined(report, 300_000, MAX_LADDER_ROWS)

    limit = out["coverage_limit"]
    assert limit == out["verification_ladder"]["coverage_limit"] == ladder_coverage_limit(300_000)
    assert limit["kind"] == "verification_ladder_max_rows"
    assert limit["rows"] == 300_000 and limit["limit"] == MAX_LADDER_ROWS == 250_000
    assert limit["layers_not_run"] == ["L2", "L4", "L5"]
    assert out["verification_verdict_qualifier"] == "coverage_limited"
    assert "Coverage limit: 300,000 rows exceed the 250,000-row" in out["message"]
    assert out["verification_ladder"]["skipped"] is True
    # Stamping twice does not repeat the sentence.
    again = _ladder_declined(out, 300_000, MAX_LADDER_ROWS)
    assert again["message"].count("Coverage limit:") == 1


def test_an_oversized_ladder_run_carries_the_limit_onto_the_report(monkeypatch):
    import services.verification_ladder as ladder_mod

    monkeypatch.setattr(ladder_mod, "MAX_LADDER_ROWS", 5)
    rows = [{"id": i, "v": str(i)} for i in range(10)]
    ladder = run_five_layer_verification(
        source_rows=rows, target_rows=[dict(r) for r in rows], columns=["id", "v"], pk_column="id",
    )
    assert ladder["skipped"] is True and ladder["coverage_limit"]["rows"] == 10

    report = attach_ladder_to_reconcile_report(
        {"passed": True, "message": "Row fidelity verified.", "verification_verdict": "verified"},
        ladder,
    )

    assert report["coverage_limit"]["limit"] == 5
    assert report["verification_verdict_qualifier"] == "coverage_limited"
    assert "Coverage limit: 10 rows exceed the 5-row" in report["message"]


def test_a_run_inside_the_limit_carries_no_coverage_limit():
    rows = [{"id": i, "v": str(i)} for i in range(10)]
    ladder = run_five_layer_verification(
        source_rows=rows, target_rows=[dict(r) for r in rows], columns=["id", "v"], pk_column="id",
    )
    report = attach_ladder_to_reconcile_report({"passed": True, "message": "ok"}, ladder)
    assert "coverage_limit" not in ladder and "coverage_limit" not in report
    assert "verification_verdict_qualifier" not in report
