from __future__ import annotations

import sqlite3
import uuid

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest


class _JobStore:
    def __init__(self):
        self.jobs = {}

    def create_transfer_job(self, payload):
        job_id = str(payload.get("_id") or payload.get("job_id") or uuid.uuid4().hex[:24])
        self.jobs[job_id] = dict(payload)
        return job_id

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def update_job_status(self, job_id, status, **fields):
        self.jobs.setdefault(job_id, {}).update(fields, status=status)
        return True

    def update_job_fields(self, job_id, fields):
        if job_id not in self.jobs:
            return False
        self.jobs[job_id].update(fields)
        return True


def _request(path, mappings):
    connection = sqlite3.connect(path)
    connection.execute(
        "CREATE TABLE amounts (id INTEGER PRIMARY KEY, amount DECIMAL(18,2))"
    )
    connection.commit()
    connection.close()
    return TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(path), table="amounts"
        ),
        mappings=mappings,
        column_types={"id": "INTEGER", "amount": "DECIMAL(38,10)"},
        source_filename="amounts.csv",
        source_content=b"id,amount\n1,123.4567890123\n",
        sync_mode="full_refresh_overwrite",
    )


def _mappings(contract=None):
    amount = {
        "source": "amount",
        "target": "amount",
        "target_type": "DECIMAL(18,2)",
        "transform": "decimal",
        "confidence": 0.99,
    }
    if contract is not None:
        amount.update(risk_acknowledged=True, risk_contract=contract)
    return [
        {"source": "id", "target": "id", "target_type": "INTEGER", "confidence": 1.0},
        amount,
    ]


def test_execute_blocks_decimal_narrowing_at_g3_without_contract(tmp_path, monkeypatch):
    from src.transfer import engine as engine_module

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: _JobStore())
    request = _request(tmp_path / "blocked.db", _mappings())

    result = UniversalTransferEngine().execute_tracked(request, "a" * 24)

    evidence = f"{result.error} {result.validation_plan}"
    assert not result.success
    assert "g3" in evidence.lower()
    assert "amount" in evidence.lower()


def test_refused_before_stream_surfaces_real_reason_not_unboundlocalerror(
    tmp_path, monkeypatch
):
    """G-1i: a run refused before the stream binds dest_summary (here the
    G3 decimal-narrowing refusal on a file upload) must surface its real
    refusal reason — never an UnboundLocalError from the exit-path
    bookkeeping that runs before the stream returns."""
    from src.transfer import engine as engine_module

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: _JobStore())
    request = _request(tmp_path / "refused.db", _mappings())

    result = UniversalTransferEngine().execute_tracked(request, "c" * 24)

    assert not result.success
    evidence = f"{result.error} {result.validation_plan}"
    assert "g3" in evidence.lower()
    assert "unboundlocalerror" not in evidence.lower()
    assert "local variable" not in evidence.lower()


def test_signed_decimal_risk_contract_executes_and_is_persisted(tmp_path, monkeypatch):
    from services.migration_risk_contract import create_migration_risk_contract
    from src.transfer import engine as engine_module

    monkeypatch.setenv("DATAFLOW_JOB_STORE", "memory")
    monkeypatch.setenv("DATAFLOW_DISABLE_OBJECT_STORE", "1")
    store = _JobStore()
    monkeypatch.setattr(engine_module, "get_mongodb_service", lambda: store)
    contract = create_migration_risk_contract(
        column="amount",
        source_type="DECIMAL(38,10)",
        destination_type="DECIMAL(18,2)",
        approved_by="risk-approver@example.com",
        reason="Accept approved decimal scale loss",
        execution_policy="CAST_AND_CONTINUE",
        expected_precision_loss=True,
        transform="decimal",
    ).to_dict()
    job_id = "b" * 24
    request = _request(tmp_path / "approved.db", _mappings(contract))
    request.triggered_by = "risk-approver@example.com"

    result = UniversalTransferEngine().execute_tracked(request, job_id)

    assert result.success, result.error
    accepted = store.jobs[job_id]["accepted_risks"]
    assert len(accepted) == 1
    assert accepted[0]["column"] == "amount"
    assert accepted[0]["approved_by"] == "risk-approver@example.com"
    assert accepted[0]["risk_id"]
