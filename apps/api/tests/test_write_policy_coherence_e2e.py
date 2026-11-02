"""The whole job honours one policy: good rows land, contracted bad rows hold out.

The unit-level coherence is asserted in ``test_write_policy_coherence.py``. This
runs the real engine path — parse → map → write → reconcile — against a declared
narrowing carrier, because the production symptom was a *job* failure ("no rows
committed"), not a writer return value.
"""

from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from typing import Any

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))
_SRC = _API_ROOT / "src"
if str(_SRC) not in sys.path:
    sys.path.insert(0, str(_SRC))

import src.transfer.engine as engine_mod  # noqa: E402
from src.transfer.engine import UniversalTransferEngine  # noqa: E402
from src.transfer.models import EndpointConfig, TransferRequest  # noqa: E402

# arr_time carries 12 fractional digits on two rows — DECIMAL(11,8) cannot hold them.
CSV = (
    "id,arr_time\n"
    "1,1.50000000\n"
    "2,12.123456789012\n"
    "3,2.25000000\n"
    "4,9.876543210987\n"
    "5,3.75000000\n"
).encode()


class _FakeMongo:
    def __init__(self) -> None:
        self.jobs: dict[str, dict] = {}

    def get_job(self, job_id: str) -> dict | None:
        return self.jobs.get(job_id)

    def update_job_status(self, job_id: str, status: str, **kwargs) -> bool:
        self.jobs.setdefault(job_id, {})
        self.jobs[job_id].update(kwargs)
        self.jobs[job_id]["status"] = status
        return True

    def update_job_fields(self, job_id: str, fields: dict) -> bool:
        self.jobs.setdefault(job_id, {}).update(fields or {})
        return True


@pytest.fixture(autouse=True)
def _patch_mongodb_service(monkeypatch):
    monkeypatch.setattr(engine_mod, "get_mongodb_service", lambda: _FakeMongo())


def _signed_contract(execution_policy: str) -> dict[str, Any]:
    from services.migration_risk_contract import sign_risk_contract

    body: dict[str, Any] = {
        "risk_id": "fidelity_collapse",
        "severity": "high",
        "root_cause": "fidelity_collapse",
        "column": "arr_time",
        "source_type": "DECIMAL(12,9)",
        "destination_type": "DECIMAL(11,8)",
        "transform": None,
        "rows_sampled": 3,
        "estimated_rows": 5,
        "expected_failure_pct": 0.4,
        "expected_precision_loss": True,
        "expected_truncation": False,
        "expected_nulls": False,
        "execution_policy": execution_policy,
        "quarantine_policy": "DLQ",
        "retry_policy": "NONE",
        "rollback_strategy": "DOCUMENT_ONLY",
        "approved_by": "admin@dataflow.app",
        "approved_at": "2026-08-17T00:00:00Z",
        "reason": "Declared fidelity collapse accepted for this load",
        "target": "arr_time",
    }
    body["signature"] = sign_risk_contract(body)
    return body


def _mappings(contract: dict[str, Any] | None) -> list[dict[str, Any]]:
    mapped: list[dict[str, Any]] = [
        {"source": "id", "target": "id", "target_type": "INTEGER"},
        {
            "source": "arr_time",
            "target": "arr_time",
            "target_type": "DECIMAL(11,8)",
            "source_type": "DECIMAL(12,9)",
            "destination_type": "DECIMAL(11,8)",
        },
    ]
    if contract is not None:
        mapped[1]["risk_contract"] = contract
    return mapped


def _run(tmp: Path, contract: dict[str, Any] | None):
    db_path = tmp / "flights.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE flights (id INTEGER, arr_time DECIMAL(11,8))")
    conn.commit()
    conn.close()

    request = TransferRequest(
        source=EndpointConfig(kind="file", format="csv"),
        destination=EndpointConfig(
            kind="database", format="sqlite", database=str(db_path), table="flights"
        ),
        source_content=CSV,
        source_filename="flights.csv",
        mappings=_mappings(contract),
        sync_mode="incremental_append",
        skip_preflight=True,
        validation_mode="strict",
    )
    result = UniversalTransferEngine().execute_tracked(request, "0" * 24)
    conn = sqlite3.connect(str(db_path))
    landed = conn.execute("SELECT id FROM flights ORDER BY id").fetchall()
    conn.close()
    return result, [r[0] for r in landed]


def test_a_signed_holdout_lands_the_good_rows_under_a_strict_posture(tmp_path: Path) -> None:
    result, landed = _run(tmp_path, _signed_contract("QUARANTINE_ROW"))
    assert result.success is True, result.error
    assert landed == [1, 3, 5], landed
    assert result.records_transferred == 3


def test_the_same_load_without_a_contract_refuses_to_write_a_partial_table(
    tmp_path: Path,
) -> None:
    result, landed = _run(tmp_path, None)
    assert result.success is False
    assert "strict error policy" in (result.error or "").lower()
    assert landed == []
