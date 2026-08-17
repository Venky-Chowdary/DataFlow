"""Cycle 6 Enterprise GA — residual fail-closed holes."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def test_integer_to_parametric_decimal_invents_capacity():
    from services.conversion_contract import invents_unproven_capacity

    assert invents_unproven_capacity("INTEGER", "DECIMAL(10,2)", dest_db="postgresql")
    assert invents_unproven_capacity("BIGINT", "NUMBER(38,4)", dest_db="snowflake")
    # Zero-scale NUMBER is Snowflake's integer carrier — widening, not invent.
    assert not invents_unproven_capacity("BIGINT", "NUMBER(38,0)", dest_db="snowflake")


def test_checkpoint_quarantine_delta_persists_and_fail_closes(monkeypatch):
    from src.transfer.engine import _persist_checkpoint_quarantine_delta

    calls: list[list] = []

    def _ok(**kwargs):
        calls.append(list(kwargs.get("rejected_details") or []))
        return {"quarantine_durable": True, "rows": len(calls[-1])}

    monkeypatch.setattr("services.quarantine_dlq.persist_rejected_rows", _ok)
    last = [0]
    _persist_checkpoint_quarantine_delta(
        "job-1",
        {"rejected_details": [{"row": 1}, {"row": 2}]},
        last_persisted=last,
    )
    assert last[0] == 2
    assert len(calls[0]) == 2
    _persist_checkpoint_quarantine_delta(
        "job-1",
        {"rejected_details": [{"row": 1}, {"row": 2}, {"row": 3}]},
        last_persisted=last,
    )
    assert last[0] == 3
    assert calls[1] == [{"row": 3}]


def test_checkpoint_quarantine_delta_raises_on_dlq_failure(monkeypatch):
    from src.transfer.engine import _persist_checkpoint_quarantine_delta

    def _boom(**_kwargs):
        raise RuntimeError("dlq unavailable")

    monkeypatch.setattr("services.quarantine_dlq.persist_rejected_rows", _boom)
    last = [0]
    try:
        _persist_checkpoint_quarantine_delta(
            "job-1",
            {"rejected_details": [{"row": 1}]},
            last_persisted=last,
        )
        raised = False
    except RuntimeError as exc:
        raised = True
        assert "cannot disappear" in str(exc).lower() or "refuse" in str(exc).lower()
    assert raised is True
    assert last[0] == 0


def test_probe_fidelity_collapse_needs_contract(monkeypatch):
    from services.coercion_probe import analyze_coercion
    from services.migration_risk_contract import create_migration_risk_contract

    # Bare boolean ack — must stay block on fidelity collapse.
    report = analyze_coercion(
        sample_rows=[{"amt": "1.5"}],
        mappings=[{
            "source": "amt",
            "target": "amt",
            "source_type": "FLOAT",
            "target_type": "DECIMAL(12,4)",
            "risk_acknowledged": True,
        }],
        source_types={"amt": "FLOAT"},
        dest_types={"amt": "DECIMAL(12,4)"},
        dest_db_type="postgresql",
        validation_mode="strict",
    )
    cols = report.get("columns") or []
    # If probe surfaces the column, severity must not soft-pass on boolean alone.
    for col in cols:
        if col.get("fidelity_collapse"):
            assert col.get("severity") == "block"

    contract = create_migration_risk_contract(
        column="amt",
        source_type="FLOAT",
        destination_type="DECIMAL(12,4)",
        approved_by="admin@dataflow.app",
        reason="IEEE float accepted",
        execution_policy="CAST_AND_CONTINUE",
    ).to_dict()
    report2 = analyze_coercion(
        sample_rows=[{"amt": "1.5"}],
        mappings=[{
            "source": "amt",
            "target": "amt",
            "source_type": "FLOAT",
            "target_type": "DECIMAL(12,4)",
            "risk_acknowledged": True,
            "risk_contract": contract,
        }],
        source_types={"amt": "FLOAT"},
        dest_types={"amt": "DECIMAL(12,4)"},
        dest_db_type="postgresql",
        validation_mode="strict",
    )
    for col in report2.get("columns") or []:
        if col.get("fidelity_collapse"):
            assert col.get("severity") == "warn"
