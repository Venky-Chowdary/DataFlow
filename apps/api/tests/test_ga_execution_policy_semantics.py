"""Enterprise GA — distinct execution policy write semantics."""

from __future__ import annotations

import sys
from pathlib import Path

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))


def _contract(policy: str, **kwargs):
    from services.migration_risk_contract import create_migration_risk_contract

    return create_migration_risk_contract(
        column=kwargs.get("column", "amt"),
        source_type="TEXT",
        destination_type="INTEGER",
        approved_by="ops@dataflow.app",
        reason=f"policy={policy}",
        execution_policy=policy,
        transform=kwargs.get("transform"),
    )


def test_every_policy_has_distinct_write_action():
    from services.migration_risk_contract import (
        ALL_POLICIES,
        execution_policy_semantics,
        resolve_write_action_for_mapping,
    )

    semantics = execution_policy_semantics()
    assert set(semantics) == set(ALL_POLICIES)
    actions = {}
    for pol in sorted(ALL_POLICIES):
        c = _contract(pol)
        action, got, _rid = resolve_write_action_for_mapping(
            {"source": "amt", "risk_contract": c.to_dict()},
            "quarantine",
        )
        assert got == pol
        actions[pol] = action
        assert action == semantics[pol]["write_action"] or (
            # CAST/TRANSFORM may coerce_null when quarantine_policy asks — default quarantine
            pol in {"CAST_AND_CONTINUE", "TRANSFORM_AND_CONTINUE"}
            and action == "quarantine"
        )
    # Distinct abort family vs continue family
    assert actions["FAIL_JOB"] == "fail"
    assert actions["STOP_TABLE"] == "stop_table"
    assert actions["STOP_COLUMN"] == "stop_column"
    assert actions["ABORT_TRANSACTION"] == "abort_transaction"
    assert actions["RETRY"] == "retry_then_fail"
    assert actions["SKIP_ROW"] == "skip_row"
    assert actions["QUARANTINE_ROW"] == "quarantine"
    assert actions["FAIL_JOB"] != actions["STOP_TABLE"]
    assert actions["SKIP_ROW"] != actions["QUARANTINE_ROW"]
    assert actions["STOP_COLUMN"] != actions["FAIL_JOB"]


def test_stop_column_writes_row_omitting_bad_cell():
    from connectors.writer_common import build_mapped_rows_with_details

    c = _contract("STOP_COLUMN")
    mappings = [
        {
            "source": "id",
            "target": "id",
            "transform": "none",
        },
        {
            "source": "amt",
            "target": "amt",
            "transform": "integer",
            "risk_contract": c.to_dict(),
            "risk_acknowledged": True,
        },
    ]
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["id", "amt"],
        data_rows=[["1", "not-a-number"], ["2", "10"]],
        mappings=mappings,
        target_cols=["id", "amt"],
        column_types={"id": "INTEGER", "amt": "TEXT"},
        error_policy="quarantine",
    )
    # Row 1: amt fails STOP_COLUMN — row still written with amt omitted/None
    # Row 2: both ok
    assert len(mapped) == 2
    assert any(d.get("execution_policy") == "STOP_COLUMN" for d in details)
    assert any(d.get("disposition") == "column_stopped" for d in details)
    assert any(d.get("stop_scope") == "column" for d in details)


def test_skip_row_drops_row_with_skipped_disposition():
    from connectors.writer_common import build_mapped_rows_with_details

    c = _contract("SKIP_ROW")
    mappings = [
        {
            "source": "amt",
            "target": "amt",
            "transform": "integer",
            "risk_contract": c.to_dict(),
            "risk_acknowledged": True,
        },
    ]
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["amt"],
        data_rows=[["bad"], ["3"]],
        mappings=mappings,
        target_cols=["amt"],
        column_types={"amt": "TEXT"},
        error_policy="quarantine",
    )
    assert len(mapped) == 1
    assert details[0]["execution_policy"] == "SKIP_ROW"
    assert details[0]["disposition"] == "skipped"
    assert details[0].get("quarantine_required") is False


def test_quarantine_row_holds_out_with_quarantine_required():
    from connectors.writer_common import build_mapped_rows_with_details

    c = _contract("QUARANTINE_ROW")
    mappings = [
        {
            "source": "amt",
            "target": "amt",
            "transform": "integer",
            "risk_contract": c.to_dict(),
            "risk_acknowledged": True,
        },
    ]
    mapped, _errs, details = build_mapped_rows_with_details(
        headers=["amt"],
        data_rows=[["bad"], ["3"]],
        mappings=mappings,
        target_cols=["amt"],
        column_types={"amt": "TEXT"},
        error_policy="quarantine",
    )
    assert len(mapped) == 1
    assert details[0]["disposition"] == "quarantined"
    assert details[0].get("quarantine_required") is True


def test_fail_job_aborts_partial_write():
    from connectors.writer_common import reject_on_strict_policy

    msg = reject_on_strict_policy(
        "quarantine",
        [{"row": 1, "execution_policy": "FAIL_JOB", "policy": "fail", "stop_scope": "job"}],
        "writer",
    )
    assert msg is not None
    assert "abort" in msg.lower() or "blocks" in msg.lower()


def test_stop_table_aborts_with_table_scope():
    from connectors.writer_common import reject_on_strict_policy

    msg = reject_on_strict_policy(
        "quarantine",
        [
            {
                "row": 1,
                "execution_policy": "STOP_TABLE",
                "policy": "stop_table",
                "stop_scope": "table",
            }
        ],
        "writer",
    )
    assert msg is not None
    assert "STOP_TABLE" in msg or "table" in msg.lower() or "abort" in msg.lower()
