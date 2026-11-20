"""D372 and D373: plan and execute use the same fidelity and reversibility rules.

datetime2(7) into a 6-digit carrier is judged on the measured values by the
one shared predicate plan and execute both call: a non-zero 7th digit is a
risk contract on every new and existing PostgreSQL/MySQL target, a 7th digit
of 0 is not a value loss, and the unmeasured SQL Server default tick is the
product's own create-new carrier (no microsecond engine holds 100 ns).
A schedule that requires reversibility is refused at create when a transform
cannot be undone.
"""

from __future__ import annotations

import sys
from pathlib import Path

_API = Path(__file__).resolve().parents[1]
if str(_API) not in sys.path:
    sys.path.insert(0, str(_API))

from services.decision_kernel.type_invent import (  # noqa: E402
    temporal_precision_would_narrow,
)
from services.reversibility_schedule import (  # noqa: E402
    reversibility_refusal,
    schedule_reversibility_block,
)


def test_datetime2_7_into_6_digits_is_flagged_until_values_fit() -> None:
    seventh = ["2026-01-01 00:00:00.1234567", "2026-01-01 00:00:00.0000001"]
    for dest, carrier in (("mysql", "DATETIME(6)"), ("postgresql", "TIMESTAMP(6)")):
        assert temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", carrier, dest_db=dest, population=seventh,
        )
        # The 7th digit is 0: the profiler says this value is not lossy.
        assert not temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", carrier, dest_db=dest,
            population=["2026-01-01 00:00:00.1234560"],
        )
        # Unmeasured: the dialect-default tick is the create-new carrier.
        assert not temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", carrier, dest_db=dest
        )


def test_plan_and_execute_grade_new_and_existing_targets_alike() -> None:
    """AC-1: one verdict for new and existing MySQL / PostgreSQL targets."""
    from services.type_coercion import is_lossy_coercion

    seventh = ["2026-01-01 00:00:00.1234567", "2026-01-01 00:00:00.0000001"]
    for dest, carrier in (("mysql", "DATETIME(6)"), ("postgresql", "TIMESTAMP(6)")):
        verdicts = {
            is_lossy_coercion(
                "datetime2(7)", carrier, dest_db=dest, source_db="sqlserver",
                dest_table_exists=exists, population=seventh,
            )
            for exists in (False, True)
        }
        assert verdicts == {True}, (dest, verdicts)


def test_schedule_reversibility_names_omit_and_mask() -> None:
    message = reversibility_refusal(
        [
            {"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"},
            {"source": "note", "target": "note", "transform": "mask_pii"},
            {"source": "long_txt", "target": "long_txt", "transform": "omit"},
        ],
        table="q3_rev_out",
    )
    assert "must be reversible" in message
    assert "q3_rev_out.note" in message
    assert "q3_rev_out.long_txt" in message
    assert reversibility_refusal(
        [{"source": "id", "target": "id", "source_type": "INTEGER", "target_type": "INTEGER"}],
        table="q3_rev_out",
    ) == ""


def test_rest_and_mcp_update_use_the_same_reversibility_block() -> None:
    payload = {
        "require_reversibility": True,
        "dest_table": "q3_rev_out",
        "mappings": [
            {"source": "note", "target": "note", "transform": "mask_pii"},
        ],
    }
    assert "q3_rev_out.note" in schedule_reversibility_block(payload)
    assert schedule_reversibility_block({**payload, "require_reversibility": False}) == ""
