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
    temporal_tick_disclosure,
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
        # Unmeasured: the dialect-default tick is the create-new carrier, so
        # not a blocker - but R3 asks that the possible loss be disclosed.
        assert not temporal_precision_would_narrow(
            "TIMESTAMP_NTZ(7)", carrier, dest_db=dest
        )
        assert "a 7th fractional digit was not observed" in temporal_tick_disclosure(
            "TIMESTAMP_NTZ(7)", carrier, dest_db=dest, column="dt"
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


def test_the_tick_disclosure_follows_what_was_measured() -> None:
    """Observed 7th digit: lossy, no note. Every row read, none has one: nothing.
    A sample or no measurement: disclosed, naming the column."""
    sampled_fit = ["2026-01-01 00:00:00.1234560", None]
    for dest, carrier in (("mysql", "DATETIME(6)"), ("postgresql", "TIMESTAMP(6)")):
        seventh = ["2026-01-01 00:00:00.1234567"]
        assert temporal_precision_would_narrow("datetime2(7)", carrier, dest_db=dest, population=seventh)
        assert temporal_tick_disclosure(
            "datetime2(7)", carrier, dest_db=dest, population=seventh, population_complete=True
        ) == ""
        assert temporal_tick_disclosure(
            "datetime2(7)", carrier, dest_db=dest, population=sampled_fit, population_complete=True
        ) == ""
        sampled = temporal_tick_disclosure(
            "datetime2(7)", carrier, dest_db=dest, population=sampled_fit, column="dt"
        )
        assert sampled.startswith(f"dt: datetime2(7) → {carrier} (microseconds): a 7th fractional digit")
        assert sampled.endswith("any such digit would be truncated.")
        # Only NULLs is unmeasured, even when it is every row.
        assert temporal_tick_disclosure(
            "datetime2(7)", carrier, dest_db=dest, population=[None], population_complete=True
        )
    # A destination that holds the digit, or a 6-digit source, says nothing.
    assert temporal_tick_disclosure("datetime2(7)", "DATETIME2(7)", dest_db="sqlserver") == ""
    assert temporal_tick_disclosure("datetime2(6)", "TIMESTAMP(6)", dest_db="postgresql") == ""


def test_plan_and_execute_disclose_through_the_same_preflight() -> None:
    """Validate, the copilot plan and Execute all call run_file_preflight.

    A sample that does not show a 7th digit is disclosed for every target;
    the same rows read whole (rows_are_population, row_count covered) are not.
    """
    from services.preflight_service import run_file_preflight

    rows = [{"id": 1, "dt": "2026-01-01 00:00:00.1234560"}, {"id": 2, "dt": None}]
    mappings = [
        {"source": "id", "target": "id", "confidence": 1.0, "target_type": "INTEGER"},
        {"source": "dt", "target": "dt", "confidence": 1.0, "target_type": "TIMESTAMP(6)"},
    ]

    def warnings(**kw) -> list[dict]:
        result = run_file_preflight(
            columns=["id", "dt"],
            column_types={"id": "int", "dt": "datetime2(7)"},
            mappings=mappings,
            destination_connected=True,
            destination_column_types={"id": "INTEGER", "dt": "TIMESTAMP(6)"},
            destination_db_type="postgresql",
            estimated_bytes=4096,
            sample_rows=rows,
            source_kind="database",
            **kw,
        )
        return [
            w for w in result.get("warnings") or []
            if isinstance(w, dict) and w.get("id") == "temporal_tick_unobserved"
        ]

    sampled = warnings(row_count=5000)
    assert len(sampled) == 1
    assert [c["column"] for c in sampled[0]["details"]["columns"]] == ["dt"]
    assert "dt: datetime2(7) → TIMESTAMP(6) (microseconds)" in sampled[0]["message"]
    assert warnings(row_count=2, rows_are_population=True) == []


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


def test_schedule_save_leaves_an_untyped_identity_column_to_execute() -> None:
    """R8: save refuses what Execute refuses, no more (D388).

    A saved mapping often carries no types. ``id -> id`` with no type is decided
    by Execute from the catalogs, so save must not refuse it; a lossy transform
    needs no type and is still refused, naming only that column.
    """
    assert reversibility_refusal([{"source": "id", "destination": "id"}], table="users") == ""
    message = reversibility_refusal(
        [
            {"source": "id", "destination": "id"},
            {"source": "note", "target": "note", "transform": "omit"},
        ],
        table="users",
    )
    assert "users.note" in message
    assert "users.id" not in message
