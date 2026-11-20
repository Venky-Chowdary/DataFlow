"""Disclose a declared sub-microsecond tick the destination cannot hold (D372, R3, R8).

SQL Server's default ``datetime2(7)`` (Snowflake ``TIMESTAMP_NTZ(7)`` reads the
same) counts 100 ns. PostgreSQL ``timestamp(p)`` takes p in 0..6 and MySQL
``DATETIME(fsp)`` fsp in 0..6, so no carrier there holds the 7th digit:

* a 7th digit observed non-zero is lossy, and the coercion gate asks for a
  Risk Contract (``temporal_precision_would_narrow``);
* every row measured and none carries one: nothing is lost, nothing is said;
* otherwise (unmeasured, or a sample that did not show one) it is not a
  blocker, since it is the product's own create-new carrier, but the plan says
  which column could lose the digit.

Validate, the copilot plan and Execute all build their plan through
``run_file_preflight``, which calls :func:`temporal_tick_warnings`; the verdict
and the sentence both come from
:func:`services.decision_kernel.type_invent.temporal_tick_disclosure`, so plan
and execute disclose the same columns.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


def temporal_tick_warnings(
    plan_mappings: Iterable[Any],
    sample_rows: list[Mapping[str, Any]] | None,
    *,
    source_types: Mapping[str, str],
    dest_types: Mapping[str, str] | None = None,
    dest_db: str = "",
    row_count: int | None = None,
    rows_are_population: bool = False,
) -> list[dict[str, Any]]:
    """One plan warning naming every column whose tick may be truncated.

    ``rows_are_population`` with a ``row_count`` the sample covers is the claim
    that the sample is every row (a small table read whole); anything less is
    a sample, and a sample that did not show a 7th digit is not proof.
    """
    from services.decision_kernel.type_invent import temporal_tick_disclosure

    rows = [r for r in (sample_rows or []) if isinstance(r, Mapping)]
    complete = bool(rows_are_population and row_count and len(rows) >= int(row_count))
    live = dict(dest_types or {})
    findings: list[dict[str, str]] = []
    for pm in plan_mappings or []:
        source = str(getattr(pm, "source", "") or "")
        target = str(getattr(pm, "target", "") or "")
        source_type = str(source_types.get(source) or "")
        target_type = str(live.get(target) or getattr(pm, "target_type", "") or "")
        if not source or not source_type or not target_type:
            continue
        message = temporal_tick_disclosure(
            source_type,
            target_type,
            dest_db=dest_db,
            population=[row.get(source) for row in rows] if rows else None,
            population_complete=complete,
            column=source,
        )
        if message:
            findings.append({"column": source, "target": target, "message": message})
    if not findings:
        return []
    return [
        {
            "id": "temporal_tick_unobserved",
            "message": " ".join(f["message"] for f in findings[:6]),
            "details": {"columns": findings},
        }
    ]
