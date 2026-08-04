"""Module 17 — Historical Success Contract.

Charter mapping field ``historical_success`` must be **measured** or explicitly
unmeasured — never invented (no silent 0.99 / greenwash).

Scope today: route-level load history (source→destination ring buffer).
Column-level write success is not invented when only route aggregates exist.
"""

from __future__ import annotations

from typing import Any

HISTORICAL_SUCCESS_CONTRACT_VERSION = "historical_success_contract.v1"
# At least one completed load with rows is required before a rate is published.
MIN_RUNS_FOR_RATE = 1


def unmeasured_historical_success(*, reason: str = "") -> dict[str, Any]:
    """Honest empty posture — success_rate is null, never 0 or 1 invent."""
    return {
        "measured": False,
        "scope": "none",
        "runs_observed": 0,
        "min_runs_required": MIN_RUNS_FOR_RATE,
        "rows_written_total": 0,
        "rows_rejected_total": 0,
        "success_rate": None,
        "coverage": "none",
        "never_invented": True,
        "contract_version": HISTORICAL_SUCCESS_CONTRACT_VERSION,
        "note": reason
        or (
            "No measured load history for this route — historical_success is "
            "unmeasured (never invent a success rate)."
        ),
    }


def measure_from_runs(runs: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Compute route historical success from persisted load-history runs.

    ``success_rate`` = (rows_written - rejected) / rows_written when measured.
    Rejected > written is clamped to 0.0 (never invent >100% success).
    """
    history = [r for r in (runs or []) if isinstance(r, dict)]
    if not history:
        return unmeasured_historical_success()

    rows_written = 0
    rows_rejected = 0
    usable = 0
    for run in history:
        try:
            written = int(run.get("row_count") or 0)
        except (TypeError, ValueError):
            written = 0
        try:
            rejected = int(run.get("rejected_rows") or 0)
        except (TypeError, ValueError):
            rejected = 0
        if written <= 0 and rejected <= 0:
            continue
        usable += 1
        rows_written += max(0, written)
        rows_rejected += max(0, rejected)

    if usable < MIN_RUNS_FOR_RATE or rows_written <= 0:
        return {
            **unmeasured_historical_success(
                reason=(
                    f"Load history has {len(history)} run(s) but insufficient "
                    "row_count to compute a success rate — never invent one."
                )
            ),
            "runs_observed": len(history),
            "scope": "route_load_history",
            "coverage": "insufficient_rows",
        }

    kept = max(0, rows_written - rows_rejected)
    rate = round(kept / rows_written, 6)
    return {
        "measured": True,
        "scope": "route_load_history",
        "runs_observed": usable,
        "min_runs_required": MIN_RUNS_FOR_RATE,
        "rows_written_total": rows_written,
        "rows_rejected_total": rows_rejected,
        "success_rate": rate,
        "coverage": "route_load_history",
        "never_invented": True,
        "contract_version": HISTORICAL_SUCCESS_CONTRACT_VERSION,
        "note": (
            f"Measured from {usable} load(s): "
            f"{kept}/{rows_written} rows kept after quarantine "
            f"(rate={rate:.2%}). Route-scoped — not per-column invent."
        ),
    }


def measure_route_historical_success(
    source: dict[str, Any] | None,
    destination: dict[str, Any] | None,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Load route history and measure — fail open to unmeasured, never invent."""
    if not source or not destination:
        return unmeasured_historical_success(
            reason="Source/destination missing — cannot measure route history.",
        )
    try:
        from services.data_quality_history import load_run_history

        runs = load_run_history(source, destination, limit=limit)
    except Exception as exc:
        return unmeasured_historical_success(
            reason=f"Load history unavailable ({exc}) — historical_success unmeasured.",
        )
    return measure_from_runs(runs)


def stamp_mappings_historical_success(
    mappings: list[dict[str, Any]] | None,
    evidence: dict[str, Any] | None,
) -> list[dict[str, Any]]:
    """Attach route historical success to every mapping without inventing column rates.

    Existing measured column-scoped evidence is preserved when already present
    with measured=True and scope != route.
    """
    evidence = evidence or unmeasured_historical_success()
    out: list[dict[str, Any]] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        existing = m.get("historical_success")
        if (
            isinstance(existing, dict)
            and existing.get("measured") is True
            and str(existing.get("scope") or "") not in {"", "route_load_history", "none"}
        ):
            out.append(m)
            continue
        # Never keep a bare float invent — replace with structured evidence.
        if isinstance(existing, (int, float)):
            out.append({**m, "historical_success": dict(evidence)})
            continue
        out.append({**m, "historical_success": dict(evidence)})
    return out
