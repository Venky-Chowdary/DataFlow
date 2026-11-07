"""Load-history comparison for a run (re-exported by src.transfer.engine)."""

from __future__ import annotations

import logging
from .models import TransferRequest
from .models import transfer_request_to_dict
from typing import Any

logger = logging.getLogger("src.transfer.engine")


def _shape_recipe_removes_rows(request: "TransferRequest") -> bool:
    """True when the approved recipe declares a step that legitimately removes
    rows from the destination population: divert_rows / drop / filter, or a
    router whose unmatched policy is divert. Routed tables split the rows, but
    only a divert actually shrinks the surviving population — the load-history
    count/volume-drop signals must not fire on an accounted removal (RT-3)."""
    recipe = getattr(request, "shape_recipe", None)
    if not isinstance(recipe, dict):
        return False
    for step in recipe.get("steps") or []:
        if not isinstance(step, dict) or step.get("enabled") is False:
            continue
        op = str(step.get("op") or "")
        options = step.get("options") or {}
        if op in {"divert_rows", "drop", "filter"}:
            return True
        if op == "router" and str(options.get("unmatched") or "") == "divert":
            return True
    return False


def _compare_and_publish_load_history(
    mongo: Any,
    job_id: str,
    rows: list[dict],
    request: TransferRequest,
    schema: dict[str, str] | None,
    *,
    validation_mode: str,
    row_count_hint: int | None = None,
) -> dict[str, Any]:
    """Compare current sample/batch to last-N route history; publish on the job.

    Streaming paths pass a bounded ``rows`` sample plus ``row_count_hint`` for
    volume drift. Never raises — history must not abort writes.
    """
    load_history_report: dict[str, Any] = {}
    try:
        from services.data_quality_history import compare_route_to_history

        route = transfer_request_to_dict(request)
        load_history_report = compare_route_to_history(
            rows,
            route["source"],
            route["destination"],
            schema=schema,
            current_row_count=row_count_hint,
        )
        quality_anomalies = list(load_history_report.get("anomalies") or [])
        # Count/volume-drop signals are expected, not anomalous, when a
        # divert/drop/filter/router-divert recipe shrinks the population (the
        # ledger already accounts those rows) or the operator acknowledged the
        # volume (D82: the guard had no override). Every other signal
        # (null-rate shift, mean drift, novel quarantine) still blocks.
        volume_acknowledged = bool(
            getattr(request, "load_volume_acknowledged", False)
            or (
                getattr(request, "empty_source_overwrite_acknowledged", False)
                and not row_count_hint
            )
        )
        if quality_anomalies and (
            volume_acknowledged or _shape_recipe_removes_rows(request)
        ):
            kept = [
                a
                for a in quality_anomalies
                if "row count dropped" not in a
                and "is below 50% of prior median" not in a
            ]
            if len(kept) != len(quality_anomalies):
                load_history_report["population_shift_notes"] = [
                    a for a in quality_anomalies if a not in kept
                ]
                load_history_report["anomalies"] = kept
                quality_anomalies = kept
        if quality_anomalies:
            mongo.update_job_status(
                job_id,
                "running",
                phase="quality_check",
                progress_pct=20,
                message="; ".join(quality_anomalies[:5]),
                load_history_report=load_history_report,
            )
            if validation_mode == "strict":
                load_history_report = {
                    **load_history_report,
                    "passed": False,
                    "strict_blocked": True,
                }
        else:
            mongo.update_job_status(
                job_id,
                "running",
                load_history_report=load_history_report,
            )
    except Exception as hist_exc:
        load_history_report = {
            "passed": True,
            "anomalies": [],
            "warning": f"Load-history compare unavailable: {hist_exc!s}"[:240],
            "prior_load_count": 0,
        }
        try:
            mongo.update_job_status(
                job_id,
                "running",
                load_history_report=load_history_report,
                message=load_history_report["warning"],
            )
        except Exception as exc:
            logger.warning("load-history status update failed: %s", exc, exc_info=exc)
    return load_history_report
