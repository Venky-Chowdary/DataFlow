"""Pilot / MCP tool ``explain_parity``: the parity report of one job, in words.

Read-only. It answers from what the job already holds — the stored Gate-8
evidence and Parity Studio decisions — and re-reads no database, so it is as
cheap as ``get_job`` and gated by the same ``job.read``. Approving a fix stays a
REST action a person takes (``POST /api/v1/jobs/{id}/parity-report/fixes/...``).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .tools import ToolResult

PARITY_TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "explain_parity",
        "description": (
            "Explain whether a finished job's destination matches its source: the parity "
            "verdict, how sure it is, every difference with its deterministic cause, the "
            "proposed fixes and sign-off status. Use for 'why did rows differ', 'is every "
            "difference explained', 'can we sign off this migration'."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "job_id": {"type": "string", "description": "Job ID from Jobs / Job Theater."},
                "selector": {
                    "type": "string",
                    "description": "'last' or 'failed' when no job id was given",
                },
            },
            "required": [],
        },
    },
]


def _summary(report: dict[str, Any]) -> dict[str, Any]:
    from services.parity_explain import model_safe_detail

    parity = report["parity"]
    return {
        "job_id": report["job_id"],
        "verdict": report["verdict"],
        "verdict_reason": report["verdict_reason"],
        "rows_source": parity["rows_source"],
        "rows_destination": parity["rows_destination"],
        "rows_differ": parity["rows_differ"],
        "explained": parity["explained"],
        "unexplained": parity["unexplained"],
        "proven_equal_pct": parity["proven_equal_pct"],
        "confidence": report["confidence"]["statement"],
        "tables": [
            {k: t[k] for k in ("name", "status", "check", "unexplained", "proven_equal_pct")}
            for t in report["tables"]
        ],
        "differences": [
            {
                "table": d["table"], "column": d["column"], "kind": d["kind"], "rows": d["rows"],
                "cause": d["cause"]["label"], "explained": d["cause"]["explained"],
                # The assistant is a model: never who accepted a difference, nor their note.
                "detail": model_safe_detail(d["cause"]),
            }
            for d in report["differences"]
        ],
        "fixes": [
            {k: f[k] for k in ("id", "action", "title", "status", "applicable")}
            for f in report["fixes"]
        ],
        "signoff": report["signoff"]["status"],
    }


def explain_parity(job_id: str = "", selector: str = "") -> "ToolResult":
    from services.parity_explain import template_explanation
    from services.parity_report import build_parity_report, public_report

    from .lifecycle_tools import resolve_job
    from .tools import ToolResult

    job, error = resolve_job(job_id, selector)
    if job is None:
        return ToolResult(name="explain_parity", success=False, output=None, error=error)
    report = public_report(build_parity_report(job, reread=False))
    explanation = report.get("explanation") or template_explanation(report)
    return ToolResult(
        name="explain_parity",
        success=True,
        output={
            "summary": _summary(report),
            "explanation": explanation,
            "open": f"/jobs/{report['job_id']}?tab=trust",
            "note": (
                "Verdicts come from the deterministic check and classifier; fixes are applied "
                "only after a person approves them on the Trust Report."
            ),
        },
    )
