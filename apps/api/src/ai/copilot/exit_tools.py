"""Pilot / MCP tools for Reversibility Proof: exit readiness, a rehearsal, reversibility.

Read only, and scoped to the caller's workspace like the REST reads they twin
(``GET /exit/readiness``, ``GET /exit/rehearsals/{id}``,
``GET /transfer/plans/{id}/reversibility``). The verdicts they return come from
the deterministic rules in :mod:`services.exit_completeness` and
:mod:`services.reversibility`; the assistant words them and never changes one.
Declaring a system, starting a rehearsal and signing stay REST actions a
person takes on the pages.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .tools import ToolResult

EXIT_TOOL_DEFINITIONS: list[dict] = [
    {
        "name": "get_exit_readiness",
        "description": (
            "Exit readiness of this workspace: the declared systems (what would have to be taken out), "
            "which have a proven exit rehearsal, which rehearsals are overdue, the longest measured exit "
            "against its window and the next regulatory date. Use for 'can we leave this provider', "
            "'which exits are proven', 'what is overdue'."
        ),
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "explain_exit_rehearsal",
        "description": (
            "Explain one exit rehearsal: its verdict, every table's completeness (row counts, key-set "
            "and row digests, control totals, value parity method), the measured duration against the "
            "exit window, the Merkle root of the sealed extract and the sign-off status. Give a "
            "rehearsal id, or a system id for its latest rehearsal."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "rehearsal_id": {"type": "string", "description": "Rehearsal id (exr_...)."},
                "system_id": {"type": "string", "description": "Exit system id (exs_...): its latest rehearsal."},
            },
            "required": [],
        },
    },
    {
        "name": "check_reversibility",
        "description": (
            "Whether a transfer plan's (or a job's) destination can be turned back into the source: the "
            "verdict per table (reversible / partly / not reversible), every lossy column with its reason "
            "(hashed or masked PII, dropped or narrowed columns, rounding), and whether the "
            "'require reversibility' gate blocks it."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "plan_id": {"type": "string", "description": "Transfer plan id."},
                "job_id": {"type": "string", "description": "Job id, when there is no plan."},
            },
            "required": [],
        },
    },
]

NOTE = ("Verdicts come from deterministic checks; Datawrap produces evidence, it does not certify "
        "compliance. Declaring systems, starting rehearsals and signing are done on the Exit readiness page.")


def _result(name: str, *, output: Any = None, error: str = "") -> "ToolResult":
    from .tools import ToolResult

    return ToolResult(name=name, success=not error, output=None if error else output, error=error or None)


def exit_link(system_id: str = "", rehearsal_id: str = "") -> str:
    from urllib.parse import urlencode

    params = {k: v for k, v in (("system", system_id), ("rehearsal", rehearsal_id)) if v}
    return "/#/exit" + (f"?{urlencode(params)}" if params else "")


def get_exit_readiness() -> "ToolResult":
    from services import exit_store
    from services.exit_inventory import board
    from services.workspace_filter import bound_allows

    systems = exit_store.list_docs(exit_store.SYSTEMS, where=lambda d: bound_allows(d.get("workspace_id")))
    out = board(sorted(systems, key=lambda s: str(s.get("name") or "").lower()))
    return _result("get_exit_readiness", output={**out, "open": exit_link(), "note": NOTE})


def _visible(doc: dict[str, Any] | None) -> dict[str, Any] | None:
    from services.workspace_filter import bound_allows

    return doc if doc and bound_allows(doc.get("workspace_id")) else None


def explain_exit_rehearsal(rehearsal_id: str = "", system_id: str = "") -> "ToolResult":
    from services import exit_store
    from services.exit_inventory import rehearsals_of

    name = "explain_exit_rehearsal"
    rehearsal = None
    if rehearsal_id.strip():
        rehearsal = _visible(exit_store.get(exit_store.REHEARSALS, rehearsal_id.strip()))
    elif system_id.strip():
        system = _visible(exit_store.get(exit_store.SYSTEMS, system_id.strip()))
        rows = rehearsals_of(system["id"], limit=1) if system else []
        rehearsal = rows[0] if rows else None
        if system and not rehearsal:
            return _result(name, error=f"{system.get('name')} has never been rehearsed.")
    else:
        return _result(name, error="Which rehearsal? Give a rehearsal id (exr_...) or a system id (exs_...).")
    if not rehearsal:
        return _result(name, error="That rehearsal or system was not found in this workspace.")
    from services.exit_evidence import signoff_block
    from services.exit_rehearsal import _currency, settle

    rehearsal = settle(rehearsal["id"], inline=False, start=False) or rehearsal  # a read tool starts nothing
    duration = rehearsal.get("duration") or {}
    seal = rehearsal.get("seal") or {}
    current, stale_reason = _currency(rehearsal)
    ack = rehearsal.get("pii_acknowledgement") if isinstance(rehearsal.get("pii_acknowledgement"), dict) else None
    return _result(name, output={
        "rehearsal_id": rehearsal["id"], "system": rehearsal.get("system_name"), "n": rehearsal.get("n"),
        "verdict": rehearsal.get("verdict"), "verdict_reason": rehearsal.get("verdict_reason"),
        # Whether the system still has the definition this rehearsal ran against (a stale one proves the past).
        "current": current, "stale": current is False, "stale_reason": stale_reason,
        # Whose PII acknowledgement its extracts ran under (never the acknowledged data).
        "pii_acknowledgement": ({"actor": ack.get("actor"), "at": ack.get("at"), "reason": ack.get("reason")}
                                if ack else None),
        "completeness": (rehearsal.get("completeness") or {}).get("statement"),
        "tables": [
            {k: t.get(k) for k in ("name", "status", "reason", "rows_source", "rows_extracted")}
            | {"value_check": (t.get("value_parity") or {}).get("method")}
            for t in rehearsal.get("tables") or []
        ],
        "duration": duration.get("statement"), "measured_seconds": duration.get("measured_seconds"),
        "window_seconds": duration.get("window_seconds"), "fits_window": duration.get("fits_window"),
        "merkle_root": seal.get("merkle_root"), "parts": seal.get("parts_count"),
        "signoff": signoff_block(rehearsal)["status"],
        "open": exit_link(str(rehearsal.get("system_id") or ""), rehearsal["id"]),
        "note": NOTE,
    })


def check_reversibility(plan_id: str = "", job_id: str = "") -> "ToolResult":
    from services.reversibility_sources import job_report, plan_report

    name = "check_reversibility"
    if plan_id.strip():
        from services.transfer_plan_store import get_plan

        plan = get_plan(plan_id.strip())
        if not plan or not _visible({"workspace_id": plan.workspace_id or None}):
            return _result(name, error=f"Plan '{plan_id}' was not found.")
        report = plan_report(plan)
    elif job_id.strip():
        from .lifecycle_tools import resolve_job

        job, error = resolve_job(job_id.strip(), "")
        if job is None:
            return _result(name, error=error)
        report = job_report(job, job_id.strip())
    else:
        return _result(name, error="Which transfer? Give a plan id or a job id.")
    return _result(name, output={
        "verdict": report["verdict"], "verdict_reason": report["verdict_reason"],
        "gate": report["gate"], "lossy_columns": report["lossy_columns"],
        "tables": [{k: t[k] for k in ("name", "verdict", "reason", "rollback_note")} for t in report["tables"]],
        "data_checked": report["data_checked"],
        "note": ("Reversibility is decided per column by a fixed invertibility table; a conditional column "
                 "is checked against the observed sample only."),
    })
