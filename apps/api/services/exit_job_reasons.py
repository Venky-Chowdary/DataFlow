"""Why a rehearsal's extract job failed, in words an operator can act on and nothing more.

A child job's error quotes row values (a cell the writer refused, a blocker
that names the first value that did not fit), so it never reaches a reason
as text (contract 20, :func:`services.parity_errors.safe_error`). What can
be said safely is built from the engine's own structured findings: the root
cause kind and gate id preflight blocked on, the writer's counts of refused
cells and the columns they were in, plus the job id and the server log's
reference id (review 2, N8). Column names are schema, not values.
"""

from __future__ import annotations

import re
from typing import Any

ERROR_EVENT = "exit.rehearsal.error"
#: Root-cause gate ids (``services.root_cause_engine._root_id``): ``rc-<kind>-<hex digest>``.
_ROOT_GATE = re.compile(r"^rc-([a-z0-9]+(?:-[a-z0-9]+)*?)-([0-9a-f]{6,})$")
#: Preflight hard gate ids (``g24_reversibility``, ``g3_schema_contract``, ...).
_HARD_GATE = re.compile(r"^g\d+[a-z]?_[a-z0-9_]+$")
#: The writer's refusal sentence (``connectors.writer_common``): counts and fixed words only.
_CELL_REFUSAL = re.compile(
    r"^([a-z_]+) rejected (\d+) cell finding\(s\) across (\d+) row\(s\)"
    r"( on column\(s\) no signed Risk Contract covers)?; "
    r"(strict error policy|Migration Risk Contract abort policy) blocks partial write")
_COLUMN = re.compile(r"^[A-Za-z_][\w .$#-]{0,63}$")
#: Whole error texts the engine writes with no value in them.
_FIXED = frozenset({"Source table is empty", "The scheduler did not accept the extract job."})


def _columns(job: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for detail in job.get("rejected_details") or []:
        name = str((detail or {}).get("column") or "").strip() if isinstance(detail, dict) else ""
        if name and _COLUMN.match(name) and name not in names:
            names.append(name)
    return names[:5]


def engine_finding(job: dict[str, Any]) -> str:
    """The job's failure from the engine's structured findings (allowlisted), or ""."""
    error = str(job.get("error") or job.get("message") or "").strip()
    if error in _FIXED:
        return error.rstrip(".").lower()
    refusal = _CELL_REFUSAL.match(error)
    if refusal:
        cols = _columns(job)
        return refusal.group(0) + (f" (column(s) {', '.join(cols)})" if cols else "")
    details = job.get("error_details") if isinstance(job.get("error_details"), dict) else {}
    gates: list[str] = []
    for item in details.get("guidance") or []:
        gate = str((item or {}).get("gate") or "") if isinstance(item, dict) else ""
        root = _ROOT_GATE.match(gate)
        if root:
            gates.append(f"{root.group(1).replace('-', ' ')} (gate {gate})")
        elif _HARD_GATE.match(gate):
            gates.append(f"gate {gate}")
    gates = list(dict.fromkeys(gates))
    if gates:
        return "preflight blocked it: " + "; ".join(gates[:4])
    if str(details.get("reason") or "") == "Preflight blocked transfer":
        return "preflight blocked it (open the job for the blocking checks)"
    return ""


def failed_reason(job: dict[str, Any], job_id: str) -> str:
    """Why the extract job ``job_id`` failed: its finding when allowlisted, always its id and a reference."""
    from services.parity_errors import safe_error

    status = str(job.get("status") or "without a status")
    error = job.get("error") or job.get("message")
    if not error:
        return f"The extract job {job_id} ended {status}; no error was recorded."
    what = f"The extract job {job_id}" if status.lower() == "failed" else f"The extract job {job_id} ({status})"
    return safe_error(error, what=what, event=ERROR_EVENT, limit=600, reference=True,
                      finding=engine_finding(job))
