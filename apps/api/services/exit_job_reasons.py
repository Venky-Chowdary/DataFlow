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
#: The engine's read-back of the file it wrote (``src.transfer.reconcile_step``): "row N column a→b: ...".
_READBACK = "Read-back verification failed on the written file"
_READBACK_COLUMN = re.compile(r"\bcolumn ([^→:'\"]{1,64})→")
#: A declared type as the engine spells it: letters, an optional (n) / (p, s), an optional [].
_TYPE = r"[A-Za-z][A-Za-z0-9_ ]{0,39}(?:\(\s*\d+(?:\s*,\s*\d+)?\s*\))?(?:\[\])?"
#: The fidelity gate's own sentence: "N column(s) collapse fidelity on write (c CHAR(5) → VARCHAR, ...)".
_COLLAPSE = re.compile(r"collapse fidelity on write \((.{1,600}?)\) — ")
_COLLAPSE_ITEM = re.compile(rf"^([A-Za-z_][\w.$#-]{{0,63}}) ({_TYPE}) → ({_TYPE})$")
#: Whole error texts the engine writes with no value in them.
_FIXED = frozenset({"Source table is empty", "The scheduler did not accept the extract job."})


def _columns(job: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for detail in job.get("rejected_details") or []:
        name = str((detail or {}).get("column") or "").strip() if isinstance(detail, dict) else ""
        if name and _COLUMN.match(name) and name not in names:
            names.append(name)
    return names[:5]


def _schema_names(job: dict[str, Any], schema: Any = ()) -> set[str]:
    """The table's column names (``schema``, its mappings and declared types): only these are ever repeated."""
    request = job.get("transfer_request") if isinstance(job.get("transfer_request"), dict) else {}
    names = {str(c) for c in schema or ()}
    names.update(str(c) for c in (request.get("column_types") or {}) if isinstance(request.get("column_types"), dict))
    for m in request.get("mappings") or []:
        if isinstance(m, dict):
            names.update(str(m.get(k) or "") for k in ("source", "target"))
    return {n for n in names if n and _COLUMN.match(n)}


def _readback_finding(error: str, schema: set[str]) -> str:
    """"the read-back of the written file failed on column(s) a, b": column names are schema, values are not.

    A name is repeated only when it is one of the job's own columns, so text
    inside a quoted value that happens to read like "column x→" never is.
    """
    if _READBACK not in error:
        return ""
    names: list[str] = []
    for found in _READBACK_COLUMN.findall(error):
        name = found.strip()
        if name in schema and name not in names:
            names.append(name)
    if "written file could not be re-read" in error and not names:
        return "the file it wrote could not be read back"
    if not names:
        return "the read-back of the file it wrote did not match what was sent"
    return ("the read-back of the file it wrote failed on column" + ("s " if len(names) > 1 else " ")
            + ", ".join(names[:5]) + ": what was read back is not what was sent")


def _collapse_columns(error: str, schema: set[str]) -> str:
    """" on column c (CHAR(5) to VARCHAR)" from the fidelity gate's sentence: names and types only, or ""."""
    found = _COLLAPSE.search(error)
    if not found:
        return ""
    items = []
    for part in re.split(r",\s+|;\s+", found.group(1)):
        item = _COLLAPSE_ITEM.match(part.strip())
        if item and item.group(1) in schema:
            items.append(f"{item.group(1)} ({item.group(2).strip()} to {item.group(3).strip()})")
    if not items:
        return ""
    return (" on column" + ("s " if len(items) > 1 else " ") + ", ".join(items[:5])
            + (f" (+{len(items) - 5} more)" if len(items) > 5 else "")
            + ": a file export holds that type as plain text, so the extract would not keep it exactly")


def engine_finding(job: dict[str, Any], schema: Any = ()) -> str:
    """The job's failure from the engine's structured findings (allowlisted), or "".

    ``schema`` is the table's column names: a column is named only when it is one of them.
    """
    error = str(job.get("error") or job.get("message") or "").strip()
    if error in _FIXED:
        return error.rstrip(".").lower()
    schema = _schema_names(job, schema)
    readback = _readback_finding(error, schema)
    if readback:
        return readback
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
            kind = root.group(1).replace("-", " ")
            # The fidelity gate names the column and both types (schema, never a value): say them plainly.
            where = _collapse_columns(error, schema) if kind == "fidelity collapse" else ""
            gates.append(f"{kind}{where} (gate {gate})")
        elif _HARD_GATE.match(gate):
            gates.append(f"gate {gate}")
    gates = list(dict.fromkeys(gates))
    if gates:
        return "preflight blocked it: " + "; ".join(gates[:4])
    if str(details.get("reason") or "") == "Preflight blocked transfer":
        return "preflight blocked it (open the job for the blocking checks)"
    return ""


def failed_reason(job: dict[str, Any], job_id: str, *, schema: Any = ()) -> str:
    """Why the extract job ``job_id`` failed: its finding when allowlisted, always its id and a reference."""
    from services.parity_errors import safe_error

    status = str(job.get("status") or "without a status")
    error = job.get("error") or job.get("message")
    if not error:
        return f"The extract job {job_id} ended {status}; no error was recorded."
    what = f"The extract job {job_id}" if status.lower() == "failed" else f"The extract job {job_id} ({status})"
    return safe_error(error, what=what, event=ERROR_EVENT, limit=600, reference=True,
                      finding=engine_finding(job, schema))
