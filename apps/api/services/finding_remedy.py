"""The fix a parked schedule finding offers matches the error it is attached to (D435).

A schedule parked on "Host not found … Check the spelling of the host name"
told the operator to "Open Validate for this job and resolve the blocking
check": Validate cannot fix a connection's host, credentials or port. The
error taxonomy already knows which failures are the connection's to fix (its
template's next step is ``open_connection``); this module turns that into the
finding's remedy, for findings stored now and for the ones already parked.
"""

from __future__ import annotations

from typing import Any, Mapping

TARGET_CONNECTION = "connection"


def _connection_code(code: str) -> str:
    from services.error_taxonomy import TEMPLATES

    tpl = TEMPLATES.get(str(code or ""))
    return code if tpl is not None and tpl.ui == "open_connection" else ""


def connection_error_code(error_code: str = "", text: str = "") -> str:
    """The taxonomy code when the failure is the connection's to fix, else ``""``."""
    found = _connection_code(error_code)
    if found or error_code:
        return found
    if not str(text or "").strip():
        return ""
    from services.error_taxonomy import classify

    classified = classify(str(text), fallback=False)
    return _connection_code(classified.code) if classified is not None else ""


def connection_corrective(code: str) -> str:
    """One sentence: what to change on the connection, then how to re-run."""
    from services.error_taxonomy import TEMPLATES

    action = TEMPLATES[code].action.rstrip(".") if code in TEMPLATES else "Fix the connection"
    return (
        f"{action}. Open the schedule's connection (Test, or Edit connection) to confirm "
        "it connects, then run the schedule again."
    )


def finding_remedy(request: Mapping[str, Any]) -> dict[str, str]:
    """``{"target", "code", "corrective_action"}`` for a connection finding; ``{}`` otherwise."""
    evidence = request.get("evidence") if isinstance(request.get("evidence"), Mapping) else {}
    code = connection_error_code(
        str((evidence or {}).get("error_code") or ""), str(request.get("finding") or "")
    )
    if not code:
        return {}
    return {"target": TARGET_CONNECTION, "code": code, "corrective_action": connection_corrective(code)}
