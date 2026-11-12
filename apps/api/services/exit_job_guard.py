"""A rehearsal's extract job belongs to its rehearsal: it is never re-run, and never reused (review 3, H1, M1).

An extract request carries the system's PII acknowledgement and a fixed
``exit_<rehearsal>_<n>_<table>`` export name. Rebuilt from the stored job by a
re-run, a retry, a resume, a quarantine replay, Sync now or a Parity re-sync it
would run as an ordinary job under the acknowledging person's name and write
over the sealed part. So:

* every route that rebuilds a stored request refuses a job that has
  ``exit_rehearsal_id`` (:func:`refusal`: "start a new rehearsal instead"), and
  Sync now refuses a request built for a rehearsal (:func:`request_refusal`);
* whatever path still reaches the scheduler, :func:`before_run` runs first on
  the job's worker: a rehearsal's request runs only as that rehearsal's own job
  while the rehearsal is still running, and its acknowledgement is re-checked
  there, when the job *starts* (a withdrawn or lapsed one is taken off the
  request, so the PII gate holds the extract);
* a sealed ``exit_*`` export is created exclusively and never overwritten
  (:func:`sealed_export_name`, used by ``services.file_export_append``).
"""

from __future__ import annotations

import re
from typing import Any

#: ``exit_<rehearsal id>_<table index>_<table>.<format>`` (``services.exit_rehearsal.extract_request``).
_EXPORT = re.compile(r"^exit_(exr_[0-9a-f]{12})_\d{4}_")
_KEY = re.compile(r"^exit-(exr_[0-9a-f]{12})-\d+$")
_TRIGGER = re.compile(r"^exit rehearsal (exr_[0-9a-f]{12})$")


class ExtractRefused(RuntimeError):
    """This request must not run as this job (the message says why and what to do instead)."""


def sealed_export_name(name: str) -> bool:
    """Whether ``name`` is a rehearsal extract's export file (written once, by its own job)."""
    return bool(_EXPORT.match(str(name or "").replace("\\", "/").rsplit("/", 1)[-1]))


def _field(request: Any, name: str) -> Any:
    return request.get(name) if isinstance(request, dict) else getattr(request, name, None)


def rehearsal_of_request(request: Any) -> str:
    """The rehearsal a (stored or live) transfer request was built for, or ``""``."""
    if request is None:
        return ""
    destination = _field(request, "destination")
    path = str(_field(destination, "output_path") or "") if destination is not None else ""
    for pattern, text in ((_EXPORT, path.replace("\\", "/").rsplit("/", 1)[-1]),
                          (_KEY, str(_field(request, "idempotency_key") or "")),
                          (_TRIGGER, str(_field(request, "triggered_by") or ""))):
        found = pattern.match(text)
        if found:
            return found.group(1)
    return ""


def rehearsal_of_job(job: Any) -> str:
    if not isinstance(job, dict):
        return ""
    return str(job.get("exit_rehearsal_id") or "") or rehearsal_of_request(job.get("transfer_request"))


def refusal(job: Any, verb: str = "re-run") -> str | None:
    """Why ``job`` cannot be ``verb`` (it is a rehearsal's extract), or ``None`` for an ordinary job."""
    rid = rehearsal_of_job(job)
    if not rid:
        return None
    return (f"This job is an extract of exit rehearsal {rid}; it cannot be {verb}. Its evidence belongs to that "
            "rehearsal and it ran under the system's PII review. Start a new rehearsal instead.")


def request_refusal(request: Any, verb: str = "run again") -> str | None:
    """Why a stored request cannot be ``verb`` (it was built for a rehearsal), or ``None``."""
    rid = rehearsal_of_request(request)
    if not rid:
        return None
    return (f"This request was built for exit rehearsal {rid}; it cannot be {verb}. "
            "Start a new rehearsal instead.")


def _without_acknowledgement(request: Any, doc: dict[str, Any]) -> None:
    request.compliance_acknowledged = False
    request.acknowledgment_actor = str(doc.get("actor") or "")
    request.acknowledgment_reason = f"Exit rehearsal {doc.get('id')} of {doc.get('system_name')}."


def before_run(job_id: str, request: Any, store: Any) -> None:
    """Called on the job's worker before the engine starts ``request`` as ``job_id``.

    A no-op for an ordinary request and job. Raises :class:`ExtractRefused`
    when a rehearsal's request would run as anything but that rehearsal's own
    job while it is still running; re-checks the PII acknowledgement (M1).
    """
    named = rehearsal_of_request(request)
    job = (store.get_job(job_id) if store is not None else None) or {}
    owner = str(job.get("exit_rehearsal_id") or "")
    if not named and not owner:
        return
    if not owner or (named and named != owner):
        raise ExtractRefused(request_refusal(request, "run as another job") or
                             "This job is not its rehearsal's extract; start a new rehearsal instead.")
    from services import exit_store
    from services.exit_pii import acknowledgement_lapsed

    doc = exit_store.get(exit_store.REHEARSALS, owner)
    own = doc is not None and any(str(t.get("job_id") or "") == job_id for t in doc.get("tables") or [])
    if doc is None or not own or doc.get("verdict") != "running":
        raise ExtractRefused(f"Exit rehearsal {owner} is closed (or this is not one of its extract jobs), so its "
                             "extract cannot run again. Start a new rehearsal instead.")
    if not getattr(request, "compliance_acknowledged", False):
        return
    lapsed = acknowledgement_lapsed(doc)
    if lapsed:
        # The gate decides on what is true when the extract starts, not when the rehearsal was started.
        _without_acknowledgement(request, doc)
        store.update_job_fields(job_id, {"exit_pii_acknowledgement": {"used": False, "lapsed": lapsed}})
        return
    from services.exit_inventory import audit

    ack = doc.get("pii_acknowledgement") or {}
    store.update_job_fields(job_id, {"exit_pii_acknowledgement": {"used": True, "actor": ack.get("actor"),
                                                                 "at": ack.get("at")}})
    audit("exit.rehearsal.extract_acknowledged", actor=str(ack.get("actor") or ""),
          system={"id": doc.get("system_id"), "workspace_id": doc.get("workspace_id")},
          details={"rehearsal_id": owner, "job_id": job_id, "acknowledged_at": ack.get("at"),
                   "table": next((t.get("name") for t in doc.get("tables") or []
                                  if str(t.get("job_id") or "") == job_id), None)})


def refuse_synchronous_run(job_id: str, request: Any) -> str | None:
    """:func:`before_run` for a synchronous execute: why ``request`` must not run as ``job_id``, else ``None``.

    The background worker calls :func:`before_run`; a synchronous execute did
    not, so a job naming a running rehearsal's sealed export could create it
    first and fail the real extract (review 4). The refused job is failed with
    the reason.
    """
    from services.mongodb_service import get_mongodb_service

    store = get_mongodb_service()
    try:
        before_run(job_id, request, store)
    except ExtractRefused as exc:
        store.update_job_status(job_id, "failed", phase="failed", error=str(exc), message=str(exc))
        return str(exc)
    return None
