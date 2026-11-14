"""Everything one tracked transfer job holds for its whole run.

Split out of ``engine.py`` (a module at its size budget). ``execute_tracked``
enters this once; every scope here is left on every outcome of the job
(completed, failed, cancelled, raised):

* the workspace's connector egress policy;
* one SFTP login per endpoint for the whole job (D189): listing, stat, spill,
  preview and probes share it (database sessions stay the job's);
* the job's ``hash_pii`` key version;
* the keyed write witness (``services.keyed_write_witness``): the ladder
  normally takes it, and a run that fails before the ladder must not keep a
  ladder budget of digests in memory after the job;
* the destination-table write lock (``services.destination_write_lock``),
  taken once the connectors are resolved and held until the job ends;
* the export-bucket binding (``services.export_object_store``): the workspace
  a file export's object key is scoped to, and the job's staged local copy of
  that export (read back by Gate-8), deleted when the job ends.

:func:`start_refusal` is the other thing every tracked run passes through: the
reasons a resolved job may not write at all (data residency, local-disk policy).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any

logger = logging.getLogger(__name__)


def start_refusal(request: Any) -> str | None:
    """Why this run may not write to its destination, or ``None``.

    ``execute_tracked`` asks once, on the resolved request, before any write:

    * data residency (security review H2/H3). Job creation already refuses an
      out-of-region destination, but a job is created once and run many times:
      a resume, a retry of the stored request, a worker that claims the job
      later, the orphan sweep. Each run is judged again here, on the address
      it will dial now, so a connector moved to another region after the job
      was created does not carry the earlier answer.
    * the local-disk policy for file destinations (D276).
    * a multi-stream run into a file export, which would write only the first
      stream (``services.multi_stream_targets.file_export_refusal``).
    """
    from services.data_residency import residency_refusal
    from services.local_file_policy import local_destination_refusal
    from services.multi_stream_targets import file_export_refusal

    workspace_id = str(getattr(request, "workspace_id", "") or "")
    out_of_region = residency_refusal(workspace_id, request.destination)
    if out_of_region:
        logger.warning(
            "Run refused before any write: destination outside the data region of workspace %s",
            workspace_id or "(none)",
        )
        return out_of_region
    return local_destination_refusal(request.destination) or file_export_refusal(request)


@contextmanager
def tracked_job_scope(request: Any, job_id: str, *, store: Callable[[], Any]) -> Iterator[None]:
    from services.egress_guard import egress_workspace
    from services.endpoint_session import endpoint_session_scope
    from services.export_object_store import export_job_scope
    from services.keyed_write_witness import keyed_write_witness_scope
    from services.pii_hash_keys import transfer_job_scope
    from services.run_clock import run_clock_scope

    from services.destination_write_lock import release_destination_write_lock
    from services.naming_history import naming_job_scope

    try:
        with naming_job_scope(job_id, str(request.workspace_id or "")), \
                keyed_write_witness_scope(job_id), egress_workspace(
            str(request.workspace_id or "")
        ), endpoint_session_scope(engines=("sftp",)), transfer_job_scope(
            request, job_id, store=store
        ), export_job_scope(job_id, str(request.workspace_id or "")), run_clock_scope(
            job_id, store=store
        ):
            yield
    finally:
        release_destination_write_lock(job_id)
        # A CALL/SELECT source's spool, including one a Gate-8 / proof re-read
        # opened after the stream closed its own, never outlives the job.
        from services.procedure_source import close_callable_spool

        close_callable_spool(job_id=job_id)
