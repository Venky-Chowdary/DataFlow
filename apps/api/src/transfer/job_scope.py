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
  taken once the connectors are resolved and held until the job ends.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from typing import Any


@contextmanager
def tracked_job_scope(request: Any, job_id: str, *, store: Callable[[], Any]) -> Iterator[None]:
    from services.egress_guard import egress_workspace
    from services.endpoint_session import endpoint_session_scope
    from services.keyed_write_witness import keyed_write_witness_scope
    from services.pii_hash_keys import transfer_job_scope

    from services.destination_write_lock import release_destination_write_lock

    try:
        with keyed_write_witness_scope(job_id), egress_workspace(
            str(request.workspace_id or "")
        ), endpoint_session_scope(engines=("sftp",)), transfer_job_scope(
            request, job_id, store=store
        ):
            yield
    finally:
        release_destination_write_lock(job_id)
