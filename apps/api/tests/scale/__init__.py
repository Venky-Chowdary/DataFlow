"""Scale harnesses — measured >=100,000-row live proofs, one module per track.

Every module here talks to a *running* engine and refuses to invent a green:
an engine that is not listening is reported ``skip`` with the probe error, and
every destination number comes from an independent driver connection —
``COUNT(*)`` plus a content checksum over the mapped projection — never from the
writer's acknowledgement.

Env-gated so CI skips when the fleet is absent:

* ``DATAFLOW_SCALE_MODES=1`` — advanced sync modes, CDC, scheduler, crash cells.
* ``DATAFLOW_SCALE_NOSQL=1`` — NoSQL / analytical matrix (Track C).
* ``DATAFLOW_SCALE_FILE_MATRIX=1`` — file-format / object-store matrix.
* ``DATAFLOW_SCALE_ROWS`` / ``DATAFLOW_SCALE_CHANGE_ROWS`` — rows per route
  (default 100000) and rows changed per incremental beat.
* ``DATAFLOW_SCALE_JOB_WAIT`` / ``DATAFLOW_SCALE_KILL_WAIT`` — job and
  crash-injection timeouts.
* ``DATAFLOW_SCALE_HOST`` — engine host.
* ``DATAFLOW_SCALE_FIXTURE_DIR`` / ``DATAFLOW_SCALE_ARTIFACTS`` — where
  generated fixtures and measured reports are written.
"""
