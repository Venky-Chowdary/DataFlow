"""Scale harnesses — measured >=100,000-row live proofs, one module per track.

Every destination number in here comes from an independent driver connection —
``COUNT(*)`` plus a content checksum over the mapped projection — never from a
writer ack. A runner without engines skips honestly instead of reporting a green
it never measured.

Env-gated:

* ``DATAFLOW_SCALE_MODES=1`` — advanced sync modes, CDC, scheduler, crash cells.
* ``DATAFLOW_SCALE_FILE_MATRIX=1`` — file-format / object-store matrix.
* ``DATAFLOW_SCALE_ROWS`` / ``DATAFLOW_SCALE_CHANGE_ROWS`` — rows per route and
  rows changed per incremental beat.
* ``DATAFLOW_SCALE_JOB_WAIT`` / ``DATAFLOW_SCALE_KILL_WAIT`` — job and
  crash-injection timeouts.
* ``DATAFLOW_SCALE_FIXTURE_DIR`` / ``DATAFLOW_SCALE_ARTIFACTS`` — where
  generated fixtures and measured reports are written.
"""
