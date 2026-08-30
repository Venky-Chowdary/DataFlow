"""Scale harnesses — measured >=100K-row proofs on live engines, one per track.

Every module here talks to a *running* engine and refuses to invent a green:
an engine that is not listening is reported ``skip`` with the probe error, and
destination proof is always an independent driver COUNT plus a content
checksum, never the writer's acknowledgement.

The relational duplex matrix (PostgreSQL / MySQL / SQL Server / SQLite / Oracle
/ DuckDB, each as source **and** destination) lives in ``matrix.py`` and
``run_matrix.py``; see ``docs/SCALE_MATRIX_SQL.md`` for the measured cells.

Env-gated so CI skips when the fleet is absent:

* ``DATAFLOW_SCALE_MATRIX=1`` — relational duplex matrix (Track A).
* ``DATAFLOW_SCALE_NOSQL=1`` — NoSQL / analytical matrix (Track C).
* ``DATAFLOW_SCALE_FILE_MATRIX=1`` — file-format / object-store matrix.
* ``DATAFLOW_SCALE_ROWS`` — row count per route (default 100000).
* ``DATAFLOW_SCALE_HOST`` / ``DATAFLOW_SCALE_FIXTURE_DIR`` — engine host and
  where generated fixtures are written.
"""
