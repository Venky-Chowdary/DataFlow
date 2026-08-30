"""Live scale harnesses — measured >=100K-row proof on running engines.

Every module here talks to a *running* engine and refuses to invent a green: an
engine that is not listening is reported ``skip`` with the probe error, and
destination proof is always an independent driver COUNT plus a content
checksum, never the writer's acknowledgement.

Env-gated so CI skips when the fleet is absent:

* ``DATAFLOW_SCALE_MATRIX=1`` — SQL duplex matrix (Track A).
* ``DATAFLOW_SCALE_NOSQL=1`` — NoSQL / analytical matrix (Track C).
* ``DATAFLOW_SCALE_ROWS`` — row count per route (default 100000).

See ``docs/SCALE_MATRIX_SQL.md`` and the per-track evidence documents.
"""
