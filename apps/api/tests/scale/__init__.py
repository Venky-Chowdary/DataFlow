"""Scale harness — measured >=100K-row proof on live engines.

Every module here talks to a *running* engine and refuses to invent a green:
an engine that is not listening is reported ``skip`` with the probe error, and
destination proof is always an independent driver COUNT plus a content
checksum, never the writer's acknowledgement.

Env-gated so CI skips when the fleet is absent:

* ``DATAFLOW_SCALE_NOSQL=1`` — run the NoSQL / analytical matrix (Track C).
* ``DATAFLOW_SCALE_ROWS`` — row count per route (default 100000).
"""
