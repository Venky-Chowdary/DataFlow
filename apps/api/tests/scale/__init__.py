"""Scale harness: advanced sync modes and the scheduler at >= 100,000 rows.

Env-gated (``DATAFLOW_SCALE_MODES=1``) so a runner without engines skips
honestly instead of reporting a green it never measured. Every destination
number in here comes from an independent driver connection — ``COUNT(*)`` plus
a content checksum over the mapped projection — never from a writer ack.
"""
