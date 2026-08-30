"""Live 100K-row scale harnesses (SQL duplex, files/object stores, NoSQL, sync modes).

Env-gated: nothing here executes unless ``DATAFLOW_SCALE_MATRIX=1`` and the
engines are actually listening, so a missing service is a reported skip and
never an invented green. See ``docs/SCALE_MATRIX_SQL.md`` and the per-track
evidence documents.
"""
