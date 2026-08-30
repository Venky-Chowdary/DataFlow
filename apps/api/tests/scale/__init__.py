"""Scale harnesses — 100K-row live proofs, one module per track.

Relational duplex matrix (PostgreSQL / MySQL / SQL Server / SQLite / Oracle /
DuckDB, each as source **and** destination) lives in ``matrix.py`` and
``run_matrix.py``; see ``docs/SCALE_MATRIX_SQL.md`` for the measured cells.

Everything here is env-gated: nothing executes unless ``DATAFLOW_SCALE_MATRIX=1``
and the engines are actually listening.
"""
