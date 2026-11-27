"""QA batch 1 item 13 (the widening cluster): one rule for a widened source.

On a table Datawrap created (run 1), a same-family source widening is a
proposed widen; ``schema_policy=propagate_columns`` applies it with no
acknowledgement (the backlog's D334 acceptance: PG -> MySQL schedules with
propagate_columns keep running after varchar(12) -> varchar(30)). Under the
default manual_review it still needs the acknowledgement (D200), and on a
table somebody else created the live type stays the ceiling (D161) — those
are pinned by tests/test_r4_widening_parity_e2e.py and
tests/test_r13_d200_stored_create_new_mapping_widens.py.

Live: local PostgreSQL -> MariaDB (MySQL wire), through start_transfer.
"""

from __future__ import annotations

import uuid

import pytest

from tests.test_r4_widening_parity_e2e import (  # noqa: F401
    _columns,
    _connector,
    _drop,
    _require,
    _run,
    _sql,
    transfer_client,
)


@pytest.mark.parametrize("mode", ["full_refresh_overwrite", "incremental_upsert"])
def test_propagate_widens_a_datawrap_table_without_an_acknowledgement(transfer_client, tmp_path, mode):
    _require("postgresql")
    _require("mysql")
    table = f"qb13_{uuid.uuid4().hex[:6]}"
    _sql("postgresql", tmp_path, f"CREATE TABLE {table} (id INT PRIMARY KEY, status VARCHAR(12))")
    _sql("postgresql", tmp_path, f"INSERT INTO {table} VALUES (1, 'open'), (2, NULL)")
    args = dict(
        source_connector_id=_connector("postgresql", tmp_path),
        source_table=table,
        dest_connector_id=_connector("mysql", tmp_path),
        dest_table=f"{table}_d",
        sync_mode=mode,
        schema_policy="propagate_columns",
    )
    if mode == "incremental_upsert":
        args.update(upsert_key="id", cursor_column="id", cursor_semantics="monotonic_sequence")
    try:
        _run(transfer_client, **args)  # run 1 creates the destination
        assert _columns("mysql", tmp_path, f"{table}_d")["status"][1] == 12
        _sql("postgresql", tmp_path, f"ALTER TABLE {table} ALTER COLUMN status TYPE VARCHAR(30)")
        _sql("postgresql", tmp_path, f"INSERT INTO {table} VALUES (3, %s)", ("x" * 30,))
        _run(transfer_client, **args)  # no schema_drift_acknowledged
        assert _columns("mysql", tmp_path, f"{table}_d")["status"][1] == 30
        rows = _sql("mysql", tmp_path, f"SELECT id, status FROM {table}_d ORDER BY id", fetch=True)
        assert [tuple(r) for r in rows] == [(1, "open"), (2, None), (3, "x" * 30)]
    finally:
        _drop("mysql", tmp_path, f"{table}_d")
        _sql("postgresql", tmp_path, f"DROP TABLE IF EXISTS {table}")
