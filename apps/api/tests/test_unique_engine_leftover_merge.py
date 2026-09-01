"""Leftover MERGE on every unique engine the desktop cartesian certified.

Overwrite cartesian 225/225 proves dest COUNT after a 2-row write. Fivetran
historical re-sync soft-flags leftovers (``_fivetran_deleted``) so COUNT(*)
does not drop. Airbyte incremental refuses inferred deletes. This fixture is
the dest-engine identity on the same 15 unique engines:

    dest {1,2,3,99} vs S {1,2,3} → DELETE 99
    dest COUNT 4→3 (native COUNT, never catalog stats)
    incremental leftover MERGE is a hard no-op

``100%`` here is every reachable unique engine in
``LIVE_UNIQUE_ENGINES``. Closed ports skip. Kafka is not a unique engine
here — leftover MERGE is a PK anti-join, not log compaction.
Emulators are not a customer tenant. CDC remains at-least-once upsert.
"""

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path

import pytest

from services.desktop_lab_cross import LIVE_UNIQUE_ENGINES, _cfg, bind_live_engine
from services.dest_precount import (
    EXTRA_KEYS_KEY,
    MISSING_KEYS_KEY,
    destination_keyset_census,
    destination_row_count,
)
from services.row_conservation import apply_inferred_leftover_deletes
from src.transfer.adapters import write_destination_database

MAPPINGS = [
    {"source": "id", "target": "id", "transform": "direct"},
    {"source": "v", "target": "v", "transform": "direct"},
]
GHOST_ROWS = [["1", "a"], ["2", "b"], ["3", "c"], ["99", "ghost"]]
SOURCE_KEYS = [("1",), ("2",), ("3",)]


def _assert_leftover_merge(db_type: str, cfg: dict, *, schema: str, table: str) -> None:
    before = destination_row_count(db_type, cfg, schema=schema, table_name=table)
    assert before == 4, f"{db_type} dest COUNT before leftover MERGE: {before}"
    census_before = destination_keyset_census(
        db_type, cfg, schema=schema, table_name=table, key_columns=["id"], keys=SOURCE_KEYS
    )
    assert census_before is not None, f"{db_type} dest keyset census unmeasured"
    assert census_before["dest_count"] == 4
    assert census_before[EXTRA_KEYS_KEY] == 1
    assert census_before[MISSING_KEYS_KEY] == 0

    refused = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=False,
    )
    assert refused is None
    assert destination_row_count(db_type, cfg, schema=schema, table_name=table) == 4

    deleted = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=True,
    )
    assert deleted == 1, f"{db_type} leftover MERGE deleted {deleted}, expected 1"
    after = destination_keyset_census(
        db_type, cfg, schema=schema, table_name=table, key_columns=["id"], keys=SOURCE_KEYS
    )
    assert after is not None
    assert after["dest_count"] == 3
    assert after[EXTRA_KEYS_KEY] == 0
    assert after[MISSING_KEYS_KEY] == 0
    assert destination_row_count(db_type, cfg, schema=schema, table_name=table) == 3

    second = apply_inferred_leftover_deletes(
        db_type=db_type,
        cfg=cfg,
        schema=schema,
        table_name=table,
        key_columns=["id"],
        keys=SOURCE_KEYS,
        complete_snapshot=True,
    )
    assert second == 0


def _write_ghost(ep) -> None:
    records = [{"id": row[0], "v": row[1]} for row in GHOST_ROWS]
    written, _ddl, summary = write_destination_database(
        ep,
        records,
        ["id", "v"],
        {"id": "INTEGER", "v": "STRING"},
        MAPPINGS,
        write_mode="insert",
        conflict_columns=["id"],
    )
    err = ""
    if isinstance(summary, dict):
        err = str(summary.get("error") or "")
    assert int(written or 0) == 4, f"{ep.format} wrote {written}: {err}"


@pytest.mark.parametrize("engine", LIVE_UNIQUE_ENGINES)
def test_unique_engine_leftover_merge_4_to_3(engine: str) -> None:
    table = f"lo{uuid.uuid4().hex[:10]}"
    with tempfile.TemporaryDirectory(prefix="df-leftover-") as tmp:
        bound = bind_live_engine(engine, table, Path(tmp))
        if isinstance(bound, str):
            pytest.skip(bound)
        _write_ghost(bound)
        cfg = _cfg(bound)
        _assert_leftover_merge(
            bound.format,
            cfg,
            schema=bound.schema or "",
            table=bound.table,
        )


def test_redis_non_json_key_refuses_key_list() -> None:
    """A raw string under the prefix is not a PK tuple — leftover MERGE stays unapplied."""
    from connectors.redis_reader import _redis_client, redis_key_for
    from services.dest_precount import destination_key_list

    bound = bind_live_engine("redis", f"nj{uuid.uuid4().hex[:8]}", Path("/tmp"))
    if isinstance(bound, str):
        pytest.skip(bound)
    cfg = _cfg(bound)
    client = _redis_client(cfg)
    client.set(redis_key_for(bound.table, "1"), "not-json")
    listed = destination_key_list(
        "redis", cfg, schema="", table_name=bound.table, key_columns=["id"]
    )
    assert listed is None
    client.delete(redis_key_for(bound.table, "1"))
