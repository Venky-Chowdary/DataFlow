"""SCD2-F1: a renamed column quarantined every row and the run still passed.

An SCD2 transfer mapping ``clv`` -> ``clv_value`` into SQLite quarantined
every row, left the target empty, and reported ``success=True`` with a green
Gate-8. Two causes, both on the real engine path:

(a) The staged merge (database source -> PostgreSQL / MySQL / SQLite /
    warehouses) loads the source into a spool under the *target* names, then
    remapped the spool rows with the source-space Map: ``clv`` was "not found
    in batch headers ['id', 'name', 'clv_value']" for every row. The same
    remap re-applied value transforms (a ``hash_pii`` column was hashed
    twice) and broke ``full_refresh_mirror`` identically.
(b) Nothing judged the population: a merge that held every row out was
    reported as a successful empty load, and on the buffered path an existing
    history table had every current version closed as "deleted at source".

Every route here is the engine's own (``UniversalTransferEngine.execute``).
SQLite and DuckDB run in-process; PostgreSQL and MySQL run when the local
services are configured and skip otherwise.
"""

from __future__ import annotations

import sqlite3
import uuid
from pathlib import Path
from typing import Any

import pytest

from src.transfer.engine import UniversalTransferEngine
from src.transfer.models import EndpointConfig, TransferRequest

ROWS = [(1, "A", 10.5), (2, "B", 20.25), (3, "C", 30.0)]
RENAME_MAP = [
    {"source": "id", "target": "id", "confidence": 1.0, "user_override": True},
    {"source": "name", "target": "name", "confidence": 1.0, "user_override": True},
    {"source": "clv", "target": "clv_value", "confidence": 1.0, "user_override": True},
]
BAD_CLV_MAP = RENAME_MAP[:2] + [{**RENAME_MAP[2], "target_type": "integer"}]


def _sqlite_source(path: Path, rows: list[tuple], clv_type: str = "REAL") -> EndpointConfig:
    with sqlite3.connect(path) as c:
        c.execute("DROP TABLE IF EXISTS cust")
        c.execute(f"CREATE TABLE cust (id INTEGER, name TEXT, clv {clv_type})")
        c.executemany("INSERT INTO cust VALUES (?, ?, ?)", rows)
    return EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{path}",
        database=str(path), table="cust",
    )


def _sqlite_dest(path: Path, table: str = "dim") -> EndpointConfig:
    return EndpointConfig(
        kind="database", format="sqlite", connection_string=f"sqlite:///{path}",
        database=str(path), table=table,
    )


def _run(
    source: EndpointConfig,
    dest: EndpointConfig,
    *,
    sync: str = "scd2",
    mappings: list[dict] = RENAME_MAP,
    validation_mode: str = "balanced",
    **extra: Any,
):
    return UniversalTransferEngine().execute(
        TransferRequest(
            source=source,
            destination=dest,
            sync_mode=sync,
            validation_mode=validation_mode,
            stream_contracts=extra.pop(
                "stream_contracts",
                [{"selected": True, "primary_key": "id", "sync_mode": sync}],
            ),
            mappings=[dict(m) for m in mappings],
            **extra,
        )
    )


def _recon(result) -> dict:
    rec = result.reconciliation
    return rec if isinstance(rec, dict) else {}


def _sqlite_rows(path: Path, sql: str) -> list[tuple]:
    with sqlite3.connect(path) as c:
        return [tuple(r) for r in c.execute(sql).fetchall()]


def _sqlite_tables(path: Path) -> set[str]:
    return {r[0] for r in _sqlite_rows(path, "SELECT name FROM sqlite_master WHERE type='table'")}


def _assert_all_quarantined_refusal(result, rows: int, *, merge_side: bool = True) -> None:
    from services.error_handling import humanize_transfer_failure

    assert result.success is False, result.error
    assert f"All {rows} row(s) were quarantined, so nothing was written" in str(result.error)
    assert humanize_transfer_failure(ValueError(result.error))["code"] == "all_rows_quarantined"
    assert not _recon(result).get("passed"), "an all-quarantined run is never a green Gate-8"
    if not merge_side:
        return  # the staging load's own refusal (the streaming writer's path)
    summary = result.destination_summary or {}
    assert int(summary.get("rejected_rows") or 0) >= rows
    assert summary.get("rejected_details"), "the held-out rows stay in quarantine for replay"


# --- (a) staged merge maps a renamed column ---------------------------------


@pytest.mark.parametrize("validation_mode", ["strict", "balanced"])
def test_staged_scd2_renamed_column_lands_every_row(tmp_path, validation_mode):
    src = _sqlite_source(tmp_path / "src.db", ROWS)
    dst_path = tmp_path / "dst.db"
    result = _run(src, _sqlite_dest(dst_path), validation_mode=validation_mode)

    assert result.success, result.error
    assert any("STAGING" in d for d in result.ddl_executed or []), "staged merge route"
    assert result.records_transferred == 3
    assert int((result.destination_summary or {}).get("rejected_rows") or 0) == 0
    rec = _recon(result)
    assert rec.get("passed") is True and rec.get("source_rows") == 3, rec.get("message")
    got = _sqlite_rows(
        dst_path, "SELECT id, name, clv_value, typeof(clv_value) FROM dim "
        "WHERE is_current = 1 ORDER BY id",
    )
    # The renamed column keeps the source's REAL carrier, not a TEXT fallback.
    assert got == [(1, "A", 10.5, "real"), (2, "B", 20.25, "real"), (3, "C", 30.0, "real")]

    # Run 2: one changed value versions exactly that key.
    _sqlite_source(tmp_path / "src.db", [(1, "A", 10.5), (2, "B", 99.0), (3, "C", 30.0)])
    again = _run(src, _sqlite_dest(dst_path), validation_mode=validation_mode)
    assert again.success, again.error
    assert again.records_transferred == 1
    assert _sqlite_rows(
        dst_path, "SELECT id, clv_value, is_current FROM dim WHERE id = 2 ORDER BY valid_from"
    ) == [(2, 20.25, 0), (2, 99.0, 1)]


def test_staged_mirror_renamed_column_lands_every_row(tmp_path):
    src = _sqlite_source(tmp_path / "src.db", ROWS)
    dst_path = tmp_path / "dst.db"
    result = _run(src, _sqlite_dest(dst_path), sync="full_refresh_mirror")

    assert result.success, result.error
    assert _sqlite_rows(dst_path, "SELECT id, name, clv_value FROM dim ORDER BY id") == ROWS


def test_staged_scd2_renamed_primary_key_merges_on_the_target_key(tmp_path):
    src = _sqlite_source(tmp_path / "src.db", ROWS)
    dst_path = tmp_path / "dst.db"
    mappings = [{**RENAME_MAP[0], "target": "customer_id"}] + RENAME_MAP[1:]
    result = _run(src, _sqlite_dest(dst_path), mappings=mappings)

    assert result.success, result.error
    assert _sqlite_rows(
        dst_path, "SELECT customer_id, clv_value FROM dim WHERE is_current = 1 ORDER BY 1"
    ) == [(1, 10.5), (2, 20.25), (3, 30.0)]


@pytest.mark.parametrize("sync", ["scd2", "full_refresh_mirror"])
def test_staged_merge_applies_value_transforms_once(tmp_path, sync):
    """The spool already holds transformed values; the merge must not redo them."""
    src = _sqlite_source(tmp_path / "src.db", [(1, "a@x.io", 1.0)])
    dst_path = tmp_path / "dst.db"
    mappings = [
        RENAME_MAP[0],
        {**RENAME_MAP[1], "target": "email_hash", "transform": "hash_pii"},
        RENAME_MAP[2],
    ]
    once = UniversalTransferEngine().execute(
        TransferRequest(
            source=src, destination=_sqlite_dest(tmp_path / "plain.db"),
            sync_mode="full_refresh_overwrite", validation_mode="balanced",
            mappings=[dict(m) for m in mappings],
        )
    )
    assert once.success, once.error
    expected = _sqlite_rows(tmp_path / "plain.db", "SELECT email_hash FROM dim")

    result = _run(src, _sqlite_dest(dst_path), sync=sync, mappings=mappings)
    assert result.success, result.error
    assert _sqlite_rows(dst_path, "SELECT email_hash FROM dim") == expected


# --- (b) a run that quarantines every row is refused, never a green run ------


def test_staged_scd2_every_row_quarantined_fails_and_creates_nothing(tmp_path):
    """Merge-side hold-out (NULL business key) after a clean staging load."""
    src = _sqlite_source(tmp_path / "src.db", [(None, "A", 1.0)])
    dst_path = tmp_path / "dst.db"
    result = _run(src, _sqlite_dest(dst_path), skip_preflight=True)

    _assert_all_quarantined_refusal(result, 1)
    assert "dim" not in _sqlite_tables(dst_path), "nothing was created for an empty merge"
    assert not [t for t in _sqlite_tables(dst_path) if t.startswith("_dataflow_stg_")]


def test_buffered_scd2_every_row_quarantined_keeps_existing_history(tmp_path):
    """File source -> buffered merge: an all-held-out run 2 must not close history."""
    dst_path = tmp_path / "dst.db"
    csv = EndpointConfig(kind="file", format="csv")
    common = dict(
        mappings=BAD_CLV_MAP,
        source_filename="cust.csv",
        column_types={"id": "integer", "name": "string", "clv": "string"},
        skip_preflight=True,
    )
    first = _run(csv, _sqlite_dest(dst_path),
                 source_content=b"id,name,clv\n1,A,10\n2,B,20\n3,C,30\n", **common)
    assert first.success, first.error
    second = _run(csv, _sqlite_dest(dst_path),
                  source_content=b"id,name,clv\n1,A,bad\n2,B,worse\n3,C,awful\n", **common)

    _assert_all_quarantined_refusal(second, 3)
    assert _sqlite_rows(
        dst_path, "SELECT id, clv_value, is_current FROM dim ORDER BY id"
    ) == [(1, 10, 1), (2, 20, 1), (3, 30, 1)], "every current version must stay current"


def test_duckdb_scd2_rename_and_all_quarantined_keeps_history(tmp_path):
    """DuckDB (generic SQLAlchemy) takes the staged merge: rename, then a run
    whose every row is held out at the staging load."""
    duckdb = pytest.importorskip("duckdb")
    src_path = tmp_path / "src.db"
    dst_path = tmp_path / "dst.duckdb"
    dest = EndpointConfig(kind="database", format="duckdb",
                          connection_string=f"duckdb:///{dst_path}",
                          database=str(dst_path), table="dim")
    src = _sqlite_source(src_path, [(1, "A", "10"), (2, "B", "20"), (3, "C", "30")], "TEXT")
    first = _run(src, dest, mappings=BAD_CLV_MAP, skip_preflight=True)
    assert first.success, first.error
    assert any("STAGING" in d for d in first.ddl_executed or []), "staged merge route"

    _sqlite_source(src_path, [(1, "A", "bad"), (2, "B", "worse"), (3, "C", "awful")], "TEXT")
    second = _run(src, dest, mappings=BAD_CLV_MAP, skip_preflight=True)
    _assert_all_quarantined_refusal(second, 3, merge_side=False)
    with duckdb.connect(str(dst_path)) as c:
        got = c.execute("SELECT id, clv_value, is_current FROM dim ORDER BY id").fetchall()
    assert [tuple(r) for r in got] == [(1, 10, True), (2, 20, True), (3, 30, True)]


# --- live PostgreSQL / MySQL (staged merge) ----------------------------------


def _live_dest(kind: str, table: str) -> EndpointConfig:
    from tests import r4_route_harness as h

    cfg = h.dest_config(kind)  # skips when the service is not configured
    return EndpointConfig(
        kind="database", format=kind, host=cfg["host"], port=int(cfg["port"]),
        database=cfg["database"], username=cfg["username"], password=cfg["password"],
        schema=cfg.get("schema", ""), table=table,
    )


@pytest.mark.parametrize("kind", ["postgresql", "mysql"])
def test_live_staged_scd2_rename_and_all_quarantined(tmp_path, kind):
    from tests import r4_route_harness as h

    table = f"scd2f1_{uuid.uuid4().hex[:8]}"
    empty_table = f"{table}_q"
    dest = _live_dest(kind, table)
    try:
        src = _sqlite_source(tmp_path / "src.db", ROWS)
        result = _run(src, dest)
        assert result.success, result.error
        assert any("STAGING" in d for d in result.ddl_executed or []), "staged merge route"
        assert _recon(result).get("passed") is True, _recon(result).get("message")
        got = h.query(kind, f"SELECT id, name, clv_value FROM {h.quote(kind, table)} "  # nosec B608
                            "ORDER BY id")
        assert [(int(i), n, float(v)) for i, n, v in got] == ROWS

        null_key = _sqlite_source(tmp_path / "null.db", [(None, "A", 1.0)])
        refused = _run(null_key, _live_dest(kind, empty_table), skip_preflight=True)
        _assert_all_quarantined_refusal(refused, 1)
        assert not h.query(
            kind,
            "SELECT 1 FROM information_schema.tables "  # nosec B608
            f"WHERE table_name = '{empty_table}'",
        ), "nothing was created for an empty merge"
    finally:
        h.drop_table(kind, table)
        h.drop_table(kind, empty_table)
        h.drop_table(kind, f"{empty_table}_df_quarantine")
        h.drop_table(kind, f"{table}_df_quarantine")
