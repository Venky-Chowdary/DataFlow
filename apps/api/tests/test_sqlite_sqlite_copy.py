"""SQLite → SQLite ATTACH + INSERT SELECT — dest COUNT(*)."""

from __future__ import annotations

import sqlite3
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_sqlite_common import sqlite_type_is_copy_safe  # noqa: E402
from services.copy_sqlite_sqlite import (  # noqa: E402
    copy_sqlite_to_sqlite,
    sqlite_sqlite_copy_enabled,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _dest_count(path: Path | str, table: str) -> int:
    n = destination_row_count("sqlite", _cfg(path, table), schema="", table_name=table)
    assert n is not None
    return int(n)


def _seed(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


def _drop_table(path: Path, table: str) -> None:
    if not path.exists():
        return
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.commit()
    conn.close()


def test_sqlite_sqlite_copy_safe_types():
    assert sqlite_type_is_copy_safe("INTEGER") is True
    assert sqlite_type_is_copy_safe("TEXT") is True
    assert sqlite_type_is_copy_safe("VARCHAR(32)") is True
    assert sqlite_type_is_copy_safe("") is True
    assert sqlite_type_is_copy_safe("REAL") is True
    assert sqlite_type_is_copy_safe("BLOB") is False
    assert sqlite_type_is_copy_safe("BYTEA") is False


def test_sqlite_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_SQLITE_COPY", "0")
    assert sqlite_sqlite_copy_enabled() is False
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(src, "missing"),
            source_table="missing",
            dest_cfg=_cfg(dest, "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_sqlite_sqlite_same_file_same_table_declines(tmp_path):
    db = tmp_path / "same.db"
    _seed(db, "orders", 3)
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(db, "orders"),
            source_table="orders",
            dest_cfg=_cfg(db, "orders"),
            dest_table="orders",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )


def test_sqlite_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_cfg("/tmp/nope.db", "out"),
            dest_table="out",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_sqlite_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_SQLITE_COPY", raising=False)
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    _seed(src, "src_t", 800)
    result = copy_sqlite_to_sqlite(
        source_cfg=_cfg(src, "src_t"),
        source_table="src_t",
        dest_cfg=_cfg(dest, "dst_t"),
        dest_table="dst_t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.source_rows == 800
    assert result.target_rows == 800
    assert result.source_snapshot.get("copy_split") == "serial"
    assert result.source_snapshot.get("sqlite_read") == "attach_select"
    assert result.source_snapshot.get("sqlite_write") == "insert"
    assert _dest_count(dest, "dst_t") == 800
    assert _dest_count(src, "src_t") == 800


def test_live_sqlite_sqlite_same_file_different_tables(tmp_path):
    db = tmp_path / "one.db"
    _seed(db, "src_t", 80)
    _drop_table(db, "dst_t")
    result = copy_sqlite_to_sqlite(
        source_cfg=_cfg(db, "src_t"),
        source_table="src_t",
        dest_cfg=_cfg(db, "dst_t"),
        dest_table="dst_t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.source_snapshot.get("sqlite_read") == "same_file_select"
    assert result.target_rows == 80
    assert _dest_count(db, "dst_t") == 80
    assert _dest_count(db, "src_t") == 80


def test_live_sqlite_sqlite_empty_string_and_null_preserved(tmp_path):
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, label TEXT)")
    conn.executemany(
        "INSERT INTO src_t (id, label) VALUES (?, ?)",
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    result = copy_sqlite_to_sqlite(
        source_cfg=_cfg(src, "src_t"),
        source_table="src_t",
        dest_cfg=_cfg(dest, "dst_t"),
        dest_table="dst_t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.source_rows == 3
    assert _dest_count(dest, "dst_t") == 3
    conn = sqlite3.connect(dest)
    rows = conn.execute("SELECT id, label FROM dst_t ORDER BY id").fetchall()
    conn.close()
    assert rows[0] == (1, None)
    assert rows[1] == (2, "")
    assert rows[2] == (3, "x")


def test_live_sqlite_sqlite_blob_declines(tmp_path):
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    conn = sqlite3.connect(src)
    conn.execute("CREATE TABLE src_t (id INTEGER NOT NULL PRIMARY KEY, payload BLOB)")
    conn.execute("INSERT INTO src_t (id, payload) VALUES (1, x'00ff')")
    conn.commit()
    conn.close()
    with pytest.raises(FastPathUnavailable, match="not SQLite COPY-safe"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("payload", "payload")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
    assert not dest.exists() or _dest_count(dest, "dst_t") == 0


def test_live_sqlite_sqlite_equal_count_append_declines(tmp_path):
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    _seed(src, "src_t", 800)
    first = copy_sqlite_to_sqlite(
        source_cfg=_cfg(src, "src_t"),
        source_table="src_t",
        dest_cfg=_cfg(dest, "dst_t"),
        dest_table="dst_t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=False,
    )
    assert first.target_rows == 800
    with pytest.raises(FastPathUnavailable, match="occupied"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
    assert _dest_count(dest, "dst_t") == 800


def test_live_sqlite_sqlite_occupied_mismatch_declines(tmp_path):
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    _seed(src, "src_t", 800)
    _seed(dest, "dst_t", 2)
    with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dest, "dst_t"),
            dest_table="dst_t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
    assert _dest_count(dest, "dst_t") == 2


def test_live_sqlite_sqlite_overwrite_replaces_dest(tmp_path):
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    _seed(src, "src_t", 800)
    _seed(dest, "dst_t", 1)
    result = copy_sqlite_to_sqlite(
        source_cfg=_cfg(src, "src_t"),
        source_table="src_t",
        dest_cfg=_cfg(dest, "dst_t"),
        dest_table="dst_t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=True,
    )
    assert result.source_snapshot.get("sqlite_write") == "overwrite"
    assert _dest_count(dest, "dst_t") == 800


def test_live_sqlite_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_SQLITE_COPY", raising=False)
    src = tmp_path / "src.db"
    dest = tmp_path / "dst.db"
    _seed(src, "src_t", 800)
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    ensure_memory_job_store_if_mongo_down()
    tag = uuid.uuid4().hex[:8]
    job_id = f"sqlite-sqlite-copy-{tag}"
    get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
    source = EndpointConfig.from_dict("database", {**_cfg(src, "src_t"), "format": "sqlite"})
    destination = EndpointConfig.from_dict(
        "database", {**_cfg(dest, "dst_t"), "format": "sqlite"}
    )
    mappings = [
        {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
        {"source": "label", "target": "label", "type": "TEXT", "transform": "none"},
    ]
    transferred, ddl_log, summary, _cols = stream_database_transfer(
        source,
        destination,
        mappings,
        {"id": "INTEGER", "label": "TEXT"},
        sync_mode="full_refresh_overwrite",
        job_id=job_id,
    )
    assert transferred == 800
    assert summary.get("load_method") == "attach_insert_select_sqlite"
    assert summary.get("source_row_count") == 800
    assert int(summary.get("rejected_rows") or 0) == 0
    assert any("SQLite" in line for line in ddl_log)
    assert _dest_count(dest, "dst_t") == 800


def test_failed_copy_never_touches_source_table_of_same_name(monkeypatch, tmp_path):
    """A failed COPY rolls back the dest transaction and leaves the source intact.

    Regression: with ``srcdb`` attached, an unqualified ``DROP TABLE "t"`` in
    the failure cleanup resolved to ``srcdb."t"`` once the rollback had removed
    ``main."t"`` — the operator's source table was destroyed.
    """
    import connectors.sqlite_writer as sqlite_writer

    src = tmp_path / "src.sqlite"
    dst = tmp_path / "dst.sqlite"
    _seed(src, "t", 6)

    def _kill(*_a, **_k):
        raise RuntimeError("simulated kill after INSERT SELECT")

    monkeypatch.setattr(sqlite_writer, "mark_raw_chunk_committed", _kill)
    from services.copy_fast_path import copy_ledger_key

    with pytest.raises(RuntimeError):
        copy_sqlite_to_sqlite(
            source_cfg=_cfg(src, "t"),
            source_table="t",
            dest_cfg=_cfg(dst, "t"),
            dest_table="t",
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
            ledger=copy_ledger_key(uuid.uuid4().hex[:24], "t"),
        )
    assert _dest_count(src, "t") == 6, "source table must survive a failed COPY"
    conn = sqlite3.connect(dst)
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    finally:
        conn.close()
    assert "t" not in names, "rolled-back dest CREATE must not persist"


def test_ledger_retry_of_committed_copy_shard_does_not_duplicate(tmp_path):
    """Same job_id retry after a committed COPY reads the shard from the ledger."""
    from services.copy_fast_path import copy_ledger_key

    src = tmp_path / "src.sqlite"
    dst = tmp_path / "dst.sqlite"
    _seed(src, "t", 5)
    key = copy_ledger_key(uuid.uuid4().hex[:24], "t")
    kwargs = dict(
        source_cfg=_cfg(src, "t"),
        source_table="t",
        dest_cfg=_cfg(dst, "t"),
        dest_table="t",
        pairs=[("id", "id"), ("label", "label")],
        sqlite_ddls=["INTEGER", "TEXT"],
        replace_destination=False,
        ledger=key,
    )
    first = copy_sqlite_to_sqlite(**kwargs)
    assert first.rows_copied == 5
    assert first.source_snapshot["guarantee"] == "sqlite_transaction_snapshot"
    second = copy_sqlite_to_sqlite(**kwargs)
    assert second.rows_copied == 5
    assert second.proof_scope == "write_ledger_shard_already_committed"
    assert _dest_count(dst, "t") == 5
    # A different job into the occupied dest still declines (would duplicate).
    with pytest.raises(FastPathUnavailable):
        copy_sqlite_to_sqlite(**{**kwargs, "ledger": copy_ledger_key(uuid.uuid4().hex[:24], "t")})
