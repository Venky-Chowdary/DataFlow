"""DuckDB → DuckDB ATTACH + INSERT SELECT — dest COUNT(*), never table metadata."""

from __future__ import annotations

import sys
import tempfile
import uuid
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_duckdb_common import (  # noqa: E402
    duckdb_family_name,
    duckdb_type_is_copy_safe,
    duckdb_type_text_is_safe,
)
from services.copy_duckdb_duckdb import (  # noqa: E402
    copy_duckdb_to_duckdb,
    duckdb_duckdb_copy_enabled,
)
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402

pytest.importorskip("duckdb")


def _path(tag: str, name: str) -> str:
    return str(Path(tempfile.gettempdir()) / f"dfddb_{name}_{tag}.duckdb")


def _cfg(path: str, table: str) -> dict:
    return {
        "type": "duckdb",
        "format": "duckdb",
        "database": path,
        "schema": "main",
        "table": table,
    }


def _dest_count(path: str, table: str) -> int:
    n = destination_row_count(
        "duckdb", _cfg(path, table), schema="main", table_name=table
    )
    assert n is not None
    return int(n)


def _connect(path: str):
    import duckdb

    return duckdb.connect(path)


def _seed(path: str, table: str, rows: int, *, pk: bool = True) -> None:
    conn = _connect(path)
    try:
        key = " PRIMARY KEY" if pk else ""
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(
            f'CREATE TABLE "{table}" (id INTEGER{key}, label VARCHAR NOT NULL)'
        )
        conn.execute(
            f'INSERT INTO "{table}" SELECT i, \'r\' || i FROM range(1, {rows + 1}) t(i)'
        )
    finally:
        conn.close()


def _drop(path: str, table: str) -> None:
    conn = _connect(path)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    finally:
        conn.close()


def _pairs():
    return [("id", "id"), ("label", "label")]


def _ddls():
    return ["INTEGER", "VARCHAR"]


def _cleanup(*paths: str) -> None:
    for p in paths:
        for suffix in ("", ".wal"):
            try:
                Path(p + suffix).unlink()
            except OSError:
                pass


def test_duckdb_family_and_copy_safe_types():
    assert duckdb_family_name("duckdb") == "duckdb"
    assert duckdb_family_name("generic_sql", {"type": "duckdb"}) == "duckdb"
    assert duckdb_family_name("motherduck") == "duckdb"
    assert duckdb_family_name("generic_sql", {"type": "clickhouse"}) == "generic_sql"
    assert duckdb_type_is_copy_safe("DECIMAL(10,2)") is True
    assert duckdb_type_is_copy_safe("VARCHAR[]") is True
    assert duckdb_type_is_copy_safe("TIMESTAMP WITH TIME ZONE") is True
    assert duckdb_type_is_copy_safe("not_a_type") is False
    assert duckdb_type_text_is_safe("ENUM('a', 'b')") is True
    assert duckdb_type_text_is_safe("VARCHAR; DROP TABLE t") is False


def test_duckdb_duckdb_copy_kill_switch(monkeypatch):
    tag = uuid.uuid4().hex[:8]
    monkeypatch.setenv("DATAFLOW_DUCKDB_DUCKDB_COPY", "0")
    assert duckdb_duckdb_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_duckdb_to_duckdb(
            source_cfg=_cfg(_path(tag, "src"), "a"),
            source_table="a",
            dest_cfg=_cfg(_path(tag, "dst"), "b"),
            dest_table="b",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )


def test_duckdb_same_file_same_table_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    same = _path(tag, "same")
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_duckdb_to_duckdb(
            source_cfg=_cfg(same, "t"),
            source_table="t",
            dest_cfg=_cfg(same, "t"),
            dest_table="t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )


def test_duckdb_memory_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_duckdb_to_duckdb(
            source_cfg=_cfg(":memory:", "a"),
            source_table="a",
            dest_cfg=_cfg(_path(tag, "dst"), "b"),
            dest_table="b",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )


def test_duckdb_motherduck_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    with pytest.raises(FastPathUnavailable, match="MotherDuck"):
        copy_duckdb_to_duckdb(
            source_cfg=_cfg("md:analytics", "a"),
            source_table="a",
            dest_cfg=_cfg(_path(tag, "dst"), "b"),
            dest_table="b",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )


def test_duckdb_column_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_duckdb_to_duckdb(
            source_cfg=_cfg(_path(tag, "src"), "a"),
            source_table="a",
            dest_cfg=_cfg(_path(tag, "dst"), "b"),
            dest_table="b",
            pairs=[("id", "user_id")],
            duckdb_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_duckdb_duckdb_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 800)
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("duckdb_read") == "attach_select"
        assert result.source_snapshot.get("duckdb_write") == "insert"
        assert _dest_count(dst_p, "dst_t") == 800
        assert _dest_count(src_p, "src_t") == 800
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_same_file_different_tables(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    one = _path(tag, "one")
    try:
        _seed(one, "src_t", 120)
        _drop(one, "dst_t")
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(one, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(one, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )
        assert result.source_rows == 120
        assert result.source_snapshot.get("duckdb_read") == "same_file_select"
        assert _dest_count(one, "dst_t") == 120
    finally:
        _cleanup(one)


def test_live_duckdb_carries_source_types_and_keys(monkeypatch):
    """Identity dest must not widen INTEGER→BIGINT or drop NOT NULL / PK."""
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        conn = _connect(src_p)
        conn.execute(
            'CREATE TABLE "src_t" ('
            "id INTEGER PRIMARY KEY, "
            "amount DECIMAL(10,2), "
            "note VARCHAR NOT NULL DEFAULT 'x', "
            "created DATE, "
            "payload BLOB, "
            "tags VARCHAR[])"
        )
        conn.execute(
            'INSERT INTO "src_t" VALUES '
            "(1, 1000.00, 'a', DATE '2024-01-15', 'ab'::BLOB, ['p','q']), "
            "(2, 2.50, '', DATE '2024-02-01', NULL, [])"
        )
        conn.close()
        pairs = [
            ("id", "id"),
            ("amount", "amount"),
            ("note", "note"),
            ("created", "created"),
            ("payload", "payload"),
            ("tags", "tags"),
        ]
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=pairs,
            duckdb_ddls=[
                "INTEGER",
                "DECIMAL(10,2)",
                "VARCHAR",
                "DATE",
                "BLOB",
                "VARCHAR[]",
            ],
            replace_destination=True,
        )
        assert result.target_rows == 2
        assert _dest_count(dst_p, "dst_t") == 2
        conn = _connect(dst_p)
        try:
            ddl = conn.execute(
                "SELECT sql FROM duckdb_tables() WHERE table_name='dst_t'"
            ).fetchone()[0]
            cols = {
                r[0]: r[1]
                for r in conn.execute(
                    "SELECT column_name, data_type FROM duckdb_columns() "
                    "WHERE table_name='dst_t'"
                ).fetchall()
            }
            constraints = {
                r[0]
                for r in conn.execute(
                    "SELECT constraint_type FROM duckdb_constraints() "
                    "WHERE table_name='dst_t'"
                ).fetchall()
            }
            rows = conn.execute(
                'SELECT id, amount, note, created, payload, tags FROM "dst_t" ORDER BY id'
            ).fetchall()
        finally:
            conn.close()
        assert cols["id"] == "INTEGER", f"identity must not widen id: {ddl}"
        assert cols["amount"] == "DECIMAL(10,2)"
        assert cols["payload"] == "BLOB"
        assert cols["tags"] == "VARCHAR[]"
        assert "PRIMARY KEY" in constraints
        assert "NOT NULL" in constraints
        assert "DEFAULT('x')" in ddl
        assert rows[0][1] == Decimal("1000.00")
        assert rows[0][2] == "a"
        assert rows[0][3] == date(2024, 1, 15)
        assert bytes(rows[0][4]) == b"ab"
        assert list(rows[0][5]) == ["p", "q"]
        assert rows[1][2] == "", "empty string must not become NULL"
        assert rows[1][4] is None
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_check_constraint_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        conn = _connect(src_p)
        conn.execute(
            'CREATE TABLE "src_t" (id INTEGER, label VARCHAR, '
            "CHECK (id > 0))"
        )
        conn.execute('INSERT INTO "src_t" VALUES (1, \'a\')')
        conn.close()
        with pytest.raises(FastPathUnavailable, match="CHECK"):
            copy_duckdb_to_duckdb(
                source_cfg=_cfg(src_p, "src_t"),
                source_table="src_t",
                dest_cfg=_cfg(dst_p, "dst_t"),
                dest_table="dst_t",
                pairs=_pairs(),
                duckdb_ddls=_ddls(),
                replace_destination=True,
            )
        assert _dest_count(dst_p, "dst_t") == 0
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_unmapped_key_column_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        conn = _connect(src_p)
        conn.execute(
            'CREATE TABLE "src_t" (id INTEGER PRIMARY KEY, label VARCHAR)'
        )
        conn.execute('INSERT INTO "src_t" VALUES (1, \'a\')')
        conn.close()
        with pytest.raises(FastPathUnavailable, match="unmapped column"):
            copy_duckdb_to_duckdb(
                source_cfg=_cfg(src_p, "src_t"),
                source_table="src_t",
                dest_cfg=_cfg(dst_p, "dst_t"),
                dest_table="dst_t",
                pairs=[("label", "label")],
                duckdb_ddls=["VARCHAR"],
                replace_destination=True,
            )
        assert _dest_count(dst_p, "dst_t") == 0
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_copy_is_not_pandas_or_parquet(monkeypatch):
    """The copy must stay in-engine: no fetch into Python, no parquet staging."""
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    _seed(src_p, "src_t", 90)

    import duckdb

    def _no_df(*_a, **_k):
        raise AssertionError("DuckDB identity COPY must not materialize a dataframe")

    for attr in ("df", "fetchdf", "fetch_df", "arrow", "fetch_arrow_table"):
        if hasattr(duckdb.DuckDBPyConnection, attr):
            monkeypatch.setattr(duckdb.DuckDBPyConnection, attr, _no_df, raising=False)
    try:
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )
        assert result.target_rows == 90
        assert _dest_count(dst_p, "dst_t") == 90
        assert not list(Path(tempfile.gettempdir()).glob(f"*{tag}*.parquet"))
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_skip_when_dest_count_matches(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 800)
        first = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dst_p, "dst_t") == 800
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_occupied_mismatch_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 800)
        _seed(dst_p, "dst_t", 2)
        assert _dest_count(dst_p, "dst_t") == 2
        with pytest.raises(FastPathUnavailable, match="occupied DuckDB dest"):
            copy_duckdb_to_duckdb(
                source_cfg=_cfg(src_p, "src_t"),
                source_table="src_t",
                dest_cfg=_cfg(dst_p, "dst_t"),
                dest_table="dst_t",
                pairs=_pairs(),
                duckdb_ddls=_ddls(),
                replace_destination=False,
            )
        assert _dest_count(dst_p, "dst_t") == 2
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_overwrite_replaces_dest(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 800)
        _seed(dst_p, "dst_t", 1)
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )
        assert result.source_snapshot.get("duckdb_write") == "overwrite"
        assert _dest_count(dst_p, "dst_t") == 800
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_dest_count_is_not_table_metadata(monkeypatch):
    """A second table in the same file must not inflate the dest COUNT."""
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 80)
        _seed(dst_p, "other_t", 50)
        result = copy_duckdb_to_duckdb(
            source_cfg=_cfg(src_p, "src_t"),
            source_table="src_t",
            dest_cfg=_cfg(dst_p, "dst_t"),
            dest_table="dst_t",
            pairs=_pairs(),
            duckdb_ddls=_ddls(),
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dst_p, "dst_t") == 80
        assert _dest_count(dst_p, "other_t") == 50
    finally:
        _cleanup(src_p, dst_p)


def test_live_duckdb_in_process_source_holder_declines(monkeypatch):
    """A holder in this process owns the file handle: decline, do not fail the job."""
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    import duckdb

    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    _seed(src_p, "src_t", 10)
    holder = duckdb.connect(src_p)
    try:
        with pytest.raises(FastPathUnavailable, match="held by another connection"):
            copy_duckdb_to_duckdb(
                source_cfg=_cfg(src_p, "src_t"),
                source_table="src_t",
                dest_cfg=_cfg(dst_p, "dst_t"),
                dest_table="dst_t",
                pairs=_pairs(),
                duckdb_ddls=_ddls(),
                replace_destination=True,
            )
        assert _dest_count(dst_p, "dst_t") == 0
    finally:
        holder.close()
        _cleanup(src_p, dst_p)


def test_live_duckdb_foreign_process_writer_declines(monkeypatch):
    """Another OS process holding the source for writing declines the same way."""
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    import subprocess
    import textwrap
    import time

    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    ready = _path(tag, "ready") + ".flag"
    _seed(src_p, "src_t", 10)
    holder_src = textwrap.dedent(
        f"""
        import duckdb, pathlib, time
        c = duckdb.connect(r"{src_p}")
        c.execute("SELECT COUNT(*) FROM src_t")
        pathlib.Path(r"{ready}").write_text("1")
        time.sleep(30)
        """
    )
    proc = subprocess.Popen([sys.executable, "-c", holder_src])
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline and not Path(ready).exists():
            time.sleep(0.2)
        assert Path(ready).exists(), "holder process never opened the source"
        with pytest.raises(FastPathUnavailable, match="held by another connection"):
            copy_duckdb_to_duckdb(
                source_cfg=_cfg(src_p, "src_t"),
                source_table="src_t",
                dest_cfg=_cfg(dst_p, "dst_t"),
                dest_table="dst_t",
                pairs=_pairs(),
                duckdb_ddls=_ddls(),
                replace_destination=True,
            )
        assert _dest_count(dst_p, "dst_t") == 0
    finally:
        proc.kill()
        proc.wait(timeout=10)
        try:
            Path(ready).unlink()
        except OSError:
            pass
        _cleanup(src_p, dst_p)


def test_live_duckdb_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_DUCKDB_DUCKDB_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src_p, dst_p = _path(tag, "src"), _path(tag, "dst")
    try:
        _seed(src_p, "src_t", 800)
        _drop(dst_p, "dst_t")
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"duckdb-duckdb-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict("database", _cfg(src_p, "src_t"))
        destination = EndpointConfig.from_dict("database", _cfg(dst_p, "dst_t"))
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "INTEGER", "label": "VARCHAR"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "attach_insert_select_duckdb_duckdb"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("DuckDB" in line for line in ddl_log)
        assert _dest_count(dst_p, "dst_t") == 800
    finally:
        _cleanup(src_p, dst_p)
