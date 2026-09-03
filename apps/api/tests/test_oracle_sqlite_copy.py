"""Oracle SHARE-lock SELECT → SQLite executemany — dest COUNT(*)."""

from __future__ import annotations

import os
import socket
import sqlite3
import sys
import uuid
from datetime import date
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_oracle_pg import oracle_type_is_copy_safe  # noqa: E402
from services.copy_oracle_sqlite import (  # noqa: E402
    copy_oracle_to_sqlite,
    oracle_sqlite_copy_enabled,
    oracle_value_to_sqlite,
)
from services.copy_sqlite_oracle import copy_sqlite_to_oracle  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _oracle_password() -> str:
    env = (
        os.environ.get("DATAFLOW_ORACLE_PASSWORD")
        or os.environ.get("ORA_PASSWORD")
        or ""
    ).strip()
    if env:
        return env
    path = Path("/tmp/df-desktop-lab/oracle_password")
    if path.is_file():
        return path.read_text().strip()
    return "dataflow"


def _ora_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("Oracle 1521 not reachable")


def _sqlite_cfg(path: Path | str, table: str) -> dict:
    return {
        "type": "sqlite",
        "format": "sqlite",
        "database": str(path),
        "table": table,
    }


def _ora_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 1521,
        "database": "XEPDB1",
        "service_name": "XEPDB1",
        "username": "dataflow",
        "password": _oracle_password(),
        "schema": "DATAFLOW",
    }


def _ora_connect():
    _ora_or_skip()
    oracledb = pytest.importorskip("oracledb")
    try:
        return oracledb.connect(
            user="dataflow",
            password=_oracle_password(),
            dsn="127.0.0.1:1521/XEPDB1",
        )
    except Exception as exc:
        pytest.skip(f"Oracle auth failed: {exc}")


def _seed_sqlite(path: Path, table: str, rows: int) -> None:
    conn = sqlite3.connect(path)
    conn.execute(f'DROP TABLE IF EXISTS "{table}"')
    conn.execute(
        f'CREATE TABLE "{table}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
    )
    conn.executemany(
        f'INSERT INTO "{table}" (id, label) VALUES (?, ?)',
        [(i, f"r{i}") for i in range(1, rows + 1)],
    )
    conn.commit()
    conn.close()


def _drop_ora(table: str) -> None:
    conn = _ora_connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        conn.commit()
    finally:
        conn.close()


def _ora_count(table: str) -> int:
    n = destination_row_count(
        "oracle", _ora_cfg(), schema="DATAFLOW", table_name=table
    )
    assert n is not None
    return int(n)


def _sqlite_count(path: Path, table: str) -> int:
    n = destination_row_count(
        "sqlite", _sqlite_cfg(path, table), schema="", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_ora_from_sqlite(src: Path, src_table: str, dest: str, rows: int) -> None:
    _seed_sqlite(src, src_table, rows)
    _drop_ora(dest)
    result = copy_sqlite_to_oracle(
        source_cfg=_sqlite_cfg(src, src_table),
        source_table=src_table,
        dest_cfg=_ora_cfg(),
        dest_table=dest,
        pairs=[("id", "id"), ("label", "label")],
        oracle_ddls=["NUMBER", "VARCHAR2(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _ora_count(dest) == rows


def test_oracle_sqlite_copy_safe_types():
    assert oracle_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_type_is_copy_safe("NUMBER") is True
    assert oracle_type_is_copy_safe("DATE") is True
    assert oracle_type_is_copy_safe("BLOB") is False
    assert oracle_type_is_copy_safe("CLOB") is False
    assert oracle_type_is_copy_safe("JSON") is False


def test_oracle_sqlite_date_binds_as_text():
    assert oracle_value_to_sqlite(date(2020, 1, 2)) == "2020-01-02"
    assert oracle_value_to_sqlite(None) is None
    assert oracle_value_to_sqlite("") == ""
    with pytest.raises(FastPathUnavailable, match="binary"):
        oracle_value_to_sqlite(b"x")


def test_oracle_sqlite_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_ORACLE_SQLITE_COPY", "0")
    assert oracle_sqlite_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_oracle_sqlite_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(":memory:", "orders"),
            dest_table="orders",
            pairs=[("id", "id")],
            sqlite_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_oracle_sqlite_blob_dest_declines(tmp_path):
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_sqlite_cfg(tmp_path / "dest.db", "nope"),
            dest_table="nope",
            pairs=[("payload", "payload")],
            sqlite_ddls=["BLOB"],
            replace_destination=True,
        )


def test_live_oracle_sqlite_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_ORACLE_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_mid_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_dst"
    try:
        _seed_ora_from_sqlite(src, "src_t", ora_mid, 800)
        result = copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_schema="DATAFLOW",
            source_table=ora_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("sqlite_write") == "insert"
        assert result.source_snapshot.get("oracle_read") == "share_select"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ora(ora_mid)


def test_live_oracle_sqlite_null_stays_null(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_null_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_null_dst"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    try:
        _drop_ora(ora_mid)
        copy_sqlite_to_oracle(
            source_cfg=_sqlite_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=ora_mid,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        result = copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_schema="DATAFLOW",
            source_table=ora_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _sqlite_count(dest_db, dest) == 3
        out = sqlite3.connect(dest_db)
        try:
            rows = out.execute(f'SELECT id, label FROM "{dest}" ORDER BY id').fetchall()
        finally:
            out.close()
        # VARCHAR2 '' IS NULL — engine law, not a row drop. Round-trip NULL stays NULL.
        assert rows == [(1, None), (2, None), (3, "x")]
    finally:
        _drop_ora(ora_mid)


def test_live_oracle_sqlite_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_skip_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_skip_dst"
    try:
        _seed_ora_from_sqlite(src, "src_t", ora_mid, 800)
        first = copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_schema="DATAFLOW",
            source_table=ora_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_schema="DATAFLOW",
            source_table=ora_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ora(ora_mid)


def test_live_oracle_sqlite_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_occ_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_occ_dst"
    try:
        _seed_ora_from_sqlite(src, "src_t", ora_mid, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.executemany(
            f'INSERT INTO "{dest}" (id, label) VALUES (?, ?)',
            [(1, "ghost"), (2, "ghost")],
        )
        conn.commit()
        conn.close()
        with pytest.raises(FastPathUnavailable, match="occupied SQLite dest"):
            copy_oracle_to_sqlite(
                source_cfg=_ora_cfg(),
                source_schema="DATAFLOW",
                source_table=ora_mid,
                dest_cfg=_sqlite_cfg(dest_db, dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                sqlite_ddls=["INTEGER", "TEXT"],
                replace_destination=False,
            )
        assert _sqlite_count(dest_db, dest) == 2
    finally:
        _drop_ora(ora_mid)


def test_live_oracle_sqlite_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_ow_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_ow_dst"
    try:
        _seed_ora_from_sqlite(src, "src_t", ora_mid, 800)
        conn = sqlite3.connect(dest_db)
        conn.execute(
            f'CREATE TABLE "{dest}" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)'
        )
        conn.execute(f'INSERT INTO "{dest}" (id, label) VALUES (1, "ghost")')
        conn.commit()
        conn.close()
        result = copy_oracle_to_sqlite(
            source_cfg=_ora_cfg(),
            source_schema="DATAFLOW",
            source_table=ora_mid,
            dest_cfg=_sqlite_cfg(dest_db, dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            sqlite_ddls=["INTEGER", "TEXT"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("sqlite_write") == "overwrite"
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ora(ora_mid)


def test_live_oracle_sqlite_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_ORACLE_SQLITE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    ora_mid = f"ora_sqlite_stream_{tag}"
    dest_db = tmp_path / "dest.db"
    dest = "ora_sqlite_stream_dst"
    try:
        _seed_ora_from_sqlite(src, "src_t", ora_mid, 800)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-sqlite-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": ora_mid}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_sqlite_cfg(dest_db, dest), "format": "sqlite"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "NUMBER", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR2(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "NUMBER", "label": "VARCHAR2(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "select_oracle_executemany_sqlite"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("SQLite" in line for line in ddl_log)
        assert _sqlite_count(dest_db, dest) == 800
    finally:
        _drop_ora(ora_mid)
