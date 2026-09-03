"""SQLite SELECT → Oracle executemany — dest COUNT(*)."""

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
from services.copy_sqlite_oracle import (  # noqa: E402
    copy_sqlite_to_oracle,
    sqlite_oracle_copy_enabled,
    sqlite_oracle_type_is_copy_safe,
    sqlite_value_to_oracle,
)
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


def _cfg(path: Path | str, table: str) -> dict:
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


def _seed(path: Path, table: str, rows: int) -> None:
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


def _dest_count(table: str) -> int:
    n = destination_row_count(
        "oracle", _ora_cfg(), schema="DATAFLOW", table_name=table
    )
    assert n is not None
    return int(n)


def test_sqlite_oracle_text_rematerializes_varchar2():
    from services.copy_sqlite_oracle import sqlite_declared_to_oracle_ddl

    assert sqlite_declared_to_oracle_ddl("TEXT") == "VARCHAR2(4000)"
    assert sqlite_declared_to_oracle_ddl("INTEGER").startswith("NUMBER")
    assert sqlite_declared_to_oracle_ddl("DATE") == "DATE"


def test_sqlite_oracle_copy_safe_types():
    assert sqlite_oracle_type_is_copy_safe("INTEGER") is True
    assert sqlite_oracle_type_is_copy_safe("TEXT") is True
    assert sqlite_oracle_type_is_copy_safe("DATE") is True
    assert sqlite_oracle_type_is_copy_safe("REAL") is True
    assert sqlite_oracle_type_is_copy_safe("BOOLEAN") is True
    assert sqlite_oracle_type_is_copy_safe("DATETIME") is False
    assert sqlite_oracle_type_is_copy_safe("TIMESTAMP") is False
    assert sqlite_oracle_type_is_copy_safe("JSON") is False
    assert sqlite_oracle_type_is_copy_safe("BLOB") is False


def test_sqlite_oracle_date_iso_bind():
    coerced = [0]
    assert sqlite_value_to_oracle("2020-01-02", "DATE", coerced) == date(2020, 1, 2)
    assert sqlite_value_to_oracle(date(2020, 1, 2), "DATE", coerced) == date(2020, 1, 2)
    assert sqlite_value_to_oracle("2020-01-02", "VARCHAR2(32)", coerced) == "2020-01-02"
    assert sqlite_value_to_oracle(None, "DATE", coerced) is None
    assert sqlite_value_to_oracle("", "VARCHAR2(32)", coerced) is None
    assert coerced[0] == 1
    with pytest.raises(FastPathUnavailable, match="not ISO"):
        sqlite_value_to_oracle("not-a-date", "DATE", [0])
    with pytest.raises(FastPathUnavailable, match="DATETIME"):
        sqlite_value_to_oracle("2020-01-02 12:00:00", "DATE", [0])
    with pytest.raises(FastPathUnavailable, match="BLOB"):
        sqlite_value_to_oracle(b"x", "VARCHAR2(32)", [0])


def test_sqlite_oracle_copy_kill_switch(monkeypatch, tmp_path):
    monkeypatch.setenv("DATAFLOW_SQLITE_ORACLE_COPY", "0")
    assert sqlite_oracle_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_sqlite_to_oracle(
            source_cfg=_cfg(tmp_path / "src.db", "missing"),
            source_table="missing",
            dest_cfg=_ora_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_sqlite_oracle_memory_declines():
    with pytest.raises(FastPathUnavailable, match=":memory:"):
        copy_sqlite_to_oracle(
            source_cfg=_cfg(":memory:", "orders"),
            source_table="orders",
            dest_cfg=_ora_cfg(),
            dest_table="missing",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_live_sqlite_oracle_dest_count(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_ORACLE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_dst_{tag}"
    _seed(src, "src_t", 800)
    try:
        result = copy_sqlite_to_oracle(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("oracle_write") == "insert"
        assert result.source_snapshot.get("sqlite_read") == "select"
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(dest)


def test_live_sqlite_oracle_empty_string_as_null(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_null_{tag}"
    conn = sqlite3.connect(src)
    conn.execute('CREATE TABLE "src_t" (id INTEGER NOT NULL PRIMARY KEY, label TEXT)')
    conn.executemany(
        'INSERT INTO "src_t" (id, label) VALUES (?, ?)',
        [(1, None), (2, ""), (3, "x")],
    )
    conn.commit()
    conn.close()
    ora = _ora_connect()
    try:
        result = copy_sqlite_to_oracle(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        assert int(result.source_snapshot.get("empty_string_as_null_cells") or 0) == 1
        cur = ora.cursor()
        cur.execute(f"SELECT id, label FROM {dest} ORDER BY id")
        rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] is None
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        ora.close()
        _drop_ora(dest)


def test_live_sqlite_oracle_skip_when_dest_count_matches(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_skip_{tag}"
    _seed(src, "src_t", 800)
    try:
        first = copy_sqlite_to_oracle(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_sqlite_to_oracle(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(dest)


def test_live_sqlite_oracle_occupied_mismatch_declines(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_occ_{tag}"
    _seed(src, "src_t", 800)
    ora = _ora_connect()
    try:
        cur = ora.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{dest} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        cur.execute(
            f"CREATE TABLE {dest} ("
            "id NUMBER NOT NULL PRIMARY KEY, label VARCHAR2(32) NULL)"
        )
        cur.execute(f"INSERT INTO {dest} (id, label) VALUES (1, 'g')")
        cur.execute(f"INSERT INTO {dest} (id, label) VALUES (2, 'g')")
        ora.commit()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Oracle dest"):
            copy_sqlite_to_oracle(
                source_cfg=_cfg(src, "src_t"),
                source_table="src_t",
                dest_cfg=_ora_cfg(),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        ora.close()
        _drop_ora(dest)


def test_live_sqlite_oracle_overwrite_replaces_dest(tmp_path):
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_ow_{tag}"
    _seed(src, "src_t", 800)
    ora = _ora_connect()
    try:
        cur = ora.cursor()
        cur.execute(
            "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
            f"{dest} PURGE'; EXCEPTION WHEN OTHERS THEN "
            "IF SQLCODE != -942 THEN RAISE; END IF; END;"
        )
        cur.execute(
            f"CREATE TABLE {dest} ("
            "id NUMBER NOT NULL PRIMARY KEY, label VARCHAR2(32) NULL)"
        )
        cur.execute(f"INSERT INTO {dest} (id, label) VALUES (1, 'ghost')")
        ora.commit()
        result = copy_sqlite_to_oracle(
            source_cfg=_cfg(src, "src_t"),
            source_table="src_t",
            dest_cfg=_ora_cfg(),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("oracle_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        ora.close()
        _drop_ora(dest)


def test_live_sqlite_oracle_stream_load_method(monkeypatch, tmp_path):
    monkeypatch.delenv("DATAFLOW_SQLITE_ORACLE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = tmp_path / "src.db"
    dest = f"sqlite_ora_str_{tag}"
    _seed(src, "src_t", 800)
    try:
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sqlite-ora-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_cfg(src, "src_t"), "format": "sqlite"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": dest}
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
        assert summary.get("load_method") == "select_sqlite_executemany_oracle"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Oracle" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(dest)
