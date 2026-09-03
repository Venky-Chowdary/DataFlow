"""Snowflake → Snowflake CTAS / INSERT SELECT — dest COUNT(*)."""

from __future__ import annotations

import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("fakesnow", reason="requires the optional Snowflake test dependency")

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_snowflake_common import (  # noqa: E402
    snowflake_family_name,
    snowflake_type_is_copy_safe,
)
from services.copy_snowflake_snowflake import (  # noqa: E402
    copy_snowflake_to_snowflake,
    snowflake_snowflake_copy_enabled,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _sf_cfg(table: str) -> dict:
    return {
        "type": "snowflake",
        "format": "snowflake",
        "host": "localhost",
        "port": 443,
        "database": "dataflow",
        "schema": "public",
        "table": table,
        "username": "test",
        "password": "test",
    }


def _connect():
    from connectors.snowflake_conn import get_connection

    return get_connection(
        account="localhost",
        username="test",
        password="test",
        database="dataflow",
        schema="public",
        warehouse="",
        connection_string="",
    )


def _drop(name: str) -> None:
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f'DROP TABLE IF EXISTS "{name}"')
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def _dest_count(name: str) -> int:
    n = destination_row_count(
        "snowflake", _sf_cfg(name), schema="public", table_name=name
    )
    assert n is not None
    return int(n)


def _seed(name: str, rows: int) -> None:
    _drop(name)
    conn = _connect()
    try:
        cur = conn.cursor()
        cur.execute(f'CREATE TABLE "{name}" (id INTEGER, label VARCHAR)')
        batch = [(i, f"r{i}") for i in range(1, rows + 1)]
        cur.executemany(f'INSERT INTO "{name}" (id, label) VALUES (%s, %s)', batch)
        try:
            conn.commit()
        except Exception:
            pass
    finally:
        conn.close()


def test_snowflake_family_and_copy_safe_types():
    assert snowflake_family_name("snowflake") == "snowflake"
    assert snowflake_family_name("snowflake_aws") == "snowflake"
    assert snowflake_family_name("snowflake_azure") == "snowflake"
    assert snowflake_family_name("snowflake_gcp") == "snowflake"
    assert snowflake_family_name("snowflake_standard") == "snowflake"
    assert snowflake_family_name("snowflake_enterprise") == "snowflake"
    assert snowflake_type_is_copy_safe("NUMBER") is True
    assert snowflake_type_is_copy_safe("INTEGER") is True
    assert snowflake_type_is_copy_safe("VARCHAR(16777216)") is True
    assert snowflake_type_is_copy_safe("string") is True
    assert snowflake_type_is_copy_safe("VARIANT") is True
    assert snowflake_type_is_copy_safe("TIMESTAMP_NTZ") is True
    assert snowflake_type_is_copy_safe("GEOGRAPHY") is False
    assert snowflake_type_is_copy_safe("GEOMETRY") is False
    assert snowflake_type_is_copy_safe("VECTOR") is False


def test_snowflake_snowflake_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", "0")
    assert snowflake_snowflake_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg("missing"),
            source_table="missing",
            dest_cfg=_sf_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            snowflake_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_snowflake_snowflake_same_table_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", raising=False)
    cfg = _sf_cfg("same_table")
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_snowflake_to_snowflake(
            source_cfg=cfg,
            source_table="same_table",
            dest_cfg=cfg,
            dest_table="same_table",
            pairs=[("id", "id")],
            snowflake_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_snowflake_snowflake_public_proxy_declines():
    dest = {
        **_sf_cfg("b"),
        "host": "proxy.rlwy.net",
        "connection_string": "snowflake://test:test@proxy.rlwy.net/dataflow/public",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            snowflake_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_snowflake_snowflake_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg("a"),
            source_table="a",
            dest_cfg=_sf_cfg("b"),
            dest_table="b",
            pairs=[("id", "emp_id")],
            snowflake_ddls=["INTEGER"],
            replace_destination=True,
        )


def test_live_snowflake_snowflake_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_src_{tag}"
    dest = f"sf_copy_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        result = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("snowflake_read") == "insert_select"
        assert result.source_snapshot.get("snowflake_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_not_copy_into(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_into_{tag}"
    dest = f"sf_copy_into_dst_{tag}"
    _seed(src, 80)
    _drop(dest)

    from services import copy_snowflake_snowflake as mod

    orig_connect = mod.snowflake_connect

    def _wrap_connect(cfg):
        conn = orig_connect(cfg)
        orig_cursor = conn.cursor

        def cursor(*a, **k):
            cur = orig_cursor(*a, **k)
            orig_ex = cur.execute

            def execute(sql, *args, **kwargs):
                text = str(sql).upper()
                if "COPY INTO" in text:
                    raise AssertionError("Snowflake identity COPY must not COPY INTO")
                if " CLONE " in f" {text} ":
                    raise AssertionError("Snowflake identity COPY must not CLONE")
                if text.lstrip().startswith("MERGE "):
                    raise AssertionError("Snowflake identity COPY must not MERGE")
                return orig_ex(sql, *args, **kwargs)

            cur.execute = execute
            return cur

        conn.cursor = cursor
        return conn

    monkeypatch.setattr(mod, "snowflake_connect", _wrap_connect)
    try:
        result = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("snowflake_read") == "insert_select"
        assert _dest_count(dest) == 80
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_empty_string_and_null_preserved():
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_null_{tag}"
    dest = f"sf_copy_null_dst_{tag}"
    try:
        _drop(src)
        _drop(dest)
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(f'CREATE TABLE "{src}" (id INTEGER, label VARCHAR)')
            cur.execute(
                f"""INSERT INTO "{src}" (id, label) VALUES
                    (1, NULL), (2, ''), (3, 'x')"""
            )
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()
        result = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(f'SELECT id, label FROM "{dest}" ORDER BY id')
            rows = list(cur.fetchall())
        finally:
            conn.close()
        assert rows[0][1] is None
        assert rows[1][1] == ""
        assert rows[2][1] == "x"
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_skip_when_dest_count_matches():
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_skip_{tag}"
    dest = f"sf_copy_skip_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        first = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_occupied_mismatch_declines():
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_occ_{tag}"
    dest = f"sf_copy_occ_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(f'CREATE TABLE "{dest}" (id INTEGER, label VARCHAR)')
            cur.executemany(
                f'INSERT INTO "{dest}" (id, label) VALUES (%s, %s)',
                [(1, "ghost"), (2, "ghost")],
            )
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Snowflake dest"):
            copy_snowflake_to_snowflake(
                source_cfg=_sf_cfg(src),
                source_table=src,
                dest_cfg=_sf_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                snowflake_ddls=["INTEGER", "VARCHAR"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_overwrite_replaces_dest():
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_ow_{tag}"
    dest = f"sf_copy_ow_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        conn = _connect()
        try:
            cur = conn.cursor()
            cur.execute(f'CREATE TABLE "{dest}" (id INTEGER, label VARCHAR)')
            cur.execute(f"""INSERT INTO "{dest}" (id, label) VALUES (1, 'ghost')""")
            try:
                conn.commit()
            except Exception:
                pass
        finally:
            conn.close()
        result = copy_snowflake_to_snowflake(
            source_cfg=_sf_cfg(src),
            source_table=src,
            dest_cfg=_sf_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            snowflake_ddls=["INTEGER", "VARCHAR"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("snowflake_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)


def test_live_snowflake_snowflake_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_SNOWFLAKE_SNOWFLAKE_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"sf_copy_stream_{tag}"
    dest = f"sf_copy_stream_dst_{tag}"
    try:
        _seed(src, 800)
        _drop(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"sf-sf-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict("database", _sf_cfg(src))
        destination = EndpointConfig.from_dict("database", _sf_cfg(dest))
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "VARCHAR", "transform": "none"},
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
        assert summary.get("load_method") == "insert_select_snowflake_snowflake"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("Snowflake" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop(src)
        _drop(dest)
