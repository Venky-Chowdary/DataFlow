"""Oracle → MongoDB SHARE-lock SELECT + insert_many — dest count_documents."""

from __future__ import annotations

import os
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_oracle_mongo import (  # noqa: E402
    copy_oracle_to_mongo,
    oracle_mongo_copy_enabled,
    oracle_mongo_type_is_copy_safe,
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


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _ora_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 1521), timeout=1):
            pass
    except OSError:
        pytest.skip("Oracle 1521 not reachable")


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


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
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


def _mongo_coll(name: str):
    _mongo_or_skip()
    pymongo = pytest.importorskip("pymongo")
    client = pymongo.MongoClient("mongodb://127.0.0.1:27017", serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except Exception as exc:
        client.close()
        pytest.skip(f"MongoDB ping failed: {exc}")
    return client, client["dataflow"][name]


def _drop_ora(cur, table: str) -> None:
    cur.execute(
        "BEGIN EXECUTE IMMEDIATE 'DROP TABLE "
        f"{table} PURGE'; EXCEPTION WHEN OTHERS THEN "
        "IF SQLCODE != -942 THEN RAISE; END IF; END;"
    )


def _seed_ora(cur, table: str, rows: int) -> None:
    _drop_ora(cur, table)
    cur.execute(
        f"CREATE TABLE {table} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
    )
    cur.execute(
        f"INSERT INTO {table} (ID, LABEL) "
        f"SELECT LEVEL, 'r' || LEVEL FROM dual CONNECT BY LEVEL <= {int(rows)}"
    )


def _drop_mongo(name: str) -> None:
    client, coll = _mongo_coll(name)
    try:
        coll.drop()
    finally:
        client.close()


def _dest_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def test_oracle_mongo_copy_safe_types():
    assert oracle_mongo_type_is_copy_safe("NUMBER") is True
    assert oracle_mongo_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_mongo_type_is_copy_safe("DATE") is True
    assert oracle_mongo_type_is_copy_safe("BLOB") is False
    assert oracle_mongo_type_is_copy_safe("CLOB") is False
    assert oracle_mongo_type_is_copy_safe("XMLTYPE") is False
    assert oracle_mongo_type_is_copy_safe("TIMESTAMP") is False
    assert oracle_mongo_type_is_copy_safe("TIMESTAMP(6)") is False
    assert oracle_mongo_type_is_copy_safe("TIMESTAMP WITH TIME ZONE") is False


def test_oracle_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ORACLE_MONGO_COPY", "0")
    assert oracle_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_live_oracle_mongo_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_ORACLE_MONGO_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_SRC_{tag}"
    dest = f"ora_mongo_dst_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_mongo(dest)
        result = copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert result.source_snapshot.get("oracle_lock") == "share"
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_varchar2_empty_is_null():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_NULL_{tag}"
    dest = f"ora_mongo_null_{tag.lower()}"
    try:
        cur = ora.cursor()
        _drop_ora(cur, src)
        cur.execute(
            f"CREATE TABLE {src} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(
            f"INSERT INTO {src} (ID, LABEL) "
            "SELECT 1, NULL FROM dual UNION ALL "
            "SELECT 2, '' FROM dual UNION ALL "
            "SELECT 3, 'x' FROM dual"
        )
        ora.commit()
        _drop_mongo(dest)
        result = copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        client, coll = _mongo_coll(dest)
        try:
            docs = list(coll.find({}, {"_id": 0}).sort("id", 1))
        finally:
            client.close()
        assert docs[0]["label"] is None
        assert docs[1]["label"] is None
        assert docs[2]["label"] == "x"
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_SKIP_{tag}"
    dest = f"ora_mongo_skip_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_mongo(dest)
        first = copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_OCC_{tag}"
    dest = f"ora_mongo_occ_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_many(
                [{"id": 1, "label": "ghost"}, {"id": 2, "label": "ghost"}],
                ordered=False,
            )
        finally:
            client.close()
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Mongo dest"):
            copy_oracle_to_mongo(
                source_cfg=_ora_cfg(),
                source_table=src,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_OW_{tag}"
    dest = f"ora_mongo_ow_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_EST_{tag}"
    dest = f"ora_mongo_est_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 80)
        ora.commit()
        _drop_mongo(dest)
        copy_oracle_to_mongo(
            source_cfg=_ora_cfg(),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        assert _dest_count(dest) == 80
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()


def test_live_oracle_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_ORACLE_MONGO_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"ORA_MONGO_STR_{tag}"
    dest = f"ora_mongo_str_{tag.lower()}"
    try:
        cur = ora.cursor()
        _seed_ora(cur, src, 800)
        ora.commit()
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"ora-mongo-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
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
        assert summary.get("load_method") == "select_oracle_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_ora(ora.cursor(), src)
        ora.commit()
        _drop_mongo(dest)
        ora.close()
