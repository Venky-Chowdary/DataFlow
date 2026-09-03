"""MongoDB → Oracle snapshot find + executemany — dest COUNT(*)."""

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
from services.copy_mongo_oracle import (  # noqa: E402
    copy_mongo_to_oracle,
    mongo_oracle_copy_enabled,
)
from services.copy_mongo_pg import mongo_type_is_copy_safe  # noqa: E402
from services.copy_oracle_mongo import copy_oracle_to_mongo  # noqa: E402
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


def _mongo_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _ora_count(table: str) -> int:
    n = destination_row_count(
        "oracle", _ora_cfg(), schema="DATAFLOW", table_name=table
    )
    assert n is not None
    return int(n)


def _seed_mongo_from_ora(ora, src: str, mongo: str, rows: int) -> None:
    cur = ora.cursor()
    _seed_ora(cur, src, rows)
    ora.commit()
    _drop_mongo(mongo)
    result = copy_oracle_to_mongo(
        source_cfg=_ora_cfg(),
        source_table=src,
        dest_cfg=_mongo_cfg(mongo),
        dest_table=mongo,
        pairs=[("id", "id"), ("label", "label")],
        mongo_ddls=["NUMBER", "VARCHAR2(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _mongo_count(mongo) == rows


def test_mongo_oracle_copy_safe_types():
    assert mongo_type_is_copy_safe("string") is True
    assert mongo_type_is_copy_safe("long") is True
    assert mongo_type_is_copy_safe("NUMBER") is True
    assert mongo_type_is_copy_safe("object") is False
    assert mongo_type_is_copy_safe("array") is False
    assert mongo_type_is_copy_safe("bindata") is False
    assert mongo_type_is_copy_safe("timestamptz") is False
    from services.copy_oracle_mongo import oracle_mongo_type_is_copy_safe

    assert oracle_mongo_type_is_copy_safe("VARCHAR2(32)") is True
    assert oracle_mongo_type_is_copy_safe("NUMBER") is True


def test_mongo_oracle_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_ORACLE_COPY", "0")
    assert mongo_oracle_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_oracle(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table="NOPE",
            pairs=[("id", "id")],
            oracle_ddls=["NUMBER"],
            replace_destination=True,
        )


def test_live_mongo_oracle_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_ORACLE_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_SRC_{tag}"
    mid = f"mongo_ora_mid_{tag.lower()}"
    dest = f"MONGO_ORA_DST_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()
        result = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert _ora_count(dest) == 800
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_empty_string_as_null():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    dest = f"MONGO_ORA_NULL_{tag}"
    mid = f"mongo_ora_null_{tag.lower()}"
    try:
        _drop_mongo(mid)
        client, coll = _mongo_coll(mid)
        try:
            coll.insert_many(
                [
                    {"id": 1, "label": None},
                    {"id": 2, "label": ""},
                    {"id": 3, "label": "x"},
                ],
                ordered=False,
            )
        finally:
            client.close()
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()
        result = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
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
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_SKIP_{tag}"
    mid = f"mongo_ora_skip_{tag.lower()}"
    dest = f"MONGO_ORA_SKD_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()
        first = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _ora_count(dest) == 800
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_OCC_{tag}"
    mid = f"mongo_ora_occ_{tag.lower()}"
    dest = f"MONGO_ORA_OCD_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        cur.execute(
            f"CREATE TABLE {dest} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(f"INSERT INTO {dest} (ID, LABEL) VALUES (1, 'ghost'), (2, 'ghost')")
        ora.commit()
        with pytest.raises(FastPathUnavailable, match="occupied Oracle dest"):
            copy_mongo_to_oracle(
                source_cfg=_mongo_cfg(mid),
                source_table=mid,
                dest_cfg=_ora_cfg(),
                dest_schema="DATAFLOW",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                oracle_ddls=["NUMBER", "VARCHAR2(32)"],
                replace_destination=False,
            )
        assert _ora_count(dest) == 2
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_OW_{tag}"
    mid = f"mongo_ora_ow_{tag.lower()}"
    dest = f"MONGO_ORA_OWD_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        cur.execute(
            f"CREATE TABLE {dest} (ID NUMBER NOT NULL PRIMARY KEY, LABEL VARCHAR2(32))"
        )
        cur.execute(f"INSERT INTO {dest} (ID, LABEL) VALUES (1, 'ghost')")
        ora.commit()
        result = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        assert _ora_count(dest) == 800
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_source_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_EST_{tag}"
    mid = f"mongo_ora_est_{tag.lower()}"
    dest = f"MONGO_ORA_ESD_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 80)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo source COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_oracle(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_ora_cfg(),
            dest_schema="DATAFLOW",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            oracle_ddls=["NUMBER", "VARCHAR2(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _ora_count(dest) == 80
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()


def test_live_mongo_oracle_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_ORACLE_COPY", raising=False)
    ora = _ora_connect()
    tag = uuid.uuid4().hex[:8].upper()
    src = f"MONGO_ORA_STR_{tag}"
    mid = f"mongo_ora_str_{tag.lower()}"
    dest = f"MONGO_ORA_STD_{tag}"
    try:
        _seed_mongo_from_ora(ora, src, mid, 800)
        cur = ora.cursor()
        _drop_ora(cur, dest)
        ora.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-ora-copy-{tag.lower()}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(mid), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_ora_cfg(), "format": "oracle", "table": dest}
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
        assert summary.get("load_method") == "mongo_snapshot_find_executemany_oracle"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _ora_count(dest) == 800
    finally:
        cur = ora.cursor()
        _drop_ora(cur, src)
        _drop_ora(cur, dest)
        ora.commit()
        _drop_mongo(mid)
        ora.close()
