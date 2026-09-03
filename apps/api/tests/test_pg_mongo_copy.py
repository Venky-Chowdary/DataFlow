"""PostgreSQL → MongoDB COPY text + insert_many — dest count_documents."""

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
from services.copy_pg_mongo import (  # noqa: E402
    copy_postgres_to_mongo,
    pg_mongo_copy_enabled,
    pg_mongo_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pg_cfg() -> dict:
    return {
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
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


def _pg_connect():
    _pg_or_skip()
    psycopg2 = pytest.importorskip("psycopg2")
    try:
        return psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            user="dataflow",
            password="dataflow",
            dbname="dataflow",
        )
    except Exception as exc:
        pytest.skip(f"PostgreSQL auth failed: {exc}")


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


def _seed_pg(cur, table: str, rows: int) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')
    cur.execute(
        f'CREATE TABLE public."{table}" ('
        "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
    )
    cur.execute(
        f'INSERT INTO public."{table}" (id, label) '
        f"SELECT g, 'r' || g::text FROM generate_series(1, {int(rows)}) g"
    )


def _drop_pg(cur, table: str) -> None:
    cur.execute(f'DROP TABLE IF EXISTS public."{table}"')


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


def test_pg_mongo_copy_safe_types():
    assert pg_mongo_type_is_copy_safe("BIGINT") is True
    assert pg_mongo_type_is_copy_safe("VARCHAR(32)") is True
    assert pg_mongo_type_is_copy_safe("DATE") is True
    assert pg_mongo_type_is_copy_safe("BOOLEAN") is True
    assert pg_mongo_type_is_copy_safe("JSONB") is False
    assert pg_mongo_type_is_copy_safe("BYTEA") is False
    assert pg_mongo_type_is_copy_safe("TIMESTAMPTZ") is False
    assert pg_mongo_type_is_copy_safe("TIMESTAMP") is False


def test_pg_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PG_MONGO_COPY", "0")
    assert pg_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_pg_mongo_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_PG_MONGO_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_src_{tag}"
    dest = f"pg_mongo_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _drop_mongo(dest)
        result = copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_null_{tag}"
    dest = f"pg_mongo_null_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            cur.execute(
                f'CREATE TABLE public."{src}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{src}" (id, label) VALUES '
                "(1, NULL), (2, ''), (3, 'x')"
            )
        pg.commit()
        _drop_mongo(dest)
        result = copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
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
        assert docs[1]["label"] == ""
        assert docs[2]["label"] == "x"
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_skip_{tag}"
    dest = f"pg_mongo_skip_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _drop_mongo(dest)
        first = copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_occ_{tag}"
    dest = f"pg_mongo_occ_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
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
            copy_postgres_to_mongo(
                source_cfg=_pg_cfg(),
                source_schema="public",
                source_table=src,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_ow_{tag}"
    dest = f"pg_mongo_ow_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_est_{tag}"
    dest = f"pg_mongo_est_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 80)
        pg.commit()
        _drop_mongo(dest)
        copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        assert _dest_count(dest) == 80
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()


def test_live_pg_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_PG_MONGO_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"pg_mongo_stream_{tag}"
    dest = f"pg_mongo_stream_dst_{tag}"
    try:
        with pg.cursor() as cur:
            _seed_pg(cur, src, 800)
        pg.commit()
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"pg-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": src}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "BIGINT", "transform": "none"},
            {
                "source": "label",
                "target": "label",
                "type": "VARCHAR(32)",
                "transform": "none",
            },
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "BIGINT", "label": "VARCHAR(32)"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "copy_text_pg_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
        pg.commit()
        _drop_mongo(dest)
        pg.close()
