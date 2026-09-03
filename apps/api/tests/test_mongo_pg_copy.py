"""MongoDB → PostgreSQL snapshot find + COPY FROM STDIN — dest COUNT(*)."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_mongo_pg import (  # noqa: E402
    copy_mongo_to_postgres,
    mongo_pg_copy_enabled,
    mongo_type_is_copy_safe,
)
from services.copy_pg_mongo import copy_postgres_to_mongo  # noqa: E402
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


def _mongo_count(name: str) -> int:
    n = destination_row_count(
        "mongodb", _mongo_cfg(name), schema="", table_name=name
    )
    assert n is not None
    return int(n)


def _seed_mongo_from_pg(pg, src: str, mongo: str, rows: int) -> None:
    with pg.cursor() as cur:
        _seed_pg(cur, src, rows)
    pg.commit()
    _drop_mongo(mongo)
    result = copy_postgres_to_mongo(
        source_cfg=_pg_cfg(),
        source_schema="public",
        source_table=src,
        dest_cfg=_mongo_cfg(mongo),
        dest_table=mongo,
        pairs=[("id", "id"), ("label", "label")],
        mongo_ddls=["BIGINT", "VARCHAR(32)"],
        replace_destination=True,
    )
    assert result.target_rows == rows
    assert _mongo_count(mongo) == rows


def test_mongo_pg_copy_safe_types():
    assert mongo_type_is_copy_safe("string") is True
    assert mongo_type_is_copy_safe("long") is True
    assert mongo_type_is_copy_safe("VARCHAR(32)") is True
    assert mongo_type_is_copy_safe("BIGINT") is True
    assert mongo_type_is_copy_safe("object") is False
    assert mongo_type_is_copy_safe("array") is False
    assert mongo_type_is_copy_safe("bindata") is False
    assert mongo_type_is_copy_safe("timestamptz") is False


def test_mongo_pg_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_PG_COPY", "0")
    assert mongo_pg_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_postgres(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table="nope",
            pairs=[("id", "id")],
            pg_ddls=["BIGINT"],
            replace_destination=True,
        )


def test_live_mongo_pg_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_PG_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_src_{tag}"
    mid = f"mongo_pg_mid_{tag}"
    dest = f"mongo_pg_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 800)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_null_{tag}"
    mid = f"mongo_pg_null_mid_{tag}"
    dest = f"mongo_pg_null_dst_{tag}"
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
        _drop_mongo(mid)
        copy_postgres_to_mongo(
            source_cfg=_pg_cfg(),
            source_schema="public",
            source_table=src,
            dest_cfg=_mongo_cfg(mid),
            dest_table=mid,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        result = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        with pg.cursor() as cur:
            cur.execute(f'SELECT id, label FROM public."{dest}" ORDER BY id')
            rows = list(cur.fetchall())
        assert int(rows[0][0]) == 1
        assert rows[0][1] is None
        assert int(rows[1][0]) == 2
        assert rows[1][1] == ""
        assert int(rows[2][0]) == 3
        assert str(rows[2][1]) == "x"
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_skip_{tag}"
    mid = f"mongo_pg_skip_mid_{tag}"
    dest = f"mongo_pg_skip_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 800)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        first = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_occ_{tag}"
    mid = f"mongo_pg_occ_mid_{tag}"
    dest = f"mongo_pg_occ_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 800)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
            cur.execute(
                f'CREATE TABLE public."{dest}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(
                f'INSERT INTO public."{dest}" (id, label) VALUES '
                "(1, 'ghost'), (2, 'ghost')"
            )
        pg.commit()
        with pytest.raises(FastPathUnavailable, match="occupied PostgreSQL dest"):
            copy_mongo_to_postgres(
                source_cfg=_mongo_cfg(mid),
                source_table=mid,
                dest_cfg=_pg_cfg(),
                dest_schema="public",
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                pg_ddls=["BIGINT", "VARCHAR(32)"],
                replace_destination=False,
            )
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 2
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_ow_{tag}"
    mid = f"mongo_pg_ow_mid_{tag}"
    dest = f"mongo_pg_ow_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 800)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
            cur.execute(
                f'CREATE TABLE public."{dest}" ('
                "id BIGINT NOT NULL PRIMARY KEY, label VARCHAR(32) NULL)"
            )
            cur.execute(f'INSERT INTO public."{dest}" (id, label) VALUES (1, \'ghost\')')
        pg.commit()
        result = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 800
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_source_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_est_{tag}"
    mid = f"mongo_pg_est_mid_{tag}"
    dest = f"mongo_pg_est_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 80)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo source COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_postgres(
            source_cfg=_mongo_cfg(mid),
            source_table=mid,
            dest_cfg=_pg_cfg(),
            dest_schema="public",
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            pg_ddls=["BIGINT", "VARCHAR(32)"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 80
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()


def test_live_mongo_pg_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_PG_COPY", raising=False)
    pg = _pg_connect()
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_pg_stream_{tag}"
    mid = f"mongo_pg_stream_mid_{tag}"
    dest = f"mongo_pg_stream_dst_{tag}"
    try:
        _seed_mongo_from_pg(pg, src, mid, 800)
        with pg.cursor() as cur:
            _drop_pg(cur, dest)
        pg.commit()
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-pg-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(mid), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_pg_cfg(), "format": "postgresql", "table": dest}
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
        assert summary.get("load_method") == "mongo_snapshot_find_copy_from_stdin_pg"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        with pg.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM public."{dest}"')
            assert int(cur.fetchone()[0]) == 800
    finally:
        with pg.cursor() as cur:
            _drop_pg(cur, src)
            _drop_pg(cur, dest)
        pg.commit()
        _drop_mongo(mid)
        pg.close()
