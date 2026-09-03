"""MongoDB → MongoDB snapshot find + insert_many — dest count_documents."""

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
from services.copy_mongo_mongo import (  # noqa: E402
    copy_mongo_to_mongo,
    mongo_mongo_copy_enabled,
    mongo_mongo_type_is_copy_safe,
)
from services.dest_precount import destination_row_count  # noqa: E402


def _mongo_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 27017), timeout=1):
            pass
    except OSError:
        pytest.skip("MongoDB 27017 not reachable")


def _mongo_cfg(collection: str) -> dict:
    return {
        "type": "mongodb",
        "host": "127.0.0.1",
        "port": 27017,
        "database": "dataflow",
        "table": collection,
        "collection": collection,
    }


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


def _seed_docs(name: str, rows: int) -> None:
    _drop_mongo(name)
    client, coll = _mongo_coll(name)
    try:
        coll.insert_many(
            [{"id": i, "label": f"r{i}"} for i in range(1, rows + 1)],
            ordered=False,
        )
    finally:
        client.close()


def test_mongo_mongo_copy_safe_types():
    assert mongo_mongo_type_is_copy_safe("string") is True
    assert mongo_mongo_type_is_copy_safe("long") is True
    assert mongo_mongo_type_is_copy_safe("object") is True
    assert mongo_mongo_type_is_copy_safe("array") is True
    assert mongo_mongo_type_is_copy_safe("bindata") is True
    assert mongo_mongo_type_is_copy_safe("javascript") is False
    assert mongo_mongo_type_is_copy_safe("regex") is False


def test_mongo_mongo_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MONGO_MONGO_COPY", "0")
    assert mongo_mongo_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_mongo_to_mongo(
            source_cfg=_mongo_cfg("missing"),
            source_table="missing",
            dest_cfg=_mongo_cfg("nope"),
            dest_table="nope",
            pairs=[("id", "id")],
            mongo_ddls=["long"],
            replace_destination=True,
        )


def test_mongo_mongo_same_collection_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MONGO_MONGO_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="same collection"):
        copy_mongo_to_mongo(
            source_cfg=_mongo_cfg("same_coll"),
            source_table="same_coll",
            dest_cfg=_mongo_cfg("same_coll"),
            dest_table="same_coll",
            pairs=[("id", "id")],
            mongo_ddls=["long"],
            replace_destination=True,
        )


def test_live_mongo_mongo_dest_count(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_MONGO_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_src_{tag}"
    dest = f"mongo_mongo_dst_{tag}"
    try:
        _seed_docs(src, 800)
        _drop_mongo(dest)
        result = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("mongo_write") == "insert"
        assert result.source_snapshot.get("mongo_read") == "snapshot_find"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_nested_identity():
    pytest.importorskip("pymongo")
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_nest_{tag}"
    dest = f"mongo_mongo_nest_dst_{tag}"
    try:
        _drop_mongo(src)
        _drop_mongo(dest)
        client, coll = _mongo_coll(src)
        try:
            coll.insert_one({"id": 1, "payload": {"a": 1, "b": ["x", "y"]}})
        finally:
            client.close()
        result = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("payload", "payload")],
            mongo_ddls=["long", "object"],
            replace_destination=True,
        )
        assert result.target_rows == 1
        client, dest_coll = _mongo_coll(dest)
        try:
            doc = dest_coll.find_one({"id": 1}, {"_id": 0})
        finally:
            client.close()
        assert doc == {"id": 1, "payload": {"a": 1, "b": ["x", "y"]}}
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_empty_string_and_null_preserved():
    pytest.importorskip("pymongo")
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_null_{tag}"
    dest = f"mongo_mongo_null_dst_{tag}"
    try:
        _drop_mongo(src)
        _drop_mongo(dest)
        client, coll = _mongo_coll(src)
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
        result = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        client, dest_coll = _mongo_coll(dest)
        try:
            docs = list(dest_coll.find({}, {"_id": 0}).sort("id", 1))
        finally:
            client.close()
        assert docs[0]["label"] is None
        assert docs[1]["label"] == ""
        assert docs[2]["label"] == "x"
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_skip_when_dest_count_matches():
    pytest.importorskip("pymongo")
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_skip_{tag}"
    dest = f"mongo_mongo_skip_dst_{tag}"
    try:
        _seed_docs(src, 800)
        _drop_mongo(dest)
        first = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_occupied_mismatch_declines():
    pytest.importorskip("pymongo")
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_occ_{tag}"
    dest = f"mongo_mongo_occ_dst_{tag}"
    try:
        _seed_docs(src, 800)
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
            copy_mongo_to_mongo(
                source_cfg=_mongo_cfg(src),
                source_table=src,
                dest_cfg=_mongo_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                mongo_ddls=["long", "string"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_overwrite_replaces_dest():
    pytest.importorskip("pymongo")
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_ow_{tag}"
    dest = f"mongo_mongo_ow_dst_{tag}"
    try:
        _seed_docs(src, 800)
        _drop_mongo(dest)
        client, coll = _mongo_coll(dest)
        try:
            coll.insert_one({"id": 1, "label": "ghost"})
        finally:
            client.close()
        result = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("mongo_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_dest_count_is_not_estimated(monkeypatch):
    pytest.importorskip("pymongo")
    from pymongo.collection import Collection

    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_est_{tag}"
    dest = f"mongo_mongo_est_dst_{tag}"
    try:
        _seed_docs(src, 80)
        _drop_mongo(dest)

        def _no_est(self, *args, **kwargs):
            raise AssertionError("Mongo dest COUNT must not estimatedDocumentCount")

        monkeypatch.setattr(Collection, "estimated_document_count", _no_est)
        result = copy_mongo_to_mongo(
            source_cfg=_mongo_cfg(src),
            source_table=src,
            dest_cfg=_mongo_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            mongo_ddls=["long", "string"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)


def test_live_mongo_mongo_stream_load_method(monkeypatch):
    pytest.importorskip("pymongo")
    monkeypatch.delenv("DATAFLOW_MONGO_MONGO_COPY", raising=False)
    tag = uuid.uuid4().hex[:8]
    src = f"mongo_mongo_stream_{tag}"
    dest = f"mongo_mongo_stream_dst_{tag}"
    try:
        _seed_docs(src, 800)
        _drop_mongo(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"mongo-mongo-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(src), "format": "mongodb"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_mongo_cfg(dest), "format": "mongodb"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "string", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "string"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "mongo_snapshot_find_insert_many_mongo"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("MongoDB" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop_mongo(src)
        _drop_mongo(dest)
