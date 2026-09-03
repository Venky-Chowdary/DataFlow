"""Elasticsearch → Elasticsearch ``_reindex`` — dest ``_count``, never ``docs.count``."""

from __future__ import annotations

import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_elasticsearch_common import (  # noqa: E402
    elasticsearch_family_name,
    elasticsearch_type_is_copy_safe,
)
from services.copy_elasticsearch_elasticsearch import (  # noqa: E402
    copy_elasticsearch_to_elasticsearch,
    elasticsearch_elasticsearch_copy_enabled,
)
from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.dest_precount import destination_row_count  # noqa: E402


def _es_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 9200), timeout=1):
            pass
    except OSError:
        pytest.skip("Elasticsearch 9200 not reachable")


def _es_cfg(index: str) -> dict:
    return {
        "type": "elasticsearch",
        "format": "elasticsearch",
        "host": "127.0.0.1",
        "port": 9200,
        "database": index,
        "table": index,
        "connection_string": "http://127.0.0.1:9200",
    }


def _client():
    _es_or_skip()
    pytest.importorskip("elasticsearch")
    from connectors.elasticsearch_reader import _client

    client = _client(_es_cfg("probe"))
    try:
        client.info()
    except Exception as exc:
        pytest.skip(f"Elasticsearch unavailable: {exc}")
    return client


def _dest_count(index: str) -> int:
    n = destination_row_count(
        "elasticsearch", _es_cfg(index), schema="", table_name=index
    )
    assert n is not None
    return int(n)


def _seed(client, index: str, rows: int) -> None:
    from elasticsearch.helpers import bulk

    if client.indices.exists(index=index):
        client.indices.delete(index=index)
    client.indices.create(
        index=index,
        mappings={
            "properties": {
                "id": {"type": "long"},
                "label": {"type": "keyword"},
            }
        },
    )
    actions = [
        {
            "_index": index,
            "_id": str(i),
            "_source": {"id": i, "label": f"r{i}" if i != 2 else ""},
        }
        for i in range(1, rows + 1)
    ]
    bulk(client, actions, refresh="wait_for")


def _drop(client, index: str) -> None:
    if client.indices.exists(index=index):
        client.indices.delete(index=index)


def test_elasticsearch_family_and_copy_safe_types():
    assert elasticsearch_family_name("opensearch") == "elasticsearch"
    assert elasticsearch_family_name("elastic_cloud") == "elasticsearch"
    assert elasticsearch_type_is_copy_safe("keyword") is True
    assert elasticsearch_type_is_copy_safe("long") is True
    assert elasticsearch_type_is_copy_safe("nested") is True
    assert elasticsearch_type_is_copy_safe("join") is False


def test_elasticsearch_elasticsearch_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", "0")
    assert elasticsearch_elasticsearch_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_es_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            elasticsearch_ddls=["long"],
            replace_destination=True,
        )


def test_elasticsearch_elasticsearch_same_index_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    cfg = _es_cfg("same_index")
    with pytest.raises(FastPathUnavailable, match="same index"):
        copy_elasticsearch_to_elasticsearch(
            source_cfg=cfg,
            source_table="same_index",
            dest_cfg=cfg,
            dest_table="same_index",
            pairs=[("id", "id")],
            elasticsearch_ddls=["long"],
            replace_destination=True,
        )


def test_elasticsearch_elasticsearch_public_proxy_declines():
    dest = {
        **_es_cfg("b"),
        "host": "",
        "connection_string": "https://caboose.proxy.rlwy.net:9200",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            elasticsearch_ddls=["long"],
            replace_destination=True,
        )


def test_elasticsearch_elasticsearch_cross_endpoint_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    dest = {**_es_cfg("b"), "port": 9201, "connection_string": "http://127.0.0.1:9201"}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            elasticsearch_ddls=["long"],
            replace_destination=True,
        )


def test_elasticsearch_elasticsearch_column_rename_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg("a"),
            source_table="a",
            dest_cfg=_es_cfg("b"),
            dest_table="b",
            pairs=[("id", "user_id")],
            elasticsearch_ddls=["long"],
            replace_destination=True,
        )


def test_live_elasticsearch_elasticsearch_dest_count(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _seed(client, src, 800)
        _drop(client, dest)
        result = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=True,
        )
        assert result.source_rows == 800
        assert result.target_rows == 800
        assert result.source_snapshot.get("copy_split") == "serial"
        assert result.source_snapshot.get("elasticsearch_read") == "reindex"
        assert result.source_snapshot.get("elasticsearch_write") == "insert"
        assert _dest_count(dest) == 800
        assert _dest_count(src) == 800
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_copy_is_not_bulk(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    _seed(client, src, 80)
    _drop(client, dest)
    import elasticsearch.helpers as helpers
    from elasticsearch import Elasticsearch

    orig_bulk_helper = helpers.bulk
    orig_scan = helpers.scan
    orig_index = Elasticsearch.index
    orig_bulk = Elasticsearch.bulk

    def _no_bulk(*a, **k):
        raise AssertionError("Elasticsearch→Elasticsearch COPY must not bulk/scroll")

    monkeypatch.setattr(helpers, "bulk", _no_bulk)
    monkeypatch.setattr(helpers, "scan", _no_bulk)
    monkeypatch.setattr(Elasticsearch, "index", _no_bulk)
    monkeypatch.setattr(Elasticsearch, "bulk", _no_bulk)
    try:
        result = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert result.source_snapshot.get("elasticsearch_read") == "reindex"
        assert _dest_count(dest) == 80
    finally:
        monkeypatch.setattr(helpers, "bulk", orig_bulk_helper)
        monkeypatch.setattr(helpers, "scan", orig_scan)
        monkeypatch.setattr(Elasticsearch, "index", orig_index)
        monkeypatch.setattr(Elasticsearch, "bulk", orig_bulk)
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_empty_string_and_null_preserved():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _drop(client, src)
        _drop(client, dest)
        client.indices.create(
            index=src,
            mappings={"properties": {"id": {"type": "long"}, "label": {"type": "keyword"}}},
        )
        client.index(index=src, id="1", document={"id": 1, "label": None}, refresh="wait_for")
        client.index(index=src, id="2", document={"id": 2, "label": ""}, refresh="wait_for")
        client.index(index=src, id="3", document={"id": 3, "label": "x"}, refresh="wait_for")
        result = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=True,
        )
        assert result.source_rows == 3
        assert _dest_count(dest) == 3
        got = {
            hit["_id"]: hit["_source"]
            for hit in client.search(index=dest, size=10)["hits"]["hits"]
        }
        assert got["2"]["label"] == ""
        assert got["3"]["label"] == "x"
        assert got["1"].get("label") in {None, ""}
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_skip_when_dest_count_matches():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _seed(client, src, 800)
        _drop(client, dest)
        first = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=False,
        )
        assert first.target_rows == 800
        second = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=False,
        )
        assert second.source_snapshot.get("copy_split") == "skip"
        assert second.source_snapshot.get("partitions_skipped") == 1
        assert _dest_count(dest) == 800
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_occupied_mismatch_declines():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _seed(client, src, 800)
        _seed(client, dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Elasticsearch dest"):
            copy_elasticsearch_to_elasticsearch(
                source_cfg=_es_cfg(src),
                source_table=src,
                dest_cfg=_es_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                elasticsearch_ddls=["long", "keyword"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_overwrite_replaces_dest():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _seed(client, src, 800)
        _seed(client, dest, 1)
        result = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("elasticsearch_write") == "overwrite"
        assert _dest_count(dest) == 800
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()


def test_live_elasticsearch_elasticsearch_dest_count_is_not_cat_docs():
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    other = f"dfeoth{tag}"
    try:
        _seed(client, src, 80)
        _seed(client, other, 50)
        _drop(client, dest)
        cat = client.cat.indices(format="json")
        cluster_docs = sum(int(row.get("docs.count") or 0) for row in cat)
        assert cluster_docs >= 130
        result = copy_elasticsearch_to_elasticsearch(
            source_cfg=_es_cfg(src),
            source_table=src,
            dest_cfg=_es_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            elasticsearch_ddls=["long", "keyword"],
            replace_destination=True,
        )
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
        assert _dest_count(dest) != cluster_docs
        assert _dest_count(other) == 50
    finally:
        _drop(client, src)
        _drop(client, dest)
        _drop(client, other)
        client.close()


def test_live_elasticsearch_elasticsearch_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_ELASTICSEARCH_ELASTICSEARCH_COPY", raising=False)
    client = _client()
    tag = uuid.uuid4().hex[:8]
    src = f"dfesrc{tag}"
    dest = f"dfedst{tag}"
    try:
        _seed(client, src, 800)
        _drop(client, dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"es-es-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_es_cfg(src), "format": "elasticsearch"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_es_cfg(dest), "format": "elasticsearch"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "long", "transform": "none"},
            {"source": "label", "target": "label", "type": "keyword", "transform": "none"},
        ]
        transferred, ddl_log, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            {"id": "long", "label": "keyword"},
            sync_mode="full_refresh_overwrite",
            job_id=job_id,
        )
        assert transferred == 800
        assert summary.get("load_method") == "reindex_elasticsearch_elasticsearch"
        assert summary.get("source_row_count") == 800
        assert int(summary.get("rejected_rows") or 0) == 0
        assert any("reindex" in line.lower() or "Elasticsearch" in line for line in ddl_log)
        assert _dest_count(dest) == 800
    finally:
        _drop(client, src)
        _drop(client, dest)
        client.close()
