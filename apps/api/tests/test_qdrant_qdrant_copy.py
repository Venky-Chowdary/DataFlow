"""Qdrant → Qdrant scroll+upsert — dest ``points_count``, never DISTINCT source_id."""

from __future__ import annotations

import json
import socket
import sys
import uuid
from pathlib import Path

import pytest

_API_ROOT = Path(__file__).resolve().parents[1]
if str(_API_ROOT) not in sys.path:
    sys.path.insert(0, str(_API_ROOT))

from services.copy_fast_path import FastPathUnavailable  # noqa: E402
from services.copy_qdrant_common import (  # noqa: E402
    qdrant_family_name,
    qdrant_points_count,
    qdrant_type_is_copy_safe,
)
from services.copy_qdrant_qdrant import (  # noqa: E402
    copy_qdrant_to_qdrant,
    qdrant_qdrant_copy_enabled,
)


def _qdrant_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 6333), timeout=1):
            pass
    except OSError:
        pytest.skip("Qdrant 6333 not reachable")


def _qdrant_cfg(collection: str) -> dict:
    return {
        "type": "qdrant",
        "format": "qdrant",
        "host": "127.0.0.1",
        "port": 6333,
        "database": collection,
        "table": collection,
    }


def _session():
    _qdrant_or_skip()
    from connectors.qdrant_writer import qdrant_rest

    return qdrant_rest(_qdrant_cfg("probe"))


def _dest_count(collection: str) -> int:
    return qdrant_points_count(_qdrant_cfg(collection), collection)


def _seed(collection: str, rows: int) -> None:
    session, base_url, headers = _session()
    session.delete(f"{base_url}/collections/{collection}", headers=headers, timeout=15)
    create = session.put(
        f"{base_url}/collections/{collection}",
        data=json.dumps({"vectors": {"size": 4, "distance": "Cosine"}}),
        headers=headers,
        timeout=15,
    )
    assert create.status_code in {200, 201}, create.text
    points = [
        {
            "id": i,
            "vector": [0.1 * (i % 5), 0.2, 0.3, 0.4],
            "payload": {"id": i, "source_id": f"doc-{i}", "label": f"r{i}"},
        }
        for i in range(1, rows + 1)
    ]
    for start in range(0, len(points), 100):
        batch = points[start : start + 100]
        resp = session.put(
            f"{base_url}/collections/{collection}/points?wait=true",
            data=json.dumps({"points": batch}),
            headers=headers,
            timeout=30,
        )
        assert resp.status_code in {200, 201}, resp.text


def _drop(collection: str) -> None:
    session, base_url, headers = _session()
    session.delete(f"{base_url}/collections/{collection}", headers=headers, timeout=15)


def test_qdrant_family_and_copy_safe_types():
    assert qdrant_family_name("qdrant") == "qdrant"
    assert qdrant_family_name("qdrant_cloud") == "qdrant"
    assert qdrant_type_is_copy_safe("keyword") is True
    assert qdrant_type_is_copy_safe("integer") is True
    assert qdrant_type_is_copy_safe("join") is False


def test_qdrant_qdrant_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_QDRANT_QDRANT_COPY", "0")
    assert qdrant_qdrant_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_qdrant_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            qdrant_ddls=["integer"],
            replace_destination=True,
        )


def test_qdrant_qdrant_same_collection_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_QDRANT_QDRANT_COPY", raising=False)
    cfg = _qdrant_cfg("same_coll")
    with pytest.raises(FastPathUnavailable, match="same collection"):
        copy_qdrant_to_qdrant(
            source_cfg=cfg,
            source_table="same_coll",
            dest_cfg=cfg,
            dest_table="same_coll",
            pairs=[("id", "id")],
            qdrant_ddls=["integer"],
            replace_destination=True,
        )


def test_qdrant_qdrant_public_proxy_declines():
    dest = {
        **_qdrant_cfg("b"),
        "host": "",
        "connection_string": "https://caboose.proxy.rlwy.net:6333",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            qdrant_ddls=["integer"],
            replace_destination=True,
        )


def test_qdrant_qdrant_cross_endpoint_declines():
    src = _qdrant_cfg("a")
    dest = {**_qdrant_cfg("b"), "host": "10.0.0.99", "port": 6333}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_qdrant_to_qdrant(
            source_cfg=src,
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            qdrant_ddls=["integer"],
            replace_destination=True,
        )


def test_qdrant_qdrant_column_rename_declines():
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg("a"),
            source_table="a",
            dest_cfg=_qdrant_cfg("b"),
            dest_table="b",
            pairs=[("id", "other")],
            qdrant_ddls=["integer"],
            replace_destination=True,
        )


def test_live_qdrant_qdrant_empty_dest_copy(monkeypatch):
    monkeypatch.delenv("DATAFLOW_QDRANT_QDRANT_COPY", raising=False)
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        _seed(src, 80)
        _drop(dest)
        result = copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg(src),
            source_table=src,
            dest_cfg=_qdrant_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            qdrant_ddls=["integer", "keyword"],
            replace_destination=True,
        )
        assert result.source_rows == 80
        assert result.target_rows == 80
        assert _dest_count(dest) == 80
        assert result.source_snapshot.get("qdrant_read") == "scroll"
        assert result.source_snapshot.get("qdrant_write") == "insert"
    finally:
        _drop(src)
        _drop(dest)


def test_live_qdrant_qdrant_skip_complete():
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        _seed(src, 50)
        _seed(dest, 50)
        result = copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg(src),
            source_table=src,
            dest_cfg=_qdrant_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            qdrant_ddls=["integer", "keyword"],
            replace_destination=False,
        )
        assert result.source_snapshot.get("copy_split") == "skip"
        assert _dest_count(dest) == 50
    finally:
        _drop(src)
        _drop(dest)


def test_live_qdrant_qdrant_occupied_mismatch_declines():
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        _seed(src, 80)
        _seed(dest, 2)
        assert _dest_count(dest) == 2
        with pytest.raises(FastPathUnavailable, match="occupied Qdrant dest"):
            copy_qdrant_to_qdrant(
                source_cfg=_qdrant_cfg(src),
                source_table=src,
                dest_cfg=_qdrant_cfg(dest),
                dest_table=dest,
                pairs=[("id", "id"), ("label", "label")],
                qdrant_ddls=["integer", "keyword"],
                replace_destination=False,
            )
        assert _dest_count(dest) == 2
    finally:
        _drop(src)
        _drop(dest)


def test_live_qdrant_qdrant_overwrite_replaces_dest():
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        _seed(src, 80)
        _seed(dest, 1)
        result = copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg(src),
            source_table=src,
            dest_cfg=_qdrant_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            qdrant_ddls=["integer", "keyword"],
            replace_destination=True,
        )
        assert result.source_snapshot.get("qdrant_write") == "overwrite"
        assert _dest_count(dest) == 80
    finally:
        _drop(src)
        _drop(dest)


def test_live_qdrant_qdrant_preserves_vectors_and_payload():
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        session, base_url, headers = _session()
        session.delete(f"{base_url}/collections/{src}", headers=headers, timeout=15)
        session.put(
            f"{base_url}/collections/{src}",
            data=json.dumps({"vectors": {"size": 4, "distance": "Cosine"}}),
            headers=headers,
            timeout=15,
        )
        special = {
            "id": 42,
            "vector": [0.9, 0.8, 0.7, 0.6],
            "payload": {"id": 42, "source_id": "doc-42", "label": "keep-me", "nested": {"a": 1}},
        }
        session.put(
            f"{base_url}/collections/{src}/points?wait=true",
            data=json.dumps({"points": [special]}),
            headers=headers,
            timeout=15,
        )
        src_scroll = session.post(
            f"{base_url}/collections/{src}/points/scroll",
            data=json.dumps({"limit": 10, "with_vectors": True, "with_payload": True}),
            headers=headers,
            timeout=15,
        ).json()["result"]["points"][0]
        _drop(dest)
        copy_qdrant_to_qdrant(
            source_cfg=_qdrant_cfg(src),
            source_table=src,
            dest_cfg=_qdrant_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("label", "label")],
            qdrant_ddls=["integer", "keyword"],
            replace_destination=True,
        )
        resp = session.post(
            f"{base_url}/collections/{dest}/points/scroll",
            data=json.dumps({"limit": 10, "with_vectors": True, "with_payload": True}),
            headers=headers,
            timeout=15,
        )
        body = resp.json()
        points = body["result"]["points"]
        assert len(points) == 1
        # Cosine collections store normalized vectors — identity is post-storage bytes.
        assert points[0]["vector"] == src_scroll["vector"]
        assert points[0]["payload"]["label"] == "keep-me"
        assert points[0]["payload"]["nested"] == {"a": 1}
    finally:
        _drop(src)
        _drop(dest)


def test_live_qdrant_qdrant_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_QDRANT_QDRANT_COPY", raising=False)
    _qdrant_or_skip()
    tag = uuid.uuid4().hex[:8]
    src = f"dfqsrc{tag}"
    dest = f"dfqdst{tag}"
    try:
        _seed(src, 80)
        _drop(dest)
        from services.million_row_proof import ensure_memory_job_store_if_mongo_down
        from services.mongodb_service import get_mongodb_service
        from src.transfer.models import EndpointConfig
        from src.transfer.stream import stream_database_transfer

        ensure_memory_job_store_if_mongo_down()
        job_id = f"qdrant-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_qdrant_cfg(src), "format": "qdrant"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_qdrant_cfg(dest), "format": "qdrant"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "INTEGER", "transform": "none"},
            {"source": "label", "target": "label", "type": "KEYWORD", "transform": "none"},
        ]
        schema = {"id": "INTEGER", "label": "KEYWORD"}
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="full_refresh_append",
            job_id=job_id,
        )
        assert transferred == 80
        assert summary.get("load_method") == "scroll_upsert_points_qdrant_qdrant"
        assert summary.get("qdrant_read") == "scroll"
        assert _dest_count(dest) == 80
    finally:
        _drop(src)
        _drop(dest)
