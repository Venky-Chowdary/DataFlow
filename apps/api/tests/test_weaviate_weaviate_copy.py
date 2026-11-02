"""Weaviate → Weaviate list+batch — dest ``meta.count``, never DISTINCT source_id."""

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
from services.copy_weaviate_common import (  # noqa: E402
    weaviate_family_name,
    weaviate_object_count,
    weaviate_type_is_copy_safe,
)
from services.copy_weaviate_weaviate import (  # noqa: E402
    copy_weaviate_to_weaviate,
    weaviate_weaviate_copy_enabled,
)


def _weaviate_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 8080), timeout=1):
            pass
    except OSError:
        pytest.skip("Weaviate 8080 not reachable")


def _weaviate_cfg(class_name: str) -> dict:
    return {
        "type": "weaviate",
        "format": "weaviate",
        "host": "127.0.0.1",
        "port": 8080,
        "database": class_name,
        "table": class_name,
    }


def test_weaviate_family_and_copy_safe_types():
    assert weaviate_family_name("weaviate") == "weaviate"
    assert weaviate_family_name("weaviate_cloud") == "weaviate"
    assert weaviate_type_is_copy_safe("text") is True
    assert weaviate_type_is_copy_safe("integer") is True
    assert weaviate_type_is_copy_safe("join") is False


def test_weaviate_weaviate_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", "0")
    assert weaviate_weaviate_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_weaviate_to_weaviate(
            source_cfg=_weaviate_cfg("MissingSrc"),
            source_table="MissingSrc",
            dest_cfg=_weaviate_cfg("MissingDst"),
            dest_table="MissingDst",
            pairs=[("content", "content")],
            weaviate_ddls=["text"],
            replace_destination=True,
        )


def test_weaviate_weaviate_same_class_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    cfg = _weaviate_cfg("SameClass")
    with pytest.raises(FastPathUnavailable, match="same class"):
        copy_weaviate_to_weaviate(
            source_cfg=cfg,
            source_table="SameClass",
            dest_cfg=cfg,
            dest_table="SameClass",
            pairs=[("content", "content")],
            weaviate_ddls=["text"],
            replace_destination=True,
        )


def test_weaviate_weaviate_public_proxy_declines():
    dest = {
        **_weaviate_cfg("B"),
        "host": "",
        "connection_string": "https://caboose.proxy.rlwy.net:8080",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_weaviate_to_weaviate(
            source_cfg=_weaviate_cfg("A"),
            source_table="A",
            dest_cfg=dest,
            dest_table="B",
            pairs=[("content", "content")],
            weaviate_ddls=["text"],
            replace_destination=True,
        )


def test_weaviate_weaviate_cross_endpoint_declines():
    src = _weaviate_cfg("A")
    dest = {**_weaviate_cfg("B"), "host": "10.0.0.99", "port": 8080}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_weaviate_to_weaviate(
            source_cfg=src,
            source_table="A",
            dest_cfg=dest,
            dest_table="B",
            pairs=[("content", "content")],
            weaviate_ddls=["text"],
            replace_destination=True,
        )


def test_weaviate_weaviate_column_rename_declines():
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_weaviate_to_weaviate(
            source_cfg=_weaviate_cfg("A"),
            source_table="A",
            dest_cfg=_weaviate_cfg("B"),
            dest_table="B",
            pairs=[("content", "other")],
            weaviate_ddls=["text"],
            replace_destination=True,
        )


def test_live_weaviate_weaviate_empty_dest_copy():
    _weaviate_or_skip()
    from connectors.weaviate_writer import write_mapped_rows

    tag = uuid.uuid4().hex[:8]
    src = f"Dfwsrc{tag}"
    dest = f"Dfwdst{tag}"
    try:
        result = write_mapped_rows(
            host="127.0.0.1",
            port=8080,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 31)],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "content", "target": "content"},
            ],
            column_types={"id": "STRING", "content": "STRING"},
            content_column="content",
            embedding_model="hash/4",
        )
        assert result.ok, result.error
        copy_result = copy_weaviate_to_weaviate(
            source_cfg=_weaviate_cfg(src),
            source_table=src,
            dest_cfg=_weaviate_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("content", "content")],
            weaviate_ddls=["text", "text"],
            replace_destination=True,
        )
        assert copy_result.source_rows == 30
        assert weaviate_object_count(_weaviate_cfg(dest), dest) == 30
        assert copy_result.source_snapshot.get("weaviate_read") == "list"
    finally:
        from services.copy_weaviate_common import weaviate_delete_class

        weaviate_delete_class(_weaviate_cfg(src), src)
        weaviate_delete_class(_weaviate_cfg(dest), dest)


def test_live_weaviate_weaviate_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_WEAVIATE_WEAVIATE_COPY", raising=False)
    _weaviate_or_skip()
    from connectors.weaviate_writer import write_mapped_rows
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    tag = uuid.uuid4().hex[:8]
    src = f"Dfwsrc{tag}"
    dest = f"Dfwdst{tag}"
    try:
        seeded = write_mapped_rows(
            host="127.0.0.1",
            port=8080,
            database="",
            username="",
            password="",
            schema="",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 16)],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "content", "target": "content"},
            ],
            column_types={"id": "STRING", "content": "STRING"},
            content_column="content",
            embedding_model="hash/4",
        )
        assert seeded.ok, seeded.error
        ensure_memory_job_store_if_mongo_down()
        job_id = f"weaviate-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_weaviate_cfg(src), "format": "weaviate"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_weaviate_cfg(dest), "format": "weaviate"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "TEXT", "transform": "none"},
            {"source": "content", "target": "content", "type": "TEXT", "transform": "none"},
        ]
        schema = {"id": "TEXT", "content": "TEXT"}
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="full_refresh_append",
            job_id=job_id,
        )
        assert transferred == 15
        assert summary.get("load_method") == "list_batch_upsert_weaviate_weaviate"
        assert weaviate_object_count(_weaviate_cfg(dest), dest) == 15
    finally:
        from services.copy_weaviate_common import weaviate_delete_class

        weaviate_delete_class(_weaviate_cfg(src), src)
        weaviate_delete_class(_weaviate_cfg(dest), dest)
