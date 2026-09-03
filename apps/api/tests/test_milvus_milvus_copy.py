"""Milvus → Milvus query+upsert — dest ``count(*)``, never DISTINCT source_id."""

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
from services.copy_milvus_common import (  # noqa: E402
    milvus_entity_count,
    milvus_family_name,
    milvus_type_is_copy_safe,
)
from services.copy_milvus_milvus import (  # noqa: E402
    copy_milvus_to_milvus,
    milvus_milvus_copy_enabled,
)


def _milvus_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 19530), timeout=1):
            pass
    except OSError:
        pytest.skip("Milvus 19530 not reachable")


def _milvus_cfg(collection: str) -> dict:
    return {
        "type": "milvus",
        "format": "milvus",
        "host": "127.0.0.1",
        "port": 19530,
        "username": "root",
        "password": "Milvus",
        "database": collection,
        "table": collection,
    }


def test_milvus_family_and_copy_safe_types():
    assert milvus_family_name("milvus") == "milvus"
    assert milvus_family_name("zilliz") == "milvus"
    assert milvus_type_is_copy_safe("varchar") is True
    assert milvus_type_is_copy_safe("integer") is True
    assert milvus_type_is_copy_safe("join") is False


def test_milvus_milvus_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_MILVUS_MILVUS_COPY", "0")
    assert milvus_milvus_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_milvus_to_milvus(
            source_cfg=_milvus_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_milvus_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_milvus_milvus_same_collection_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    cfg = _milvus_cfg("same_coll")
    with pytest.raises(FastPathUnavailable, match="same collection"):
        copy_milvus_to_milvus(
            source_cfg=cfg,
            source_table="same_coll",
            dest_cfg=cfg,
            dest_table="same_coll",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_milvus_milvus_public_proxy_declines():
    dest = {
        **_milvus_cfg("b"),
        "host": "",
        "connection_string": "https://caboose.proxy.rlwy.net:19530",
    }
    with pytest.raises(FastPathUnavailable, match="public proxy"):
        copy_milvus_to_milvus(
            source_cfg=_milvus_cfg("a"),
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_milvus_milvus_cross_endpoint_declines():
    src = _milvus_cfg("a")
    dest = {**_milvus_cfg("b"), "host": "10.0.0.99", "port": 19530}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_milvus_to_milvus(
            source_cfg=src,
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_milvus_milvus_column_rename_declines():
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_milvus_to_milvus(
            source_cfg=_milvus_cfg("a"),
            source_table="a",
            dest_cfg=_milvus_cfg("b"),
            dest_table="b",
            pairs=[("id", "other")],
            milvus_ddls=["varchar"],
            replace_destination=True,
        )


def test_live_milvus_milvus_empty_dest_copy():
    _milvus_or_skip()
    from connectors.milvus_writer import write_mapped_rows

    tag = uuid.uuid4().hex[:8]
    src = f"dfmsrc{tag}"
    dest = f"dfmdst{tag}"
    try:
        result = write_mapped_rows(
            host="127.0.0.1",
            port=19530,
            database="",
            username="root",
            password="Milvus",
            schema="",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 41)],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "content", "target": "content"},
            ],
            column_types={"id": "STRING", "content": "STRING"},
            content_column="content",
            embedding_model="hash/4",
        )
        assert result.ok, result.error
        copy_result = copy_milvus_to_milvus(
            source_cfg=_milvus_cfg(src),
            source_table=src,
            dest_cfg=_milvus_cfg(dest),
            dest_table=dest,
            pairs=[("id", "id"), ("content", "content")],
            milvus_ddls=["varchar", "varchar"],
            replace_destination=True,
        )
        assert copy_result.source_rows == 40
        assert milvus_entity_count(_milvus_cfg(dest), dest) == 40
        assert copy_result.source_snapshot.get("milvus_read") == "query"
    finally:
        from services.copy_milvus_common import milvus_delete_collection

        milvus_delete_collection(_milvus_cfg(src), src)
        milvus_delete_collection(_milvus_cfg(dest), dest)


def test_live_milvus_milvus_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_MILVUS_MILVUS_COPY", raising=False)
    _milvus_or_skip()
    from connectors.milvus_writer import write_mapped_rows
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    tag = uuid.uuid4().hex[:8]
    src = f"dfmsrc{tag}"
    dest = f"dfmdst{tag}"
    try:
        seeded = write_mapped_rows(
            host="127.0.0.1",
            port=19530,
            database="",
            username="root",
            password="Milvus",
            schema="",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 21)],
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
        job_id = f"milvus-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_milvus_cfg(src), "format": "milvus"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_milvus_cfg(dest), "format": "milvus"}
        )
        mappings = [
            {"source": "id", "target": "id", "type": "VARCHAR", "transform": "none"},
            {"source": "content", "target": "content", "type": "VARCHAR", "transform": "none"},
        ]
        schema = {"id": "VARCHAR", "content": "VARCHAR"}
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="full_refresh_append",
            job_id=job_id,
        )
        assert transferred == 20
        assert summary.get("load_method") == "query_upsert_entities_milvus_milvus"
        assert milvus_entity_count(_milvus_cfg(dest), dest) == 20
    finally:
        from services.copy_milvus_common import milvus_delete_collection

        milvus_delete_collection(_milvus_cfg(src), src)
        milvus_delete_collection(_milvus_cfg(dest), dest)
