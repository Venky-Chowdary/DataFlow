"""pgvector → pgvector binary COPY — dest COUNT(*), never DISTINCT source_id."""

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
from services.copy_pgvector_common import (  # noqa: E402
    pgvector_family_name,
    pgvector_row_count,
    pgvector_type_is_copy_safe,
)
from services.copy_pgvector_pgvector import (  # noqa: E402
    copy_pgvector_to_pgvector,
    pgvector_pgvector_copy_enabled,
)


def _pg_or_skip():
    try:
        with socket.create_connection(("127.0.0.1", 5432), timeout=1):
            pass
    except OSError:
        pytest.skip("PostgreSQL 5432 not reachable")


def _pgvector_cfg(table: str) -> dict:
    return {
        "type": "pgvector",
        "format": "pgvector",
        "host": "127.0.0.1",
        "port": 5432,
        "database": "dataflow",
        "username": "dataflow",
        "password": "dataflow",
        "schema": "public",
        "table": table,
    }


def test_pgvector_family_and_copy_safe_types():
    assert pgvector_family_name("pgvector") == "pgvector"
    assert pgvector_type_is_copy_safe("vector(768)") is True
    assert pgvector_type_is_copy_safe("jsonb") is True
    assert pgvector_type_is_copy_safe("text") is True
    assert pgvector_type_is_copy_safe("bytea") is False
    assert pgvector_type_is_copy_safe("timestamptz") is False


def test_pgvector_pgvector_copy_kill_switch(monkeypatch):
    monkeypatch.setenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", "0")
    assert pgvector_pgvector_copy_enabled() is False
    with pytest.raises(FastPathUnavailable, match="disabled"):
        copy_pgvector_to_pgvector(
            source_cfg=_pgvector_cfg("missing_src"),
            source_table="missing_src",
            dest_cfg=_pgvector_cfg("missing_dst"),
            dest_table="missing_dst",
            pairs=[("id", "id")],
            pgvector_ddls=["text"],
            replace_destination=True,
        )


def test_pgvector_pgvector_same_table_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", raising=False)
    cfg = _pgvector_cfg("same_table")
    with pytest.raises(FastPathUnavailable, match="same table"):
        copy_pgvector_to_pgvector(
            source_cfg=cfg,
            source_table="same_table",
            dest_cfg=cfg,
            dest_table="same_table",
            pairs=[("id", "id")],
            pgvector_ddls=["text"],
            replace_destination=True,
        )


def test_pgvector_pgvector_cross_endpoint_declines():
    src = _pgvector_cfg("a")
    dest = {**_pgvector_cfg("b"), "host": "10.0.0.99", "port": 5432}
    with pytest.raises(FastPathUnavailable, match="cross-endpoint"):
        copy_pgvector_to_pgvector(
            source_cfg=src,
            source_table="a",
            dest_cfg=dest,
            dest_table="b",
            pairs=[("id", "id")],
            pgvector_ddls=["text"],
            replace_destination=True,
        )


def test_pgvector_pgvector_column_rename_declines():
    with pytest.raises(FastPathUnavailable, match="rename"):
        copy_pgvector_to_pgvector(
            source_cfg=_pgvector_cfg("a"),
            source_table="a",
            dest_cfg=_pgvector_cfg("b"),
            dest_table="b",
            pairs=[("id", "other")],
            pgvector_ddls=["text"],
            replace_destination=True,
        )


def test_pgvector_unmapped_columns_decline(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_table_exists",
        lambda _cfg, _table: True,
    )
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_require_full_column_mapping",
        lambda _cfg, _table, _pairs: (_ for _ in ()).throw(
            FastPathUnavailable(
                "pgvector identity COPY requires all source columns; "
                "unmapped: embedding"
            )
        ),
    )
    with pytest.raises(FastPathUnavailable, match="unmapped: embedding"):
        copy_pgvector_to_pgvector(
            source_cfg=_pgvector_cfg("a"),
            source_table="a",
            dest_cfg=_pgvector_cfg("b"),
            dest_table="b",
            pairs=[("id", "id"), ("content", "content")],
            pgvector_ddls=["text", "text"],
            replace_destination=True,
        )


def test_pgvector_require_full_mapping_helper(monkeypatch):
    from services.copy_pgvector_common import pgvector_require_full_column_mapping

    monkeypatch.setattr(
        "services.copy_pgvector_common.pgvector_column_names",
        lambda _cfg, _table: ["id", "content", "embedding"],
    )
    with pytest.raises(FastPathUnavailable, match="unmapped: embedding"):
        pgvector_require_full_column_mapping(
            {},
            "t",
            [("id", "id"), ("content", "content")],
        )
    pgvector_require_full_column_mapping(
        {},
        "t",
        [("id", "id"), ("content", "content"), ("embedding", "embedding")],
    )


def test_pgvector_skip_complete_when_counts_match(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_table_exists",
        lambda _cfg, _table: True,
    )
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_require_full_column_mapping",
        lambda *_a, **_k: None,
    )
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_row_count",
        lambda _cfg, _table: 25,
    )
    result = copy_pgvector_to_pgvector(
        source_cfg=_pgvector_cfg("a"),
        source_table="a",
        dest_cfg=_pgvector_cfg("b"),
        dest_table="b",
        pairs=[("id", "id")],
        pgvector_ddls=["text"],
        replace_destination=False,
    )
    assert result.source_snapshot.get("copy_split") == "skip"


def test_pgvector_occupied_mismatch_declines(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", raising=False)
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_table_exists",
        lambda _cfg, _table: True,
    )
    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_require_full_column_mapping",
        lambda *_a, **_k: None,
    )

    def _count(_cfg, table):
        return 2 if table == "b" else 80

    monkeypatch.setattr(
        "services.copy_pgvector_pgvector.pgvector_row_count",
        _count,
    )
    with pytest.raises(FastPathUnavailable, match="occupied pgvector dest"):
        copy_pgvector_to_pgvector(
            source_cfg=_pgvector_cfg("a"),
            source_table="a",
            dest_cfg=_pgvector_cfg("b"),
            dest_table="b",
            pairs=[("id", "id")],
            pgvector_ddls=["text"],
            replace_destination=False,
        )


def test_live_pgvector_pgvector_empty_dest_copy():
    _pg_or_skip()
    pytest.importorskip("psycopg2")
    from connectors.pgvector_writer import write_mapped_rows

    tag = uuid.uuid4().hex[:8]
    src = f"dfpvsrc{tag}"
    dest = f"dfpvdst{tag}"
    try:
        result = write_mapped_rows(
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 26)],
            mappings=[
                {"source": "id", "target": "id"},
                {"source": "content", "target": "content"},
            ],
            column_types={"id": "STRING", "content": "STRING"},
            content_column="content",
            embedding_model="hash/4",
        )
        assert result.ok, result.error
        copy_result = copy_pgvector_to_pgvector(
            source_cfg=_pgvector_cfg(src),
            source_table=src,
            dest_cfg=_pgvector_cfg(dest),
            dest_table=dest,
            # A pgvector table is the writer's whole shape: an identity copy
            # that mapped only id/content/embedding would hand back a store
            # without its metadata, provenance or chunk order, so the route
            # requires every column and the proof covers all of them.
            pairs=[
                ("id", "id"),
                ("content", "content"),
                ("embedding", "embedding"),
                ("metadata", "metadata"),
                ("source_id", "source_id"),
                ("chunk_index", "chunk_index"),
                ("created_at", "created_at"),
            ],
            pgvector_ddls=[
                "text",
                "text",
                "vector(4)",
                "jsonb",
                "text",
                "integer",
                "timestamp",
            ],
            replace_destination=True,
        )
        assert copy_result.source_rows == 25
        assert pgvector_row_count(_pgvector_cfg(dest), dest) == 25
        assert copy_result.source_snapshot.get("pgvector_read") == "binary_copy"
    finally:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        conn.close()


def test_live_pgvector_pgvector_stream_load_method(monkeypatch):
    monkeypatch.delenv("DATAFLOW_PGVECTOR_PGVECTOR_COPY", raising=False)
    _pg_or_skip()
    pytest.importorskip("psycopg2")
    from connectors.pgvector_writer import write_mapped_rows
    from services.million_row_proof import ensure_memory_job_store_if_mongo_down
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import EndpointConfig
    from src.transfer.stream import stream_database_transfer

    tag = uuid.uuid4().hex[:8]
    src = f"dfpvsrc{tag}"
    dest = f"dfpvdst{tag}"
    try:
        seeded = write_mapped_rows(
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            username="dataflow",
            password="dataflow",
            schema="public",
            connection_string="",
            ssl=False,
            table_name=src,
            headers=["id", "content"],
            data_rows=[[str(i), f"row{i}"] for i in range(1, 11)],
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
        job_id = f"pgvector-copy-{tag}"
        get_mongodb_service().create_transfer_job({"_id": job_id, "name": job_id})
        source = EndpointConfig.from_dict(
            "database", {**_pgvector_cfg(src), "format": "pgvector"}
        )
        destination = EndpointConfig.from_dict(
            "database", {**_pgvector_cfg(dest), "format": "pgvector"}
        )
        columns = {
            "id": "TEXT",
            "content": "TEXT",
            "embedding": "VECTOR",
            "metadata": "JSONB",
            "source_id": "TEXT",
            "chunk_index": "INTEGER",
            "created_at": "TIMESTAMP",
        }
        mappings = [
            {"source": name, "target": name, "type": declared, "transform": "none"}
            for name, declared in columns.items()
        ]
        schema = dict(columns)
        transferred, _ddl, summary, _cols = stream_database_transfer(
            source,
            destination,
            mappings,
            schema,
            sync_mode="full_refresh_append",
            job_id=job_id,
        )
        assert transferred == 10
        assert summary.get("load_method") == "copy_binary_pgvector_pgvector"
        assert pgvector_row_count(_pgvector_cfg(dest), dest) == 10
    finally:
        import psycopg2

        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            database="dataflow",
            user="dataflow",
            password="dataflow",
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f'DROP TABLE IF EXISTS public."{src}"')
            cur.execute(f'DROP TABLE IF EXISTS public."{dest}"')
        conn.close()
