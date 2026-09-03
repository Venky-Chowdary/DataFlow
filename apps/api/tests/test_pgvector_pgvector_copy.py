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
            pairs=[("id", "id"), ("content", "content"), ("embedding", "embedding")],
            pgvector_ddls=["text", "text", "vector(4)"],
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
