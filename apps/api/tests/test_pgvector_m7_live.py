from __future__ import annotations

import socket
import uuid
from contextlib import closing

import pytest

_PG_PORT = 5434
_CFG = {
    "host": "localhost",
    "port": _PG_PORT,
    "database": "dataflow",
    "username": "dataflow",
    "password": "dataflow",
    "schema": "public",
    "connection_string": "",
    "ssl": False,
}


def _reachable() -> bool:
    try:
        with socket.create_connection(("localhost", _PG_PORT), timeout=1):
            return True
    except OSError:
        return False


pytestmark = pytest.mark.skipif(
    not _reachable(), reason="pgvector is not reachable on localhost:5434"
)


@pytest.fixture
def vector_table():
    name = f"gvec_m7_{uuid.uuid4().hex[:12]}"
    yield name
    import psycopg2
    from psycopg2 import sql

    conn = psycopg2.connect(
        host=_CFG["host"],
        port=_CFG["port"],
        dbname=_CFG["database"],
        user=_CFG["username"],
        password=_CFG["password"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s)", ("public._df_vector_collections",))
            if cur.fetchone()[0] is not None:
                cur.execute(
                    sql.SQL("DELETE FROM {}.{} WHERE table_name = %s").format(
                        sql.Identifier("public"),
                        sql.Identifier("_df_vector_collections"),
                    ),
                    (name,),
                )
            cur.execute(
                sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                    sql.Identifier("public"), sql.Identifier(name)
                )
            )
        conn.commit()
    finally:
        conn.close()


def _write(table_name: str, **options):
    from connectors.pgvector_writer import write_mapped_rows

    return write_mapped_rows(
        **_CFG,
        table_name=table_name,
        headers=["id", "content"],
        data_rows=[["doc-1", "M7 pgvector index and storage verification."]],
        mappings=[
            {"source": "id", "target": "id", "target_type": "VARCHAR"},
            {"source": "content", "target": "content", "target_type": "TEXT"},
        ],
        column_types={"id": "string", "content": "string"},
        content_column="content",
        embedding_model="hash/32",
        skip_chunking=True,
        **options,
    )


def _connect():
    import psycopg2

    return psycopg2.connect(
        host=_CFG["host"],
        port=_CFG["port"],
        dbname=_CFG["database"],
        user=_CFG["username"],
        password=_CFG["password"],
    )


def test_default_storage_keeps_vector_and_creates_source_index(vector_table):
    result = _write(vector_table)
    assert result.ok, result.error

    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            WHERE a.attrelid = to_regclass(%s)
              AND a.attname = 'embedding'
              AND a.attnum > 0
              AND NOT a.attisdropped
            """,
            (f"public.{vector_table}",),
        )
        assert cur.fetchone()[0] == "vector(32)"
        cur.execute(
            "SELECT indexname, indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            ("public", vector_table),
        )
        indexes = cur.fetchall()
        index_defs = [row[1] for row in indexes]
    assert any("USING btree (source_id)" in definition for definition in index_defs)
    assert not any("USING hnsw" in definition for definition in index_defs)

    source_index = next(
        name
        for name, definition in indexes
        if "USING btree (source_id)" in definition
    )
    from psycopg2 import sql

    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            sql.SQL("DROP INDEX {}").format(sql.Identifier("public", source_index))
        )
        conn.commit()
    repeated = _write(vector_table)
    assert repeated.ok, repeated.error
    assert repeated.meta.get("vector_docs_unchanged_skipped") == 1
    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            ("public", vector_table),
        )
        index_defs = [row[0] for row in cur.fetchall()]
    assert sum("USING btree (source_id)" in item for item in index_defs) == 1


def test_hnsw_index_uses_cosine_ops_and_configured_options(vector_table):
    result = _write(
        vector_table,
        vector_index="hnsw",
        vector_hnsw_m=12,
        vector_hnsw_ef_construction=80,
    )
    assert result.ok, result.error

    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            ("public", vector_table),
        )
        index_defs = [row[0] for row in cur.fetchall()]
    hnsw_def = next(
        (definition for definition in index_defs if "USING hnsw" in definition),
        None,
    )
    assert hnsw_def is not None
    assert "embedding vector_cosine_ops" in hnsw_def
    assert "m='12'" in hnsw_def or "m=12" in hnsw_def
    assert "ef_construction='80'" in hnsw_def or "ef_construction=80" in hnsw_def


def test_halfvec_storage_round_trips_a_vector(vector_table):
    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        version = tuple(int(part) for part in cur.fetchone()[0].split(".")[:2])
    if version < (0, 7):
        pytest.skip(f"pgvector {version} does not support halfvec")

    result = _write(
        vector_table, vector_storage="halfvec", vector_index="hnsw"
    )
    assert result.ok, result.error

    from psycopg2 import sql

    with closing(_connect()) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attname = 'embedding'
              AND a.attnum > 0
              AND NOT a.attisdropped
            """,
            ("public", vector_table),
        )
        storage_type = cur.fetchone()[0]
        cur.execute(
            "SELECT indexdef FROM pg_indexes WHERE schemaname = %s AND tablename = %s",
            ("public", vector_table),
        )
        index_defs = [row[0] for row in cur.fetchall()]
        cur.execute(
            sql.SQL("SELECT embedding::text FROM {}.{} LIMIT 1").format(
                sql.Identifier("public"), sql.Identifier(vector_table)
            )
        )
        embedded_text = cur.fetchone()[0]
    assert storage_type == "halfvec(32)"
    assert len(embedded_text.strip("[]").split(",")) == 32
    hnsw_def = next(item for item in index_defs if "USING hnsw" in item)
    assert "embedding halfvec_cosine_ops" in hnsw_def
    assert "m='16'" in hnsw_def or "m=16" in hnsw_def
    assert "ef_construction='64'" in hnsw_def or "ef_construction=64" in hnsw_def


def test_storage_change_is_rejected_by_fingerprint(vector_table):
    first = _write(vector_table)
    assert first.ok, first.error

    changed = _write(vector_table, vector_storage="halfvec")
    assert not changed.ok
    assert changed.meta.get("vector_fingerprint_status") == "mismatch"

