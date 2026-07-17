"""pgvector destination writer — turns rows into embedded vector chunks.

This writer is the first vector-DB destination in DataFlow. It reuses the
semantic chunking and embedding service in `services/vectorization.py` so that
any source row with textual content can be indexed for RAG without manual
field mapping.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from typing import Any, Callable

from connectors.postgresql_conn import get_connection
from connectors.writer_common import WriteResult as _WriteResult
from services.vectorization import vectorize_records


@dataclass
class WriteResult(_WriteResult):
    driver: str = "psycopg2"
    load_method: str = "pgvector_upsert"


def _vector_literal(vector: list[float] | None) -> str | None:
    if not vector:
        return None
    return "[" + ",".join(str(v) for v in vector) + "]"


def _exec_schema_table(cur: Any, schema: str, table_name: str, dimension: int) -> None:
    from psycopg2 import sql

    schema_id = sql.Identifier(schema)
    table_id = sql.Identifier(table_name)
    cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(schema_id))
    # Literal double braces in SQL so psycopg2.sql does not treat '{}' as a format placeholder.
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {}.{} (
                id TEXT PRIMARY KEY,
                content TEXT,
                embedding vector(%s),
                metadata JSONB DEFAULT '{{}}',
                source_id TEXT,
                chunk_index INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT now()
            )
            """
        ).format(schema_id, table_id),
        (dimension,),
    )


def write_mapped_rows(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    schema: str,
    connection_string: str,
    ssl: bool,
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    on_checkpoint: Callable[[int, int, int], None] | None = None,
    create_table: bool = True,
    error_policy: str | None = None,
    content_column: str | None = None,
    embedding_column: str | None = None,
    metadata_columns: list[str] | None = None,
    embedding_model: str | None = None,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    **_kwargs: Any,
) -> WriteResult:
    """Write text rows as embedded chunks into a PostgreSQL pgvector table."""
    if importlib.util.find_spec("psycopg2") is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error="psycopg2 is required for pgvector writes",
            driver="none",
        )

    records = [dict(zip(headers, row)) for row in data_rows]
    try:
        vector_rows = vectorize_records(
            records,
            content_column=content_column,
            embedding_column=embedding_column,
            metadata_columns=metadata_columns,
            model=embedding_model,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
        )
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error=f"Vectorization failed: {exc}",
        )

    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
        )

    # Determine dimension from the first row that has an embedding.
    dimension = 384
    for row in vector_rows:
        if row.get("embedding"):
            dimension = len(row["embedding"])
            break

    inserted = 0
    conn = get_connection(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
    )
    try:
        with conn.cursor() as cur:
            from psycopg2 import sql

            _exec_schema_table(cur, schema or "public", table_name, dimension)

            schema_id = sql.Identifier(schema or "public")
            table_id = sql.Identifier(table_name)

            batch_size = 1000
            inserted = 0
            total = len(vector_rows)
            for i in range(0, total, batch_size):
                batch = vector_rows[i : i + batch_size]
                values = []
                for row in batch:
                    vector = _vector_literal(row.get("embedding"))
                    metadata = row.get("metadata") or {}
                    values.append((
                        row["id"],
                        row.get("content", ""),
                        vector,
                        json.dumps(metadata, ensure_ascii=False, default=str),
                        row.get("source_id", ""),
                        row.get("chunk_index", 0),
                    ))

                args_str = ",".join(
                    cur.mogrify(
                        "(%s, %s, %s::vector, %s::jsonb, %s, %s)",
                        (row[0], row[1], row[2] if row[2] is not None else None, row[3], row[4], row[5]),
                    ).decode("utf-8")
                    for row in values
                )
                insert_sql = sql.SQL(
                    """
                    INSERT INTO {}.{} (id, content, embedding, metadata, source_id, chunk_index)
                    VALUES {}
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata,
                        source_id = EXCLUDED.source_id,
                        chunk_index = EXCLUDED.chunk_index,
                        created_at = now()
                    """
                ).format(schema_id, table_id, sql.SQL(args_str))
                cur.execute(insert_sql)
                inserted += len(batch)
                if on_checkpoint:
                    on_checkpoint(
                        (i // batch_size) + 1,
                        (total + batch_size - 1) // batch_size,
                        inserted,
                    )

            conn.commit()
    except Exception as exc:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=(inserted + 999) // 1000,
            error=str(exc),
        )
    finally:
        conn.close()

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=table_name,
        target_schema=schema or "public",
        checksum="",
        chunks_completed=(inserted + 999) // 1000,
    )
