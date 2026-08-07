"""pgvector destination writer — turns rows into embedded vector chunks.

This writer is the first vector-DB destination in Datawrap. It reuses the
semantic chunking and embedding service in `services/vectorization.py` so that
any source row with textual content can be indexed for RAG without manual
field mapping.
"""

from __future__ import annotations

import importlib.util
import json
from dataclasses import dataclass
from typing import Any, Callable

from services.value_serializer import sanitize_json_value
from services.vectorization import vectorize_records

from connectors.postgresql_conn import get_connection
from connectors.writer_common import WriteResult as _WriteResult


@dataclass
class WriteResult(_WriteResult):
    driver: str = "psycopg2"
    load_method: str = "pgvector_upsert"


def _vector_literal(vector: list[float] | None) -> str | None:
    if not vector:
        return None
    return "[" + ",".join(str(v) for v in vector) + "]"


def _pgvector_live_embedding_dim(
    cur: Any,
    schema: str,
    table_name: str,
    *,
    column: str = "embedding",
) -> int | None:
    """Read live ``vector(n)`` typmod for the embedding column.

    Uses ``format_type`` so we never guess atttypmod encoding across pgvector
    versions. Missing table/column → ``None`` (caller fail-closed).
    """
    import re

    from psycopg2 import sql

    cur.execute(
        sql.SQL(
            """
            SELECT format_type(a.atttypid, a.atttypmod)
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            """
        ),
        (schema, table_name, column),
    )
    row = cur.fetchone()
    if not row or not row[0]:
        return None
    formatted = str(row[0]).lower().replace(" ", "")
    match = re.search(r"vector\((\d+)\)", formatted)
    if not match:
        # Unbounded vector / non-vector type — refuse invent.
        return None
    try:
        dim = int(match.group(1))
    except (TypeError, ValueError):
        return None
    return dim if dim > 0 else None


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


def _pgvector_gate_existing_physical(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    connection_string: str,
    ssl: bool,
    schema: str,
    table_name: str,
    mapped_targets: list[str],
    studio_live: dict[str, Any] | None,
    studio_typed_all: bool,
) -> tuple[bool, dict[str, str] | None, str | None]:
    """Probe existing pgvector table DDL before Map bind.

    Returns ``(table_existed, destination_column_types|None, error|None)``.
    """
    from connectors.postgresql_writer import _fetch_pg_column_types
    from connectors.writer_common import require_physical_types_for_existing_table

    sch = schema or "public"
    live: dict[str, str] = {}
    if isinstance(studio_live, dict):
        live.update({str(k): str(v) for k, v in studio_live.items() if k and v})

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
            cur.execute("SELECT to_regclass(%s)", (f"{sch}.{table_name}",))
            existed = cur.fetchone()[0] is not None
            if not existed:
                return False, (live if studio_typed_all else None), None
            physical = _fetch_pg_column_types(cur, sch, table_name)
            if not physical and not studio_typed_all:
                return True, None, (
                    f"pgvector table {sch}.{table_name} exists but live column "
                    "types were unavailable — refuse Map VARCHAR bind "
                    "(empty→NULL invent risk). Re-check grants."
                )
            live.update(physical)
            # Gate only real table scalars (id/content/source_id/chunk_index/…)
            # — other mapped fields land in JSONB metadata and must not trip
            # require_physical. Known scalars still gate when information_schema
            # is partial (invent cliff).
            _scalar = {
                "id",
                "content",
                "source_id",
                "chunk_index",
                "created_at",
            }
            mapped_existing = [
                c
                for c in mapped_targets
                if c
                and str(c).lower() != "embedding"
                and (
                    str(c).lower() in _scalar
                    or c in physical
                    or str(c).lower() in physical
                    or str(c).upper() in physical
                )
            ]
            effective = dict(live)
            if isinstance(studio_live, dict):
                for c in mapped_existing:
                    if (
                        effective.get(c)
                        or effective.get(str(c).lower())
                        or effective.get(str(c).upper())
                    ):
                        continue
                    st = str(studio_live.get(c) or "").strip()
                    if st:
                        effective[c] = st
            if mapped_existing:
                phys_err = require_physical_types_for_existing_table(
                    table_existed=True,
                    physical=effective,
                    dialect_label="pgvector",
                    target_cols=mapped_existing,
                )
                if phys_err:
                    return True, None, phys_err
            return True, effective, None
    finally:
        conn.close()


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
    exclude_pii_columns: list[str] | None = None,
    embedding_model: str | None = None,
    chunk_size: int = 512,
    chunk_overlap: int = 50,
    skip_chunking: bool = False,
    durable_embedding_cache: bool | None = None,
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

    from connectors.writer_common import prepare_records_for_vector_write

    pk_cols = list(
        _kwargs.get("destination_pk_columns")
        or _kwargs.get("conflict_columns")
        or []
    ) or None
    studio_live = _kwargs.get("destination_column_types")
    mapped_targets = [
        str(m.get("target") or m.get("source") or "").strip()
        for m in (mappings or [])
        if str(m.get("target") or m.get("source") or "").strip()
    ]
    if not mapped_targets:
        mapped_targets = [str(h) for h in (headers or []) if h]
    studio_typed_all = (
        isinstance(studio_live, dict)
        and bool(mapped_targets)
        and all(str(studio_live.get(c) or "").strip() for c in mapped_targets)
    )
    _existed, gated_types, gate_err = _pgvector_gate_existing_physical(
        host=host,
        port=port,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
        ssl=ssl,
        schema=schema or "public",
        table_name=table_name,
        mapped_targets=mapped_targets,
        studio_live=studio_live if isinstance(studio_live, dict) else None,
        studio_typed_all=studio_typed_all,
    )
    if gate_err:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error=gate_err,
        )
    records, map_rejected, map_abort = prepare_records_for_vector_write(
        headers=headers,
        data_rows=data_rows,
        mappings=mappings,
        column_types=column_types,
        error_policy=error_policy,
        dest_kind="pgvector",
        destination_pk_columns=pk_cols,
        stream_contracts=_kwargs.get("stream_contracts"),
        contract_primary_key=_kwargs.get("contract_primary_key"),
        label="pgvector",
        destination_column_nullability=_kwargs.get("destination_column_nullability"),
        destination_column_types=gated_types,
    )
    if map_abort:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error=map_abort,
            rejected_details=map_rejected,
            rejected_rows=len(map_rejected),
        )
    try:
        vector_rows = vectorize_records(
            records,
            content_column=content_column,
            embedding_column=embedding_column,
            metadata_columns=metadata_columns,
            exclude_pii_columns=exclude_pii_columns,
            model=embedding_model,
            chunk_size=chunk_size,
            chunk_overlap=chunk_overlap,
            skip_chunking=skip_chunking,
            durable_embedding_cache=durable_embedding_cache,
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
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
        )

    if not vector_rows:
        return WriteResult(
            ok=True,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
            warnings=[r.get("reason") or "" for r in map_rejected[:10] if r.get("reason")],
        )

    # Determine dimension from valid embeddings only — never invent 384.
    from services.vector_embedding import coerce_embedding, resolve_embedding_dimension

    dimension, dim_err = resolve_embedding_dimension(vector_rows, default=None)
    if dimension is None:
        return WriteResult(
            ok=False,
            rows_written=0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=0,
            error=dim_err or "embedding dimension unknown — refuse fabricated defaults",
            rejected_details=list(map_rejected),
            rejected_rows=len(map_rejected),
        )

    inserted = 0
    committed = False
    rejected_details: list[dict[str, Any]] = list(map_rejected)
    valid_rows: list[dict[str, Any]] = []
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

            if create_table:
                _exec_schema_table(cur, schema or "public", table_name, dimension)
            else:
                # Respect create_table=False — never contradict preflight deny-create.
                cur.execute(
                    "SELECT to_regclass(%s)",
                    (f"{schema or 'public'}.{table_name}",),
                )
                if cur.fetchone()[0] is None:
                    return WriteResult(
                        ok=False,
                        rows_written=0,
                        table_name=table_name,
                        target_schema=schema or "public",
                        checksum="",
                        chunks_completed=0,
                        error=(
                            f"pgvector destination {schema or 'public'}.{table_name} "
                            "is missing and create_table is disabled"
                        ),
                        rejected_details=list(map_rejected),
                        rejected_rows=len(map_rejected),
                    )

            # CREATE TABLE IF NOT EXISTS does not alter an existing vector(n) —
            # always probe live typmod and refuse dim invent / silent truncate.
            live_dim = _pgvector_live_embedding_dim(
                cur, schema or "public", table_name
            )
            if live_dim is None:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "public",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"pgvector table {schema or 'public'}.{table_name} "
                        "embedding vector(n) typmod unavailable — refuse upsert "
                        "with source-only dimension (silent dim invent risk)."
                    ),
                    rejected_details=list(map_rejected),
                    rejected_rows=len(map_rejected),
                )
            if int(live_dim) != int(dimension):
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "public",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        f"pgvector column embedding is vector({live_dim}), but "
                        f"embeddings are dimension {dimension} — refuse silent "
                        "truncate/pad invent. Use a matching model or a new table."
                    ),
                    rejected_details=list(map_rejected)
                    + [
                        {
                            "row": "",
                            "column": "embedding",
                            "target": "embedding",
                            "value": f"source={dimension} live={live_dim}",
                            "reason": "vector dimension mismatch",
                            "policy": "fail",
                        }
                    ],
                    rejected_rows=len(map_rejected) + 1,
                )

            schema_id = sql.Identifier(schema or "public")
            table_id = sql.Identifier(table_name)

            batch_size = 1000
            inserted = 0
            written_rows: list[dict[str, Any]] = []
            # Filter to rows with valid embeddings matching dimension.
            valid_rows = []
            for row in vector_rows:
                emb, err = coerce_embedding(row.get("embedding"), expected_dimension=dimension)
                if err or emb is None:
                    from services.vector_embedding import embedding_reject_reason

                    rejected_details.append({
                        "row": str(row.get("id") or ""),
                        "column": "embedding",
                        "target": "embedding",
                        "value": "",
                        "reason": embedding_reject_reason(row, err),
                        "policy": "quarantine",
                    })
                    continue
                row = dict(row)
                row["embedding"] = emb
                valid_rows.append(row)
            from connectors.writer_common import reject_on_strict_policy

            strict_error = reject_on_strict_policy(error_policy, rejected_details, "pgvector")
            if strict_error:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "public",
                    checksum="",
                    chunks_completed=0,
                    error=strict_error,
                    rejected_details=rejected_details,
                    rejected_rows=len(rejected_details),
                )
            if not valid_rows and rejected_details:
                return WriteResult(
                    ok=False,
                    rows_written=0,
                    table_name=table_name,
                    target_schema=schema or "public",
                    checksum="",
                    chunks_completed=0,
                    error=(
                        rejected_details[-1].get("reason")
                        if rejected_details
                        else None
                    )
                    or "all embeddings rejected",
                    rejected_details=rejected_details,
                    rejected_rows=len(rejected_details),
                )
            total = len(valid_rows)
            for i in range(0, total, batch_size):
                batch = valid_rows[i : i + batch_size]
                values = []
                batch_written: list[dict[str, Any]] = []
                for row in batch:
                    from services.vector_embedding import coerce_chunk_index

                    vector = _vector_literal(row.get("embedding"))
                    metadata = row.get("metadata") or {}
                    try:
                        chunk_idx = coerce_chunk_index(row.get("chunk_index"))
                    except ValueError as exc:
                        rejected_details.append({
                            "row": row.get("id") or row.get("source_id") or "?",
                            "column": "chunk_index",
                            "target": "chunk_index",
                            "value": str(row.get("chunk_index"))[:120],
                            "reason": str(exc),
                            "policy": "write_quarantine",
                        })
                        continue
                    values.append((
                        row["id"],
                        row.get("content", ""),
                        vector,
                        json.dumps(metadata, ensure_ascii=False, default=sanitize_json_value),
                        row.get("source_id", ""),
                        chunk_idx,
                    ))
                    batch_written.append(row)

                if not values:
                    continue
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
                inserted += len(values)
                written_rows.extend(batch_written)
                if on_checkpoint:
                    on_checkpoint(
                        (i // batch_size) + 1,
                        (total + batch_size - 1) // batch_size,
                        inserted,
                    )

            conn.commit()
            committed = True
    except Exception as exc:
        try:
            if conn is not None and not committed:
                conn.rollback()
        except Exception:
            pass
        return WriteResult(
            ok=False,
            # Only report durable rows — uncommitted inserts are not written.
            rows_written=inserted if committed else 0,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=(inserted + 999) // 1000 if committed else 0,
            error=str(exc),
            rejected_details=rejected_details,
            rejected_rows=len(rejected_details),
        )
    finally:
        conn.close()

    from connectors.writer_common import reject_on_strict_policy as _reject_final

    _final_abort = _reject_final(error_policy, rejected_details, "pgvector")
    if _final_abort:
        return WriteResult(
            ok=False,
            rows_written=inserted,
            table_name=table_name,
            target_schema=schema or "public",
            checksum="",
            chunks_completed=(inserted + 999) // 1000,
            error=_final_abort,
            rejected_details=rejected_details,
            rejected_rows=len(rejected_details),
            warnings=[r.get("reason") or "" for r in rejected_details[:10] if r.get("reason")],
        )

    return WriteResult(
        ok=True,
        rows_written=inserted,
        table_name=table_name,
        target_schema=schema or "public",
        checksum="",
        chunks_completed=(inserted + 999) // 1000,
        rejected_details=rejected_details,
        rejected_rows=len(rejected_details),
        warnings=[r.get("reason") or "" for r in rejected_details[:10] if r.get("reason")],
        meta=_pgvector_gate8_meta(written_rows),
    )


def _pgvector_gate8_meta(valid_rows: list[dict[str, Any]]) -> dict[str, Any]:
    from connectors.writer_common import vector_gate8_meta

    rows = []
    for row in valid_rows:
        meta = dict(row.get("metadata") or {}) if isinstance(row.get("metadata"), dict) else {}
        rows.append({
            "id": row.get("id"),
            "source_id": row.get("source_id"),
            "content": row.get("content"),
            "chunk_index": row.get("chunk_index"),
            **meta,
        })
    return vector_gate8_meta(rows)
