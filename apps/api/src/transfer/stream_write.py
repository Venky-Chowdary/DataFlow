from __future__ import annotations

from services.brand import bigquery_dataset_of
from typing import Any
from src.transfer.stream import (
    Callable,
    EndpointConfig,
    WriteBatchBlocked,
    resolve_driver_type,
)
from src.transfer.job_quarantine import split_refused_unit

def _writer_diagnostics(result: Any) -> dict[str, Any]:
    rejected = int(getattr(result, "rejected_rows", 0) or 0)
    coerced = int(getattr(result, "coerced_null_rows", 0) or 0)
    skipped = int(getattr(result, "rows_skipped", 0) or 0)
    # GA: never truncate rejected_details before merge/DLQ — rows cannot disappear.
    details = list(getattr(result, "rejected_details", []) or [])
    warnings = list(getattr(result, "warnings", []) or [])
    try:
        from services.vectorization import attach_embedding_fallback_warning

        # Qdrant and the other vector destinations write through this stream
        # path. The adapter copy of this helper already harvested the notice;
        # this one did not, so a TF-IDF substitution stayed on the point and
        # never on the job.
        warnings = attach_embedding_fallback_warning(warnings)
    except ImportError:
        pass
    out: dict[str, Any] = {
        "rejected_rows": rejected,
        "coerced_null_rows": coerced,
        "rows_skipped": skipped,
        "rejected_details": details,
        "rejected_details_sample": details[:200],
        "warnings": warnings[:10],
        "error_policy": "quarantine" if (rejected or coerced) else "none",
        "load_method": getattr(result, "load_method", None),
    }
    meta = getattr(result, "meta", None) or {}
    if isinstance(meta, dict) and meta.get("schema_fidelity"):
        out["schema_fidelity"] = meta["schema_fidelity"]
    if isinstance(meta, dict):
        if isinstance(meta.get("skipped_identity_keys"), list):
            out["skipped_identity_keys"] = [str(key) for key in meta["skipped_identity_keys"]]
        for key in ("vector_docs_unchanged_skipped", "vector_docs_embedded"):
            if isinstance(meta.get(key), (int, float)):
                out[key] = int(meta[key])
        # Gate-8 identity/expansion stamps from the writer — adapters parity.
        # ``rows_expanded`` is the STRUCT explode delta (spool out minus the
        # rows the reader fetched); without it Gate-8 reads exploded rows as
        # unexplained extras on the target (X-30).
        for key in (
            "reconcile_sample",
            "written_ids",
            "source_row_count",
            "rows_expanded",
            # Older images of a key the upsert kept the latest of (D89).
            "collapsed_duplicate_rows",
        ):
            if key in meta and meta[key] is not None:
                out[key] = meta[key]
    if isinstance(meta, dict) and isinstance(meta.get("embedding_usage"), dict):
        out["embedding_usage"] = dict(meta["embedding_usage"])
    return out


def _raise_write_failure(result: Any, label: str) -> None:
    """Fail a batch write; connection drops are retriable when the writer ledger can skip commits.

    Always raise :class:`WriteBatchBlocked` (not bare RuntimeError) so mid-write
    FAIL_JOB / quarantine details reach DLQ persist before the job is marked failed.
    """
    err = result.error or label
    written = int(getattr(result, "rows_written", 0) or 0)
    details = list(getattr(result, "rejected_details", []) or [])
    rejected_rows = int(getattr(result, "rejected_rows", 0) or 0) or len(details)
    summary = _writer_diagnostics(result)
    rejected_rows = split_refused_unit(details, rejected_rows, summary)
    try:
        from connectors.write_resilience import is_connection_lost
    except ImportError:
        is_connection_lost = lambda _e: False  # noqa: E731
    # Writers stamp committed chunks in `_dataflow_write_ledger` when job_id is set,
    # so re-invoking the same batch after a proxy drop is safe (already-written
    # chunks are skipped). Prefer ConnectionError so with_retry can finish the job.
    if is_connection_lost(err):
        # Retriable transport failure — but never discard quarantine already
        # collected on the WriteResult (retry exhaustion must still DLQ).
        lost = ConnectionError(err)
        setattr(lost, "rejected_details", details)
        setattr(lost, "rejected_rows", rejected_rows)
        setattr(lost, "rows_written", written)
        setattr(lost, "dest_summary", summary)
        raise lost
    msg = (
        f"partial write ({written} rows committed before failure): {err}"
        if written > 0
        else str(err)
    )
    raise WriteBatchBlocked(
        msg,
        rejected_details=details,
        rejected_rows=rejected_rows,
        rows_written=written,
        dest_summary=summary,
    )


def _write_batch(
    dest_type: str,
    dest: EndpointConfig,
    cfg: dict[str, Any],
    table_name: str,
    headers: list[str],
    data_rows: list[list[str]],
    mappings: list[dict],
    column_types: dict[str, str],
    create_table: bool,
    on_checkpoint: Callable[..., None] | None,
    chunk_idx: int,
    total_chunks: int,
    rows_so_far: int,
    *,
    write_mode: str = "insert",
    conflict_columns: list[str] | None = None,
    sync_mode: str = "",
    backfill_new_fields: bool = False,
    error_policy: str | None = None,
    connection: Any | None = None,
    close_connection: bool | None = None,
    connection_holder: dict[str, Any] | None = None,
    skip_session_setup: bool = False,
    job_id: str | None = None,
    skip_preflight: bool = False,
    source_schema_catalog: dict[str, Any] | None = None,
    empty_cells_as_null: bool = False,
    records: list[dict[str, Any]] | None = None,
    source_spool: Any = None,
    allow_equal_lsn: bool = False,
) -> tuple[int, str, dict]:
    # Live dest nullability for write-time NOT NULL escalate (G3 / adapters parity).
    dest_nullability = dict(
        (getattr(dest, "extra", None) or {}).get("schema_nullability")
        or (cfg.get("extra") or {}).get("schema_nullability")
        or {}
    )
    # Live dest DDL from Studio probe — writers prefer this over Map stamps
    # (adapters parity; invent cliff when stream omitted schema_types).
    dest_column_types = dict(
        (getattr(dest, "extra", None) or {}).get("schema_types")
        or (cfg.get("extra") or {}).get("schema_types")
        or (getattr(dest, "extra", None) or {}).get("destination_column_types")
        or (cfg.get("extra") or {}).get("destination_column_types")
        or {}
    )
    if dest_type == "postgresql" and cfg.get("vector_layout") == "table":
        from services.pgvector_typed_embedding import augment_typed_batch, materialize_batch

        # pgvector typed table: the embedding cells ride the same INSERT (D209).
        headers, data_rows = materialize_batch(headers, data_rows, records, source_spool)
        records = source_spool = None
        headers, data_rows, mappings, column_types = augment_typed_batch(
            cfg, headers, data_rows, mappings, column_types,
            create_table=create_table, dest_column_types=dest_column_types,
        )
    source_handoff: dict[str, Any] = {}
    if source_spool is not None:
        source_handoff["source_spool"] = source_spool
    if records is not None:
        source_handoff["records"] = records
    if allow_equal_lsn:
        source_handoff["allow_equal_lsn"] = True
    if (getattr(dest, "extra", None) or {}).get("upsert_ddl_approved"):
        # Operator-approved unique index on an existing table (D101).
        source_handoff["upsert_ddl_approved"] = True
    from services.duplicate_policy import (
        DuplicatePolicyError,
        reset_match_action,
        resolve_duplicate_policy,
        set_match_action,
    )

    try:
        decided = resolve_duplicate_policy(
            duplicate_policy=str((getattr(dest, "extra", None) or {}).get("duplicate_policy") or ""),
            allow_duplicates=bool((getattr(dest, "extra", None) or {}).get("allow_duplicates")),
            sync_mode=sync_mode,
            business_key=bool(conflict_columns),
            write_mode=write_mode,
        )
    except DuplicatePolicyError:
        # A keyed policy with no key must not fall through to INSERT.
        raise
    except Exception:
        decided = None
    if decided is not None and (decided.explicit or (decided.policy == "upsert" and write_mode == "insert" and conflict_columns)):
        write_mode = decided.write_mode
    match_token = set_match_action(decided.match_action if decided else "update")
    from services.sync_cursor import is_overwrite_sync

    # Overwrite rebuilds the table from one source snapshot, so the source key
    # holds on the copy. Only append create-new withholds it (write_mode alone
    # cannot tell them apart: both are "insert").
    key_carry: dict[str, Any] = (
        {"carry_source_keys": True} if create_table and is_overwrite_sync(sync_mode) else {}
    )
    if dest_type == "postgresql" or dest_type == "redshift":
        from connectors.postgresql_writer import write_mapped_rows
        from connectors.write_resilience import build_write_batch_key

        pg_port = int(cfg.get("port") or (5439 if dest_type == "redshift" else 5432))
        result = write_mapped_rows(
            host=cfg["host"],
            port=pg_port,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            job_id=job_id,
            engine=dest_type,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            connection=connection,
            close_connection=close_connection,
            connection_holder=connection_holder,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            source_schema_catalog=source_schema_catalog,
            empty_cells_as_null=empty_cells_as_null,
            preserve_columns=list(
                (getattr(dest, "extra", None) or {}).get("preserve_columns") or []
            ),
            pg_overwrite_in_transaction=bool(
                dest_type == "postgresql"
                and (getattr(dest, "extra", None) or {}).get("pg_overwrite_in_transaction")
            ),
            **source_handoff,
            **key_carry,
        )
        if not result.ok:
            _raise_write_failure(result, f"{dest_type} batch write failed")
        summary = {
            "type": dest_type,
            "schema": result.target_schema,
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type == "mysql":
        from connectors.mysql_writer import write_mapped_rows
        from connectors.write_resilience import build_write_batch_key

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 3306),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            job_id=job_id,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            empty_cells_as_null=empty_cells_as_null,
            source_schema_catalog=source_schema_catalog,
            preserve_columns=list(
                (getattr(dest, "extra", None) or {}).get("preserve_columns") or []
            ),
            **source_handoff,
            **key_carry,
        )
        if not result.ok:
            _raise_write_failure(result, "MySQL batch write failed")
        summary = {
            "type": "mysql",
            "database": cfg["database"],
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type == "mongodb":
        from connectors.mongodb_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 27017),
            database=dest.database or cfg.get("database") or "test_db",
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "db"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
        )
        if not result.ok:
            _raise_write_failure(result, "MongoDB batch write failed")
        summary = {
            "type": "mongodb",
            "database": result.target_schema,
            "collection": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type == "sqlite":
        from connectors.sqlite_writer import write_mapped_rows
        from connectors.write_resilience import build_write_batch_key
        from services.sync_cursor import is_overwrite_sync

        # Property 4 — arm the same-txn write ledger (parity with PG/MySQL).
        # Without job_id + write_batch_key, insert retries silently duplicate.
        result = write_mapped_rows(
            host=cfg["host"],
            port=0,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            job_id=job_id,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            source_schema_catalog=source_schema_catalog,
            empty_cells_as_null=empty_cells_as_null,
            total_chunks=total_chunks,
            atomic_overwrite=is_overwrite_sync(sync_mode),
            **source_handoff,
            **key_carry,
        )
        if not result.ok:
            _raise_write_failure(result, "SQLite batch write failed")
        summary = {
            "type": "sqlite",
            "database": cfg["database"],
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type == "snowflake":
        from connectors.snowflake_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            role=cfg.get("role", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            connection=connection,
            close_connection=close_connection,
            skip_session_setup=skip_session_setup,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            # This dedicated branch bypassed the ``common`` payload, so the
            # file/object-store blank→NULL opt-in never reached the writer
            # (D1 — blanks preflight promised were rejected at write).
            empty_cells_as_null=empty_cells_as_null,
            **source_handoff,
        )
        if not result.ok:
            _raise_write_failure(result, "Snowflake batch write failed")
        summary = {"type": "snowflake", "schema": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type == "bigquery":
        from connectors.bigquery_writer import write_mapped_rows

        result = write_mapped_rows(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=bigquery_dataset_of(cfg),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            service_account=cfg.get("service_account", ""),
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            **source_handoff,
        )
        if not result.ok:
            _raise_write_failure(result, "BigQuery batch write failed")
        summary = {"type": "bigquery", "dataset": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if dest_type in ("s3", "gcs", "adls", "dynamodb", "elasticsearch", "redis", "pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
        writers = {
            "s3": "connectors.s3_writer",
            "gcs": "connectors.gcs_writer",
            "adls": "connectors.adls_writer",
            "dynamodb": "connectors.dynamodb_writer",
            "elasticsearch": "connectors.elasticsearch_writer",
            "redis": "connectors.redis_writer",
            "pgvector": "connectors.pgvector_writer",
            "qdrant": "connectors.qdrant_writer",
            "weaviate": "connectors.weaviate_writer",
            "pinecone": "connectors.pinecone_writer",
            "milvus": "connectors.milvus_writer",
        }
        import importlib
        mod = importlib.import_module(writers[dest_type])
        kwargs = {
            "host": cfg["host"],
            "port": int(cfg.get("port") or 0),
            "database": cfg["database"],
            "username": cfg.get("username", ""),
            "password": cfg.get("password", ""),
            "schema": cfg.get("schema", ""),
            "connection_string": cfg.get("connection_string", ""),
            "ssl": cfg.get("ssl", False),
            "warehouse": cfg.get("warehouse", ""),
            "role": cfg.get("role", ""),
            "auth_mode": cfg.get("auth_mode", ""),
            "api_key": cfg.get("api_key", ""),
            "service_account": cfg.get("service_account", ""),
            "auth_source": cfg.get("auth_source", ""),
            # MinIO / custom S3 / DynamoDB local — must reach writers (was dropped).
            "endpoint_url": cfg.get("endpoint_url", "") or "",
            "path_style": bool(cfg.get("path_style", False)),
            "region": cfg.get("region", "") or "",
            "table_name": table_name,
            "headers": headers,
            "data_rows": data_rows,
            "mappings": mappings,
            "column_types": column_types,
            "create_table": create_table,
            "error_policy": error_policy,
            "on_checkpoint": lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            "destination_column_nullability": dest_nullability,
            "destination_column_types": dest_column_types,
        }
        if dest_type == "redis":
            kwargs["write_mode"] = write_mode
            kwargs["conflict_columns"] = conflict_columns
            kwargs["sync_mode"] = sync_mode
            kwargs["file_batch_idx"] = chunk_idx
        if dest_type == "dynamodb":
            # Create-table needs HASH[/RANGE] from conflict_columns; without
            # them Dynamo invents nothing and refuses, or (worse) operators
            # get a half-configured table mid-demo.
            kwargs["write_mode"] = write_mode
            kwargs["conflict_columns"] = conflict_columns
            kwargs["sync_mode"] = sync_mode
            kwargs["job_id"] = job_id or ""
        if dest_type in ("s3", "gcs", "adls"):
            # Multi-chunk object-store writes must receive chunk index + total so
            # writers emit part-* keys instead of overwriting a fixed object.
            # job_id lets append runs isolate their part set from earlier runs.
            # conflict_columns lets a single-chunk deduped write merge by key
            # instead of replacing the object with only the new batch.
            kwargs["sync_mode"] = sync_mode
            kwargs["file_batch_idx"] = chunk_idx
            kwargs["total_chunks"] = total_chunks
            kwargs["job_id"] = job_id or ""
            kwargs["conflict_columns"] = conflict_columns
            # An extensionless key resolves to export.<file_format>; readers
            # derive the same key from the endpoint's file_format.
            dest_extra = getattr(dest, "extra", None)
            kwargs["file_format"] = str(
                cfg.get("file_format")
                or (dest_extra.get("file_format") if isinstance(dest_extra, dict) else "")
                or ""
            )
            kwargs.update(source_handoff)
        if dest_type in ("pgvector", "qdrant", "weaviate", "pinecone", "milvus"):
            kwargs["conflict_columns"] = conflict_columns
            kwargs["write_mode"] = write_mode
            kwargs["sync_mode"] = sync_mode
            extra = getattr(dest, "extra", {}) or {}
            kwargs["content_column"] = extra.get("content_column")
            kwargs["embedding_column"] = extra.get("embedding_column")
            kwargs["metadata_columns"] = extra.get("metadata_columns")
            kwargs["exclude_pii_columns"] = extra.get("exclude_pii_columns")
            kwargs["embedding_model"] = extra.get("embedding_model")
            kwargs["chunk_size"] = int(extra.get("chunk_size", 512)) if extra.get("chunk_size") else 512
            kwargs["chunk_overlap"] = int(extra.get("chunk_overlap", 50)) if extra.get("chunk_overlap") else 50
            kwargs["skip_chunking"] = bool(extra.get("skip_chunking"))
            kwargs["chunk_strategy"] = extra.get("chunk_strategy", "recursive")
            kwargs["chunk_unit"] = extra.get("chunk_unit", "chars")
            kwargs["chunk_tokenizer"] = extra.get("chunk_tokenizer")
            kwargs["text_template"] = extra.get("text_template")
            kwargs["vector_skip_unchanged"] = extra.get(
                "vector_skip_unchanged", True
            )
            for option in (
                "embedding_api_key",
                "embedding_base_url",
                "embedding_endpoint",
                "embedding_api_version",
                "embedding_base_model",
                "embedding_region",
                "embedding_dimensions",
                "embedding_requests_per_minute",
                "embedding_tokens_per_minute",
            ):
                if option in extra:
                    kwargs[option] = extra[option]
        result = mod.write_mapped_rows(**kwargs)
        if not result.ok:
            _raise_write_failure(result, f"{dest_type} batch write failed")
        # Name what was written like the batch writers do: job records and
        # reconcile read ``table``. An object-store writer's name is this
        # chunk's part key, not the logical object, so it stays ``key``.
        name_field = "key" if dest_type in ("s3", "gcs", "adls") else "table"
        summary = {"type": dest_type, name_field: result.table_name, "checksum": result.checksum,
                   "driver": result.driver, **_writer_diagnostics(result)}
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    if resolve_driver_type(dest_type) == "generic_sql":
        from connectors.generic_sql import write_mapped_rows

        from .connector_dispatch import writer_extra_kwargs as _writer_extra

        type_name = cfg.get("type", "") or dest_type
        from connectors.write_resilience import build_write_batch_key

        result = write_mapped_rows(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            type=type_name,
            table_name=table_name,
            headers=headers,
            data_rows=data_rows,
            mappings=mappings,
            column_types=column_types,
            create_table=create_table,
            write_mode=write_mode,
            conflict_columns=conflict_columns,
            backfill_new_fields=backfill_new_fields,
            auth_source=cfg.get("auth_source", ""),
            error_policy=error_policy,
            on_checkpoint=lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r) if on_checkpoint else None,
            skip_preflight=skip_preflight,
            # Arm the SQLAlchemy chunk ledger so insert-mode retries after a
            # mid-transfer failure cannot re-insert already-committed chunks.
            job_id=job_id,
            write_batch_key=build_write_batch_key(
                table_name=table_name, file_batch_idx=chunk_idx
            ),
            file_batch_idx=chunk_idx,
            destination_column_nullability=dest_nullability,
            destination_column_types=dest_column_types,
            **_writer_extra("generic_sql", cfg=cfg, dest=dest),
            **source_handoff,
            **key_carry,
        )
        if not result.ok:
            _raise_write_failure(result, f"{dest_type} batch write failed")
        summary = {"type": type_name, "schema": result.target_schema, "table": result.table_name,
                   "checksum": result.checksum, "driver": result.driver, **_writer_diagnostics(result)}
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    from .connector_dispatch import (
        has_writer,
        write_via_registry,
        writer_extra_kwargs,
    )

    if has_writer(dest_type):
        mode = write_mode
        if dest_type in ("salesforce", "hubspot") and (mode or "insert").lower() == "insert":
            mode = "upsert"
        common = {
            "host": cfg.get("host", ""),
            "port": int(cfg.get("port") or 0),
            "database": cfg.get("database", ""),
            "username": cfg.get("username", ""),
            "password": cfg.get("password", ""),
            "schema": cfg.get("schema", ""),
            "connection_string": cfg.get("connection_string", ""),
            "ssl": cfg.get("ssl", False),
            "api_key": cfg.get("api_key", ""),
            "table_name": table_name,
            "headers": headers,
            "data_rows": data_rows,
            "mappings": mappings,
            "column_types": column_types,
            "create_table": create_table,
            "error_policy": error_policy,
            "destination_column_nullability": dest_nullability,
            "destination_column_types": dest_column_types,
            # Create-new fidelity (PK / NOT NULL / defaults / collation) and the
            # empty-vs-NULL rule are route facts, not Postgres/MySQL facts: the
            # registry path dropped both, so a SQL Server destination created
            # itself with the server default collation and then failed its own
            # re-read as a fidelity collapse.
            "source_schema_catalog": source_schema_catalog,
            "empty_cells_as_null": empty_cells_as_null,
            "on_checkpoint": (
                (lambda c, t, r: on_checkpoint(chunk_idx, total_chunks, rows_so_far + r))
                if on_checkpoint
                else None
            ),
        }
        # The SQL Server / Oracle writers forward to the generic SQL CREATE,
        # which withholds the source key on append: an overwrite must carry it
        # as the PostgreSQL / MySQL / SQLite writers do (D144).
        if dest_type in ("sqlserver", "mssql", "oracle"):
            common.update(key_carry)
        from connectors.engine_record_spill import spool_write_kinds

        if dest_type in spool_write_kinds():
            common.update(source_handoff)
        extra = writer_extra_kwargs(dest_type, cfg=cfg, dest=dest, common=common)
        if dest_type == "sftp":
            # Without the mode the writer cannot tell append from overwrite and
            # replaces the remote file with only this run's rows.
            common["sync_mode"] = sync_mode
            # An extensionless remote name takes its extension from file_format;
            # the destination readers resolve the same file from it.
            raw_extra = getattr(dest, "extra", None)
            sftp_extra = dict(raw_extra) if isinstance(raw_extra, dict) else {}
            if cfg.get("file_format") and not sftp_extra.get("file_format"):
                sftp_extra["file_format"] = cfg["file_format"]
            common["dest_extra"] = {**sftp_extra, **dict(common.get("dest_extra") or {})}
        if dest_type == "iceberg":
            # Iceberg full-refresh overwrite must truncate once per job, not per chunk.
            common["sync_mode"] = sync_mode
            common["file_batch_idx"] = chunk_idx
        result = write_via_registry(
            dest_type,
            common=common,
            write_mode=mode or "insert",
            conflict_columns=conflict_columns or [],
            extra=extra or None,
        )
        if not result.ok:
            _raise_write_failure(result, f"{dest_type} batch write failed")
        summary = {
            "type": dest_type,
            "schema": result.target_schema,
            "table": result.table_name,
            "checksum": result.checksum,
            "driver": result.driver,
            **_writer_diagnostics(result),
        }
        from services.duplicate_policy import reset_match_action

        reset_match_action(match_token)
        return result.rows_written, result.checksum, summary

    from services.duplicate_policy import reset_match_action

    reset_match_action(match_token)
    raise ValueError(f"Streaming write not supported for destination type '{dest_type}'")
