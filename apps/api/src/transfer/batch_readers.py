"""Per-source batch read dispatch for the streaming transfer engine.

Split out of ``stream.py`` (a god module over its size budget). This is the
single place that knows how each source driver paginates a read: keyset cursor,
numeric offset, opaque continuation token (DynamoDB / Elasticsearch search_after
/ Redis SCAN / Kafka offsets), or SaaS cursor. Connector imports stay lazy so
optional driver dependencies are only required for routes that use them.

``stream._read_batch`` wraps this with retry/telemetry; callers should use that.
"""

from __future__ import annotations

from typing import Any

from .connector_capabilities import resolve_driver_type


def _read_batch_impl(
    src_type: str,
    cfg: dict[str, Any],
    table: str,
    columns: list[str] | None,
    offset: int,
    limit: int,
    database: str = "",
    dynamodb_cursor: dict | None = None,
    dynamodb_total: int | None = None,
    *,
    cursor_column: str = "",
    cursor_after: str | None = None,
    cursor_type: str | None = None,
    known_total_rows: int | None = None,
    es_search_after: list | None = None,
    redis_scan_state=None,
    kafka_cursor: dict | None = None,
    cursor_primary_key: str | None = None,
    cursor_key_columns: list[str] | None = None,
    scan_state: dict[str, Any] | None = None,
):
    from services.procedure_source import is_callable_source, read_callable_batch

    # Procedure / custom-SQL extract — one CALL, then page the spool.
    # Must run before table readers so a leftover table name cannot hijack the read.
    if is_callable_source(cfg):
        return read_callable_batch(
            cfg,
            offset=offset,
            limit=limit,
            peek=False,
            columns=columns,
            cursor_column=cursor_column or None,
            cursor_after=cursor_after,
        )

    # Phase F2 — N-col composite keyset (≥3) on SQLAlchemy dialects goes through
    # generic_sql so PG/MySQL/Snowflake share the portable OR/AND builder.
    _key_cols = [c for c in (cursor_key_columns or []) if c]
    if (
        cursor_column
        and len(_key_cols) >= 3
        and src_type
        in ("postgresql", "redshift", "mysql", "snowflake", "sqlite", "generic_sql")
    ):
        from connectors.generic_sql import read_table_cursor_batch as _gs_cursor

        return _gs_cursor(
            host=cfg.get("host", ""),
            port=int(cfg.get("port") or 5432),
            database=cfg.get("database", "") or database,
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=bool(cfg.get("ssl", False)),
            table=table,
            cursor_column=cursor_column,
            cursor_after=cursor_after,
            type=src_type if src_type != "redshift" else "postgresql",
            columns=columns,
            limit=limit,
            cursor_primary_key=cursor_primary_key,
            cursor_key_columns=_key_cols,
        )

    if src_type == "postgresql" or src_type == "redshift":
        from connectors.postgresql_reader import (
            read_table_batch,
            read_table_cursor_batch,
        )

        pg_port = int(cfg.get("port") or (5439 if src_type == "redshift" else 5432))
        if scan_state is not None and not cursor_column:
            from connectors.postgresql_reader import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg["host"],
                port=pg_port,
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "public"),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                scan_state=scan_state,
            )
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=pg_port,
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "public"),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
            )
        return read_table_batch(
            host=cfg["host"],
            port=pg_port,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "public"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "mysql":
        from connectors.mysql_reader import read_table_batch, read_table_cursor_batch

        if scan_state is not None and not cursor_column:
            from connectors.mysql_reader import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 3306),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                scan_state=scan_state,
            )
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 3306),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
            )
        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 3306),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "mongodb":
        from connectors.mongodb_reader import (
            read_collection_batch,
            read_collection_cursor_batch,
        )

        if cursor_column:
            return read_collection_cursor_batch(
                cfg=cfg,
                database=database or cfg.get("database", "test"),
                collection=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                cursor_type=cursor_type,
                columns=columns,
                limit=limit,
                known_total_rows=known_total_rows,
                cursor_primary_key=cursor_primary_key,
            )
        return read_collection_batch(
            cfg=cfg,
            database=database or cfg.get("database", "test"),
            collection=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type == "snowflake":
        from connectors.snowflake_reader import (
            read_table_batch,
            read_table_cursor_batch,
        )
        from services.connector_auth import snowflake_session_kwargs

        session = snowflake_session_kwargs(cfg)
        if scan_state is not None and not cursor_column:
            from connectors.snowflake_reader import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 443),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "PUBLIC"),
                connection_string=cfg.get("connection_string", ""),
                warehouse=cfg.get("warehouse", ""),
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                cursor_primary_key=cursor_primary_key,
                scan_state=scan_state,
                **session,
            )
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 443),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "PUBLIC"),
                connection_string=cfg.get("connection_string", ""),
                warehouse=cfg.get("warehouse", ""),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
                **session,
            )
        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "PUBLIC"),
            connection_string=cfg.get("connection_string", ""),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
            cursor_primary_key=cursor_primary_key,
            **session,
        )
    if src_type == "bigquery":
        from connectors.bigquery_reader import read_table_batch, read_table_cursor_batch

        if scan_state is not None and not cursor_column:
            from connectors.bigquery_reader import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 443),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "dataflow"),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                warehouse=cfg.get("warehouse", ""),
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                service_account=cfg.get("service_account", ""),
                scan_state=scan_state,
            )
        if cursor_column:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=int(cfg.get("port") or 443),
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "dataflow"),
                connection_string=cfg.get("connection_string", ""),
                ssl=cfg.get("ssl", False),
                warehouse=cfg.get("warehouse", ""),
                table=table,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                service_account=cfg.get("service_account", ""),
                cursor_primary_key=cursor_primary_key,
            )
        return read_table_batch(
            host=cfg["host"],
            port=int(cfg.get("port") or 443),
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", "dataflow"),
            connection_string=cfg.get("connection_string", ""),
            ssl=cfg.get("ssl", False),
            warehouse=cfg.get("warehouse", ""),
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
            service_account=cfg.get("service_account", ""),
        )
    if src_type == "gcs":
        from connectors.gcs_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "s3":
        from connectors.s3_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "adls":
        from connectors.adls_reader import read_object

        return read_object(cfg=cfg, bucket=cfg["database"], key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "sftp":
        from connectors.sftp_reader import read_object

        return read_object(cfg=cfg, bucket=cfg.get("database", ""), key=table, offset=offset, limit=limit, known_total_rows=known_total_rows)
    if src_type == "dynamodb":
        from connectors.dynamodb_reader import read_table_batch

        batch, _next = read_table_batch(
            cfg=cfg,
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            exclusive_start_key=dynamodb_cursor,
            total_rows=dynamodb_total,
        )
        return batch, _next
    if src_type == "elasticsearch":
        from connectors.elasticsearch_reader import read_index_batch

        return read_index_batch(
            cfg=cfg, index=table, columns=columns, limit=limit,
            known_total_rows=known_total_rows, search_after=es_search_after,
        )
    if src_type == "redis":
        from connectors.redis_reader import read_keys_batch

        pattern = table or "*"
        if pattern != "*" and "*" not in pattern and "?" not in pattern:
            pattern = f"{pattern}:*"
        return read_keys_batch(
            cfg=cfg, pattern=pattern, limit=limit,
            known_total_rows=known_total_rows, scan_state=redis_scan_state,
        )
    if src_type == "kafka":
        from connectors.kafka_reader import read_topic_batch

        return read_topic_batch(
            cfg=cfg,
            topic=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
            kafka_cursor=kafka_cursor,
        )
    if src_type == "sqlite":
        if cursor_column or cursor_key_columns:
            from connectors.generic_sql import read_table_cursor_batch as _gs_cursor

            return _gs_cursor(
                host=cfg.get("host", ""),
                port=0,
                database=cfg.get("database", "") or database,
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=False,
                table=table,
                cursor_column=cursor_column
                or (cursor_key_columns[0] if cursor_key_columns else ""),
                cursor_after=cursor_after,
                type="sqlite",
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
                cursor_key_columns=cursor_key_columns,
            )
        from connectors.sqlite_reader import read_table_batch

        return read_table_batch(
            host=cfg["host"],
            port=0,
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            table=table,
            limit=limit,
            offset=offset,
            known_total_rows=known_total_rows,
        )
    if resolve_driver_type(src_type) == "generic_sql":
        from connectors.generic_sql import read_table_batch, read_table_cursor_batch

        type_name = cfg.get("type", "") or src_type
        if scan_state is not None and not cursor_column and not cursor_key_columns:
            from connectors.generic_sql import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=False,
                type=type_name,
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                scan_state=scan_state,
            )
        if cursor_column or cursor_key_columns:
            return read_table_cursor_batch(
                host=cfg["host"],
                port=cfg["port"],
                database=cfg["database"],
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=False,
                type=type_name,
                table=table,
                cursor_column=cursor_column
                or (cursor_key_columns[0] if cursor_key_columns else ""),
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
                cursor_key_columns=cursor_key_columns,
            )
        return read_table_batch(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            username=cfg.get("username", ""),
            password=cfg.get("password", ""),
            schema=cfg.get("schema", ""),
            connection_string=cfg.get("connection_string", ""),
            ssl=False,
            type=type_name,
            table=table,
            columns=columns,
            offset=offset,
            limit=limit,
            known_total_rows=known_total_rows,
        )
    if src_type in ("sqlserver", "oracle"):
        # Phase F2 — keyset when cursor columns are provided; else one-SELECT scan.
        if scan_state is not None and not cursor_column and not cursor_key_columns:
            if src_type == "sqlserver":
                from connectors.sqlserver_reader import read_table_scan_batch
            else:
                from connectors.oracle_reader import read_table_scan_batch

            return read_table_scan_batch(
                host=cfg.get("host", ""),
                port=int(cfg.get("port") or (1433 if src_type == "sqlserver" else 1521)),
                database=cfg.get("database", ""),
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "dbo" if src_type == "sqlserver" else ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=bool(cfg.get("ssl", False)),
                table=table,
                columns=columns,
                offset=offset,
                limit=limit,
                known_total_rows=known_total_rows,
                type=src_type,
                scan_state=scan_state,
            )
        if cursor_column or cursor_key_columns:
            if src_type == "sqlserver":
                from connectors.sqlserver_reader import read_table_cursor_batch
            else:
                from connectors.oracle_reader import read_table_cursor_batch

            return read_table_cursor_batch(
                host=cfg.get("host", ""),
                port=int(cfg.get("port") or (1433 if src_type == "sqlserver" else 1521)),
                database=cfg.get("database", ""),
                username=cfg.get("username", ""),
                password=cfg.get("password", ""),
                schema=cfg.get("schema", "dbo" if src_type == "sqlserver" else ""),
                connection_string=cfg.get("connection_string", ""),
                ssl=bool(cfg.get("ssl", False)),
                table=table,
                cursor_column=cursor_column or (cursor_key_columns[0] if cursor_key_columns else ""),
                cursor_after=cursor_after,
                columns=columns,
                limit=limit,
                cursor_primary_key=cursor_primary_key,
                cursor_key_columns=cursor_key_columns,
                type=src_type,
            )
        from .connector_dispatch import read_via_registry

        return read_via_registry(
            src_type,
            cfg=cfg,
            table=table,
            limit=limit,
            offset=offset,
            columns=columns,
        )
    if src_type in ("salesforce", "hubspot"):
        from .connector_dispatch import read_via_registry

        if src_type == "salesforce" and cursor_column:
            # SOQL OFFSET is capped at 2000 rows, so any object larger than that
            # can only be walked by seeking on the cursor (normally Id).
            return read_via_registry(
                src_type,
                cfg=cfg,
                table=table,
                limit=limit,
                offset=0,
                cursor_column=cursor_column,
                cursor_after=cursor_after,
            )
        return read_via_registry(src_type, cfg=cfg, table=table, limit=limit, offset=offset)
    if src_type == "iceberg":
        from .connector_dispatch import read_via_registry

        return read_via_registry(
            "iceberg", cfg=cfg, table=table, limit=limit, offset=offset, columns=columns
        )
    raise ValueError(f"Streaming read not supported for source type '{src_type}'")
