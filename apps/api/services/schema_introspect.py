"""Destination schema introspection for real target column discovery."""

from __future__ import annotations

import datetime
import json
import logging
import re
from typing import Any

from services.value_serializer import json_default

logger = logging.getLogger(__name__)


def _bson_decimal_type():
    try:
        from bson.decimal128 import Decimal128
        return Decimal128
    except Exception:
        return None


_BSON_DECIMAL = _bson_decimal_type()


def _infer_logical_from_strings(samples: list[str], field_name: str = "") -> str | None:
    """Use DataFlow value inference to narrow TEXT/CHAR columns."""
    try:
        from services.schema_inference import infer_column

        mapped = {
            "JSON": "JSON",
            "BINARY": "BINARY",
            "UUID": "UUID",
            "DATE": "DATE",
            "TIMESTAMP": "DATETIME",
            "TIME": "TIME",
            "BOOLEAN": "BOOLEAN",
            "VARCHAR": "TEXT",
            "TEXT": "TEXT",
        }
        intel = infer_column(samples, field_name=field_name)
        return mapped.get(str(intel["logical_type"]))
    except Exception:
        logger.debug("schema infer_column failed for %s", field_name, exc_info=True)
        return None


def _refine_columns_by_samples(
    conn: Any,
    columns: list[dict],
    table: str,
    schema: str,
    sample_limit: int = 200,
    quote_char: str = '"',
) -> list[dict]:
    """Sample string columns and use heuristics to recover UUID/JSON/BINARY/etc.

    Only refine columns whose database type is character/text. PostgreSQL
    types such as point, bit, interval, money, inet, cidr, macaddr, xml,
    tsvector, and hstore are intentionally stored as TEXT to preserve their
    native formatting, so value-based inference must not override them.
    """
    text_like_db_types = {
        "text", "character varying", "varchar", "character", "char", "name",
    }
    candidates = [
        c for c in columns
        if c["inferred_type"] in ("TEXT", "VARCHAR", "CHAR", "CHARACTER VARYING")
        and (c.get("data_type") or "").lower() in text_like_db_types
    ]
    if not candidates:
        return columns

    q = quote_char
    cols_sql = ", ".join(f"{q}{c['name']}{q}" for c in candidates)
    qualified = f"{q}{schema}{q}.{q}{table}{q}" if schema else f"{q}{table}{q}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {cols_sql} FROM {qualified} LIMIT %s", (sample_limit,))
            rows = cur.fetchall()
        for idx, c in enumerate(candidates):
            values = [row[idx] for row in rows if row[idx] is not None]
            if not values:
                continue
            str_values = [str(v) for v in values]
            inferred = _infer_logical_from_strings(str_values, field_name=c["name"])
            if inferred and inferred != "TEXT":
                c["inferred_type"] = inferred
    except Exception:
        logger.warning(
            "sample refine failed for %s.%s", schema, table, exc_info=True,
        )
    return columns


def introspect_schema(
    db_type: str,
    *,
    host: str = "",
    port: int = 5432,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "public",
    connection_string: str = "",
    ssl: bool = True,
    warehouse: str = "",
    table: str | None = None,
    catalog_type: str = "",
    auth_source: str = "",
    api_key: str = "",
) -> dict[str, Any]:
    if db_type in ("generic_sql", "duckdb"):
        from connectors.generic_sql import introspect_table_schema

        cfg = {
            "host": host,
            "port": port,
            "database": database,
            "username": username,
            "password": password,
            "schema": schema,
            "connection_string": connection_string,
            "ssl": ssl,
            "type": catalog_type or ("duckdb" if db_type == "duckdb" else ""),
        }
        return introspect_table_schema(cfg, table or "")
    if db_type == "postgresql" or db_type == "redshift":
        return _introspect_postgresql(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
            table=table,
        )
    if db_type == "snowflake":
        return _introspect_snowflake(
            host=host,
            database=database,
            username=username,
            password=password,
            schema=schema or "PUBLIC",
            connection_string=connection_string,
            warehouse=warehouse,
            table=table,
        )
    if db_type == "mysql":
        return _introspect_mysql(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
            table=table,
        )
    if db_type in ("oracle", "oracle_db", "amazon_rds_oracle"):
        return _introspect_oracle(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
            table=table,
        )
    if db_type in ("sqlserver", "mssql", "sql_server", "azure_sql"):
        return _introspect_sqlserver(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema or "dbo",
            connection_string=connection_string,
            ssl=ssl,
            table=table,
        )
    if db_type == "bigquery":
        return _introspect_bigquery(
            database=database,
            schema=schema or "dataflow",
            connection_string=connection_string,
            table=table,
        )
    if db_type == "mongodb":
        return _introspect_mongodb(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            connection_string=connection_string,
            auth_source=auth_source,
            table=table,
        )
    if db_type == "dynamodb":
        return _introspect_dynamodb(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database,
            table=table,
        )
    if db_type == "elasticsearch":
        return _introspect_elasticsearch(
            host=host,
            port=port,
            username=username,
            password=password,
            connection_string=connection_string,
            ssl=ssl,
            database=database,
            table=table,
        )
    if db_type == "salesforce":
        return _introspect_salesforce(
            host=host,
            database=database,
            table=table,
            connection_string=connection_string,
            api_key=api_key,
            username=username,
            password=password,
        )
    if db_type == "hubspot":
        return _introspect_hubspot(
            host=host,
            database=database,
            table=table,
            connection_string=connection_string,
            api_key=api_key,
            username=username,
            password=password,
        )
    if db_type == "kafka":
        return _introspect_kafka(
            host=host,
            port=port,
            database=database,
            table=table,
            connection_string=connection_string,
            username=username,
            password=password,
        )
    if db_type in ("s3", "amazon_s3"):
        return _introspect_object_store("s3", host=host, database=database, table=table, schema=schema, **{
            "username": username, "password": password, "connection_string": connection_string,
        })
    if db_type in ("gcs", "google_cloud_storage"):
        return _introspect_object_store("gcs", host=host, database=database, table=table, schema=schema, **{
            "username": username, "password": password, "connection_string": connection_string,
        })
    if db_type == "redis":
        return _introspect_redis(host=host, port=port, password=password, table=table, connection_string=connection_string)
    if db_type == "sqlite":
        return _introspect_sqlite(database=database, connection_string=connection_string, host=host, table=table)
    return {"ok": False, "error": f"Schema introspection not implemented for {db_type}", "columns": [], "tables": []}


def _introspect_object_store(
    store_type: str,
    *,
    host: str = "",
    database: str = "",
    table: str | None = None,
    schema: str = "",
    username: str = "",
    password: str = "",
    connection_string: str = "",
    **_: Any,
) -> dict[str, Any]:
    cfg = {
        "host": host,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
    }
    bucket = database or ""
    key = table or ""
    prefix = schema or ""
    try:
        from services.object_store_introspect import (
            introspect_gcs_object,
            introspect_s3_object,
        )

        if store_type == "gcs":
            result = introspect_gcs_object(cfg, bucket=bucket, key=key or None, prefix=prefix)
        else:
            result = introspect_s3_object(cfg, bucket=bucket, key=key or None, prefix=prefix)
        if not result.get("ok"):
            return {
                "ok": False,
                "error": result.get("error", "Object introspection failed"),
                "columns": result.get("columns", []),
                "tables": result.get("tables", []),
            }
        return {
            "ok": True,
            "columns": result.get("columns", []),
            "column_types": result.get("schema", {}),
            "tables": result.get("tables", []),
            "row_estimate": result.get("total_rows", 0),
            "object_key": result.get("object_key"),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_redis(
    *,
    host: str = "",
    port: int = 6379,
    password: str = "",
    table: str | None = None,
    connection_string: str = "",
) -> dict[str, Any]:
    cfg = {
        "host": host or "localhost",
        "port": port or 6379,
        "password": password,
        "connection_string": connection_string,
    }
    pattern = table or "*"
    try:
        from services.object_store_introspect import introspect_redis_keys

        result = introspect_redis_keys(cfg, pattern=pattern)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "Redis introspection failed"), "columns": []}
        return {
            "ok": True,
            "columns": result.get("columns", []),
            "column_types": result.get("schema", {}),
            "tables": [pattern],
            "row_estimate": result.get("total_rows", 0),
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_postgresql(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = kwargs.get("schema") or "public"
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=kwargs.get("host", ""),
            port=kwargs.get("port", 5432),
            database=kwargs.get("database", ""),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            connection_string=kwargs.get("connection_string", ""),
            ssl=kwargs.get("ssl", True),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 100
                """,
                (schema,),
            )
            tables = [r[0] for r in cur.fetchall()]

            columns: list[dict] = []
            resolved_schema = schema
            target = table or (tables[0] if tables else None)
            if target:
                columns = _pg_fetch_columns(cur, schema, target)
                # Table may live outside the requested schema (common when UI
                # schema is blank / wrong but database+table are correct).
                if not columns:
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND lower(table_name) = lower(%s)
                        ORDER BY CASE
                          WHEN table_schema = %s THEN 0
                          WHEN table_schema = 'public' THEN 1
                          ELSE 2
                        END
                        LIMIT 5
                        """,
                        (target, schema),
                    )
                    for found_schema, found_table in cur.fetchall() or []:
                        columns = _pg_fetch_columns(cur, found_schema, found_table)
                        if columns:
                            resolved_schema = found_schema
                            target = found_table
                            break
                if columns:
                    columns = _refine_columns_by_samples(
                        conn, columns, target, resolved_schema
                    )
        conn.close()
        return {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": resolved_schema if table else schema,
        }
    except ImportError:
        return {
            "ok": False,
            "error": "Install psycopg2-binary for live PostgreSQL schema introspection",
            "columns": [],
            "tables": [],
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _snowflake_list_schemas(cur: Any) -> list[str]:
    """Return schema names visible in the current database (uppercase)."""
    try:
        cur.execute(
            """
            SELECT schema_name
            FROM information_schema.schemata
            WHERE catalog_name = CURRENT_DATABASE()
               OR catalog_name IS NULL
            ORDER BY schema_name
            LIMIT 200
            """
        )
        names = [str(r[0]).upper() for r in (cur.fetchall() or []) if r and r[0]]
        if names:
            return names
    except Exception:
        logger.debug("Snowflake schemata catalog query failed; trying SHOW SCHEMAS", exc_info=True)
    try:
        cur.execute("SHOW SCHEMAS")
        # SHOW SCHEMAS columns vary; "name" is typically index 1
        rows = cur.fetchall() or []
        names = []
        for row in rows:
            if not row:
                continue
            # Prefer named access when available
            if hasattr(row, "get"):
                n = row.get("name") or row.get("schema_name")
            else:
                n = row[1] if len(row) > 1 else row[0]
            if n:
                names.append(str(n).upper())
        return names
    except Exception:
        logger.debug("SHOW SCHEMAS failed", exc_info=True)
        return []


def _snowflake_resolve_schema(cur: Any, requested: str) -> tuple[str, list[str], str | None]:
    """Pick a usable schema. Returns (schema, available, warning_or_none)."""
    from connectors.sql_identifiers import quote_sql_identifier, snowflake_fold_identifier

    requested = (requested or "PUBLIC").strip() or "PUBLIC"
    # Snowflake unquoted identifiers fold to uppercase — never USE SCHEMA "public".
    candidates = []
    for c in (
        snowflake_fold_identifier(requested),
        requested.upper(),
        "PUBLIC",
        requested,
        requested.lower(),
    ):
        folded = snowflake_fold_identifier(c) if c else ""
        if folded and folded not in candidates:
            candidates.append(folded)

    available = _snowflake_list_schemas(cur)
    available_set = {a.upper() for a in available}

    for cand in candidates:
        try:
            cur.execute(f"USE SCHEMA {quote_sql_identifier(cand)}")
            resolved = snowflake_fold_identifier(cand)
            warning = None
            if snowflake_fold_identifier(requested) != resolved:
                warning = (
                    f"Schema '{requested}' was not usable; using '{resolved}' instead."
                )
            elif available_set and resolved not in available_set:
                warning = None
            return resolved, available, warning
        except Exception as exc:
            msg = str(exc).lower()
            if "002043" in str(exc) or "002003" in str(exc) or "does not exist" in msg or "not exist" in msg:
                continue
            # Unexpected errors (permissions, etc.) — re-raise for outer handler
            raise

    # Requested schema missing: fall back to first available, preferring PUBLIC.
    fallback = None
    if "PUBLIC" in available_set:
        fallback = "PUBLIC"
    elif available:
        fallback = snowflake_fold_identifier(available[0])
    if fallback:
        cur.execute(f"USE SCHEMA {quote_sql_identifier(fallback)}")
        sample = ", ".join(available[:12])
        more = f" (+{len(available) - 12} more)" if len(available) > 12 else ""
        warning = (
            f"Schema '{requested}' does not exist in this database. "
            f"Using '{fallback}'. Available schemas: {sample}{more}."
        )
        return fallback, available, warning

    raise RuntimeError(
        f"Schema '{requested}' does not exist, and no schemas were found in the "
        f"current Snowflake database. Check the database name and that your role "
        f"can see information_schema.schemata."
    )


def _introspect_snowflake(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = (kwargs.get("schema") or "PUBLIC").strip() or "PUBLIC"
    try:
        from connectors.snowflake_conn import get_connection, normalize_account
        from connectors.writer_common import quote_sql_identifier

        conn = get_connection(
            account=normalize_account(kwargs.get("host", "")),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            database=kwargs.get("database", ""),
            schema=schema.upper(),
            warehouse=kwargs.get("warehouse", ""),
            connection_string=kwargs.get("connection_string", ""),
        )

        warnings: list[str] = []
        with conn.cursor() as cur:
            wh = (kwargs.get("warehouse") or "").strip()
            if wh:
                try:
                    cur.execute(f"USE WAREHOUSE {quote_sql_identifier(wh)}")
                except Exception as exc:
                    # Warehouse optional for metadata reads on some accounts.
                    warnings.append(f"USE WAREHOUSE '{wh}' failed: {exc}")
                    logger.info("Snowflake USE WAREHOUSE skipped: %s", exc)

            db = (kwargs.get("database") or "").strip()
            if db:
                try:
                    from connectors.sql_identifiers import snowflake_fold_identifier

                    db_folded = snowflake_fold_identifier(db)
                    cur.execute(f"USE DATABASE {quote_sql_identifier(db_folded)}")
                except Exception as exc:
                    conn.close()
                    return {
                        "ok": False,
                        "error": (
                            f"Snowflake database '{db}' does not exist or is not accessible "
                            f"with the current role ({exc})."
                        ),
                        "columns": [],
                        "tables": [],
                        "schema": schema.upper(),
                    }

            try:
                schema, _available, schema_warning = _snowflake_resolve_schema(cur, schema)
            except RuntimeError as exc:
                conn.close()
                return {
                    "ok": False,
                    "error": str(exc),
                    "columns": [],
                    "tables": [],
                    "schema": (kwargs.get("schema") or "PUBLIC").upper(),
                }
            if schema_warning:
                warnings.append(schema_warning)

            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 100
                """,
                (schema,),
            )
            tables = [r[0] for r in cur.fetchall()]
            columns: list[dict] = []
            target_table = table or (tables[0] if tables else None)
            if target_table:
                from connectors.snowflake_conn import resolve_or_fold_snowflake_table

                try:
                    target_table = resolve_or_fold_snowflake_table(cur, schema, str(target_table))
                except Exception:
                    pass
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE UPPER(table_schema) = UPPER(%s) AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, target_table),
                )
                col_rows = list(cur.fetchall() or [])
                if not col_rows:
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND UPPER(table_name) = UPPER(%s)
                        ORDER BY CASE
                          WHEN UPPER(table_schema) = UPPER(%s) THEN 0
                          WHEN UPPER(table_schema) = 'PUBLIC' THEN 1
                          ELSE 2
                        END
                        LIMIT 5
                        """,
                        (str(target_table), schema),
                    )
                    for found_schema, found_table in cur.fetchall() or []:
                        try:
                            found_table = resolve_or_fold_snowflake_table(
                                cur, found_schema, str(found_table)
                            )
                        except Exception:
                            pass
                        cur.execute(
                            """
                            SELECT column_name, data_type, is_nullable
                            FROM information_schema.columns
                            WHERE UPPER(table_schema) = UPPER(%s) AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (found_schema, found_table),
                        )
                        col_rows = list(cur.fetchall() or [])
                        if col_rows:
                            schema = found_schema
                            target_table = found_table
                            break
                for name, dtype, nullable in col_rows:
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _sf_to_logical(dtype),
                            "nullable": nullable == "YES",
                        }
                    )
        conn.close()
        out: dict[str, Any] = {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": schema,
        }
        if warnings:
            out["warnings"] = warnings
            # Surface the primary warning in error-adjacent field for older UI clients.
            out["message"] = warnings[0]
        return out
    except ImportError:
        return {
            "ok": False,
            "error": "Install snowflake-connector-python for live Snowflake schema introspection",
            "columns": [],
            "tables": [],
        }
    except Exception as exc:
        msg = str(exc)
        # Expected "object does not exist" — actionable, no stack spam.
        if "002043" in msg or "does not exist" in msg.lower():
            logger.info("Snowflake introspect schema/database missing: %s", msg)
            return {
                "ok": False,
                "error": (
                    "Snowflake object does not exist or cannot be accessed. "
                    "Verify database, schema, warehouse, and role. "
                    f"Detail: {msg}"
                ),
                "columns": [],
                "tables": [],
            }
        logger.warning("Snowflake introspect failed: %s", msg, exc_info=True)
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {msg}",
            "columns": [],
            "tables": [],
        }


def _introspect_mysql(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=kwargs.get("host", ""),
            port=int(kwargs.get("port", 3306) or 3306),
            database=kwargs.get("database", ""),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            connection_string=kwargs.get("connection_string", ""),
            ssl=kwargs.get("ssl", True),
        )
        with conn.cursor() as cur:
            # MySQL has no separate schema layer — database is the namespace.
            # Prefer explicit database, then schema field (UI sometimes fills it),
            # then the session default.
            db_name = (kwargs.get("database") or kwargs.get("schema") or "").strip()
            if not db_name:
                cur.execute("SELECT DATABASE()")
                row = cur.fetchone()
                db_name = (row[0] if row else None) or ""
            if not db_name:
                return {
                    "ok": False,
                    "error": "MySQL database name is required for schema introspection",
                    "columns": [],
                    "tables": [],
                }
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 200
                """,
                (db_name,),
            )
            tables = [r[0] for r in cur.fetchall()]
            columns: list[dict] = []
            # Case-insensitive match — Linux MySQL is case-sensitive for table
            # filesystem names but operators often type lowercase.
            requested = (table or "").strip()
            target = None
            if requested:
                for name in tables:
                    if name == requested or name.lower() == requested.lower():
                        target = name
                        break
                if target is None:
                    # Table might exist but sit outside the LIMIT 200 list — probe directly.
                    target = requested
            else:
                target = tables[0] if tables else None
            if target:
                cur.execute(
                    """
                    SELECT column_name, column_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (db_name, target),
                )
                rows = cur.fetchall()
                if not rows:
                    cur.execute(
                        """
                        SELECT column_name, column_type, is_nullable
                        FROM information_schema.columns
                        WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)
                        ORDER BY ordinal_position
                        """,
                        (db_name, requested or target),
                    )
                    rows = cur.fetchall()
                # Wrong database in the form is common (UI filled schema as DB).
                # Search other schemas the account can see before inventing create-new.
                if not rows:
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND LOWER(table_name) = LOWER(%s)
                        ORDER BY CASE
                          WHEN table_schema = %s THEN 0
                          ELSE 1
                        END
                        LIMIT 5
                        """,
                        (requested or target, db_name),
                    )
                    for found_db, found_table in cur.fetchall() or []:
                        cur.execute(
                            """
                            SELECT column_name, column_type, is_nullable
                            FROM information_schema.columns
                            WHERE table_schema = %s AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (found_db, found_table),
                        )
                        rows = cur.fetchall()
                        if rows:
                            db_name = found_db
                            target = found_table
                            break
                for name, dtype, nullable in rows:
                    columns.append({
                        "name": name,
                        "inferred_type": _mysql_to_logical(dtype),
                        "nullable": nullable == "YES",
                    })
                if columns:
                    columns = _refine_columns_by_samples(
                        conn, columns, target, db_name, quote_char="`"
                    )
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": db_name}
    except ImportError:
        return {"ok": False, "error": "Install pymysql for MySQL schema introspection", "columns": [], "tables": []}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_bigquery(**kwargs) -> dict[str, Any]:
    project_id = kwargs.get("database", "")
    dataset_id = kwargs.get("schema") or "dataflow"
    table = kwargs.get("table")
    try:
        from connectors.bigquery_conn import get_client

        client = get_client(project_id=project_id, credentials_path=kwargs.get("connection_string", ""))
        tables = [t.table_id for t in client.list_tables(f"{project_id}.{dataset_id}", max_results=100)]
        columns: list[dict] = []
        resolved_dataset = dataset_id
        target = table or (tables[0] if tables else None)
        if target:
            try:
                tbl = client.get_table(f"{project_id}.{dataset_id}.{target}")
            except Exception:
                tbl = None
            if tbl is None and table:
                # Wrong dataset in the form — scan a bounded set of datasets.
                try:
                    datasets = list(client.list_datasets(max_results=25))
                except Exception:
                    datasets = []
                for ds in datasets:
                    ds_id = getattr(ds, "dataset_id", None) or str(ds)
                    try:
                        tbl = client.get_table(f"{project_id}.{ds_id}.{table}")
                    except Exception:
                        continue
                    if tbl is not None:
                        resolved_dataset = ds_id
                        target = table
                        break
            if tbl is not None:
                for field in tbl.schema:
                    columns.append({
                        "name": field.name,
                        "inferred_type": _bq_field_to_logical(field),
                        "nullable": getattr(field, "mode", "NULLABLE") != "REQUIRED",
                    })
        return {"ok": True, "tables": tables, "columns": columns, "schema": resolved_dataset}
    except ImportError:
        return {"ok": False, "error": "Install google-cloud-bigquery for schema introspection", "columns": [], "tables": []}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _bq_to_logical(dtype: str, *, precision: int | None = None, scale: int | None = None) -> str:
    d = (dtype or "").upper()
    if d in ("INT64", "INTEGER"):
        return "INTEGER"
    if d in ("NUMERIC", "BIGNUMERIC"):
        if precision is not None and scale is not None:
            return f"DECIMAL({int(precision)},{int(scale)})"
        if precision is not None:
            return f"DECIMAL({int(precision)})"
        return "DECIMAL"
    if d in ("FLOAT64", "FLOAT", "DOUBLE"):
        return "FLOAT"
    if d == "BOOL":
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if d == "TIME":
        return "TIME"
    # BigQuery DATETIME is wall-clock NTZ; TIMESTAMP is UTC instant (TZ-aware).
    if d == "DATETIME":
        return "TIMESTAMP_NTZ"
    if d == "TIMESTAMP" or "TIMESTAMP" in d:
        return "TIMESTAMPTZ"
    if d == "INTERVAL":
        return "INTERVAL"
    if d == "BYTES":
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if d == "GEOGRAPHY":
        return "GEOGRAPHY"
    if d in ("RECORD", "STRUCT"):
        return "JSON"
    if d == "STRING":
        return "TEXT"
    return "TEXT"


def _bq_field_to_logical(field: Any) -> str:
    """Preserve BigQuery RECORD/ARRAY nesting — never collapse child types to bare JSON."""
    precision = getattr(field, "precision", None)
    scale = getattr(field, "scale", None)
    ftype = str(getattr(field, "field_type", "") or "")
    mode = str(getattr(field, "mode", "NULLABLE") or "NULLABLE").upper()
    children = list(getattr(field, "fields", None) or [])

    if ftype.upper() in {"RECORD", "STRUCT"} and children:
        parts: list[str] = []
        for child in children:
            child_mode = str(getattr(child, "mode", "NULLABLE") or "NULLABLE").upper()
            child_t = _bq_field_to_logical(child)
            # Avoid double ARRAY<> when child is already REPEATED.
            if child_mode == "REPEATED" and not child_t.upper().startswith("ARRAY<"):
                child_t = f"ARRAY<{child_t}>"
            parts.append(f"{child.name}:{child_t}")
        base = f"STRUCT<{', '.join(parts)}>"
    else:
        base = _bq_to_logical(
            ftype,
            precision=precision if isinstance(precision, int) else None,
            scale=scale if isinstance(scale, int) else None,
        )

    if mode == "REPEATED" and not base.upper().startswith("ARRAY<"):
        return f"ARRAY<{base}>"
    return base


_PG_COLUMN_SQL = """
SELECT a.attname AS column_name,
       format_type(a.atttypid, a.atttypmod) AS data_type,
       CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = %s
  AND c.relname = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""


def _pg_fetch_columns(cur: Any, schema: str, table: str) -> list[dict]:
    """Read column metadata via ``format_type`` so typmod (vector dim, decimal p,s) survives."""
    cur.execute(_PG_COLUMN_SQL, (schema, table))
    columns: list[dict] = []
    for name, dtype, nullable in cur.fetchall():
        columns.append(
            {
                "name": name,
                "inferred_type": _pg_to_logical(str(dtype or "")),
                "nullable": str(nullable).upper() == "YES",
                "data_type": dtype,
            }
        )
    return columns


def _pg_to_logical(dtype: str) -> str:
    """Map PostgreSQL ``format_type`` / data_type strings to DataFlow logical carriers.

    Parametric types keep their dimensions in the type string (DECIMAL(p,s),
    VECTOR(n)) so ``ddl_type`` can propagate them — same contract as DECIMAL.
    INTERVAL / GEOGRAPHY / VECTOR are first-class; they must not collapse to TEXT.
    """
    raw = (dtype or "").strip()
    d = raw.lower()

    # DECIMAL / NUMERIC with typmod — preserve (p,s) for transfer fidelity.
    m = re.match(r"^(numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"

    # pgvector / halfvec — preserve dimension.
    m = re.match(r"^(vector|halfvec|sparsevec)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"VECTOR({m.group(2)})"
    if d in {"vector", "halfvec", "sparsevec"}:
        return "VECTOR"

    if d == "interval" or d.startswith("interval "):
        return "INTERVAL"
    if d in {"geometry", "geography"} or d.startswith("geometry(") or d.startswith("geography("):
        return "GEOGRAPHY"

    if d in ("integer", "smallint", "bigint", "serial", "bigserial",
             "smallserial", "oid", "xid", "cid", "tid"):
        return "INTEGER"
    # IEEE floats stay FLOAT — never silently rewrite to fixed-point DECIMAL.
    if d in ("real", "double precision", "double", "float", "float4", "float8"):
        return "FLOAT"
    if d in ("numeric", "decimal", "money"):
        return "DECIMAL"
    if d == "boolean":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    # Preserve TZ polarity (Redshift TIMESTAMPTZ shares this mapper).
    if "timestamp" in d:
        if "with time zone" in d or d in {"timestamptz", "timestamp with time zone"}:
            return "TIMESTAMPTZ"
        # Explicit NTZ token so ddl_type does not invent TIMESTAMPTZ on PG.
        return "TIMESTAMP_NTZ"
    if d == "time" or d.startswith("time with") or d.startswith("time without"):
        return "TIME"
    if d == "uuid":
        return "UUID"
    if d == "bytea":
        return "BINARY"
    # Redshift SUPER / VARBYTE (exposed via PG wire format_type).
    if d == "super":
        return "JSON"
    if d == "varbyte" or d.startswith("varbyte("):
        return "BINARY"
    if d in {"json", "jsonb"} or d.endswith("[]") or " array" in d:
        return "JSON"
    if d.startswith("bit(") or d == "bit" or d.startswith("bit varying") or d == "varbit":
        # BIT(1) → boolean via type_system; BIT(n>1) → binary.
        return raw.upper() if "(" in d else "BIT"
    if d in ("xml", "tsvector", "tsquery", "text", "character varying",
             "varchar", "character", "char", "name",
             "line", "lseg", "box", "path", "polygon", "circle",
             "inet", "cidr", "macaddr", "macaddr8", "pg_lsn",
             "txid_snapshot", "pg_snapshot", "user-defined"):
        return "TEXT"
    if d == "hstore":
        return "JSON"
    if d == "point":
        return "GEOGRAPHY"
    # character varying(n) / character(n)
    if d.startswith("character varying") or d.startswith("varchar") or d.startswith("character("):
        return "TEXT"
    return "TEXT"

def _mysql_to_logical(dtype: str) -> str:
    """Map MySQL ``column_type`` to logical carriers, preserving DECIMAL(p,s)."""
    raw = (dtype or "").strip()
    d = raw.lower()
    if "tinyint(1)" in d:
        return "BOOLEAN"
    # Preserve DECIMAL(p,s) / NUMERIC(p,s) from column_type for ddl_type propagation.
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)", d)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d.startswith("decimal") or d.startswith("numeric"):
        return "DECIMAL"
    if any(tok in d for tok in ("geometry", "point", "polygon", "linestring", "multipoint",
                                  "multipolygon", "multilinestring", "geomcollection")):
        return "GEOGRAPHY"
    # BIGINT UNSIGNED exceeds signed 64-bit — DECIMAL carrier (matches type_system CANONICAL).
    # Must run BEFORE the generic "int" branch ("int" is a substring of "bigint").
    if "unsigned" in d and "bigint" in d:
        return "BIGINT UNSIGNED"
    if d == "year" or d.startswith("year("):
        return "INTEGER"
    if "int" in d:
        return "INTEGER"
    # IEEE float/double/real — distinct from DECIMAL(p,s).
    if "double" in d or "float" in d or "real" in d:
        return "FLOAT"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    # MySQL TIMESTAMP is session-TZ aware; DATETIME is wall-clock NTZ.
    if "timestamp" in d and "datetime" not in d:
        return "TIMESTAMPTZ"
    if "datetime" in d:
        return "TIMESTAMP_NTZ"
    if d.startswith("time"):
        return "TIME"
    if "json" in d:
        return "JSON"
    if "binary" in d or "blob" in d or "varbinary" in d:
        return "BINARY"
    if "uuid" in d:
        return "UUID"
    return "TEXT"


def _oracle_to_logical(dtype: str) -> str:
    """Map Oracle data_type (+ optional precision/scale) to logical carriers.

    NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0). NUMBER(p,s) → DECIMAL(p,s).
    BINARY_FLOAT/DOUBLE → FLOAT. Oracle DATE includes time-of-day → TIMESTAMP.
    """
    raw = (dtype or "").strip()
    d = raw.upper().replace(" ", "")
    # NUMBER(p,s) / FLOAT(p) carriers
    m = re.match(r"^NUMBER\((\d+)(?:,(\d+))?\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(2) is not None and int(m.group(2)) == 0:
            return zero_scale_numeric_carrier(int(m.group(1)))
        if m.group(2) is not None:
            return f"DECIMAL({m.group(1)},{m.group(2)})"
        return f"DECIMAL({m.group(1)})"
    if d == "NUMBER" or d.startswith("NUMBER("):
        return "DECIMAL"
    if d in {"BINARY_FLOAT", "BINARY_DOUBLE"} or d.startswith("FLOAT"):
        return "FLOAT"
    if d in {"INTEGER", "INT", "SMALLINT", "BIGINT"}:
        return "INTEGER"
    if d == "BOOLEAN":
        return "BOOLEAN"
    if d == "DATE":
        return "TIMESTAMP"  # Oracle DATE is datetime
    if "TIMESTAMP" in d:
        if "WITHLOCALTIMEZONE" in d or "WITHTIMEZONE" in d:
            return "TIMESTAMPTZ"
        return "TIMESTAMP_NTZ"
    if d.startswith("INTERVAL"):
        return "INTERVAL"
    if d in {"CLOB", "NCLOB", "LONG", "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "VARCHAR"}:
        return "TEXT"
    if d in {"BLOB", "RAW", "LONGRAW", "BFILE"} or d.startswith("RAW("):
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if "SDO_GEOMETRY" in d or d in {"GEOMETRY", "GEOGRAPHY"}:
        return "GEOGRAPHY"
    return "TEXT"


def _sqlserver_to_logical(dtype: str) -> str:
    """Map SQL Server type_name (+ optional (p,s)) to logical carriers."""
    raw = (dtype or "").strip()
    d = raw.lower()
    m = re.match(r"^(decimal|numeric)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d.startswith("decimal") or d.startswith("numeric"):
        return "DECIMAL"
    if d in {"money", "smallmoney"}:
        return "DECIMAL(19,4)" if d == "money" else "DECIMAL(10,4)"
    if d in {"float", "real"}:
        return "FLOAT"
    if d in {"int", "bigint", "smallint", "tinyint"}:
        return "INTEGER"
    if d == "bit":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if d == "time" or d.startswith("time("):
        return "TIME"
    if d == "datetimeoffset" or d.startswith("datetimeoffset"):
        return "TIMESTAMPTZ"
    if d in {"datetime", "datetime2", "smalldatetime"} or d.startswith("datetime"):
        return "TIMESTAMP_NTZ"
    if d == "uniqueidentifier":
        return "UUID"
    if d == "json":
        return "JSON"
    if d == "xml":
        return "TEXT"
    if d in {"geography", "geometry"}:
        return "GEOGRAPHY"
    if d in {"binary", "varbinary", "image", "rowversion", "timestamp"}:
        return "BINARY"
    if "binary" in d or d.endswith("(max)") and "varbinary" in d:
        return "BINARY"
    if any(tok in d for tok in ("nvarchar", "varchar", "nchar", "char", "text", "ntext", "sysname")):
        return "TEXT"
    return "TEXT"


def _introspect_oracle(**kwargs) -> dict[str, Any]:
    """Oracle ALL_TAB_COLUMNS introspect with NUMBER(p,s) / FLOAT honesty."""
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import _engine
    except Exception:
        return {
            "ok": False,
            "error": "Install oracledb/SQLAlchemy for Oracle introspection",
            "columns": [],
            "tables": [],
        }

    table = (kwargs.get("table") or "").strip()
    schema = (kwargs.get("schema") or kwargs.get("username") or "").strip().upper()
    cfg = {
        "type": "oracle",
        "host": kwargs.get("host") or "",
        "port": int(kwargs.get("port") or 1521),
        "database": kwargs.get("database") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "schema": schema,
        "connection_string": kwargs.get("connection_string") or "",
        "ssl": bool(kwargs.get("ssl", True)),
    }
    try:
        engine = _engine(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    try:
        with engine.connect() as conn:
            tables: list[str] = []
            if schema:
                rows = conn.execute(
                    sa.text(
                        "SELECT table_name FROM all_tables WHERE owner = :owner ORDER BY table_name"
                    ),
                    {"owner": schema},
                ).fetchall()
            else:
                rows = conn.execute(
                    sa.text("SELECT table_name FROM user_tables ORDER BY table_name")
                ).fetchall()
            tables = [r[0] for r in rows]

            if not table:
                return {"ok": True, "columns": [], "tables": tables, "schema": schema}

            owner = schema or (kwargs.get("username") or "").upper()
            col_rows = conn.execute(
                sa.text(
                    """
                    SELECT column_name, data_type, data_precision, data_scale, nullable
                    FROM all_tab_columns
                    WHERE owner = :owner AND table_name = :table
                    ORDER BY column_id
                    """
                ),
                {"owner": owner, "table": table.upper()},
            ).fetchall()
            if not col_rows:
                found = conn.execute(
                    sa.text(
                        """
                        SELECT owner, table_name FROM all_tables
                        WHERE UPPER(table_name) = UPPER(:table)
                        ORDER BY CASE
                          WHEN owner = :owner THEN 0
                          ELSE 1
                        END
                        FETCH FIRST 5 ROWS ONLY
                        """
                    ),
                    {"table": table, "owner": owner},
                ).fetchall()
                for found_owner, found_table in found or []:
                    col_rows = conn.execute(
                        sa.text(
                            """
                            SELECT column_name, data_type, data_precision, data_scale, nullable
                            FROM all_tab_columns
                            WHERE owner = :owner AND table_name = :table
                            ORDER BY column_id
                            """
                        ),
                        {"owner": found_owner, "table": found_table},
                    ).fetchall()
                    if col_rows:
                        owner = found_owner
                        break
            columns: list[dict] = []
            for name, data_type, precision, scale, nullable in col_rows:
                dtype = str(data_type or "")
                if str(data_type or "").upper() == "NUMBER" and precision is not None:
                    if scale is not None:
                        dtype = f"NUMBER({int(precision)},{int(scale)})"
                    else:
                        dtype = f"NUMBER({int(precision)})"
                columns.append(
                    {
                        "name": name,
                        "inferred_type": _oracle_to_logical(dtype),
                        "nullable": str(nullable).upper() == "Y",
                        "data_type": dtype,
                    }
                )
            return {"ok": True, "columns": columns, "tables": tables, "schema": owner}
    except Exception as exc:
        logger.warning("oracle introspect failed", exc_info=True)
        try:
            from connectors.generic_sql import introspect_table_schema

            info = introspect_table_schema(cfg, table)
            if info.get("ok") and info.get("columns"):
                for col in info["columns"]:
                    inferred = str(col.get("inferred_type") or "").lower()
                    if inferred in {"float", "double"}:
                        col["inferred_type"] = "FLOAT"
                    elif inferred.startswith("decimal"):
                        col["inferred_type"] = col["inferred_type"].upper() if "(" in inferred else "DECIMAL"
                return info
        except Exception:
            pass
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def _introspect_sqlserver(**kwargs) -> dict[str, Any]:
    """SQL Server INFORMATION_SCHEMA introspect with FLOAT≠DECIMAL honesty."""
    try:
        import sqlalchemy as sa

        from connectors.generic_sql import _engine
    except Exception:
        return {
            "ok": False,
            "error": "Install pyodbc/SQLAlchemy for SQL Server introspection",
            "columns": [],
            "tables": [],
        }

    table = (kwargs.get("table") or "").strip()
    schema = (kwargs.get("schema") or "dbo").strip()
    cfg = {
        "type": "sqlserver",
        "host": kwargs.get("host") or "",
        "port": int(kwargs.get("port") or 1433),
        "database": kwargs.get("database") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "schema": schema,
        "connection_string": kwargs.get("connection_string") or "",
        "ssl": bool(kwargs.get("ssl", True)),
    }
    try:
        engine = _engine(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    try:
        with engine.connect() as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    sa.text(
                        """
                        SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = :schema
                        ORDER BY TABLE_NAME
                        """
                    ),
                    {"schema": schema},
                ).fetchall()
            ]
            if not table:
                return {"ok": True, "columns": [], "tables": tables, "schema": schema}

            col_rows = conn.execute(
                sa.text(
                    """
                    SELECT
                      c.COLUMN_NAME,
                      c.DATA_TYPE,
                      c.NUMERIC_PRECISION,
                      c.NUMERIC_SCALE,
                      c.IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS c
                    WHERE c.TABLE_SCHEMA = :schema AND c.TABLE_NAME = :table
                    ORDER BY c.ORDINAL_POSITION
                    """
                ),
                {"schema": schema, "table": table},
            ).fetchall()
            if not col_rows:
                found = conn.execute(
                    sa.text(
                        """
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_TYPE = 'BASE TABLE'
                          AND LOWER(TABLE_NAME) = LOWER(:table)
                        ORDER BY CASE
                          WHEN TABLE_SCHEMA = :schema THEN 0
                          WHEN TABLE_SCHEMA = 'dbo' THEN 1
                          ELSE 2
                        END
                        """
                    ),
                    {"table": table, "schema": schema},
                ).fetchall()
                for found_schema, found_table in found or []:
                    col_rows = conn.execute(
                        sa.text(
                            """
                            SELECT
                              c.COLUMN_NAME,
                              c.DATA_TYPE,
                              c.NUMERIC_PRECISION,
                              c.NUMERIC_SCALE,
                              c.IS_NULLABLE
                            FROM INFORMATION_SCHEMA.COLUMNS c
                            WHERE c.TABLE_SCHEMA = :schema AND c.TABLE_NAME = :table
                            ORDER BY c.ORDINAL_POSITION
                            """
                        ),
                        {"schema": found_schema, "table": found_table},
                    ).fetchall()
                    if col_rows:
                        schema = found_schema
                        break
            columns: list[dict] = []
            for name, data_type, precision, scale, nullable in col_rows:
                dtype = str(data_type or "")
                base = dtype.lower()
                if base in {"decimal", "numeric"} and precision is not None:
                    if scale is not None:
                        dtype = f"{base}({int(precision)},{int(scale)})"
                    else:
                        dtype = f"{base}({int(precision)})"
                columns.append(
                    {
                        "name": name,
                        "inferred_type": _sqlserver_to_logical(dtype),
                        "nullable": str(nullable).upper() == "YES",
                        "data_type": dtype,
                    }
                )
            return {"ok": True, "columns": columns, "tables": tables, "schema": schema}
    except Exception as exc:
        logger.warning("sqlserver introspect failed", exc_info=True)
        try:
            from connectors.generic_sql import introspect_table_schema

            return introspect_table_schema(cfg, table)
        except Exception:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


def _sf_to_logical(dtype: str) -> str:
    """Map Snowflake data types, preserving VECTOR(FLOAT, n) and NUMBER(p,s)."""
    raw = (dtype or "").strip()
    d = raw.upper()
    if "VECTOR" in d:
        return raw  # keep VECTOR(FLOAT, n) carrier
    if "GEOGRAPHY" in d or "GEOMETRY" in d:
        return "GEOGRAPHY"
    if "INTERVAL" in d:
        return "INTERVAL"
    # Semi-structured — never collapse to TEXT (Airbyte-class catalog honesty).
    if d in {"VARIANT", "OBJECT"}:
        return "JSON"
    if d == "ARRAY" or d.startswith("ARRAY"):
        return "ARRAY"
    if d == "BINARY" or d.startswith("BINARY("):
        return "BINARY"
    if d == "TIME" or d.startswith("TIME("):
        return "TIME"
    # NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0) (never silent BIGINT overflow).
    m = re.match(r"^(NUMBER|DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if "NUMBER" in d or "DECIMAL" in d or "NUMERIC" in d or "INT" in d:
        return "INTEGER" if ",0" in d.replace(" ", "") else "DECIMAL"
    # Snowflake FLOAT / DOUBLE / REAL — approximate IEEE, not NUMBER.
    if d in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"} or d.startswith("FLOAT"):
        return "FLOAT"
    if "BOOLEAN" in d:
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    # Preserve TZ polarity when Snowflake declared it (Map/Validate can warn).
    if "TIMESTAMP_TZ" in d or "TIMESTAMP_LTZ" in d:
        return "TIMESTAMPTZ"
    if "TIMESTAMP_NTZ" in d:
        return "TIMESTAMP_NTZ"
    if "TIMESTAMP" in d:
        return "TIMESTAMP_NTZ"
    return "TEXT"


def _sample_logical_type(value: Any, key: str = "") -> str:
    if value is None:
        # Null/absent is unknown, not TEXT. Returning "" keeps a null observation
        # from demoting a field that is typed (e.g. OBJECT) in other documents.
        return ""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, datetime.datetime):
        return "TIMESTAMP"
    if isinstance(value, datetime.date):
        return "DATE"
    if _BSON_DECIMAL and isinstance(value, _BSON_DECIMAL):
        return "DECIMAL"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "OBJECT"
    if isinstance(value, str):
        from services.schema_inference import infer_column

        inferred = str(infer_column([value], field_name=key)["logical_type"])
        if inferred == "JSON":
            return "OBJECT"
        if inferred == "VARCHAR":
            return "TEXT"
        return inferred
    return "TEXT"


_STRUCTURAL_TYPES = {"OBJECT", "ARRAY", "JSON"}
_TEXTUAL_TYPES = {"TEXT", "VARCHAR"}
_MONGO_TYPE_ORDER = {
    "TEXT": 0,
    "VARCHAR": 0,
    "BOOLEAN": 1,
    "INTEGER": 2,
    "DECIMAL": 3,
    "DATE": 4,
    "UUID": 5,
    "TIMESTAMP": 6,
    "BINARY": 7,
    "ARRAY": 8,
    "OBJECT": 9,
    "JSON": 9,
}
# Keep a typed inference when ≥85% of non-null samples agree (Airbyte-class
# majority vote). Below that, TEXT is safer than a false INTEGER/DATE.
_MONGO_TYPED_MAJORITY = 0.85


def _widen_mongodb_type(current: str, observed: str) -> str:
    """Widen inferred type across sampled documents; prefer more specific type.

    Prefer :func:`_finalize_mongodb_type` with per-type counts for accuracy.
    This pairwise helper remains for incremental callers.
    """
    if not observed:
        return current
    if not current:
        return observed
    if current in _STRUCTURAL_TYPES or observed in _STRUCTURAL_TYPES:
        if current in _STRUCTURAL_TYPES and observed in _STRUCTURAL_TYPES:
            return current if current == observed else "JSON"
        return current if current in _STRUCTURAL_TYPES else observed
    if current in _TEXTUAL_TYPES or observed in _TEXTUAL_TYPES:
        return "TEXT"
    return observed if _MONGO_TYPE_ORDER.get(observed, 0) > _MONGO_TYPE_ORDER.get(current, 0) else current


def _finalize_mongodb_type(type_counts: dict[str, int]) -> str:
    """Majority-vote Mongo field type — one TEXT sentinel must not demote 49 ints."""
    counts = {str(k).upper(): int(v) for k, v in (type_counts or {}).items() if v and k}
    total = sum(counts.values())
    if total <= 0:
        return "TEXT"

    structural = {k: counts[k] for k in _STRUCTURAL_TYPES if counts.get(k, 0) > 0}
    if structural:
        # Sticky: any nested observation keeps a semi-structured type.
        if "OBJECT" in structural and "ARRAY" in structural:
            return "JSON"
        return max(structural, key=lambda k: (structural[k], _MONGO_TYPE_ORDER.get(k, 0)))

    text_n = counts.get("TEXT", 0) + counts.get("VARCHAR", 0)
    typed = {k: v for k, v in counts.items() if k not in _TEXTUAL_TYPES}
    if not typed:
        return "TEXT"

    # Promote INTEGER+DECIMAL → DECIMAL, DATE+TIMESTAMP → TIMESTAMP.
    if "DECIMAL" in typed and "INTEGER" in typed:
        typed["DECIMAL"] = typed.get("DECIMAL", 0) + typed.pop("INTEGER", 0)
    if "TIMESTAMP" in typed and "DATE" in typed:
        typed["TIMESTAMP"] = typed.get("TIMESTAMP", 0) + typed.pop("DATE", 0)

    best = max(typed, key=lambda k: (typed[k], _MONGO_TYPE_ORDER.get(k, 0)))
    typed_share = sum(typed.values()) / total
    if typed_share >= _MONGO_TYPED_MAJORITY:
        return best
    if text_n / total >= (1.0 - _MONGO_TYPED_MAJORITY):
        return "TEXT"
    return best


def _introspect_mongodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from pymongo import MongoClient

        from connectors.mongodb_common import normalize_mongodb_connection_string

        conn_str = normalize_mongodb_connection_string(
            kwargs.get("connection_string", ""),
            database=kwargs.get("database", ""),
            host=kwargs.get("host", ""),
            port=int(kwargs.get("port") or 0),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            ssl=bool(kwargs.get("ssl")),
            auth_source=kwargs.get("auth_source", ""),
        )
        client = MongoClient(conn_str, serverSelectionTimeoutMS=10000)
        db_name = kwargs.get("database") or "test"
        db = client[db_name]
        tables = db.list_collection_names()[:100]
        target = table or (tables[0] if tables else None)
        columns: dict[str, dict[str, Any]] = {}
        if target:
            for doc in db[target].find().limit(100):
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                for key, val in doc.items():
                    inferred = _sample_logical_type(val, key)
                    sample_text = "" if val is None else str(val)
                    if key not in columns:
                        # Null-first fields stay untyped until a non-null sample
                        # votes — do not invent TEXT from BSON null alone.
                        columns[key] = {
                            "name": key,
                            "inferred_type": inferred,
                            "nullable": val is None,
                            "samples": [sample_text] if sample_text else [],
                            "type_counts": {},
                        }
                    else:
                        if val is None:
                            columns[key]["nullable"] = True
                        samples = columns[key].setdefault("samples", [])
                        if sample_text and len(samples) < 8 and sample_text not in samples:
                            samples.append(sample_text)
                    if inferred and val is not None:
                        tc = columns[key].setdefault("type_counts", {})
                        tc[inferred] = int(tc.get(inferred, 0)) + 1
        client.close()
        for col in columns.values():
            counts = col.pop("type_counts", {}) or {}
            if counts:
                col["inferred_type"] = _finalize_mongodb_type(counts)
            elif not col.get("inferred_type"):
                col["inferred_type"] = "TEXT"
            # Re-infer from samples only when majority vote stayed textual / weak —
            # never overwrite a high-confidence sticky numeric/date/structural type.
            samples = [s for s in (col.get("samples") or []) if str(s).strip()]
            if (
                len(samples) >= 2
                and col["inferred_type"] not in _STRUCTURAL_TYPES
                and col["inferred_type"] in _TEXTUAL_TYPES
            ):
                try:
                    from services.schema_inference import infer_column

                    intel = infer_column(samples, field_name=col["name"])
                    logical = str(intel.get("logical_type") or col["inferred_type"])
                    if logical == "VARCHAR":
                        logical = "TEXT"
                    # Never narrow a sticky OBJECT/ARRAY with scalar-only re-infer.
                    if col["inferred_type"] not in _STRUCTURAL_TYPES:
                        col["inferred_type"] = logical
                except Exception:
                    logger.debug("Mongo sample re-infer failed for %s", col.get("name"), exc_info=True)
        return {"ok": True, "tables": tables, "columns": list(columns.values()), "schema": db_name}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

def _introspect_dynamodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table") or kwargs.get("database")
    if not table:
        return {"ok": False, "error": "DynamoDB table name required", "columns": [], "tables": []}
    try:
        from connectors.dynamodb_reader import (
            DDB_NULL_SENTINEL,
            describe_table_schema,
            estimate_item_count,
            list_tables,
            union_attribute_keys,
        )

        cfg = {
            "host": kwargs.get("host") or "us-east-1",
            "port": kwargs.get("port") or 443,
            "username": kwargs.get("username") or "",
            "password": kwargs.get("password") or "",
            "connection_string": kwargs.get("connection_string") or "",
        }
        names, types = describe_table_schema(cfg, table)
        # Sample real items — union every attribute (sparse keys) + native types.
        try:
            from connectors.dynamodb_reader import read_table_batch
            from services.schema_inference import infer_schema_map

            sample, _ = read_table_batch(cfg=cfg, table=table, limit=50)
            if sample.headers:
                names = union_attribute_keys(names, sample.headers)
            meta = getattr(sample, "meta", None) or {}
            native_types = meta.get("native_types") if isinstance(meta, dict) else {}
            if isinstance(native_types, dict):
                for name, lt in native_types.items():
                    if name not in types or types.get(name) in {"VARCHAR", "TEXT", "S"}:
                        types[name] = str(lt)
            if sample.rows:
                samples_by_col: dict[str, list[str]] = {h: [] for h in sample.headers}
                for row in sample.rows:
                    for i, h in enumerate(sample.headers):
                        if i < len(row):
                            cell = row[i]
                            # Skip explicit Dynamo NULL sentinel for inference.
                            if cell == DDB_NULL_SENTINEL or cell == "":
                                continue
                            samples_by_col[h].append(cell)
                inferred_map, _intel = infer_schema_map(samples_by_col)
                for name in names:
                    if name in inferred_map and (types.get(name) in {"TEXT", "VARCHAR", "S"} or name not in types):
                        types[name] = inferred_map[name]
        except Exception:
            logger.warning("DynamoDB sample inference failed for %s", table, exc_info=True)

        columns = [
            {"name": name, "inferred_type": types.get(name, "TEXT"), "nullable": True}
            for name in names
        ]
        tables = [table]
        try:
            tables = list_tables(cfg) or [table]
        except Exception:
            logger.warning("DynamoDB list_tables failed", exc_info=True)
        row_estimate = 0
        try:
            row_estimate = estimate_item_count(cfg, table)
        except Exception:
            logger.debug("DynamoDB estimate_item_count failed for %s", table, exc_info=True)
        return {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": table,
            "row_estimate": row_estimate,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_elasticsearch(**kwargs) -> dict[str, Any]:
    index = kwargs.get("table") or kwargs.get("database")
    if not index:
        return {"ok": False, "error": "Elasticsearch index name required", "columns": [], "tables": []}
    try:
        from connectors.elasticsearch_reader import _client
        from services.schema_inference import infer_column

        cfg = {
            "host": kwargs.get("host") or "localhost",
            "port": kwargs.get("port") or 9200,
            "username": kwargs.get("username") or "",
            "password": kwargs.get("password") or "",
            "connection_string": kwargs.get("connection_string") or "",
            "ssl": kwargs.get("ssl", False),
        }
        client = _client(cfg)
        try:
            if not client.indices.exists(index=index):
                return {"ok": False, "error": f"Index `{index}` not found", "columns": [], "tables": []}
            mapping = client.indices.get_mapping(index=index)
            props = (
                mapping.get(index, {})
                .get("mappings", {})
                .get("properties", {})
            )

            # Sample real docs — union mapping ⊕ dynamic _source keys (no silent miss).
            samples_by_name: dict[str, list[str]] = {name: [] for name in props}
            dynamic_keys: dict[str, None] = {}
            try:
                resp = client.search(index=index, body={"size": 50, "query": {"match_all": {}}, "sort": ["_doc"]})
                for hit in resp.get("hits", {}).get("hits") or []:
                    src = hit.get("_source") or {}
                    for name, value in src.items():
                        if name not in props and name not in dynamic_keys:
                            dynamic_keys[name] = None
                        if name not in samples_by_name:
                            samples_by_name[name] = []
                        if value is None:
                            continue
                        if isinstance(value, (dict, list)):
                            samples_by_name[name].append(json.dumps(value, default=json_default))
                        elif isinstance(value, (bytes, bytearray)):
                            import base64

                            samples_by_name[name].append(base64.b64encode(value).decode("ascii"))
                        else:
                            samples_by_name[name].append(str(value))
            except Exception:
                logger.warning(
                    "Elasticsearch sample fetch failed for index %s", index, exc_info=True,
                )

            columns = []
            for name, info in props.items():
                es_type = info.get("type", "text")
                mapped = _es_mapping_type(es_type)
                samples = samples_by_name.get(name, [])
                semantic_role = None
                if es_type == "date" or (es_type in ("text", "keyword") and samples):
                    intel = infer_column(samples, field_name=name)
                    inferred = str(intel["logical_type"])
                    semantic_role = intel.get("semantic_role")
                    if inferred in ("VARCHAR", "TEXT"):
                        inferred = mapped if mapped != "VARCHAR" else inferred
                    mapped = inferred
                elif es_type == "binary":
                    mapped = "BINARY"
                elif es_type == "object":
                    mapped = "JSON"
                col_rec: dict[str, Any] = {
                    "name": name,
                    "inferred_type": mapped,
                    "nullable": True,
                }
                if semantic_role:
                    col_rec["semantic_role"] = semantic_role
                columns.append(col_rec)
            # Dynamic fields present in docs but absent from index mapping.
            for name in dynamic_keys:
                samples = samples_by_name.get(name, [])
                intel = infer_column(samples, field_name=name) if samples else {"logical_type": "TEXT"}
                columns.append({
                    "name": name,
                    "inferred_type": str(intel.get("logical_type") or "TEXT"),
                    "nullable": True,
                    "semantic_role": intel.get("semantic_role"),
                })
            return {"ok": True, "tables": [index], "columns": columns, "schema": index}
        finally:
            client.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _es_mapping_type(es_type: str) -> str:
    t = (es_type or "text").lower()
    if t in ("long", "integer", "short", "byte"):
        return "INTEGER"
    if t in ("float", "double"):
        return "FLOAT"
    if t == "scaled_float":
        # Elasticsearch scaled_float is fixed-point-like — keep as DECIMAL.
        return "DECIMAL"
    if t == "boolean":
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    return "TEXT"


def _introspect_sqlite(
    *,
    database: str = "",
    connection_string: str = "",
    host: str = "",
    table: str | None = None,
) -> dict[str, Any]:
    """Introspect a SQLite table using PRAGMA table_info plus sample-value inference.

    SQLite is dynamically typed, so we read the declared affinity and then sample
    rows to recover logical types (BOOLEAN, DATE, JSON, UUID, etc.) that cannot be
    determined from affinity alone.
    """
    import sqlite3

    from connectors.sqlite_common import sqlite_file_path

    try:
        path = sqlite_file_path(database or "", connection_string or "", host or "")
    except ValueError as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}
    if not path:
        return {"ok": False, "error": "SQLite path is required", "columns": [], "tables": []}
    if not table:
        return {"ok": False, "error": "SQLite table name is required", "columns": [], "tables": []}

    try:
        conn = sqlite3.connect(path, timeout=8)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cur.fetchone():
                return {"ok": False, "error": f"Table `{table}` not found", "columns": [], "tables": []}

            cur.execute(f'PRAGMA table_info("{table}")')
            info_rows = cur.fetchall()
            if not info_rows:
                return {"ok": False, "error": f"No columns for table `{table}`", "columns": [], "tables": []}

            col_names: list[str] = [row[1] for row in info_rows]
            declared_types: dict[str, str] = {row[1]: (row[2] or "").upper() for row in info_rows}

            # Sample up to 100 rows for value-based inference
            samples: dict[str, list[str]] = {name: [] for name in col_names}
            try:
                cur.execute(f'SELECT * FROM "{table}" LIMIT 100')
                for row in cur.fetchall():
                    for i, name in enumerate(col_names):
                        value = row[i]
                        if isinstance(value, bytes):
                            # keep BLOB as-is for BINARY classification
                            samples[name].append(value)
                        else:
                            samples[name].append(str(value) if value is not None else "")
            except Exception:
                logger.warning("SQLite sample read failed for %s", table, exc_info=True)

            columns: list[dict[str, Any]] = []
            for name in col_names:
                declared = declared_types.get(name, "")
                declared_base = declared.split("(", 1)[0].strip().upper()
                values = samples.get(name, [])
                semantic_role = None
                if declared == "BLOB" or any(isinstance(v, bytes) for v in values):
                    inferred = "BINARY"
                else:
                    from services.schema_inference import infer_column

                    str_values = [v for v in values if not isinstance(v, bytes)]
                    intel = infer_column(str_values, field_name=name)
                    inferred = str(intel["logical_type"])
                    semantic_role = intel.get("semantic_role")
                    # Prefer declared affinity over sample narrowing. SQLite stores
                    # NUMERIC(38,15) values as ints when they have no fraction, and
                    # sample inference alone would report INTEGER — then SCD2/upsert
                    # re-runs falsely block DECIMAL → INTEGER as lossy.
                    if declared_base in {"NUMERIC", "DECIMAL", "NUMBER"} or declared.startswith(
                        ("NUMERIC", "DECIMAL", "NUMBER")
                    ):
                        inferred = "DECIMAL"
                    elif inferred in ("VARCHAR", "TEXT") and declared_base in {"INTEGER", "INT", "BIGINT"}:
                        inferred = "INTEGER"
                    elif inferred in ("VARCHAR", "TEXT") and declared_base in {
                        "REAL", "FLOAT", "DOUBLE"
                    }:
                        inferred = "DECIMAL"

                col_out: dict[str, Any] = {
                    "name": name,
                    "inferred_type": inferred,
                    "nullable": True,
                }
                if semantic_role:
                    col_out["semantic_role"] = semantic_role
                columns.append(col_out)

            return {"ok": True, "tables": [table], "columns": columns, "schema": ""}
        finally:
            conn.close()
    except Exception as exc:
        logger.warning("SQLite introspect failed for %s", table, exc_info=True)
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: SQLite schema introspection failed",
            "columns": [],
            "tables": [],
        }


def salesforce_field_to_logical(
    field_type: str,
    *,
    precision: int | None = None,
    scale: int | None = None,
) -> str:
    """Map Salesforce describe field type → DataFlow logical carrier.

    Currency/percent without Describe precision still get honest DECIMAL defaults
    (never bare DECIMAL that invents warehouse NUMBER). Datetime is UTC → TIMESTAMPTZ.
    """
    t = (field_type or "string").strip().lower()
    if t in {"boolean"}:
        return "BOOLEAN"
    if t in {"int", "long", "integer"}:
        return "INTEGER"
    if t in {"double", "currency", "percent"}:
        if precision is not None and scale is not None:
            p, s = int(precision), int(scale)
            if s == 0 and p <= 18:
                return "INTEGER"
            return f"DECIMAL({p},{s})"
        if t == "currency":
            return "DECIMAL(18,2)"
        if t == "percent":
            return "DECIMAL(18,2)"
        return "FLOAT"
    if t == "date":
        return "DATE"
    if t == "datetime":
        return "TIMESTAMPTZ"
    if t == "time":
        return "TIME"
    if t == "base64":
        return "BINARY"
    if t in {"address", "location", "complexvalue", "json"}:
        return "JSON"
    if t == "id":
        return "TEXT"  # Salesforce Ids are 15/18-char strings, not UUID
    # string, textarea, phone, url, email, picklist, reference, …
    return "TEXT"


def hubspot_property_to_logical(
    prop_type: str,
    *,
    field_type: str = "",
    number_display_hint: str = "",
    name: str = "",
) -> str:
    """Map HubSpot property type → DataFlow logical carrier.

    Uses ``fieldType`` / ``numberDisplayHint`` when present so currency and
    whole-number properties do not all collapse to bare DECIMAL.
    """
    t = (prop_type or "string").strip().lower()
    ft = (field_type or "").strip().lower()
    hint = (number_display_hint or "").strip().lower()
    n = (name or "").strip().lower()
    if t == "bool" or ft == "booleancheckbox":
        return "BOOLEAN"
    if t == "number":
        if hint in {"currency"}:
            return "DECIMAL(18,2)"
        if hint in {"percentage", "percent"}:
            return "DECIMAL(18,2)"
        if hint == "duration":
            return "INTEGER"
        if ft in {"calculation_equation", "calculation_score", "calculation_read_time"}:
            return "FLOAT"
        if (
            n.endswith(("_count", "count"))
            or n.endswith("_num")
            or n in {"num_employees", "numberofemployees", "hs_object_id"}
            or n.endswith("numberofemployees")
        ):
            return "INTEGER"
        return "DECIMAL"
    if t == "date":
        return "DATE"
    if t == "datetime":
        return "TIMESTAMPTZ"
    if t == "json" or ft in {"html", "calculation_equation"}:
        return "JSON" if t == "json" else "TEXT"
    # string, enumeration, phone_number, …
    return "TEXT"


def _saas_cfg(**kwargs: Any) -> dict[str, Any]:
    return {
        "host": kwargs.get("host") or "",
        "database": kwargs.get("database") or "",
        "table": kwargs.get("table") or "",
        "connection_string": kwargs.get("connection_string") or "",
        "api_key": kwargs.get("api_key") or "",
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
    }


def _introspect_salesforce(**kwargs: Any) -> dict[str, Any]:
    """Salesforce Describe → typed columns for Map / preflight."""
    try:
        from connectors.salesforce import describe_sobject, list_sobjects
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    cfg = _saas_cfg(**kwargs)
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    try:
        tables = list_sobjects(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    if not table:
        return {"ok": True, "columns": [], "tables": tables, "schema": ""}

    try:
        fields = describe_sobject(cfg, table)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": tables}

    columns = [
        {
            "name": f["name"],
            "inferred_type": salesforce_field_to_logical(
                str(f.get("type") or "string"),
                precision=f.get("precision") if isinstance(f.get("precision"), int) else None,
                scale=f.get("scale") if isinstance(f.get("scale"), int) else None,
            ),
            "nullable": bool(f.get("nillable", True)),
            "data_type": f.get("type") or "string",
            "label": f.get("label") or "",
        }
        for f in fields
        if f.get("name")
    ]
    return {"ok": True, "columns": columns, "tables": tables, "schema": table}


def _introspect_hubspot(**kwargs: Any) -> dict[str, Any]:
    """HubSpot Properties API → typed columns for Map / preflight."""
    try:
        from connectors.hubspot import describe_properties, list_object_types
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    cfg = _saas_cfg(**kwargs)
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    try:
        tables = list_object_types(cfg)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}

    if not table:
        return {"ok": True, "columns": [], "tables": tables, "schema": ""}

    try:
        props = describe_properties(cfg, table)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": tables}

    columns = [
        {
            "name": "id",
            "inferred_type": "TEXT",
            "nullable": False,
            "data_type": "string",
            "label": "Record ID",
        }
    ]
    for p in props:
        if not p.get("name") or p["name"] == "id":
            continue
        columns.append(
            {
                "name": p["name"],
                "inferred_type": hubspot_property_to_logical(
                    str(p.get("type") or "string"),
                    field_type=str(p.get("fieldType") or ""),
                    number_display_hint=str(p.get("numberDisplayHint") or ""),
                    name=str(p.get("name") or ""),
                ),
                "nullable": True,
                "data_type": p.get("type") or "string",
                "label": p.get("label") or "",
            }
        )
    return {"ok": True, "columns": columns, "tables": tables, "schema": table}


def _kafka_value_to_logical(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        return "INTEGER"
    if isinstance(value, float):
        return "FLOAT"
    if isinstance(value, dict):
        return "JSON"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, str):
        try:
            from services.schema_inference import infer_column

            inferred = str(infer_column([value], field_name="")["logical_type"])
            return "TEXT" if inferred == "VARCHAR" else inferred
        except Exception:
            return "TEXT"
    return "TEXT"


def _introspect_kafka(**kwargs: Any) -> dict[str, Any]:
    """Infer Kafka topic field types from a small poll of JSON/Debezium envelopes."""
    try:
        from connectors.kafka_reader import infer_topic_schema
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    topic = (kwargs.get("table") or kwargs.get("database") or "").strip()
    registry = str(
        kwargs.get("schema_registry_url")
        or (
            kwargs.get("api_key")
            if str(kwargs.get("api_key") or "").startswith("http")
            else ""
        )
        or (
            kwargs.get("connection_string")
            if str(kwargs.get("connection_string") or "").startswith("http")
            else ""
        )
        or ""
    ).strip()
    cfg = {
        "host": kwargs.get("host") or "localhost",
        "port": int(kwargs.get("port") or 9092),
        "database": kwargs.get("database") or "",
        "table": topic,
        "connection_string": (
            ""
            if str(kwargs.get("connection_string") or "").startswith("http")
            else (kwargs.get("connection_string") or "")
        ),
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "schema_registry_url": registry,
    }
    if not topic:
        return {"ok": True, "columns": [], "tables": [], "schema": ""}
    try:
        schema_map, native, warning = infer_topic_schema(cfg, topic, sample_limit=50)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": [topic]}

    columns = [
        {
            "name": name,
            "inferred_type": logical,
            "nullable": True,
            "data_type": native.get(name, logical),
        }
        for name, logical in schema_map.items()
    ]
    out: dict[str, Any] = {"ok": True, "columns": columns, "tables": [topic], "schema": topic}
    if warning:
        out["warning"] = warning
    return out
