"""Destination schema introspection for real target column discovery."""

from __future__ import annotations

import datetime
import json
from typing import Any


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
        from services.schema_inference import infer_type

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
        return mapped.get(infer_type(samples, field_name=field_name))
    except Exception:
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
        pass
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
) -> dict[str, Any]:
    if db_type == "generic_sql":
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
            "type": catalog_type,
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
        from services.object_store_introspect import introspect_gcs_object, introspect_s3_object

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
        import psycopg2
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
            if table:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, table),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _pg_to_logical(dtype),
                            "nullable": nullable == "YES",
                            "data_type": dtype,
                        }
                    )
                if table:
                    columns = _refine_columns_by_samples(conn, columns, table, schema)
            elif tables:
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, tables[0]),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _pg_to_logical(dtype),
                            "nullable": nullable == "YES",
                            "data_type": dtype,
                        }
                    )
                if tables:
                    columns = _refine_columns_by_samples(conn, columns, tables[0], schema)
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": schema}
    except ImportError:
        return {
            "ok": False,
            "error": "Install psycopg2-binary for live PostgreSQL schema introspection",
            "columns": [],
            "tables": [],
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_snowflake(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = (kwargs.get("schema") or "PUBLIC").upper()
    try:
        from connectors.snowflake_conn import get_connection, normalize_account

        conn = get_connection(
            account=normalize_account(kwargs.get("host", "")),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            database=kwargs.get("database", ""),
            schema=schema,
            warehouse=kwargs.get("warehouse", ""),
            connection_string=kwargs.get("connection_string", ""),
        )
        with conn.cursor() as cur:
            wh = kwargs.get("warehouse", "")
            if wh:
                cur.execute(f"USE WAREHOUSE {wh}")
            db = kwargs.get("database", "")
            if db:
                cur.execute(f"USE DATABASE {db}")
            cur.execute(f"USE SCHEMA {schema}")
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
                cur.execute(
                    """
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (schema, target_table),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append(
                        {
                            "name": name,
                            "inferred_type": _sf_to_logical(dtype),
                            "nullable": nullable == "YES",
                        }
                    )
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": schema}
    except ImportError:
        return {
            "ok": False,
            "error": "Install snowflake-connector-python for live Snowflake schema introspection",
            "columns": [],
            "tables": [],
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_mysql(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from connectors.mysql_conn import get_connection

        conn = get_connection(
            host=kwargs.get("host", ""),
            port=int(kwargs.get("port", 3306)),
            database=kwargs.get("database", ""),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            connection_string=kwargs.get("connection_string", ""),
            ssl=kwargs.get("ssl", False),
        )
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = %s AND table_type = 'BASE TABLE'
                ORDER BY table_name LIMIT 100
                """,
                (kwargs.get("database", ""),),
            )
            tables = [r[0] for r in cur.fetchall()]
            columns: list[dict] = []
            target = table or (tables[0] if tables else None)
            if target:
                cur.execute(
                    """
                    SELECT column_name, column_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (kwargs.get("database", ""), target),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append({
                        "name": name,
                        "inferred_type": _mysql_to_logical(dtype),
                        "nullable": nullable == "YES",
                    })
                if target:
                    columns = _refine_columns_by_samples(conn, columns, target, kwargs.get("database", ""), quote_char="`")
        conn.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": kwargs.get("database", "")}
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
        target = table or (tables[0] if tables else None)
        if target:
            tbl = client.get_table(f"{project_id}.{dataset_id}.{target}")
            for field in tbl.schema:
                columns.append({
                    "name": field.name,
                    "inferred_type": _bq_to_logical(field.field_type),
                    "nullable": field.mode != "REQUIRED",
                })
        return {"ok": True, "tables": tables, "columns": columns, "schema": dataset_id}
    except ImportError:
        return {"ok": False, "error": "Install google-cloud-bigquery for schema introspection", "columns": [], "tables": []}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _bq_to_logical(dtype: str) -> str:
    d = dtype.upper()
    if d in ("INT64", "INTEGER"):
        return "INTEGER"
    if d in ("NUMERIC", "BIGNUMERIC", "FLOAT64"):
        return "DECIMAL"
    if d == "BOOL":
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if "TIMESTAMP" in d:
        return "TIMESTAMP"
    return "TEXT"


def _pg_to_logical(dtype: str) -> str:
    """Map PostgreSQL information_schema.data_type values to DataFlow logical types.

    Uses a precise lookup table so that `interval`, `point`, `geometry`, etc.
    are not incorrectly matched as integers by substring search.
    """
    d = dtype.lower().strip()
    if d in ("integer", "smallint", "bigint", "serial", "bigserial",
             "smallserial", "oid", "xid", "cid", "tid"):
        return "INTEGER"
    if d in ("numeric", "decimal", "real", "double precision", "double",
             "float", "float4", "float8"):
        return "DECIMAL"
    if d == "boolean":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if "timestamp" in d:
        return "TIMESTAMP"
    if d == "time" or "time with" in d or "time without" in d:
        return "TIME"
    if d == "uuid":
        return "UUID"
    if d == "bytea":
        return "BINARY"
    if d == "json" or d == "jsonb" or "array" in d:
        return "JSON"
    if d in ("xml", "tsvector", "tsquery", "text", "character varying",
             "varchar", "character", "char", "name", "interval", "point",
             "line", "lseg", "box", "path", "polygon", "circle", "geometry",
             "geography", "inet", "cidr", "macaddr", "macaddr8", "money",
             "bit", "bit varying", "varbit", "hstore", "pg_lsn",
             "txid_snapshot", "pg_snapshot", "user-defined"):
        return "TEXT"
    return "TEXT"


def _mysql_to_logical(dtype: str) -> str:
    d = (dtype or "").lower()
    if "tinyint(1)" in d:
        return "BOOLEAN"
    if "int" in d:
        return "INTEGER"
    if "numeric" in d or "decimal" in d or "double" in d or "float" in d or "real" in d:
        return "DECIMAL"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if "timestamp" in d or "datetime" in d:
        return "TIMESTAMP"
    if "time" in d:
        return "TIME"
    if "json" in d:
        return "JSON"
    if "binary" in d or "blob" in d or "varbinary" in d:
        return "BINARY"
    if "uuid" in d:
        return "UUID"
    return "TEXT"


def _sf_to_logical(dtype: str) -> str:
    d = dtype.upper()
    if "NUMBER" in d or "INT" in d:
        return "INTEGER" if ",0" in d else "DECIMAL"
    if "BOOLEAN" in d:
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if "TIMESTAMP" in d:
        return "TIMESTAMP"
    return "TEXT"


def _sample_logical_type(value: Any, key: str = "") -> str:
    if value is None:
        return "TEXT"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL"
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
        from services.schema_inference import infer_type

        inferred = infer_type([value], field_name=key)
        if inferred == "JSON":
            return "OBJECT"
        if inferred == "VARCHAR":
            return "TEXT"
        return inferred
    return "TEXT"


def _widen_mongodb_type(current: str, observed: str) -> str:
    """Widen inferred type across sampled documents; prefer more specific type.

    TEXT is the least informative. DECIMAL absorbs INTEGER, TIMESTAMP absorbs DATE,
    UUID/BINARY/OBJECT are retained when observed.

    Any observed or current TEXT/VARCHAR value demotes a column to TEXT so that
    mixed fields (e.g. a referral code that is sometimes a date string and
    sometimes a hex token) do not force a strict date/number type on all rows.
    """
    if current in {"TEXT", "VARCHAR"} or observed in {"TEXT", "VARCHAR"}:
        return "TEXT"
    order = {
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
    return observed if order.get(observed, 0) > order.get(current, 0) else current


def _introspect_mongodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from connectors.mongodb_common import normalize_mongodb_connection_string
        from pymongo import MongoClient

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
            for doc in db[target].find().limit(50):
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                for key, val in doc.items():
                    inferred = _sample_logical_type(val, key)
                    if key not in columns:
                        columns[key] = {"name": key, "inferred_type": inferred, "nullable": True}
                    else:
                        columns[key]["inferred_type"] = _widen_mongodb_type(
                            columns[key]["inferred_type"], inferred
                        )
        client.close()
        return {"ok": True, "tables": tables, "columns": list(columns.values()), "schema": db_name}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_dynamodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table") or kwargs.get("database")
    if not table:
        return {"ok": False, "error": "DynamoDB table name required", "columns": [], "tables": []}
    try:
        from connectors.dynamodb_reader import describe_table_schema, estimate_item_count, list_tables

        cfg = {
            "host": kwargs.get("host") or "us-east-1",
            "port": kwargs.get("port") or 443,
            "username": kwargs.get("username") or "",
            "password": kwargs.get("password") or "",
            "connection_string": kwargs.get("connection_string") or "",
        }
        names, types = describe_table_schema(cfg, table)
        # Sample real items and let the schema inference engine decide types for
        # attributes not defined by the table key schema.
        try:
            from connectors.dynamodb_reader import read_table_batch
            from services.schema_inference import infer_type

            sample, _ = read_table_batch(cfg=cfg, table=table, limit=50)
            if sample.rows:
                samples_by_col: dict[str, list[str]] = {h: [] for h in sample.headers}
                for row in sample.rows:
                    for i, h in enumerate(sample.headers):
                        if i < len(row):
                            samples_by_col[h].append(row[i])
                for name in names:
                    if name in samples_by_col and (types.get(name) == "TEXT" or name not in types):
                        types[name] = infer_type(samples_by_col[name], field_name=name)
        except Exception:
            pass

        columns = [
            {"name": name, "inferred_type": types.get(name, "TEXT"), "nullable": True}
            for name in names
        ]
        tables = [table]
        try:
            tables = list_tables(cfg) or [table]
        except Exception:
            pass
        row_estimate = 0
        try:
            row_estimate = estimate_item_count(cfg, table)
        except Exception:
            pass
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
        from services.schema_inference import infer_type

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

            # Sample real docs so date fields become TIMESTAMP when they carry time
            # and so text/binary/object fields are classified by value, not just mapping.
            samples_by_name: dict[str, list[str]] = {name: [] for name in props}
            try:
                resp = client.search(index=index, body={"size": 50, "query": {"match_all": {}}, "sort": ["_doc"]})
                for hit in resp.get("hits", {}).get("hits") or []:
                    src = hit.get("_source") or {}
                    for name in props:
                        value = src.get(name)
                        if value is None:
                            continue
                        if isinstance(value, (dict, list)):
                            samples_by_name[name].append(json.dumps(value, default=str))
                        elif isinstance(value, (bytes, bytearray)):
                            import base64

                            samples_by_name[name].append(base64.b64encode(value).decode("ascii"))
                        else:
                            samples_by_name[name].append(str(value))
            except Exception:
                pass

            columns = []
            for name, info in props.items():
                es_type = info.get("type", "text")
                mapped = _es_mapping_type(es_type)
                samples = samples_by_name.get(name, [])
                if es_type == "date" or (es_type in ("text", "keyword") and samples):
                    inferred = infer_type(samples, field_name=name)
                    if inferred in ("VARCHAR", "TEXT"):
                        inferred = mapped if mapped != "VARCHAR" else inferred
                    mapped = inferred
                elif es_type == "binary":
                    mapped = "BINARY"
                elif es_type == "object":
                    mapped = "JSON"
                columns.append({
                    "name": name,
                    "inferred_type": mapped,
                    "nullable": True,
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
    if t in ("float", "double", "scaled_float"):
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

    from services.schema_inference import infer_type

    path = connection_string or database or host
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
                pass

            columns: list[dict[str, Any]] = []
            for name in col_names:
                declared = declared_types.get(name, "")
                values = samples.get(name, [])
                if declared == "BLOB" or any(isinstance(v, bytes) for v in values):
                    inferred = "BINARY"
                else:
                    str_values = [v for v in values if not isinstance(v, bytes)]
                    inferred = infer_type(str_values, field_name=name)
                    if inferred in ("VARCHAR", "TEXT") and declared in ("INTEGER", "INT"):
                        inferred = "INTEGER"
                    elif inferred in ("VARCHAR", "TEXT") and declared in ("REAL", "FLOAT", "NUMERIC", "DOUBLE"):
                        inferred = "DECIMAL"

                columns.append(
                    {
                        "name": name,
                        "inferred_type": inferred,
                        "nullable": True,
                    }
                )

            return {"ok": True, "tables": [table], "columns": columns, "schema": ""}
        finally:
            conn.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}
