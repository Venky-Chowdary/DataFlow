"""Destination schema introspection for real target column discovery."""

from __future__ import annotations

from typing import Any


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
) -> dict[str, Any]:
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
            table=table,
        )
    if db_type == "dynamodb":
        return _introspect_dynamodb(
            host=host,
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
                        }
                    )
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
                        }
                    )
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
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                    """,
                    (kwargs.get("database", ""), target),
                )
                for name, dtype, nullable in cur.fetchall():
                    columns.append({
                        "name": name,
                        "inferred_type": _pg_to_logical(dtype),
                        "nullable": nullable == "YES",
                    })
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
    d = dtype.lower()
    if "int" in d:
        return "INTEGER"
    if "numeric" in d or "decimal" in d or "double" in d or "real" in d:
        return "DECIMAL"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if "timestamp" in d:
        return "TIMESTAMP"
    if "json" in d:
        return "JSON"
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


def _sample_logical_type(value: Any) -> str:
    if value is None:
        return "TEXT"
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        return "INTEGER"
    if isinstance(value, float):
        return "DECIMAL"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "OBJECT"
    return "TEXT"


def _introspect_mongodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from pymongo import MongoClient

        if kwargs.get("connection_string"):
            conn_str = kwargs["connection_string"]
        elif kwargs.get("username") and kwargs.get("password"):
            conn_str = (
                f"mongodb://{kwargs['username']}:{kwargs['password']}"
                f"@{kwargs.get('host', 'localhost')}:{kwargs.get('port', 27017)}/"
            )
        else:
            conn_str = f"mongodb://{kwargs.get('host', 'localhost')}:{kwargs.get('port', 27017)}/"
        client = MongoClient(conn_str, serverSelectionTimeoutMS=10000)
        db_name = kwargs.get("database") or "test"
        db = client[db_name]
        tables = db.list_collection_names()[:100]
        columns: list[dict] = []
        target = table or (tables[0] if tables else None)
        if target:
            for doc in db[target].find().limit(50):
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
                for key, val in doc.items():
                    existing = next((c for c in columns if c["name"] == key), None)
                    inferred = _sample_logical_type(val)
                    if existing is None:
                        columns.append({"name": key, "inferred_type": inferred, "nullable": True})
        client.close()
        return {"ok": True, "tables": tables, "columns": columns, "schema": db_name}
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
            columns = [
                {
                    "name": name,
                    "inferred_type": _es_mapping_type(info.get("type", "text")),
                    "nullable": True,
                }
                for name, info in props.items()
            ]
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
