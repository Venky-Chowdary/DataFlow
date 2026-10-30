"""Destination schema introspection for real target column discovery."""

from __future__ import annotations

import datetime
import json
import logging
import re
import sys
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from services.unique_key_introspect import (
    _mysql_fetch_unique_keys,
    _pg_fetch_unique_keys,
    _snowflake_fetch_unique_keys,
    _sqlite_fetch_unique_keys,
)
from services.catalog_defaults import normalize_catalog_default
from services.check_constraints import probe_check_constraints
from services.foreign_key_metadata import probe_foreign_keys
from services.identity_carry import apply_identity_probe
from services.physical_storage_metadata import probe_physical_storage
from services.secondary_indexes import probe_secondary_indexes
from services.value_serializer import cell_to_string, evidence_text, json_default

logger = logging.getLogger(__name__)
_si = sys.modules[__name__]


def _catalog_type_head(token: str) -> str:
    """Bare temporal/family token for comparing lattice vs catalog spellings."""
    raw = (token or "").strip().upper()
    if not raw:
        return ""
    raw = re.sub(r"\s*\(\s*\d+(?:\s*,\s*\d+)?\s*\)", "", raw)
    if "WITHOUT TIME ZONE" in raw:
        return "TIMESTAMP_NTZ"
    if "WITH LOCAL TIME ZONE" in raw:
        return "TIMESTAMP_LTZ"
    if "WITH TIME ZONE" in raw:
        return "TIMESTAMPTZ"
    return raw.split()[0]


def attach_declared_catalog_type(
    col: dict[str, Any],
    physical: str = "",
    *,
    dest_db: str = "",
) -> dict[str, Any]:
    """Keep the destination catalog's DDL beside the logical lattice.

    Destination probes restore ``declared_type`` when ``logical_translated``
    (``src.transfer.adapters_introspect._columns_schema_meta``). BigQuery already
    stamps both; SQL engines that map ``datetime(6)`` → ``TIMESTAMP_NTZ(6)`` did
    not, so ``POST /map`` returned a type MySQL does not spell while the UI
    printed ``DATETIME(6)``.

    ``tinyint(1)`` → ``BOOLEAN`` is not a temporal lattice alias and must not
    flip dest Map to ``TINYINT(1)``.
    """
    logical = str(col.get("inferred_type") or "").strip()
    phys = (physical or "").strip()
    legalized = ""
    if dest_db and logical:
        from services.type_system import materialize_dest_ddl

        legalized = str(materialize_dest_ddl(dest_db, logical) or "").strip()
    declared = legalized or phys
    if not declared:
        return col
    log_h = _catalog_type_head(logical)
    decl_h = _catalog_type_head(declared)
    if not (log_h and decl_h and log_h != decl_h):
        return col
    temporal = log_h.startswith("TIMESTAMP") or decl_h.startswith("TIMESTAMP") or decl_h in {
        "DATETIME",
        "DATETIME2",
        "DATETIMEOFFSET",
        "SMALLDATETIME",
        "DATE",
        "TIME",
    }
    if not temporal:
        return col
    col["declared_type"] = declared
    col["native_type"] = declared
    col["logical_translated"] = True
    return col


def _mysql_catalog_physical(dtype: str) -> str:
    """INFORMATION_SCHEMA COLUMN_TYPE as Map/CREATE spelling (``DATETIME(6)``)."""
    raw = (dtype or "").strip()
    if not raw:
        return ""
    low = raw.lower()
    if low.startswith("enum(") or low.startswith("set("):
        kind, _, rest = raw.partition("(")
        return f"{kind.upper()}({rest}"
    return raw.upper()


def _bson_decimal_type():
    try:
        from bson.decimal128 import Decimal128
        return Decimal128
    except Exception:
        return None


_BSON_DECIMAL = _bson_decimal_type()


def _infer_logical_from_strings(samples: list[str], field_name: str = "") -> str | None:
    """Use Datawrap value inference to narrow TEXT/CHAR columns."""
    try:
        from services.schema_inference import infer_column

        mapped = {
            "JSON": "JSON",
            "BINARY": "BINARY",
            "UUID": "UUID",
            "DATE": "DATE",
            "TIMESTAMP": "DATETIME",
            "TIMESTAMPTZ": "TIMESTAMPTZ",
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


def _as_int(value: Any, fallback: int) -> int:
    """Catalog number as an int, whatever container the driver used.

    SQL Server returns ``sys.identity_columns.seed_value`` as ``sql_variant``,
    which pyodbc hands back as little-endian bytes — ``int()`` raises on it, and
    swallowing that loses the source's key progression.
    """
    if isinstance(value, bool) or value is None:
        return fallback
    if isinstance(value, bytes):
        return int.from_bytes(value, "little", signed=True)
    try:
        return int(value)
    except (TypeError, ValueError):
        return fallback


def _destination_probe(kwargs: dict[str, Any]) -> bool:
    """A destination read reports the physical column, never its rows' look.

    The writer binds into the catalog type. A ``text`` column of run 1's table
    holding ``'t'`` / ``'20240101'`` refined to BOOLEAN / DATE made run 2 plan a
    lossy TEXT → BOOLEAN cast into a column that is TEXT (D190 / D194).
    """
    return str(kwargs.get("introspect_purpose") or "").strip().lower() == "destination"


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

    from connectors.sql_identifiers import quote_sql_identifier

    q = quote_char
    cols_sql = ", ".join(quote_sql_identifier(c["name"], q) for c in candidates)
    qualified = (
        f"{quote_sql_identifier(schema, q)}.{quote_sql_identifier(table, q)}"
        if schema
        else quote_sql_identifier(table, q)
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {cols_sql} FROM {qualified} LIMIT %s", (sample_limit,))  # nosec B608
            rows = cur.fetchall()
        for idx, c in enumerate(candidates):
            values = [row[idx] for row in rows if row[idx] is not None]
            if not values:
                continue
            str_values = [str(v) for v in values]
            inferred = _infer_logical_from_strings(str_values, field_name=c["name"])
            # An offset string (``Z`` or ``±HH:MM``) is text that happens to
            # look like a timestamp. Promoting it to DATETIME drops the offset.
            if inferred in {"DATETIME", "TIMESTAMP", "TIMESTAMPTZ", "DATE"} and any(
                re.search(r"(?:Z|[+-]\d{2}:?\d{2})\s*$", str(value).strip(), re.I)
                for value in str_values
            ):
                continue
            if inferred and inferred != "TEXT":
                # Keep the catalog carrier: value inference describes the rows a
                # source holds, not what a destination column will accept. A
                # ``text`` sink that currently holds timestamps still accepts any
                # string, and reading it back as DATETIME reports a fidelity
                # collapse (VARCHAR → DATETIME) the physical column does not have.
                c.setdefault("declared_type", c["inferred_type"])
                c["sample_refined"] = True
                c["inferred_type"] = inferred
    except Exception:
        logger.warning(
            "sample refine failed for %s.%s", schema, table, exc_info=True,
        )
    return columns


#: Engines whose catalog keys on (namespace, bare object name). Studio may carry
#: a qualified name ("public.orders", "SALES.PUBLIC.ORDERS"), and
#: ``information_schema`` stores ``orders`` — never ``public.orders`` — so the
#: name has to be split before the lookup. Document stores are deliberately
#: absent: a MongoDB collection name may legitimately contain a dot.
_NAMESPACED_SQL_ENGINES = frozenset({
    "postgresql", "redshift", "pgvector", "mysql", "mariadb", "snowflake",
    "sqlserver", "mssql", "sql_server", "azure_sql", "oracle", "oracle_db",
    "amazon_rds_oracle", "bigquery", "generic_sql", "duckdb", "clickhouse",
    "trino", "presto",
})


def split_object_namespace(
    db_type: str, table: str | None, *, schema: str, database: str
) -> tuple[str, str, str]:
    """``(schema, database, object)`` for a possibly qualified object name.

    A source table that sorted outside the bounded object listing was declared
    "not found" for exactly this reason: the listing is what normalises
    ``public.vt_src`` to ``vt_src``, so past the listing cap the catalog was
    asked for a table literally named ``public.vt_src`` and answered no rows.
    Existence must never depend on where a name sorts in a truncated page.
    """
    name = (table or "").strip()
    if not name or (db_type or "").lower() not in _NAMESPACED_SQL_ENGINES:
        return schema, database, name
    parts = [p.strip().strip('"').strip("`").strip("[]") for p in name.split(".")]
    parts = [p for p in parts if p]
    if len(parts) < 2:
        return schema, database, name
    leaf = parts[-1]
    namespace = parts[-2]
    if (db_type or "").lower() in {"mysql", "mariadb"}:
        # MySQL has no schema layer: the qualifier is the database, and the
        # introspector prefers ``database`` over ``schema``.
        return schema, namespace, leaf
    catalog = parts[-3] if len(parts) >= 3 else ""
    return namespace, (catalog or database), leaf


def introspect_schema(db_type: str, **kwargs: Any) -> dict[str, Any]:
    """Introspect ``db_type``, with a schemaless store's sampled widths unbound.

    A document / keyspace store has no column DDL, so its probe reports the
    widest value the sampled page held. That width is not a declaration: the
    first run would create a destination column sized to the page, and the next
    wider value would be refused for a bound the source never had. One owner
    for both roles — the same probe feeds Map's source profile and the
    dest-exists shape.
    """
    from services.type_system import (
        destination_carriers_are_inferred,
        sampled_numeric_carriers_unbound,
    )

    from services.introspect_memo import memoized_introspection

    result = memoized_introspection(
        db_type, kwargs, lambda: _introspect_schema(db_type, **kwargs)
    )
    if not destination_carriers_are_inferred(db_type):
        return result
    types = result.get("column_types")
    if isinstance(types, dict):
        result["column_types"] = sampled_numeric_carriers_unbound(types)
    cols = result.get("columns")
    if isinstance(cols, list):
        from services.type_system import unbound_sampled_numeric_carrier

        for col in cols:
            if isinstance(col, dict) and isinstance(col.get("type"), str):
                col["type"] = unbound_sampled_numeric_carrier(col["type"])
    return result


def _introspect_schema(db_type: str, **kwargs: Any) -> dict[str, Any]:
    """Introspect under the connector egress policy (pre-dial check + DNS pin)."""
    from services.egress_guard import connector_dial_scope

    reader_cfg = kwargs.get("reader_cfg") or {}
    scope_cfg = {
        "host": str(kwargs.get("host") or ""),
        "connection_string": str(kwargs.get("connection_string") or ""),
        "endpoint_url": str(kwargs.get("endpoint_url") or ""),
        "type": db_type,
        "workspace_id": str(
            kwargs.get("workspace_id")
            or (reader_cfg.get("workspace_id") if isinstance(reader_cfg, dict) else "")
            or ""
        ),
    }
    with connector_dial_scope(scope_cfg, db_type=db_type, point="introspect"):
        return _introspect_schema_unscoped(db_type, **kwargs)


def _introspect_schema_unscoped(
    db_type: str,
    *,
    host: str = "",
    port: int = 5432,
    database: str = "",
    username: str = "",
    password: str = "",
    schema: str = "public",
    connection_string: str = "",
    ssl: bool | None = None,
    warehouse: str = "",
    table: str | None = None,
    catalog_type: str = "",
    auth_source: str = "",
    api_key: str = "",
    role: str = "",
    auth_role: str = "",
    private_key: str = "",
    strict_namespace: bool = False,
    reader_cfg: dict[str, Any] | None = None,
    introspect_purpose: str = "",
    **options: Any,
) -> dict[str, Any]:
    """Load tables/columns for ``table`` in the requested database/schema.

    ``reader_cfg`` asks a document store for the shape its Execute reader will
    emit (the connector config the reader is built from). Without it MongoDB
    reports raw top-level keys, which is right for ``$group`` pipelines but not
    for a transfer plan: the reader expands nested objects into ``parent_child``
    columns, and a plan built from the raw keys never maps them.

    ``options`` carries the connection-affecting extras from the connector
    config (``connectors.generic_sql.connection_options``): TLS material,
    Oracle service name/SID, MSSQL driver and failover keywords. Introspection
    must open the *same* connection the transfer will, otherwise a route whose
    writer connects fine is reported unreachable (and vice versa).

    ``strict_namespace=True`` (destination probes): never steal columns from
    another database/schema. A missing object in the operator-chosen namespace
    must report empty columns so Studio can honestly create-on-write — not
    claim ``users`` exists in ``railway`` because another DB on the host has it.
    """
    # Timescale is the Postgres catalog. Cockroach stays unresolved here so a
    # planned engine does not inherit a live schema probe.
    if db_type in {"timescaledb", "timescale"}:
        db_type = "postgresql"
    # MariaDB is the MySQL catalog (same INFORMATION_SCHEMA, same driver).
    if db_type in {"mariadb", "maria"}:
        db_type = "mysql"
    if ssl is None:
        # A caller that never said anything about TLS must not make it mandatory.
        from services.connector_store import default_ssl

        ssl = default_ssl(db_type)
    if db_type in {
        "generic_sql",
        "duckdb",
        "clickhouse",
        "trino",
        "presto",
        "questdb",
        "risingwave",
    }:
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
            "type": catalog_type or db_type,
            **options,
        }
        return introspect_table_schema(cfg, table or "")
    # A qualified name is resolved once, here, so every engine branch below asks
    # its catalog for the bare object in the right namespace.
    schema, database, table = split_object_namespace(
        db_type, table, schema=schema, database=database
    )
    if db_type in {"postgresql", "redshift", "pgvector"}:
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
            strict_namespace=strict_namespace,
            # Redshift constraints are informational — mark advisory like BQ.
            advisory_keys=(db_type == "redshift"),
            introspect_purpose=introspect_purpose,
            **options,
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
            role=role,
            auth_role=auth_role,
            private_key=private_key,
            strict_namespace=strict_namespace,
            introspect_purpose=introspect_purpose,
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
            schema=schema,
            strict_namespace=strict_namespace,
            introspect_purpose=introspect_purpose,
            **options,
        )
    if db_type in ("oracle", "oracle_db", "amazon_rds_oracle"):
        return _si._introspect_oracle(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema,
            connection_string=connection_string,
            ssl=ssl,
            table=table,
            strict_namespace=strict_namespace,
            **options,
        )
    if db_type in ("sqlserver", "mssql", "sql_server", "azure_sql"):
        return _si._introspect_sqlserver(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            schema=schema or "dbo",
            connection_string=connection_string,
            ssl=ssl,
            table=table,
            strict_namespace=strict_namespace,
            **options,
        )
    if db_type == "bigquery":
        return _introspect_bigquery(
            database=database,
            schema=schema or "dataflow",
            connection_string=connection_string,
            table=table,
            strict_namespace=strict_namespace,
            host=host,
            port=port,
            service_account=str(options.get("service_account") or ""),
            location=str(options.get("location") or ""),
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
            reader_cfg=reader_cfg,
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
    if db_type in ("stripe", "airtable", "notion", "rest_api"):
        return _introspect_thin_saas(
            db_type,
            host=host,
            database=database,
            table=table,
            connection_string=connection_string,
            api_key=api_key,
            username=username,
            password=password,
        )
    if db_type == "shopify":
        return _introspect_shopify(
            host=host,
            database=database,
            table=table,
            connection_string=connection_string,
            api_key=api_key,
            username=username,
            password=password,
        )
    if db_type == "zendesk":
        return _introspect_zendesk(
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
            schema=schema,
            **options,
        )
    if db_type == "qdrant":
        return _introspect_qdrant(
            host=host,
            port=port,
            database=database,
            table=table,
            connection_string=connection_string,
            username=username,
            password=password,
            ssl=ssl,
        )
    if db_type in ("s3", "amazon_s3"):
        return _introspect_object_store("s3", host=host, database=database, table=table, schema=schema, **{
            "username": username, "password": password, "connection_string": connection_string,
            "port": port, "ssl": ssl, "store_cfg": reader_cfg, "options": options,
        })
    if db_type in ("gcs", "google_cloud_storage"):
        return _introspect_object_store("gcs", host=host, database=database, table=table, schema=schema, **{
            "username": username, "password": password, "connection_string": connection_string,
            "port": port, "ssl": ssl, "store_cfg": reader_cfg, "options": options,
        })
    if db_type in ("adls", "azure_blob", "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage"):
        return _introspect_object_store("adls", host=host, database=database, table=table, schema=schema, **{
            "username": username, "password": password, "connection_string": connection_string,
            "port": port, "ssl": ssl, "store_cfg": reader_cfg, "options": options,
        })
    if db_type == "redis":
        return _introspect_redis(host=host, port=port, password=password, table=table, connection_string=connection_string)
    if db_type == "sftp":
        return _introspect_sftp(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            table=table,
            reader_cfg=reader_cfg,
        )
    if db_type == "sqlite":
        return _introspect_sqlite(database=database, connection_string=connection_string, host=host, table=table)
    if db_type == "neo4j":
        return _introspect_neo4j(
            host=host,
            port=port,
            database=database,
            username=username,
            password=password,
            table=table,
            ssl=ssl,
        )
    if db_type == "weaviate":
        return _introspect_weaviate(
            host=host,
            port=port,
            database=database,
            table=table,
            connection_string=connection_string,
            api_key=api_key,
            ssl=ssl,
        )
    return {"ok": False, "error": f"Schema introspection not implemented for {db_type}", "columns": [], "tables": []}


def _introspect_sftp(
    *,
    host: str,
    port: int,
    database: str,
    username: str,
    password: str,
    table: str | None,
    reader_cfg: dict[str, Any] | None,
) -> dict[str, Any]:
    """Column profile for one remote file. The transfer reader already owns this."""
    from src.transfer.endpoint_intelligence import introspect_endpoint
    from src.transfer.models import EndpointConfig

    cfg = dict(reader_cfg or {})
    nested = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    extra = {}
    for key in ("host_key", "known_hosts", "host_key_policy"):
        value = cfg.get(key)
        if value in (None, "") and key in nested:
            value = nested.get(key)
        if value not in (None, ""):
            extra[key] = value
    endpoint = EndpointConfig(
        kind="database",
        format="sftp",
        host=str(cfg.get("host") or host or ""),
        port=int(cfg.get("port") or port or 0),
        database=str(cfg.get("database") or database or ""),
        username=str(cfg.get("username") or username or ""),
        password=str(cfg.get("password") or password or ""),
        table=str(table or cfg.get("table") or ""),
        extra=extra,
    )
    probed = introspect_endpoint(endpoint)
    schema = probed.get("schema") if isinstance(probed.get("schema"), dict) else {}
    raw_cols = probed.get("columns") or []
    columns: list[dict[str, Any]] = []
    for item in raw_cols:
        if isinstance(item, dict):
            name = str(item.get("name") or "").strip()
            if not name:
                continue
            columns.append(
                {
                    "name": name,
                    "type": str(item.get("type") or schema.get(name) or "TEXT"),
                    "nullable": True,
                }
            )
            continue
        name = str(item or "").strip()
        if not name:
            continue
        columns.append(
            {"name": name, "type": str(schema.get(name) or "TEXT"), "nullable": True}
        )
    if not columns:
        detail = str(
            probed.get("sample_error") or probed.get("message") or probed.get("error") or ""
        ).strip()
        return {
            "ok": False,
            "error": detail or "SFTP file has no readable columns",
            "columns": [],
            "tables": [],
        }
    return {
        "ok": True,
        "columns": columns,
        "tables": [endpoint.table] if endpoint.table else [],
        "schema": schema,
    }


#: Driver types whose introspection is an object read (bucket + key).
OBJECT_STORE_INTROSPECT_TYPES = frozenset({
    "s3", "amazon_s3", "gcs", "google_cloud_storage", "adls", "azure_blob",
    "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage",
})


def is_object_store_type(db_type: str) -> bool:
    return str(db_type or "").strip().lower() in OBJECT_STORE_INTROSPECT_TYPES


#: Connector keys an object-store client is built from (connectors.aws_common).
_OBJECT_STORE_DIAL_KEYS = (
    "endpoint_url",
    "path_style",
    "region",
    "use_default_aws_credentials",
    "workspace_id",
    "account_name",
    "account_key",
    "project_id",
    "credentials_json",
)


def object_store_dial_cfg(
    store_cfg: dict[str, Any] | None,
    options: dict[str, Any] | None,
    *,
    host: str,
    port: Any,
    ssl: bool | None,
    database: str,
    username: str,
    password: str,
    connection_string: str,
) -> dict[str, Any]:
    """The config an object-store read dials, built like the Execute reader's.

    ``store_cfg`` is the resolved connector config (callers pass it as
    ``reader_cfg``); its port, TLS flag and endpoint keys are what Test and the
    Execute reader dial. Endpoint keys passed through ``options`` are kept too.
    """
    full = dict(store_cfg) if isinstance(store_cfg, dict) else {}
    extra = full.get("extra") if isinstance(full.get("extra"), dict) else {}
    cfg: dict[str, Any] = {}
    for source in (options or {}, extra, full):
        for key in _OBJECT_STORE_DIAL_KEYS:
            if source.get(key) not in (None, ""):
                cfg[key] = source[key]
    cfg.update({
        "host": host,
        "database": database,
        "username": username,
        "password": password,
        "connection_string": connection_string,
    })
    # Only the connector's own port. Callers without the config pass a
    # placeholder default (5432) that would invent ``http://us-east-1:5432``.
    if full.get("port") not in (None, "", 0, "0"):
        cfg["port"] = full["port"]
    if full.get("ssl") is not None:
        cfg["ssl"] = bool(full["ssl"])
    return cfg


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
    port: Any = None,
    ssl: bool | None = None,
    store_cfg: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    **_: Any,
) -> dict[str, Any]:
    # D142 — the object read dials the same endpoint Test and the reader do.
    # This cfg used to carry host/credentials only: the port, the Endpoint URL
    # field and path-style addressing were dropped, so ``127.0.0.1`` + a custom
    # port resolved to no endpoint and boto went to AWS.
    cfg = object_store_dial_cfg(
        store_cfg,
        options,
        host=host,
        port=port,
        ssl=ssl,
        database=database,
        username=username,
        password=password,
        connection_string=connection_string,
    )
    bucket = database or ""
    key = table or ""
    prefix = schema or ""
    try:
        from services.object_store_introspect import (
            introspect_adls_object,
            introspect_gcs_object,
            introspect_s3_object,
        )

        if store_type == "gcs":
            result = introspect_gcs_object(cfg, bucket=bucket, key=key or None, prefix=prefix)
        elif store_type == "adls":
            result = introspect_adls_object(cfg, bucket=bucket, key=key or None, prefix=prefix)
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
    from connectors.redis_reader import resolve_key_pattern

    pattern = resolve_key_pattern(table)
    try:
        from services.object_store_introspect import introspect_redis_keys

        result = introspect_redis_keys(cfg, pattern=pattern)
        if not result.get("ok"):
            return {"ok": False, "error": result.get("error", "Redis introspection failed"), "columns": []}
        row_estimate = int(result.get("total_rows") or 0)
        # The reader's ``redis_key``/``redis_value``/``redis_type`` envelope is
        # not a live schema: an empty keyspace has no fields, and reporting the
        # envelope as typed columns made a writer refuse Map stamps for
        # create-new ("live schema is missing mapped field(s)").
        columns = list(result.get("columns") or [])
        schema = dict(result.get("schema") or {})
        if row_estimate <= 0:
            columns, schema = [], {}
        return {
            "ok": True,
            "columns": columns,
            "column_types": schema,
            "tables": [table or pattern],
            "row_estimate": row_estimate,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _mark_unique_keys_advisory(unique_meta: dict[str, Any]) -> dict[str, Any]:
    """Flag catalog PK/UNIQUE as NOT ENFORCED (BigQuery / Redshift class)."""
    keys = []
    for uk in unique_meta.get("unique_keys") or []:
        bucket = dict(uk)
        bucket["enforced"] = False
        keys.append(bucket)
    return {
        "primary_key_columns": list(unique_meta.get("primary_key_columns") or []),
        "unique_keys": keys,
    }


def _introspect_postgresql(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    schema = kwargs.get("schema") or "public"
    advisory_keys = bool(kwargs.get("advisory_keys"))
    try:
        from connectors.postgresql_conn import get_connection

        conn = get_connection(
            host=kwargs.get("host", ""),
            port=kwargs.get("port", 5432),
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
                (schema,),
            )
            tables = [r[0] for r in cur.fetchall()]

            columns: list[dict] = []
            resolved_schema = schema
            target = table or (tables[0] if tables else None)
            unique_meta: dict[str, Any] = {
                "primary_key_columns": [],
                "unique_keys": [],
            }
            foreign_keys: list[dict[str, Any]] = []
            foreign_keys_meta: dict[str, Any] | None = None
            physical_storage: dict[str, Any] | None = None
            check_meta: dict[str, Any] | None = None
            indexes_meta: dict[str, Any] | None = None
            if target:
                columns = _pg_fetch_columns(
                    cur, schema, target, measure_decimals=not _destination_probe(kwargs)
                )
                # Table may live outside the requested schema (common when UI
                # schema is blank / wrong but database+table are correct).
                # Destination probes must NOT do this — wrong-schema columns
                # falsely mark create-new targets as "existing".
                if not columns and not bool(kwargs.get("strict_namespace")):
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
                        columns = _pg_fetch_columns(
                            cur,
                            found_schema,
                            found_table,
                            measure_decimals=not _destination_probe(kwargs),
                        )
                        if columns:
                            resolved_schema = found_schema
                            target = found_table
                            break
                if columns and not _destination_probe(kwargs):
                    columns = _refine_columns_by_samples(
                        conn, columns, target, resolved_schema
                    )
                    unique_meta = _pg_fetch_unique_keys(cur, resolved_schema, target)
                    if advisory_keys:
                        unique_meta = _mark_unique_keys_advisory(unique_meta)
                    foreign_keys, foreign_keys_meta = _fetch_foreign_keys(
                        "postgresql", cur, resolved_schema, target
                    )
                    physical_storage = probe_physical_storage(
                        "postgresql", cur, resolved_schema, target
                    ).to_dict()
                    check_meta = probe_check_constraints(
                        "postgresql", cur, resolved_schema, target
                    ).to_dict()
                    indexes_meta = probe_secondary_indexes(
                        "postgresql", cur, resolved_schema, target
                    ).to_dict()
        conn.close()
        out: dict[str, Any] = {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": resolved_schema if table else schema,
            "primary_key_columns": unique_meta.get("primary_key_columns") or [],
            "unique_keys": unique_meta.get("unique_keys") or [],
            "foreign_keys": foreign_keys,
            "foreign_keys_meta": foreign_keys_meta,
            "physical_storage": physical_storage,
            "check_constraints_meta": check_meta,
            "indexes_meta": indexes_meta,
        }
        if advisory_keys and (out["primary_key_columns"] or out["unique_keys"]):
            out["warnings"] = [
                "Redshift PRIMARY KEY / UNIQUE constraints are informational "
                "(not enforced at write) — Validate will not invent duplicate "
                "blockers; maintain uniqueness in the pipeline (warehouse advisory keys)."
            ]
            out["message"] = out["warnings"][0]
        return out
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
    from connectors.sql_identifiers import (
        quote_sql_identifier,
        snowflake_fold_identifier,
    )

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

    # Try the operator schema first. Listing every schema via
    # information_schema.schemata is a cold-warehouse tax and is why
    # "Analyzing destination schema" sat for minutes on Snowflake dest.
    for cand in candidates:
        try:
            cur.execute(f"USE SCHEMA {quote_sql_identifier(cand)}")
            resolved = snowflake_fold_identifier(cand)
            warning = None
            if snowflake_fold_identifier(requested) != resolved:
                warning = (
                    f"Schema '{requested}' was not usable; using '{resolved}' instead."
                )
            return resolved, [], warning
        except Exception as exc:
            msg = str(exc).lower()
            if "002043" in str(exc) or "002003" in str(exc) or "does not exist" in msg or "not exist" in msg:
                continue
            # Unexpected errors (permissions, etc.) — re-raise for outer handler
            raise

    available = _snowflake_list_schemas(cur)
    available_set = {a.upper() for a in available}

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

        from services.connector_auth import engine_login_role

        conn = get_connection(
            account=normalize_account(kwargs.get("host", "")),
            username=kwargs.get("username", ""),
            password=kwargs.get("password", ""),
            database=kwargs.get("database", ""),
            schema=schema.upper(),
            warehouse=kwargs.get("warehouse", ""),
            connection_string=kwargs.get("connection_string", ""),
            role=engine_login_role(kwargs.get("auth_role"), kwargs.get("role")),
            private_key=str(kwargs.get("private_key") or ""),
            private_key_passphrase=str(kwargs.get("password") or ""),
        )

        warnings: list[str] = []
        with conn.cursor() as cur:
            # Dest Map only needs DESC TABLE. A suspended warehouse plus
            # information_schema on SNOWFLAKE_SAMPLE_DATA is why the UI sat on
            # "Checking destination…" for minutes. Fail closed with an honest
            # timeout instead of an unbounded resume + catalog scan.
            try:
                cur.execute("ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = 45")
            except Exception as exc:
                logger.debug("Snowflake statement timeout skipped: %s", exc)
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

            tables: list[str] = []
            if not table:
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
                from connectors.snowflake_conn import snowflake_physical_column_rows
                from connectors.sql_identifiers import snowflake_fold_identifier

                named = bool(table) or bool(kwargs.get("strict_namespace"))
                if named:
                    # Dest/named table: DESC the folded name. Do not probe
                    # information_schema.tables five times on SNOWFLAKE_SAMPLE_DATA.
                    target_table = snowflake_fold_identifier(str(target_table))
                else:
                    from connectors.snowflake_conn import resolve_or_fold_snowflake_table

                    try:
                        target_table = resolve_or_fold_snowflake_table(
                            cur, schema, str(target_table)
                        )
                    except Exception as exc:
                        logging.getLogger(__name__).warning(
                            "Exception suppressed: %s", exc, exc_info=exc
                        )
                col_rows = snowflake_physical_column_rows(
                    cur, schema, str(target_table)
                )
                # A destination stays in the operator's namespace: a table of
                # the same name in another schema is not where the run writes,
                # and the cross-schema search runs on the warehouse (a 5-10s
                # resume when suspended) for every NEW destination table (D187).
                if (
                    not col_rows
                    and not bool(kwargs.get("strict_namespace"))
                    and str(kwargs.get("introspect_purpose") or "") != "destination"
                ):
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
                        except Exception as exc:
                            logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
                        col_rows = snowflake_physical_column_rows(
                            cur, str(found_schema), str(found_table)
                        )
                        if col_rows:
                            schema = found_schema
                            target_table = found_table
                            break
                for row in col_rows:
                    # Tolerate legacy 3-tuple mocks and full INFORMATION_SCHEMA rows.
                    name = row[0]
                    dtype = row[1]
                    nullable = row[2]
                    char_len = row[3] if len(row) > 3 else None
                    num_prec = row[4] if len(row) > 4 else None
                    num_scale = row[5] if len(row) > 5 else None
                    dt_prec = row[6] if len(row) > 6 else None
                    col_sf: dict[str, Any] = {
                        "name": name,
                        "inferred_type": _sf_to_logical(
                            dtype,
                            character_maximum_length=char_len,
                            numeric_precision=num_prec,
                            numeric_scale=num_scale,
                            datetime_precision=dt_prec,
                        ),
                        "nullable": nullable == "YES",
                        "data_type": str(dtype or ""),
                    }
                    attach_declared_catalog_type(
                        col_sf, str(dtype or ""), dest_db="snowflake"
                    )
                    columns.append(col_sf)
            unique_meta: dict[str, Any]
            if target_table and columns:
                desc_pk = [str(c) for c in (getattr(cur, "_dataflow_desc_pk", None) or []) if c]
                # Named dest/source table: DESC already answered columns + PK.
                # The information_schema.table_constraints join on
                # SNOWFLAKE_SAMPLE_DATA is the multi-minute Destination hang.
                if desc_pk or table or bool(kwargs.get("strict_namespace")):
                    unique_meta = {
                        "primary_key_columns": desc_pk,
                        "unique_keys": [],
                    }
                else:
                    unique_meta = _snowflake_fetch_unique_keys(
                        cur, schema, str(target_table)
                    )
            else:
                unique_meta = {"primary_key_columns": [], "unique_keys": []}
        conn.close()
        out: dict[str, Any] = {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": schema,
            "primary_key_columns": unique_meta.get("primary_key_columns") or [],
            "unique_keys": unique_meta.get("unique_keys") or [],
        }
        if "table_kind" in unique_meta:
            out["table_kind"] = unique_meta.get("table_kind") or ""
        # Advisory (NOT ENFORCED) keys — do not invent write blockers, but tell the operator.
        advisory = [
            u.get("name")
            for u in (unique_meta.get("unique_keys") or [])
            if u.get("enforced") is False
        ]
        if advisory:
            warnings.append(
                "Snowflake UNIQUE/PRIMARY KEY declared but NOT ENFORCED "
                f"({', '.join(str(n) for n in advisory[:5])}) — Validate will not "
                "block duplicates; hybrid tables enforce constraints at write time."
            )
        db_name = str(kwargs.get("database") or "").strip().upper()
        if db_name == "SNOWFLAKE_SAMPLE_DATA":
            warnings.append(
                "SNOWFLAKE_SAMPLE_DATA is a shared sample catalog (usually "
                "read-only). Destination tables normally live in your own "
                "database — looking up a write table here waits on warehouse "
                "resume and a huge information_schema."
            )
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
            purpose="introspect",
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
                    SELECT column_name, column_type, is_nullable, EXTRA,
                           COLLATION_NAME, CHARACTER_SET_NAME, COLUMN_DEFAULT
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
                        SELECT column_name, column_type, is_nullable, EXTRA,
                               COLLATION_NAME, CHARACTER_SET_NAME, COLUMN_DEFAULT
                        FROM information_schema.columns
                        WHERE table_schema = %s AND LOWER(table_name) = LOWER(%s)
                        ORDER BY ordinal_position
                        """,
                        (db_name, requested or target),
                    )
                    rows = cur.fetchall()
                # Wrong database in the form is common for *source* discovery.
                # Destination probes must stay in the operator-chosen database —
                # otherwise a host-wide `users` table makes railway.users look real.
                if not rows and not bool(kwargs.get("strict_namespace")):
                    cur.execute(
                        """
                        SELECT table_schema, table_name
                        FROM information_schema.tables
                        WHERE table_type = 'BASE TABLE'
                          AND LOWER(table_name) = LOWER(%s)
                          AND table_schema NOT IN (
                            'mysql', 'information_schema', 'performance_schema', 'sys'
                          )
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
                            SELECT column_name, column_type, is_nullable, EXTRA,
                                   COLLATION_NAME, CHARACTER_SET_NAME, COLUMN_DEFAULT
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
                for row in rows:
                    name, dtype, nullable = row[0], row[1], row[2]
                    extra = str(row[3] if len(row) > 3 else "").lower()
                    collation = str(row[4] if len(row) > 4 and row[4] else "").strip()
                    charset = str(row[5] if len(row) > 5 and row[5] else "").strip()
                    logical = _mysql_to_logical(dtype)
                    if "auto_increment" in extra:
                        logical = f"{logical} AUTO_INCREMENT"
                    # VIRTUAL/STORED GENERATED — client INSERT must omit (like PG ALWAYS).
                    # DEFAULT_GENERATED is an expression default (CURRENT_TIMESTAMP),
                    # not a computed column.
                    elif _mysql_extra_is_computed_column(extra):
                        logical = f"{logical} GENERATED ALWAYS"
                    if collation:
                        logical = f"{logical} COLLATE {collation}"
                    default = row[6] if len(row) > 6 else None
                    col_my: dict[str, Any] = {
                        "name": name,
                        "inferred_type": logical,
                        "nullable": nullable == "YES",
                        # MySQL stores a literal default as its bare value while
                        # MariaDB stores an expression; normalize to SQL text so
                        # the fidelity whitelist sees one spelling.
                        "default": normalize_catalog_default(
                            "mysql", default, data_type=str(dtype or ""), extra=extra
                        ),
                        "is_identity": "auto_increment" in extra,
                        "generation": (
                            "always"
                            if _mysql_extra_is_computed_column(extra)
                            else ("by_default" if "auto_increment" in extra else "")
                        ),
                        "collation": collation,
                        "charset": charset,
                        "data_type": str(dtype or ""),
                    }
                    attach_declared_catalog_type(
                        col_my,
                        _mysql_catalog_physical(str(dtype or "")),
                        dest_db="mysql",
                    )
                    columns.append(col_my)
                if columns and not _destination_probe(kwargs):
                    columns = _refine_columns_by_samples(
                        conn, columns, target, db_name, quote_char="`"
                    )
            unique_meta: dict[str, Any] = {
                "primary_key_columns": [],
                "unique_keys": [],
            }
            foreign_keys: list[dict[str, Any]] = []
            foreign_keys_meta: dict[str, Any] | None = None
            physical_storage: dict[str, Any] | None = None
            check_meta: dict[str, Any] | None = None
            indexes_meta: dict[str, Any] | None = None
            if columns and target:
                unique_meta = _mysql_fetch_unique_keys(cur, db_name, target)
                foreign_keys, foreign_keys_meta = _fetch_foreign_keys(
                    "mysql", cur, db_name, target
                )
                physical_storage = probe_physical_storage(
                    "mysql", cur, db_name, target
                ).to_dict()
                check_meta = probe_check_constraints(
                    "mysql", cur, db_name, target
                ).to_dict()
                indexes_meta = probe_secondary_indexes(
                    "mysql", cur, db_name, target
                ).to_dict()
                apply_identity_probe("mysql", cur, db_name, target, columns)
        conn.close()
        return {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": db_name,
            "primary_key_columns": unique_meta.get("primary_key_columns") or [],
            "unique_keys": unique_meta.get("unique_keys") or [],
            "foreign_keys": foreign_keys,
            "foreign_keys_meta": foreign_keys_meta,
            "physical_storage": physical_storage,
            "check_constraints_meta": check_meta,
            "indexes_meta": indexes_meta,
        }
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

        client = get_client(
            project_id=project_id,
            credentials_path=kwargs.get("connection_string", ""),
            service_account=str(kwargs.get("service_account") or ""),
            location=str(kwargs.get("location") or ""),
            host=str(kwargs.get("host") or ""),
            port=int(kwargs.get("port") or 0),
            connection_string=str(kwargs.get("connection_string") or ""),
        )
        tables = [t.table_id for t in client.list_tables(f"{project_id}.{dataset_id}", max_results=100)]
        columns: list[dict] = []
        resolved_dataset = dataset_id
        target = table or (tables[0] if tables else None)
        if target:
            try:
                tbl = client.get_table(f"{project_id}.{dataset_id}.{target}")
            except Exception:
                tbl = None
            if tbl is None and table and not bool(kwargs.get("strict_namespace")):
                # Wrong dataset in the form — scan a bounded set of datasets (source only).
                try:
                    datasets = list(client.list_datasets(max_results=25))
                except Exception:
                    datasets = []
                for ds in datasets:
                    ds_id = getattr(ds, "dataset_id", None) or str(ds)
                    try:
                        tbl = client.get_table(f"{project_id}.{ds_id}.{table}")
                    except Exception as exc:
                        logger.debug("BigQuery table lookup failed for %s.%s: %s", project_id, ds_id, exc)
                        continue
                    if tbl is not None:
                        resolved_dataset = ds_id
                        target = table
                        break
            tbl_for_keys = None
            if tbl is not None:
                tbl_for_keys = tbl
                for field in tbl.schema:
                    columns.append({
                        "name": field.name,
                        "inferred_type": _bq_field_to_logical(field),
                        "declared_type": _bq_field_physical(field),
                        "logical_translated": True,
                        "nullable": getattr(field, "mode", "NULLABLE") != "REQUIRED",
                    })
        else:
            tbl_for_keys = None
        unique_meta = (
            _bigquery_fetch_primary_key(tbl_for_keys)
            if tbl_for_keys is not None and columns
            else {"primary_key_columns": [], "unique_keys": []}
        )
        out: dict[str, Any] = {
            "ok": True,
            "tables": tables,
            "columns": columns,
            "schema": resolved_dataset,
            "primary_key_columns": unique_meta.get("primary_key_columns") or [],
            "unique_keys": unique_meta.get("unique_keys") or [],
        }
        if unique_meta.get("primary_key_columns"):
            out["warnings"] = [
                "BigQuery PRIMARY KEY is NOT ENFORCED (optimizer metadata only) — "
                "Validate will not invent write blockers; prove uniqueness with "
                "pipeline unique tests / Gate-9 sample before trusting merges."
            ]
            out["message"] = out["warnings"][0]
        return out
    except ImportError:
        return {"ok": False, "error": "Install google-cloud-bigquery for schema introspection", "columns": [], "tables": []}
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _bigquery_fetch_primary_key(tbl: Any) -> dict[str, Any]:
    """Return declared BigQuery PRIMARY KEY columns — always ``enforced=False``.

    BigQuery has no UNIQUE constraint API; PK/FK are NOT ENFORCED optimizer
    hints (Google Cloud docs). Never invent uniqueness from clustering.
    """
    pk_cols: list[str] = []
    try:
        constraints = getattr(tbl, "table_constraints", None)
        primary = getattr(constraints, "primary_key", None) if constraints else None
        cols = getattr(primary, "columns", None) if primary else None
        if cols:
            pk_cols = [str(c) for c in cols if c]
    except Exception:
        pk_cols = []
    if not pk_cols:
        return {"primary_key_columns": [], "unique_keys": []}
    return {
        "primary_key_columns": pk_cols,
        "unique_keys": [
            {
                "name": "PRIMARY",
                "columns": list(pk_cols),
                "primary": True,
                "expression": "",
                "expression_columns": [],
                "case_insensitive": False,
                "filter_predicate": "",
                "enforced": False,
            }
        ],
    }


def _bq_to_logical(
    dtype: str,
    *,
    precision: int | None = None,
    scale: int | None = None,
    max_length: int | None = None,
) -> str:
    """Map BigQuery dtype strings to logical carriers (never invent TEXT).

    Nested ``ARRAY<T>`` / ``STRUCT<…>`` / ``RANGE<T>`` must be parsed *before*
    scalar token checks — otherwise ``RANGE<TIMESTAMP>`` falsely becomes
    ``TIMESTAMPTZ`` via a substring trap (Airbyte/Fivetran-class fidelity).

    ``BIGNUMERIC`` stays distinct from ``NUMERIC``/``DECIMAL`` so create-new and
    preflight can honor the (76,38) vs (38,9) contract (Google SQL docs).
    """
    raw = (dtype or "").strip()
    if not raw:
        return "TEXT"
    upper = raw.upper()

    # --- Nested / RANGE carriers (before any "TIMESTAMP" substring match) ---
    if (upper.startswith("ARRAY<") or upper.startswith("LIST<")) and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip()
        if not inner:
            return "ARRAY"
        return f"ARRAY<{_bq_to_logical(inner)}>"
    if upper in {"ARRAY", "LIST"}:
        return "ARRAY"

    if (upper.startswith("STRUCT<") or upper.startswith("RECORD<")) and raw.endswith(">"):
        from services.type_system import parse_struct_fields

        fields = parse_struct_fields(raw)
        if not fields:
            return "STRUCT"
        parts = [f"{name}:{_bq_to_logical(typ)}" for name, typ in fields]
        return f"STRUCT<{', '.join(parts)}>"
    if upper in {"RECORD", "STRUCT"}:
        # Fielded nested — distinct from opaque JSON (G3 nested→document collapse).
        return "STRUCT"

    if upper.startswith("RANGE<") and raw.endswith(">"):
        inner = raw[raw.index("<") + 1 : -1].strip().upper()
        # BigQuery RANGE ↔ PostgreSQL range twins (no Snowflake native RANGE).
        if inner == "DATE":
            return "DATERANGE"
        if inner == "DATETIME":
            return "TSRANGE"
        if inner in {"TIMESTAMP", "TIMESTAMPTZ"}:
            return "TSTZRANGE"
        return f"RANGE<{inner}>" if inner else "RANGE"
    if upper == "RANGE":
        return "RANGE"

    # Parametric numeric from dtype string (INFORMATION_SCHEMA / DDL paste).
    m_num = re.match(
        r"^(NUMERIC|DECIMAL|BIGNUMERIC|BIGDECIMAL)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$",
        raw,
        re.I,
    )
    if m_num:
        kind = m_num.group(1).upper()
        p = int(m_num.group(2))
        s = int(m_num.group(3)) if m_num.group(3) is not None else None
        if kind in {"BIGNUMERIC", "BIGDECIMAL"}:
            return f"BIGNUMERIC({p},{s})" if s is not None else f"BIGNUMERIC({p})"
        return f"DECIMAL({p},{s})" if s is not None else f"DECIMAL({p})"

    d = upper
    if d in {"INT64", "INTEGER", "SMALLINT", "BIGINT", "TINYINT", "BYTEINT"}:
        from services.type_system import integer_width_carrier

        # INT64/BIGINT → BIGINT carrier; SMALLINT stays SMALLINT (never INT32 collapse).
        return integer_width_carrier(d) or "BIGINT"
    if d in {"BIGNUMERIC", "BIGDECIMAL"}:
        if precision is not None and scale is not None:
            return f"BIGNUMERIC({int(precision)},{int(scale)})"
        if precision is not None:
            return f"BIGNUMERIC({int(precision)})"
        return "BIGNUMERIC"
    if d in {"NUMERIC", "DECIMAL"}:
        if precision is not None and scale is not None:
            return f"DECIMAL({int(precision)},{int(scale)})"
        if precision is not None:
            return f"DECIMAL({int(precision)})"
        return "DECIMAL"
    if d in {"FLOAT64", "FLOAT", "DOUBLE"}:
        from services.type_system import float_width_carrier

        return float_width_carrier(d) or "DOUBLE"
    if d == "BOOL":
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    if d == "TIME":
        return "TIME"
    # BigQuery DATETIME is wall-clock NTZ; TIMESTAMP is UTC instant (TZ-aware).
    if d == "DATETIME":
        return "TIMESTAMP_NTZ"
    if d == "TIMESTAMP":
        return "TIMESTAMPTZ"
    if d == "INTERVAL":
        return "INTERVAL"
    if d == "BYTES" or d.startswith("BYTES("):
        if d.startswith("BYTES("):
            m_len = re.match(r"^BYTES\((\d+)\)$", d)
            if m_len:
                return f"BINARY({int(m_len.group(1))})"
        if max_length is not None and int(max_length) > 0:
            return f"BINARY({int(max_length)})"
        return "BINARY"
    if d == "JSON":
        return "JSON"
    if d == "GEOGRAPHY":
        return "GEOGRAPHY"
    if d == "STRING" or d.startswith("STRING("):
        if d.startswith("STRING("):
            m_len = re.match(r"^STRING\((\d+)\)$", d)
            if m_len:
                return f"STRING({int(m_len.group(1))})"
        if max_length is not None and int(max_length) > 0:
            return f"STRING({int(max_length)})"
        return "TEXT"
    return "TEXT"


def _bq_field_physical(field: Any) -> str:
    """The BigQuery carrier as the catalog declares it (``DATETIME``, ``INT64``).

    ``_bq_field_to_logical`` translates into DataFlow's neutral vocabulary, which
    is what profiling wants. The destination role asks a different question — what
    will this column accept — and the neutral label answers it wrong on this
    dialect: ``DATETIME`` read back as ``TIMESTAMP_NTZ`` and ``BIGNUMERIC(24,6)``
    read back as bare ``BIGNUMERIC`` are graded against BigQuery's own rules for
    those spellings and invent a narrow/quarantine the column does not have.
    """
    ftype = str(getattr(field, "field_type", "") or "").strip().upper()
    mode = str(getattr(field, "mode", "NULLABLE") or "NULLABLE").upper()
    children = list(getattr(field, "fields", None) or [])
    precision = getattr(field, "precision", None)
    scale = getattr(field, "scale", None)
    max_length = getattr(field, "max_length", None)

    if ftype in {"RECORD", "STRUCT"} and children:
        parts = []
        for child in children:
            child_mode = str(getattr(child, "mode", "NULLABLE") or "NULLABLE").upper()
            child_t = _bq_field_physical(child)
            if child_mode == "REPEATED" and not child_t.startswith("ARRAY<"):
                child_t = f"ARRAY<{child_t}>"
            parts.append(f"{child.name}:{child_t}")
        base = f"STRUCT<{', '.join(parts)}>"
    else:
        base = ftype or "STRING"
        if base in {"NUMERIC", "DECIMAL", "BIGNUMERIC", "BIGDECIMAL"}:
            if isinstance(precision, int) and precision > 0:
                if isinstance(scale, int):
                    base = f"{base}({precision},{scale})"
                else:
                    base = f"{base}({precision})"
        elif base == "STRING" and isinstance(max_length, int) and max_length > 0:
            base = f"STRING({max_length})"
    if mode == "REPEATED" and not base.startswith("ARRAY<"):
        return f"ARRAY<{base}>"
    return base


def _bq_field_to_logical(field: Any) -> str:
    """Preserve BigQuery RECORD/ARRAY nesting — never collapse child types to bare JSON."""
    precision = getattr(field, "precision", None)
    scale = getattr(field, "scale", None)
    max_length = getattr(field, "max_length", None)
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
            max_length=max_length if isinstance(max_length, int) else None,
        )

    if mode == "REPEATED" and not base.upper().startswith("ARRAY<"):
        return f"ARRAY<{base}>"
    return base


_PG_COLUMN_SQL = """
SELECT a.attname AS column_name,
       format_type(a.atttypid, a.atttypmod) AS data_type,
       CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
       COALESCE(a.attidentity, '') AS attidentity,
       pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
       COALESCE(col.collname, '') AS collname,
       COALESCE(col.collisdeterministic, TRUE) AS coll_deterministic,
       COALESCE(a.attgenerated, '') AS attgenerated,
       a.atttypid AS type_oid,
       t.typtype AS typ_type
FROM pg_catalog.pg_attribute a
JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
LEFT JOIN pg_catalog.pg_attrdef ad
  ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
LEFT JOIN pg_catalog.pg_collation col
  ON a.attcollation = col.oid AND a.attcollation <> 0
WHERE n.nspname = %s
  AND c.relname = %s
  AND a.attnum > 0
  AND NOT a.attisdropped
ORDER BY a.attnum
"""


def format_enum_domain_carrier(members: list[str] | tuple[str, ...]) -> str:
    """Build ``ENUM('a','b')`` carrier from ordered labels (MySQL / PG pg_enum)."""
    from services.type_system import format_enum_domain_carrier as _fmt

    return _fmt(members)


def _pg_fetch_enum_labels(cur: Any, type_oids: list[int]) -> dict[int, list[str]]:
    """Load ``pg_enum`` labels ordered by ``enumsortorder`` (Postgres catalog)."""
    if not type_oids:
        return {}
    # Deduplicate while preserving order for stable ANY/IN binds.
    uniq: list[int] = []
    seen: set[int] = set()
    for oid in type_oids:
        if oid in seen:
            continue
        seen.add(oid)
        uniq.append(int(oid))
    placeholders = ",".join(["%s"] * len(uniq))
    cur.execute(
        f"""
        SELECT enumtypid, enumlabel
        FROM pg_catalog.pg_enum
        WHERE enumtypid IN ({placeholders})
        ORDER BY enumtypid, enumsortorder
        """,
        tuple(uniq),
    )
    out: dict[int, list[str]] = {}
    for typoid, label in cur.fetchall():
        out.setdefault(int(typoid), []).append(str(label))
    return out


def _pg_apply_identity_carrier(logical: str, attidentity: str, default_expr: str | None) -> str:
    """Annotate integer carriers with GENERATED / SERIAL when catalog says so."""
    ident = (attidentity or "").strip().lower()
    default = (default_expr or "").lower()
    base = (logical or "INT4").upper()
    if ident == "a":
        # GENERATED ALWAYS AS IDENTITY — client INSERT must omit the column.
        return f"{base} GENERATED ALWAYS"
    if ident == "d":
        return f"{base} GENERATED BY DEFAULT"
    if "nextval(" in default:
        if "bigint" in base or base in {"INT8"}:
            return "BIGSERIAL"
        if "smallint" in base or base in {"INT2"}:
            return "SMALLSERIAL"
        return "SERIAL"
    return logical


def _fetch_foreign_keys(
    dialect: str, cur: Any, schema: str, table: str
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Measured foreign keys for ``schema.table`` plus the full status payload.

    One canonical probe (``services.foreign_key_metadata``) for every dialect:
    the carry planner and the post-load destination re-read must compare the
    same evidence shape the source was measured with.
    """
    measured = probe_foreign_keys(dialect, cur, schema, table)
    return [fk.to_dict() for fk in measured.items], measured.to_dict()


def _pg_fetch_columns(
    cur: Any, schema: str, table: str, *, measure_decimals: bool = True
) -> list[dict]:
    """Read column metadata via ``format_type`` so typmod (vector dim, decimal p,s) survives."""
    try:
        cur.execute(_PG_COLUMN_SQL, (schema, table))
        rows = list(cur.fetchall())
        has_typtype = True
    except Exception:
        # PG < 12 lacks collisdeterministic / attgenerated — fall back.
        cur.execute(
            """
            SELECT a.attname AS column_name,
                   format_type(a.atttypid, a.atttypmod) AS data_type,
                   CASE WHEN a.attnotnull THEN 'NO' ELSE 'YES' END AS is_nullable,
                   COALESCE(a.attidentity, '') AS attidentity,
                   pg_get_expr(ad.adbin, ad.adrelid) AS default_expr,
                   COALESCE(col.collname, '') AS collname,
                   TRUE AS coll_deterministic,
                   '' AS attgenerated,
                   a.atttypid AS type_oid,
                   t.typtype AS typ_type
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
            JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
            JOIN pg_catalog.pg_type t ON a.atttypid = t.oid
            LEFT JOIN pg_catalog.pg_attrdef ad
              ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum
            LEFT JOIN pg_catalog.pg_collation col
              ON a.attcollation = col.oid AND a.attcollation <> 0
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            (schema, table),
        )
        rows = list(cur.fetchall())
        has_typtype = True
        # If join to pg_type fails on ancient servers, last-resort without enum.
        if not rows:
            has_typtype = False

    enum_oids = [
        int(row[8])
        for row in rows
        if has_typtype and len(row) > 9 and str(row[9] or "").strip().lower() == "e"
    ]
    enum_labels = _pg_fetch_enum_labels(cur, enum_oids) if enum_oids else {}

    columns: list[dict] = []
    for row in rows:
        name, dtype, nullable, attidentity, default_expr = row[:5]
        collname = str(row[5] if len(row) > 5 else "").strip()
        coll_det = bool(row[6]) if len(row) > 6 else True
        attgenerated = str(row[7] if len(row) > 7 else "").strip().lower()
        type_oid = int(row[8]) if len(row) > 8 and row[8] is not None else 0
        typ_type = str(row[9] if len(row) > 9 else "").strip().lower()
        if typ_type == "e" and type_oid in enum_labels and enum_labels[type_oid]:
            # Preserve pg_enum domain — bare string invent loses labels.
            logical = format_enum_domain_carrier(enum_labels[type_oid])
        else:
            logical = _pg_to_logical(str(dtype or ""))
        logical = _pg_apply_identity_carrier(
            logical, str(attidentity or ""), default_expr
        )
        # STORED/VIRTUAL generated columns — client INSERT must omit (like ALWAYS).
        if attgenerated in {"s", "v"} and "GENERATED ALWAYS" not in logical.upper():
            logical = f"{logical} GENERATED ALWAYS"
        # Skip libc default/C/POSIX noise; keep ICU / named / nondeterministic.
        if collname and collname.lower() not in {"default", "c", "posix"}:
            logical = f"{logical} COLLATE {collname}"
            if not coll_det:
                logical = f"{logical} NONDETERMINISTIC"
        generation = ""
        if (attidentity or "").strip().lower() == "a" or attgenerated in {"s", "v"}:
            generation = "always"
        elif (attidentity or "").strip().lower() == "d" or (
            default_expr and "nextval(" in str(default_expr).lower()
        ):
            generation = "by_default"
        col_pg: dict[str, Any] = {
            "name": name,
            "inferred_type": logical,
            "nullable": str(nullable).upper() == "YES",
            "data_type": dtype,
            "is_identity": bool(
                (attidentity or "").strip()
                or (default_expr and "nextval(" in str(default_expr).lower())
            ),
            "generation": generation,
            "collation": collname,
            "collation_deterministic": coll_det,
        }
        # Property 6 — surface defaults for create-new carry (exclude sequence nextval).
        if default_expr and "nextval(" not in str(default_expr).lower():
            col_pg["default"] = str(default_expr)
        attach_declared_catalog_type(
            col_pg, str(dtype or ""), dest_db="postgresql"
        )
        columns.append(col_pg)
    apply_identity_probe("postgresql", cur, schema, table, columns)
    if measure_decimals:
        # A destination's bare ``numeric`` is the carrier the writer binds
        # into, and it is unbounded. Measuring its rows reported run 1's data
        # (DECIMAL(9,6)) as the carrier and refused run 2's larger values
        # as a collapse into a column that holds them (D68).
        _measure_unconstrained_decimals(cur, schema, table, columns)
    return columns


def _measure_unconstrained_decimals(
    cur: Any, schema: str, table: str, columns: list[dict]
) -> None:
    """Replace bare ``numeric`` with the capacity its rows actually use.

    An unconstrained ``numeric`` declares no bound, so every comparison against
    a destination carrier concludes the destination is narrower and refuses the
    route — true of the type, rarely true of the data. The aggregate covers the
    whole column, so the substituted ``DECIMAL(p,s)`` is measured rather than
    invented, and ``decimal_capacity_measured`` marks it as such.

    Anything that stops the probe leaves the bare type in place, which keeps the
    fail-closed verdict the operator would otherwise have received.
    """
    from services.decimal_capacity_probe import (
        probe_postgresql_decimal_capacity,
        unconstrained_decimal_columns,
    )

    targets = unconstrained_decimal_columns(columns)
    if not targets:
        return
    try:
        measured = probe_postgresql_decimal_capacity(cur, schema, table, targets)
    except Exception as exc:
        logging.getLogger(__name__).info(
            "decimal capacity probe failed for %s.%s: %s", schema, table, exc
        )
        return
    by_name = {str(c.get("name") or ""): c for c in columns}
    for name, capacity in measured.items():
        col = by_name.get(name)
        if col is None:
            continue
        col["inferred_type"] = capacity.as_type()
        col["declared_type"] = "DECIMAL"
        col["decimal_capacity_measured"] = True


def _pg_elem_to_logical(elem: str) -> str:
    """Map a PG array element type to a logical carrier (no array recursion)."""
    e = (elem or "").strip().lower()
    if not e:
        return "VARCHAR"
    # Avoid re-entering array branch — strip one level only.
    if e.endswith("[]"):
        e = e[:-2].strip()
    m = re.match(r"^(numeric|decimal)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", e)
    if m:
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    # Uppercase width carriers — never leave lowercase ``integer`` (ambiguous
    # with LOGICAL_INTEGER) in inferred_type.
    _elem_int = {
        "bigint": "BIGINT",
        "int8": "BIGINT",
        "bigserial": "BIGSERIAL",
        "integer": "INTEGER",
        "int": "INTEGER",
        "int4": "INTEGER",
        "serial": "SERIAL",
        "smallint": "SMALLINT",
        "int2": "SMALLINT",
        "smallserial": "SMALLSERIAL",
    }
    if e in _elem_int:
        return _elem_int[e]
    _elem_float = {
        "real": "REAL",
        "float4": "REAL",
        "double precision": "DOUBLE PRECISION",
        "float8": "DOUBLE PRECISION",
        "double": "DOUBLE",
        "float": "DOUBLE PRECISION",
    }
    if e in _elem_float:
        return _elem_float[e]
    if e in {"boolean", "bool"}:
        return "BOOLEAN"
    if e == "date":
        return "DATE"
    if e.startswith("timestamp"):
        return "TIMESTAMPTZ" if "with time zone" in e or e.endswith("tz") else "TIMESTAMP_NTZ"
    if e.startswith("time"):
        if "with time zone" in e or e in {"timetz", "time tz"}:
            return "TIMETZ"
        return "TIME"
    if e in {"uuid"}:
        return "UUID"
    if e in {"json", "jsonb"}:
        return "JSONB" if e == "jsonb" else "JSON"
    if e in {"bytea", "varbyte"}:
        return "BINARY"
    if e in {"text", "varchar", "character varying", "character", "char", "citext", "name"}:
        return "VARCHAR" if e != "citext" else "CITEXT"
    if e.startswith("character varying") or e.startswith("varchar") or e.startswith("character("):
        return "VARCHAR"
    # Specialty scalars (inet, point, pg_lsn, …) — reuse scalar mapper so
    # ARRAY<INET> survives introspect (Airbyte historically emitted untyped arrays).
    scalar = _pg_to_logical(e)
    if scalar.startswith("ARRAY<") or scalar.endswith("[]"):
        return "VARCHAR"
    return scalar


def _pg_to_logical(dtype: str) -> str:
    """Map PostgreSQL ``format_type`` / data_type strings to Datawrap logical carriers.

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
        # Preserve YM vs DS polarity — bare INTERVAL stays unqualified.
        if "year" in d and "month" in d:
            return "INTERVAL YEAR TO MONTH"
        if any(tok in d for tok in ("day", "second", "hour", "minute")):
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    if d.startswith("geography("):
        # Keep typmod/SRID (geography(Point,4326)) for contract checks.
        return f"GEOGRAPHY{raw[raw.lower().index('('):]}" if "(" in raw else "GEOGRAPHY"
    if d.startswith("geometry("):
        return f"GEOMETRY{raw[raw.lower().index('('):]}" if "(" in raw else "GEOMETRY"
    if d in {"geometry", "geography"}:
        return "GEOGRAPHY" if d == "geography" else "GEOMETRY"

    if d in ("serial", "bigserial", "smallserial"):
        return d.upper()
    # System / WAL identifiers — preserve native carriers (never invent INTEGER/TEXT).
    if d == "oid":
        return "OID"
    if d == "xid8":
        return "XID8"
    if d == "xid":
        return "XID"
    if d == "cid":
        return "CID"
    if d == "tid":
        return "TID"
    if d == "pg_lsn":
        return "PG_LSN"
    # Width-preserving unambiguous carriers (Property 1: never emit ambiguous
    # INTEGER/INT — those invent 64-bit. PG int4 → INT4 so width is explicit).
    # Must not emit lowercase ``integer`` — that token is LOGICAL_INTEGER.
    if d in ("bigint", "int8"):
        return "BIGINT"
    if d in ("smallint", "int2"):
        return "SMALLINT"
    if d in ("integer", "int", "int4"):
        return "INT4"
    # IEEE floats keep REAL vs DOUBLE polarity — never silently rewrite to DECIMAL.
    if d in ("real", "float4"):
        return "REAL"
    if d in ("double precision", "float8", "double"):
        return "DOUBLE PRECISION"
    if d == "float":
        # PostgreSQL FLOAT without precision ≡ DOUBLE PRECISION.
        return "DOUBLE PRECISION"
    if d == "money":
        # PostgreSQL money ≈ fixed-scale currency — mirror SQL Server MONEY fidelity.
        return "DECIMAL(19,4)"
    if d in ("numeric", "decimal"):
        return "DECIMAL"
    if d == "boolean":
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    if d == "citext":
        # citext is case-insensitive by type — preserve for uniqueness / Gate-8.
        return "CITEXT"
    # Preserve TZ polarity + fractional-second precision (timestamp(6), time(3)).
    # format_type may emit timestamptz(3) or "timestamp(3) with time zone".
    if "timestamp" in d:
        m = re.search(r"timestamptz\s*\(\s*(\d+)\s*\)", d)
        if m:
            return f"TIMESTAMPTZ({m.group(1)})"
        m = re.search(r"timestamp\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        if (
            "with time zone" in d
            or d == "timestamptz"
            or d.startswith("timestamptz")
        ):
            return f"TIMESTAMPTZ({fsp})" if fsp else "TIMESTAMPTZ"
        return f"TIMESTAMP_NTZ({fsp})" if fsp else "TIMESTAMP_NTZ"
    if (
        d in {"timetz", "time tz"}
        or d.startswith("timetz")
        or (d.startswith("time") and "with time zone" in d and "without" not in d)
    ):
        m = re.search(r"time(?:tz)?\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        return f"TIMETZ({fsp})" if fsp else "TIMETZ"
    if d == "time" or d.startswith("time without") or d.startswith("time("):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        fsp = m.group(1) if m else None
        return f"TIME({fsp})" if fsp else "TIME"
    if d == "uuid":
        return "UUID"
    if d == "bytea":
        return "BINARY"
    # Redshift SUPER / VARBYTE (exposed via PG wire format_type).
    if d == "super":
        return "JSON"
    if d == "varbyte" or d.startswith("varbyte("):
        return "BINARY"
    # Typed arrays — preserve element carrier (never invent bare JSON).
    if d.endswith("[]"):
        elem = d[:-2].strip()
        elem_logical = _pg_elem_to_logical(elem)
        return f"ARRAY<{elem_logical}>"
    if " array" in d:
        elem = d.replace(" array", "").strip()
        elem_logical = _pg_elem_to_logical(elem)
        return f"ARRAY<{elem_logical}>"
    if d == "jsonpath":
        # PostgreSQL jsonpath — path expression type (never invent TEXT).
        return "JSONPATH"
    if d in {"json", "jsonb"}:
        # Preserve JSONB polarity — bind SSOT uses native structures on PG.
        return "JSONB" if d == "jsonb" else "JSON"
    if d == "hstore":
        return "HSTORE"
    if d == "ltree":
        return "LTREE"
    if d == "xml":
        return "XML"
    if d == "tsvector":
        return "TSVECTOR"
    if d == "tsquery":
        return "TSQUERY"
    if d == "point":
        return "POINT"
    if d == "line":
        return "LINE"
    if d == "lseg":
        return "LSEG"
    if d == "box":
        return "BOX"
    if d == "path":
        return "PATH"
    if d == "polygon":
        return "POLYGON"
    if d == "circle":
        return "CIRCLE"
    if d == "inet":
        return "INET"
    if d == "cidr":
        return "CIDR"
    if d == "macaddr":
        return "MACADDR"
    if d == "macaddr8":
        return "MACADDR8"
    if d.endswith("multirange"):
        return d.upper()
    if d.endswith("range") and d not in {"range"}:
        return d.upper()  # int4range, tstzrange, …
    if (
        d.startswith("bit(")
        or d == "bit"
        or d.startswith("bit varying")
        or d.startswith("varbit")
    ):
        # BIT(1) → boolean via type_system; BIT(n>1)/VARBIT → bitstring binary.
        m = re.match(r"^bit\s+varying\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"BIT VARYING({m.group(1)})"
        m = re.match(r"^varbit\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"VARBIT({m.group(1)})"
        m = re.match(r"^bit\s*\(\s*(\d+)\s*\)$", d)
        if m:
            return f"BIT({m.group(1)})"
        if d in {"bit varying", "varbit"}:
            return "BIT VARYING"
        return "BIT"
    if d in {"text", "name", "user-defined"}:
        return "TEXT"
    if d == "txid_snapshot":
        return "TXID_SNAPSHOT"
    if d == "pg_snapshot":
        return "PG_SNAPSHOT"
    # Preserve declared width — VARCHAR(n)/CHAR(n) must not collapse to TEXT
    # (G3 width-narrow + write quarantine depend on parametric carriers).
    m = re.match(r"^(?:character\s+varying|varchar)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"VARCHAR({m.group(1)})"
    m = re.match(r"^(?:character|char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"CHAR({m.group(1)})"
    if d in {"character varying", "varchar"}:
        return "VARCHAR"
    if d in {"character", "char"}:
        return "CHAR"
    if d.startswith("character varying") or d.startswith("varchar") or d.startswith("character("):
        return "VARCHAR"
    return "TEXT"

def _mysql_extra_is_computed_column(extra: str) -> bool:
    """True only for a VIRTUAL or STORED generated column.

    MySQL 8 writes ``DEFAULT_GENERATED`` into EXTRA for
    ``DEFAULT CURRENT_TIMESTAMP(6)``. That substring contains ``generated``
    and is still a normal writable column. Marking it GENERATED ALWAYS made
    the writer omit ``updated_at``.
    """
    text = (extra or "").lower()
    return "virtual generated" in text or "stored generated" in text


def _mysql_to_logical(dtype: str) -> str:
    """Map MySQL ``column_type`` to logical carriers, preserving DECIMAL(p,s)."""
    raw = (dtype or "").strip()
    d_early = raw.lower().strip()
    # BIT(n) before text fallback — BIT(1)→boolean via type_system; BIT(n>1)
    # is a bitstring (never opaque TEXT — wave 71).
    if (
        d_early.startswith("bit(")
        or d_early == "bit"
        or d_early.startswith("bit varying")
    ):
        m = re.match(r"^bit\s*\(\s*(\d+)\s*\)$", d_early)
        if m:
            return f"BIT({m.group(1)})"
        return "BIT"
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
    # Preserve ALL unsigned widths so Map/preflight can auto-widen / range-check.
    if "unsigned" in d:
        if "bigint" in d:
            return "BIGINT UNSIGNED"
        if "mediumint" in d:
            return "MEDIUMINT UNSIGNED"
        if "smallint" in d:
            return "SMALLINT UNSIGNED"
        if "tinyint" in d and "tinyint(1)" not in d:
            return "TINYINT UNSIGNED"
        if "int" in d:
            return "INT4 UNSIGNED"
    if d == "year" or d.startswith("year("):
        # MySQL YEAR — keep carrier so write quarantine enforces 1901–2155 / 0000
        # (non-strict MySQL silently stores invalid years as 0000).
        return "YEAR"
    # Preserve MEDIUMINT range (−8388608..8388607) for bind quarantine.
    # Order matters: ``"int" in "bigint"`` is true — check bigint/smallint/tinyint first.
    if "bigint" in d:
        return "BIGINT"
    if "mediumint" in d:
        return "MEDIUMINT"
    if "smallint" in d:
        return "SMALLINT"
    if "tinyint" in d:
        # tinyint(1) boolean handled earlier in this mapper when present.
        return "TINYINT"
    if re.search(r"\bint\b", d) or d == "int":
        return "INT4"
    # IEEE float/double/real — preserve DOUBLE vs FLOAT polarity.
    # MySQL FLOAT is IEEE-32 — emit FLOAT32 (bare FLOAT invents IEEE-64).
    if "double" in d:
        return "DOUBLE"
    if "real" in d:
        return "REAL"
    if "float" in d:
        return "FLOAT32"
    if "bool" in d:
        return "BOOLEAN"
    if d == "date":
        return "DATE"
    # MySQL TIMESTAMP is session-TZ aware; DATETIME is wall-clock NTZ.
    # Preserve fractional-second precision (datetime(6), time(3), timestamp(2)).
    if "timestamp" in d and "datetime" not in d:
        m = re.search(r"timestamp\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMPTZ({m.group(1)})" if m else "TIMESTAMPTZ"
    if "datetime" in d:
        m = re.search(r"datetime\s*\(\s*(\d+)\s*\)", d)
        return f"TIMESTAMP_NTZ({m.group(1)})" if m else "TIMESTAMP_NTZ"
    if d.startswith("time"):
        m = re.search(r"time\s*\(\s*(\d+)\s*\)", d)
        return f"TIME({m.group(1)})" if m else "TIME"
    if "json" in d:
        return "JSON"
    # Preserve BINARY(n)/VARBINARY(n) — G3 width-narrow + write quarantine.
    m = re.match(r"^(varbinary|binary)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    if "binary" in d or "blob" in d or "varbinary" in d:
        return "BINARY"
    if "uuid" in d:
        return "UUID"
    # MySQL ENUM/SET domains — keep members so G3/write can fail-closed.
    m = re.match(r"^(enum|set)\s*\((.*)\)$", d, re.I | re.DOTALL)
    if m:
        return f"{m.group(1).upper()}({m.group(2).strip()})"
    # Preserve MySQL column_type widths (varchar(255), char(10), …).
    m = re.match(r"^(varchar|char)\s*\(\s*(\d+)\s*\)$", d)
    if m:
        return f"{m.group(1).upper()}({m.group(2)})"
    if d in {"varchar", "char"}:
        return d.upper()
    # Order matters: "text" is a substring of longtext, mediumtext, and tinytext.
    # Reading LONGTEXT back as TEXT made the next overwrite CREATE a 64 KB column.
    if d == "longtext" or d.startswith("longtext"):
        return "LONGTEXT"
    if d == "mediumtext" or d.startswith("mediumtext"):
        return "MEDIUMTEXT"
    if d == "tinytext" or d.startswith("tinytext"):
        return "TINYTEXT"
    if d == "text" or d.startswith("text"):
        return "TEXT"
    return "TEXT"












def _arrow_to_logical(dtype: str) -> str:
    """Map Apache Arrow / PyArrow dtype strings to Datawrap carriers."""
    from services.type_system import arrow_dtype_to_carrier

    carrier = arrow_dtype_to_carrier(dtype)
    return carrier if carrier else "TEXT"


def _ch_to_logical(dtype: str) -> str:
    """Map ClickHouse dtype strings to logical carriers (never invent String).

    Research: ClickHouse DateTime64 stores UTC ticks with optional column TZ
    metadata; Tuple/Map/Array are native nested; IPv4/IPv6 are specialty (PG INET).
    """
    raw = (dtype or "").strip()
    if not raw:
        return "TEXT"
    upper = raw.upper()

    from services.type_system import (
        parse_array_element,
        parse_map_key_value,
        parse_struct_fields,
    )

    arr_el = parse_array_element(raw)
    if arr_el is not None:
        return f"ARRAY<{_ch_to_logical(arr_el)}>"
    if upper == "ARRAY":
        return "ARRAY"

    map_kv = parse_map_key_value(raw)
    if map_kv is not None:
        key_t, val_t = map_kv
        return f"MAP<{_ch_to_logical(key_t)},{_ch_to_logical(val_t)}>"
    if upper == "MAP":
        return "MAP"

    if upper.startswith("TUPLE(") and raw.endswith(")"):
        fields = parse_struct_fields(raw)
        if not fields:
            return "STRUCT"
        parts = [f"{name}:{_ch_to_logical(typ)}" for name, typ in fields]
        return f"STRUCT<{', '.join(parts)}>"

    # Nested(name Type, ...) — parallel arrays; logical shape is ARRAY<STRUCT>.
    if upper.startswith("NESTED(") and raw.endswith(")"):
        fields = parse_struct_fields(raw)
        if not fields:
            return "ARRAY"
        parts = [f"{name}:{_ch_to_logical(typ)}" for name, typ in fields]
        return f"ARRAY<STRUCT<{', '.join(parts)}>>"

    # Enum8/Enum16('label' = n, ...) → closed ENUM domain (MySQL/PG class).
    if upper.startswith("ENUM8(") or upper.startswith("ENUM16("):
        labels = re.findall(r"'((?:\\'|[^'])*)'", raw)
        if labels:
            from services.type_system import format_enum_domain_carrier

            return format_enum_domain_carrier(
                [lab.replace("\\'", "'") for lab in labels]
            )

    if upper.startswith("DATETIME64"):
        return raw  # keep DateTime64(p[, tz]) carrier
    if upper == "DATETIME" or (
        upper.startswith("DATETIME(") and not upper.startswith("DATETIME2")
    ):
        return raw

    if upper == "IPV4":
        return "IPv4"
    if upper == "IPV6":
        return "IPv6"
    if upper == "UUID":
        return "UUID"
    if upper == "BOOL" or upper == "BOOLEAN":
        return "BOOLEAN"
    if upper == "DATE":
        return "DATE"
    if upper.startswith("DECIMAL(") or upper.startswith("DECIMAL32") or upper.startswith(
        "DECIMAL64"
    ) or upper.startswith("DECIMAL128") or upper.startswith("DECIMAL256"):
        # Decimal(p,s) or Decimal32/64/128/256(S) where S is scale only (CH docs).
        m_ps = re.match(
            r"^DECIMAL(?:32|64|128|256)?\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)$",
            upper,
        )
        if m_ps:
            return f"DECIMAL({m_ps.group(1)},{m_ps.group(2)})"
        m_scale = re.match(r"^DECIMAL(32|64|128|256)\s*\(\s*(\d+)\s*\)$", upper)
        if m_scale:
            prec = {"32": 9, "64": 18, "128": 38, "256": 76}[m_scale.group(1)]
            return f"DECIMAL({prec},{int(m_scale.group(2))})"
        return "DECIMAL"
    if upper in {"FLOAT32", "FLOAT64", "FLOAT", "DOUBLE"}:
        from services.type_system import float_width_carrier

        return float_width_carrier(upper) or "DOUBLE"
    # ClickHouse Int*/UInt* — preserve wire width (Int64 ≠ Int32).
    m_ch = re.match(r"^(U?Int)(8|16|32|64|128|256)$", raw.strip())
    if m_ch:
        bits = int(m_ch.group(2))
        if bits > 64:
            return f"DECIMAL({76 if bits >= 256 else 38},0)"
        from services.type_system import integer_width_carrier

        return integer_width_carrier(m_ch.group(0)) or "BIGINT"
    if re.match(
        r"^(U?INT\d*|INT8|INT16|INT32|INT64|INT128|INT256|UINT8|UINT16|UINT32|UINT64)$",
        upper,
    ):
        from services.type_system import integer_width_carrier

        return integer_width_carrier(upper) or "BIGINT"
    # FixedString(n) is a fixed-length byte string (CH) — BINARY(n), not TEXT.
    if upper.startswith("FIXEDSTRING("):
        m_fs = re.match(r"^FIXEDSTRING\((\d+)\)$", upper)
        if m_fs:
            return f"BINARY({int(m_fs.group(1))})"
        return "BINARY"
    if upper == "FIXEDSTRING":
        return "BINARY"
    if upper == "STRING":
        return "TEXT"
    if upper.startswith("NULLABLE(") and raw.endswith(")"):
        return _ch_to_logical(raw[raw.index("(") + 1 : -1].strip())
    if upper.startswith("LOWCARDINALITY(") and raw.endswith(")"):
        return _ch_to_logical(raw[raw.index("(") + 1 : -1].strip())
    return "TEXT"


def _sf_to_logical(
    dtype: str,
    *,
    character_maximum_length: Any = None,
    numeric_precision: Any = None,
    numeric_scale: Any = None,
    datetime_precision: Any = None,
) -> str:
    """Map Snowflake data types, preserving VECTOR / NUMBER(p,s) / VARCHAR(n).

    INFORMATION_SCHEMA often returns bare ``TEXT`` / ``NUMBER`` / ``BINARY``;
    fold CHARACTER_MAXIMUM_LENGTH / NUMERIC_* / DATETIME_PRECISION so Map/G3
    and write quarantine see real physical capacity (Airbyte catalog-width class).
    """
    raw = (dtype or "").strip()
    d = raw.upper()

    def _as_int(value: Any) -> int | None:
        try:
            if value is None:
                return None
            n = int(value)
            return n if n > 0 else None
        except (TypeError, ValueError):
            return None

    char_len = _as_int(character_maximum_length)
    num_prec = _as_int(numeric_precision)
    num_scale = (
        int(numeric_scale)
        if numeric_scale is not None
        and str(numeric_scale).strip() != ""
        and str(numeric_scale).lstrip("-").isdigit()
        else None
    )
    dt_prec = _as_int(datetime_precision)

    # Structured nested FIRST — ``OBJECT(... NUMBER ...)`` / ``MAP(..., INT)``
    # must never hit the NUMBER/INT substring traps below (Snowflake structured
    # types docs; Iceberg list/struct/map ↔ SF ARRAY/OBJECT/MAP).
    from services.type_system import (
        parse_array_element,
        parse_map_key_value,
        parse_struct_fields,
    )

    arr_el = parse_array_element(raw)
    if arr_el is not None:
        return f"ARRAY<{_sf_to_logical(arr_el)}>"
    if d == "ARRAY":
        return "ARRAY"

    if d.startswith("OBJECT(") and raw.endswith(")"):
        fields = parse_struct_fields(raw)
        if not fields:
            return "STRUCT"
        parts = [f"{name}:{_sf_to_logical(typ)}" for name, typ in fields]
        return f"STRUCT<{', '.join(parts)}>"
    # Bare OBJECT is semi-structured (VARIANT-shaped); structured OBJECT(...) above.
    if d == "OBJECT":
        return "JSON"

    map_kv = parse_map_key_value(raw)
    if map_kv is not None:
        key_t, val_t = map_kv
        return f"MAP<{_sf_to_logical(key_t)},{_sf_to_logical(val_t)}>"
    if d == "MAP":
        return "MAP"

    if d == "VARIANT":
        return "JSON"

    if "VECTOR" in d:
        return raw  # keep VECTOR(FLOAT, n) carrier
    if "GEOGRAPHY" in d:
        return "GEOGRAPHY"
    if "GEOMETRY" in d:
        return "GEOMETRY"
    if "INTERVAL" in d:
        if "YEAR" in d and "MONTH" in d:
            return "INTERVAL YEAR TO MONTH"
        if "DAY" in d or "SECOND" in d:
            return "INTERVAL DAY TO SECOND"
        return "INTERVAL"
    if d == "BINARY" or d.startswith("BINARY("):
        m_bin = re.match(r"^BINARY\s*\(\s*(\d+)\s*\)$", d)
        width = int(m_bin.group(1)) if m_bin else char_len
        return f"BINARY({width})" if width else "BINARY"
    if d == "TIME" or d.startswith("TIME("):
        m_time = re.match(r"^TIME\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_time.group(1)) if m_time else dt_prec
        return f"TIME({prec})" if prec is not None else "TIME"
    # NUMBER(p,0) → INTEGER when p ≤ 18; else DECIMAL(p,0) (never silent BIGINT overflow).
    m = re.match(r"^(NUMBER|DECIMAL|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+))?\s*\)$", d)
    if m:
        from services.type_system import zero_scale_numeric_carrier

        if m.group(3) is not None and int(m.group(3)) == 0:
            return zero_scale_numeric_carrier(int(m.group(2)))
        if m.group(3) is not None:
            return f"DECIMAL({m.group(2)},{m.group(3)})"
        return f"DECIMAL({m.group(2)})"
    if d in {"NUMBER", "DECIMAL", "NUMERIC"}:
        if num_prec is not None:
            from services.type_system import zero_scale_numeric_carrier

            scale = 0 if num_scale is None else int(num_scale)
            if scale == 0:
                return zero_scale_numeric_carrier(num_prec)
            return f"DECIMAL({num_prec},{scale})"
        return "DECIMAL"
    # Token-anchored integers only — never ``"INT" in d`` (breaks MAP/OBJECT).
    if re.match(
        r"^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)(\s*\(|$)",
        d,
    ):
        from services.type_system import integer_width_carrier

        tok = re.match(
            r"^(INT|INTEGER|BIGINT|SMALLINT|TINYINT|BYTEINT)",
            d,
        )
        # Every one of these spellings is an alias of NUMBER(38,0) in Snowflake
        # — the narrow ones included: SMALLINT holds 38 digits there, not 5. A
        # width carrier read off the spelling would hand the destination a
        # BIGINT and overflow on the 19th digit, which is exactly what the
        # NUMBER(p,0) branch above refuses to do. Carry the declared precision.
        from services.type_system import zero_scale_numeric_carrier

        return zero_scale_numeric_carrier(num_prec or 38) or (
            integer_width_carrier(tok.group(1) if tok else d) or "BIGINT"
        )
    # Snowflake FLOAT / DOUBLE / REAL — preserve IEEE width polarity.
    if d in {"FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION", "REAL"} or d.startswith("FLOAT"):
        from services.type_system import float_width_carrier

        return float_width_carrier(d) or "DOUBLE"
    if "BOOLEAN" in d:
        return "BOOLEAN"
    if d == "DATE":
        return "DATE"
    # Preserve LTZ vs TZ polarity (Snowflake Openflow / Airbyte #80914 class).
    # TIMESTAMP_LTZ ≈ session-relative instant; TIMESTAMP_TZ pins per-row offset.
    if "TIMESTAMP_LTZ" in d:
        m_ltz = re.match(r"^TIMESTAMP_LTZ\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_ltz.group(1)) if m_ltz else dt_prec
        return f"TIMESTAMP_LTZ({prec})" if prec is not None else "TIMESTAMP_LTZ"
    if "TIMESTAMP_TZ" in d:
        m_tz = re.match(r"^TIMESTAMP_TZ\s*\(\s*(\d+)\s*\)$", d)
        prec = int(m_tz.group(1)) if m_tz else dt_prec
        return f"TIMESTAMP_TZ({prec})" if prec is not None else "TIMESTAMP_TZ"
    if "TIMESTAMP_NTZ" in d:
        if dt_prec is not None and "(" not in d:
            return f"TIMESTAMP_NTZ({dt_prec})"
        return "TIMESTAMP_NTZ"
    if "TIMESTAMP" in d:
        if dt_prec is not None and "(" not in d:
            return f"TIMESTAMP_NTZ({dt_prec})"
        return "TIMESTAMP_NTZ"
    # TEXT / VARCHAR / CHAR — fold CHARACTER_MAXIMUM_LENGTH (SF catalog often
    # returns bare TEXT even when the column was created as VARCHAR(n)).
    if d in {"TEXT", "VARCHAR", "CHAR", "CHARACTER", "STRING"} or d.startswith(
        ("VARCHAR(", "CHAR(", "CHARACTER(", "TEXT(")
    ):
        m_str = re.match(
            r"^(?:VARCHAR|CHAR|CHARACTER(?:\s+VARYING)?|TEXT|STRING)\s*\(\s*(\d+)\s*\)$",
            d,
        )
        width = int(m_str.group(1)) if m_str else char_len
        if width:
            return f"VARCHAR({width})"
        return "TEXT"
    if char_len:
        return f"VARCHAR({char_len})"
    return "TEXT"


def _bson_int_is_named_epoch(value: int, key: str) -> bool:
    """True when a BSON integer is an epoch instant on a temporal field name.

    Digit width matches the writer (``transform_engine`` 10-second / 13-millis).
    The field-name owner is schema inference, so catalog and CSV agree.
    """
    if isinstance(value, bool) or not str(key or "").strip():
        return False
    from services.schema_inference import _is_timestamp_field_name

    if not _is_timestamp_field_name(key):
        return False
    text = str(value)
    if text[:1] in "+-":
        text = text[1:]
    return len(text) in {10, 13} and text.isdigit()


def _sample_logical_type(value: Any, key: str = "") -> str:
    if value is None:
        # Null/absent is unknown, not TEXT. Returning "" keeps a null observation
        # from demoting a field that is typed (e.g. OBJECT) in other documents.
        return ""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int):
        # A BSON int on a temporal name that is 10-digit seconds or 13-digit
        # millis is an instant (Mongo extended JSON, HubSpot, Stripe). The
        # same name rule as string inference owns this — a 13-digit ``_id``
        # stays BIGINT. Reclassifying every large int would corrupt keys.
        if _bson_int_is_named_epoch(value, key):
            return "TIMESTAMPTZ"
        # Python int is unbounded — never stamp INT32; BIGINT is safe invent.
        return "BIGINT" if abs(value) > 2_147_483_647 else "INTEGER"
    if isinstance(value, float):
        return "DOUBLE"
    # BSON ObjectId / Binary before generic str/bytes fallthrough.
    try:
        from bson import ObjectId as _BsonObjectId

        if isinstance(value, _BsonObjectId):
            return "OBJECTID"
    except Exception:
        pass
    try:
        from bson.binary import Binary as _BsonBinary

        if isinstance(value, _BsonBinary):
            return "BINARY"
    except Exception:
        pass
    if isinstance(value, datetime.datetime):
        # BSON stores milliseconds since the epoch — an instant, always UTC.
        # Whether the driver hands it back aware is a client setting
        # (``tz_aware``), not a property of the stored value, so a naive render
        # must not be stamped zoneless: that made the timezone policy see
        # naive→naive, ask for no contract, and let Validate clear a batch the
        # writer then refused row by row.
        return "TIMESTAMPTZ"
    if isinstance(value, datetime.date):
        return "DATE"
    if _BSON_DECIMAL and isinstance(value, _BSON_DECIMAL):
        return "DECIMAL"
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "BINARY"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, dict):
        return "OBJECT"
    if isinstance(value, str):
        from services.schema_inference import declared_string_logical_type

        # A BSON string is a declared string: BSON has its own int, double and
        # bool types, so text that merely looks like one ('t', '0012',
        # '20240101') stays text (D190 / D194). Formatted literals (ISO
        # instant, UUID, JSON text) keep their recognised carrier.
        inferred = declared_string_logical_type([value], field_name=key)
        if inferred == "JSON":
            return "OBJECT"
        if inferred == "VARCHAR":
            return "TEXT"
        return inferred
    return "TEXT"


_STRUCTURAL_TYPES = {"OBJECT", "ARRAY", "JSON"}
_TEXTUAL_TYPES = {"TEXT", "VARCHAR"}
# Keep a typed inference when ≥85% of non-null samples agree (industry ELT
# majority vote). Below that, TEXT is safer than a false INTEGER/DATE.
_MONGO_TYPED_MAJORITY = 0.85


def _widen_mongodb_type(current: str, observed: str) -> str:
    """Widen an inferred type across sampled documents, one pair at a time.

    Prefer :func:`_finalize_mongodb_type` with per-type counts, which can also
    tell a sentinel from a real value. This ranked carriers by "specificity",
    which is not an ordering that holds values: BINARY outranked INTEGER, so a
    field with both resolved to BINARY, which holds neither.
    """
    from services.type_lattice import join_logical_types

    return join_logical_types(current, observed)


def _finalize_mongodb_type(type_counts: dict[str, int]) -> str:
    """Resolve a Mongo field type — one TEXT sentinel must not demote 49 ints."""
    chosen, _note = _finalize_mongodb_type_with_note(type_counts)
    return chosen


def _finalize_mongodb_type_with_note(
    type_counts: dict[str, int],
) -> tuple[str, str | None]:
    """Return (resolved type, optional mix warning for Validate honesty).

    Two decisions live here and they are not the same question.

    *Is a textual value among typed ones a sentinel?* That is data quality:
    ``"N/A"`` in a numeric field should quarantine the outlier rather than widen
    the whole column to text, so a strong typed majority keeps its type and
    carries a warning. That policy is Mongo's and stays here.

    *Given the values that really are typed, what holds them all?* That is not a
    vote — it is a join, and it belongs to :mod:`services.type_lattice` along
    with every other schemaless source. Resolving it here by majority typed a
    field of 999 integers and one float as INTEGER, and the float then failed
    the write.
    """
    from services.type_lattice import resolve_observed_types

    counts = {str(k).upper(): int(v) for k, v in (type_counts or {}).items() if v and k}
    total = sum(counts.values())
    if total <= 0:
        return "TEXT", None

    structural = {k: counts[k] for k in _STRUCTURAL_TYPES if counts.get(k, 0) > 0}
    if structural:
        # Sticky: any nested observation keeps a semi-structured type.
        return resolve_observed_types(structural) or "JSON", None

    text_n = counts.get("TEXT", 0) + counts.get("VARCHAR", 0)
    typed = {k: v for k, v in counts.items() if k not in _TEXTUAL_TYPES}
    if not typed:
        return "TEXT", None

    best = resolve_observed_types(typed)
    typed_share = sum(typed.values()) / total
    mix_note: str | None = None
    if typed_share >= _MONGO_TYPED_MAJORITY:
        if text_n > 0:
            mix_note = (
                f"{text_n} TEXT sentinel(s) among {total} samples — majority {best}; "
                "outlier rows may quarantine at write"
            )
        return best, mix_note
    if text_n / total >= (1.0 - _MONGO_TYPED_MAJORITY):
        return "TEXT", None
    if text_n > 0:
        mix_note = (
            f"Mixed Mongo types ({text_n} TEXT / majority {best}) — "
            "outlier rows may quarantine at write"
        )
    return best, mix_note


_NUMERIC_LOGICAL_FAMILY = {
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "INT",
    "FLOAT",
    "DOUBLE",
    "REAL",
    "DECIMAL",
    "NUMERIC",
}

# BSON carriers whose cells are binary floats / integers, so a fixed-point
# stamp measured off stringified samples is invented rather than observed.
_BSON_INEXACT_NUMERIC = {
    "FLOAT",
    "DOUBLE",
    "REAL",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "INT",
}


def _mongodb_types_with_notes(
    documents: Any,
) -> tuple[dict[str, str], dict[str, str]]:
    """``({field: logical type}, {field: type-mix warning})`` from BSON cells."""
    counts: dict[str, dict[str, int]] = {}
    for doc in documents or []:
        if not isinstance(doc, dict):
            continue
        for key, val in doc.items():
            if val is None:
                continue
            observed = _sample_logical_type(val, str(key))
            if not observed:
                continue
            per = counts.setdefault(str(key), {})
            per[observed] = int(per.get(observed, 0)) + 1
    types: dict[str, str] = {}
    notes: dict[str, str] = {}
    for key, per in counts.items():
        resolved, note = _finalize_mongodb_type_with_note(per)
        if resolved:
            types[key] = resolved
        if note:
            notes[key] = note
    return types, notes


def mongodb_bson_column_types(documents: Any) -> dict[str, str]:
    """Resolved logical type per top-level field, from BSON evidence alone.

    Mongo has no catalog, but every cell carries its own BSON type, so a field's
    carrier is evidence rather than a guess. Resolution is the canonical Mongo
    one (:func:`_finalize_mongodb_type_with_note`), so a sentinel string among
    typed cells still keeps the typed majority.
    """
    types, _notes = _mongodb_types_with_notes(documents)
    return types


def mongodb_bson_homogeneous_date_fields(documents: Any) -> set[str]:
    """Fields whose non-null sampled values are all BSON Date datetimes."""
    homogeneous: dict[str, bool] = {}
    for doc in documents or []:
        if not isinstance(doc, dict):
            continue
        for key, value in doc.items():
            if value is None:
                continue
            name = str(key)
            homogeneous[name] = homogeneous.get(name, True) and isinstance(
                value, datetime.datetime
            )
    return {name for name, is_homogeneous in homogeneous.items() if is_homogeneous}


def prefer_bson_numeric_carrier(
    schema: dict[str, str] | None,
    bson_types: dict[str, str] | None,
) -> dict[str, str]:
    """Replace a sample-sized numeric stamp with the stored BSON carrier.

    A schemaless sample can bound the *values it saw* and nothing else. Profiling
    100 stringified doubles that happen to start at ``0.01`` yields
    ``DECIMAL(3,2)``, which then becomes the create-new destination column and
    quarantines every later row at ``10.00`` — a 100k Mongo→PostgreSQL snapshot
    landed 999 rows this way. The BSON type (``double``, ``int``) is the domain
    the source actually declares per cell, so it wins for numeric fields;
    text/temporal/semantic refinement from the sample is untouched.

    ``Decimal128`` is exempt: those cells really are decimal, so sizing them from
    observed digits refines the right family instead of inventing one.
    """
    merged = dict(schema or {})
    for col, bson_type in (bson_types or {}).items():
        stamped = str(merged.get(col) or "").strip()
        if not stamped:
            continue
        base = stamped.split("(", 1)[0].strip().upper()
        if base not in _NUMERIC_LOGICAL_FAMILY:
            continue
        if str(bson_type).strip().upper() not in _BSON_INEXACT_NUMERIC:
            continue
        merged[col] = str(bson_type)
    return merged


def prefer_bson_source_carrier(
    schema: dict[str, str] | None,
    bson_types: dict[str, str] | None,
    *,
    homogeneous_date_fields: set[str] | None = None,
) -> dict[str, str]:
    """Preserve BSON carriers lost by stringification when their evidence fits."""
    merged = prefer_bson_numeric_carrier(schema, bson_types)
    schema_names = {str(name).casefold(): str(name) for name in merged}
    homogeneous_dates = {
        str(name).casefold() for name in (homogeneous_date_fields or set())
    }
    for col, bson_type in (bson_types or {}).items():
        if str(bson_type).strip().upper() != "TIMESTAMPTZ":
            continue
        if str(col).casefold() not in homogeneous_dates:
            continue
        name = schema_names.get(str(col).casefold())
        if name is not None:
            merged[name] = "TIMESTAMPTZ"
    return merged


def _introspect_mongodb(**kwargs) -> dict[str, Any]:
    table = kwargs.get("table")
    try:
        from connectors.mongodb_common import normalize_mongodb_connection_string
        from connectors.mongodb_common import guarded_mongo_client

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
        client = guarded_mongo_client(conn_str, serverSelectionTimeoutMS=10000)
        db_name = kwargs.get("database") or "test"
        db = client[db_name]
        tables = db.list_collection_names()[:100]
        target = table or (tables[0] if tables else None)
        columns: dict[str, dict[str, Any]] = {}
        # Sample BSON types BEFORE stringifying _id — otherwise ObjectId is
        # erased to TEXT and create-new never stamps VARCHAR(24).
        docs = list(db[target].find().limit(100)) if target else []
        reader_cfg = kwargs.get("reader_cfg")
        if reader_cfg is not None and docs:
            # Same expansion as connectors.mongodb_reader._page_docs_to_batch, on
            # the BSON values (ObjectId / Decimal128 carriers survive it).
            from services.json_intelligence import expand_mongo_documents

            raw_keys = {k for doc in docs for k in doc}
            docs = expand_mongo_documents(docs, cfg=reader_cfg)
            expanded = sorted({k for doc in docs for k in doc} - raw_keys)
            if expanded:
                logger.info(
                    "mongodb introspect %s.%s: %d nested column(s) from the reader's "
                    "expansion: %s",
                    db_name,
                    target,
                    len(expanded),
                    ", ".join(expanded[:12]),
                )
        resolved, mix_notes = _mongodb_types_with_notes(docs)
        homogeneous_date_fields = mongodb_bson_homogeneous_date_fields(docs)
        for doc in docs:
            for key, val in list(doc.items()):
                sample_text = "" if val is None else evidence_text(val)
                if key not in columns:
                    # Null-first fields stay untyped until a non-null sample
                    # votes — do not invent TEXT from BSON null alone.
                    columns[key] = {
                        "name": key,
                        "inferred_type": _sample_logical_type(val, key),
                        # A document store enforces no NOT NULL but ``_id``: a
                        # field the sampled documents carry is still NULL (absent)
                        # in the next one that omits it, and in every row an
                        # empty sub-document flattens to. Reporting it required
                        # made the destination column NOT NULL and the sparse row
                        # an EMPTY_VALUE_NOT_NULLABLE block (D147).
                        "nullable": key != "_id",
                        "samples": [sample_text] if sample_text else [],
                    }
                else:
                    samples = columns[key].setdefault("samples", [])
                    if sample_text and len(samples) < 8 and sample_text not in samples:
                        samples.append(sample_text)
        client.close()
        for col in columns.values():
            name = str(col.get("name") or "")
            if resolved.get(name):
                col["inferred_type"] = resolved[name]
                if (
                    col["inferred_type"] == "TIMESTAMPTZ"
                    and name not in homogeneous_date_fields
                ):
                    from services.schema_inference import infer_schema_map

                    samples = [
                        cell_to_string(doc[name], preserve_sql_null=True)
                        for doc in docs
                        if isinstance(doc, dict)
                        and name in doc
                        and doc[name] is not None
                    ]
                    sampled_schema, _intel = infer_schema_map({name: samples})
                    col["inferred_type"] = sampled_schema.get(name) or col[
                        "inferred_type"
                    ]
                if mix_notes.get(name):
                    col["type_mix_warning"] = mix_notes[name]
            elif not col.get("inferred_type"):
                col["inferred_type"] = "TEXT"
            # Re-infer from samples only when majority vote stayed textual / weak —
            # never overwrite a high-confidence sticky numeric/date/structural type.
            # The cells were BSON strings, so the declared-string rule applies:
            # never a number / boolean / digit-temporal (D190 / D194).
            samples = [s for s in (col.get("samples") or []) if str(s).strip()]
            if (
                len(samples) >= 2
                and col["inferred_type"] not in _STRUCTURAL_TYPES
                and col["inferred_type"] in _TEXTUAL_TYPES
            ):
                try:
                    from services.schema_inference import declared_string_logical_type

                    logical = declared_string_logical_type(samples, field_name=col["name"])
                    col["inferred_type"] = "TEXT" if logical == "VARCHAR" else logical
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
            describe_table_key_contract_or_empty,
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
            "endpoint_url": kwargs.get("endpoint_url") or "",
            "region": kwargs.get("region") or "",
        }
        names, types = describe_table_schema(cfg, table)
        key_schema, index_attributes = describe_table_key_contract_or_empty(cfg, table)
        # Sample real items — union every attribute (sparse keys) + native types.
        try:
            from connectors.dynamodb_reader import read_table_batch

            from services.schema_inference import (
                declared_string_logical_type,
                infer_schema_map,
            )

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
                        # A typed-text name was declared S (AttributeDefinitions
                        # or the sampled AttributeValue). DynamoDB has N and
                        # BOOL, so '0012' / 't' stay text (D190 / D194).
                        types[name] = (
                            declared_string_logical_type(
                                samples_by_col.get(name) or [], field_name=name
                            )
                            if name in types
                            else inferred_map[name]
                        )
        except Exception:
            logger.warning("DynamoDB sample inference failed for %s", table, exc_info=True)

        pk_names = [k["name"] for k in key_schema if k.get("name")]
        key_roles = {k["name"]: k.get("key_type", "HASH") for k in key_schema}
        columns = [
            {
                "name": name,
                "inferred_type": types.get(name, "TEXT"),
                "nullable": name not in pk_names,
                "is_primary_key": name in pk_names,
                "dynamo_key_type": key_roles.get(name),
            }
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
            "primary_key_columns": pk_names,
            "dynamo_key_schema": key_schema,
            "dynamo_index_attributes": index_attributes,
        }
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _introspect_elasticsearch(**kwargs) -> dict[str, Any]:
    index = kwargs.get("table") or kwargs.get("database")
    if not index:
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
            raw = client.indices.get_alias(index="*")
            names = sorted(
                name
                for name in (raw or {})
                if name and not str(name).startswith(".")
            )
            return {"ok": True, "columns": [], "tables": names, "schema": ""}
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "columns": [],
                "tables": [],
            }
    try:
        from connectors.elasticsearch_mapping import (
            carrier_for_es_field_type,
            carrier_from_es_property,
        )
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
            # Document identity is first-class for upsert Map suggestions.
            columns.append({
                "name": "_id",
                "inferred_type": "TEXT",
                "nullable": False,
                "semantic_role": "identity",
                "source": "elasticsearch_meta",
            })
            for name, info in props.items():
                if not isinstance(info, dict):
                    info = {"type": "text"}
                es_type = info.get("type", "text")
                mapped = carrier_for_es_field_type(es_type)
                # Nested / object mapping honesty — preserve structure carriers.
                if es_type == "nested":
                    mapped = "ARRAY<JSON>"
                elif es_type == "object" or (not es_type and info.get("properties")):
                    nested_props = info.get("properties") or {}
                    if nested_props:
                        parts = []
                        for child, child_info in nested_props.items():
                            if isinstance(child_info, dict):
                                parts.append(
                                    f"{child}:{_es_mapping_type(str(child_info.get('type') or 'text'))}"
                                )
                            else:
                                parts.append(f"{child}:TEXT")
                        mapped = f"STRUCT<{', '.join(parts)}>" if parts else "JSON"
                    else:
                        mapped = "JSON"
                samples = samples_by_name.get(name, [])
                semantic_role = None
                declared_carrier = carrier_from_es_property(info)
                if declared_carrier:
                    # A transfer that created this field recorded the carrier it
                    # declared; an ES field type cannot express DECIMAL(12,4) vs
                    # DECIMAL, so the declaration outranks re-derivation.
                    mapped = declared_carrier
                elif es_type == "date" or (es_type in ("text", "keyword") and samples):
                    intel = infer_column(samples, field_name=name)
                    inferred = str(intel["logical_type"])
                    semantic_role = intel.get("semantic_role")
                    if inferred in ("VARCHAR", "TEXT"):
                        inferred = mapped if mapped != "VARCHAR" else inferred
                    mapped = inferred
                elif es_type == "binary":
                    mapped = "BINARY"
                col_rec: dict[str, Any] = {
                    "name": name,
                    "inferred_type": mapped,
                    "nullable": True,
                    "es_type": es_type or "object",
                }
                if semantic_role:
                    col_rec["semantic_role"] = semantic_role
                columns.append(col_rec)
            # Dynamic fields present in docs but absent from index mapping.
            for name in dynamic_keys:
                if name == "_id":
                    continue
                samples = samples_by_name.get(name, [])
                intel = infer_column(samples, field_name=name) if samples else {"logical_type": "TEXT"}
                columns.append({
                    "name": name,
                    "inferred_type": str(intel.get("logical_type") or "TEXT"),
                    "nullable": True,
                    "semantic_role": intel.get("semantic_role"),
                })
            # Also sample hit _id presence for operators mapping identity.
            try:
                resp = client.search(index=index, body={"size": 1, "query": {"match_all": {}}})
                hits = resp.get("hits", {}).get("hits") or []
                if hits and hits[0].get("_id") is not None:
                    columns[0]["sample"] = str(hits[0].get("_id"))
            except Exception as exc:
                logging.getLogger(__name__).warning("Exception suppressed: %s", exc, exc_info=exc)
            return {"ok": True, "tables": [index], "columns": columns, "schema": index}
        finally:
            client.close()
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}


def _es_mapping_type(es_type: str) -> str:
    t = (es_type or "text").lower()
    if t == "long":
        return "BIGINT"
    if t == "integer":
        return "INTEGER"
    if t == "short":
        return "SMALLINT"
    if t == "byte":
        return "TINYINT"
    if t == "double":
        return "DOUBLE"
    if t == "half_float":
        return "FLOAT16"
    if t == "float":
        return "FLOAT"
    if t == "scaled_float":
        # Elasticsearch scaled_float is fixed-point-like — keep as DECIMAL.
        return "DECIMAL"
    if t == "boolean":
        return "BOOLEAN"
    if t == "date":
        return "DATE"
    if t == "date_nanos":
        return "TIMESTAMPTZ"
    if t == "binary":
        return "BINARY"
    if t == "nested":
        return "ARRAY<JSON>"
    if t in {"object", "flattened"}:
        return "JSON"
    if t in {"keyword", "constant_keyword", "wildcard"}:
        return "TEXT"
    if t == "ip":
        return "TEXT"
    if t in {"geo_point", "geo_shape"}:
        return "GEOGRAPHY"
    return "TEXT"


def _sqlite_declared_over_samples(declared: str, inferred: str) -> str:
    """Keep an explicit SQLite DDL token when samples blur it within its family.

    A column's contract is its declaration, not its current contents. SQLite
    records the CREATE token verbatim, so a ``TIMESTAMPTZ`` sampled as naive
    strings must stay offset-aware — otherwise re-reading a table DataFlow
    itself created reports a polarity change against its own DDL. Cross-family
    inference is left alone: an untyped affinity genuinely carries no contract.
    """
    from services.type_system import normalize_logical_type

    decl = (declared or "").strip()
    inf = (inferred or "").strip()
    if not decl or not inf:
        return inferred
    try:
        decl_logical = normalize_logical_type(decl)
        inf_logical = normalize_logical_type(inf)
    except Exception:
        return inferred
    if decl_logical == inf_logical and decl.upper() != inf.upper():
        # Same family, different token — the declaration carries the polarity,
        # precision and width the samples cannot prove.
        return decl
    return inferred


def _sqlite_text_over_numeric_samples(declared: str, inferred: str) -> str:
    """Refuse numeric capacity invented from the contents of a TEXT column.

    TEXT is the exact-digit carrier our own DDL picks for DECIMAL on SQLite
    (REAL would round). Inferring ``DECIMAL(p,s)`` back from those digits invents
    a precision the declaration never had, so re-running a migration into a table
    DataFlow itself created read ``DECIMAL(12,2) → DECIMAL(8,4)`` and blocked as a
    narrowing. Semantic inference (temporal, JSON, UUID, boolean) claims no
    capacity and still applies.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        normalize_logical_type,
    )

    decl = (declared or "").strip()
    if not decl:
        return inferred
    try:
        inf_logical = normalize_logical_type(inferred)
    except Exception:
        return inferred
    if inf_logical in {LOGICAL_DECIMAL, LOGICAL_FLOAT, LOGICAL_INTEGER}:
        return decl
    return inferred


def _sqlite_temporal_precision_over_text(inferred: str, values: Sequence[Any]) -> str:
    """State the fractional-second precision a TEXT temporal carrier holds.

    Sample inference names the family (``TIMESTAMP``) but no precision, and a
    bare temporal token is read downstream as second precision — MySQL and
    Oracle bare ``TIMESTAMP`` really is (0). So re-reading the TEXT column our
    own SQLite DDL picks for a ``TIMESTAMP_NTZ(6)`` source reported
    ``TIMESTAMP_NTZ(6) → TIMESTAMP`` and the second run of an unchanged route
    blocked as a precision collapse. TEXT bounds no fraction at all, so the
    honest statement is the precision the values themselves carry.
    """
    token = (inferred or "").strip()
    if not token or "(" in token:
        return inferred
    if token.upper() not in {
        "TIMESTAMP",
        "TIMESTAMPTZ",
        "TIMESTAMP_NTZ",
        "TIMESTAMP_TZ",
        "TIMESTAMP_LTZ",
        "DATETIME",
        "TIME",
        "TIMETZ",
    }:
        return inferred
    digits = 0
    for value in values:
        if isinstance(value, bytes):
            continue
        match = re.search(r"\d{2}:\d{2}:\d{2}\.(\d{1,9})", str(value))
        if match:
            digits = max(digits, len(match.group(1)))
    return f"{token}({digits})" if digits else inferred


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

    # A file the destination writer has not created yet is "table not found",
    # not a missing source. Connecting here would create an empty database
    # (R3-H3). Source reads still go through require_sqlite_source_file.
    from pathlib import Path

    if path != ":memory:" and not Path(path).is_file():
        return {
            "ok": False,
            "error": f"Table `{table}` not found",
            "columns": [],
            "tables": [],
        }

    try:
        from connectors.sql_identifiers import quote_sql_identifier

        conn = sqlite3.connect(path, timeout=8)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
            if not cur.fetchone():
                return {"ok": False, "error": f"Table `{table}` not found", "columns": [], "tables": []}

            table_q = quote_sql_identifier(table)
            cur.execute(f"PRAGMA table_info({table_q})")
            info_rows = cur.fetchall()
            if not info_rows:
                return {"ok": False, "error": f"No columns for table `{table}`", "columns": [], "tables": []}

            col_names: list[str] = [row[1] for row in info_rows]
            declared_types: dict[str, str] = {row[1]: (row[2] or "").upper() for row in info_rows}

            # Sample up to 100 rows for value-based inference
            samples: dict[str, list[str]] = {name: [] for name in col_names}
            try:
                cur.execute(f"SELECT * FROM {table_q} LIMIT 100")  # nosec B608
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

            # PRAGMA table_info: cid, name, type, notnull, dflt_value, pk
            pragma_by_name = {str(row[1]): row for row in info_rows if row and row[1]}

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
                        # Keep the declared (p,s) — SQLite records the DDL token
                        # verbatim, and dropping it to bare DECIMAL forces
                        # downstream invent to guess a precision from samples.
                        from services.type_system import parse_numeric_precision_scale

                        p, s = parse_numeric_precision_scale(declared)
                        if p is not None:
                            inferred = f"DECIMAL({p},{s if s is not None else 0})"
                        else:
                            inferred = "DECIMAL"
                    elif declared_base in {"INTEGER", "INT", "BIGINT"}:
                        # SQLite INTEGER affinity is a signed int64 storage class
                        # (not PG/MySQL INT32). Stamping INTEGER invents INT32 on
                        # those destinations and rejects/quarantines values >
                        # 2147483647 (audit ITEM 1). Keep BOOLEAN/temporal when
                        # samples prove them; otherwise never-narrower BIGINT.
                        inf_u = str(inferred or "").strip().upper()
                        if inf_u in {"BOOLEAN", "BOOL"}:
                            inferred = "BOOLEAN"
                        elif inf_u in {
                            "DATE",
                            "DATETIME",
                            "TIMESTAMP",
                            "TIME",
                            "TIMESTAMPTZ",
                        }:
                            pass
                        else:
                            inferred = "BIGINT"
                    elif declared_base in {"REAL", "FLOAT", "DOUBLE"}:
                        # REAL affinity holds IEEE-754 float64 values in SQLite.
                        inf_u = str(inferred or "").strip().upper()
                        if inf_u in {"DECIMAL", "NUMERIC", "NUMBER"}:
                            inferred = "DECIMAL"
                        else:
                            inferred = "DOUBLE PRECISION"
                    elif declared_base in {"TEXT", "VARCHAR", "CHAR", "CLOB", "STRING"}:
                        inferred = _sqlite_temporal_precision_over_text(
                            _sqlite_text_over_numeric_samples(
                                declared,
                                _sqlite_declared_over_samples(declared, inferred),
                            ),
                            str_values,
                        )
                    else:
                        inferred = _sqlite_declared_over_samples(declared, inferred)

                prow = pragma_by_name.get(name)
                notnull = int(prow[3] or 0) if prow is not None else 0
                dflt = prow[4] if prow is not None else None
                native_type = str(prow[2] or "").strip() if prow is not None else ""
                col_out: dict[str, Any] = {
                    "name": name,
                    "inferred_type": inferred,
                    "declared_type": native_type,
                    "native_type": native_type,
                    "sample_refined": any(bool(value) for value in values),
                    # Property 6 — never invent nullable=True when PRAGMA says NOT NULL.
                    "nullable": notnull == 0,
                }
                if dflt is not None and str(dflt).strip() != "":
                    col_out["default"] = str(dflt)
                if semantic_role:
                    col_out["semantic_role"] = semantic_role
                columns.append(col_out)

            apply_identity_probe("sqlite", cur, "", table, columns)
            unique_meta = _sqlite_fetch_unique_keys(cur, table_q, info_rows)
            foreign_keys, foreign_keys_meta = _fetch_foreign_keys("sqlite", cur, "", table)
            check_meta = probe_check_constraints("sqlite", cur, "", table).to_dict()
            indexes_meta = probe_secondary_indexes("sqlite", cur, "", table).to_dict()
            return {
                "ok": True,
                "tables": [table],
                "columns": columns,
                "schema": "",
                "primary_key_columns": unique_meta.get("primary_key_columns") or [],
                "unique_keys": unique_meta.get("unique_keys") or [],
                "foreign_keys": foreign_keys,
                "foreign_keys_meta": foreign_keys_meta,
                "check_constraints_meta": check_meta,
                "indexes_meta": indexes_meta,
            }
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
    length: int | None = None,
) -> str:
    """Map Salesforce describe field type → Datawrap logical carrier.

    Currency/percent without Describe precision still get honest DECIMAL defaults
    (never bare DECIMAL that invents warehouse NUMBER). Datetime is UTC → TIMESTAMPTZ.
    String fields with Describe ``length`` stamp ``VARCHAR(n)`` (writer SSOT).
    """
    t = (field_type or "string").strip().lower()
    if t in {"boolean"}:
        return "BOOLEAN"
    if t in {"long"}:
        return "BIGINT"
    if t in {"int", "integer"}:
        return "INTEGER"
    if t in {"double", "currency", "percent"}:
        if precision is not None and scale is not None:
            p, s = int(precision), int(scale)
            if s == 0 and p <= 18:
                return "BIGINT"
            return f"DECIMAL({p},{s})"
        if t == "currency":
            return "DECIMAL(18,2)"
        if t == "percent":
            return "DECIMAL(18,2)"
        return "DOUBLE"
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
        # Salesforce Ids are 15/18-char strings — bound when length absent.
        if length is not None and int(length) > 0:
            return f"VARCHAR({int(length)})"
        return "VARCHAR(18)"
    # string, textarea, phone, url, email, picklist, reference, …
    if length is not None:
        try:
            n = int(length)
            if n > 0:
                return f"VARCHAR({n})"
        except (TypeError, ValueError):
            pass
    return "TEXT"


def hubspot_property_to_logical(
    prop_type: str,
    *,
    field_type: str = "",
    number_display_hint: str = "",
    name: str = "",
) -> str:
    """Map HubSpot property type → Datawrap logical carrier.

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

    columns = []
    for f in fields:
        if not f.get("name"):
            continue
        # Prefer writer SSOT carrier (VARCHAR(n) from Describe length) so Map
        # chips and reverse-ETL quarantine share one honesty bar.
        try:
            from connectors.salesforce_writer import salesforce_field_to_carrier

            inferred = salesforce_field_to_carrier(f)
        except Exception:
            inferred = salesforce_field_to_logical(
                str(f.get("type") or "string"),
                precision=f.get("precision") if isinstance(f.get("precision"), int) else None,
                scale=f.get("scale") if isinstance(f.get("scale"), int) else None,
                length=f.get("length") if isinstance(f.get("length"), int) else None,
            )
        columns.append(
            {
                "name": f["name"],
                "inferred_type": inferred,
                "nullable": bool(f.get("nillable", True)),
                "data_type": f.get("type") or "string",
                "label": f.get("label") or "",
                "is_primary_key": f.get("name") == "Id" or bool(f.get("externalId")),
                "updateable": bool(f.get("updateable", True)),
                "createable": bool(f.get("createable", True)),
                "calculated": bool(f.get("calculated", False)),
                "externalId": bool(f.get("externalId", False)),
                "idLookup": bool(f.get("idLookup", False)),
            }
        )
    pk_cols = ["Id"] if any(c.get("name") == "Id" for c in columns) else []
    unique_keys: list[dict[str, Any]] = []
    for c in columns:
        if c.get("externalId") or (c.get("idLookup") and c.get("name") != "Id"):
            unique_keys.append(
                {
                    "name": f"sf_ext_{c['name']}",
                    "columns": [str(c["name"])],
                    "primary": False,
                    "enforced": True,
                    "external_id": bool(c.get("externalId")),
                }
            )
    return {
        "ok": True,
        "columns": columns,
        "tables": tables,
        "schema": table,
        "primary_key_columns": pk_cols,
        "unique_keys": unique_keys,
    }


def _thin_saas_logical_to_carrier(logical: str) -> str:
    """Map thin-SaaS logical types to Map/DDL carrier names."""
    lt = (logical or "string").strip().lower()
    return {
        "boolean": "BOOLEAN",
        "integer": "INTEGER",
        "decimal": "DECIMAL",
        "float": "FLOAT",
        "datetime": "TIMESTAMP",
        "date": "DATE",
        "json": "JSON",
        "array": "ARRAY",
        "string": "TEXT",
        "text": "TEXT",
    }.get(lt, "TEXT")


def _introspect_thin_saas(brand: str, **kwargs: Any) -> dict[str, Any]:
    """Sample-based typed introspect for Airtable/Notion/Stripe/REST.

    Honesty: improves Map types via ``native_types``; does **not** certify the
    connector as TRANSFER_READY / PRODUCTION_SKU.
    """
    table = (kwargs.get("table") or kwargs.get("database") or "").strip()
    if not table:
        return {"ok": True, "columns": [], "tables": [], "schema": ""}

    cfg = {
        **_saas_cfg(**kwargs),
        "type": brand,
        "format": brand,
    }
    try:
        if brand == "stripe":
            from connectors.stripe import read_object
        else:
            from connectors.rest_api import read_object

        batch = read_object(cfg=cfg, object=table, limit=25, offset=0)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": [table] if table else [],
        }

    meta = getattr(batch, "meta", None) or {}
    native = meta.get("native_types") or meta.get("schema") or {}
    if not isinstance(native, dict):
        native = {}
    headers = list(batch.headers or [])
    columns = [
        {
            "name": name,
            "inferred_type": _thin_saas_logical_to_carrier(str(native.get(name) or "string")),
            "nullable": True,
            "data_type": str(native.get(name) or "string"),
            "label": name,
        }
        for name in headers
        if name
    ]
    columns = _stamp_thin_saas_write_carriers(brand, table, columns, cfg)
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": meta.get("certification") or "planned_typed_read",
        "saas_typed": bool(meta.get("saas_typed")),
    }


def _stamp_thin_saas_write_carriers(
    brand: str,
    table: str,
    columns: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    """Overlay writer SSOT carriers (VARCHAR(n)/DECIMAL) onto thin-SaaS Map types."""
    if not columns:
        return columns
    names = [str(c.get("name") or "") for c in columns if c.get("name")]
    live: dict[str, str] = {}
    try:
        if brand == "stripe":
            from connectors.saas_write_carriers import stripe_live_types_for_columns

            live = stripe_live_types_for_columns(table, names)
        elif brand == "airtable":
            from connectors.airtable_writer import (
                airtable_field_to_carrier,
                _fetch_table_fields,
            )
            from connectors.saas_common import token as saas_token

            access = saas_token(
                str(cfg.get("api_key") or ""),
                str(cfg.get("connection_string") or ""),
                str(cfg.get("username") or ""),
                str(cfg.get("password") or ""),
            )
            base_id = str(cfg.get("database") or "").strip()
            if access and base_id:
                meta_fields, meta_exc = _fetch_table_fields(base_id, table, access)
                if meta_exc is None and meta_fields:
                    for f in meta_fields:
                        if isinstance(f, dict) and f.get("name"):
                            live[str(f["name"])] = airtable_field_to_carrier(f)
        elif brand == "notion":
            from connectors.notion_writer import (
                notion_property_to_carrier,
                _fetch_database_properties,
                _database_id,
            )
            from connectors.saas_common import token as saas_token

            access = saas_token(
                str(cfg.get("api_key") or ""),
                str(cfg.get("connection_string") or ""),
                str(cfg.get("username") or ""),
                str(cfg.get("password") or ""),
            )
            db_id = _database_id(table or str(cfg.get("database") or ""))
            if access and db_id:
                props, options = _fetch_database_properties(db_id, access)
                for name, typ in (props or {}).items():
                    live[name] = notion_property_to_carrier(
                        typ,
                        option_names=(
                            (options or {}).get(str(name).lower())
                            or (options or {}).get(name)
                        ),
                    )
    except Exception:
        live = {}
    if not live:
        return columns
    live_l = {str(k).lower(): v for k, v in live.items()}
    out: list[dict[str, Any]] = []
    for c in columns:
        name = str(c.get("name") or "")
        stamped = live.get(name) or live_l.get(name.lower())
        if stamped:
            out.append({**c, "inferred_type": stamped})
        else:
            out.append(c)
    return out


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
            "inferred_type": "VARCHAR(65536)",
            "nullable": False,
            "data_type": "string",
            "label": "Record ID",
        }
    ]
    try:
        from connectors.hubspot_writer import hubspot_property_to_carrier
    except Exception:
        hubspot_property_to_carrier = None  # type: ignore[assignment]
    for p in props:
        if not p.get("name") or p["name"] == "id":
            continue
        if hubspot_property_to_carrier is not None:
            inferred = hubspot_property_to_carrier(p)
        else:
            inferred = hubspot_property_to_logical(
                str(p.get("type") or "string"),
                field_type=str(p.get("fieldType") or ""),
                number_display_hint=str(p.get("numberDisplayHint") or ""),
                name=str(p.get("name") or ""),
            )
        columns.append(
            {
                "name": p["name"],
                "inferred_type": inferred,
                "nullable": True,
                "data_type": p.get("type") or "string",
                "label": p.get("label") or "",
            }
        )
    return {"ok": True, "columns": columns, "tables": tables, "schema": table}


def _introspect_shopify(**kwargs: Any) -> dict[str, Any]:
    """Shopify Admin core + live metafield definitions → Map carriers."""
    table = (kwargs.get("table") or kwargs.get("database") or "customers").strip()
    cfg = _saas_cfg(**kwargs)
    cfg["shop"] = cfg.get("host") or ""
    metafield_defs: list[dict[str, Any]] = []
    try:
        from connectors.shopify import describe_metafield_definitions

        metafield_defs = describe_metafield_definitions(cfg, table) or []
    except Exception:
        metafield_defs = []
    try:
        from connectors.saas_write_carriers import (
            shopify_core_field_carriers,
            shopify_live_types_for_columns,
        )

        # Core Admin fields always stamp (note 5000, email 255) even without samples.
        core = shopify_core_field_carriers(table)
        names = list(
            dict.fromkeys(
                [
                    *core.keys(),
                    *[
                        f"{d.get('namespace')}.{d.get('key')}"
                        for d in metafield_defs
                        if d.get("namespace") and d.get("key")
                    ],
                    *[str(d.get("key")) for d in metafield_defs if d.get("key")],
                ]
            )
        )
        live = shopify_live_types_for_columns(
            table, names, metafield_defs=metafield_defs
        )
        for k, v in core.items():
            live.setdefault(k, v)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": [table]}

    columns = [
        {
            "name": name,
            "inferred_type": carrier,
            "nullable": name.lower() != "id",
            "data_type": carrier,
            "label": name,
        }
        for name, carrier in live.items()
        if name
    ]
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": "planned_typed_read",
        "saas_typed": True,
    }


def _introspect_zendesk(**kwargs: Any) -> dict[str, Any]:
    """Zendesk ticket/user/org fields → Map carriers (writer SSOT)."""
    table = (kwargs.get("table") or kwargs.get("database") or "tickets").strip()
    cfg = _saas_cfg(**kwargs)
    try:
        from connectors.zendesk import describe_fields
        from connectors.zendesk_writer import zendesk_field_to_carrier

        fields = describe_fields(cfg, table) or []
    except Exception as exc:
        # Fail closed — never invent seed VARCHAR carriers as saas_typed truth.
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": [],
        }

    seen: set[str] = set()
    columns: list[dict[str, Any]] = []
    for f in fields:
        if not isinstance(f, dict):
            continue
        carrier = zendesk_field_to_carrier(f)
        for key in (f.get("name"), f.get("title")):
            name = str(key or "").strip()
            if not name or name.lower() in seen:
                continue
            seen.add(name.lower())
            columns.append(
                {
                    "name": name,
                    "inferred_type": carrier,
                    "nullable": True,
                    "data_type": str(f.get("type") or carrier),
                    "label": str(f.get("title") or name),
                }
            )
    if not columns:
        return {
            "ok": False,
            "error": (
                f"Zendesk schema Describe returned no fields for {table!r} — "
                "refuse Map VARCHAR invent (empty→null invent risk)."
            ),
            "columns": [],
            "tables": [table],
        }
    return {
        "ok": True,
        "columns": columns,
        "tables": [table],
        "schema": table,
        "certification": "planned_typed_read",
        "saas_typed": True,
    }


def _kafka_value_to_logical(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "BOOLEAN"
    if isinstance(value, int) and not isinstance(value, bool):
        if abs(value) <= 2_147_483_647:
            return "INTEGER"
        return "BIGINT" if -(2**63) <= value < 2**63 else "DECIMAL"
    if isinstance(value, float):
        return "DOUBLE"
    # The exact JSON decoder keeps a number binary64 would round as Decimal;
    # typing it TEXT flipped the whole column to text (D203).
    if isinstance(value, Decimal):
        return "DECIMAL" if value.is_finite() else "DOUBLE"
    if isinstance(value, dict):
        return "JSON"
    if isinstance(value, list):
        return "ARRAY"
    if isinstance(value, str):
        try:
            from services.schema_inference import declared_string_logical_type

            # JSON spells numbers and booleans itself, so a JSON *string* that
            # looks like one ("20240101", "0012", "t") is declared text, not
            # an INTEGER / TIMESTAMP / BOOLEAN (D190). A JSON string is not a
            # LOB either: collapsing VARCHAR → TEXT made Validate fingerprint
            # TEXT while Execute rematerialized MySQL LONGTEXT / Oracle CLOB.
            return declared_string_logical_type([value], field_name="") or "VARCHAR"
        except Exception:
            return "VARCHAR"
    return "TEXT"


def _introspect_kafka(**kwargs: Any) -> dict[str, Any]:
    """Infer Kafka topic field types from a small poll of JSON/Debezium envelopes."""
    try:
        from connectors.kafka_reader import infer_topic_schema
        from services.copy_kafka_common import is_sasl_mechanism, kafka_security_options
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    # The connector form keeps the SASL mechanism in ``database``; "PLAIN" is
    # not a topic, so a topic list must not turn into a sample of it.
    database = str(kwargs.get("database") or "")
    topic = (
        kwargs.get("table") or ("" if is_sasl_mechanism(database) else database) or ""
    ).strip()
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
        # The same SASL listener Test dialed (D83).
        **kafka_security_options(kwargs),
    }
    if not topic:
        try:
            from connectors.kafka_reader import list_topics

            topics = list_topics(cfg)
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "columns": [],
                "tables": [],
            }
        return {"ok": True, "columns": [], "tables": topics, "schema": ""}
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


# A bounded scroll is not a census. Inventing NUMERIC(4,2) from the first
# eight prices quarantined every later value that needed another integer digit.
_PAYLOAD_SAMPLE_ENGINES = frozenset({
    "qdrant",
    "mongodb",
    "dynamodb",
    "elasticsearch",
    "opensearch",
    "couchbase",
    "redis",
    "weaviate",
    "pinecone",
    "milvus",
})


def sample_page_is_not_a_precision_contract(source_db: str) -> bool:
    """Payload and document engines do not declare decimal precision."""
    key = (source_db or "").lower().strip()
    if not key:
        return False
    if key in _PAYLOAD_SAMPLE_ENGINES:
        return True
    try:
        from src.transfer.connector_capabilities import CATALOG_ID_ALIASES
    except Exception:
        return False
    return CATALOG_ID_ALIASES.get(key, key) in _PAYLOAD_SAMPLE_ENGINES


def _unbound_sampled_decimal(carrier: str) -> str:
    """Drop a precision invented from a sample page.

    Catalog ``DECIMAL(p,s)`` must not be passed here. A payload sample that
    happened to fit ``DECIMAL(4,2)`` is not the column's contract.
    """
    from services.type_system import (
        LOGICAL_DECIMAL,
        normalize_logical_type,
        parse_numeric_precision_scale,
    )

    text = str(carrier or "").strip()
    if not text:
        return text
    try:
        logical = normalize_logical_type(text)
    except (TypeError, ValueError):
        return text
    if logical != LOGICAL_DECIMAL:
        return text
    precision, _scale = parse_numeric_precision_scale(text)
    if precision is None:
        return text
    return "DECIMAL"


def _introspect_qdrant(**kwargs: Any) -> dict[str, Any]:
    """Infer Qdrant payload fields from payload_schema and a bounded scroll.

    Embeddings stay out of the catalog. Missing collection is create-new
    (empty columns, ok) so dest preflight does not invent a 404 as a schema.
    """
    collection = str(kwargs.get("table") or kwargs.get("database") or "").strip()
    cfg = {
        "host": kwargs.get("host") or "localhost",
        "port": int(kwargs.get("port") or 6333),
        "username": kwargs.get("username") or "",
        "password": kwargs.get("password") or "",
        "connection_string": kwargs.get("connection_string") or "",
        "ssl": bool(kwargs.get("ssl", False)),
    }
    if not collection:
        try:
            from connectors.qdrant_writer import qdrant_rest

            session, base_url, headers = qdrant_rest(cfg)
            listed = session.get(f"{base_url}/collections", headers=headers, timeout=10)
            if listed.status_code != 200:
                return {
                    "ok": False,
                    "error": f"Qdrant collection list failed: {listed.status_code}",
                    "columns": [],
                    "tables": [],
                }
            body = listed.json() if listed.content else {}
            result = body.get("result") if isinstance(body, dict) else {}
            collections = result.get("collections") if isinstance(result, dict) else []
            names = sorted(
                str(item.get("name") or "")
                for item in (collections or [])
                if isinstance(item, dict) and item.get("name")
            )
            return {"ok": True, "columns": [], "tables": names, "schema": ""}
        except Exception as exc:
            return {
                "ok": False,
                "error": f"{type(exc).__name__}: {exc}",
                "columns": [],
                "tables": [],
            }
    try:
        from connectors.qdrant_reader import QDRANT_OMIT_PAYLOAD_KEYS, read_points_batch
        from connectors.qdrant_writer import _qdrant_live_payload_types, qdrant_rest
        from services.schema_inference import infer_column
    except Exception as exc:
        return {"ok": False, "error": str(exc), "columns": [], "tables": []}

    session, base_url, headers = qdrant_rest(cfg)
    exists = session.get(
        f"{base_url}/collections/{collection}", headers=headers, timeout=10
    )
    if exists.status_code == 404:
        return {"ok": True, "columns": [], "tables": [collection], "schema": collection}
    if exists.status_code != 200:
        return {
            "ok": False,
            "error": f"Qdrant collection probe failed: {exists.status_code}",
            "columns": [],
            "tables": [collection],
        }
    try:
        info = exists.json()
    except Exception:
        info = {}
    declared = _qdrant_live_payload_types(info if isinstance(info, dict) else None)
    try:
        batch, _nxt = read_points_batch(
            cfg=cfg, collection=collection, limit=50, qdrant_offset=None
        )
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": [collection]}

    samples: dict[str, list[str]] = {h: [] for h in (batch.headers or [])}
    for row in batch.rows or []:
        for i, h in enumerate(batch.headers or []):
            if i < len(row) and row[i] is not None:
                samples.setdefault(h, []).append(str(row[i]))
    names = list(batch.headers or [])
    for name in declared:
        if name and name not in names and name not in QDRANT_OMIT_PAYLOAD_KEYS:
            names.append(name)
    columns: list[dict[str, Any]] = []
    for name in names:
        if name in QDRANT_OMIT_PAYLOAD_KEYS:
            continue
        carrier = str(declared.get(name) or "").strip()
        if not carrier:
            inferred = infer_column(samples.get(name) or [], field_name=name)
            carrier = str(
                (inferred.get("logical_type") if isinstance(inferred, dict) else inferred)
                or "TEXT"
            )
            # A scroll of payload points is not a precision contract. DECIMAL(4,2)
            # from the first page quarantined the rest of the collection on
            # Postgres NUMERIC(4,2). Bare DECIMAL is unbounded there and the
            # create-new floor elsewhere. A declared payload schema is kept.
            carrier = _unbound_sampled_decimal(carrier)
        columns.append(
            {
                "name": name,
                "inferred_type": carrier,
                "nullable": True,
                "data_type": carrier,
                "source": "qdrant_payload",
            }
        )
    return {"ok": True, "columns": columns, "tables": [collection], "schema": collection}


def _introspect_neo4j(**kwargs: Any) -> dict[str, Any]:
    """List Neo4j labels, and sample one label's properties when named."""
    from connectors.neo4j import list_labels, read_object

    label = str(kwargs.get("table") or "").strip()
    common = {
        "host": str(kwargs.get("host") or ""),
        "port": int(kwargs.get("port") or 7474),
        "database": str(kwargs.get("database") or "neo4j"),
        "username": str(kwargs.get("username") or ""),
        "password": str(kwargs.get("password") or ""),
        "ssl": bool(kwargs.get("ssl")),
    }
    try:
        labels = list_labels(**common)
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    if not label:
        return {"ok": True, "columns": [], "tables": labels, "schema": common["database"]}
    try:
        batch = read_object(cfg=common, object=label, limit=20)
    except Exception as exc:
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "columns": [],
            "tables": labels or [label],
        }
    columns = [
        {"name": name, "inferred_type": "TEXT", "nullable": True}
        for name in (batch.headers or [])
    ]
    return {"ok": True, "columns": columns, "tables": labels or [label], "schema": label}


def _introspect_weaviate(**kwargs: Any) -> dict[str, Any]:
    """List Weaviate classes, and read one class's properties when named."""
    import requests

    from connectors.weaviate_writer import (
        _base_url,
        _headers,
        _weaviate_property_to_carrier,
    )

    class_name = str(kwargs.get("table") or kwargs.get("database") or "").strip()
    base = _base_url(
        str(kwargs.get("host") or ""),
        int(kwargs.get("port") or 8080),
        bool(kwargs.get("ssl")),
        str(kwargs.get("connection_string") or ""),
    )
    headers = _headers(str(kwargs.get("api_key") or ""))
    try:
        listed = requests.get(f"{base}/v1/schema", headers=headers, timeout=10)
        listed.raise_for_status()
        body = listed.json() if listed.content else {}
    except Exception as exc:
        return {"ok": False, "error": f"{type(exc).__name__}: {exc}", "columns": [], "tables": []}
    classes = body.get("classes") if isinstance(body, dict) else []
    names = sorted(
        str(item.get("class") or "")
        for item in (classes or [])
        if isinstance(item, dict) and item.get("class")
    )
    if not class_name:
        return {"ok": True, "columns": [], "tables": names, "schema": ""}
    match = next(
        (
            item
            for item in (classes or [])
            if isinstance(item, dict)
            and str(item.get("class") or "").lower() == class_name.lower()
        ),
        None,
    )
    if match is None:
        return {
            "ok": True,
            "columns": [],
            "tables": names,
            "schema": class_name,
        }
    columns = []
    for prop in match.get("properties") or []:
        if not isinstance(prop, dict) or not prop.get("name"):
            continue
        columns.append(
            {
                "name": str(prop["name"]),
                "inferred_type": _weaviate_property_to_carrier(prop.get("dataType")),
                "nullable": True,
            }
        )
    return {
        "ok": True,
        "columns": columns,
        "tables": names or [str(match.get("class"))],
        "schema": str(match.get("class") or class_name),
    }


def catalog_read_back(dest_db: str, physical: str) -> str:
    """The ``inferred_type`` this module's introspection reports for ``physical``.

    Introspection hands back a logical lattice type, not the DDL spelling: an
    Oracle ``CLOB`` reads back as ``TEXT``. Anything that must recognise a type
    DataFlow itself created (run 2 into its own table) compares against this,
    not against the CREATE spelling (D198). Engines without a catalog mapper
    here read back the physical spelling unchanged.
    """
    from services.dest_dialect_facts import _normalize_dest_db

    db = _normalize_dest_db(dest_db)
    raw = str(physical or "").strip()
    if not raw:
        return ""
    if "oracle" in db:
        from services.schema_introspect_enterprise import _oracle_to_logical

        return _oracle_to_logical(raw)
    if db == "sqlserver":
        from services.schema_introspect_enterprise import _sqlserver_to_logical

        # INFORMATION_SCHEMA spells types lower-case; the reader folds (max) in.
        return _sqlserver_to_logical(raw.lower())
    mapper = {
        "postgresql": _pg_to_logical,
        "redshift": _pg_to_logical,
        "mysql": _mysql_to_logical,
        "bigquery": _bq_to_logical,
        "clickhouse": _ch_to_logical,
        "snowflake": _sf_to_logical,
    }.get(db)
    return mapper(raw) if mapper else raw


_ENTERPRISE_INTROSPECT_EXPORTS = frozenset(
    {
        "_introspect_oracle",
        "_introspect_sqlserver",
        "_oracle_apply_timestamp_scale",
        "_oracle_to_logical",
        "_sqlserver_to_logical",
    }
)


def __getattr__(name: str):
    if name in _ENTERPRISE_INTROSPECT_EXPORTS:
        from services import schema_introspect_enterprise

        value = getattr(schema_introspect_enterprise, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
