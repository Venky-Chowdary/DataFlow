"""Which destinations can apply a CDC delete (claims audit CLM-2).

A CDC delete is applied by key, and under at-least-once redelivery it is only
safe when the destination can first read the ``_df_lsn`` it already stores for
those keys (``connectors.table_manager._fetch_pk_lsn_map``): an unguarded
delete would wipe a row a newer change recreated. A destination with no such
read used to pass Validate and the snapshot, then fail at the first source
DELETE with the update before it already applied.

One dispatch table answers both questions, so Validate, the run-time guard and
the delete path can never disagree:

* :func:`lsn_fetch_route` is the branch ``_fetch_pk_lsn_map`` takes;
* :func:`cdc_delete_route` adds the destinations whose delete path applies
  without that read (object stores, warehouses, Iceberg, vector stores);
* :func:`cdc_deletes_supported` is the public yes/no the UI picker and the
  Validate gate use.
"""

from __future__ import annotations

#: Error code the Validate gate and the run-time refusal both carry.
CDC_DELETES_UNSUPPORTED_CODE = "CDC_DEST_DELETES_UNSUPPORTED"

_VECTOR_DELETE = frozenset({"pgvector", "qdrant", "weaviate", "pinecone"})
_ICEBERG = frozenset({"iceberg", "apache_iceberg"})


def _key(dest_type: str | None) -> str:
    return str(dest_type or "").strip().lower()


def lsn_fetch_route(dest_type: str | None) -> str | None:
    """The ``_fetch_pk_lsn_map`` branch for ``dest_type``, or ``None``."""
    dt = _key(dest_type)
    if not dt:
        return None
    if dt in ("postgresql", "redshift"):
        return "postgresql"
    if dt == "mysql":
        return "mysql"
    if dt in {"mongodb", "mongo"}:
        return "mongodb"
    if dt == "redis":
        return "redis"
    if dt in {"elasticsearch", "opensearch"}:
        return "elasticsearch"
    if dt == "dynamodb":
        return "dynamodb"
    from connectors.table_manager import _routes_generic_sql_mutate

    if dt == "sqlite" or _routes_generic_sql_mutate(dt):
        return "generic_sql"
    return None


def cdc_delete_route(dest_type: str | None) -> str | None:
    """How ``delete_by_primary_keys`` applies a CDC delete here, or ``None``."""
    dt = _key(dest_type)
    if not dt:
        return None
    from services.dest_precount import _object_store_kind

    if _object_store_kind(dt) in {"s3", "gcs", "adls"}:
        return "object_store"
    if dt in {"snowflake", "bigquery"}:
        return dt
    if dt in _ICEBERG:
        return "iceberg"
    if dt in _VECTOR_DELETE:
        return "vector"
    return lsn_fetch_route(dt)


def cdc_deletes_supported(dest_type: str) -> bool:
    """True when a CDC run into ``dest_type`` can apply source deletes.

    Accepts a catalog SKU (``amazon_rds_postgresql``) or an engine name.
    """
    if cdc_delete_route(dest_type) is not None:
        return True
    from services.dialect_profiles import normalize_driver

    normalized = normalize_driver(str(dest_type or "")) or ""
    return bool(normalized) and cdc_delete_route(normalized) is not None


def unsupported_cdc_deletes_message(dest_type: str, table: str = "") -> str:
    """Plain words for the Validate gate and the run-time refusal."""
    where = f" ({table})" if table else ""
    return (
        f"CDC can't run into this {dest_type or 'destination'} destination{where}: "
        "it can't apply source deletes safely, so deleted rows would stay and the "
        "stream would stop at the first DELETE. Choose a database destination that "
        "supports CDC deletes, or use incremental upsert instead."
    )


__all__ = [
    "CDC_DELETES_UNSUPPORTED_CODE",
    "cdc_delete_route",
    "cdc_deletes_supported",
    "lsn_fetch_route",
    "unsupported_cdc_deletes_message",
]
