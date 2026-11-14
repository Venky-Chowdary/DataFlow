"""Which destinations can apply a CDC delete (CLM-2 / CLM-8).

Minimal version, derived from the delete dispatch in
``connectors/table_manager.py``: ``delete_by_primary_keys`` applies a delete
either without an LSN read (warehouses, Iceberg, the vector stores that key by
document) or after ``_fetch_pk_lsn_map`` reads the stored ``_df_lsn``. Any
other destination (Redis, Elasticsearch, DynamoDB, Kafka, the SaaS writers,
Milvus, SFTP ...) raises "LSN delete fetch is not wired" at the first source
DELETE, after the run already applied the snapshot and the updates.

The sync-mode picker and ``/capabilities`` ask this before offering CDC.
"""

from __future__ import annotations

# delete_by_primary_keys branches that delete without reading _df_lsn.
_DELETES_WITHOUT_LSN_READ = frozenset({
    "snowflake", "bigquery", "iceberg", "apache_iceberg",
    "pgvector", "qdrant", "weaviate", "pinecone",
})

# _fetch_pk_lsn_map branches (plus _routes_generic_sql_mutate dialects).
_LSN_READ_WIRED = frozenset({
    "postgresql", "redshift", "mysql", "sqlite", "generic_sql", "mongodb", "mongo", "oracle",
})


def _normalize(dest_type: str) -> str:
    key = (dest_type or "").strip().lower()
    if not key:
        return ""
    try:
        from src.transfer.connector_capabilities import resolve_driver_type

        return (resolve_driver_type(key) or key).lower()
    except Exception:  # noqa: BLE001 - an unknown id is simply not supported
        return key


def cdc_deletes_supported(dest_type: str) -> bool:
    """True when a CDC DELETE can be applied to this destination type."""
    dt = _normalize(dest_type)
    if not dt:
        return False
    if dt in _DELETES_WITHOUT_LSN_READ or dt in _LSN_READ_WIRED:
        return True
    from connectors.table_manager import _routes_generic_sql_mutate

    return bool(_routes_generic_sql_mutate(dt))
