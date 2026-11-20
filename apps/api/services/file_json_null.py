"""When a JSON ``null`` read from a file is SQL NULL (D144, D307).

The object-store / SFTP / file-export JSON writers keep every key and write
SQL NULL as ``null`` on every column, JSON columns included (D307). A
present ``null`` in a JSON or JSONL file is therefore SQL NULL on a scalar
column, and on a JSON column it is the one spelling that SQL NULL and a JSON
``null`` value share. Gate-8, the read-back sample and the append boundary
dedupe fingerprint both sides through this rule so a nullable ``jsonb``
column does not fail a correct load, and a row the destination already holds
is recognised (R1, R9). A real value against ``null`` still differs.

PostgreSQL's own ``row_to_json`` / ``to_jsonb(record)`` render a SQL NULL
column the same way (``"payload": null``).
"""

from __future__ import annotations

from typing import Any

_DOCUMENT_KINDS = frozenset({"json", "array", "object", "struct", "map"})

#: Destinations whose JSON read-back hands a present ``null`` over as the
#: JSON-null token: the object stores, SFTP, and the local file formats.
FILE_JSON_ENGINES = frozenset({
    "s3", "minio", "s3_compatible", "aws_s3", "gcs", "google_cloud_storage",
    "adls", "azure_blob_storage", "azure_data_lake", "azure_data_lake_storage",
    "sftp", "json", "jsonl", "ndjson", "file", "file_export",
})


def _is_document(ddl_type: str) -> bool:
    from services.type_system import normalize_logical_type

    return normalize_logical_type(ddl_type) in _DOCUMENT_KINDS


def json_null_reads_as_sql_null(value: Any, ddl_type: str, engine: str) -> bool:
    """True when ``value`` fingerprints as SQL NULL for this column.

    * The JSON-null token on a non-document column is the SQL NULL the writer
      emitted (D144).
    * On a document column of a file destination the token, and a source's
      JSON-null text ``null``, are the bytes SQL NULL is written as (D307).
    """
    if not ddl_type or value is None:
        return False
    is_token = type(value).__name__ == "JsonNullToken"
    if is_token and not _is_document(ddl_type):
        return True
    if str(engine or "").strip().lower() not in FILE_JSON_ENGINES or not _is_document(ddl_type):
        return False
    return is_token or (isinstance(value, str) and value.strip() == "null")
