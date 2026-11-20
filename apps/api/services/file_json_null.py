"""When a JSON ``null`` read from a file is SQL NULL (D144).

The object-store / SFTP JSON writers keep SQL NULL and a JSON ``null`` value
apart on document columns (D144): SQL NULL in a JSON column is an absent key,
and a JSON ``null`` value is written as ``null``. On every other column the
writer emits SQL NULL as ``null`` and the file read-back hands it over as the
JSON-null token. Gate-8, the read-back sample and the append boundary dedupe
fingerprint both sides through this rule (R1, R9), so a nullable scalar column
verifies, while a JSON ``null`` value on a document column still compares as
that value and is never confused with SQL NULL (R2, R3).
"""

from __future__ import annotations

from typing import Any

_DOCUMENT_KINDS = frozenset({"json", "array", "object", "struct", "map"})

def _is_document(ddl_type: str) -> bool:
    from services.type_system import normalize_logical_type

    return normalize_logical_type(ddl_type) in _DOCUMENT_KINDS


def json_null_reads_as_sql_null(value: Any, ddl_type: str, engine: str) -> bool:
    """True when the file's JSON-null token is the SQL NULL of a scalar column.

    ``engine`` is accepted for call-site symmetry; the rule is the same for
    every file destination because every one of them keeps the D144 split.
    """
    if not ddl_type or value is None:
        return False
    return type(value).__name__ == "JsonNullToken" and not _is_document(ddl_type)
