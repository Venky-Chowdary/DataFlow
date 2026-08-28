"""Schema fingerprinting — detect drift between mapping and execution."""

from __future__ import annotations

import hashlib
import json
from typing import Any

from services.value_serializer import json_default

# Logical families that are dialect synonyms for contract drift (not DDL width).
_FINGERPRINT_TEXT_FAMILY = frozenset({"string", "text"})
_FINGERPRINT_INT_FAMILY = frozenset({"integer", "bigint"})

# Stable physical labels used when replaying legacy hashes after synonym collapse.
_LEGACY_PHYSICAL = {
    "string": "VARCHAR",
    "integer": "INTEGER",
    "float": "DOUBLE",
    "decimal": "DECIMAL",
    "boolean": "BOOLEAN",
    "date": "DATE",
    "datetime": "TIMESTAMP",
    "time": "TIME",
    "json": "JSON",
    "array": "ARRAY",
    "binary": "BINARY",
    "uuid": "UUID",
}


def fingerprint_type_token(inferred: str | None) -> str:
    """Canonical type token for schema fingerprints.

    Dialect synonyms (TEXT/VARCHAR/STRING, INT/INTEGER) must not flip the
    contract hash. Width/precision churn is handled by DDL / coercion gates,
    not by renaming VARCHAR↔VARCHAR(255) as "schema changed".
    """
    try:
        from services.type_system import normalize_logical_type

        logical = normalize_logical_type(inferred)
    except Exception:
        logical = (inferred or "VARCHAR").strip().lower() or "string"
    if logical in _FINGERPRINT_TEXT_FAMILY:
        return "string"
    if logical in _FINGERPRINT_INT_FAMILY:
        return "integer"
    return logical or "string"


def _hash_payload(payload: list[dict[str, Any]]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=json_default)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def fingerprint_schema_legacy(
    columns: list[str],
    column_types: dict[str, str] | None = None,
) -> str:
    """Pre-normalization fingerprint — kept so stored revisions still match."""
    column_types = column_types or {}
    payload = [
        {"name": c, "type": (column_types.get(c) or "VARCHAR").upper()}
        for c in sorted(columns)
    ]
    return _hash_payload(payload)


def live_dest_schema_fingerprint(
    destination_column_types: dict[str, str] | None,
    *,
    destination_table_exists: bool | None = None,
    sync_mode: str = "",
) -> str:
    """Hash observed dest names+types. Empty when there is no live DDL contract.

    Overwrite / create-new / unprobed dest must not invent columns from Map
    or fall back to the engine name. That hid dest-exists drift after Validate.
    """
    mode = str(sync_mode or "").strip().lower()
    try:
        from services.sync_cursor import is_overwrite_sync

        overwrite = is_overwrite_sync(mode)
    except Exception:
        overwrite = mode in {"full_refresh_overwrite", "overwrite"}
    if overwrite:
        return ""
    if destination_table_exists is not True:
        return ""
    types = {
        str(k): str(v)
        for k, v in (destination_column_types or {}).items()
        if str(k).strip()
    }
    if not types:
        return ""
    return fingerprint_schema(list(types.keys()), types)


def live_source_schema_fingerprint(
    source_column_types: dict[str, str] | None,
    *,
    authoritative: bool = False,
) -> str:
    """Hash observed source names+types. Empty when types are inferred or missing.

    Overwrite is a dest concept — source still exists and must bind. File /
    document inference is not a catalog contract; do not invent from Map
    ``source_type`` stamps or fall back to the literal ``map``.
    """
    if not authoritative:
        return ""
    types = {
        str(k): str(v)
        for k, v in (source_column_types or {}).items()
        if str(k).strip()
    }
    if not types:
        return ""
    return fingerprint_schema(list(types.keys()), types)


def fingerprint_schema(
    columns: list[str],
    column_types: dict[str, str] | None = None,
) -> str:
    """Stable hash of column names and canonical logical types."""
    column_types = column_types or {}
    # Case-insensitive column identity — Snowflake/Oracle fold names.
    by_lower: dict[str, str] = {}
    for c in columns:
        key = str(c)
        by_lower.setdefault(key.lower(), key)
    payload = [
        {
            "name": name.lower(),
            "type": fingerprint_type_token(column_types.get(orig) or column_types.get(name)),
        }
        for name, orig in sorted(by_lower.items(), key=lambda kv: kv[0])
    ]
    return _hash_payload(payload)


def _synonym_collapsed_types(
    columns: list[str],
    column_types: dict[str, str] | None,
    *,
    string_label: str = "VARCHAR",
) -> dict[str, str]:
    """Map live types to stable physical labels for legacy-hash compatibility."""
    column_types = column_types or {}
    out: dict[str, str] = {}
    for c in columns:
        token = fingerprint_type_token(column_types.get(c))
        if token == "string":
            out[c] = string_label
        else:
            out[c] = _LEGACY_PHYSICAL.get(token, (column_types.get(c) or "VARCHAR").upper())
    return out


def fingerprint_mappings(mappings: list[dict[str, Any]]) -> str:
    """Hash of approved mapping contract."""
    payload = [
        {
            "source": str(m.get("source") or "").lower(),
            "target": str(m.get("target") or "").lower(),
            "transform": m.get("transform"),
            "confidence": round(float(m.get("confidence", 0)), 3),
        }
        for m in sorted(mappings, key=lambda x: str(x.get("source", "")).lower())
    ]
    return _hash_payload(payload)


def schemas_match(stored_fp: str, columns: list[str], column_types: dict[str, str] | None) -> bool:
    """True when stored hash matches current, legacy, or synonym-collapsed legacy."""
    if not stored_fp:
        return True
    candidates = {
        fingerprint_schema(columns, column_types),
        fingerprint_schema_legacy(columns, column_types),
    }
    # TEXT / VARCHAR / STRING were distinct in the legacy hasher — accept all.
    for label in ("VARCHAR", "TEXT", "STRING"):
        candidates.add(
            fingerprint_schema_legacy(
                columns,
                _synonym_collapsed_types(columns, column_types, string_label=label),
            )
        )
    return stored_fp in candidates
