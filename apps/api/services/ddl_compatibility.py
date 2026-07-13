"""Target DDL compatibility — real G6 validation beyond bool(mappings)."""

from __future__ import annotations

import re
from typing import Any

from services.type_system import ddl_type, is_lossy_coercion, normalize_logical_type

_VARCHAR_WIDTH = re.compile(r"(?:varchar|char|character\s+varying)\s*\(\s*(\d+)\s*\)", re.I)
_DECIMAL_PRECISION = re.compile(r"(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.I)
_SCHEMALESS_DESTS = {"mongodb", "dynamodb", "redis"}
_DB_TYPE_ALIASES = {
    "mongo": "mongodb",
    "mongodb+srv": "mongodb",
    "mongodb_atlas": "mongodb",
    "atlas": "mongodb",
    "cosmos-mongodb": "mongodb",
    "cosmos_mongodb": "mongodb",
    "documentdb": "mongodb",
    "aws_documentdb": "mongodb",
    "dynamo": "dynamodb",
    "redis-kv": "redis",
    "redis_kv": "redis",
}


def _ci_get(schema: dict[str, str], key: str) -> str | None:
    key_l = key.lower()
    for existing_key, value in schema.items():
        if existing_key.lower() == key_l:
            return value
    return None


def _max_string_len(values: list[str]) -> int:
    return max((len(v) for v in values if v), default=0)


def _parse_varchar_width(ddl: str) -> int | None:
    m = _VARCHAR_WIDTH.search(ddl or "")
    return int(m.group(1)) if m else None


def _sample_values(sample_rows: list[dict] | None, column: str) -> list[str]:
    if not sample_rows:
        return []
    out: list[str] = []
    for row in sample_rows:
        val = row.get(column)
        if val is None:
            continue
        out.append(str(val).strip())
    return out


def _normalize_dest_kind(dest_db_type: str | None) -> str:
    raw = (dest_db_type or "postgresql").strip().lower().replace(" ", "_")
    if raw in _DB_TYPE_ALIASES:
        return _DB_TYPE_ALIASES[raw]
    if raw.startswith("mongodb"):
        return "mongodb"
    if raw.startswith("dynamodb"):
        return "dynamodb"
    if raw.startswith("redis"):
        return "redis"
    return raw


def _primary_key_target(
    mappings: list[dict],
    dest_kind: str,
) -> str | None:
    """Return the target column for the most likely primary key.

    For schemaless destinations the target `_id` is the only hard uniqueness
    contract. For SQL destinations prefer exact `id`/`_id` target, then exact
    `id`/`_id` source, then the first `*_id` source.
    """
    src_by_tgt = {str(m.get("target") or ""): str(m.get("source") or "") for m in mappings if m.get("target")}
    tgt_by_src = {str(m.get("source") or ""): str(m.get("target") or "") for m in mappings if m.get("source")}
    srcs = [str(m.get("source") or "") for m in mappings if m.get("source")]
    tgts = [str(m.get("target") or "") for m in mappings if m.get("target")]

    if dest_kind in _SCHEMALESS_DESTS:
        for t in tgts:
            if t.lower() == "_id":
                return t
        pk_src = next((s for s in srcs if s.lower() == "_id"), None)
        if pk_src:
            return tgt_by_src.get(pk_src, pk_src)
        return None

    for t in tgts:
        if t.lower() in {"id", "_id"}:
            return t

    pk_src = None
    for s in srcs:
        if s.lower() in {"id", "_id"}:
            pk_src = s
            break
    if not pk_src:
        for s in srcs:
            if s.lower().endswith("_id"):
                pk_src = s
                break

    if pk_src:
        return tgt_by_src.get(pk_src, pk_src)
    return None


def _duplicate_pk_in_source(
    sample_rows: list[dict] | None,
    mappings: list[dict],
    *,
    dest_kind: str,
) -> list[str]:
    if not sample_rows:
        return []
    issues: list[str] = []
    src_by_tgt = {str(m.get("target") or ""): str(m.get("source") or "") for m in mappings if m.get("target")}

    pk_tgt = _primary_key_target(mappings, dest_kind)
    if not pk_tgt:
        return issues
    src = src_by_tgt.get(pk_tgt, pk_tgt)

    seen: dict[str, int] = {}
    for row in sample_rows:
        val = str(row.get(src, "")).strip()
        if not val:
            continue
        seen[val] = seen.get(val, 0) + 1
    dupes = [v for v, n in seen.items() if n > 1]
    if dupes:
        issues.append(
            f"Primary key candidate '{pk_tgt}' has {len(dupes)} duplicate value(s) in source sample"
        )
    return issues


def evaluate_ddl_compatibility(
    *,
    mappings: list[dict[str, Any]],
    source_schema: dict[str, str] | None = None,
    target_schema: dict[str, str] | None = None,
    sample_rows: list[dict] | None = None,
    table_exists: bool = False,
    dest_connected: bool = False,
    dest_db_type: str = "postgresql",
    allow_create: bool = False,
) -> tuple[bool, list[str]]:
    """
    Evaluate whether mapped columns can land in the destination DDL.
    Returns (compatible, issues).
    """
    source_schema = source_schema or {}
    target_schema = target_schema or {}
    issues: list[str] = []
    dest_kind = _normalize_dest_kind(dest_db_type)
    schemaless = dest_kind in _SCHEMALESS_DESTS

    if not mappings:
        return False, ["No column mappings defined"]

    seen_targets: set[str] = set()
    for m in mappings:
        src = str(m.get("source") or "")
        tgt = str(m.get("target") or "")
        if not src or not tgt:
            issues.append("Mapping missing source or target column")
            continue
        tgt_key = tgt.lower()
        if tgt_key in seen_targets:
            issues.append(f"Duplicate target column in mapping contract: {tgt}")
        seen_targets.add(tgt_key)

        src_type = _ci_get(source_schema, src) or "VARCHAR"
        tgt_type = _ci_get(target_schema, tgt)

        if not schemaless and table_exists and target_schema and tgt_type is None:
            # If the destination connector supports creating tables, we can evolve
            # the target schema (e.g. CREATE TABLE or ALTER TABLE ADD COLUMN) so
            # missing columns do not block the transfer.
            if not allow_create:
                issues.append(f"Target column '{tgt}' does not exist in destination table")
                continue

        if not schemaless and tgt_type and is_lossy_coercion(src_type, tgt_type):
            issues.append(
                f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type})"
            )

        if not schemaless and sample_rows and tgt_type:
            samples = _sample_values(sample_rows, src)
            if samples:
                width = _parse_varchar_width(tgt_type)
                if width is not None:
                    max_len = _max_string_len(samples)
                    if max_len > width:
                        issues.append(
                            f"Value width overflow: {src} sample max {max_len} chars "
                            f"exceeds {tgt} ({tgt_type})"
                        )

                src_logical = normalize_logical_type(src_type)
                tgt_logical = normalize_logical_type(tgt_type)
                if src_logical in {"integer", "decimal"} and tgt_logical == "integer":
                    for s in samples[:20]:
                        if "." in s and s.replace(".", "", 1).replace("-", "", 1).isdigit():
                            issues.append(
                                f"Fractional source values for {src} cannot fit integer target {tgt}"
                            )
                            break

        if not schemaless and not table_exists and allow_create:
            inferred_ddl = ddl_type(dest_db_type, src_type)
            width = _parse_varchar_width(inferred_ddl)
            if width is not None and sample_rows:
                max_len = _max_string_len(_sample_values(sample_rows, src))
                if max_len > width:
                    issues.append(
                        f"Proposed DDL {inferred_ddl} for {tgt} may truncate values (max {max_len} chars)"
                    )

    issues.extend(_duplicate_pk_in_source(sample_rows, mappings, dest_kind=dest_kind))

    if not schemaless and table_exists and target_schema:
        mapped_targets = {str(m.get("target")).lower() for m in mappings if m.get("target")}
        required_unmapped = [
            c
            for c in target_schema
            if c.lower().endswith("_id") and c.lower() not in mapped_targets and c.lower() not in {"id", "_id"}
        ]
        if required_unmapped[:3]:
            issues.append(
                f"{len(required_unmapped)} identifier column(s) in destination are unmapped: "
                f"{', '.join(required_unmapped[:3])}"
            )

    if not dest_connected:
        return True, []

    return len(issues) == 0, issues
