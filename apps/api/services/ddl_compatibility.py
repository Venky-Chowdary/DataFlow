"""Target DDL compatibility — real G6 validation beyond bool(mappings)."""

from __future__ import annotations

import re
from typing import Any

from services.type_system import ddl_type, is_lossy_coercion, normalize_logical_type

_VARCHAR_WIDTH = re.compile(r"(?:varchar|char|character\s+varying)\s*\(\s*(\d+)\s*\)", re.I)
_DECIMAL_PRECISION = re.compile(r"(?:decimal|numeric)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", re.I)


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


def _pk_candidates(mappings: list[dict]) -> list[str]:
    keys: list[str] = []
    for m in mappings:
        tgt = str(m.get("target") or "")
        src = str(m.get("source") or "")
        label = f"{src}->{tgt}".lower()
        if tgt.lower() in {"id", "_id"} or tgt.lower().endswith("_id") or "primary" in label:
            keys.append(tgt)
    return keys or []


def _duplicate_pk_in_source(
    sample_rows: list[dict] | None,
    mappings: list[dict],
) -> list[str]:
    if not sample_rows:
        return []
    issues: list[str] = []
    src_by_tgt = {str(m["target"]): str(m["source"]) for m in mappings if m.get("target")}
    for tgt in _pk_candidates(mappings):
        src = src_by_tgt.get(tgt, tgt)
        seen: dict[str, int] = {}
        for row in sample_rows:
            val = str(row.get(src, "")).strip()
            if not val:
                continue
            seen[val] = seen.get(val, 0) + 1
        dupes = [v for v, n in seen.items() if n > 1]
        if dupes:
            issues.append(
                f"Primary key candidate '{tgt}' has {len(dupes)} duplicate value(s) in source sample"
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
    allow_create: bool = True,
) -> tuple[bool, list[str]]:
    """
    Evaluate whether mapped columns can land in the destination DDL.
    Returns (compatible, issues).
    """
    source_schema = source_schema or {}
    target_schema = target_schema or {}
    issues: list[str] = []

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

        src_type = source_schema.get(src, "VARCHAR")
        tgt_type = target_schema.get(tgt)

        if table_exists and target_schema and tgt not in target_schema:
            issues.append(f"Target column '{tgt}' does not exist in destination table")
            continue

        if tgt_type and is_lossy_coercion(src_type, tgt_type):
            issues.append(
                f"Lossy type coercion: {src} ({src_type}) → {tgt} ({tgt_type})"
            )

        if sample_rows and tgt_type:
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

        if not table_exists and allow_create:
            inferred_ddl = ddl_type(dest_db_type, src_type)
            width = _parse_varchar_width(inferred_ddl)
            if width is not None and sample_rows:
                max_len = _max_string_len(_sample_values(sample_rows, src))
                if max_len > width:
                    issues.append(
                        f"Proposed DDL {inferred_ddl} for {tgt} may truncate values (max {max_len} chars)"
                    )

    issues.extend(_duplicate_pk_in_source(sample_rows, mappings))

    if table_exists and target_schema:
        mapped_targets = {str(m.get("target")) for m in mappings if m.get("target")}
        required_unmapped = [
            c for c in target_schema
            if c.lower().endswith("_id") and c not in mapped_targets and c.lower() not in {"id", "_id"}
        ]
        if required_unmapped[:3]:
            issues.append(
                f"{len(required_unmapped)} identifier column(s) in destination are unmapped: "
                f"{', '.join(required_unmapped[:3])}"
            )

    if not dest_connected:
        return True, []

    return len(issues) == 0, issues
