"""Compile ingested rows onto the engines that already execute them.

Fan-out is the product:

* Map pair + write transform / code_crosswalk / omit
* Shape step for derive / default / concat / replace / … (row-local, hash-gated)
* Review queue for anything that is not a closed form or does not bind

Nothing here writes a destination. The Transfer Studio applies the emitted
artifacts the same way an operator would have typed them.
"""

from __future__ import annotations

from typing import Any

from .classify import classify_rule
from .ingest import RuleIngestError, ingest_rule_file
from .normalize import resolve_name

# Shape catalog hard cap — a 427-row workbook cannot become 427 steps.
_MAX_SHAPE_STEPS = 100

_KIND_TO_TRANSFORM = {
    "trim": "trim",
    "case_lower": "lower",
    "case_upper": "upper",
    "date": "date_iso",
    "email": "email",
    "phone": "phone",
    "omit": "omit",
    "direct": "none",
    "lookup": "none",
    "hash": "hash_pii",
    "cast_integer": "cast_integer",
    "cast_number": "cast_number",
    "cast_boolean": "cast_boolean",
    "currency": "currency",
    "percentage": "percentage",
    "concat": "none",
    "replace": "none",
    "null_if": "none",
    "constant": "none",
    "pad": "none",
    "split": "none",
    "round": "none",
    "join": "none",
}

_KIND_LABEL = {
    "direct": "Direct map",
    "lookup": "Code crosswalk",
    "omit": "Omit",
    "trim": "Trim",
    "case_lower": "Lowercase",
    "case_upper": "Uppercase",
    "date": "Date → ISO",
    "email": "Normalize email",
    "phone": "Normalize phone",
    "hash": "Hash PII",
    "cast_integer": "Parse integer",
    "cast_number": "Parse decimal",
    "cast_boolean": "Parse boolean",
    "currency": "Parse currency",
    "percentage": "Parse percentage",
    "derive": "Derived value",
    "default": "Default if null",
    "concat": "Concatenate",
    "replace": "Replace text",
    "null_if": "Null if sentinel",
    "constant": "Constant value",
    "pad": "Pad",
    "split": "Split column",
    "round": "Round number",
    "join": "Join (review)",
    "unknown": "Needs review",
}

_NO_SOURCE_OK = frozenset({"omit", "constant", "join"})
_NO_DEST_OK = frozenset({"omit", "join"})


def compile_rule_workbook(
    filename: str,
    payload: bytes,
    *,
    source_columns: list[str] | None = None,
    dest_columns: list[str] | None = None,
    source_table: str = "",
    dest_table: str = "",
) -> dict[str, Any]:
    """Ingest + classify + bind. The report is the only public artifact."""
    rows = ingest_rule_file(filename, payload)
    src_cols = [c for c in (source_columns or []) if c]
    dst_cols = [c for c in (dest_columns or []) if c]
    compiled: list[dict[str, Any]] = []
    shape_steps: list[dict[str, Any]] = []
    seen_edges: dict[tuple[str, str], int] = {}
    seen_joins: set[tuple[str, str, str]] = set()

    for raw in rows:
        item = _compile_row(
            raw,
            src_cols=src_cols,
            dst_cols=dst_cols,
            source_table=source_table,
            dest_table=dest_table,
        )
        edge = (item.get("source_column") or "", item.get("dest_column") or "")
        if edge[0] and edge[1] and edge in seen_edges and item["status"] == "executable":
            item["status"] = "conflict"
            item["issues"] = [
                *item.get("issues") or [],
                f"Duplicate mapping for {edge[0]} → {edge[1]} "
                f"(also row {seen_edges[edge]}).",
            ]
        elif edge[0] and edge[1] and item["status"] == "executable":
            seen_edges[edge] = int(item["provenance"]["row"])
        compiled.append(item)
        step = item.get("shape_step")
        if step and item["status"] == "executable":
            shape_steps.append(step)
        join_item = _join_review_item(raw, source_table, dest_table)
        if join_item:
            key = (
                str(join_item.get("source_column") or ""),
                str(join_item.get("rule_text") or ""),
                str(join_item["provenance"].get("row") or ""),
            )
            if key not in seen_joins:
                seen_joins.add(key)
                compiled.append(join_item)

    if len(shape_steps) > _MAX_SHAPE_STEPS:
        overflow = shape_steps[_MAX_SHAPE_STEPS:]
        shape_steps = shape_steps[:_MAX_SHAPE_STEPS]
        compiled.append({
            "source_column": "",
            "map_source": "",
            "dest_column": "",
            "rule_text": f"{len(overflow)} additional shape step(s) past the {_MAX_SHAPE_STEPS}-step recipe cap",
            "kind": "unknown",
            "kind_label": "Needs review",
            "plane": "review",
            "confidence": 0.0,
            "transform": "none",
            "code_crosswalk": None,
            "shape_step": None,
            "status": "needs_confirmation",
            "issues": [
                f"A recipe may hold {_MAX_SHAPE_STEPS} steps. Extra derived / "
                "concat / default rules stayed in review — they were not applied."
            ],
            "provenance": {"sheet": "", "row": 0},
            "source_table": source_table,
            "dest_table": dest_table,
        })

    buckets = {
        "executable": sum(1 for r in compiled if r["status"] == "executable"),
        "needs_confirmation": sum(1 for r in compiled if r["status"] == "needs_confirmation"),
        "conflict": sum(1 for r in compiled if r["status"] == "conflict"),
    }
    mapped_dest = {
        r["dest_column"]
        for r in compiled
        if r.get("dest_column") and r["status"] == "executable" and r.get("kind") != "omit"
    }
    unused_dest = [c for c in dst_cols if c not in mapped_dest]
    return {
        "filename": filename,
        "rule_count": len(compiled),
        "buckets": buckets,
        "unused_dest_columns": unused_dest[:80],
        "unused_dest_count": len(unused_dest),
        "shape_steps": shape_steps,
        "rules": compiled,
        "honesty": (
            "Accepted rules execute deterministically on Transform + Map. "
            "Unrecognised or unbound rules stay in the review queue — they "
            "are never applied to a row. Unused destination columns are not "
            "written. Nothing is silently dropped."
        ),
    }


def _compile_row(
    raw: dict[str, Any],
    *,
    src_cols: list[str],
    dst_cols: list[str],
    source_table: str,
    dest_table: str,
) -> dict[str, Any]:
    spoken_src = str(raw.get("source_column") or "").strip()
    spoken_dst = str(raw.get("dest_column") or "").strip()
    rule_text = str(raw.get("rule") or "").strip()
    classified = classify_rule(rule_text)
    kind = str(classified.get("kind") or "unknown")

    source_column = resolve_name(spoken_src, src_cols) if src_cols else spoken_src
    dest_column = resolve_name(spoken_dst, dst_cols) if dst_cols else spoken_dst
    if not dest_column and spoken_dst and not dst_cols:
        dest_column = spoken_dst
    if not source_column and spoken_src and not src_cols:
        source_column = spoken_src

    issues: list[str] = []
    if spoken_src and src_cols and not source_column:
        issues.append(f"Source column “{spoken_src}” is not on the selected source.")
    if spoken_dst and dst_cols and not dest_column:
        issues.append(f"Destination column “{spoken_dst}” is not on the selected destination.")
    if not spoken_src and kind not in _NO_SOURCE_OK:
        issues.append("No source column was named.")
    if not spoken_dst and kind not in _NO_DEST_OK:
        issues.append("No destination column was named.")
    if classified.get("reason"):
        issues.append(str(classified["reason"]))

    status = "executable"
    if kind in {"unknown", "join"} or issues:
        status = "needs_confirmation"

    transform = _KIND_TO_TRANSFORM.get(kind, "none")
    shape_step: dict[str, Any] | None = None
    map_source = source_column

    if kind == "derive" and source_column and dest_column and status == "executable":
        expr = str(classified.get("expression") or "")
        to_name = dest_column
        shape_step = {
            "op": "derive_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"{spoken_src or source_column}: {rule_text}"[:80],
            "options": {"to": to_name, "expression": _bind_expression(expr, source_column)},
        }
        map_source = to_name
        transform = "none"
    elif kind == "default" and source_column and status == "executable":
        shape_step = {
            "op": "default_if_null",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} default",
            "options": {"value": classified.get("value") or ""},
        }
    elif kind == "concat" and dest_column and status == "executable":
        spoken_cols = [str(c) for c in (classified.get("columns") or []) if c]
        bound = []
        for name in spoken_cols:
            resolved = resolve_name(name, src_cols) if src_cols else name
            if src_cols and not resolved:
                issues.append(f"Concat column “{name}” is not on the selected source.")
                status = "needs_confirmation"
                continue
            if resolved and resolved not in bound:
                bound.append(resolved)
        if source_column and source_column not in bound:
            bound.insert(0, source_column)
        if len(bound) < 2:
            if status == "executable":
                issues.append("Concat needs at least two named source columns.")
                status = "needs_confirmation"
        else:
            shape_step = {
                "op": "concat_columns",
                "column": "",
                "enabled": True,
                "on_error": "refuse",
                "label": f"concat → {dest_column}",
                "options": {
                    "to": dest_column,
                    "columns": bound,
                    "separator": classified.get("separator") or "",
                },
            }
            map_source = dest_column
            transform = "none"
    elif kind == "replace" and source_column and status == "executable":
        shape_step = {
            "op": "replace",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} replace",
            "options": {
                "search": classified.get("search") or "",
                "replacement": classified.get("replacement") or "",
            },
        }
    elif kind == "null_if" and source_column and status == "executable":
        shape_step = {
            "op": "null_if",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} null-if",
            "options": {"values": classified.get("values") or []},
        }
    elif kind == "constant" and dest_column and status == "executable":
        shape_step = {
            "op": "constant_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"constant {dest_column}",
            "options": {"to": dest_column, "value": classified.get("value") or ""},
        }
        map_source = dest_column
    elif kind == "pad" and source_column and status == "executable":
        shape_step = {
            "op": "pad",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} pad",
            "options": {
                "width": classified.get("width") or 0,
                "side": classified.get("side") or "left",
                "fill": "0",
            },
        }
    elif kind == "split" and source_column and dest_column and status == "executable":
        shape_step = {
            "op": "split_column",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} split",
            "options": {
                "separator": classified.get("separator") or ",",
                "into": dest_column,
            },
        }
        map_source = dest_column
    elif kind == "round" and source_column and status == "executable":
        shape_step = {
            "op": "round_number",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} round",
            "options": {"places": classified.get("places") or 0},
        }

    return {
        "source_table": str(raw.get("source_table") or source_table or ""),
        "source_column": source_column or spoken_src,
        "map_source": map_source or source_column or spoken_src,
        "dest_table": str(raw.get("dest_table") or dest_table or ""),
        "dest_column": dest_column or spoken_dst,
        "rule_text": rule_text,
        "kind": kind,
        "kind_label": _KIND_LABEL.get(kind, kind),
        "plane": "review" if status != "executable" else classified.get("plane") or "map",
        "confidence": float(classified.get("confidence") or 0),
        "transform": transform,
        "code_crosswalk": classified.get("mapping") or None,
        "shape_step": shape_step if status == "executable" else None,
        "status": status,
        "issues": issues,
        "provenance": {
            "sheet": raw.get("_sheet") or "",
            "row": int(raw.get("_row") or 0),
        },
    }


def _join_review_item(
    raw: dict[str, Any],
    source_table: str,
    dest_table: str,
) -> dict[str, Any] | None:
    """A named join key is a missing rule for the query/post-load plane — not silent."""
    join_on = str(raw.get("join_on") or "").strip()
    join_from = str(raw.get("join_from") or "").strip()
    join_type = str(raw.get("join_type") or "").strip()
    if not join_on and not join_from:
        return None
    spoken = " ".join(part for part in (join_type, join_from, "on", join_on) if part).strip()
    return {
        "source_table": str(raw.get("source_table") or source_table or ""),
        "source_column": join_from,
        "map_source": "",
        "dest_table": str(raw.get("dest_table") or dest_table or ""),
        "dest_column": "",
        "rule_text": spoken or "join",
        "kind": "join",
        "kind_label": "Join (review)",
        "plane": "review",
        "confidence": 0.4,
        "transform": "none",
        "code_crosswalk": None,
        "shape_step": None,
        "status": "needs_confirmation",
        "issues": [
            "Join keys are named but this compiler does not invent a "
            "source_query. Confirm the join in Source → query or Operations "
            "Transforms — a guessed grain is how migrations lose or duplicate rows."
        ],
        "provenance": {
            "sheet": raw.get("_sheet") or "",
            "row": int(raw.get("_row") or 0),
        },
    }


def _bind_expression(expr: str, source_column: str) -> str:
    """Use the bound column name inside a derived expression when possible."""
    text = (expr or "").strip()
    if not text:
        return source_column
    return text


def compile_or_error(filename: str, payload: bytes, **kwargs: Any) -> dict[str, Any]:
    try:
        return compile_rule_workbook(filename, payload, **kwargs)
    except RuleIngestError:
        raise
