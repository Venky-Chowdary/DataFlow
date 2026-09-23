"""Compile ingested rows onto the engines that already execute them.

Fan-out is the product:

* Map pair + write transform / code_crosswalk / omit
* Shape step for derive / default / concat / replace / … (row-local, hash-gated)
* Review queue for anything that is not a closed form or does not bind

Nothing here writes a destination. The Transfer Studio applies the emitted
artifacts the same way an operator would have typed them.
"""

from __future__ import annotations

import re
from collections import defaultdict
from typing import Any

from .classify import (
    classify_rule,
    named_rule_ref,
    named_rule_targets,
    unknown_code_policy,
)
from .ingest import RuleIngestError, ingest_rule_workbook
from .match import name_similarity, unique_linguistic_match
from .normalize import fold, resolve_name, resolve_name_ex, split_qualified
from .roles import GROUNDED_METHODS, ground_workbook_rows

_MAX_SHAPE_STEPS = 100
_MAX_MACRO_DEPTH = 8

_KIND_TO_TRANSFORM = {
    "trim": "trim",
    "case_lower": "lower",
    "case_upper": "upper",
    "date": "date_iso",
    "time": "time_iso",
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
    "json": "parse_json",
    "binary": "binary",
    "strip_controls": "strip_controls",
    "concat": "none",
    "replace": "none",
    "null_if": "none",
    "constant": "none",
    "pad": "none",
    "split": "none",
    "round": "none",
    "truncate": "none",
    "absolute": "none",
    "clamp": "none",
    "substr": "none",
    "prefix": "none",
    "suffix": "none",
    "title": "none",
    "collapse": "none",
    "unicode": "none",
    "filter": "none",
    "divert": "none",
    "join": "none",
    "contract": "none",
    "timezone": "assume_timezone",
    "hash_identity": "none",
    "unnest": "none",
    "flatten": "none",
    "keep_columns": "none",
    "drop_column": "omit",
}

_KIND_LABEL = {
    "direct": "Direct map",
    "lookup": "Code crosswalk",
    "omit": "Omit",
    "trim": "Trim",
    "case_lower": "Lowercase",
    "case_upper": "Uppercase",
    "title": "Title case",
    "collapse": "Collapse spaces",
    "date": "Date → ISO",
    "time": "Time → ISO",
    "email": "Normalize email",
    "phone": "Normalize phone",
    "hash": "Hash PII",
    "cast_integer": "Parse integer",
    "cast_number": "Parse decimal",
    "cast_boolean": "Parse boolean",
    "currency": "Parse currency",
    "percentage": "Parse percentage",
    "json": "Parse JSON",
    "binary": "Binary / base64",
    "strip_controls": "Strip controls",
    "derive": "Derived value",
    "default": "Default if null",
    "concat": "Concatenate",
    "replace": "Replace text",
    "null_if": "Null if sentinel",
    "constant": "Constant value",
    "pad": "Pad",
    "split": "Split column",
    "round": "Round number",
    "truncate": "Truncate number",
    "absolute": "Absolute value",
    "clamp": "Clamp",
    "substr": "Substring",
    "prefix": "Prefix",
    "suffix": "Suffix",
    "unicode": "Normalize Unicode",
    "filter": "Filter rows",
    "divert": "Quarantine rows",
    "join": "Join (review)",
    "contract": "Validate contract",
    "timezone": "Assume timezone",
    "hash_identity": "Hash identity",
    "unnest": "Unnest JSON",
    "flatten": "Flatten JSON",
    "keep_columns": "Keep columns",
    "drop_column": "Drop column",
    "unknown": "Needs review",
}

_NO_SOURCE_OK = frozenset({
    "omit", "constant", "join", "filter", "divert", "contract",
    "hash_identity", "keep_columns", "drop_column",
})
_NO_DEST_OK = frozenset({
    "omit", "join", "filter", "divert", "keep_columns", "drop_column",
})
_PRELOAD_REFUSED_SYNCS = frozenset({"cdc", "scd2", "full_refresh_mirror", "mirror"})
_KIND_FAMILY = {
    "date": "temporal",
    "time": "temporal",
    "timezone": "temporal",
    "cast_integer": "numeric",
    "cast_number": "numeric",
    "currency": "numeric",
    "percentage": "numeric",
    "round": "numeric",
    "truncate": "numeric",
    "absolute": "numeric",
    "clamp": "numeric",
    "cast_boolean": "boolean",
    "json": "json",
    "unnest": "json",
    "flatten": "json",
    "binary": "binary",
}
_INCOMPATIBLE_FAMILIES = frozenset({
    ("temporal", "numeric"),
    ("temporal", "boolean"),
    ("numeric", "temporal"),
    ("boolean", "temporal"),
    ("json", "numeric"),
    ("json", "temporal"),
    ("binary", "temporal"),
    ("binary", "numeric"),
})
_PRECISION = re.compile(
    r"(?P<base>decimal|numeric|number|varchar|character\s+varying|nvarchar|char)"
    r"\s*\(\s*(?P<p>\d+)(?:\s*,\s*(?P<s>\d+))?\s*\)",
    re.I,
)


def _type_family(db_type: str) -> str:
    token = (db_type or "").strip().lower()
    if not token:
        return ""
    if any(part in token for part in ("bool", "bit")):
        return "boolean"
    if any(part in token for part in ("timestamp", "datetime", "date", "time")):
        return "temporal"
    if any(part in token for part in ("int", "decimal", "numeric", "float", "double", "money", "number")):
        return "numeric"
    if "json" in token:
        return "json"
    if any(part in token for part in ("binary", "bytea", "blob", "varbinary")):
        return "binary"
    return "text"


def _type_conflict(kind: str, dest_type: str, source_type: str, dest_name: str) -> str:
    """COMA / iMAP constraint: refuse a typed assignment that would lose meaning."""
    dest_fam = _type_family(dest_type)
    rule_fam = _KIND_FAMILY.get(kind, "")
    if rule_fam and dest_fam and (rule_fam, dest_fam) in _INCOMPATIBLE_FAMILIES:
        return (
            f"Rule is {rule_fam} but destination “{dest_name}” is {dest_type}. "
            "Confirm a cast — a guessed coercion is silent loss."
        )
    src_fam = _type_family(source_type)
    if kind in {"direct", "lookup"} and src_fam and dest_fam and (src_fam, dest_fam) in _INCOMPATIBLE_FAMILIES:
        return (
            f"Source is {source_type} and destination “{dest_name}” is {dest_type}. "
            "Direct/lookup will not invent a cast."
        )
    return ""


def _parse_precision(db_type: str) -> dict[str, Any] | None:
    """Cupid/COMA constraint matcher: DECIMAL(p,s) / VARCHAR(n)."""
    match = _PRECISION.search(db_type or "")
    if not match:
        return None
    base = re.sub(r"\s+", " ", match.group("base").lower())
    family = "text" if base in {"varchar", "character varying", "nvarchar", "char"} else "numeric"
    scale = match.group("s")
    return {
        "family": family,
        "p": int(match.group("p")),
        "s": int(scale) if scale is not None else None,
    }


_IDENTITY_TYPE = re.compile(
    r"\b(?:serial|bigserial|smallserial|identity|auto[_ ]?increment|"
    r"generated\s+always)\b",
    re.I,
)
_BOOL_LOOKUP = frozenset({"true", "false", "t", "f", "0", "1"})


def _identity_dest_conflict(kind: str, dest_type: str, dest_name: str) -> str:
    """Warehouse identity/generated columns are dest-owned. Do not write them."""
    if kind in {"omit", "drop_column", "keep_columns", "filter", "divert"}:
        return ""
    if dest_name and _IDENTITY_TYPE.search(dest_type or ""):
        return (
            f"Destination “{dest_name}” is {dest_type} (identity/generated). "
            "This compiler will not write it — the destination owns the sequence."
        )
    return ""


def _lookup_payload_conflict(pairs: dict[str, str], dest_type: str, dest_name: str) -> str:
    """G20 / COMA: lookup payloads must inhabit the dest type family."""
    if not pairs or not dest_name:
        return ""
    dest_fam = _type_family(dest_type)
    values = [str(value).strip() for value in pairs.values()]
    keys = [str(key).strip() for key in pairs]
    if dest_fam == "numeric" and any(not re.fullmatch(r"-?[\d.]+", value or "") for value in values):
        return (
            f"Lookup values are not numeric but destination “{dest_name}” is {dest_type}. "
            "G20 will not invent a cast."
        )
    if dest_fam == "boolean" and any(value.lower() not in _BOOL_LOOKUP for value in values):
        return (
            f"Lookup values are not boolean tokens but destination “{dest_name}” is {dest_type}. "
            "Write true/false — informal yes/Y is silent remap."
        )
    if dest_fam == "temporal":
        return (
            f"Lookup onto temporal destination “{dest_name}” needs a named date mask. "
            "Codes are not dates."
        )
    if dest_fam == "numeric" and any(re.fullmatch(r"0\d+", key) for key in keys):
        return (
            f"Lookup keys have leading zeros but destination “{dest_name}” is {dest_type}. "
            "G20 matches codes exactly — a number dest drops the zero."
        )
    return ""


def _precision_conflict(kind: str, dest_type: str, source_type: str, dest_name: str) -> str:
    """Overflow / truncation named on the dest type is review, not a guessed clip."""
    dest = _parse_precision(dest_type)
    src = _parse_precision(source_type)
    if dest and src and dest["family"] == src["family"] == "numeric":
        dest_s = dest["s"] if dest["s"] is not None else 0
        src_s = src["s"] if src["s"] is not None else 0
        dest_int = dest["p"] - dest_s
        src_int = src["p"] - src_s
        if dest_int < src_int or dest_s < src_s:
            return (
                f"Destination “{dest_name}” is {dest_type} and source is {source_type}. "
                "A narrower precision/scale would clip digits — confirm a round or this is silent loss."
            )
    if dest and src and dest["family"] == src["family"] == "text" and dest["p"] < src["p"]:
        return (
            f"Destination “{dest_name}” is {dest_type} and source is {source_type}. "
            "A shorter VARCHAR truncates — confirm length or this is silent loss."
        )
    if dest and dest["family"] == "text" and dest["p"] < 64 and kind in {"concat", "prefix", "suffix"}:
        return (
            f"Concat/prefix into “{dest_name}” ({dest_type}) may overflow the named length. "
            "Confirm width — a clipped string is silent loss."
        )
    return ""


def _preload_refused(sync_mode: str) -> bool:
    return (sync_mode or "").strip().lower() in _PRELOAD_REFUSED_SYNCS


def compile_rule_workbook(
    filename: str,
    payload: bytes,
    *,
    source_columns: list[str] | None = None,
    dest_columns: list[str] | None = None,
    source_table: str = "",
    dest_table: str = "",
    source_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
    sync_mode: str = "",
) -> dict[str, Any]:
    """Ingest + classify + bind. The report is the only public artifact."""
    ingested = ingest_rule_workbook(filename, payload)
    rows = ingested.rows
    src_cols = [c for c in (source_columns or []) if c]
    dst_cols = [c for c in (dest_columns or []) if c]
    src_types = {str(k): str(v) for k, v in (source_types or {}).items() if k}
    dst_types = {str(k): str(v) for k, v in (dest_types or {}).items() if k}
    sync = (sync_mode or "").strip()
    header_roles = ground_workbook_rows(rows, src_cols, dst_cols)
    sheet_kinds = _classify_sheets(rows)
    notes_sheets = {item["sheet"] for item in sheet_kinds if item["kind"] == "notes"}
    lookup_pairs = _collect_lookup_pairs(rows)
    orphan_notices = _attach_orphan_enumerations(rows, lookup_pairs, src_cols, dst_cols)
    named_catalog, catalog_conflicts = _collect_named_catalog(rows)
    compiled: list[dict[str, Any]] = []
    shape_steps: list[dict[str, Any]] = []
    seen_edges: dict[tuple[str, str], int] = {}
    seen_dest: dict[str, tuple[str, int]] = {}
    seen_joins: set[tuple[str, str, str]] = set()

    for sheet in sorted(notes_sheets):
        n = next((item["rows"] for item in sheet_kinds if item["sheet"] == sheet), 0)
        compiled.append(_review_notice(
            f"Sheet “{sheet}” looks like commentary, not a mapping spec",
            f"{n} row(s) were not applied. Mapping instructions belong on a "
            "rules or lookup sheet — commentary is never executed.",
            source_table,
            dest_table,
        ))
    for notice in orphan_notices:
        compiled.append(_review_notice(notice, notice, source_table, dest_table))
    for notice in catalog_conflicts:
        compiled.append(_review_notice(notice, notice, source_table, dest_table))

    for raw in rows:
        if str(raw.get("_sheet") or "") in notes_sheets:
            continue
        if _is_pair_only(raw):
            continue
        if _is_catalog_def(raw):
            continue
        apply_name, apply_cols = named_rule_targets(str(raw.get("rule") or ""))
        spoken_src = str(raw.get("source_column") or "").strip()
        spoken_dst = str(raw.get("dest_column") or "").strip()
        fanout_rows = [raw]
        if apply_name and apply_cols and not spoken_src:
            if spoken_dst and len(apply_cols) > 1:
                compiled.append(_review_notice(
                    f"apply {apply_name} to {len(apply_cols)} columns names one destination",
                    "One destination for many sources is a grain conflict. "
                    "Name each edge — they were not applied.",
                    source_table,
                    dest_table,
                ))
                continue
            fanout_rows = [
                {
                    **raw,
                    "source_column": col,
                    "dest_column": spoken_dst or col,
                    "rule": f"%{apply_name}%",
                }
                for col in apply_cols
            ]
        for work in fanout_rows:
            item = _compile_row(
                work,
                src_cols=src_cols,
                dst_cols=dst_cols,
                source_table=source_table,
                dest_table=dest_table,
                lookup_pairs=lookup_pairs,
                named_catalog=named_catalog,
                source_types=src_types,
                dest_types=dst_types,
            )
            _refuse_preload_on_history(item, sync)
            _mark_edge_conflicts(item, seen_edges, seen_dest)
            compiled.append(item)
            for step in _item_shape_steps(item):
                shape_steps.append(step)
            join_item = _join_review_item(work, source_table, dest_table)
            if join_item:
                key = (
                    str(join_item.get("source_column") or ""),
                    str(join_item.get("rule_text") or ""),
                    str(join_item["provenance"].get("row") or ""),
                )
                if key not in seen_joins:
                    seen_joins.add(key)
                    compiled.append(join_item)

    for edge, mapping in lookup_pairs.items():
        src, dst = edge
        if not src or not mapping:
            continue
        already = any(
            fold(r.get("source_column") or "") == fold(src)
            and fold(r.get("dest_column") or "") == fold(dst)
            and r.get("code_crosswalk")
            for r in compiled
        )
        if already:
            continue
        synthetic = _compile_row(
            {
                "source_column": src,
                "dest_column": dst or src,
                "rule": "",
                "lookup_from": next(iter(mapping)),
                "lookup_to": next(iter(mapping.values())),
                "_sheet": "Lookups",
                "_row": 0,
            },
            src_cols=src_cols,
            dst_cols=dst_cols,
            source_table=source_table,
            dest_table=dest_table,
            lookup_pairs=lookup_pairs,
            source_types=src_types,
            dest_types=dst_types,
        )
        _refuse_preload_on_history(synthetic, sync)
        compiled.append(synthetic)

    overflow_steps: list[dict[str, Any]] = []
    if len(shape_steps) > _MAX_SHAPE_STEPS:
        overflow_steps = shape_steps[_MAX_SHAPE_STEPS:]
        shape_steps = shape_steps[:_MAX_SHAPE_STEPS]
        compiled.append(_review_notice(
            f"{len(overflow_steps)} additional shape step(s) past the {_MAX_SHAPE_STEPS}-step recipe cap",
            f"A recipe may hold {_MAX_SHAPE_STEPS} steps. Extra derived / "
            "concat / default rules stayed in review — they were not applied.",
            source_table,
            dest_table,
        ))
    if ingested.truncated:
        compiled.append(_review_notice(
            f"{ingested.truncated} rule row(s) past the 5,000-row ingest cap",
            "Those rows were not read. Split the workbook or raise the cap — they were not applied.",
            source_table,
            dest_table,
        ))

    buckets = {
        "executable": sum(1 for r in compiled if r["status"] == "executable"),
        "needs_confirmation": sum(1 for r in compiled if r["status"] == "needs_confirmation"),
        "conflict": sum(1 for r in compiled if r["status"] == "conflict"),
    }
    named_dest = {fold(r["dest_column"]) for r in compiled if r.get("dest_column")}
    unused_dest = [c for c in dst_cols if fold(c) not in named_dest]
    accounted_src = {fold(r["source_column"]) for r in compiled if r.get("source_column")}
    unmapped_src = [c for c in src_cols if fold(c) not in accounted_src]
    return {
        "filename": filename,
        "rule_count": len(compiled),
        "buckets": buckets,
        "unused_dest_columns": unused_dest[:80],
        "unused_dest_count": len(unused_dest),
        "unmapped_source_columns": unmapped_src[:80],
        "unmapped_source_count": len(unmapped_src),
        "truncated_rows": ingested.truncated,
        "shape_steps": shape_steps,
        "rules": compiled,
        "header_roles": header_roles,
        "sheet_kinds": sheet_kinds,
        "bind_methods": _bind_method_counts(compiled),
        "lookup_coverage": _lookup_coverage(lookup_pairs),
        "named_rules": sorted(named_catalog),
        "matcher": "cupid-linguistic+instance+type+constraint",
        "sync_mode": sync,
        "honesty": (
            "Headers are inferred from the uploaded file and the selected "
            "schemas — they are not a fixed column list. Spoken column names "
            "bind with Cupid-style linguistic matching (unique winner, "
            "threshold, gap). Accepted rules execute deterministically on "
            "Transform + Map. Unrecognised, unbound, or weakly inferred "
            "rules stay in the review queue — they are never applied to a "
            "row. Date masks must name MM/DD vs DD/MM. Orphan enumeration "
            "sheets attach only when the edge is unique. Commentary sheets "
            "are not executed. Named rules expand like Informatica "
            "mapplets / dbt macros (cycle and missing stay in review). "
            "Oracle DECODE and equality CASE compile to code_crosswalk; "
            "complex CASE and Informatica IIF become if(). Clio set-valued "
            "correspondences (A, I, P → ACTIVE) expand every source code. "
            "Blank/empty is default_if_null, never a G20 code. "
            "hash identity is Gate-8 alignment, not PII hash. "
            "COMA type constraints refuse temporal↔numeric assignments. "
            "Cupid constraint matching refuses DECIMAL/VARCHAR overflow "
            "and identity/generated dest writes. Lookup payloads must match "
            "the dest type family; leading-zero codes onto a number dest stay "
            "in review. NOW/TODAY/UUID/RAND are not deterministic. "
            "Skip-deleted without a named column stays in review. "
            "Row predicates compile only when every AND/OR atom is closed; "
            "a leftover tail stays in review. Named IANA zones become "
            "assume_timezone; unnamed zones stay in review. "
            "CDC / SCD2 / mirror refuse pre-load shape — history was not "
            "written by this recipe. Map-plane pairs still compile. "
            "Unknown-code policy is "
            "recorded only — G20 still refuses unmapped codes, never "
            "silent identity. Unused destination "
            "columns are not written. Unmapped source columns stay on Map "
            "as remap-or-omit. Nothing is silently dropped."
        ),
    }


def _classify_sheets(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """COMA-style instance classification: rules vs lookups vs commentary."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("_sheet") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for sheet, items in groups.items():
        n = len(items)
        has_src = sum(1 for row in items if str(row.get("source_column") or "").strip())
        has_dest = sum(1 for row in items if str(row.get("dest_column") or "").strip())
        has_lookup = sum(
            1 for row in items
            if str(row.get("lookup_from") or "").strip() and str(row.get("lookup_to") or "").strip()
        )
        prose = []
        for row in items:
            cells = [str(v) for v in (row.get("_cells") or {}).values() if str(v).strip()]
            if cells:
                prose.append(sum(len(v) for v in cells) / len(cells))
        median_len = sorted(prose)[len(prose) // 2] if prose else 0
        has_named = sum(1 for row in items if str(row.get("rule_name") or "").strip())
        if has_named >= max(1, int(n * 0.6)) and has_src == 0:
            kind = "catalog"
        elif has_src + has_dest + has_lookup == 0 and median_len >= 40:
            kind = "notes"
        elif has_lookup >= max(2, int(n * 0.6)) and has_src == 0:
            kind = "lookups"
        elif has_src or has_dest:
            kind = "rules"
        else:
            kind = "unknown"
        out.append({"sheet": sheet, "kind": kind, "rows": n})
    return out


def _bind_method_counts(compiled: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in compiled:
        method = str(item.get("bind_method") or "")
        if not method:
            continue
        counts[method] = counts.get(method, 0) + 1
    return counts


def _collect_lookup_pairs(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, str]]:
    pairs: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for raw in rows:
        frm = str(raw.get("lookup_from") or "").strip()
        to = str(raw.get("lookup_to") or "").strip()
        if not frm or not to:
            continue
        src = str(raw.get("source_column") or "").strip()
        dst = str(raw.get("dest_column") or src)
        if src:
            pairs[(src, dst)][frm] = to
    return pairs


def _is_pair_only(raw: dict[str, Any]) -> bool:
    """Named or orphan enumeration rows are consumed via lookup_pairs."""
    return bool(
        raw.get("lookup_from")
        and raw.get("lookup_to")
        and not str(raw.get("rule") or "").strip()
    )


def _collect_named_catalog(rows: list[dict[str, Any]]) -> tuple[dict[str, str], list[str]]:
    """Informatica mapplet / dbt macro catalog: name → closed-form expression.

    A mapping row that also names the rule is both a definition and an
    application. Conflicting definitions stay in review — last-wins would
    silently remap every caller.
    """
    catalog: dict[str, str] = {}
    fold_to_name: dict[str, str] = {}
    conflicts: list[str] = []
    for raw in rows:
        name = str(raw.get("rule_name") or "").strip()
        expr = str(raw.get("rule") or "").strip()
        if not name or not expr:
            continue
        self_ref = named_rule_ref(expr)
        if self_ref and fold(self_ref) == fold(name):
            conflicts.append(
                f"Named rule “{name}” refers to itself — it was not catalogued."
            )
            continue
        key = fold(name)
        existing_name = fold_to_name.get(key)
        if existing_name and fold(catalog[existing_name]) != fold(expr):
            conflicts.append(
                f"Named rule “{name}” is defined more than once with different "
                "expressions. The first definition was kept — confirm which to use."
            )
            continue
        if existing_name:
            continue
        fold_to_name[key] = name
        catalog[name] = expr
    return catalog, conflicts


def _is_catalog_def(raw: dict[str, Any]) -> bool:
    return bool(
        str(raw.get("rule_name") or "").strip()
        and str(raw.get("rule") or "").strip()
        and not str(raw.get("source_column") or "").strip()
    )


def _expand_named_rule(
    text: str,
    catalog: dict[str, str],
    stack: tuple[str, ...] = (),
) -> tuple[str, str, str]:
    """dbt-style expand-before-compile. Return (expression, catalog_name, missing)."""
    ref = named_rule_ref(text)
    if not ref:
        return text, "", ""
    want = fold(ref)
    found_name = ""
    found_expr = ""
    for name, expr in catalog.items():
        if fold(name) == want:
            found_name, found_expr = name, expr
            break
    if not found_name:
        return text, "", ref
    if want in stack:
        return text, found_name, f"{found_name} (cycle)"
    if len(stack) >= _MAX_MACRO_DEPTH:
        return text, found_name, f"{found_name} (too deep)"
    inner, _inner_name, missing = _expand_named_rule(
        found_expr,
        catalog,
        stack + (want,),
    )
    if missing:
        return text, found_name, missing
    return inner, found_name, ""


def _lookup_coverage(pairs: dict[tuple[str, str], dict[str, str]]) -> list[dict[str, Any]]:
    return [
        {"source": src, "dest": dst, "pairs": len(mapping)}
        for (src, dst), mapping in pairs.items()
        if src and mapping
    ]


def _attach_orphan_enumerations(
    rows: list[dict[str, Any]],
    lookup_pairs: dict[tuple[str, str], dict[str, str]],
    src_cols: list[str],
    dst_cols: list[str],
) -> list[str]:
    """Informatica Domains/Enumerations + Clio: compile a named edge, never invent one."""
    orphans: dict[str, dict[str, str]] = defaultdict(dict)
    for raw in rows:
        frm = str(raw.get("lookup_from") or "").strip()
        to = str(raw.get("lookup_to") or "").strip()
        if frm and to and not str(raw.get("source_column") or "").strip():
            orphans[str(raw.get("_sheet") or "")][frm] = to
    notices: list[str] = []
    for sheet, mapping in orphans.items():
        if not mapping:
            continue
        candidates: list[tuple[str, str, str, str]] = []
        for raw in rows:
            if str(raw.get("lookup_from") or "").strip():
                continue
            spoken_src = str(raw.get("source_column") or "").strip()
            spoken_dst = str(raw.get("dest_column") or "").strip()
            if not spoken_src and not spoken_dst:
                continue
            classified = classify_rule(str(raw.get("rule") or ""))
            src = resolve_name(spoken_src, src_cols) if src_cols and spoken_src else spoken_src
            dst = resolve_name(spoken_dst, dst_cols) if dst_cols and spoken_dst else spoken_dst
            hint = str(classified.get("reason") or "").startswith("lookup named")
            name_hit = any(
                label and name_similarity(sheet, label) >= 0.72
                for label in (spoken_src, spoken_dst, src, dst)
                if label
            )
            if not (hint or name_hit):
                continue
            edge_src = src or spoken_src
            edge_dst = dst or spoken_dst or edge_src
            if edge_src:
                candidates.append((edge_src, edge_dst, spoken_src, spoken_dst))
        unique = {(item[0], item[1]): item for item in candidates}
        if len(unique) == 1:
            src, dst, spoken_src, spoken_dst = next(iter(unique.values()))
            lookup_pairs[(src, dst)].update(mapping)
            if spoken_src:
                lookup_pairs[(spoken_src, spoken_dst or spoken_src)].update(mapping)
            continue
        if not unique:
            pool = [c for c in (dst_cols or src_cols) if c]
            winner, _score = unique_linguistic_match(sheet, pool) if sheet and pool else ("", 0.0)
            if winner:
                lookup_pairs[(winner, winner)].update(mapping)
                continue
            notices.append(
                f"{len(mapping)} code pair(s) on sheet “{sheet or 'Lookups'}” have "
                "no named source column. Name the edge — they were not applied."
            )
            continue
        notices.append(
            f"Sheet “{sheet}” code table matches {len(unique)} columns. "
            "Name the source — a guessed lookup edge is silent remap."
        )
    return notices


def _mark_edge_conflicts(
    item: dict[str, Any],
    seen_edges: dict[tuple[str, str], int],
    seen_dest: dict[str, tuple[str, int]],
) -> None:
    if item["status"] != "executable":
        return
    src = item.get("source_column") or ""
    dst = item.get("dest_column") or ""
    edge = (src, dst)
    if src and dst and edge in seen_edges:
        item["status"] = "conflict"
        item["issues"] = [
            *(item.get("issues") or []),
            f"Duplicate mapping for {src} → {dst} (also row {seen_edges[edge]}).",
        ]
        item["shape_step"] = None
        item["shape_steps"] = []
        return
    if src and dst:
        seen_edges[edge] = int(item["provenance"]["row"])
        dest_key = fold(dst)
        if dest_key and dest_key in seen_dest and seen_dest[dest_key][0] != src:
            other_src, other_row = seen_dest[dest_key]
            item["status"] = "conflict"
            item["issues"] = [
                *(item.get("issues") or []),
                f"Destination “{dst}” is already mapped from “{other_src}” "
                f"(row {other_row}). Two sources writing one dest is a grain conflict.",
            ]
            item["shape_step"] = None
            item["shape_steps"] = []
            return
        if dest_key:
            seen_dest[dest_key] = (src, int(item["provenance"]["row"]))


def _item_shape_steps(item: dict[str, Any]) -> list[dict[str, Any]]:
    if item["status"] != "executable":
        return []
    steps = []
    if item.get("shape_step"):
        steps.append(item["shape_step"])
    steps.extend(item.get("shape_steps") or [])
    return steps


def _refuse_preload_on_history(item: dict[str, Any], sync_mode: str) -> None:
    """CDC / SCD2 / mirror: dest history was not written by this recipe."""
    if not _preload_refused(sync_mode):
        return
    if not item.get("shape_step") and not item.get("shape_steps"):
        return
    item["status"] = "needs_confirmation"
    item["shape_step"] = None
    item["shape_steps"] = []
    item["plane"] = "review"
    item["issues"] = [
        *(item.get("issues") or []),
        (
            f"Transform (pre-load) is not applied on the {sync_mode} route: "
            "it merges each row against history already stored on the destination, "
            "which was not written by this recipe."
        ),
    ]


def _review_notice(text: str, issue: str, source_table: str, dest_table: str) -> dict[str, Any]:
    return {
        "source_table": source_table,
        "source_column": "",
        "map_source": "",
        "dest_table": dest_table,
        "dest_column": "",
        "rule_text": text,
        "kind": "unknown",
        "kind_label": "Needs review",
        "plane": "review",
        "confidence": 0.0,
        "transform": "none",
        "code_crosswalk": None,
        "shape_step": None,
        "shape_steps": [],
        "status": "needs_confirmation",
        "issues": [issue],
        "provenance": {"sheet": "", "row": 0},
    }


def _compile_row(
    raw: dict[str, Any],
    *,
    src_cols: list[str],
    dst_cols: list[str],
    source_table: str,
    dest_table: str,
    lookup_pairs: dict[tuple[str, str], dict[str, str]] | None = None,
    named_catalog: dict[str, str] | None = None,
    source_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
) -> dict[str, Any]:
    spoken_src = str(raw.get("source_column") or "").strip()
    spoken_dst = str(raw.get("dest_column") or "").strip()
    table_from_cell, spoken_src_col = split_qualified(spoken_src)
    spoken_src = spoken_src_col or spoken_src
    spoken_rule = str(raw.get("rule") or "").strip()
    expanded, catalog_name, missing_name = _expand_named_rule(spoken_rule, named_catalog or {})
    rule_text = expanded
    classified = classify_rule(rule_text)
    kind = str(classified.get("kind") or "unknown")

    src_method = dst_method = ""
    src_score = dst_score = 0.0
    if src_cols:
        source_column, src_method, src_score = resolve_name_ex(spoken_src, src_cols)
    else:
        source_column = spoken_src
        src_method = "spoken" if spoken_src else ""
        src_score = 1.0 if spoken_src else 0.0
    if dst_cols:
        dest_column, dst_method, dst_score = resolve_name_ex(spoken_dst, dst_cols)
    else:
        dest_column = spoken_dst
        dst_method = "spoken" if spoken_dst else ""
        dst_score = 1.0 if spoken_dst else 0.0
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
    if classified.get("required"):
        issues.append("Marked required — Validate fail-closes nulls on this dest.")
    if classified.get("unique"):
        issues.append("Marked unique — Validate fail-closes duplicate keys. Confirm the identity column.")
    if kind == "date" and not classified.get("format"):
        issues.append(
            "Source date format was not named. Confirm MM/DD vs DD/MM — "
            "a swapped day is silent loss."
        )
    if missing_name:
        issues.append(
            f"Named rule “{missing_name}” is not in the catalog — it was not applied."
        )
    dest_type = (dest_types or {}).get(dest_column or spoken_dst, "")
    source_type = (source_types or {}).get(source_column or spoken_src, "")
    type_issue = _type_conflict(kind, dest_type, source_type, dest_column or spoken_dst)
    if type_issue:
        issues.append(type_issue)
    prec_issue = _precision_conflict(kind, dest_type, source_type, dest_column or spoken_dst)
    if prec_issue:
        issues.append(prec_issue)
    ident_issue = _identity_dest_conflict(kind, dest_type, dest_column or spoken_dst)
    if ident_issue:
        issues.append(ident_issue)
    policy = unknown_code_policy(spoken_rule)
    if policy["action"] == "refuse":
        policy = unknown_code_policy(rule_text)
    if classified.get("unmapped"):
        policy = {"action": "default", "value": str(classified.get("unmapped") or "")}

    methods = dict(raw.get("_role_methods") or {})
    weak_bind = False
    for key, label in (
        ("source_column", "Source column"),
        ("dest_column", "Destination column"),
    ):
        method = str(methods.get(key) or "")
        if method and method not in GROUNDED_METHODS:
            weak_bind = True
            issues.append(
                f"{label} header was inferred by {method} without a schema "
                "match — confirm the binding."
            )

    review_kinds = {"unknown", "join", "contract"}
    bind_fail = any(
        i.startswith("Source column")
        or i.startswith("Destination column")
        or i.startswith("No source")
        or i.startswith("No destination")
        or i.startswith("Concat ")
        for i in issues
    ) or weak_bind
    low_confidence_reason = bool(
        classified.get("reason") and float(classified.get("confidence") or 0) < 0.9
    )
    unnamed_zone = kind == "timezone" and not classified.get("zone")
    status = "executable"
    if kind in review_kinds or bind_fail or low_confidence_reason or type_issue or prec_issue or ident_issue or unnamed_zone:
        status = "needs_confirmation"

    transform = _KIND_TO_TRANSFORM.get(kind, "none")
    shape_step: dict[str, Any] | None = None
    extra_steps: list[dict[str, Any]] = []
    map_source = source_column
    pairs = dict((lookup_pairs or {}).get((spoken_src, spoken_dst or spoken_src)) or {})
    if not pairs and source_column and dest_column:
        pairs = dict((lookup_pairs or {}).get((source_column, dest_column)) or {})
    if classified.get("mapping"):
        pairs = {**pairs, **classified["mapping"]}
    blocked_keys = {"unmapped", "unknown", "unmatched", "else", "*"}
    pairs = {
        key: value
        for key, value in pairs.items()
        if fold(str(key)) not in blocked_keys and str(key).strip() != "*"
    }

    if kind == "date" and classified.get("format") and source_column and status == "executable":
        shape_step = {
            "op": "parse_date",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} parse {classified.get('format')}",
            "options": {
                "format": classified.get("format"),
                "output_format": classified.get("output_format") or "YYYY-MM-DD",
            },
        }
        transform = "none"
    elif kind == "derive" and dest_column and status == "executable":
        expr = str(classified.get("expression") or "")
        shape_step = {
            "op": "derive_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"{spoken_src or source_column}: {rule_text}"[:80],
            "options": {"to": dest_column, "expression": _bind_expression(expr, source_column)},
        }
        map_source = dest_column
        transform = "none"
    elif kind == "substr" and dest_column and status == "executable":
        expr = str(classified.get("expression") or "")
        shape_step = {
            "op": "derive_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} substr",
            "options": {"to": dest_column, "expression": expr},
        }
        map_source = dest_column
    elif kind == "prefix" and source_column and dest_column and status == "executable":
        shape_step = {
            "op": "derive_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} prefix",
            "options": {
                "to": dest_column,
                "expression": f"concat({_lit(classified.get('value'))}, {source_column})",
            },
        }
        map_source = dest_column
    elif kind == "suffix" and source_column and dest_column and status == "executable":
        shape_step = {
            "op": "derive_column",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} suffix",
            "options": {
                "to": dest_column,
                "expression": f"concat({source_column}, {_lit(classified.get('value'))})",
            },
        }
        map_source = dest_column
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
                **({"regex": True} if classified.get("regex") else {}),
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
    elif kind == "truncate" and source_column and status == "executable":
        shape_step = {
            "op": "truncate_number",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} truncate",
            "options": {"places": classified.get("places") or 0},
        }
    elif kind == "absolute" and source_column and status == "executable":
        shape_step = {
            "op": "absolute",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} abs",
            "options": {},
        }
    elif kind == "clamp" and source_column and status == "executable":
        shape_step = {
            "op": "clamp",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} clamp",
            "options": {"min": classified.get("min"), "max": classified.get("max")},
        }
    elif kind == "title" and source_column and status == "executable":
        shape_step = {
            "op": "case",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} title",
            "options": {"mode": "title"},
        }
    elif kind == "collapse" and source_column and status == "executable":
        shape_step = {
            "op": "collapse_whitespace",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} collapse",
            "options": {},
        }
    elif kind == "unicode" and source_column and status == "executable":
        shape_step = {
            "op": "normalize_unicode",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} unicode",
            "options": {"form": "NFC"},
        }
    elif kind == "filter" and status == "executable":
        shape_step = {
            "op": "filter_rows",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": rule_text[:80] or "filter",
            "options": {
                "condition": classified.get("condition") or "",
                "keep": classified.get("keep", True),
            },
        }
    elif kind == "divert" and status == "executable":
        shape_step = {
            "op": "divert_rows",
            "column": "",
            "enabled": True,
            "on_error": "refuse",
            "label": rule_text[:80] or "divert",
            "options": {
                "condition": classified.get("condition") or "",
                "reason": rule_text[:80] or "workbook divert",
            },
        }
    elif kind == "hash_identity" and dest_column and status == "executable":
        columns = [str(c) for c in (classified.get("columns") or []) if c]
        bound = []
        for name in columns:
            resolved = resolve_name(name, src_cols) if src_cols else name
            if src_cols and not resolved:
                issues.append(f"Hash identity column “{name}” is not on the selected source.")
                status = "needs_confirmation"
                continue
            if resolved and resolved not in bound:
                bound.append(resolved)
        if source_column and source_column not in bound:
            bound.insert(0, source_column)
        if not bound:
            if status == "executable":
                issues.append("Hash identity needs at least one named source column.")
                status = "needs_confirmation"
        else:
            shape_step = {
                "op": "hash_identity",
                "column": "",
                "enabled": True,
                "on_error": "refuse",
                "label": f"hash identity → {dest_column}",
                "options": {"to": dest_column, "columns": bound},
            }
            map_source = dest_column
    elif kind == "unnest" and source_column and status == "executable":
        shape_step = {
            "op": "unnest_json",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} unnest",
            "options": {"to": dest_column or source_column},
        }
        issues.append(
            "UNNEST expands row count. Dest COUNT is the expanded image, not a surplus."
        )
        if dest_column:
            map_source = dest_column
    elif kind == "flatten" and source_column and status == "executable":
        shape_step = {
            "op": "flatten_json",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} flatten",
            "options": {"depth": "top"},
        }
    elif kind == "keep_columns" and status == "executable":
        columns = [str(c) for c in (classified.get("columns") or []) if c]
        bound = []
        for name in columns:
            resolved = resolve_name(name, src_cols) if src_cols else name
            if src_cols and not resolved:
                issues.append(f"Keep column “{name}” is not on the selected source.")
                status = "needs_confirmation"
                continue
            if resolved and resolved not in bound:
                bound.append(resolved)
        if not bound:
            if status == "executable":
                issues.append("Keep columns needs at least one named source column.")
                status = "needs_confirmation"
        else:
            shape_step = {
                "op": "keep_columns",
                "column": "",
                "enabled": True,
                "on_error": "refuse",
                "label": "keep columns",
                "options": {"columns": bound},
            }
    elif kind == "drop_column" and status == "executable":
        spoken = str(classified.get("column") or source_column or spoken_src)
        resolved = resolve_name(spoken, src_cols) if src_cols and spoken else spoken
        if src_cols and spoken and not resolved:
            issues.append(f"Drop column “{spoken}” is not on the selected source.")
            status = "needs_confirmation"
        elif resolved:
            shape_step = {
                "op": "drop_column",
                "column": resolved,
                "enabled": True,
                "on_error": "refuse",
                "label": f"drop {resolved}",
                "options": {},
            }
            if dest_column or resolved:
                transform = "omit"

    if status == "executable" and source_column:
        for extra in classified.get("extras") or []:
            op = extra.get("op")
            if op == "trim":
                extra_steps.append({
                    "op": "trim",
                    "column": source_column,
                    "enabled": True,
                    "on_error": "refuse",
                    "label": f"{source_column} trim",
                    "options": {},
                })
            elif op == "collapse_whitespace":
                extra_steps.append({
                    "op": "collapse_whitespace",
                    "column": source_column,
                    "enabled": True,
                    "on_error": "refuse",
                    "label": f"{source_column} collapse",
                    "options": {},
                })
            elif op == "case":
                extra_steps.append({
                    "op": "case",
                    "column": source_column,
                    "enabled": True,
                    "on_error": "refuse",
                    "label": f"{source_column} {extra.get('mode') or 'lower'}",
                    "options": {"mode": extra.get("mode") or "lower"},
                })
    if status == "executable" and source_column and classified.get("blank_default"):
        extra_steps.append({
            "op": "default_if_null",
            "column": source_column,
            "enabled": True,
            "on_error": "refuse",
            "label": f"{source_column} blank default",
            "options": {"value": classified.get("blank_default") or ""},
        })

    if pairs and (
        kind in {"direct", "lookup", ""}
        or (kind == "unknown" and str(classified.get("reason") or "").startswith("lookup named"))
    ):
        kind = "lookup"
        transform = "none"
        issues = [item for item in issues if "lookup named" not in item]
        if (
            status == "needs_confirmation"
            and not bind_fail
            and not weak_bind
            and not type_issue
            and not prec_issue
            and not ident_issue
            and not classified.get("case_insensitive")
        ):
            status = "executable"

    lookup_issue = (
        _lookup_payload_conflict(pairs, dest_type, dest_column or spoken_dst)
        if (kind == "lookup" or pairs)
        else ""
    )
    if lookup_issue:
        issues.append(lookup_issue)
        status = "needs_confirmation"
        shape_step = None
        extra_steps = []

    if (kind == "lookup" or pairs) and policy["action"] != "refuse":
        issues.append(
            f"Unknown-code policy is {policy['action']}"
            + (f" → {policy['value']}" if policy["value"] else "")
            + ". G20 still refuses unmapped population codes — never silent identity."
        )

    return {
        "source_table": str(raw.get("source_table") or table_from_cell or source_table or ""),
        "source_column": source_column or spoken_src,
        "map_source": map_source or source_column or spoken_src,
        "dest_table": str(raw.get("dest_table") or dest_table or ""),
        "dest_column": dest_column or spoken_dst,
        "rule_text": spoken_rule if catalog_name else rule_text,
        "named_rule": catalog_name or "",
        "resolved_rule": rule_text if catalog_name else "",
        "unknown_code_policy": (
            policy if (kind == "lookup" or pairs) else {"action": "refuse", "value": ""}
        ),
        "kind": kind,
        "kind_label": _KIND_LABEL.get(kind, kind),
        "plane": "review" if status != "executable" else classified.get("plane") or "map",
        "confidence": float(classified.get("confidence") or 0),
        "transform": transform,
        "engine_transform": (
            f"assume_timezone:{classified.get('zone')}"
            if kind == "timezone" and classified.get("zone")
            else ""
        ),
        "timezone": classified.get("zone") or "",
        "code_crosswalk": pairs or None,
        "date_format": classified.get("format") or "",
        "shape_step": shape_step if status == "executable" else None,
        "shape_steps": extra_steps if status == "executable" else [],
        "status": status,
        "issues": issues,
        "required": bool(classified.get("required")),
        "unique": bool(classified.get("unique")),
        "bind_method": src_method or dst_method or "",
        "bind_score": round(min(src_score or 1.0, dst_score or 1.0) if (src_method or dst_method) else 0.0, 3),
        "provenance": {
            "sheet": raw.get("_sheet") or "",
            "row": int(raw.get("_row") or 0),
        },
    }


def _lit(value: Any) -> str:
    text = str(value or "")
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


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
        "shape_steps": [],
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
