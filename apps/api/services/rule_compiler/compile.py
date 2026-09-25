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
    parse_validate_check,
    unknown_code_policy,
    workbook_mask_to_strptime,
)
from .ingest import RuleIngestError, ingest_rule_workbook
from .match import name_similarity, unique_linguistic_match
from .catalog import (
    bind_columns_for_table,
    lookup_type,
    normalize_catalog,
    resolve_catalog_table,
    resolve_source_table,
    split_selected_tables,
)
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
_VALIDATION_SHEET = re.compile(r"validat|constraint|check.?rules?", re.I)
_LOOKUP_SHEET = re.compile(r"lookup|crosswalk|code.?table|enumerat|domain", re.I)
_VALIDATION_ID = re.compile(r"^v\d+$", re.I)
_KIND_INTERPRETATION = {
    "direct": "Direct copy",
    "lookup": "Lookup / code crosswalk",
    "omit": "Omit",
    "trim": "Trim",
    "case_lower": "Lowercase",
    "case_upper": "Uppercase",
    "title": "Title case",
    "collapse": "Collapse spaces",
    "date": "Date → ISO",
    "time": "Time → ISO",
    "email": "Lowercase email",
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
    "contract": "Destination validation",
    "timezone": "Assume timezone",
    "hash_identity": "Hash identity",
    "unnest": "Unnest JSON",
    "flatten": "Flatten JSON",
    "keep_columns": "Keep columns",
    "drop_column": "Drop column",
    "unknown": "Needs review",
}


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
    source_tables: list[str] | None = None,
    source_catalog: dict[str, list[str]] | None = None,
    dest_tables: list[str] | None = None,
    dest_catalog: dict[str, list[str]] | None = None,
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
    selected_tables = split_selected_tables(source_table, source_tables)
    selected_dest_tables = split_selected_tables(dest_table, dest_tables)
    catalog = normalize_catalog(source_catalog)
    dest_cat = normalize_catalog(dest_catalog)
    sync = (sync_mode or "").strip()
    header_roles = ground_workbook_rows(rows, src_cols, dst_cols)
    sheet_kinds = _classify_sheets(rows)
    kind_by_sheet = {str(item["sheet"] or ""): str(item["kind"] or "") for item in sheet_kinds}
    notes_sheets = {item["sheet"] for item in sheet_kinds if item["kind"] == "notes"}
    validation_sheets = {item["sheet"] for item in sheet_kinds if item["kind"] == "validation"}
    lookup_pairs = _collect_lookup_pairs(rows, src_cols)
    orphan_notices = _attach_orphan_enumerations(rows, lookup_pairs, src_cols, dst_cols)
    named_catalog, catalog_conflicts = _collect_named_catalog(rows)
    compiled: list[dict[str, Any]] = []
    shape_steps: list[dict[str, Any]] = []
    seen_edges: dict[tuple[str, str, str, str], int] = {}
    seen_dest: dict[tuple[str, str], tuple[str, int]] = {}
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

    mapping_rows = [
        raw for raw in rows
        if str(raw.get("_sheet") or "") not in notes_sheets
        and str(raw.get("_sheet") or "") not in validation_sheets
    ]
    validation_rows = [
        raw for raw in rows
        if str(raw.get("_sheet") or "") in validation_sheets
    ]

    for raw in mapping_rows:
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
                source_tables=selected_tables,
                source_catalog=catalog,
                dest_tables=selected_dest_tables,
                dest_catalog=dest_cat,
                lookup_pairs=lookup_pairs,
                named_catalog=named_catalog,
                source_types=src_types,
                dest_types=dst_types,
                sheet_kind=kind_by_sheet.get(str(work.get("_sheet") or ""), ""),
            )
            _refuse_preload_on_history(item, sync)
            _mark_edge_conflicts(item, seen_edges, seen_dest)
            _refresh_status_fields(item)
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
            and r.get("code_crosswalk")
            and r.get("status") == "executable"
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
            source_tables=selected_tables,
            source_catalog=catalog,
            dest_tables=selected_dest_tables,
            dest_catalog=dest_cat,
            lookup_pairs=lookup_pairs,
            source_types=src_types,
            dest_types=dst_types,
        )
        _refuse_preload_on_history(synthetic, sync)
        _refresh_status_fields(synthetic)
        compiled.append(synthetic)

    derived_columns = _derived_columns(compiled, dst_cols)
    for raw in validation_rows:
        if _is_pair_only(raw) or _is_catalog_def(raw):
            continue
        item = _compile_row(
            raw,
            src_cols=src_cols,
            dst_cols=dst_cols,
            source_table=source_table,
            dest_table=dest_table,
            source_tables=selected_tables,
            source_catalog=catalog,
            dest_tables=selected_dest_tables,
            dest_catalog=dest_cat,
            lookup_pairs=lookup_pairs,
            named_catalog=named_catalog,
            source_types=src_types,
            dest_types=dst_types,
            sheet_kind="validation",
            extra_columns=derived_columns,
        )
        _refresh_status_fields(item)
        compiled.append(item)

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
    named_dest = {
        fold(r["dest_column"])
        for r in compiled
        if r.get("dest_column") and r.get("kind") != "contract"
    }
    unused_dest = [c for c in dst_cols if fold(c) not in named_dest]
    accounted_src = {
        fold(r["source_column"])
        for r in compiled
        if r.get("source_column") and r.get("kind") not in {"contract", "join"}
    }
    unmapped_src = [c for c in src_cols if fold(c) not in accounted_src]
    detected = len(compiled)
    coverage = {
        "detected": detected,
        "executable": buckets["executable"],
        "review": buckets["needs_confirmation"],
        "conflict": buckets["conflict"],
        "writes": sum(
            1 for r in compiled
            if r["status"] == "executable" and r.get("kind") not in {"contract", "omit", "join"}
        ),
        "validations": sum(
            1 for r in compiled
            if r["status"] == "executable" and r.get("kind") == "contract"
        ),
        "percent": int(round(100 * buckets["executable"] / detected)) if detected else 0,
    }
    return {
        "filename": filename,
        "rule_count": detected,
        "buckets": buckets,
        "coverage": coverage,
        "unused_dest_columns": unused_dest[:80],
        "unused_dest_count": len(unused_dest),
        "unmapped_source_columns": unmapped_src[:80],
        "unmapped_source_count": len(unmapped_src),
        "truncated_rows": ingested.truncated,
        "shape_steps": shape_steps,
        "rules": compiled,
        "header_roles": header_roles,
        "sheet_kinds": sheet_kinds,
        "source_tables": selected_tables,
        "source_catalog_tables": list(catalog.keys()),
        "dest_tables": selected_dest_tables,
        "dest_catalog_tables": list(dest_cat.keys()),
        "projection": _named_projection(compiled),
        "shape_steps_by_table": _shape_steps_by_table(shape_steps),
        "bind_methods": _bind_method_counts(compiled),
        "lookup_coverage": _lookup_coverage(lookup_pairs),
        "named_rules": sorted(named_catalog),
        "matcher": "cupid-linguistic+instance+type+constraint",
        "sync_mode": sync,
        "contracts": [
            item for item in compiled
            if item.get("kind") == "contract" and item.get("contract")
        ],
        "honesty": (
            "Headers are inferred from the uploaded file and the selected "
            "schemas — they are not a fixed column list. Spoken column names "
            "bind with Cupid-style linguistic matching (unique winner, "
            "threshold, gap). Accepted rules execute deterministically on "
            "Transform + Map. Spoken identity (copy without modification, "
            "map A to B) is Direct. Sheets are classified first "
            "(mapping / lookup / validation) and compiled separately. "
            "Lookup tables attach to a mapping edge by source name or "
            "unique dest alias — STATE pairs compile onto state → state_code. "
            "This compiler will not invent the fifty-state table. "
            "Closed-form Validate sentences compile as destination contracts "
            "(not-null, contains, in-set, compare, 2-letter shape) on the "
            "Validate plane — they never write a dest column. Validation_ID "
            "is a rule name, not a destination. Column binds source, dest, "
            "or a derived transform column. Action is on-fail policy "
            "(quarantine), never a destination. Unstructured or unbound "
            "checks stay in review. Coverage is executable / detected on "
            "this workbook — 100% means every compiled row is executable "
            "and none are in review or conflict. Executed and validated "
            "counts land on Proof after the run, not at compile. "
            "Unrecognised, unbound, or weakly inferred "
            "rules stay in the review queue — they are never applied to a "
            "row. An operator may accept a bound rename as Direct. Date masks must name MM/DD vs DD/MM. Orphan enumeration "
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
            "and identity/generated dest writes. LPAD/RPAD use a named fill "
            "(space unless written). MD5/SHA is hash_pii, not email/phone. "
            "TO_CHAR number masks and EXTRACT/DATEADD stay in review. "
            "Lookup payloads must match "
            "the dest type family; leading-zero codes onto a number dest stay "
            "in review. NOW/TODAY/UUID/RAND are not deterministic. "
            "Skip-deleted without a named column stays in review. "
            "Row predicates compile only when every AND/OR atom is closed; "
            "a leftover tail stays in review. Named IANA zones become "
            "assume_timezone; unnamed zones stay in review. "
            "AT TIME ZONE / FROM_TZ name one zone; two-zone convert stays "
            "in review. SCD2 labels (effective date, WHEN MATCHED) are "
            "not parse_date. Column IS [NOT] NULL without keep/required "
            "stays in review. JSON path extract is not parse_json. "
            "regex_matches / REGEXP / RLIKE compile; window/aggregate/"
            "NEXTVAL stay in review. base64 is binary, not a lookup. "
            "N/A is not Direct. utf-8 is not utf minus 8. Leftover text "
            "after NVL/IIF stays in review. TO_NUMBER format masks and "
            "TRY_CAST / SAFE_CAST stay in review. "
            "Several selected tables bind only against the named table "
            "catalog. Unqualified homonyms (id on customers and orders) "
            "stay in review. Destination catalog is the same contract. "
            "Edges are (source_table, column, dest_table, dest). "
            "This compiler will not invent a join grain. "
            "Named executable columns are the projection — nothing else "
            "is written. "
            "Shape steps carry the source table they belong to; a "
            "customers date parse is not applied to orders. "
            "apply_compiled_projection runs that projection on live rows "
            "through ShapeEngine + apply_transform + apply_code_crosswalk, "
            "then evaluates destination contracts and quarantines failures. "
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
    """Classify each sheet before compile: mapping / lookup / validation / notes."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(str(row.get("_sheet") or ""), []).append(row)
    out: list[dict[str, Any]] = []
    for sheet, items in groups.items():
        n = len(items)
        headers = {
            fold(h)
            for row in items
            for h in (row.get("_headers") or [])
            if str(h).strip()
        }
        has_src = sum(1 for row in items if str(row.get("source_column") or "").strip())
        has_dest = sum(1 for row in items if str(row.get("dest_column") or "").strip())
        has_lookup = sum(
            1 for row in items
            if str(row.get("lookup_from") or "").strip() and str(row.get("lookup_to") or "").strip()
        )
        contracts = sum(
            1 for row in items
            if parse_validate_check(str(row.get("rule") or ""))
            or str(classify_rule(str(row.get("rule") or "")).get("kind") or "") == "contract"
        )
        dest_ids = sum(
            1 for row in items
            if _VALIDATION_ID.match(str(row.get("dest_column") or "").strip())
        )
        prose = []
        for row in items:
            cells = [str(v) for v in (row.get("_cells") or {}).values() if str(v).strip()]
            if cells:
                prose.append(sum(len(v) for v in cells) / len(cells))
        median_len = sorted(prose)[len(prose) // 2] if prose else 0
        has_named = sum(1 for row in items if str(row.get("rule_name") or "").strip())
        if _VALIDATION_SHEET.search(sheet) or headers & {
            "validationid", "checkid", "constraintid",
        } or (contracts >= max(2, int(n * 0.6)) and dest_ids >= max(1, int(n * 0.4))):
            kind = "validation"
        elif _LOOKUP_SHEET.search(sheet) and has_lookup >= max(2, int(n * 0.5)):
            kind = "lookups"
        elif has_named >= max(1, int(n * 0.6)) and has_src == 0:
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


def _collect_lookup_pairs(
    rows: list[dict[str, Any]],
    src_cols: list[str] | None = None,
) -> dict[tuple[str, str], dict[str, str]]:
    pairs: dict[tuple[str, str], dict[str, str]] = defaultdict(dict)
    for raw in rows:
        frm = str(raw.get("lookup_from") or "").strip()
        to = str(raw.get("lookup_to") or "").strip()
        if not frm or not to:
            continue
        src = str(raw.get("source_column") or "").strip()
        dst = str(raw.get("dest_column") or "").strip()
        if not src:
            continue
        edge_dst = dst or src
        pairs[(src, edge_dst)][frm] = to
        resolved = resolve_name(src, src_cols) if src_cols else ""
        if resolved:
            pairs[(resolved, dst or resolved)][frm] = to
            if dst:
                pairs[(resolved, dst)][frm] = to
    return pairs


def _pairs_for_edge(
    lookup_pairs: dict[tuple[str, str], dict[str, str]] | None,
    spoken_src: str,
    spoken_dst: str,
    source_column: str,
    dest_column: str,
) -> dict[str, str]:
    """Attach a lookup table to a mapping edge by source, then unique dest alias."""
    store = lookup_pairs or {}
    keys = [
        (spoken_src, spoken_dst),
        (spoken_src, spoken_dst or spoken_src),
        (source_column, dest_column),
        (source_column, dest_column or source_column),
    ]
    for key in keys:
        if key[0] and store.get(key):
            return dict(store[key])
    src_hits: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()
    for (src, _dst), mapping in store.items():
        if not mapping:
            continue
        if fold(src) in {fold(spoken_src), fold(source_column)} and fold(src):
            fingerprint = tuple(sorted((fold(k), fold(v)) for k, v in mapping.items()))
            if fingerprint not in seen:
                seen.add(fingerprint)
                src_hits.append(dict(mapping))
    if len(src_hits) == 1:
        return src_hits[0]
    dest_labels = [label for label in (spoken_dst, dest_column) if label]
    dest_hits: list[dict[str, str]] = []
    dest_seen: set[tuple[str, ...]] = set()
    for (src, dst), mapping in store.items():
        if not mapping or not dest_labels:
            continue
        if not any(fold(dst) == fold(label) or fold(src) == fold(label) for label in dest_labels):
            continue
        fingerprint = tuple(sorted((fold(k), fold(v)) for k, v in mapping.items()))
        if fingerprint not in dest_seen:
            dest_seen.add(fingerprint)
            dest_hits.append(dict(mapping))
    if len(dest_hits) == 1:
        return dest_hits[0]
    return {}


def _pairs_for_contract(
    lookup_pairs: dict[tuple[str, str], dict[str, str]] | None,
    spoken_src: str,
    spoken_dst: str,
    source_column: str,
    dest_column: str,
) -> dict[str, str]:
    """Attach lookup values to a Validate check. Unique dest alias only — no invented table."""
    direct = _pairs_for_edge(lookup_pairs, spoken_src, spoken_dst, source_column, dest_column)
    if direct:
        return direct
    store = lookup_pairs or {}
    labels = {fold(name) for name in (spoken_src, spoken_dst, source_column, dest_column) if name}
    hits: list[dict[str, str]] = []
    seen: set[tuple[str, ...]] = set()
    for (src, dst), mapping in store.items():
        if not mapping:
            continue
        if fold(src) in labels or fold(dst) in labels:
            fingerprint = tuple(sorted((fold(k), fold(v)) for k, v in mapping.items()))
            if fingerprint not in seen:
                seen.add(fingerprint)
                hits.append(dict(mapping))
    if len(hits) == 1:
        return hits[0]
    dest_labels = [name for name in (spoken_src, spoken_dst, dest_column) if name]
    contained: list[dict[str, str]] = []
    contained_seen: set[tuple[str, ...]] = set()
    for (src, _dst), mapping in store.items():
        if not mapping or not src:
            continue
        if any(fold(src) and fold(src) in fold(label) for label in dest_labels):
            fingerprint = tuple(sorted((fold(k), fold(v)) for k, v in mapping.items()))
            if fingerprint not in contained_seen:
                contained_seen.add(fingerprint)
                contained.append(dict(mapping))
    if len(contained) == 1:
        return contained[0]
    return {}


def _derived_columns(compiled: list[dict[str, Any]], dest_columns: list[str]) -> list[str]:
    names: list[str] = [c for c in dest_columns if c]
    for item in compiled:
        dest = str(item.get("dest_column") or "").strip()
        if dest and dest not in names:
            names.append(dest)
        step = item.get("shape_step") if isinstance(item.get("shape_step"), dict) else None
        to = str((step or {}).get("options", {}).get("to") or "").strip() if step else ""
        if to and to not in names:
            names.append(to)
    return names


def _action_for(status: str) -> str:
    if status == "executable":
        return "auto"
    if status == "conflict":
        return "conflict"
    return "review"


def _normalize_on_fail(text: str) -> str:
    """Workbook Action is policy. Skip would be silent drop — still quarantine."""
    token = fold(text)
    if token in {"review", "warn", "warning", "flag"}:
        return "review"
    return "quarantine"


def _unique_lookup_values(pairs: dict[str, str] | None) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for value in (pairs or {}).values():
        text = str(value or "").strip()
        key = fold(text)
        if text and key not in seen:
            seen.add(key)
            values.append(text)
    return values


def _refresh_status_fields(item: dict[str, Any]) -> None:
    """Keep action / plane honest after conflict or CDC preload refuse."""
    status = str(item.get("status") or "")
    item["action"] = _action_for(status)
    if status != "executable":
        item["plane"] = "review"


def _interpretation_for(kind: str, classified: dict[str, Any] | None = None) -> str:
    spoken = str((classified or {}).get("interpretation") or "").strip()
    if spoken:
        return spoken
    return _KIND_INTERPRETATION.get(kind, _KIND_LABEL.get(kind, kind))


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
        if parse_validate_check(expr) or str(classify_rule(expr).get("kind") or "") == "contract":
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
    seen_edges: dict[tuple[str, str, str, str], int],
    seen_dest: dict[tuple[str, str], tuple[str, int]],
) -> None:
    if item["status"] != "executable":
        return
    src_table = str(item.get("source_table") or "")
    dst_table = str(item.get("dest_table") or "")
    src = item.get("source_column") or ""
    dst = item.get("dest_column") or ""
    edge = (fold(src_table), fold(src), fold(dst_table), fold(dst))
    if src and dst and edge in seen_edges:
        item["status"] = "conflict"
        item["issues"] = [
            *(item.get("issues") or []),
            f"Duplicate mapping for {src_table + '.' if src_table else ''}{src} → "
            f"{dst_table + '.' if dst_table else ''}{dst} (also row {seen_edges[edge]}).",
        ]
        item["shape_step"] = None
        item["shape_steps"] = []
        return
    if src and dst:
        seen_edges[edge] = int(item["provenance"]["row"])
        dest_key = (fold(dst_table), fold(dst))
        qualified_src = f"{src_table}.{src}" if src_table else src
        if dest_key[1] and dest_key in seen_dest and seen_dest[dest_key][0] != qualified_src:
            other_src, other_row = seen_dest[dest_key]
            item["status"] = "conflict"
            item["issues"] = [
                *(item.get("issues") or []),
                f"Destination “{dst_table + '.' if dst_table else ''}{dst}” is already "
                f"mapped from “{other_src}” (row {other_row}). Two sources writing "
                "one dest is a grain conflict.",
            ]
            item["shape_step"] = None
            item["shape_steps"] = []
            return
        if dest_key[1]:
            seen_dest[dest_key] = (qualified_src, int(item["provenance"]["row"]))


def _named_projection(compiled: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Executable source columns the operator named — not the whole catalog."""
    by_table: dict[str, list[dict[str, str]]] = {}
    seen: set[tuple[str, str]] = set()
    for item in compiled:
        if item.get("status") != "executable" or item.get("kind") in {"omit", "join", "contract"}:
            continue
        table = str(item.get("source_table") or "")
        col = str(item.get("source_column") or "")
        if not col:
            continue
        key = (fold(table), fold(col))
        if key in seen:
            continue
        seen.add(key)
        by_table.setdefault(table or "(unnamed)", []).append({
            "source_column": col,
            "dest_column": str(item.get("dest_column") or ""),
            "dest_table": str(item.get("dest_table") or ""),
        })
    return [{"source_table": table, "columns": cols} for table, cols in by_table.items()]


def _stamp_shape_step(
    step: dict[str, Any],
    source_table: str,
    dest_table: str,
) -> dict[str, Any]:
    """Copy a step and pin the table it belongs to. ShapeEngine ignores extras."""
    stamped = dict(step)
    if source_table and not stamped.get("source_table"):
        stamped["source_table"] = source_table
    if dest_table and not stamped.get("dest_table"):
        stamped["dest_table"] = dest_table
    return stamped


def _item_shape_steps(item: dict[str, Any]) -> list[dict[str, Any]]:
    if item["status"] != "executable":
        return []
    table = str(item.get("source_table") or "")
    dest = str(item.get("dest_table") or "")
    steps: list[dict[str, Any]] = []
    if item.get("shape_step"):
        steps.append(item["shape_step"])
    steps.extend(item.get("shape_steps") or [])
    tagged = [_stamp_shape_step(step, table, dest) for step in steps if isinstance(step, dict)]
    if item.get("shape_step") and isinstance(item["shape_step"], dict):
        item["shape_step"] = _stamp_shape_step(item["shape_step"], table, dest)
    if item.get("shape_steps"):
        item["shape_steps"] = [
            _stamp_shape_step(step, table, dest)
            for step in item["shape_steps"]
            if isinstance(step, dict)
        ]
    return tagged


def _shape_steps_by_table(steps: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group stamped steps so each selected table has its own recipe."""
    by_table: dict[str, list[dict[str, Any]]] = {}
    for step in steps:
        table = str(step.get("source_table") or "(unnamed)")
        by_table.setdefault(table, []).append(step)
    return [{"source_table": table, "steps": group} for table, group in by_table.items()]


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
        "interpretation": "Needs review",
        "action": "review",
        "plane": "review",
        "confidence": 0.0,
        "transform": "none",
        "code_crosswalk": None,
        "contract": None,
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
    source_tables: list[str] | None = None,
    source_catalog: dict[str, list[str]] | None = None,
    dest_tables: list[str] | None = None,
    dest_catalog: dict[str, list[str]] | None = None,
    lookup_pairs: dict[tuple[str, str], dict[str, str]] | None = None,
    named_catalog: dict[str, str] | None = None,
    source_types: dict[str, str] | None = None,
    dest_types: dict[str, str] | None = None,
    sheet_kind: str = "",
    extra_columns: list[str] | None = None,
) -> dict[str, Any]:
    spoken_src = str(raw.get("source_column") or "").strip()
    spoken_dst = str(raw.get("dest_column") or "").strip()
    spoken_name = str(raw.get("rule_name") or "").strip()
    validation_sheet = sheet_kind == "validation" or (
        _VALIDATION_ID.match(spoken_dst) and bool(parse_validate_check(str(raw.get("rule") or "")))
    )
    if validation_sheet and _VALIDATION_ID.match(spoken_dst):
        if not spoken_name:
            spoken_name = spoken_dst
        spoken_dst = ""
    table_from_cell, spoken_src_col = split_qualified(spoken_src)
    spoken_src = spoken_src_col or spoken_src
    dest_from_cell, spoken_dst_col = split_qualified(spoken_dst)
    spoken_dst = spoken_dst_col or spoken_dst
    spoken_rule = str(raw.get("rule") or "").strip()
    spoken_on_fail = str(raw.get("action") or "").strip()
    expanded, catalog_name, missing_name = _expand_named_rule(spoken_rule, named_catalog or {})
    rule_text = expanded
    classified = classify_rule(rule_text)
    kind = str(classified.get("kind") or "unknown")
    if not spoken_dst and classified.get("dest_hint"):
        spoken_dst = str(classified.get("dest_hint") or "").strip()
        dest_from_cell, spoken_dst_col = split_qualified(spoken_dst)
        spoken_dst = spoken_dst_col or spoken_dst

    catalog = source_catalog or {}
    selected = list(source_tables or [])
    effective_table, table_issue = resolve_source_table(
        cell_table=str(raw.get("source_table") or ""),
        qualified_table=table_from_cell,
        spoken_column=spoken_src,
        selected=selected,
        catalog=catalog,
        form_default=source_table,
    )
    bind_src_cols = bind_columns_for_table(effective_table, catalog, src_cols)
    dest_cat = dest_catalog or {}
    dest_selected = list(dest_tables or [])
    effective_dest, dest_table_issue = resolve_catalog_table(
        cell_table=str(raw.get("dest_table") or ""),
        qualified_table=dest_from_cell,
        spoken_column=spoken_dst,
        selected=dest_selected,
        catalog=dest_cat,
        form_default=dest_table,
        noun="destination",
    )
    bind_dst_cols = bind_columns_for_table(effective_dest, dest_cat, dst_cols)

    src_method = dst_method = ""
    src_score = dst_score = 0.0
    if bind_src_cols:
        source_column, src_method, src_score = resolve_name_ex(spoken_src, bind_src_cols)
    elif spoken_src and (catalog or selected):
        source_column = ""
        src_method = ""
        src_score = 0.0
    elif src_cols:
        source_column, src_method, src_score = resolve_name_ex(spoken_src, src_cols)
    else:
        source_column = spoken_src
        src_method = "spoken" if spoken_src else ""
        src_score = 1.0 if spoken_src else 0.0
    if bind_dst_cols:
        dest_column, dst_method, dst_score = resolve_name_ex(spoken_dst, bind_dst_cols)
    elif spoken_dst and (dest_cat or dest_selected):
        dest_column = ""
        dst_method = ""
        dst_score = 0.0
    elif dst_cols:
        dest_column, dst_method, dst_score = resolve_name_ex(spoken_dst, dst_cols)
    else:
        dest_column = spoken_dst
        dst_method = "spoken" if spoken_dst else ""
        dst_score = 1.0 if spoken_dst else 0.0
    if not dest_column and spoken_dst and not bind_dst_cols and not dst_cols:
        dest_column = spoken_dst
    if not source_column and spoken_src and not src_cols:
        source_column = spoken_src

    extra = [c for c in (extra_columns or []) if c]
    validation_bound = ""
    if validation_sheet:
        spoken_col = spoken_src or spoken_dst
        dest_pool = bind_dst_cols or dst_cols
        src_pool = bind_src_cols or src_cols
        dest_hit = resolve_name(spoken_col, dest_pool) if spoken_col and dest_pool else ""
        extra_hit = resolve_name(spoken_col, extra) if spoken_col and extra else ""
        src_hit = source_column or (
            resolve_name(spoken_col, src_pool) if spoken_col and src_pool else ""
        )
        if dest_hit:
            dest_column = dest_hit
            dst_method = dst_method or "schema"
            dst_score = max(dst_score, 0.95)
            validation_bound = dest_hit
        elif extra_hit:
            dest_column = extra_hit
            dst_method = "derived"
            dst_score = 0.9
            validation_bound = extra_hit
        elif src_hit:
            source_column = src_hit
            src_method = src_method or "schema"
            src_score = max(src_score, 0.9)
            validation_bound = src_hit
        if src_hit:
            source_column = src_hit

    issues: list[str] = []
    if table_issue:
        issues.append(table_issue)
    if dest_table_issue:
        issues.append(dest_table_issue)
    if spoken_src and (bind_src_cols or src_cols or catalog or selected) and not source_column:
        if not table_issue and not (validation_sheet and validation_bound):
            issues.append(
                f"Source column “{spoken_src}” is not on the selected source"
                + (f" table {effective_table}" if effective_table else "")
                + "."
            )
    if spoken_dst and (bind_dst_cols or dst_cols or dest_cat or dest_selected) and not dest_column:
        if not dest_table_issue and not (validation_sheet and validation_bound):
            issues.append(
                f"Destination column “{spoken_dst}” is not on the selected destination"
                + (f" table {effective_dest}" if effective_dest else "")
                + "."
            )
    if not spoken_src and kind not in _NO_SOURCE_OK and not (validation_sheet and validation_bound):
        issues.append("No source column was named.")
    if not spoken_dst and kind not in _NO_DEST_OK and not validation_sheet:
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
    dest_type = lookup_type(dest_column or spoken_dst, effective_dest, dest_types)
    source_type = lookup_type(source_column or spoken_src, effective_table, source_types)
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
        if validation_sheet and key == "dest_column":
            continue
        if method and method not in GROUNDED_METHODS:
            weak_bind = True
            issues.append(
                f"{label} header was inferred by {method} without a schema "
                "match — confirm the binding."
            )

    structured_contract = bool(classified.get("contract")) and kind == "contract"
    review_kinds = {"unknown", "join"}
    if kind == "contract" and not structured_contract:
        review_kinds = {"unknown", "join", "contract"}
    bind_fail = any(
        i.startswith("Source column")
        or i.startswith("Destination column")
        or i.startswith("No source")
        or i.startswith("No destination")
        or i.startswith("Concat ")
        for i in issues
    ) or weak_bind or bool(table_issue) or bool(dest_table_issue)
    low_confidence_reason = bool(
        classified.get("reason")
        and float(classified.get("confidence") or 0) < 0.9
        and not structured_contract
    )
    unnamed_zone = kind == "timezone" and not classified.get("zone")
    status = "executable"
    if kind in review_kinds or bind_fail or low_confidence_reason or type_issue or prec_issue or ident_issue or unnamed_zone:
        status = "needs_confirmation"

    transform = _KIND_TO_TRANSFORM.get(kind, "none")
    shape_step: dict[str, Any] | None = None
    extra_steps: list[dict[str, Any]] = []
    map_source = source_column
    pairs = _pairs_for_edge(
        lookup_pairs, spoken_src, spoken_dst, source_column, dest_column,
    )
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
                "format": workbook_mask_to_strptime(str(classified.get("format") or "")),
                "output_format": workbook_mask_to_strptime(
                    str(classified.get("output_format") or "YYYY-MM-DD")
                ),
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
                "fill": classified.get("fill") if classified.get("fill") not in (None, "") else " ",
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

    state_name_prose = "fifty-state" in str(classified.get("reason") or "").lower() or (
        "lookup sheet of pairs" in str(classified.get("reason") or "").lower()
    )
    if pairs and not validation_sheet and (
        kind in {"direct", "lookup", ""}
        or (
            kind == "unknown"
            and (
                str(classified.get("reason") or "").startswith("lookup named")
                or state_name_prose
            )
        )
    ):
        kind = "lookup"
        transform = "none"
        issues = [
            item for item in issues
            if "lookup named" not in item
            and "fifty-state" not in item.lower()
            and "lookup sheet of pairs" not in item.lower()
            and "not a closed form" not in item.lower()
        ]
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

    if kind == "contract":
        shape_step = None
        extra_steps = []
        transform = "none"
        map_source = dest_column or source_column or spoken_src
        contract_ir = classified.get("contract") if isinstance(classified.get("contract"), dict) else None
        if contract_ir and str(contract_ir.get("type") or "") == "in_lookup":
            values = _unique_lookup_values(
                _pairs_for_contract(
                    lookup_pairs, spoken_src, spoken_dst, source_column, dest_column,
                ) or pairs
            )
            if values:
                classified["contract"] = {"type": "in_set", "values": values}
                classified["interpretation"] = "Exist in named lookup"
                issues = [
                    item for item in issues
                    if "exist-in-lookup" not in item.lower()
                    and "fifty-state" not in item.lower()
                ]
                if (
                    status == "needs_confirmation"
                    and validation_bound
                    and not bind_fail
                    and not weak_bind
                    and not type_issue
                    and not prec_issue
                    and not ident_issue
                ):
                    status = "executable"
            else:
                issues.append(
                    "Exist-in-lookup needs a lookup sheet of pairs. "
                    "The fifty-state table was not invented."
                )
                status = "needs_confirmation"
        if structured_contract and validation_bound and status == "executable":
            issues = [
                item for item in issues
                if not item.startswith("Marked required")
                and not item.startswith("Marked unique")
            ]

    plane = "review"
    if status == "executable":
        plane = "validate" if kind == "contract" else (classified.get("plane") or "map")
    confidence = float(classified.get("confidence") or 0)
    if kind == "lookup" and pairs:
        confidence = max(confidence, 0.99)
    if kind == "direct":
        confidence = max(confidence, 0.99)

    return {
        "source_table": effective_table or str(raw.get("source_table") or table_from_cell or source_table or ""),
        "source_column": source_column or spoken_src,
        "map_source": map_source or source_column or spoken_src,
        "dest_table": effective_dest or str(raw.get("dest_table") or dest_from_cell or dest_table or ""),
        "dest_column": dest_column or spoken_dst,
        "rule_text": spoken_rule if catalog_name else rule_text,
        "named_rule": catalog_name or spoken_name or "",
        "resolved_rule": rule_text if catalog_name else "",
        "unknown_code_policy": (
            policy if (kind == "lookup" or pairs) else {"action": "refuse", "value": ""}
        ),
        "kind": kind,
        "kind_label": _KIND_LABEL.get(kind, kind),
        "interpretation": _interpretation_for(kind, classified),
        "action": _action_for(status),
        "plane": plane,
        "confidence": confidence,
        "transform": transform,
        "engine_transform": (
            f"assume_timezone:{classified.get('zone')}"
            if kind == "timezone" and classified.get("zone")
            else ""
        ),
        "timezone": classified.get("zone") or "",
        "code_crosswalk": None if kind == "contract" else (pairs or None),
        "contract": classified.get("contract") if kind == "contract" else None,
        "on_fail": _normalize_on_fail(spoken_on_fail),
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
        "interpretation": "Join (review)",
        "action": "review",
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
