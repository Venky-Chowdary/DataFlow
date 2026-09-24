"""Apply a compiled rule report onto live source rows.

This is not a fourth runtime. Closed-form artifacts already execute on:

* ``ShapeEngine`` — pre-load recipe (parse, derive, default, concat, filter)
* ``apply_transform`` — Map write transform
* ``apply_code_crosswalk`` — Gate G20 lookup (never silent identity)

Review, conflict, omit, and join never write. Unnamed catalog columns never
appear. Several selected tables are N independent projections — this module
will not invent a join grain or a ``source_query``.

100% here means: every accepted compiled edge is applied exactly as compiled,
and nothing else is written.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from services.code_crosswalk import apply_code_crosswalk
from services.shape_apply import build_shape_runner
from services.shape_engine import ShapeRowError
from services.transform_engine import apply_transform

from .normalize import fold

_SKIP_KINDS = frozenset({"omit", "join", "unknown", "contract"})
_MISSING = object()
_ENGINE_META = frozenset({"source_table", "dest_table"})


def engine_step(step: Mapping[str, Any]) -> dict[str, Any]:
    """ShapeEngine.parse ignores unknown keys; strip table stamps anyway."""
    return {key: value for key, value in step.items() if key not in _ENGINE_META}


def _table_matches(named: str, want: str) -> bool:
    return bool(want) and fold(named) == fold(want)


def _selected_tables(report: Mapping[str, Any]) -> list[str]:
    return [str(name) for name in (report.get("source_tables") or []) if str(name).strip()]


def shape_steps_for_table(report: Mapping[str, Any], source_table: str) -> list[dict[str, Any]]:
    """Steps bound to this table. Untagged steps do not apply among many tables."""
    multi = len(_selected_tables(report)) > 1
    out: list[dict[str, Any]] = []
    for step in report.get("shape_steps") or []:
        if not isinstance(step, Mapping):
            continue
        tagged = str(step.get("source_table") or "")
        if tagged:
            if _table_matches(tagged, source_table):
                out.append(dict(step))
            continue
        if not multi:
            out.append(dict(step))
    return out


def executable_edges_for_table(
    report: Mapping[str, Any],
    source_table: str,
) -> list[dict[str, Any]]:
    """Named executable Map edges for one source table. Review stays out."""
    multi = len(_selected_tables(report)) > 1
    edges: list[dict[str, Any]] = []
    for item in report.get("rules") or []:
        if not isinstance(item, Mapping):
            continue
        if item.get("status") != "executable":
            continue
        if item.get("kind") in _SKIP_KINDS:
            continue
        dest = str(item.get("dest_column") or "").strip()
        if not dest:
            continue
        table = str(item.get("source_table") or "")
        if table and not _table_matches(table, source_table):
            continue
        if not table and multi:
            continue
        edges.append(dict(item))
    return edges


def executable_contracts_for_table(
    report: Mapping[str, Any],
    source_table: str,
) -> list[dict[str, Any]]:
    """Closed-form destination contracts. They never write a dest column."""
    multi = len(_selected_tables(report)) > 1
    out: list[dict[str, Any]] = []
    for item in report.get("rules") or []:
        if not isinstance(item, Mapping):
            continue
        if item.get("status") != "executable" or item.get("kind") != "contract":
            continue
        if not isinstance(item.get("contract"), Mapping):
            continue
        table = str(item.get("source_table") or "")
        if table and source_table and not _table_matches(table, source_table):
            continue
        if not table and multi:
            continue
        out.append(dict(item))
    return out


def evaluate_contract(value: Any, contract: Mapping[str, Any], *, missing: bool) -> str | None:
    """Deterministic check. None means pass. A string is the quarantine reason."""
    kind = str(contract.get("type") or "")
    if kind == "not_null":
        if missing or value is None or (isinstance(value, str) and not str(value).strip()):
            return "must not be null"
        return None
    if missing:
        return "column is not in the dest image"
    if kind == "contains":
        needle = str(contract.get("value") or "")
        if needle and needle not in str(value):
            return f"must contain {needle}"
        return None
    if kind == "in_set":
        allowed = [str(item) for item in (contract.get("values") or [])]
        if allowed and str(value) not in allowed:
            return f"must be one of {', '.join(allowed)}"
        return None
    if kind == "compare":
        try:
            number = float(value)
            bound = float(contract.get("value"))
        except (TypeError, ValueError):
            return "must be numeric"
        op = str(contract.get("op") or "")
        ok = (
            (op == ">" and number > bound)
            or (op == ">=" and number >= bound)
            or (op == "<" and number < bound)
            or (op == "<=" and number <= bound)
        )
        if not ok:
            return f"must be {op} {contract.get('value')}"
        return None
    if kind == "pattern":
        pattern = str(contract.get("pattern") or "")
        if not pattern:
            return "contract pattern is missing"
        try:
            compiled = re.compile(pattern)
        except re.error:
            return "contract pattern is not executable"
        if compiled.fullmatch(str(value) or "") is None:
            return "must match the compiled pattern"
        return None
    if kind == "unique":
        return None
    if kind == "in_lookup":
        return "exist-in-lookup has no named pairs"
    return "contract type is not executable"


def _contract_value(
    image: Mapping[str, Any],
    shaped: Mapping[str, Any],
    edges: Sequence[Mapping[str, Any]],
    rule: Mapping[str, Any],
) -> tuple[Any, bool]:
    dest = str(rule.get("dest_column") or "").strip()
    src = str(rule.get("source_column") or "").strip()
    if dest and dest in image:
        return image.get(dest), False
    if src and src in image:
        return image.get(src), False
    for edge in edges:
        edge_src = str(edge.get("source_column") or "")
        edge_dest = str(edge.get("dest_column") or "")
        if src and fold(edge_src) == fold(src) and edge_dest in image:
            return image.get(edge_dest), False
        if dest and fold(edge_dest) == fold(dest) and edge_dest in image:
            return image.get(edge_dest), False
    if dest and dest in shaped:
        return shaped.get(dest), False
    if src and src in shaped:
        return shaped.get(src), False
    return _MISSING, True


def _apply_edge(raw: Any, edge: Mapping[str, Any]) -> tuple[Any, str | None]:
    crosswalk = edge.get("code_crosswalk")
    if isinstance(crosswalk, Mapping) and crosswalk:
        return apply_code_crosswalk(
            raw,
            {
                "source": edge.get("source_column"),
                "target": edge.get("dest_column"),
                "code_crosswalk": crosswalk,
            },
        )
    transform = str(edge.get("engine_transform") or edge.get("transform") or "none")
    if transform and transform not in {"none", ""}:
        text = None if raw is None else (raw if type(raw) is str else str(raw))
        return apply_transform(text, transform)
    return raw, None


def _read_shaped(shaped: Mapping[str, Any], edge: Mapping[str, Any]) -> Any:
    src = str(edge.get("map_source") or edge.get("source_column") or "")
    dest = str(edge.get("dest_column") or "")
    if src and src in shaped:
        return shaped.get(src)
    if dest and dest in shaped:
        return shaped.get(dest)
    if src:
        return shaped.get(src)
    return None


def _project_row(
    shaped: Mapping[str, Any],
    edges: Sequence[Mapping[str, Any]],
    *,
    row_index: int,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    """One dest image, or quarantine. A single edge error refuses the row."""
    image: dict[str, Any] = {}
    errors: list[dict[str, Any]] = []
    for edge in edges:
        dest = str(edge.get("dest_column") or "").strip()
        if not dest:
            continue
        raw = _read_shaped(shaped, edge)
        value, err = _apply_edge(raw, edge)
        if err:
            errors.append({
                "row_index": row_index,
                "column": dest,
                "source_column": str(edge.get("map_source") or edge.get("source_column") or ""),
                "error": err,
            })
            continue
        image[dest] = value
    if errors:
        return None, errors
    return image, []


def _shape_population(
    rows: Sequence[Mapping[str, Any]],
    steps: Sequence[Mapping[str, Any]],
    source_columns: Sequence[str] | None,
) -> tuple[list[tuple[int, dict[str, Any]]], list[dict[str, Any]]]:
    """Shape each source row independently so one refuse does not drop the batch."""
    if not steps:
        return [(index, dict(row)) for index, row in enumerate(rows)], []
    columns = list(source_columns or [])
    if not columns and rows:
        columns = [str(key) for key in rows[0].keys()]
    runner = build_shape_runner(
        {"steps": [engine_step(step) for step in steps]},
        source_columns=columns or None,
    )
    if runner is None:
        return [(index, dict(row)) for index, row in enumerate(rows)], []
    kept: list[tuple[int, dict[str, Any]]] = []
    quarantine: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        try:
            produced = runner.records([dict(row)])
        except ShapeRowError as exc:
            quarantine.append({
                "row_index": index,
                "column": getattr(exc, "column", "") or "",
                "source_column": getattr(exc, "column", "") or "",
                "error": str(exc),
                "plane": "shape",
            })
            continue
        for shaped in produced:
            kept.append((index, shaped))
    return kept, quarantine


def apply_compiled_projection(
    report: Mapping[str, Any],
    *,
    source_table: str,
    rows: Sequence[Mapping[str, Any]],
    source_columns: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Project live rows of one source table onto named dest images.

    Several dest tables from the same source stay separate images. A failed
    edge quarantines that source row for that dest — never a partial write.
    """
    table = (source_table or "").strip()
    edges = executable_edges_for_table(report, table)
    contracts = executable_contracts_for_table(report, table)
    steps = shape_steps_for_table(report, table)
    shaped_rows, shape_quarantine = _shape_population(rows, steps, source_columns)

    by_dest: dict[str, list[dict[str, Any]]] = {}
    for edge in edges:
        by_dest.setdefault(str(edge.get("dest_table") or ""), []).append(edge)

    destinations: list[dict[str, Any]] = []
    if not by_dest:
        destinations.append({
            "dest_table": "",
            "columns": [],
            "rows": [],
            "quarantine": list(shape_quarantine),
            "written": 0,
            "refused": len(shape_quarantine),
        })
    else:
        for dest_table, dest_edges in by_dest.items():
            pending: list[tuple[int, dict[str, Any]]] = []
            dest_quarantine = [dict(item, dest_table=dest_table) for item in shape_quarantine]
            columns = []
            seen_cols: set[str] = set()
            for edge in dest_edges:
                col = str(edge.get("dest_column") or "")
                key = fold(col)
                if col and key not in seen_cols:
                    seen_cols.add(key)
                    columns.append(col)
            for row_index, shaped in shaped_rows:
                image, errors = _project_row(shaped, dest_edges, row_index=row_index)
                if errors:
                    dest_quarantine.extend({**item, "dest_table": dest_table, "plane": "map"} for item in errors)
                    continue
                if image is None:
                    continue
                contract_errors: list[dict[str, Any]] = []
                for rule in contracts:
                    if str((rule.get("contract") or {}).get("type") or "") == "unique":
                        continue
                    value, missing = _contract_value(image, shaped, dest_edges, rule)
                    err = evaluate_contract(
                        None if value is _MISSING else value,
                        rule.get("contract") or {},
                        missing=missing,
                    )
                    if err:
                        contract_errors.append({
                            "row_index": row_index,
                            "column": str(rule.get("dest_column") or rule.get("source_column") or ""),
                            "source_column": str(rule.get("source_column") or ""),
                            "error": err,
                            "dest_table": dest_table,
                            "plane": "validate",
                        })
                if contract_errors:
                    dest_quarantine.extend(contract_errors)
                    continue
                pending.append((row_index, image))
            dest_quarantine.extend(_unique_failures(pending, contracts, dest_table))
            unique_rows = {
                int(item["row_index"])
                for item in dest_quarantine
                if item.get("error") == "must be unique"
            }
            dest_rows = [image for row_index, image in pending if row_index not in unique_rows]
            destinations.append({
                "dest_table": dest_table,
                "columns": columns,
                "rows": dest_rows,
                "quarantine": dest_quarantine,
                "written": len(dest_rows),
                "refused": len(dest_quarantine),
            })

    written = sum(int(item["written"]) for item in destinations)
    refused = sum(int(item["refused"]) for item in destinations)
    return {
        "source_table": table,
        "source_rows": len(rows),
        "shaped_rows": len(shaped_rows),
        "shape_steps": len(steps),
        "edges": len(edges),
        "destinations": destinations,
        "written": written,
        "refused": refused,
        "honesty": (
            "Named executable columns only. Review, join, omit, contract, and "
            "unnamed catalog columns were not written. Destination contracts "
            "quarantine after Map. Unique is a population check — duplicates "
            "are quarantined together, never silently kept. One refused edge "
            "quarantines that dest image — never a partial row."
        ),
    }


def _unique_failures(
    pending: Sequence[tuple[int, Mapping[str, Any]]],
    contracts: Sequence[Mapping[str, Any]],
    dest_table: str,
) -> list[dict[str, Any]]:
    """Population unique: every row that shares a key is quarantined."""
    out: list[dict[str, Any]] = []
    for rule in contracts:
        if str((rule.get("contract") or {}).get("type") or "") != "unique":
            continue
        dest = str(rule.get("dest_column") or "").strip()
        src = str(rule.get("source_column") or "").strip()
        col = dest or src
        if not col:
            continue
        seen: dict[str, list[int]] = {}
        for row_index, image in pending:
            if col not in image:
                continue
            key = fold(str(image.get(col) if image.get(col) is not None else ""))
            if not key:
                continue
            seen.setdefault(key, []).append(int(row_index))
        for rows in seen.values():
            if len(rows) < 2:
                continue
            for row_index in rows:
                out.append({
                    "row_index": row_index,
                    "column": col,
                    "source_column": src,
                    "error": "must be unique",
                    "dest_table": dest_table,
                    "plane": "validate",
                })
    return out


def apply_selected_tables(
    report: Mapping[str, Any],
    populations: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    """Apply each selected table independently. Missing tables are named, not joined."""
    tables = [
        apply_compiled_projection(report, source_table=name, rows=list(rows))
        for name, rows in populations.items()
    ]
    named = {fold(name) for name in populations}
    missing = [
        str(item.get("source_table") or "")
        for item in (report.get("projection") or [])
        if str(item.get("source_table") or "") and fold(item.get("source_table") or "") not in named
    ]
    return {
        "tables": tables,
        "missing_populations": [name for name in missing if name],
        "written": sum(int(item["written"]) for item in tables),
        "refused": sum(int(item["refused"]) for item in tables),
        "honesty": (
            "N independent projections — not one joined dest row. "
            "A table without a population is listed in missing_populations."
        ),
    }
