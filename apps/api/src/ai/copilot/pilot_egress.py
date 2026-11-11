"""Pilot's side of the AI egress gate (``services.ai_egress``).

Pilot shows the operator sampled rows; that is its job. What must not happen
under the metadata-only policy is those rows travelling on to a model — in the
draft a narrator rewrites, in the transcript sent as history, or in a tool
result serialised for a model's tool loop (audit C6). This module decides, for
each of those three channels, what a model may be handed:

* **narration** — a turn whose answer carries cell values is not narrated
  under metadata-only; the deterministic answer is returned as is;
* **history** — rows Pilot rendered in earlier answers are withheld from the
  transcript before it is sent;
* **tool results** — a row-returning tool's payload is reduced to its
  metadata (object, columns, counts).

With cells allowed nothing is withheld, and the cells are declared to the gate
so the egress manifest records that they left.
"""

from __future__ import annotations

import re
from typing import Any

from services.ai_egress import (
    ROW_KEYS,
    declare_cells,
    metadata_only_enabled,
    withhold_row_values,
)

#: Tools whose result *is* cell data (rows, per-value statistics, group keys).
CELL_TOOLS = frozenset({
    "sample_connector_object", "run_query", "filter_result", "analyze_result",
    "aggregate_data", "analyze_dataset", "search_data", "compare_datasets",
    "profile_quality_rules",
})
#: What a model may still see of such a result: where it came from and its shape.
_METADATA_KEYS = (
    "result_id", "parent_result_id", "connector_name", "connector_id", "connector_type",
    "table", "object", "dataset", "columns", "column_count", "row_count", "match_count",
    "source_row_count", "truncated", "dialect", "count", "total",
)
_ROWS_WITHHELD = "[rows withheld: metadata-only]"
#: Shorter cells ("1", "ok") are not chased through prose: they would match everywhere.
_MIN_QUOTED_CELL = 4
_WITHHELD_NOTE = (
    "Cell values are withheld from the model by the metadata-only policy. The operator "
    "sees them in the app; describe the result by its columns and counts only."
)

_TABLE_ROW = re.compile(r"^\s*\|.*\|\s*$")
_TABLE_RULE = re.compile(r"^\s*\|(?:\s*:?-{3,}:?\s*\|)+\s*$")
_SAMPLE_LINE = re.compile(r"^(\s*[•*-]?\s*Sample row:)\s*(.+)$", re.I)
_TOP_VALUE = re.compile(r"`([^`]+)`×\d+")


def _table_cells(line: str) -> list[str]:
    return [cell.strip().strip("`") for cell in line.strip().strip("|").split("|")]


def _row_spans(lines: list[str]) -> list[int]:
    """Indexes of markdown-table *data* rows (the header and rule are structure)."""
    out: list[int] = []
    for i, line in enumerate(lines):
        if _TABLE_RULE.match(line):
            j = i + 1
            while j < len(lines) and _TABLE_ROW.match(lines[j]):
                out.append(j)
                j += 1
    return out


def rendered_cells(text: str) -> list[str]:
    """Cell values Pilot rendered into an answer: table rows, sample rows, top values."""
    lines = (text or "").splitlines()
    cells: list[str] = []
    for index in _row_spans(lines):
        cells.extend(_table_cells(lines[index]))
    for line in lines:
        sample = _SAMPLE_LINE.match(line)
        if sample:
            cells.extend(part.split("=", 1)[-1].strip() for part in sample.group(2).split(","))
        cells.extend(_TOP_VALUE.findall(line))
    return [c for c in cells if c]


def withhold_rendered_rows(text: str) -> str:
    """``text`` with rendered rows replaced by a marker; prose and headers stay."""
    lines = (text or "").splitlines()
    rows = set(_row_spans(lines))
    out: list[str] = []
    for i, line in enumerate(lines):
        if i in rows:
            if i - 1 not in rows:
                out.append(_ROWS_WITHHELD)
            continue
        sample = _SAMPLE_LINE.match(line)
        if sample:
            out.append(f"{sample.group(1)} {_ROWS_WITHHELD}")
            continue
        out.append(_TOP_VALUE.sub(_ROWS_WITHHELD, line))
    cleaned = "\n".join(out)
    # The lead sentence can quote a cell too ("… including 1, a@b.com"): any
    # value that stood in a withheld row is withheld wherever else it appears.
    for cell in sorted({c for c in rendered_cells(text) if len(c) >= _MIN_QUOTED_CELL}, key=len, reverse=True):
        cleaned = cleaned.replace(cell, _ROWS_WITHHELD)
    return cleaned


def narration_allowed(answer: str, tools_used: list[dict] | None) -> bool:
    """Whether this turn's draft may be sent to a narrator.

    Under metadata-only a draft built from cell data is not narrated at all: a
    rewrite cannot keep the rows without the rows being sent.
    """
    if not metadata_only_enabled():
        declare_cells(rendered_cells(answer))
        return True
    used_cells = any(
        tool.get("success") and str(tool.get("name") or "") in CELL_TOOLS
        for tool in tools_used or []
    )
    return not used_cells and not rendered_cells(answer)


def history_for_model(history: list[dict] | None) -> list[dict]:
    """The transcript a model may be sent: rendered rows withheld under metadata-only."""
    turns = [dict(turn) for turn in (history or []) if isinstance(turn, dict)]
    if not metadata_only_enabled():
        for turn in turns:
            declare_cells(rendered_cells(str(turn.get("content") or "")))
        return turns
    for turn in turns:
        if isinstance(turn.get("content"), str):
            turn["content"] = withhold_rendered_rows(turn["content"])
    return turns


def _row_values(payload: Any) -> list[Any]:
    found: list[Any] = []
    stack = [payload]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                (found if key in ROW_KEYS else stack).append(value)
        elif isinstance(node, (list, tuple)):
            stack.extend(node)
    return found


def model_payload(tool_name: str, output: Any) -> Any:
    """What a model's tool loop is handed for one successful tool result."""
    if not metadata_only_enabled():
        declare_cells(_row_values(output))
        return output
    if tool_name in CELL_TOOLS and isinstance(output, dict):
        shown = {key: output[key] for key in _METADATA_KEYS if key in output}
        shown["withheld"] = _WITHHELD_NOTE
        return shown
    cleaned, withheld = withhold_row_values(output)
    if withheld and isinstance(cleaned, dict):
        cleaned["withheld"] = _WITHHELD_NOTE
    return cleaned
