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

Free text that quotes cells is cell data too (review C1): a job's ``error`` /
``message`` (a driver error prints the row it choked on), validation blockers,
and the value lines of earlier answers ("Error: …", list rows). It is withheld
from every channel above and from the system prompt.

With cells allowed nothing is withheld, and the cells are declared to the gate
so the egress manifest records that they left.
"""

from __future__ import annotations

import hashlib
import re
import threading
from collections import OrderedDict
from typing import Any

from services.ai_egress import (
    ROW_KEYS,
    WITHHELD,
    declare_cells,
    declare_free_text,
    declare_turn_cells,
    free_text_declared,
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
    "[withheld: metadata-only] Cell values are withheld from the model by the metadata-only policy. The operator "
    "sees them in the app; describe the result by its columns and counts only."
)

#: Tools whose output carries job / run free text (driver errors, blockers).
FREE_TEXT_TOOLS = frozenset({
    "get_job", "list_jobs", "brief_workspace", "explain_parity", "get_preflight_run",
    "list_schedules", "get_schedule", "open_job",
})
#: The keys that hold that free text inside those outputs.
FREE_TEXT_KEYS = frozenset({
    "error", "message", "error_message", "last_error", "last_message", "error_details",
    "blockers", "failure_reason",
})
#: A line of an earlier answer that states a value: "Error: …", "Reason: …".
_VALUE_LABEL = re.compile(
    r"^(\s*(?:[-*•–]\s*)?(?:\*\*)?(?:error|errors|reason|row|message|cause|detail|details|blocker[^:]{0,40})"
    r"(?:\*\*)?\s*:)\s*(\S.*)$",
    re.I,
)
#: A list item of an earlier answer: numbered rows and bullets name values.
_LIST_ITEM = re.compile(r"^\s*(?:\d+[.)]|[-*•–])\s+\S")
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


def free_text_values(output: Any) -> list[str]:
    """Every string a job / run output holds under a free-text key."""
    found: list[str] = []
    stack = [output]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            for key, value in node.items():
                if key in FREE_TEXT_KEYS:
                    found.extend(_strings(value))
                elif isinstance(value, (dict, list, tuple)):
                    stack.append(value)
        elif isinstance(node, (list, tuple)):
            stack.extend(node)
    return [text for text in found if text.strip()]


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [text for item in value.values() for text in _strings(item)]
    if isinstance(value, (list, tuple)):
        return [text for item in value for text in _strings(item)]
    return []


#: Scalar results of a cell tool: an aggregate's value, a profile's extremes.
_SCALAR_CELL_KEYS = frozenset({"value", "min", "max", "mode", "median", "most_common", "first", "last"})


def cell_tool_values(output: Any) -> list[Any]:
    """Every value a cell tool's output holds: scalars, rows and group keys."""
    if not isinstance(output, dict):
        return []
    found = [output[key] for key in _SCALAR_CELL_KEYS if output.get(key) is not None]
    return found + _row_values(output)


def note_tool_result(name: str, success: bool, output: Any) -> None:
    """Declare what a tool read that is cell data, when it runs (review 2, C1).

    Job / run free text (driver errors, blockers) and a cell tool's values —
    an aggregate scalar written into a sentence, group keys — are declared, so
    the gate withholds them under metadata-only and the manifest records them
    as included when cells are allowed.
    """
    if success and name in FREE_TEXT_TOOLS:
        declare_free_text(free_text_values(output))
    if success and name in CELL_TOOLS:
        declare_turn_cells(cell_tool_values(output))


# ---- what each answered turn read (review 2, C1) ------------------------------
#
# The transcript is the client's copy of earlier answers. A value an answer
# stated in a sentence ("Maximum of `email` …: **a@b.com**") matches no
# pattern, so whether a turn read cells is recorded when it is answered, keyed
# by a digest of the answer text, and the transcript is judged by that record.
# A turn with no record (another replica, a restart, an edited transcript) is
# treated as cell-bearing: under metadata-only it fails closed.

#: The router keeps this much of each history turn; the digest covers the same span.
_TURN_DIGEST_CHARS = 8000
_MAX_RECORDED_TURNS = 4096
_turns: "OrderedDict[str, bool]" = OrderedDict()
_turns_lock = threading.Lock()
_EARLIER_ANSWER_WITHHELD = (
    "[withheld: metadata-only] An earlier answer built from cell values; its text is not sent to the model."
)


def _turn_digest(text: str) -> str:
    body = str(text or "")[:_TURN_DIGEST_CHARS].strip()
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def turn_read_cells(tools_used: list[dict] | None) -> bool:
    """Whether the turn being answered read cell data: a cell tool, or job free text."""
    if free_text_declared():
        return True
    return any(str(tool.get("name") or "") in CELL_TOOLS for tool in tools_used or [])


def record_turn(answer: str, tools_used: list[dict] | None) -> bool:
    """Record whether this answer was built from cell data. Returns that verdict."""
    if not str(answer or "").strip():
        return False
    cells = turn_read_cells(tools_used)
    digest = _turn_digest(answer)
    with _turns_lock:
        # Once cell-bearing, always: the same text answered twice is still those cells.
        _turns[digest] = cells or _turns.get(digest, False)
        _turns.move_to_end(digest)
        while len(_turns) > _MAX_RECORDED_TURNS:
            _turns.popitem(last=False)
    return cells


def recorded_cells(text: str) -> bool | None:
    """True / False as recorded for an earlier answer; ``None`` when never recorded."""
    with _turns_lock:
        return _turns.get(_turn_digest(text))


def forget_recorded_turns() -> None:
    """Tests only: drop every turn record."""
    with _turns_lock:
        _turns.clear()


def _value_lines(text: str) -> list[str]:
    """The values stated on "Error: …"-style lines of ``text``."""
    out = []
    for line in (text or "").splitlines():
        found = _VALUE_LABEL.match(line)
        if found:
            out.append(found.group(2).strip())
    return out


def narration_allowed(answer: str, tools_used: list[dict] | None) -> bool:
    """Whether this turn's draft may be sent to a narrator.

    Under metadata-only a draft built from cell data is not narrated at all: a
    rewrite cannot keep the rows without the rows being sent. A draft that
    states free text (a job's error, a blocker) is cell data by the same rule.
    """
    if not metadata_only_enabled():
        declare_cells(rendered_cells(answer))
        declare_cells(_value_lines(answer))
        return True
    used_cells = any(
        tool.get("success") and str(tool.get("name") or "") in CELL_TOOLS
        for tool in tools_used or []
    )
    return (
        not used_cells
        and not rendered_cells(answer)
        and not _value_lines(answer)
        and not free_text_declared()
    )


def _withhold_value_lines(text: str) -> str:
    """An earlier answer with its "Error: …" lines and list items withheld."""
    out: list[str] = []
    for line in (text or "").splitlines():
        label = _VALUE_LABEL.match(line)
        if label:
            out.append(f"{label.group(1)} {_ROWS_WITHHELD}")
        elif _LIST_ITEM.match(line):
            if not out or out[-1] != _ROWS_WITHHELD:
                out.append(_ROWS_WITHHELD)
        else:
            out.append(line)
    return "\n".join(out)


def _list_items(text: str) -> list[str]:
    return [line for line in (text or "").splitlines() if _LIST_ITEM.match(line)]


def _turn_body(turn: dict) -> tuple[str, str]:
    """The key a transcript turn keeps its text under, and the text."""
    for key in ("content", "text"):
        if isinstance(turn.get(key), str) and turn[key].strip():
            return key, turn[key]
    return "content", ""


def history_for_model(history: list[dict] | None) -> list[dict]:
    """The transcript a model may be sent.

    Under metadata-only an earlier answer that read cells — or whose record
    this server does not hold — is withheld whole; every other turn has its
    rendered rows and value lines withheld. With cells allowed, a recorded
    cell-bearing answer is declared, so the manifest says its cells left.
    """
    turns = [dict(turn) for turn in (history or []) if isinstance(turn, dict)]
    if not metadata_only_enabled():
        for turn in turns:
            _key, content = _turn_body(turn)
            declare_cells(rendered_cells(content))
            if turn.get("role") == "assistant":
                declare_cells(_value_lines(content) + _list_items(content))
                if recorded_cells(content) is not False:
                    declare_cells([line for line in content.splitlines() if line.strip()])
        return turns
    for turn in turns:
        key, content = _turn_body(turn)
        if not content:
            continue
        if turn.get("role") == "assistant" and recorded_cells(content) is not False:
            turn[key] = _EARLIER_ANSWER_WITHHELD
            continue
        content = withhold_rendered_rows(content)
        # Pilot's own earlier answers state values outside tables too.
        if turn.get("role") == "assistant":
            content = _withhold_value_lines(content)
        turn[key] = content
    return turns


def blockers_for_model(blockers: list[Any] | None) -> list[str]:
    """Validation blockers as a model may see them: they quote the failing cells."""
    texts = [str(b) for b in (blockers or []) if str(b or "").strip()]
    if not metadata_only_enabled():
        declare_cells(texts)
        return texts
    return [WITHHELD for _ in texts]


def tool_results_for_model(results: list[Any]) -> list[Any]:
    """Copies of tool results with each payload reduced to what a model may see."""
    from dataclasses import replace

    return [replace(r, output=model_payload(r.name, r.output)) if r.success else r for r in results]


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
        if tool_name in FREE_TEXT_TOOLS:
            declare_cells(free_text_values(output))
        return output
    if tool_name in CELL_TOOLS and isinstance(output, dict):
        shown = {key: output[key] for key in _METADATA_KEYS if key in output}
        shown["withheld"] = _WITHHELD_NOTE
        return shown
    if tool_name in FREE_TEXT_TOOLS:
        output = _withhold_free_text(output)
    cleaned, withheld = withhold_row_values(output)
    if withheld and isinstance(cleaned, dict):
        cleaned["withheld"] = _WITHHELD_NOTE
    return cleaned


def _withhold_free_text(node: Any) -> Any:
    """``node`` with every non-empty free-text value replaced by the marker."""
    if isinstance(node, dict):
        return {
            key: (
                WITHHELD
                if key in FREE_TEXT_KEYS and any(text.strip() for text in _strings(value))
                else _withhold_free_text(value)
            )
            for key, value in node.items()
        }
    if isinstance(node, (list, tuple)):
        return [_withhold_free_text(item) for item in node]
    return node
