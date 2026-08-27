"""How a shaping recipe *runs* — pure, streaming, and self-accounting.

The engine is deliberately free of I/O: it takes rows and returns rows plus a
`ShapeEffect` describing exactly what it did — rows in, rows out, rows removed,
rows diverted, cells changed and nulls introduced, per step. That accounting is
the point. Every competitor can apply a transformation; the reason this one
records its own effect is that the ledger downstream must still balance:

    rows_read = rows_shaped_out + dest_count + held_out + skipped

A row a `filter_rows` step removed is a deliberate exclusion, not a data-quality
finding, so it is counted separately from quarantine and never reported as one.

Because every step is row-local, one row can be pushed through the whole recipe
independently of its neighbours. That is what makes the same recipe safe in a
preview of 50 rows, in Validate's population scan and in a chunked 1M-row
Execute — and it is why an instance can be reused across batches while the
counters keep accumulating.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from decimal import Decimal, DecimalException, InvalidOperation
from typing import Any, Iterable, Mapping, Sequence

from services.shape_expr import (
    FUNCTIONS,
    EvalError,
    _as_text,
    is_blank,
)
from services.shape_models import ShapeRecipe, ShapeStep

__all__ = [
    "ShapeRowError",
    "StepEffect",
    "ShapeEffect",
    "DivertedRow",
    "ShapeEngine",
    "shape_records",
]

# Samples are for the operator's eyes, not for accounting; the counts are exact
# regardless of how many samples were kept.
_DEFAULT_SAMPLES = 25


class ShapeRowError(ValueError):
    """A row a step could not shape under the ``refuse`` policy."""

    def __init__(self, message: str, *, step: ShapeStep, step_index: int, row_index: int, column: str):
        super().__init__(message)
        self.step = step
        self.step_index = step_index
        self.row_index = row_index
        self.column = column

    def as_dict(self) -> dict[str, Any]:
        return {
            "step": self.step_index + 1,
            "op": self.step.op,
            "column": self.column,
            "row": self.row_index + 1,
            "message": str(self),
        }


@dataclass
class StepEffect:
    """What one step did, across every batch this engine has seen."""

    index: int
    op: str
    label: str
    active: bool
    rows_in: int = 0
    rows_out: int = 0
    rows_removed: int = 0
    rows_diverted: int = 0
    cells_changed: int = 0
    nulls_introduced: int = 0
    errors: int = 0
    error_samples: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.index + 1,
            "op": self.op,
            "label": self.label,
            "active": self.active,
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_removed": self.rows_removed,
            "rows_diverted": self.rows_diverted,
            "cells_changed": self.cells_changed,
            "nulls_introduced": self.nulls_introduced,
            "errors": self.errors,
            "error_samples": list(self.error_samples),
        }


@dataclass
class DivertedRow:
    """A row a step sent to quarantine, with the reason stated."""

    row_index: int
    reason: str
    step: int
    op: str
    record: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "row": self.row_index + 1,
            "reason": self.reason,
            "step": self.step + 1,
            "op": self.op,
            "record": dict(self.record),
        }


@dataclass
class ShapeEffect:
    """The recipe's total effect — the numbers the ledger and the UI both use."""

    rows_in: int = 0
    rows_out: int = 0
    rows_shaped_out: int = 0
    rows_diverted: int = 0
    cells_changed: int = 0
    nulls_introduced: int = 0
    steps: list[StepEffect] = field(default_factory=list)
    diverted_samples: list[DivertedRow] = field(default_factory=list)

    @property
    def balanced(self) -> bool:
        """Every row read is accounted for: kept, filtered out, or diverted."""
        return self.rows_in == self.rows_out + self.rows_shaped_out + self.rows_diverted

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows_in": self.rows_in,
            "rows_out": self.rows_out,
            "rows_shaped_out": self.rows_shaped_out,
            "rows_diverted": self.rows_diverted,
            "cells_changed": self.cells_changed,
            "nulls_introduced": self.nulls_introduced,
            "balanced": self.balanced,
            "steps": [s.to_dict() for s in self.steps],
            "diverted_samples": [d.to_dict() for d in self.diverted_samples],
        }


class _Drop(Exception):
    """Internal: this row leaves the stream."""

    def __init__(self, *, diverted: bool, reason: str, step_index: int, op: str):
        self.diverted = diverted
        self.reason = reason
        self.step_index = step_index
        self.op = op


class ShapeEngine:
    """Applies a recipe to rows, accumulating its effect across batches."""

    def __init__(
        self,
        recipe: ShapeRecipe,
        *,
        sample_limit: int = _DEFAULT_SAMPLES,
        keep_diverted_records: bool = True,
    ):
        self.recipe = recipe
        self.steps = recipe.enabled_steps
        self.sample_limit = max(0, sample_limit)
        self.keep_diverted_records = keep_diverted_records
        self.effect = ShapeEffect(
            steps=[
                StepEffect(
                    index=i,
                    op=s.op,
                    label=s.describe(),
                    active=s.active,
                )
                for i, s in enumerate(self.steps)
            ]
        )
        self._row_index = -1

    # -- public API ---------------------------------------------------------

    @property
    def is_noop(self) -> bool:
        return not self.steps

    def apply_batch(self, records: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Shape a batch, continuing the row numbering from earlier batches."""
        out: list[dict[str, Any]] = []
        for record in records:
            shaped = self.apply_row(record)
            if shaped is not None:
                out.append(shaped)
        return out

    def apply_row(self, record: Mapping[str, Any]) -> dict[str, Any] | None:
        """Shape one row. ``None`` means the recipe removed or diverted it."""
        self._row_index += 1
        self.effect.rows_in += 1
        row: dict[str, Any] = dict(record)
        if not self.steps:
            self.effect.rows_out += 1
            return row

        for index, step in enumerate(self.steps):
            tally = self.effect.steps[index]
            tally.rows_in += 1
            try:
                row = self._apply_step(step, index, row, tally)
            except _Drop as drop:
                if drop.diverted:
                    tally.rows_diverted += 1
                    self.effect.rows_diverted += 1
                    self._sample_diverted(drop, row)
                else:
                    tally.rows_removed += 1
                    self.effect.rows_shaped_out += 1
                return None
            tally.rows_out += 1

        self.effect.rows_out += 1
        return row

    def rows_accounted_for(self) -> bool:
        return self.effect.balanced

    # -- step dispatch -----------------------------------------------------

    def _apply_step(
        self,
        step: ShapeStep,
        index: int,
        row: dict[str, Any],
        tally: StepEffect,
    ) -> dict[str, Any]:
        op = step.op
        options = step.options

        if op == "drop_column":
            row.pop(step.column, None)
            return row

        if op == "keep_columns":
            wanted = [str(c) for c in options.get("columns", [])]
            return {name: row.get(name) for name in wanted if name in row}

        if op == "rename_column":
            target = str(options.get("to") or "")
            if step.column not in row or not target:
                return row
            return {
                (target if key == step.column else key): value
                for key, value in row.items()
            }

        if op == "constant_column":
            target = str(options.get("to") or "")
            value = options.get("value")
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op in ("derive_column", "set_value"):
            target = str(options.get("to") or "") if op == "derive_column" else step.column
            assert step.expression is not None
            value = self._guarded(step, index, tally, row, lambda: step.expression.evaluate(row))
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "concat_columns":
            target = str(options.get("to") or "")
            separator = str(options.get("separator") or "")
            parts = [_as_text(row.get(name)) for name in options.get("columns", [])]
            value = separator.join(p for p in parts if p is not None)
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "hash_identity":
            target = str(options.get("to") or "_df_row_key")
            value = _hash_identity_value(row, [str(c) for c in options.get("columns", [])])
            self._record_write(tally, before=row.get(target), after=value, existed=target in row)
            row[target] = value
            return row

        if op == "split_column":
            separator = str(options.get("separator") or "")
            targets = [str(name) for name in options.get("into", [])]
            text = _as_text(row.get(step.column))
            pieces = text.split(separator) if text is not None else []
            for position, name in enumerate(targets):
                value = pieces[position] if position < len(pieces) else None
                self._record_write(tally, before=row.get(name), after=value, existed=name in row)
                row[name] = value
            return row

        if op == "filter_rows":
            assert step.expression is not None
            keep = bool(options.get("keep", True))
            matched = self._guarded(step, index, tally, row, lambda: step.expression.matches(row))
            if bool(matched) != keep:
                raise _Drop(diverted=False, reason="filtered out", step_index=index, op=op)
            return row

        if op == "divert_rows":
            assert step.expression is not None
            matched = self._guarded(step, index, tally, row, lambda: step.expression.matches(row))
            if matched:
                raise _Drop(
                    diverted=True,
                    reason=str(options.get("reason") or "diverted by a transform rule"),
                    step_index=index,
                    op=op,
                )
            return row

        # Everything else is a single-cell value transform.
        before = row.get(step.column)
        after = self._guarded(
            step, index, tally, row, lambda: _transform_value(step, before)
        )
        self._record_write(tally, before=before, after=after, existed=True)
        row[step.column] = after
        return row

    # -- bookkeeping -------------------------------------------------------

    def _record_write(self, tally: StepEffect, *, before: Any, after: Any, existed: bool) -> None:
        if existed and _same_value(before, after):
            return
        tally.cells_changed += 1
        self.effect.cells_changed += 1
        if is_blank(after) and not is_blank(before):
            tally.nulls_introduced += 1
            self.effect.nulls_introduced += 1

    def _guarded(
        self,
        step: ShapeStep,
        index: int,
        tally: StepEffect,
        row: dict[str, Any],
        work: Any,
    ) -> Any:
        """Run a step's computation under its declared error policy.

        ``refuse`` (the default) fails the run and names the row — a value the
        recipe cannot compute is a decision for a human, not something to paper
        over. ``divert`` keeps the row but sends it to quarantine with the
        reason. ``null`` is the only policy that discards information, and it is
        counted so the proof pack can state how often it happened.
        """
        try:
            return work()
        except (EvalError, ValueError, ArithmeticError, TypeError) as exc:
            tally.errors += 1
            column = step.column or str(step.options.get("to") or "")
            # A refusal surfaces to the operator as the run's failure message, so
            # it has to say where to look: step 3 of the recipe, row 431, column
            # arr_time — not just "not a number" for a million-row file.
            located = f"transform step {index + 1} ({step.op})"
            if column:
                located += f" on column '{column}'"
            # One-based, like every other row citation the operator sees.
            located += f", source row {self._row_index + 1}: {exc}"
            failure = ShapeRowError(
                located,
                step=step,
                step_index=index,
                row_index=self._row_index,
                column=column,
            )
            if len(tally.error_samples) < self.sample_limit:
                tally.error_samples.append(failure.as_dict())
            if step.on_error == "refuse":
                raise failure from exc
            if step.on_error == "divert":
                raise _Drop(
                    diverted=True,
                    reason=f"{step.op} could not be applied: {exc}",
                    step_index=index,
                    op=step.op,
                ) from exc
            return None

    def _sample_diverted(self, drop: _Drop, row: dict[str, Any]) -> None:
        if len(self.effect.diverted_samples) >= self.sample_limit:
            return
        self.effect.diverted_samples.append(
            DivertedRow(
                row_index=self._row_index,
                reason=drop.reason,
                step=drop.step_index,
                op=drop.op,
                record=dict(row) if self.keep_diverted_records else {},
            )
        )


# ---------------------------------------------------------------------------
# Single-cell transforms — one implementation, shared by preview and execute
# ---------------------------------------------------------------------------


def _call(name: str, *args: Any) -> Any:
    """Reuse the expression function library so both planes agree exactly.

    A `trim` step and a `trim(col)` expression must produce the same value; the
    only way to guarantee that is for there to be one implementation.
    """
    return FUNCTIONS[name].impl(*args)


def _transform_value(step: ShapeStep, value: Any) -> Any:
    op = step.op
    options = step.options

    if op == "trim":
        return _call("trim", value)
    if op == "collapse_whitespace":
        return _call("collapse_whitespace", value)
    if op == "case":
        mode = options.get("mode")
        if mode == "upper":
            return _call("upper", value)
        if mode == "lower":
            return _call("lower", value)
        return _call("title_case", value)
    if op == "strip_characters":
        return _call("strip_characters", value, options.get("characters"))
    if op == "pad":
        fn = "lpad" if options.get("side", "left") == "left" else "rpad"
        return _call(fn, value, options.get("width"), options.get("fill", " "))
    if op == "replace":
        if options.get("regex"):
            return _call("regex_replace", value, options.get("search"), options.get("replacement", ""))
        return _call("replace", value, options.get("search"), options.get("replacement", ""))
    if op == "default_if_null":
        return options.get("value") if is_blank(value) else value
    if op == "null_if":
        if is_blank(value):
            return None
        sentinels = {
            text
            for raw in options.get("values", [])
            if (text := _as_text(raw)) is not None
        }
        text = _as_text(value)
        return None if text is not None and text in sentinels else value
    if op == "round_number":
        return _call("round", value, options.get("places", 0)) if not is_blank(value) else None
    if op == "truncate_number":
        return _call("truncate", value, options.get("places", 0)) if not is_blank(value) else None
    if op == "absolute":
        return _call("abs", value)
    if op == "clamp":
        low = options.get("min")
        high = options.get("max")
        if is_blank(value):
            return None
        number = _call("to_number", value)
        if low not in (None, ""):
            number = _call("greatest", number, _call("to_number", low))
        if high not in (None, ""):
            number = _call("least", number, _call("to_number", high))
        return number
    if op == "parse_number":
        return _call("to_number", value)
    if op == "parse_date":
        parsed = _call("to_date", value, options.get("format"))
        output = options.get("output_format")
        if parsed is not None and output:
            return _call("format_date", parsed, output)
        return parsed
    if op == "parse_boolean":
        return _call("to_boolean", value)
    if op == "normalize_unicode":
        return _call("normalize_unicode", value, options.get("form", "NFC"))
    if op == "cast_column":
        return _cast(value, str(options.get("to_type")), options.get("format"))

    raise ValueError(f"'{op}' has no value implementation")


def _cast(value: Any, to_type: str, fmt: Any) -> Any:
    """Declare a value's logical type, so the profiler is told rather than asked.

    This is the direct answer to a profiler collapsing `1.50000000` to a
    narrower scale: the operator states the type, and Map sees the declaration
    instead of an inference drawn from a sample.
    """
    if is_blank(value):
        return None
    if to_type == "text":
        return _call("to_text", value)
    if to_type == "integer":
        number = _call("to_number", value)
        whole = number.to_integral_value(rounding="ROUND_HALF_UP")
        if whole != number:
            raise EvalError(f"'{value}' is not a whole number")
        return int(whole)
    if to_type == "decimal":
        return _call("to_number", value)
    if to_type == "boolean":
        return _call("to_boolean", value)
    if to_type in ("date", "timestamp"):
        parsed = _call("to_date", value, fmt)
        if parsed is None:
            return None
        return parsed.date() if to_type == "date" else parsed
    raise EvalError(f"'{to_type}' is not a logical type")


def _hash_identity_value(row: Mapping[str, Any], columns: Sequence[str]) -> str:
    """Stable SHA-256 of the named cells — row-local, no clock, no randomness.

    Empty and missing stay distinct so two sparse rows cannot collide into one
    key. Gate-8 uses this as the align key when the source has no natural PK
    (flights-1m ``FL_DATE`` repeats).
    """
    parts: list[str] = []
    for name in columns:
        if name not in row:
            parts.append("\x1eABSENT")
            continue
        text = _as_text(row.get(name))
        parts.append("\x1eEMPTY" if text is None else text)
    payload = "\x1f".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _same_value(before: Any, after: Any) -> bool:
    """Whether a write actually changed the cell, for an honest change count."""
    if before is after:
        return True
    if is_blank(before) and is_blank(after):
        return True
    if is_blank(before) or is_blank(after):
        return False
    if isinstance(before, (int, float, Decimal)) and isinstance(after, (int, float, Decimal)):
        if isinstance(before, bool) != isinstance(after, bool):
            return False
        try:
            return Decimal(str(before)) == Decimal(str(after))
        except (InvalidOperation, DecimalException, ValueError):
            # NaN or infinity: not comparable as equal, so the cell counts as written.
            return False
    return _as_text(before) == _as_text(after)


def shape_records(
    recipe: ShapeRecipe,
    records: Sequence[Mapping[str, Any]],
    *,
    sample_limit: int = _DEFAULT_SAMPLES,
) -> tuple[list[dict[str, Any]], ShapeEffect]:
    """One-shot convenience for preview and tests."""
    engine = ShapeEngine(recipe, sample_limit=sample_limit)
    shaped = engine.apply_batch(records)
    return shaped, engine.effect
