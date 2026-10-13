"""What a shaping recipe *is* — steps, their classification, and its identity.

A recipe is an ordered list of named steps applied to the source stream before
Map sees it, in the spirit of Power Query's Applied Steps: the list is the
program, and the source file is never mutated.

Three rules are enforced here rather than left to the engine, because a step
that cannot be run honestly must be refused while the operator is still looking
at the screen:

1. **Row-local and deterministic only.** A join, aggregate, pivot, sort, window
   or unbounded dedupe cannot be evaluated on a stream without buffering the
   population; each is refused by name with a pointer to the post-load
   Transforms page, the way Informatica refuses transformations that its
   SQL-ELT pushdown mode cannot express.
2. **Active steps are declared.** A step that changes the row count moves the
   conservation ledger. Removals close
   ``rows_read = rows_shaped_out + dest_count + held_out + skipped``.
   Expansions (unnest) close
   ``rows_read + rows_expanded = dest_count + held_out + skipped + rows_shaped_out``.
   A passive step must not move either term. The classification lives on the
   step definition so the ledger and the UI read the same source of truth.
3. **One identity.** ``recipe_hash`` is a canonical hash over the parsed steps,
   so Validate and Execute can be proved to have run the same recipe and a
   whitespace edit does not invalidate an approval.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from services.shape_expr import (
    Expression,
    ExpressionError,
    compile_expression,
)

__all__ = [
    "MAX_STEPS",
    "ShapeError",
    "ShapeStep",
    "ShapeRecipe",
    "STEP_CATALOG",
    "describe_catalog",
]


class ShapeError(ValueError):
    """A recipe that cannot be honoured. The message names the fix."""


# A recipe longer than this is a program, not a cleanup, and every step costs a
# pass over every row. Refuse rather than quietly making a 1M-row run crawl.
MAX_STEPS = 100

# Ops a user reasonably reaches for that cannot be evaluated row-locally on a
# stream. Naming each one, with where it *can* be done, is the difference
# between a product that teaches and one that says "invalid step type".
_GLOBAL_OPS: dict[str, str] = {
    "join": "joins another table",
    "lookup": "reads a second dataset",
    "aggregate": "collapses many rows into one",
    "group_by": "collapses many rows into one",
    "pivot": "turns rows into columns",
    "unpivot": "turns columns into rows",
    "sort": "orders the whole population",
    "order_by": "orders the whole population",
    "rank": "compares a row against the whole population",
    "window": "compares a row against neighbouring rows",
    "dedupe": "must remember every key it has seen",
    "distinct": "must remember every row it has seen",
    "running_total": "carries state between rows",
    "fill_down": "carries state between rows",
}

_CASE_MODES = ("upper", "lower", "title")

_IDENTIFIER = re.compile(r"^[^\x00-\x1f]{1,255}$")


@dataclass(frozen=True, slots=True)
class _StepDef:
    """Static facts about one operation type."""

    op: str
    summary: str
    active: bool = False          # may change the row count
    expands: bool = False         # may add rows (unnest); still active
    family: str = "cleanse"       # structural | cleanse | nested | rows
    needs_column: bool = True     # operates on an existing named column
    produces_column: bool = False # writes a new column name
    expression: str = ""          # name of the option carrying an expression
    options: tuple[str, ...] = ()
    required: tuple[str, ...] = ()


_CATALOG_LIST: tuple[_StepDef, ...] = (
    # --- structural (Tier 2) -------------------------------------------------
    _StepDef("drop_column", "Remove a column", family="structural", needs_column=True),
    _StepDef(
        "keep_columns",
        "Keep only the named columns, in this order",
        family="structural",
        needs_column=False,
        options=("columns",),
        required=("columns",),
    ),
    _StepDef(
        "rename_column",
        "Rename a column",
        family="structural",
        options=("to",),
        required=("to",),
        produces_column=True,
    ),
    _StepDef(
        "cast_column",
        "Declare a column's logical type explicitly",
        family="structural",
        options=("to_type", "format"),
        required=("to_type",),
    ),
    _StepDef(
        "constant_column",
        "Add a column holding one literal value",
        family="structural",
        needs_column=False,
        produces_column=True,
        options=("to", "value"),
        required=("to",),
    ),
    _StepDef(
        "derive_column",
        "Add a column computed from this row",
        family="structural",
        needs_column=False,
        produces_column=True,
        expression="expression",
        options=("to", "expression"),
        required=("to", "expression"),
    ),
    _StepDef(
        "split_column",
        "Split one column into several by a separator",
        family="structural",
        options=("separator", "into", "limit"),
        required=("separator", "into"),
        produces_column=True,
    ),
    _StepDef(
        "concat_columns",
        "Join several columns into one",
        family="structural",
        needs_column=False,
        produces_column=True,
        options=("to", "columns", "separator"),
        required=("to", "columns"),
    ),
    _StepDef(
        "hash_identity",
        "Add a stable SHA-256 row key from selected columns so Gate-8 can align source and dest",
        family="structural",
        needs_column=False,
        produces_column=True,
        options=("to", "columns"),
        required=("columns",),
    ),
    # --- value cleansing (Tier 3) -------------------------------------------
    _StepDef("trim", "Remove leading and trailing whitespace", family="cleanse"),
    _StepDef("collapse_whitespace", "Squeeze runs of whitespace to one space", family="cleanse"),
    _StepDef("case", "Change letter case", family="cleanse", options=("mode",), required=("mode",)),
    _StepDef(
        "strip_characters",
        "Remove a class of characters",
        options=("characters",),
        required=("characters",),
    ),
    _StepDef("pad", "Pad to a fixed width", options=("width", "fill", "side"), required=("width",)),
    _StepDef(
        "replace",
        "Replace text, literally or by pattern",
        options=("search", "replacement", "regex"),
        required=("search",),
    ),
    _StepDef("default_if_null", "Substitute a value for blanks", options=("value",), required=("value",)),
    _StepDef("null_if", "Treat a sentinel value as null", options=("values",), required=("values",)),
    _StepDef("round_number", "Round to N decimal places", options=("places",), required=("places",)),
    _StepDef("truncate_number", "Drop digits beyond N places", options=("places",), required=("places",)),
    _StepDef("absolute", "Absolute value"),
    _StepDef("clamp", "Hold a number between two bounds", options=("min", "max")),
    _StepDef("parse_number", "Parse a human-written number"),
    _StepDef(
        "parse_date",
        "Parse a write-path calendar date, or an explicit format",
        options=("format", "output_format"),
    ),
    _StepDef(
        "parse_boolean",
        "Parse true/t/1 and false/f/0 — informal yes/Y/2 refuse",
    ),
    _StepDef("normalize_unicode", "Apply a Unicode normal form", options=("form",)),
    _StepDef(
        "set_value",
        "Overwrite a column with an expression",
        expression="expression",
        options=("expression",),
        required=("expression",),
    ),
    # --- nested JSON (declared in-flight; Map explode is a different plane) --
    _StepDef(
        "unnest_json",
        "Explode a JSON array into one row per element — dest COUNT is the expanded image, not a surplus",
        family="nested",
        active=True,
        expands=True,
        options=("to", "index_to", "keep_parent"),
    ),
    _StepDef(
        "flatten_json",
        "Promote JSON object keys into columns on this row — parent blob is kept unless you drop it",
        family="nested",
        options=("depth", "keys"),
    ),
    # --- row shape (Tier 4, active) -----------------------------------------
    _StepDef(
        "filter_rows",
        "Keep only rows the condition matches",
        family="rows",
        active=True,
        needs_column=False,
        expression="condition",
        options=("condition", "keep"),
        required=("condition",),
    ),
    _StepDef(
        "divert_rows",
        "Send matching rows to quarantine with a stated reason",
        family="rows",
        active=True,
        needs_column=False,
        expression="condition",
        options=("condition", "reason"),
        required=("condition",),
    ),
)

STEP_CATALOG: dict[str, _StepDef] = {d.op: d for d in _CATALOG_LIST}

_CAST_TYPES = ("text", "integer", "decimal", "boolean", "date", "timestamp")

# What a step does with a value it cannot compute. Fail-closed by default: a
# migration that silently nulls a cell it could not parse is the class of
# dishonesty this engine exists to remove.
_ERROR_POLICIES = ("refuse", "divert", "null")


def describe_catalog() -> list[dict[str, Any]]:
    """The operation catalog, for the Shape editor."""
    return [
        {
            "op": d.op,
            "summary": d.summary,
            "active": d.active,
            "expands": d.expands,
            "family": d.family,
            "needs_column": d.needs_column,
            "options": list(d.options),
            "required": list(d.required),
            "expression_option": d.expression or None,
        }
        for d in _CATALOG_LIST
    ]


@dataclass(frozen=True, slots=True)
class ShapeStep:
    """One operation, already parsed and checked against the source columns."""

    op: str
    column: str = ""
    options: Mapping[str, Any] = field(default_factory=dict)
    enabled: bool = True
    on_error: str = "refuse"
    label: str = ""
    expression: Expression | None = None

    @property
    def definition(self) -> _StepDef:
        return STEP_CATALOG[self.op]

    @property
    def active(self) -> bool:
        """Whether this step can change the row count."""
        return self.definition.active

    def describe(self) -> str:
        if self.label:
            return self.label
        target = self.column or str(self.options.get("to") or "")
        return f"{self.op}({target})" if target else self.op

    @property
    def writes(self) -> tuple[str, ...]:
        """Columns whose values this step can change or create.

        Used to decide which columns' declared types must be re-inferred after
        shaping: a column no step writes keeps the type the source declared, so
        shaping cannot re-open a carrier decision it never touched.
        """
        options = self.options
        if self.op in ("drop_column", "keep_columns", "filter_rows", "divert_rows"):
            return ()
        if self.op == "rename_column":
            target = str(options.get("to") or "")
            return (target,) if target else ()
        if self.op in ("constant_column", "derive_column", "concat_columns", "hash_identity"):
            target = str(options.get("to") or "")
            return (target,) if target else ()
        if self.op == "split_column":
            return tuple(str(name) for name in options.get("into", []))
        if self.op == "unnest_json":
            names = [str(options.get("to") or "")]
            index_to = str(options.get("index_to") or "")
            if index_to:
                names.append(index_to)
            return tuple(name for name in names if name)
        if self.op == "flatten_json":
            return tuple(str(name) for name in options.get("keys", []) if name)
        return (self.column,) if self.column else ()

    def canonical(self) -> dict[str, Any]:
        """Identity of this step: shape, not spelling."""
        options = dict(self.options)
        if self.expression is not None:
            # The parsed tree is the meaning; the typed text is one rendering of
            # it, so reformatting an expression must not invalidate an approval.
            options.pop(self.definition.expression, None)
        body: dict[str, Any] = {
            "op": self.op,
            "column": self.column,
            "on_error": self.on_error,
            "options": _canonical_options(options),
        }
        if self.expression is not None:
            body["expression_ast"] = self.expression.canonical()
        return body

    def to_wire(self) -> dict[str, Any]:
        body: dict[str, Any] = {"op": self.op, "options": dict(self.options)}
        if self.column:
            body["column"] = self.column
        if self.label:
            body["label"] = self.label
        if not self.enabled:
            body["enabled"] = False
        if self.on_error != "refuse":
            body["on_error"] = self.on_error
        return body


@dataclass(frozen=True, slots=True)
class ShapeRecipe:
    """An ordered recipe, valid against a known source column set."""

    steps: tuple[ShapeStep, ...] = ()
    input_columns: tuple[str, ...] = ()
    output_columns: tuple[str, ...] = ()

    def __bool__(self) -> bool:
        return bool(self.enabled_steps)

    @property
    def enabled_steps(self) -> tuple[ShapeStep, ...]:
        return tuple(s for s in self.steps if s.enabled)

    @property
    def has_active_step(self) -> bool:
        """Whether any enabled step can change the row count — i.e. the ledger will move."""
        return any(s.active for s in self.enabled_steps)

    @property
    def identity_columns(self) -> tuple[str, ...]:
        """Columns a ``hash_identity`` step declared as the Gate-8 align key."""
        keys: list[str] = []
        for step in self.enabled_steps:
            if step.op != "hash_identity":
                continue
            target = str(step.options.get("to") or "_df_row_key")
            if target and target not in keys:
                keys.append(target)
        return tuple(keys)

    @property
    def recipe_hash(self) -> str:
        """Stable identity, pinned at approval and re-checked before Execute.

        Disabled steps are excluded: they do not touch a single row, so toggling
        one off must not invalidate an approval, and toggling one *on* must.

        A recipe with no enabled step has no identity, because there is no
        program to identify. Hashing the empty case would name the pass-through
        path — and an approval carrying that name, sent with no recipe (there is
        none to send), reads as "the approved recipe went missing" and refuses a
        run that is asking for exactly today's behaviour.

        The identity is the program alone. The source column set is not part of
        it, because every surface that computes this hash is holding its own
        rendering of that set — design-time introspection, a preview called with
        the sample's keys, the live read's headers — and folding it in made one
        recipe carry several identities, which reads to an operator as "this is
        not the recipe you approved" for a recipe nobody touched. Agreement with
        the source is a separate, better-worded check: ``parse`` refuses a step
        that reads a column the source does not have, and the read refuses a
        column the approved recipe never saw.
        """
        if not self.enabled_steps:
            return ""
        payload = json.dumps(
            {
                "steps": [s.canonical() for s in self.enabled_steps],
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]

    @property
    def touched_columns(self) -> frozenset[str]:
        """Every column an enabled step writes, plus every column it introduced."""
        written: set[str] = set()
        for step in self.enabled_steps:
            written.update(step.writes)
        written.update(set(self.output_columns) - set(self.input_columns))
        return frozenset(name for name in written if name)

    def to_wire(self) -> dict[str, Any]:
        return {"steps": [s.to_wire() for s in self.steps]}

    def describe(self) -> str:
        if not self.enabled_steps:
            return "no transform"
        return ", ".join(s.describe() for s in self.enabled_steps)

    @classmethod
    def parse(
        cls,
        payload: Any,
        *,
        source_columns: Sequence[str] | None = None,
    ) -> "ShapeRecipe":
        """Build a recipe from an API payload, refusing anything unrunnable.

        ``source_columns`` is what the source actually has. When given, every
        step is checked against the column set *as it stands at that point in
        the recipe*, so a step that reads a column an earlier step dropped is
        refused at design time rather than per row.
        """
        raw_steps = _payload_steps(payload)
        if len(raw_steps) > MAX_STEPS:
            raise ShapeError(
                f"a recipe may hold {MAX_STEPS} steps; this one has {len(raw_steps)}"
            )

        columns: list[str] | None = list(source_columns) if source_columns is not None else None
        original = tuple(columns) if columns is not None else ()
        steps: list[ShapeStep] = []
        for index, raw in enumerate(raw_steps):
            step = _parse_step(raw, index=index, columns=columns)
            steps.append(step)
            if columns is not None and step.enabled:
                _apply_column_effect(step, columns)

        return cls(
            steps=tuple(steps),
            input_columns=original,
            output_columns=tuple(columns) if columns is not None else (),
        )


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _payload_steps(payload: Any) -> list[Mapping[str, Any]]:
    if payload is None or payload == "" or payload == {}:
        return []
    if isinstance(payload, ShapeRecipe):
        return [s.to_wire() for s in payload.steps]
    if isinstance(payload, str):
        text = payload.strip()
        if not text:
            return []
        try:
            payload = json.loads(text)
        except ValueError as exc:
            raise ShapeError(f"shape recipe is not valid JSON: {exc}") from exc
    if isinstance(payload, Mapping):
        raw = payload.get("steps", [])
    else:
        raw = payload
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise ShapeError("shape recipe steps must be a list")
    out: list[Mapping[str, Any]] = []
    for item in raw:
        if not isinstance(item, Mapping):
            raise ShapeError(f"each shape step must be an object, got {type(item).__name__}")
        out.append(item)
    return out


def _parse_step(
    raw: Mapping[str, Any],
    *,
    index: int,
    columns: list[str] | None,
) -> ShapeStep:
    where = f"step {index + 1}"
    op = str(raw.get("op") or raw.get("type") or "").strip().casefold()
    if not op:
        raise ShapeError(f"{where} has no op")
    if op in _GLOBAL_OPS:
        raise ShapeError(
            f"{where}: '{op}' {_GLOBAL_OPS[op]}, which cannot be done row by row "
            "on a stream — do it as a post-load model on the Transforms page, "
            "where the whole population is addressable"
        )
    definition = STEP_CATALOG.get(op)
    if definition is None:
        raise ShapeError(
            f"{where}: '{op}' is not a transform operation. Available: "
            f"{', '.join(sorted(STEP_CATALOG))}"
        )

    options = raw.get("options")
    if options is None:
        # A UI that puts options beside the op rather than nested is a normal
        # shape for a form payload; accept both rather than making the client
        # nest a single value.
        options = {
            k: v
            for k, v in raw.items()
            if k in definition.options
        }
    if not isinstance(options, Mapping):
        raise ShapeError(f"{where}: options must be an object")
    unknown = sorted(set(options) - set(definition.options))
    if unknown:
        raise ShapeError(
            f"{where}: {op} has no option {unknown[0]!r}; it accepts "
            f"{', '.join(definition.options) or 'none'}"
        )
    for name in definition.required:
        if options.get(name) in (None, ""):
            raise ShapeError(f"{where}: {op} needs '{name}'")

    on_error = str(raw.get("on_error") or "refuse").strip().casefold()
    if on_error not in _ERROR_POLICIES:
        raise ShapeError(
            f"{where}: on_error must be one of {', '.join(_ERROR_POLICIES)}"
        )

    column = str(raw.get("column") or raw.get("source_column") or "").strip()
    if definition.needs_column:
        if not column:
            raise ShapeError(f"{where}: {op} needs a column")
        if columns is not None and column not in columns:
            raise ShapeError(
                f"{where}: column '{column}' is not available at this point in the "
                f"recipe. Available: {', '.join(columns[:20])}"
                + (" …" if len(columns) > 20 else "")
            )

    normalized = _normalize_options(
        op, definition, options, where=where, columns=columns, column=column
    )

    expression: Expression | None = None
    if definition.expression:
        source = str(normalized.get(definition.expression) or "")
        try:
            expression = compile_expression(
                source,
                known_columns=columns,
                label=f"{where} {definition.expression}",
            )
        except ExpressionError as exc:
            raise ShapeError(str(exc)) from exc

    return ShapeStep(
        op=op,
        column=column,
        options=normalized,
        enabled=bool(raw.get("enabled", True)),
        on_error=on_error,
        label=str(raw.get("label") or "").strip(),
        expression=expression,
    )


def _normalize_options(
    op: str,
    definition: _StepDef,
    options: Mapping[str, Any],
    *,
    where: str,
    columns: list[str] | None,
    column: str = "",
) -> dict[str, Any]:
    out = dict(options)

    if op == "case":
        mode = str(out.get("mode") or "").strip().casefold()
        if mode not in _CASE_MODES:
            raise ShapeError(f"{where}: case mode must be one of {', '.join(_CASE_MODES)}")
        out["mode"] = mode

    if op == "cast_column":
        to_type = str(out.get("to_type") or "").strip().casefold()
        if to_type not in _CAST_TYPES:
            raise ShapeError(
                f"{where}: '{to_type}' is not a logical type; use "
                f"{', '.join(_CAST_TYPES)}"
            )
        out["to_type"] = to_type

    if op == "pad":
        out["width"] = _as_positive_int(out.get("width"), "width", where=where)
        side = str(out.get("side") or "left").strip().casefold()
        if side not in ("left", "right"):
            raise ShapeError(f"{where}: pad side must be left or right")
        out["side"] = side
        fill = out.get("fill")
        out["fill"] = " " if fill in (None, "") else str(fill)

    if op in ("round_number", "truncate_number"):
        places = _as_int(out.get("places"), "places", where=where)
        if abs(places) > 30:
            raise ShapeError(f"{where}: places must be within 30 digits")
        out["places"] = places

    if op == "clamp":
        if out.get("min") in (None, "") and out.get("max") in (None, ""):
            raise ShapeError(f"{where}: clamp needs a min, a max, or both")

    if op == "null_if":
        values = out.get("values")
        if isinstance(values, (str, int, float, bool)):
            values = [values]
        if not isinstance(values, (list, tuple)) or not values:
            raise ShapeError(f"{where}: null_if needs a list of sentinel values")
        persisted = [_option_cell_token(v) for v in values]
        persisted = [t for t in persisted if t is not None]
        if not persisted:
            raise ShapeError(f"{where}: null_if needs a list of sentinel values")
        out["values"] = persisted

    if op == "normalize_unicode":
        form = str(out.get("form") or "NFC").strip().upper()
        if form not in ("NFC", "NFD", "NFKC", "NFKD"):
            raise ShapeError(f"{where}: '{form}' is not a Unicode normal form")
        out["form"] = form

    if op == "strip_characters":
        kind = str(out.get("characters") or "").strip().casefold()
        allowed = (
            "punctuation", "digits", "letters",
            "non_numeric", "non_printable", "whitespace",
        )
        if kind not in allowed:
            raise ShapeError(
                f"{where}: characters must be one of {', '.join(allowed)}"
            )
        out["characters"] = kind

    if op == "replace":
        out["search"] = str(out.get("search"))
        out["replacement"] = "" if out.get("replacement") is None else str(out["replacement"])
        out["regex"] = bool(out.get("regex"))
        if out["regex"]:
            try:
                re.compile(out["search"])
            except re.error as exc:
                raise ShapeError(f"{where}: invalid pattern — {exc}") from exc

    if op == "split_column":
        into = out.get("into")
        if isinstance(into, str):
            into = [part.strip() for part in into.split(",") if part.strip()]
        if not isinstance(into, (list, tuple)) or len(into) < 2:
            raise ShapeError(
                f"{where}: split_column needs at least two target column names"
            )
        out["into"] = [_valid_name(str(name), where=where) for name in into]
        separator = str(out.get("separator") or "")
        if not separator:
            raise ShapeError(f"{where}: split_column needs a separator")
        out["separator"] = separator

    if op in ("concat_columns", "keep_columns", "hash_identity"):
        names = out.get("columns")
        if isinstance(names, str):
            names = [part.strip() for part in names.split(",") if part.strip()]
        if not isinstance(names, (list, tuple)) or not names:
            raise ShapeError(f"{where}: {op} needs a list of columns")
        names = [str(n) for n in names]
        if columns is not None:
            missing = [n for n in names if n not in columns]
            if missing:
                raise ShapeError(
                    f"{where}: column '{missing[0]}' is not available at this point "
                    f"in the recipe. Available: {', '.join(columns[:20])}"
                )
        out["columns"] = names
        if op == "concat_columns":
            out["separator"] = "" if out.get("separator") is None else str(out["separator"])
        if op == "hash_identity" and not out.get("to"):
            out["to"] = "_df_row_key"

    if definition.produces_column and op != "split_column":
        target = out.get("to")
        if target is not None:
            out["to"] = _valid_name(str(target), where=where)

    if op == "filter_rows":
        keep = out.get("keep", True)
        if isinstance(keep, str):
            keep = keep.strip().casefold() not in ("false", "0", "no")
        out["keep"] = bool(keep)

    if op == "divert_rows":
        reason = str(out.get("reason") or "").strip()
        out["reason"] = reason or "diverted by a transform rule"

    if op == "unnest_json":
        target = out.get("to")
        if target in (None, ""):
            target = f"{column}_item" if column else "item"
        out["to"] = _valid_name(str(target), where=where)
        index_to = out.get("index_to")
        if index_to in (None, ""):
            out["index_to"] = ""
        else:
            out["index_to"] = _valid_name(str(index_to), where=where)
        keep = out.get("keep_parent", True)
        if isinstance(keep, str):
            keep = keep.strip().casefold() not in ("false", "0", "no")
        out["keep_parent"] = bool(keep)

    if op == "flatten_json":
        depth = str(out.get("depth") or "top").strip().casefold()
        if depth in ("1", "top_level", "flatten_top_level_keys"):
            depth = "top"
        elif depth in ("2", "deep_flatten", "flatten_deep"):
            depth = "deep"
        if depth not in ("top", "deep"):
            raise ShapeError(f"{where}: flatten_json depth must be top or deep")
        out["depth"] = depth
        keys = out.get("keys")
        if keys in (None, ""):
            out["keys"] = []
        else:
            if isinstance(keys, str):
                keys = [part.strip() for part in keys.split(",") if part.strip()]
            if not isinstance(keys, (list, tuple)):
                raise ShapeError(f"{where}: flatten_json keys must be a list of names")
            out["keys"] = [_valid_name(str(name), where=where) for name in keys]

    return out


def _apply_column_effect(step: ShapeStep, columns: list[str]) -> None:
    """Track how the column set looks after this step, for later steps' checks."""
    op = step.op
    options = step.options
    if op == "drop_column":
        if step.column in columns:
            columns.remove(step.column)
        return
    if op == "keep_columns":
        wanted = [str(c) for c in options.get("columns", [])]
        columns[:] = [c for c in wanted if c in columns]
        return
    if op == "rename_column":
        target = str(options.get("to") or "")
        if step.column in columns and target:
            columns[columns.index(step.column)] = target
        return
    if op in ("constant_column", "derive_column", "concat_columns", "hash_identity"):
        target = str(options.get("to") or "")
        if target and target not in columns:
            columns.append(target)
        return
    if op == "split_column":
        for name in options.get("into", []):
            if name not in columns:
                columns.append(str(name))
        return
    if op == "unnest_json":
        for name in (options.get("to"), options.get("index_to")):
            if name and str(name) not in columns:
                columns.append(str(name))
        if options.get("keep_parent") is False and step.column in columns:
            columns.remove(step.column)
        return
    if op == "flatten_json":
        for name in options.get("keys", []):
            if name and str(name) not in columns:
                columns.append(str(name))
        return


def _option_cell_token(value: Any) -> str | None:
    """One recipe option cell on the transfer wire.

    ``str(True)`` is ``True``; dest and apply use ``true``. Reader-null
    is not a sentinel the operator can ask to match — skip it.
    """
    from services.value_serializer import present_cell_text

    return present_cell_text(value)


def _canonical_options(options: Mapping[str, Any]) -> dict[str, Any]:
    """Options with list order preserved and scalars stringified consistently."""
    out: dict[str, Any] = {}
    for key in sorted(options):
        value = options[key]
        if isinstance(value, (list, tuple)):
            out[key] = [t for v in value if (t := _option_cell_token(v)) is not None]
        elif isinstance(value, bool) or value is None:
            out[key] = value
        else:
            token = _option_cell_token(value)
            out[key] = value if token is None else token
    return out


def _valid_name(name: str, *, where: str) -> str:
    cleaned = name.strip()
    if not cleaned:
        raise ShapeError(f"{where}: a column name cannot be empty")
    if not _IDENTIFIER.match(cleaned):
        raise ShapeError(
            f"{where}: '{name}' is not a usable column name (control characters "
            "or over 255 characters)"
        )
    return cleaned


def _as_int(value: Any, field_name: str, *, where: str) -> int:
    if isinstance(value, bool):
        raise ShapeError(f"{where}: {field_name} must be a whole number")
    try:
        return int(str(value).strip())
    except (TypeError, ValueError) as exc:
        raise ShapeError(f"{where}: {field_name} must be a whole number, got {value!r}") from exc


def _as_positive_int(value: Any, field_name: str, *, where: str) -> int:
    number = _as_int(value, field_name, where=where)
    if number <= 0:
        raise ShapeError(f"{where}: {field_name} must be greater than zero")
    if number > 10_000:
        raise ShapeError(f"{where}: {field_name} must be at most 10000")
    return number
