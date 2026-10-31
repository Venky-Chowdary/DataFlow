"""What a shaping recipe *is* — steps, their classification, and its identity.

A recipe is an ordered list of named steps applied to the source stream before
Map sees it, in the spirit of Power Query's Applied Steps: the list is the
program, and the source file is never mutated.

Three rules are enforced here rather than left to the engine, because a step
that cannot be run honestly must be refused while the operator is still looking
at the screen:

1. **Row-local steps + declared buffering ops only.** Row-local steps evaluate
   a row without its neighbours. The buffered set-ops — ``dedupe_rows``,
   ``aggregate``, ``sort_rows``, ``rank_rows`` — declare that they hold the
   population (bounded by ``_MAX_BUFFERED``) and emit on drain at
   end-of-stream; a truly unbounded/multi-input op (join, union, pivot,
   window) is still refused by name with a pointer to the post-load Transforms
   page, the way Informatica refuses transformations its SQL-ELT pushdown
   cannot express.
2. **Active steps are declared.** A step that changes the row count moves the
   conservation ledger. Removals close
   ``rows_read = rows_shaped_out + dest_count + held_out + skipped``.
   Expansions (unnest) close
   ``rows_read + rows_expanded = dest_count + held_out + skipped + rows_shaped_out``.
   A passive step must not move either term. The classification lives on the
   step definition so the ledger and the UI read the same source of truth.
3. **One identity.** ``recipe_hash`` is a canonical hash over the parsed steps,
   so Validate and Execute can be proved to have run the same recipe and a
   whitespace edit does not invalidate an approval. A set graph, when present,
   has its own ``set_hash`` and is not part of ``recipe_hash``.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from services.set_graph import SetGraph, attach_set_graph
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

# Ops a user reasonably reaches for under another name, or that still cannot
# be evaluated on this engine. Naming each one — with the op that does it when
# one exists — is the difference between a product that teaches and one that
# says "invalid step type".
_GLOBAL_OPS: dict[str, str] = {
    "group_by": "collapses many rows into one — the 'aggregate' op does this on-stream",
    "pivot": "turns rows into columns — that is a post-load model step",
    "unpivot": "turns columns into rows — that is a post-load model step",
    "sort": "orders the whole population — the 'sort_rows' op does this on-stream",
    "order_by": "orders the whole population — the 'sort_rows' op does this on-stream",
    "rank": "compares a row against the whole population — the 'rank_rows' op does this on-stream",
    "window": "compares a row against neighbouring rows — that is a post-load model step",
    "dedupe": "must remember every key it has seen — the 'dedupe_rows' op does this on-stream",
    "distinct": "must remember every row it has seen — the 'dedupe_rows' op does this on-stream",
    "running_total": "carries state between rows — that is a post-load model step",
    "fill_down": "carries state between rows — that is a post-load model step",
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
    buffered: bool = False        # holds the population in memory until drain
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
        options=("to_type", "format", "precision", "scale", "timezone"),
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
        options=("format", "output_format", "timezone"),
    ),
    _StepDef(
        "hash_pii",
        "Replace a value with an HMAC-SHA256 digest",
        family="cleanse",
    ),
    _StepDef(
        "mask_pii",
        "Mask a value in place and keep its shape",
        family="cleanse",
        options=("pattern",),
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
    _StepDef(
        "nest_json",
        "Group listed columns into a JSON object in a new column — relational → nested",
        family="nested",
        needs_column=False,
        produces_column=True,
        options=("fields", "to", "drop"),
        required=("fields", "to"),
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
    # --- advanced set operations (Tier 5, may hold rows to end-of-stream) ----
    _StepDef(
        "dedupe_rows",
        "Keep one row per key — first/last seen, the best by ordering columns, "
        "or a non-null merge — every loser ledgered to quarantine",
        family="advanced",
        active=True,
        needs_column=False,
        buffered=True,
        options=(
            "keys", "keep", "order_by", "order_dir", "then_by",
            "tie_break", "merge", "max_keys", "nulls",
        ),
        required=("keys",),
    ),
    _StepDef(
        "aggregate",
        "Group rows and emit one row per group with computed measures",
        family="advanced",
        active=True,
        needs_column=False,
        buffered=True,
        options=("group_by", "measures"),
        required=("group_by", "measures"),
    ),
    _StepDef(
        "sort_rows",
        "Order the whole stream by one or more columns",
        family="advanced",
        needs_column=False,
        buffered=True,
        options=("order", "order_by", "direction"),
        required=(),
    ),
    _StepDef(
        "rank_rows",
        "Keep the top or bottom N rows per partition by an ordering column",
        family="advanced",
        active=True,
        needs_column=False,
        buffered=True,
        options=("order_by", "direction", "limit", "partition_by"),
        required=("order_by", "limit"),
    ),
    _StepDef(
        "lookup",
        "Enrich a row by key — either from an inline reference list or a "
        "saved connector's table (loaded once at run start)",
        family="advanced",
        options=(
            "lookup_key", "rows", "reference", "map", "unmatched",
            "defaults", "match", "reason",
        ),
        required=("lookup_key", "map"),
    ),
    _StepDef(
        "surrogate_key",
        "Assign a running integer key to every row",
        family="advanced",
        needs_column=False,
        produces_column=True,
        options=("to", "start", "step"),
        required=("to",),
    ),
    _StepDef(
        "join",
        "Merge mapped columns from a secondary input (inline rows or a saved "
        "connector's table, loaded once at run start) matched on 'on' keys — "
        "inner drops or diverts non-matches, left keeps them with nulls",
        family="advanced",
        active=True,
        expands=True,
        needs_column=False,
        options=(
            "type", "on", "rows", "reference", "map", "fanout",
            "unmatched", "reason",
        ),
        required=("on", "map"),
    ),
    _StepDef(
        "union",
        "Append a secondary input (inline rows or a saved connector's table, "
        "loaded once at run start) after the stream, aligned to output "
        "columns by name — an extra column refuses unless mapped or extra=drop",
        family="advanced",
        active=True,
        expands=True,
        needs_column=False,
        options=("rows", "reference", "map", "extra"),
        required=(),
    ),
    _StepDef(
        "router",
        "Route each row to one named output — first matching condition wins; "
        "a route may declare default: true for unmatched rows, otherwise "
        "unmatched must be divert — each route writes its own destination "
        "table and may carry its own map",
        family="advanced",
        active=True,
        needs_column=False,
        options=("routes", "unmatched", "targets"),
        required=("routes",),
    ),
    _StepDef(
        "scd2",
        "Type-2 history contract — declares the business key and the stamp "
        "column names the destination merge writes (validity window + "
        "current flag + change hash); on re-run the merge closes the prior "
        "version of every changed key and inserts a new current row, and an "
        "unchanged key never duplicates",
        family="advanced",
        active=True,
        needs_column=False,
        options=(
            "keys",
            "tracked",
            "type1",
            "close_missing",
            "version",
            "effective_from",
            "end_date",
            "is_current",
            "row_hash",
        ),
        required=("keys",),
    ),
)

# Column every routed row is tagged with — the write path strips it and
# splits the batch by its value, so a routed row lands in exactly one
# route's destination table.
ROUTE_COLUMN = "_df_route"

#: Step options that name input columns (see ``ShapeRecipe.read_columns``).
_READ_OPTION_KEYS = (
    "columns", "keys", "order_by", "then_by", "group_by", "partition_by",
    "fields", "lookup_key", "on", "tracked", "type1", "order",
)

STEP_CATALOG: dict[str, _StepDef] = {d.op: d for d in _CATALOG_LIST}

_CAST_TYPES = ("text", "integer", "decimal", "boolean", "date", "timestamp")

# What a step does with a value it cannot compute. Fail-closed by default: a
# migration that silently nulls a cell it could not parse is the class of
# dishonesty this engine exists to remove.
_ERROR_POLICIES = ("refuse", "divert", "null")

#: The Transform Library (and its built-in templates) spells ``refuse`` as
#: ``fail``; one vocabulary means a recipe step pasted into Shape means the
#: same thing there (QA round 11, T9). The canonical word is what is stored
#: and hashed, so ``fail`` and ``refuse`` are the same recipe.
ON_ERROR_ALIASES = {"fail": "refuse"}


def canonical_on_error(raw: Any, *, where: str = "step") -> str:
    """The engine policy for a step's ``on_error``, or a ShapeError naming why not."""
    value = str(raw or "refuse").strip().casefold() or "refuse"
    value = ON_ERROR_ALIASES.get(value, value)
    if value in _ERROR_POLICIES:
        return value
    if value == "keep_original":
        raise ShapeError(
            f"{where}: on_error 'keep_original' writes the untransformed value, "
            "which the transfer engine cannot honor. Choose refuse (alias fail), "
            "divert or null"
        )
    raise ShapeError(
        f"{where}: on_error must be one of {', '.join(_ERROR_POLICIES)} "
        "(fail is accepted as refuse)"
    )


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
            "buffered": d.buffered,
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
        if self.op in ("dedupe_rows", "sort_rows", "rank_rows"):
            return ()
        if self.op == "aggregate":
            targets = [str(c) for c in options.get("group_by", [])]
            for measure in options.get("measures", []):
                if isinstance(measure, Mapping) and measure.get("as"):
                    targets.append(str(measure["as"]))
            return tuple(name for name in targets if name)
        if self.op in ("lookup", "join", "union"):
            return tuple(
                str(target) for target in (options.get("map") or {}).values() if target
            )
        if self.op == "router":
            targets = [ROUTE_COLUMN]
            for route in options.get("routes") or []:
                targets.extend(
                    str(t) for t in (route.get("map") or {}).values() if t
                )
            return tuple(dict.fromkeys(targets))
        if self.op == "surrogate_key":
            target = str(options.get("to") or "")
            return (target,) if target else ()
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
    set_graph: SetGraph | None = None
    # Source column -> declared logical type, carried alongside the recipe so
    # ordering steps compare under the real type, never wire text (DD-1).
    source_types: Mapping[str, str] | None = None

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
    def scd2_step(self) -> ShapeStep | None:
        """The enabled scd2 contract step, when the recipe declares one."""
        return next((s for s in self.enabled_steps if s.op == "scd2"), None)

    def scd2_column_names(self) -> dict[str, str] | None:
        """Recipe-declared SCD2 stamp column names, canonical keys."""
        step = self.scd2_step
        if step is None:
            return None
        opts = step.options
        return {
            "keys": [str(k) for k in (opts.get("keys") or [])],
            "tracked": [str(t) for t in (opts.get("tracked") or [])],
            "type1": bool(opts.get("type1")),
            "close_missing": bool(opts.get("close_missing", True)),
            "version": str(opts.get("version") or ""),
            "valid_from": str(opts.get("effective_from") or "valid_from"),
            "valid_to": str(opts.get("end_date") or "valid_to"),
            "is_current": str(opts.get("is_current") or "is_current"),
            "row_hash": str(opts.get("row_hash") or "row_hash"),
        }

    @property
    def router_step(self) -> ShapeStep | None:
        """The enabled router step, when this recipe routes rows to per-route
        destination tables. At most one router step is honoured; a recipe
        with more refuses at parse (single routing decision per stream)."""
        return next((s for s in self.enabled_steps if s.op == "router"), None)

    def route_output_columns(self, route_name: str) -> list[str]:
        """The columns one route's destination table gets.

        A route with a declared ``map`` emits exactly its map targets; a
        pass-through route emits every output column except the route tag
        and other routes' map targets (they belong to those routes' tables).
        """
        step = self.router_step
        if step is None:
            return []
        route = next(
            (r for r in (step.options.get("routes") or []) if r.get("name") == route_name),
            None,
        )
        if route is None:
            return []
        route_map = route.get("map")
        if route_map:
            return [str(t) for t in route_map.values()]
        mapped = {
            str(t)
            for r in (step.options.get("routes") or [])
            for t in (r.get("map") or {}).values()
        }
        return [
            c for c in (self.output_columns or self.input_columns or ())
            if c != ROUTE_COLUMN and c not in mapped
        ]

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
    def set_hash(self) -> str:
        """Identity of ``set_graph``. Empty when there are no nodes."""
        if self.set_graph is None:
            return ""
        return self.set_graph.set_hash

    @property
    def set_errors(self) -> tuple[Any, ...]:
        if self.set_graph is None:
            return ()
        return self.set_graph.errors

    @property
    def read_columns(self) -> tuple[str, ...]:
        """Columns the recipe reads from its INPUT, in first-read order.

        What a reader must project for this recipe to run: every column a step
        reads that no earlier step produced. A population walk that projected
        only its own fit columns handed a ``trim`` on ``name`` rows without
        ``name`` and the recipe refused ("column 'name' is not available").
        """
        produced: set[str] = set()
        reads: list[str] = []

        def _add(value: Any) -> None:
            if isinstance(value, Mapping):
                _add(value.get("column"))
                return
            name = str(value or "").strip() if isinstance(value, str) else ""
            if name and name not in produced and name not in reads:
                reads.append(name)

        for step in self.enabled_steps:
            _add(step.column)
            if step.expression is not None:
                for name in sorted(step.expression.columns):
                    _add(name)
            for key in _READ_OPTION_KEYS:
                value = step.options.get(key)
                if isinstance(value, Mapping):
                    for name in value:
                        _add(name)
                elif isinstance(value, (list, tuple)):
                    for item in value:
                        _add(item)
                else:
                    _add(value)
            for measure in step.options.get("measures") or []:
                _add(measure)
            produced.update(step.writes)
        return tuple(reads)

    @property
    def touched_columns(self) -> frozenset[str]:
        """Every column an enabled step writes, plus every column it introduced."""
        written: set[str] = set()
        for step in self.enabled_steps:
            written.update(step.writes)
        written.update(set(self.output_columns) - set(self.input_columns))
        return frozenset(name for name in written if name)

    def to_wire(self) -> dict[str, Any]:
        body: dict[str, Any] = {"steps": [s.to_wire() for s in self.steps]}
        if self.set_graph is not None and not self.set_graph.is_empty:
            body["set_graph"] = self.set_graph.to_wire()
        return body

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
        source_types: Mapping[str, str] | None = None,
    ) -> "ShapeRecipe":
        """Build a recipe from an API payload, refusing anything unrunnable.

        ``source_columns`` is what the source actually has. When given, every
        step is checked against the column set *as it stands at that point in
        the recipe*, so a step that reads a column an earlier step dropped is
        refused at design time rather than per row. ``source_types`` is the
        same column set's declared logical types — ordering steps compare
        under them because the wire carries every value as text.
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

        scd2_count = sum(1 for s in steps if s.op == "scd2" and s.enabled)
        if scd2_count > 1:
            raise ShapeError(
                "a recipe may hold one scd2 step — one history contract "
                "per destination"
            )
        router_count = sum(1 for s in steps if s.op == "router" and s.enabled)
        if router_count > 1:
            raise ShapeError(
                "a recipe may hold one router step — one routing decision "
                "per stream; model further outputs in a second transfer"
            )

        return cls(
            steps=tuple(steps),
            input_columns=original,
            output_columns=tuple(columns) if columns is not None else (),
            set_graph=attach_set_graph(payload),
            source_types=dict(source_types or {}),
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
        raise ShapeError(f"{where}: '{op}' {_GLOBAL_OPS[op]}")
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

    on_error = canonical_on_error(raw.get("on_error"), where=where)

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
        precision, scale = _optional_decimal_size(out, where=where)
        if (precision is not None or scale is not None) and to_type != "decimal":
            raise ShapeError(f"{where}: precision and scale apply only to a decimal cast")
        if precision is None:
            out.pop("precision", None)
        else:
            out["precision"] = precision
        if scale is None:
            out.pop("scale", None)
        else:
            out["scale"] = scale
        if to_type == "timestamp":
            _optional_timezone(out, where=where)
        elif str(out.get("timezone") or "").strip():
            raise ShapeError(f"{where}: timezone applies only to a timestamp cast")
        else:
            out.pop("timezone", None)

    if op == "parse_date":
        _optional_timezone(out, where=where)

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
            "non_numeric", "non_printable", "controls", "whitespace",
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

    # --- buffered set-ops: strict config, because a wrong option emitting
    # None/last silently is silent wrong data, not a style choice. ---------
    if op == "dedupe_rows":
        out["keys"] = _required_column_list(out, "keys", columns, where=where)
        keep = str(out.get("keep") or "first").strip().casefold()
        if keep not in ("first", "last"):
            raise ShapeError(f"{where}: dedupe keep must be first or last")
        out["keep"] = keep
        order_by = str(out.get("order_by") or "").strip()
        _assert_columns_exist([order_by], columns, "order_by", where=where)
        # Default 'desc' matches the engine: keep=last means the largest
        # order_by value wins when no direction is stated.
        order_dir = str(out.get("order_dir") or "desc").strip().casefold()
        if order_dir not in ("asc", "desc"):
            raise ShapeError(f"{where}: dedupe order_dir must be asc or desc")
        out["order_dir"] = order_dir
        if order_by:
            out["order_by"] = order_by
        else:
            out.pop("order_by", None)
            out.pop("order_dir", None)
        # nulls: 'last' (default) — a NULL stamp never displaces a real one
        # under either direction (DD-4); 'first' restores SQL NULLS FIRST.
        nulls = str(out.get("nulls") or "last").strip().casefold()
        if nulls not in ("first", "last"):
            raise ShapeError(f"{where}: dedupe nulls must be first or last")
        out["nulls"] = nulls
        # then_by: extra ordering columns consulted in order before the
        # arrival tie-break — the declared tie-break order.
        then_by = out.get("then_by")
        if then_by in (None, "", []):
            out.pop("then_by", None)
        else:
            if isinstance(then_by, str):
                then_by = [part.strip() for part in then_by.split(",") if part.strip()]
            if not isinstance(then_by, (list, tuple)):
                raise ShapeError(f"{where}: dedupe then_by must be a list of columns")
            normalized_tb: list[dict[str, str]] = []
            for entry in then_by:
                if isinstance(entry, Mapping):
                    col = str(entry.get("column") or "").strip()
                    direction = str(entry.get("dir") or entry.get("direction") or order_dir).strip().casefold()
                else:
                    col = str(entry).strip()
                    direction = order_dir
                if direction not in ("asc", "desc"):
                    raise ShapeError(f"{where}: dedupe then_by dir must be asc or desc")
                tnulls = (
                    str(entry.get("nulls") or nulls).strip().casefold()
                    if isinstance(entry, Mapping)
                    else nulls
                )
                if tnulls not in ("first", "last"):
                    raise ShapeError(f"{where}: dedupe then_by nulls must be first or last")
                _assert_columns_exist([col], columns, "then_by", where=where)
                normalized_tb.append(
                    {"column": col, "dir": direction, "nulls": tnulls}
                )
            if normalized_tb:
                out["then_by"] = normalized_tb
            else:
                out.pop("then_by", None)
        # tie_break: who wins when every ordering column compares equal —
        # first_seen keeps the incumbent, last_seen replaces it.
        tie_break = str(out.get("tie_break") or "first_seen").strip().casefold()
        if tie_break not in ("first_seen", "last_seen"):
            raise ShapeError(f"{where}: dedupe tie_break must be first_seen or last_seen")
        out["tie_break"] = tie_break
        # merge: 'non_null' fills the winner's null columns with the
        # best-ranked non-null member value; 'none' emits the winner as-is.
        merge = out.get("merge")
        if merge in (None, "", "none", False):
            out.pop("merge", None)
        else:
            if merge is True:
                merge = "non_null"
            merge = str(merge).strip().casefold()
            if merge != "non_null":
                raise ShapeError(f"{where}: dedupe merge must be non_null")
            out["merge"] = "non_null"
        if out.get("max_keys") not in (None, ""):
            number = _as_int(out["max_keys"], "max_keys", where=where)
            if number <= 0:
                raise ShapeError(f"{where}: max_keys must be greater than zero")
            out["max_keys"] = number
        else:
            out.pop("max_keys", None)

    if op == "aggregate":
        out["group_by"] = _optional_column_list(out, "group_by", columns, where=where)
        measures = out.get("measures")
        if not isinstance(measures, (list, tuple)) or not measures:
            raise ShapeError(f"{where}: aggregate needs a non-empty measures list")
        allowed_fns = ("sum", "avg", "count", "min", "max", "first", "last")
        normalized_measures = []
        for i, measure in enumerate(measures, start=1):
            if not isinstance(measure, Mapping):
                raise ShapeError(f"{where}: measure {i} must be an object")
            function = str(measure.get("function") or "").strip().casefold()
            if function not in allowed_fns:
                raise ShapeError(
                    f"{where}: measure {i} function '{function}' is not one of "
                    f"{', '.join(allowed_fns)}"
                )
            measure_col = str(measure.get("column") or "").strip()
            if function != "count" and not measure_col:
                raise ShapeError(f"{where}: measure {i} ({function}) needs a column")
            if measure_col:
                _assert_columns_exist([measure_col], columns, f"measure {i} column", where=where)
            alias = str(measure.get("as") or "").strip()
            if not alias:
                raise ShapeError(f"{where}: measure {i} needs an 'as' output name")
            entry = {"function": function, "as": _valid_name(alias, where=where)}
            if measure_col:
                entry["column"] = measure_col
            normalized_measures.append(entry)
        out["measures"] = normalized_measures

    if op == "sort_rows":
        order = out.get("order")
        if order in (None, ""):
            order = [
                {
                    "column": str(out.get("order_by") or "").strip(),
                    "direction": str(out.get("direction") or "asc").strip().casefold(),
                }
            ]
        if not isinstance(order, (list, tuple)) or not order:
            raise ShapeError(f"{where}: sort_rows needs an order list")
        normalized_order = []
        for i, spec in enumerate(order, start=1):
            if isinstance(spec, str):
                spec = {"column": spec, "direction": "asc"}
            if not isinstance(spec, Mapping):
                raise ShapeError(f"{where}: sort order {i} must name a column")
            col = str(spec.get("column") or "").strip()
            _assert_columns_exist([col], columns, f"sort order {i}", where=where)
            direction = str(spec.get("direction") or "asc").strip().casefold()
            if direction not in ("asc", "desc"):
                raise ShapeError(f"{where}: sort order {i} direction must be asc or desc")
            normalized_order.append({"column": col, "direction": direction})
        out["order"] = normalized_order

    if op == "rank_rows":
        order_by = str(out.get("order_by") or "").strip()
        if not order_by:
            raise ShapeError(f"{where}: rank_rows needs order_by")
        _assert_columns_exist([order_by], columns, "order_by", where=where)
        out["order_by"] = order_by
        direction = str(out.get("direction") or "top").strip().casefold()
        if direction not in ("top", "bottom"):
            raise ShapeError(f"{where}: rank_rows direction must be top or bottom")
        out["direction"] = direction
        if out.get("limit") in (None, ""):
            out["limit"] = 0  # no cap
        else:
            limit = _as_int(out["limit"], "limit", where=where)
            if limit < 0:
                raise ShapeError(f"{where}: limit must be zero (no cap) or positive")
            out["limit"] = limit
        out["partition_by"] = _optional_column_list(
            out, "partition_by", columns, where=where
        )

    if op == "lookup":
        reference = _normalize_rows_or_reference(out, where=where, op=op)
        lookup_key = str(out.get("lookup_key") or "").strip()
        if not lookup_key:
            raise ShapeError(f"{where}: lookup needs lookup_key")
        out["lookup_key"] = lookup_key
        mapping = out.get("map")
        if not isinstance(mapping, Mapping) or not mapping:
            raise ShapeError(
                f"{where}: lookup needs a non-empty map of reference column → output column"
            )
        out["map"] = {
            str(ref_col): _valid_name(str(dest_col), where=where)
            for ref_col, dest_col in mapping.items()
        }
        # A declared return-column list bounds what the reference read selects;
        # a map key outside it is a typo that would silently produce nulls.
        # lookup_key outside it is worse: every row resolves to "no match" and
        # the run silently emits nulls/diverts — refuse at parse (A1-D1).
        if reference is not None and out["reference"]["columns"]:
            declared = out["reference"]["columns"]
            if lookup_key not in declared:
                raise ShapeError(
                    f"{where}: lookup_key '{lookup_key}' is not in "
                    "reference.columns — add it or drop the column list so the "
                    "reference read can key rows"
                )
            unknown = [c for c in out["map"] if c not in declared]
            if unknown:
                raise ShapeError(
                    f"{where}: lookup map reads '{unknown[0]}' which is not in "
                    "reference.columns"
                )
        match_policy = str(out.get("match") or "first").strip().casefold()
        if match_policy not in ("first", "refuse"):
            raise ShapeError(
                f"{where}: lookup match must be first or refuse"
            )
        out["match"] = match_policy
        unmatched = str(out.get("unmatched") or "null").strip().casefold()
        if unmatched not in ("null", "drop", "divert", "default"):
            raise ShapeError(
                f"{where}: lookup unmatched must be null, drop, divert or default"
            )
        if unmatched == "default":
            defaults = out.get("defaults")
            if not isinstance(defaults, Mapping) or not defaults:
                raise ShapeError(
                    f"{where}: lookup unmatched=default needs a non-empty "
                    "defaults map of output column → value"
                )
            out["defaults"] = dict(defaults)
        out["unmatched"] = unmatched

    if op == "join":
        ref_block = _normalize_rows_or_reference(out, where=where, op=op)
        join_type = str(out.get("type") or "inner").strip().casefold()
        if join_type not in ("inner", "left"):
            raise ShapeError(f"{where}: join type must be inner or left")
        out["type"] = join_type
        on = out.get("on")
        if not isinstance(on, Mapping) or not on:
            raise ShapeError(
                f"{where}: join needs a non-empty on map of "
                "source column → reference column"
            )
        out["on"] = {str(s).strip(): str(r).strip() for s, r in on.items()}
        if any(not s or not r for s, r in out["on"].items()):
            raise ShapeError(f"{where}: join on cannot name an empty column")
        mapping = out.get("map")
        if not isinstance(mapping, Mapping) or not mapping:
            raise ShapeError(
                f"{where}: join needs a non-empty map of "
                "reference column → output column"
            )
        out["map"] = {
            str(ref_col): _valid_name(str(dest_col), where=where)
            for ref_col, dest_col in mapping.items()
        }
        fanout = str(out.get("fanout") or "refuse").strip().casefold()
        if fanout not in ("first", "refuse", "allow"):
            raise ShapeError(
                f"{where}: join fanout must be first, refuse or allow"
            )
        out["fanout"] = fanout
        unmatched = str(out.get("unmatched") or "drop").strip().casefold()
        if unmatched not in ("drop", "divert"):
            raise ShapeError(f"{where}: join unmatched must be drop or divert")
        out["unmatched"] = unmatched
        # The on/map reference columns must be inside a declared select list;
        # the on source columns must exist in a scoped recipe.
        if ref_block is not None and ref_block.get("columns"):
            declared = ref_block["columns"]
            unknown_on = [c for c in out["on"].values() if c not in declared]
            if unknown_on:
                raise ShapeError(
                    f"{where}: join on reads '{unknown_on[0]}' which is not "
                    "in reference.columns"
                )
            unknown_map = [c for c in out["map"] if c not in declared]
            if unknown_map:
                raise ShapeError(
                    f"{where}: join map reads '{unknown_map[0]}' which is not "
                    "in reference.columns"
                )
        if columns:
            missing = [c for c in out["on"] if c not in columns]
            if missing:
                raise ShapeError(
                    f"{where}: join on reads '{missing[0]}' which is not a "
                    "source column"
                )

    if op == "union":
        _normalize_rows_or_reference(out, where=where, op=op)
        mapping = out.get("map")
        if mapping is not None:
            if not isinstance(mapping, Mapping):
                raise ShapeError(
                    f"{where}: union map must be an object of "
                    "reference column → output column"
                )
            out["map"] = {
                str(ref_col): _valid_name(str(dest_col), where=where)
                for ref_col, dest_col in mapping.items()
            }
        extra = str(out.get("extra") or "refuse").strip().casefold()
        if extra not in ("refuse", "drop"):
            raise ShapeError(f"{where}: union extra must be refuse or drop")
        out["extra"] = extra
        # A declared reference.columns contract that names columns the union
        # cannot place (not an output column, not renamed by map) must refuse
        # at design time under extra=refuse — refusing at run time surfaced
        # the union error under the destination-capacity wrapper (R5).
        ref_cols = ((out.get("reference") or {}).get("columns") or [])
        if extra == "refuse" and ref_cols and columns is not None:
            map_keys = {str(k) for k in (mapping or {})}
            output_cols = {str(c) for c in columns}
            unplaced = [
                str(c)
                for c in ref_cols
                if str(c) not in output_cols and str(c) not in map_keys
            ]
            if unplaced:
                raise ShapeError(
                    f"{where}: union reference.columns declares "
                    f"{', '.join(sorted(unplaced))} — not an output column "
                    "and not renamed by union map; add it to map or set "
                    "extra=drop"
                )

    if op == "router":
        routes = out.get("routes")
        if not isinstance(routes, (list, tuple)) or not routes:
            raise ShapeError(f"{where}: router needs a non-empty routes list")
        seen_names: set[str] = set()
        default_count = 0
        norm_routes: list[dict[str, Any]] = []
        for i, raw_route in enumerate(routes):
            if not isinstance(raw_route, Mapping):
                raise ShapeError(f"{where}: route {i} must be an object")
            name = str(raw_route.get("name") or "").strip()
            if not name:
                raise ShapeError(f"{where}: route {i} needs a name")
            if name in seen_names:
                raise ShapeError(f"{where}: route '{name}' is declared twice")
            seen_names.add(name)
            is_default = bool(raw_route.get("default"))
            condition = str(raw_route.get("condition") or "").strip()
            if is_default:
                default_count += 1
                if condition:
                    raise ShapeError(
                        f"{where}: route '{name}' is the default — it takes no condition"
                    )
            elif not condition:
                raise ShapeError(
                    f"{where}: route '{name}' needs a condition or default: true"
                )
            if condition:
                try:
                    compile_expression(
                        condition,
                        known_columns=columns,
                        label=f"{where} route '{name}' condition",
                    )
                except ExpressionError as exc:
                    raise ShapeError(str(exc)) from exc
            route_map = raw_route.get("map")
            norm_route: dict[str, Any] = {
                "name": name,
                "condition": condition,
                "default": is_default,
            }
            if route_map is not None:
                if not isinstance(route_map, Mapping) or not route_map:
                    raise ShapeError(
                        f"{where}: route '{name}' map must be a non-empty object "
                        "of column → output column"
                    )
                norm_route["map"] = {
                    str(src_col): _valid_name(str(dst_col), where=where)
                    for src_col, dst_col in route_map.items()
                }
                _assert_columns_exist(
                    [str(src_col) for src_col in route_map], columns, "map", where=where
                )
            norm_routes.append(norm_route)
        if default_count > 1:
            raise ShapeError(f"{where}: only one route may be default: true")
        unmatched = str(out.get("unmatched") or "").strip().casefold()
        if unmatched:
            if unmatched not in ("divert", "fail"):
                raise ShapeError(
                    f"{where}: router unmatched must be divert or fail"
                )
            if default_count:
                raise ShapeError(
                    f"{where}: unmatched has no effect when a default route exists"
                )
            out["unmatched"] = unmatched
        elif not default_count:
            raise ShapeError(
                f"{where}: declare a default route or set unmatched — "
                "an unmatched row must go somewhere explicit"
            )
        else:
            out.pop("unmatched", None)
        targets = out.get("targets")
        if targets is not None:
            if not isinstance(targets, Mapping):
                raise ShapeError(f"{where}: router targets must be an object")
            unknown_targets = [
                str(name) for name in targets if str(name) not in seen_names
            ]
            if unknown_targets:
                raise ShapeError(
                    f"{where}: router targets name unknown route(s) "
                    f"{', '.join(sorted(unknown_targets))}"
                )
            out["targets"] = {
                str(name): _valid_name(str(table), where=where)
                for name, table in targets.items()
            }
        out["routes"] = norm_routes

    if op == "scd2":
        keys = out.get("keys")
        if not isinstance(keys, (list, tuple)) or not keys:
            raise ShapeError(f"{where}: scd2 needs a non-empty keys list")
        out["keys"] = [str(k) for k in keys]
        _assert_columns_exist(out["keys"], columns, "keys", where=where)
        version = out.get("version")
        if version is not None and str(version).strip():
            out["version"] = _valid_name(str(version), where=where)
        else:
            out.pop("version", None)
        tracked = out.get("tracked")
        if isinstance(tracked, (list, tuple)) and tracked:
            out["tracked"] = [str(t) for t in tracked]
            _assert_columns_exist(out["tracked"], columns, "tracked", where=where)
        else:
            out.pop("tracked", None)
        for flag, default in (("type1", False), ("close_missing", True)):
            raw = out.get(flag, default)
            out[flag] = (
                raw if isinstance(raw, bool)
                else str(raw).strip().lower() in ("1", "true", "yes", "on")
            )
        for field, default in (
            ("effective_from", "valid_from"),
            ("end_date", "valid_to"),
            ("is_current", "is_current"),
            ("row_hash", "row_hash"),
        ):
            value = out.get(field)
            out[field] = (
                _valid_name(str(value), where=where)
                if value is not None and str(value).strip()
                else default
            )
        stamp_names = [
            out["effective_from"],
            out["end_date"],
            out["is_current"],
            out["row_hash"],
        ]
        if out.get("version"):
            stamp_names.append(out["version"])
        if len(set(stamp_names)) != len(stamp_names):
            raise ShapeError(
                f"{where}: scd2 stamp column names must be distinct — "
                f"got {stamp_names}"
            )

    if op == "surrogate_key":
        out["start"] = _as_int(out.get("start", 1), "start", where=where)
        out["step"] = _as_int(out.get("step", 1), "step", where=where)

    if op == "nest_json":
        fields = out.get("fields")
        if not isinstance(fields, (list, tuple)) or not fields:
            raise ShapeError(f"{where}: nest_json needs a non-empty fields list")
        sources = []
        for item in fields:
            if isinstance(item, Mapping):
                source = str(item.get("from") or item.get("column") or item.get("to") or "")
            else:
                source = str(item)
            if not source:
                raise ShapeError(f"{where}: a nest_json field needs a source column")
            sources.append(source)
        _assert_columns_exist(sources, columns, "fields", where=where)

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
    if op == "aggregate":
        columns[:] = [str(c) for c in options.get("group_by", [])]
        for measure in options.get("measures", []):
            if isinstance(measure, Mapping) and measure.get("as"):
                columns.append(str(measure["as"]))
        return
    if op in ("lookup", "join", "union"):
        for target in (options.get("map") or {}).values():
            if target and str(target) not in columns:
                columns.append(str(target))
        return
    if op == "router":
        # A mapped route emits only its map targets; the shared column set
        # is the union plus the route tag so preview shows where each row went.
        for route in options.get("routes") or []:
            for target in (route.get("map") or {}).values():
                if target and str(target) not in columns:
                    columns.append(str(target))
        if ROUTE_COLUMN not in columns:
            columns.append(ROUTE_COLUMN)
        return
    if op == "surrogate_key":
        target = str(options.get("to") or "")
        if target and target not in columns:
            columns.append(target)
        return
    if op == "nest_json":
        target = str(options.get("to") or "")
        if target and target not in columns:
            columns.append(target)
        if options.get("drop"):
            fields = options.get("fields") or []
            names = [
                str(f.get("from") or f.get("column") or f.get("to") or "")
                if isinstance(f, Mapping) else str(f)
                for f in fields
            ]
            for name in names:
                if name and name in columns:
                    columns.remove(name)
        return


def _normalize_rows_or_reference(
    out: dict[str, Any], *, where: str, op: str
) -> dict[str, Any] | None:
    """Validate the rows-or-reference secondary input shared by lookup/join/union.

    Mutates ``out`` in place: ``reference`` normalizes to
    ``{connector_id, table, columns}`` and ``rows`` to a list of dicts.
    Returns the normalized reference dict, or None when inline rows are used.
    """
    rows = out.get("rows")
    reference = out.get("reference")
    if rows is not None and reference is not None:
        raise ShapeError(f"{where}: {op} takes either rows or a reference, not both")
    if reference is not None:
        if not isinstance(reference, Mapping):
            raise ShapeError(
                f"{where}: {op} reference must be an object "
                "{connector_id, table, columns?}"
            )
        connector_id = str(reference.get("connector_id") or "").strip()
        if not connector_id:
            raise ShapeError(f"{where}: {op} reference needs connector_id")
        table = str(reference.get("table") or "").strip()
        if not table:
            raise ShapeError(f"{where}: {op} reference needs table")
        ref_columns = _name_list(
            reference.get("columns"), "reference.columns", where=where
        )
        out["reference"] = {
            "connector_id": connector_id,
            "table": table,
            "columns": ref_columns,
        }
        return out["reference"]
    if not isinstance(rows, (list, tuple)) or not rows:
        raise ShapeError(
            f"{where}: {op} needs either a reference "
            "{connector_id, table} or a non-empty rows list of "
            "reference objects"
        )
    for i, ref in enumerate(rows, start=1):
        if not isinstance(ref, Mapping):
            raise ShapeError(f"{where}: {op} reference row {i} is not an object")
    out["rows"] = [dict(r) for r in rows]
    return None


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


def _optional_decimal_size(options: dict[str, Any], *, where: str) -> tuple[int | None, int | None]:
    """Precision is total digits. Scale is digits after the point. Overflow refuses."""
    raw_precision = options.get("precision")
    raw_scale = options.get("scale")
    precision = None if raw_precision in (None, "") else _as_positive_int(raw_precision, "precision", where=where)
    scale = None if raw_scale in (None, "") else _as_int(raw_scale, "scale", where=where)
    if precision is not None and precision > 38:
        raise ShapeError(f"{where}: precision must be at most 38")
    if scale is not None and (scale < 0 or scale > 38):
        raise ShapeError(f"{where}: scale must be between 0 and 38")
    if precision is not None and scale is not None and scale > precision:
        raise ShapeError(f"{where}: scale {scale} is wider than precision {precision}")
    return precision, scale


def _optional_timezone(options: dict[str, Any], *, where: str) -> None:
    """A named zone is an explicit reading of a naive clock. Blank keeps the source clock."""
    zone = str(options.get("timezone") or "").strip()
    if not zone:
        options.pop("timezone", None)
        return
    from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

    try:
        ZoneInfo(zone)
    except ZoneInfoNotFoundError as exc:
        raise ShapeError(f"{where}: '{zone}' is not a known time zone") from exc
    options["timezone"] = zone


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


def _assert_columns_exist(
    names: list[str], columns: list[str] | None, field: str, *, where: str
) -> None:
    """Refuse when a named column is not available at this point in the recipe."""
    if columns is None:
        return
    missing = [n for n in names if n and n not in columns]
    if missing:
        raise ShapeError(
            f"{where}: {field} column '{missing[0]}' is not available at this "
            f"point in the recipe. Available: {', '.join(columns[:20])}"
        )


def _name_list(value: Any, field: str, *, where: str) -> list[str] | None:
    if isinstance(value, str):
        value = [part.strip() for part in value.split(",") if part.strip()]
    if not isinstance(value, (list, tuple)):
        return None
    return [str(n).strip() for n in value if str(n).strip()]


def _required_column_list(
    options: dict[str, Any], field: str, columns: list[str] | None, *, where: str
) -> list[str]:
    names = _name_list(options.get(field), field, where=where)
    if not names:
        raise ShapeError(f"{where}: '{field}' needs at least one column name")
    _assert_columns_exist(names, columns, field, where=where)
    return names


def _optional_column_list(
    options: dict[str, Any], field: str, columns: list[str] | None, *, where: str
) -> list[str]:
    names = _name_list(options.get(field), field, where=where) or []
    _assert_columns_exist(names, columns, field, where=where)
    return names
