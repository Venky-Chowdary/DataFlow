"""Standalone, reusable row-level transform recipes — the Transform Library.

A recipe is a named, versioned, ordered list of steps applied to source rows
while a transfer loads. It exists so an operator builds a cleansing/casting
pipeline once and reuses it across transfers — the row-level counterpart of
the post-load :class:`services.transform_store.TransformProject`.

A recipe compiles (:func:`compile_recipe`) to the canonical ``shape_recipe``
and runs through :class:`services.shape_engine.ShapeEngine` — in preview, in
Validate, in Execute and in Gate-8 — so a bound recipe and a hand-authored
shape recipe are one program with one hash. The recipe is *where the
declaration lives*, never a second engine.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from services.transform_engine import normalize_engine_transform

#: What a step does when its cell cannot be transformed.
#:
#: ``fail``          — the error propagates; the transfer's normal CAST /
#:                     quarantine decision applies (default, fail-closed).
#: ``keep_original`` — the raw cell is written untransformed. Explicit in the
#:                     recipe, counted in preview, never silent.
ON_ERROR_MODES = frozenset({"fail", "keep_original"})

#: Lifecycle. ``draft`` recipes may be saved but can never bind to a transfer
#: or run; ``active`` recipes passed every check the compiler knows how to run.
#: Validation is what flips draft→active — never an operator's confidence.
RECIPE_STATUSES = frozenset({"draft", "active"})

#: A ``:name`` token inside a step option value is a recipe parameter
#: reference. Exact ``:name`` values substitute preserving the parameter's
#: type; ``:name`` inside a longer string interpolates as text.
_PARAM_TOKEN = re.compile(r"^:([A-Za-z_][A-Za-z0-9_]*)$")
_PARAM_INLINE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")

#: Engine-op → shape-op for step names that predate the shape catalog.
#: Anything absent here refuses to compile with the op named — a recipe step
#: that cannot reach the engine's canonical path must never be half-applied.
_ENGINE_OP_TO_SHAPE: dict[str, tuple[str, dict[str, Any]]] = {
    "trim": ("trim", {}),
    "upper": ("case", {"mode": "upper"}),
    "lower": ("case", {"mode": "lower"}),
    "decimal": ("cast_column", {"to_type": "decimal"}),
    "integer": ("cast_column", {"to_type": "integer"}),
    "boolean": ("cast_column", {"to_type": "boolean"}),
    "date": ("cast_column", {"to_type": "date"}),
    "datetime": ("cast_column", {"to_type": "timestamp"}),
    "string": ("cast_column", {"to_type": "text"}),
    "varchar": ("cast_column", {"to_type": "text"}),
    "text": ("cast_column", {"to_type": "text"}),
    "hash_pii": ("hash_pii", {}),
    "mask_pii": ("mask_pii", {}),
    "normalize_unicode": ("normalize_unicode", {}),
    "strip_controls": ("strip_characters", {"characters": "controls"}),
}

#: Recipe on_error → shape on_error. ``keep_original`` has no shape
#: equivalent — the shape engine's honest answers are refuse/divert/null —
#: so a keep_original step refuses to compile rather than quietly becoming
#: ``null`` and writing NULL where the recipe promised the source value.
_ON_ERROR_TO_SHAPE = {"fail": "refuse"}

#: Engine parameterized ops that map onto shape options faithfully.
#: ``assume_timezone:`` is a timestamp cast with a zone: both attach the zone
#: through ``transform_engine.attach_timezone`` (gap refused, PostgreSQL fold).
#: ``concat:`` (suffix literal) has no faithful shape step, so it refuses to
#: compile — listed here so the refusal is deliberate, not an unhandled branch.
_ENGINE_PREFIX_TO_SHAPE = (
    ("replace:", "replace"),
    ("default:", "default_if_null"),
    ("assume_timezone:", "cast_column"),
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class RecipeStep:
    """One ordered transform on one column — or one row-level step.

    ``op`` may be a shape-catalog op (``cast_column``, ``filter_rows``,
    ``derive_column``, ``concat_columns`` …) or a legacy engine name
    (``trim``, ``upper``, ``decimal`` …). ``options`` carries the op's
    arguments; ``output_column`` names the target for ops that write a new
    column (derive/concat/split/rename-to).
    """

    column: str
    op: str
    #: ``fail`` | ``keep_original`` — see ON_ERROR_MODES.
    on_error: str = "fail"
    enabled: bool = True
    options: dict[str, Any] = field(default_factory=dict)
    output_column: str = ""

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {
            "column": self.column,
            "op": self.op,
            "on_error": self.on_error,
            "enabled": self.enabled,
        }
        if self.options:
            body["options"] = dict(self.options)
        if self.output_column:
            body["output_column"] = self.output_column
        return body

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RecipeStep":
        return cls(
            column=str(data.get("column") or "").strip(),
            op=str(data.get("op") or "").strip(),
            on_error=str(data.get("on_error") or "fail").strip().lower(),
            enabled=bool(data.get("enabled", True)),
            options=dict(data.get("options") or {}),
            output_column=str(data.get("output_column") or "").strip(),
        )


@dataclass
class TransformRecipe:
    """Named, versioned, ordered row-level transform definition."""

    id: str = ""
    name: str = ""
    steps: list[RecipeStep] = field(default_factory=list)
    workspace_id: str = ""
    description: str = ""
    enabled: bool = True
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    version: int = 0
    #: ``{name: {"default": ..., "description": ...}}`` — a transfer binds
    #: values over these defaults; the definition itself never changes per
    #: transfer (parameters are the ONLY per-use override, per the
    #: reusable-vs-one-off model).
    parameters: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: The recipe's schema contract: which input columns it reads and which
    #: columns it emits. Declared, not inferred — schema drift against
    #: ``input_columns`` is what blocks a transfer at bind/run time.
    input_columns: tuple[str, ...] = ()
    output_columns: tuple[str, ...] = ()
    #: ``draft`` | ``active`` — see RECIPE_STATUSES. Recomputed on save.
    status: str = "draft"
    issues: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not self.id:
            self.id = uuid.uuid4().hex[:24]
        if self.status not in RECIPE_STATUSES:
            self.status = "draft"

    def check(self) -> list[str]:
        """Every reason this recipe cannot run, without raising.

        ``validate`` is the raising front-door (name/steps/op sanity) the
        store uses; ``check`` adds the deeper compile pass so a recipe that
        would fail at bind time is flagged at design time instead.
        """
        issues: list[str] = []
        if not (self.name or "").strip():
            issues.append("Transform recipe needs a name")
        if not self.steps:
            issues.append("Transform recipe needs at least one step")
            return issues
        for i, step in enumerate(self.steps, start=1):
            from services.shape_models import STEP_CATALOG

            if step.op in STEP_CATALOG:
                definition = STEP_CATALOG[step.op]
                if definition.needs_column and not step.column:
                    issues.append(f"step {i}: column is required")
                for req in definition.required:
                    if step.options.get(req) in (None, "") and req != "to":
                        issues.append(
                            f"step {i}: '{step.op}' requires option {req!r}"
                        )
                if "to" in definition.required and not (
                    step.output_column or step.options.get("to")
                ):
                    issues.append(
                        f"step {i}: '{step.op}' requires an output column"
                    )
            else:
                if not step.column:
                    issues.append(f"step {i}: column is required")
                if normalize_engine_transform(step.op) is None:
                    issues.append(f"step {i}: unknown transform {step.op!r}")
            if step.on_error not in ON_ERROR_MODES:
                issues.append(
                    f"step {i}: on_error must be one of {sorted(ON_ERROR_MODES)}"
                )
        if not issues:
            compile_issue = self._compile_probe()
            if compile_issue:
                issues.append(compile_issue)
        return issues

    def _compile_probe(self) -> str:
        """Compile the recipe the same way a bind does, with declared defaults.

        Parameters without a declared default stand in with ``"1"`` — a probe
        value that passes numeric and column-name checks — so a parameterized
        recipe still gets the deep check instead of skipping it. A step whose
        options the shape engine refuses (a bad character class, an unknown
        aggregate function, a malformed lookup) is reported here instead of
        surprising the first transfer that binds the recipe.
        """
        supplied = {
            name: spec["default"] if isinstance(spec, dict) and "default" in spec else "1"
            for name, spec in self.parameters.items()
        }
        try:
            bound = self.bind_parameters(supplied)
            payload = {"steps": [_step_to_shape_wire(s) for s in bound.steps]}
            from services.shape_models import ShapeRecipe

            ShapeRecipe.parse(payload)
        except Exception as exc:
            return f"compile check: {exc}"
        return ""

    def refresh_status(self) -> "TransformRecipe":
        """Recompute ``issues`` + ``status`` from ``check`` — called on save."""
        self.issues = self.check()
        self.status = "active" if not self.issues else "draft"
        return self

    def validate(self) -> None:
        issues = self.check()
        if issues:
            prefix = "Transform recipe" if not self.steps or not self.name else "Invalid recipe"
            raise ValueError(f"{prefix}: " + "; ".join(issues))

    def bind_parameters(
        self, params: dict[str, Any] | None = None
    ) -> "TransformRecipe":
        """A copy of this recipe with ``:param`` tokens resolved.

        Resolution order: supplied value → declared default → error. A
        parameter with no default and no supplied value makes the recipe
        unrunnable — the error names the missing parameter, the transfer
        that forgets it never runs a half-bound recipe.
        """
        supplied = dict(params or {})
        bound: dict[str, Any] = {}
        missing: list[str] = []
        for name, spec in self.parameters.items():
            spec = spec or {}
            if name in supplied:
                bound[name] = supplied[name]
            elif "default" in spec:
                bound[name] = spec["default"]
            else:
                missing.append(name)
        undeclared = sorted(set(supplied) - set(self.parameters))
        if missing or undeclared:
            parts = []
            if missing:
                parts.append(
                    f"missing value(s) for parameter(s) {', '.join(sorted(missing))}"
                )
            if undeclared:
                parts.append(
                    f"undeclared parameter(s) {', '.join(undeclared)} — "
                    "declare them on the recipe before a transfer can pass them"
                )
            raise ValueError(
                f"Transform recipe '{self.name}' cannot bind: " + "; ".join(parts)
            )
        recipe = TransformRecipe.from_dict(self.to_dict())
        for step in recipe.steps:
            step.options = {
                k: _substitute_params(v, bound) for k, v in step.options.items()
            }
            if step.output_column:
                resolved = _substitute_params(step.output_column, bound)
                step.output_column = str(resolved)
            if step.column and str(step.column).startswith(":"):
                resolved = _substitute_params(str(step.column), bound)
                step.column = str(resolved)
        return recipe

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "steps": [s.to_dict() for s in self.steps],
            "workspace_id": self.workspace_id,
            "description": self.description,
            "enabled": self.enabled,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "version": self.version,
            "parameters": {k: dict(v or {}) for k, v in self.parameters.items()},
            "input_columns": list(self.input_columns),
            "output_columns": list(self.output_columns),
            "status": self.status,
            "issues": list(self.issues),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransformRecipe":
        return cls(
            id=str(data.get("id") or ""),
            name=str(data.get("name") or ""),
            steps=[RecipeStep.from_dict(s) for s in (data.get("steps") or [])],
            workspace_id=str(data.get("workspace_id") or ""),
            description=str(data.get("description") or ""),
            enabled=bool(data.get("enabled", True)),
            created_at=str(data.get("created_at") or _now()),
            updated_at=str(data.get("updated_at") or _now()),
            version=int(data.get("version") or 0),
            parameters={
                str(k): dict(v or {})
                for k, v in (data.get("parameters") or {}).items()
            },
            input_columns=tuple(str(c) for c in (data.get("input_columns") or ())),
            output_columns=tuple(str(c) for c in (data.get("output_columns") or ())),
            status=str(data.get("status") or "draft"),
            issues=[str(i) for i in (data.get("issues") or [])],
        )


def _substitute_params(value: Any, bound: dict[str, Any]) -> Any:
    """Resolve ``:param`` tokens in one option value against bound parameters."""
    if isinstance(value, dict):
        return {k: _substitute_params(v, bound) for k, v in value.items()}
    if isinstance(value, list):
        out: list[Any] = []
        for v in value:
            resolved = _substitute_params(v, bound)
            # A :param bound to a list splices into a list option — otherwise
            # 'keys': [':pk_columns'] would nest one list inside another.
            if isinstance(resolved, list):
                out.extend(resolved)
            else:
                out.append(resolved)
        return out
    if not isinstance(value, str):
        return value
    exact = _PARAM_TOKEN.match(value)
    if exact:
        name = exact.group(1)
        return bound.get(name, value)
    if ": " in value:
        return value
    return _PARAM_INLINE.sub(
        lambda m: str(bound[m.group(1)]) if m.group(1) in bound else m.group(0),
        value,
    )


def _step_to_shape_wire(step: RecipeStep) -> dict[str, Any]:
    """One recipe step → one shape-catalog wire step.

    Refuses (raises ValueError naming the step) when the op cannot reach the
    shape engine faithfully — a recipe that half-compiles is worse than one
    that refuses to bind.
    """
    from services.shape_models import STEP_CATALOG

    options = dict(step.options)
    if step.output_column and "to" not in options:
        options["to"] = step.output_column

    if step.op in STEP_CATALOG:
        op = step.op
    else:
        prefix_hit = next(
            (p for p, _ in _ENGINE_PREFIX_TO_SHAPE if step.op.startswith(p)),
            None,
        )
        if prefix_hit is not None:
            arg = step.op[len(prefix_hit) :]
            if prefix_hit == "replace:":
                search, _, repl = arg.partition(":")
                op = "replace"
                options = {"search": search, "replacement": repl}
            elif prefix_hit == "assume_timezone:":
                op = "cast_column"
                options = {"to_type": "timestamp", "timezone": arg.strip()}
            else:  # default:
                op = "default_if_null"
                options = {"value": arg}
        elif (canonical := normalize_engine_transform(step.op)) in _ENGINE_OP_TO_SHAPE:
            # Aliases (``to_decimal`` → ``decimal``) map through their canonical
            # engine id — the faithful shape op is the same one.
            op, defaults = _ENGINE_OP_TO_SHAPE[canonical]
            options = {**defaults, **options}
        elif canonical is not None:
            raise ValueError(
                f"recipe step on '{step.column}' uses engine transform "
                f"{step.op!r}, which has no faithful shape-step equivalent — "
                "express it with a shape-catalog op (e.g. cast_column, "
                "derive_column) so the transfer path and the preview agree"
            )
        else:
            raise ValueError(f"unknown transform {step.op!r}")

    wire: dict[str, Any] = {"op": op, "options": options}
    if step.column:
        wire["column"] = step.column
    if not step.enabled:
        wire["enabled"] = False
    mapped_error = _ON_ERROR_TO_SHAPE.get(step.on_error)
    if mapped_error is None:
        raise ValueError(
            f"recipe step {step.op!r} on '{step.column}' uses on_error "
            f"'{step.on_error}', which the transfer engine cannot honor — "
            "keep_original is a preview concept; a bound recipe must choose "
            "'fail' (refuse/quarantine) for the row or remove the step"
        )
    if mapped_error != "refuse":
        wire["on_error"] = mapped_error
    return wire


def compile_recipe(
    recipe: TransformRecipe,
    *,
    source_columns: list[str] | None = None,
    params: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], Any]:
    """Compile a recipe to the canonical shape payload + parsed ShapeRecipe.

    Returns ``(payload, shape_recipe)``. Raises ValueError when the recipe is
    not runnable — a draft (invalid) recipe never reaches this point in
    production, and a parameter or op failure surfaces here with its name.

    ``source_columns`` is the read's declared column set; when the recipe
    declares ``input_columns``, any contract column missing from the source
    fails the bind with the drift named.
    """
    issues = recipe.check()
    if issues:
        raise ValueError(
            f"Transform recipe '{recipe.name}' is not runnable: "
            + "; ".join(issues)
        )
    bound = recipe.bind_parameters(params)

    if recipe.input_columns and source_columns is not None:
        missing = sorted(set(recipe.input_columns) - set(source_columns))
        if missing:
            raise ValueError(
                f"Transform recipe '{recipe.name}' expects input column(s) "
                f"{', '.join(missing)} that the source does not carry — "
                "re-validate the recipe against the source as it is now"
            )

    from services.shape_models import ShapeRecipe

    payload: dict[str, Any] = {
        "steps": [
            _step_to_shape_wire(s) for s in bound.steps if s.enabled or True
        ],
    }
    if recipe.input_columns:
        payload["input_columns"] = list(recipe.input_columns)
    if recipe.output_columns:
        payload["output_columns"] = list(recipe.output_columns)
    shape = ShapeRecipe.parse(payload, source_columns=source_columns)
    return payload, shape


def _preview_via_engine(
    recipe: TransformRecipe,
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
    per_step: bool,
) -> dict[str, Any]:
    """Preview through the same ShapeEngine the transfer uses.

    A previewed row and a written row cannot disagree — both go through
    ``ShapeRecipe.parse`` → ``ShapeEngine``. ``per_step`` replays each
    step-prefix so the UI can show the row *as of that step*, the
    Informatica per-transformation preview pattern.
    """
    from services.shape_engine import ShapeEngine, ShapeRowError
    from services.shape_models import ShapeRecipe

    wires = list(payload.get("steps") or [])
    engines: dict[int, ShapeEngine] = {}
    if per_step:
        for k in range(len(wires)):
            engines[k] = ShapeEngine(
                ShapeRecipe.parse({"steps": wires[: k + 1]})
            )
    full = ShapeEngine(ShapeRecipe.parse(payload))

    results: list[dict[str, Any]] = []
    failed = 0
    removed = 0
    buffered = 0
    for row in rows:
        errors: list[dict[str, Any]] = []
        buffered_before = full.effect.rows_buffered
        diverted_before = full.effect.rows_diverted
        try:
            outs = full.apply_records(dict(row))
            after = outs[0] if outs else None
        except ShapeRowError as exc:
            outs = None
            after = dict(row)
            errors.append({"step": None, "error": str(exc)})
        failed += int(bool(errors))
        removed += int(outs == [])

        entry: dict[str, Any] = {"before": row, "after": after, "errors": errors}
        held = full.effect.rows_buffered > buffered_before
        if held:
            # A dedupe/aggregate/sort/rank step keeps the row until the
            # end of the sample — "held", not "removed".
            entry["held"] = True
            buffered += 1
            removed -= int(outs == [])
        elif full.effect.rows_diverted > diverted_before:
            entry["diverted"] = True
            removed -= int(outs == [])
        elif outs == []:
            entry["removed"] = True
        if per_step:
            trace: list[dict[str, Any]] = []
            for k, wire in enumerate(wires):
                step_errors: list[str] = []
                step_buffered_before = engines[k].effect.rows_buffered
                try:
                    stepped = engines[k].apply_records(dict(row))
                except ShapeRowError as exc:
                    step_errors.append(str(exc))
                    stepped = None
                trace.append(
                    {
                        "step": k,
                        "op": wire.get("op"),
                        "column": wire.get("column") or "",
                        "after": stepped[0] if stepped else None,
                        "removed": stepped == [] and engines[k].effect.rows_buffered == step_buffered_before,
                        "held": engines[k].effect.rows_buffered > step_buffered_before,
                        "errors": step_errors,
                    }
                )
            entry["steps"] = trace
        results.append(entry)

    # End of the sample is the stream's end: drain every buffered step so a
    # dedupe/aggregate/sort recipe shows the rows it actually emits.
    drained = full.drain()
    for row in drained:
        results.append({"before": None, "after": row, "errors": [], "drained": True})
    return {
        "rows": len(rows),
        "rows_with_errors": failed,
        "rows_removed": removed,
        "rows_buffered": buffered,
        "rows_emitted_after_drain": len(drained),
        "results": results,
        "compiled": True,
    }


def preview_recipe(
    recipe: TransformRecipe,
    rows: list[dict[str, Any]],
    *,
    params: dict[str, Any] | None = None,
    per_step: bool = False,
) -> dict[str, Any]:
    """Dry-run a recipe over sample rows — before/after plus error counts.

    The preview runs the compiled recipe through the shape engine the
    transfer runs — one composition model. A recipe that does not compile
    cannot bind to a transfer, so it previews nothing: the refusal is the
    answer (a per-cell fallback used to show output no run could produce).
    """
    try:
        payload, _shape = compile_recipe(recipe, params=params)
    except ValueError as exc:
        return {
            "rows": len(rows),
            "rows_with_errors": 0,
            "results": [],
            "compiled": False,
            "error": str(exc),
        }
    return _preview_via_engine(recipe, payload, rows, per_step)
