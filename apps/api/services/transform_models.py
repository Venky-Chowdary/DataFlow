"""Post-load SQL transformation models — definition, dependency graph, planning.

Datawrap moved data well and then stopped. Everything downstream of the load —
the "T" that Airbyte delegates to dbt and Fivetran ships as dbt Core plus
Quickstart models — did not exist at all. An operator who landed raw orders and
wanted a daily revenue rollup had to leave the product.

This module owns *what* a transformation is and *in what order* models run.
:mod:`services.transform_runner` owns *how* they execute against a destination.
Keeping those apart matters: planning is pure and exhaustively testable without
a database, which is what lets the ordering guarantees below be proven rather
than asserted.

Design decisions worth stating, because they are the ones a reviewer will
question:

* ``ref('name')`` is the only way to declare a dependency, exactly as dbt does.
  A model that hardcodes a physical table name is not a dependency the planner
  can see, so ordering silently breaks. :func:`extract_refs` therefore also
  reports refs to models that do not exist rather than dropping them.
* Cycles are a hard error at plan time, never at execution time. Discovering a
  cycle halfway through a run leaves the destination in a partially transformed
  state that nothing describes.
* The plan is *layered*, not merely topologically sorted. Models within a layer
  have no dependency on each other and may run concurrently; a flat sort throws
  that information away and forces serial execution.
"""

from __future__ import annotations

import graphlib
import re
from dataclasses import dataclass, field
from typing import Any, Iterable

#: How a model's result is persisted at the destination.
#:
#: ``view``        - no data movement; recomputed on read. Cheapest, always fresh.
#: ``table``       - full rebuild each run. Simple and always correct.
#: ``incremental`` - append/merge only new rows. Needs a unique key to be
#:                   idempotent, or a re-run duplicates data.
#: ``ephemeral``   - never materialized; inlined into dependents as a CTE.
MATERIALIZATIONS = frozenset({"view", "table", "incremental", "ephemeral"})

#: Incremental strategies. ``merge`` requires a unique key and is idempotent.
#: ``append`` is at-least-once and will duplicate on re-run — callers must say
#: so out loud rather than letting an operator assume otherwise.
INCREMENTAL_STRATEGIES = frozenset({"merge", "append", "delete_insert"})

_REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([A-Za-z0-9_]+)['\"]\s*\)\s*\}\}")
_SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(\s*['\"]([A-Za-z0-9_.]+)['\"]\s*\)\s*\}\}"
)
_MODEL_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,62}$")

# Statements a model body must never contain. A model is a SELECT that the
# runner wraps; letting it carry its own DDL/DML means the materialization
# contract is a lie and an operator can drop a table from inside a "view".
_FORBIDDEN_STATEMENTS = (
    "drop ",
    "truncate ",
    "alter ",
    "grant ",
    "revoke ",
    "create ",
    "insert ",
    "update ",
    "delete ",
    "merge ",
    "call ",
    "execute ",
    "attach ",
    "copy ",
    "vacuum ",
)


class TransformDefinitionError(ValueError):
    """A model is malformed — raised at definition or plan time, never mid-run."""


class TransformCycleError(TransformDefinitionError):
    """Models form a dependency cycle.

    Carries the participating model names so the operator gets the actual
    cycle instead of "graph contains a cycle".
    """

    def __init__(self, cycle: list[str]) -> None:
        self.cycle = cycle
        arrow = " -> ".join(cycle)
        super().__init__(
            f"Transformation models form a dependency cycle: {arrow}. "
            "Break the cycle by removing one ref() or splitting a model."
        )


def extract_refs(sql: str) -> list[str]:
    """Model names referenced by ``{{ ref('name') }}``, in first-seen order.

    Order is preserved and duplicates removed so error messages and plans are
    deterministic; a set would make test output depend on hash seed.
    """
    seen: list[str] = []
    for match in _REF_PATTERN.finditer(sql or ""):
        name = match.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def extract_sources(sql: str) -> list[str]:
    """Raw landed tables referenced by ``{{ source('table') }}``."""
    seen: list[str] = []
    for match in _SOURCE_PATTERN.finditer(sql or ""):
        name = match.group(1)
        if name not in seen:
            seen.append(name)
    return seen


def _reject_unsafe_sql(name: str, sql: str) -> None:
    """Fail a model whose body is not a bare SELECT.

    Checked against a statement-stripped copy so a legitimate column named
    ``created_at`` or a string literal containing 'delete ' cannot trip it,
    while a real trailing ``; DROP TABLE`` cannot hide either.
    """
    body = (sql or "").strip()
    if not body:
        raise TransformDefinitionError(f"Model '{name}' has an empty SQL body.")

    stripped = _strip_sql_noise(body)
    if ";" in stripped.rstrip(";"):
        raise TransformDefinitionError(
            f"Model '{name}' contains multiple statements. A model is one SELECT; "
            "split it into separate models."
        )

    lowered = stripped.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise TransformDefinitionError(
            f"Model '{name}' must start with SELECT or WITH. The runner supplies "
            "the CREATE/INSERT itself so the materialization it reports is the "
            "one that actually ran."
        )

    padded = f" {lowered} "
    for statement in _FORBIDDEN_STATEMENTS:
        if f" {statement}" in padded:
            raise TransformDefinitionError(
                f"Model '{name}' contains a '{statement.strip().upper()}' statement. "
                "Models are read-only SELECTs; the runner owns all writes."
            )


def _strip_sql_noise(sql: str) -> str:
    """Remove string literals and comments so keyword scanning is honest."""
    out = []
    i = 0
    length = len(sql)
    while i < length:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < length else ""
        if ch == "-" and nxt == "-":
            i = sql.find("\n", i)
            if i == -1:
                break
            continue
        if ch == "/" and nxt == "*":
            end = sql.find("*/", i + 2)
            i = length if end == -1 else end + 2
            continue
        if ch in ("'", '"'):
            quote = ch
            i += 1
            while i < length:
                if sql[i] == "\\":
                    i += 2
                    continue
                if sql[i] == quote:
                    i += 1
                    break
                i += 1
            out.append(" ")
            continue
        out.append(ch)
        i += 1
    return "".join(out)


@dataclass
class DataTest:
    """A dbt-style generic test, evaluated as pushdown SQL by the runner.

    ``severity`` mirrors the expectations engine already in the codebase so the
    two produce comparable verdicts: ``error`` fails the run, ``warn`` reports.
    """

    test_type: str
    column: str = ""
    severity: str = "error"
    #: accepted_values
    values: list[str] = field(default_factory=list)
    #: relationships
    to_model: str = ""
    to_column: str = ""

    VALID_TYPES = frozenset(
        {"unique", "not_null", "accepted_values", "relationships", "positive"}
    )

    def validate(self, model_name: str) -> None:
        if self.test_type not in self.VALID_TYPES:
            raise TransformDefinitionError(
                f"Model '{model_name}' declares unknown test '{self.test_type}'. "
                f"Supported: {', '.join(sorted(self.VALID_TYPES))}."
            )
        if self.test_type != "relationships" and not self.column:
            raise TransformDefinitionError(
                f"Model '{model_name}' test '{self.test_type}' needs a column."
            )
        if self.severity not in {"error", "warn"}:
            raise TransformDefinitionError(
                f"Model '{model_name}' test severity must be 'error' or 'warn'."
            )
        if self.test_type == "accepted_values" and not self.values:
            raise TransformDefinitionError(
                f"Model '{model_name}' accepted_values test needs a value list."
            )
        if self.test_type == "relationships" and not (self.to_model and self.to_column):
            raise TransformDefinitionError(
                f"Model '{model_name}' relationships test needs to_model and to_column."
            )
        if self.column:
            require_safe_column(self.column, model_name)
        if self.to_column:
            require_safe_column(self.to_column, model_name)

    def to_dict(self) -> dict[str, Any]:
        return {
            "test_type": self.test_type,
            "column": self.column,
            "severity": self.severity,
            "values": list(self.values),
            "to_model": self.to_model,
            "to_column": self.to_column,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "DataTest":
        return cls(
            test_type=str(data.get("test_type") or data.get("type") or ""),
            column=str(data.get("column") or ""),
            severity=str(data.get("severity") or "error"),
            values=[str(v) for v in (data.get("values") or [])],
            to_model=str(data.get("to_model") or ""),
            to_column=str(data.get("to_column") or ""),
        )


def require_safe_column(column: str, model_name: str) -> str:
    """Column references in tests are interpolated into SQL — validate hard."""
    if not _MODEL_NAME_PATTERN.match(column or ""):
        raise TransformDefinitionError(
            f"Model '{model_name}' references an invalid column name "
            f"{column!r}. Use letters, digits and underscores."
        )
    return column


@dataclass
class TransformModel:
    """One SQL model: a name, a SELECT, and how to persist its result."""

    name: str
    sql: str
    materialization: str = "view"
    description: str = ""
    #: incremental only
    unique_key: str = ""
    incremental_strategy: str = "merge"
    tests: list[DataTest] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    enabled: bool = True

    def __post_init__(self) -> None:
        self.name = (self.name or "").strip()
        self.materialization = (self.materialization or "view").strip().lower()
        self.incremental_strategy = (
            self.incremental_strategy or "merge"
        ).strip().lower()
        self.validate()

    def validate(self) -> None:
        if not _MODEL_NAME_PATTERN.match(self.name):
            raise TransformDefinitionError(
                f"Model name {self.name!r} is invalid. Use a letter followed by "
                "letters, digits or underscores (max 63 characters) — the name "
                "becomes a relation at the destination."
            )
        if self.materialization not in MATERIALIZATIONS:
            raise TransformDefinitionError(
                f"Model '{self.name}' has unknown materialization "
                f"{self.materialization!r}. Supported: "
                f"{', '.join(sorted(MATERIALIZATIONS))}."
            )
        if self.incremental_strategy not in INCREMENTAL_STRATEGIES:
            raise TransformDefinitionError(
                f"Model '{self.name}' has unknown incremental strategy "
                f"{self.incremental_strategy!r}. Supported: "
                f"{', '.join(sorted(INCREMENTAL_STRATEGIES))}."
            )
        if self.materialization == "incremental":
            if self.incremental_strategy in {"merge", "delete_insert"} and not self.unique_key:
                raise TransformDefinitionError(
                    f"Model '{self.name}' is incremental with strategy "
                    f"'{self.incremental_strategy}', which needs a unique_key to "
                    "be idempotent. Without one a re-run duplicates rows; declare "
                    "the key or use the 'append' strategy and accept "
                    "at-least-once."
                )
            if self.unique_key:
                require_safe_column(self.unique_key, self.name)
        _reject_unsafe_sql(self.name, self.sql)
        for test in self.tests:
            test.validate(self.name)

    @property
    def refs(self) -> list[str]:
        return extract_refs(self.sql)

    @property
    def sources(self) -> list[str]:
        return extract_sources(self.sql)

    @property
    def is_materialized(self) -> bool:
        """Ephemeral models produce no relation, so nothing can be tested on them."""
        return self.materialization != "ephemeral"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "sql": self.sql,
            "materialization": self.materialization,
            "description": self.description,
            "unique_key": self.unique_key,
            "incremental_strategy": self.incremental_strategy,
            "tests": [t.to_dict() for t in self.tests],
            "tags": list(self.tags),
            "enabled": self.enabled,
            "refs": self.refs,
            "sources": self.sources,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "TransformModel":
        return cls(
            name=str(data.get("name") or ""),
            sql=str(data.get("sql") or ""),
            materialization=str(data.get("materialization") or "view"),
            description=str(data.get("description") or ""),
            unique_key=str(data.get("unique_key") or ""),
            incremental_strategy=str(data.get("incremental_strategy") or "merge"),
            tests=[DataTest.from_dict(t) for t in (data.get("tests") or [])],
            tags=[str(t) for t in (data.get("tags") or [])],
            enabled=bool(data.get("enabled", True)),
        )


@dataclass
class TransformPlan:
    """An ordered, layered execution plan over a set of models."""

    #: Each layer's models have no dependency on one another and may run
    #: concurrently. Layer N may depend only on layers < N.
    layers: list[list[str]]
    models: dict[str, TransformModel]
    #: refs pointing at models that are not defined or not enabled.
    unresolved_refs: dict[str, list[str]]

    @property
    def order(self) -> list[str]:
        """Flat topological order — the serial equivalent of :attr:`layers`."""
        return [name for layer in self.layers for name in layer]

    @property
    def max_parallelism(self) -> int:
        return max((len(layer) for layer in self.layers), default=0)

    def to_dict(self) -> dict[str, Any]:
        return {
            "layers": [list(layer) for layer in self.layers],
            "order": self.order,
            "model_count": len(self.models),
            "layer_count": len(self.layers),
            "max_parallelism": self.max_parallelism,
            "unresolved_refs": {k: list(v) for k, v in self.unresolved_refs.items()},
        }


def build_plan(
    models: Iterable[TransformModel],
    *,
    select: Iterable[str] | None = None,
) -> TransformPlan:
    """Resolve dependencies and return a layered execution plan.

    ``select`` restricts the run to the named models **and everything they
    depend on**. Running a model without its upstream would read a stale or
    absent relation, so the closure is computed rather than trusting the
    caller's list.

    Raises :class:`TransformCycleError` on a cycle and
    :class:`TransformDefinitionError` on duplicate names. Both are plan-time
    failures by design — finding either mid-run leaves the destination in a
    state nothing describes.
    """
    by_name: dict[str, TransformModel] = {}
    for model in models:
        if model.name in by_name:
            raise TransformDefinitionError(
                f"Duplicate model name '{model.name}'. Model names become "
                "relations at the destination and must be unique."
            )
        by_name[model.name] = model

    enabled = {name: m for name, m in by_name.items() if m.enabled}

    if select is not None:
        wanted = _dependency_closure(enabled, select)
        enabled = {n: m for n, m in enabled.items() if n in wanted}

    # Fail closed on missing refs. Substituting a typo'd ``ref('ordrs')`` with
    # a physical relation of that name would silently read (or create) the
    # wrong table — that is worse than refusing to plan. Disabled upstreams
    # count as missing for the same reason: the relation will not be built.
    unresolved: dict[str, list[str]] = {}
    for name, model in enabled.items():
        missing = [r for r in model.refs if r not in enabled]
        if missing:
            unresolved[name] = missing
    if unresolved:
        parts = [
            f"{model} → {', '.join(refs)}" for model, refs in sorted(unresolved.items())
        ]
        raise TransformDefinitionError(
            "Unresolved model ref(s): "
            + "; ".join(parts)
            + ". Every {{ ref('name') }} must name an enabled model in this project."
        )

    graph = {
        name: {r for r in model.refs if r in enabled}
        for name, model in enabled.items()
    }

    sorter = graphlib.TopologicalSorter(graph)
    try:
        sorter.prepare()
    except graphlib.CycleError as exc:
        # CycleError args are (message, cycle_nodes); the node list is the part
        # an operator can act on.
        cycle = list(exc.args[1]) if len(exc.args) > 1 else []
        raise TransformCycleError(cycle) from exc

    layers: list[list[str]] = []
    while sorter.is_active():
        ready = sorter.get_ready()
        if not ready:
            break
        # Sorted so the plan is reproducible across runs; graphlib does not
        # guarantee ordering within a ready group.
        layers.append(sorted(ready))
        for node in ready:
            sorter.done(node)

    return TransformPlan(layers=layers, models=enabled, unresolved_refs={})


def _dependency_closure(
    models: dict[str, TransformModel], select: Iterable[str]
) -> set[str]:
    """Selected models plus every model they transitively depend on."""
    wanted: set[str] = set()
    stack = [str(s).strip() for s in select if str(s).strip()]
    while stack:
        name = stack.pop()
        if name in wanted or name not in models:
            continue
        wanted.add(name)
        stack.extend(models[name].refs)
    return wanted


def describe_plan(plan: TransformPlan) -> str:
    """Operator-readable one-liner for logs and job summaries."""
    if not plan.layers:
        return "no models to run"
    parts = [
        f"layer {i + 1}: {', '.join(layer)}" for i, layer in enumerate(plan.layers)
    ]
    return " | ".join(parts)
