"""The approved recipe's image of a re-read boundary row (D359).

The tie dedupe of an incremental run sees the page the source handed over;
the destination holds the recipe's output. A keyless append compares every
column, so a transformed column (``name`` trimmed and lowered) must be shaped
before it is compared, or the watermark row never matches its stored copy and
is appended again on every run.

Three facts about a recipe decide how a single re-read row is imaged:

* most steps are row-local: one row in, one row out (or none, for a filter);
* ``sort_rows`` / ``dedupe_rows`` hold rows until the stream drains, but a
  single row drains as itself, so each peer is shaped on its own fork and
  drained;
* ``aggregate`` and ``rank_rows`` compute a row from the whole population, and
  an expanding step can turn one row into several: there is no per-row image,
  so a comparison that needs one is refused, naming the step;
* ``run_timestamp`` stamps the run's start, and a probe would stamp *this*
  run's start while the stored copy carries the earlier run's: those columns
  (and anything a later step derives from them) are left out of the match.
"""

from __future__ import annotations

from typing import Any

from .stream_boundary_read import BoundaryStructural

#: Steps whose output row depends on rows other than the one shaped.
_NO_ROW_IMAGE_OPS = frozenset({"aggregate", "rank_rows"})
#: Steps that stamp a value not derived from the row (the run's start).
_RUN_STAMPED_OPS = frozenset({"run_timestamp"})


class RecipeBoundaryStructural(BoundaryStructural):
    """The recipe has no per-row image: the message carries its own remedy."""


def _recipe(shape_runner: Any) -> Any:
    return getattr(shape_runner, "recipe", None) if shape_runner is not None else None


def _step_reads(step: Any) -> set[str]:
    from services.shape_models import _READ_OPTION_KEYS

    out: set[str] = set()

    def _add(value: Any) -> None:
        if isinstance(value, dict):
            out.update(str(k) for k in value)
            if value.get("column"):
                out.add(str(value["column"]))
        elif isinstance(value, (list, tuple)):
            for item in value:
                _add(item)
        elif isinstance(value, str) and value.strip():
            out.add(value.strip())

    _add(step.column)
    if step.expression is not None:
        out.update(str(c) for c in step.expression.columns)
    for key in _READ_OPTION_KEYS:
        _add(step.options.get(key))
    return out


def run_stamped_columns(shape_runner: Any) -> set[str]:
    """Recipe output columns that carry the run's own stamp, directly or derived."""
    recipe = _recipe(shape_runner)
    stamped: set[str] = set()
    for step in getattr(recipe, "enabled_steps", ()) or ():
        if step.op in _RUN_STAMPED_OPS or (stamped and _step_reads(step) & stamped):
            stamped.update(str(c) for c in step.writes if c)
    return stamped


def run_stamped_targets(shape_runner: Any, mappings: list[dict] | None) -> set[str]:
    """The destination columns :func:`run_stamped_columns` land in."""
    stamped = run_stamped_columns(shape_runner)
    if not stamped:
        return set()
    out = set()
    for m in mappings or []:
        if isinstance(m, dict) and str(m.get("source") or "") in stamped and m.get("target"):
            out.add(str(m["target"]))
    return out | stamped


def recipe_touches(shape_runner: Any, mappings: list[dict] | None, targets: list[str]) -> bool:
    """Whether any recipe step writes a column these destination columns come from."""
    recipe = _recipe(shape_runner)
    if recipe is None:
        return False
    touched = set(recipe.touched_columns)
    to_source = {
        str(m.get("target") or ""): str(m.get("source") or "")
        for m in mappings or []
        if isinstance(m, dict) and m.get("target")
    }
    return any(to_source.get(t, t) in touched or t in touched for t in targets)


def _no_image_message(index: int, op: str) -> str:
    return (
        f"Step {index} ({op}) of this route's transform recipe computes each written row "
        "from many source rows, so a re-read row at the stored watermark has no single "
        "image to compare with its stored copy, and an append could write it twice. "
        "Run this route as upsert on a key, or as full_refresh_overwrite, move that "
        "step out of the recipe, or record the route's acknowledgement 'accept "
        "possible boundary duplicates'."
    )


def shape_boundary_peers(
    shape_runner: Any, headers: list[str], rows: list[list[Any]]
) -> tuple[list[str], list[list[Any] | None]]:
    """The re-read peers as the write sees them, aligned with ``rows``.

    ``None`` where the recipe removes the row (the write drops it too, so it is
    never "already landed"). The run's own effect ledger is untouched: every
    image comes from a fork of the runner.
    """
    recipe = _recipe(shape_runner)
    if recipe is None or not rows:
        return list(headers or []), list(rows)
    steps = list(recipe.enabled_steps)
    for index, step in enumerate(steps, start=1):
        if step.op in _NO_ROW_IMAGE_OPS:
            raise RecipeBoundaryStructural(_no_image_message(index, step.op))
    buffered = any(step.definition.buffered for step in steps)
    out_headers = list(headers or [])
    shaped: list[list[Any] | None] = []
    probe = None
    for row in rows:
        if probe is None or buffered:
            # A buffering step keeps state across rows: each peer gets its own.
            probe = shape_runner.fork()
        out_headers, got = probe.matrix(list(headers or []), [row])
        if buffered:
            tail_headers, tail = probe.drain_matrix()
            if tail:
                out_headers = tail_headers
                got = [*got, *tail]
        if len(got) > 1:
            index, step = next(
                ((i, s) for i, s in enumerate(steps, start=1) if s.definition.expands),
                (0, None),
            )
            raise RecipeBoundaryStructural(
                _no_image_message(index, step.op) if step is not None
                else _no_image_message(len(steps), steps[-1].op)
            )
        shaped.append(got[0] if got else None)
    return out_headers, shaped


def refusal_text(exc: BaseException) -> str:
    """The operator text for a structural boundary refusal."""
    if isinstance(exc, RecipeBoundaryStructural):
        return str(exc)
    from .stream_boundary_keyed import _refused_message

    return _refused_message(str(exc))
