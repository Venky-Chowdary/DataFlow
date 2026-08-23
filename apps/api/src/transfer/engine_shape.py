"""Shape-recipe orchestration for the transfer engine.

Split out of ``engine.py`` (a god module over its size budget). These helpers
decide whether a declared recipe may run on a route, apply it to a materialized
read, and record what it did so conservation counts the rows it removed instead
of reading them as loss.
"""

from __future__ import annotations

from typing import Any, Iterator, Sequence

try:
    from services.shape_apply import (
        ShapeRunner,
        build_shape_runner,
        shape_ledger_terms,
        shaped_schema,
    )
except (
    ImportError
):  # pragma: no cover - compatibility for tests with api root on PYTHONPATH
    from src.services.shape_apply import (
        ShapeRunner,
        build_shape_runner,
        shape_ledger_terms,
        shaped_schema,
    )

from .models import TransferRequest


def _open_shape_runner(
    request: TransferRequest,
    columns: list[str] | None,
) -> "ShapeRunner | None":
    """The declared shaping recipe, refused unless it is the approved one.

    Shaping runs before mapping on purpose: Map decides carriers, narrowing risk
    contracts and the DDL identity from the columns and values it is shown, so a
    recipe that changes source-side truth has to run first or those decisions
    describe data that never reaches the writer.
    """
    return build_shape_runner(
        getattr(request, "shape_recipe", None),
        source_columns=list(columns or []),
        approved_hash=str(getattr(request, "approved_shape_recipe_hash", "") or ""),
    )


def _stamp_shape_evidence(
    dest_summary: dict[str, Any],
    runner: "ShapeRunner | None",
) -> None:
    """Record what the recipe did to this run, on the run's own summary.

    Without this the ledger would compare a source COUNT(*) against a
    destination population short by the removed rows and report an unbalanced
    load, and the proof pack would not say which recipe produced the values it
    is proving.
    """
    if runner is None:
        return
    terms = shape_ledger_terms(runner)
    # A resumed streaming pass carries the removals of every committed page,
    # earlier passes included, while this runner only shaped the tail. Taking
    # the runner's smaller tally would drop the earlier passes' removed rows
    # out of conservation and read a correct load as short delivery.
    for key in ("rows_shaped_out", "rows_shaped_in", "rows_shape_filtered"):
        prior = dest_summary.get(key)
        if isinstance(prior, int) and prior > int(terms.get(key, 0) or 0):
            terms[key] = prior
    dest_summary.update(terms)
    dest_summary["shape_proof"] = runner.report()
    # The rows the recipe read are this run's source population, whether or not
    # they reached the writer. A materialized read hands reconciliation only the
    # surviving records, so the removed rows would be missing from both sides of
    # conservation and the recipe's effect would balance by disappearing.
    if not isinstance(dest_summary.get("source_row_count"), int):
        dest_summary["source_row_count"] = int(runner.effect.rows_in)
        dest_summary["source_row_count_source"] = "shape_read"


def _shaped_population_rows(
    runner: "ShapeRunner | None",
    rows: Iterator[dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    """The population the writer will see, for the pre-write fit scan.

    The scan asks whether every row survives the destination carrier, so it has
    to be asked of the shaped values: a ``round to 8`` step makes 27 otherwise
    unfittable decimals fit, and scanning the raw file would block a run that is
    correct. Its own runner, because the scan's counts are not the load's.
    """
    if runner is None:
        return rows
    probe = ShapeRunner(runner.recipe)

    def _iter() -> Iterator[dict[str, Any]]:
        for row in rows:
            shaped = probe.records([row])
            if shaped:
                yield shaped[0]

    return _iter()


def _shape_stream_refusal(
    runner: "ShapeRunner | None",
    *,
    effective_sync: str,
    multi_stream: bool,
    cursor_field: str,
    key_columns: Sequence[str],
) -> str:
    """Why this streaming route cannot honour the recipe, or ``""``.

    Silently ignoring a recipe on a route that cannot run it is the worst
    outcome: the operator approved shaped values and the destination would
    receive raw ones. So each route that cannot re-apply the recipe row by row
    says so before anything is written.

    History and change-data routes compare a row against the row already stored,
    which was written by a possibly different recipe — that comparison is not
    ours to guess at. An incremental or upsert route resolves rows and watermarks
    by cursor and key, so a recipe that rewrites, renames or drops one of those
    columns would move the watermark or the identity itself.
    """
    if runner is None:
        return ""
    sync = (effective_sync or "").lower()
    if sync in ("cdc", "scd2", "full_refresh_mirror", "mirror"):
        return (
            f"Shaping is not applied on the {sync} route: it merges each row "
            "against history already stored on the destination, which was not "
            "written by this recipe. Remove the Shape recipe for this sync mode, "
            "or use full refresh / incremental append and shape on the read."
        )
    if multi_stream:
        return (
            "Shaping a multi-stream selection is refused: one recipe names "
            "columns of one stream, so applying it to every selected stream "
            "would either miss columns or rewrite unrelated ones. Run one "
            "stream per transfer to shape it."
        )
    touched = set(runner.recipe.touched_columns)
    outputs = set(runner.output_columns)
    guarded = [c for c in ([cursor_field] if cursor_field else []) + list(key_columns) if c]
    hit = [c for c in guarded if c in touched or c not in outputs]
    if hit:
        return (
            "Shaping refuses to rewrite the columns this sync mode resolves rows "
            f"by ({', '.join(sorted(set(hit)))}): the cursor and key decide which "
            "rows are read and which stored row is replaced, so a shaped value "
            "there would move the watermark or change row identity. Shape other "
            "columns, or switch to a full refresh."
        )
    return ""


def _shape_materialized_read(
    runner: "ShapeRunner",
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str],
) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
    """Apply the recipe to a fully-read source and re-declare what it produced."""
    shaped = runner.records(records)
    out_columns = list(runner.output_columns or columns)
    return shaped, out_columns, shaped_schema(runner, shaped, schema)
