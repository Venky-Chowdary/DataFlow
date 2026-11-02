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
    schema: dict[str, str] | None = None,
) -> "ShapeRunner | None":
    """The declared shaping recipe, refused unless it is the approved one.

    Shaping runs before mapping on purpose: Map decides carriers, narrowing risk
    contracts and the DDL identity from the columns and values it is shown, so a
    recipe that changes source-side truth has to run first or those decisions
    describe data that never reaches the writer.
    """
    from services.shape_apply import is_typed_value_source

    return build_shape_runner(
        getattr(request, "shape_recipe", None),
        source_columns=list(columns or []),
        source_types=dict(schema or getattr(request, "column_types", None) or {}),
        approved_hash=str(getattr(request, "approved_shape_recipe_hash", "") or ""),
        approved_set_hash=str(getattr(request, "approved_set_hash", "") or ""),
        workspace_id=str(getattr(request, "workspace_id", "") or ""),
        # X-1 live: the materialized-read path ran the recipe untyped, so
        # database-sourced '' / '   ' collapsed to NULL at write while the
        # proof's expected side (typed re-read) still held them — every
        # blank-text cell flagged 'did not apply as declared'.
        typed_values=is_typed_value_source(
            str(getattr(getattr(request, "source", None), "format", "") or "")
        ),
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
    for key in ("rows_shaped_out", "rows_shaped_in", "rows_shape_filtered", "rows_expanded"):
        prior = dest_summary.get(key)
        if isinstance(prior, int) and prior > int(terms.get(key, 0) or 0):
            terms[key] = prior
    dest_summary.update(terms)
    dest_summary["shape_proof"] = runner.report()
    identity = [str(c) for c in (getattr(runner.recipe, "identity_columns", ()) or ()) if c]
    if identity:
        dest_summary.setdefault("shape_identity_columns", identity)
        dest_summary.setdefault("primary_key_columns", identity)
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
    return runner.fork().population(rows)


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
    if sync in ("cdc", "full_refresh_mirror", "mirror"):
        return (
            f"Transform (pre-load) is not applied on the {sync} route: it merges "
            "each row against history already stored on the destination, which was "
            "not written by this recipe. Remove the transform recipe for this sync "
            "mode, or use full refresh / incremental append and transform on the read."
        )
    if sync == "scd2":
        # The scd2 step IS the history contract — it declares the business
        # key, tracked columns and stamp names the merge honours, so a recipe
        # carrying it runs on this route. Row-local steps before it are
        # honoured only while they cannot move the history: they must not
        # change the row population (a missing key would soft-close it) and
        # must not rewrite a key, tracked or stamp column (stored history
        # was hashed against different names).
        recipe = runner.recipe
        step = recipe.scd2_step
        if step is None:
            return (
                "Transform (pre-load) is not applied on the scd2 route: it "
                "merges each row against history already stored on the "
                "destination, which was not written by this recipe. Declare "
                "the history contract as an scd2 step, or remove the "
                "transform recipe for this sync mode."
            )
        opts = step.options
        guarded = (
            {str(k) for k in (opts.get("keys") or [])}
            | {str(t) for t in (opts.get("tracked") or [])}
            | {
                str(opts.get("effective_from") or "valid_from"),
                str(opts.get("end_date") or "valid_to"),
                str(opts.get("is_current") or "is_current"),
                str(opts.get("row_hash") or "row_hash"),
            }
            | ({str(opts["version"])} if opts.get("version") else set())
        )
        for pre in recipe.enabled_steps:
            if pre.op == "scd2":
                continue
            if pre.active:
                return (
                    f"Transform step {pre.op} is refused on the scd2 route: "
                    "it changes the row population, and a row the stored "
                    "history cannot see would be soft-closed on every "
                    "snapshot. Only row-local steps that keep every row may "
                    "precede the scd2 contract step."
                )
            hit = sorted(set(pre.writes) & guarded)
            if hit:
                return (
                    f"Transform step {pre.op} is refused on the scd2 route: "
                    f"it rewrites history column(s) {', '.join(hit)} — the "
                    "stored versions were written and hashed under the "
                    "declared key/tracked/stamp names. Transform other "
                    "columns, or rename inside the scd2 step's own options."
                )
        return ""
    if multi_stream:
        return (
            "Transforming a multi-stream selection is refused: one recipe names "
            "columns of one stream, so applying it to every selected stream "
            "would either miss columns or rewrite unrelated ones. Run one "
            "stream per transfer to transform it."
        )
    touched = set(runner.recipe.touched_columns)
    outputs = set(runner.output_columns)
    guarded = [c for c in ([cursor_field] if cursor_field else []) + list(key_columns) if c]
    hit = [c for c in guarded if c in touched or c not in outputs]
    if hit:
        return (
            "Transform (pre-load) refuses to rewrite the columns this sync mode "
            f"resolves rows by ({', '.join(sorted(set(hit)))}): the cursor and key "
            "decide which rows are read and which stored row is replaced, so a "
            "transformed value there would move the watermark or change row "
            "identity. Transform other columns, or switch to a full refresh."
        )
    return ""


def _shape_materialized_read(
    runner: "ShapeRunner",
    records: list[dict[str, Any]],
    columns: list[str],
    schema: dict[str, str],
) -> tuple[list[dict[str, Any]], list[str], dict[str, str]]:
    """Apply the recipe to a fully-read source and re-declare what it produced."""
    if runner.recipe.router_step is not None:
        raise ValueError(
            "the transform recipe routes rows to per-route destination "
            "tables — that path is only supported on streaming database "
            "sources in this version; a fully-read source cannot be routed"
        )
    shaped = runner.records(records)
    # Buffering steps (dedupe keep-last, aggregate, sort, rank) emit only at
    # end-of-stream — a fully-read source is that stream.
    shaped.extend(runner.drain())
    out_columns = list(runner.output_columns or columns)
    return shaped, out_columns, shaped_schema(runner, shaped, schema)
