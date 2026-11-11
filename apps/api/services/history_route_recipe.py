"""Whether a pre-load transform recipe can run on a history-merging route.

One answer for every surface (D361): Execute's stream refusal, the preflight
gate that plan_transfer / start_transfer / MCP create_schedule read, the
schedule store's create and update, and the schedule runner's pre-dispatch
check for a schedule saved before that store check existed.

cdc and mirror never apply a pre-load recipe. scd2 applies one only when it
declares the history contract as an ``scd2`` step and its other steps cannot
move the history; a plain column pipeline is refused there exactly as Execute
refuses it, instead of being accepted at create and failing the first run.
"""

from __future__ import annotations

from typing import Any

#: Sync modes whose recipe transforms on the read, named in the way out.
READ_SIDE_SYNC_MODES = ("full_refresh_overwrite", "full_refresh_append", "incremental_append")


def scd2_recipe_refusal(recipe: Any) -> str:
    """Why an scd2 route cannot honour this parsed recipe, or ``""``.

    The scd2 step IS the history contract — it declares the business key,
    tracked columns and stamp names the merge honours, so a recipe carrying it
    runs on this route. Row-local steps before it are honoured only while they
    cannot move the history: they must not change the row population (a missing
    key would soft-close it) and must not rewrite a key, tracked or stamp column
    (stored history was hashed against different names).
    """
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


def history_recipe_refusal(shape_recipe: Any, sync_mode: str) -> str:
    """Execute's refusal for this recipe payload on this route, or ``""``.

    A recipe that does not parse is not judged here: its own parse refusal is
    the clearer message, and every surface that runs it reports that one.
    """
    from services.multi_stream_plan import enabled_recipe_steps, history_shape_refusal

    if not enabled_recipe_steps(shape_recipe):
        return ""
    sync = (sync_mode or "").strip().lower()
    if sync == "scd2":
        from services.shape_models import ShapeError, ShapeRecipe

        try:
            recipe = ShapeRecipe.parse(shape_recipe)
        except (ShapeError, TypeError):
            return ""
        return scd2_recipe_refusal(recipe)
    return history_shape_refusal(sync)


def schedule_recipe_refusal(shape_recipe: Any, sync_mode: str) -> str:
    """The schedule-facing refusal: what is refused, why, and the way out.

    Short enough for the 500 characters a run record and a parked finding hold.
    """
    from services.multi_stream_plan import enabled_recipe_steps

    refusal = history_recipe_refusal(shape_recipe, sync_mode)
    if not refusal:
        return ""
    ops = [str(s.get("op") or "?") for s in enabled_recipe_steps(shape_recipe)]
    named = ", ".join(ops[:3]) + (", …" if len(ops) > 3 else "")
    text = (
        f"Recipe steps [{named}]: {refusal} To remove it, PATCH this schedule with "
        'shape_recipe {"steps": []} (transform_recipe_id "" if bound).'
    )
    if (sync_mode or "").strip().lower() == "scd2":
        text += (
            " To keep them, use sync_mode "
            f"{' or '.join(READ_SIDE_SYNC_MODES[::2])}."
        )
    return text
