"""Parity Studio: read the evidence a run already produced, and the cells it names.

Nothing here diffs a table. The differences come from what Gate-8 and the
verification ladder stored on the job — ``sample_compare`` mismatches, the
ladder's L4/L5 localization, ``culprit_keys`` from the key census and the
transform proof, ``concurrent_source_drift`` from the keyed write witness, the
row counts and the quarantine. :func:`reread_cells` then fetches the current
source and destination values for *those* keys only (at most
:data:`MAX_REREAD_KEYS` per table), through the same keyed reads Gate-8 uses
(``services.target_sample.read_target_sample``) and the writer's own Map stage
(``reconcile_step._source_rows_as_written``), so a value is compared the way
the run wrote it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)

#: Keys re-read per table per report (both sides). Bounded on purpose.
MAX_REREAD_KEYS = 200
#: Key renderings kept on one difference.
MAX_KEYS_SAMPLE = 20

_PROTECTING = frozenset({"mask_pii", "hash_pii", "redact"})
_OVERWRITE_MODES = frozenset({"full_refresh_overwrite", "mirror"})


@dataclass
class Observation:
    """One difference the evidence names, before it is grouped and classified."""

    table: str
    kind: str  # value | missing_in_destination | extra_in_destination | duplicate_key
    column: str | None = None
    key: str | None = None
    rows: int = 1
    expected: str | None = None
    dest: str | None = None
    raw: str | None = None
    raw_known: bool = False
    origin: str = ""
    cause: dict[str, Any] | None = None  # set when the evidence itself names the cause
    keys_sample: list[str] = field(default_factory=list)
    sampled: bool = False
    #: Gate-8 proved the difference without counting its rows: ``rows`` is only a lower bound.
    at_least: bool = False


@dataclass
class TableEvidence:
    name: str
    check: str
    rows_source: int | None
    rows_destination: int | None
    sampled_rows: int = 0
    #: How Gate-8 chose its sample (``sample_seed.method``) and that choice in words.
    sample_method: str = ""
    sample_description: str = ""
    key_columns: list[str] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)
    quarantined_keys: set[str] = field(default_factory=set)
    drifted_keys: set[str] = field(default_factory=set)
    dest_types: dict[str, str] = field(default_factory=dict)
    source_types: dict[str, str] = field(default_factory=dict)
    transforms: dict[str, tuple[str, ...]] = field(default_factory=dict)
    protected: set[str] = field(default_factory=set)
    reread: dict[str, Any] = field(default_factory=dict)
    resolved_now: list[str] = field(default_factory=list)
    capped: bool = False
    outside_run: bool = False  # a destination-only key cannot have been written by this run
    note: str = ""
    #: Gate-8 failed this run and the stored evidence names nothing that accounts for it.
    gate8_unaccounted: bool = False
    #: A recorded fix re-check resolved keys the evidence named.
    resolved_by_fix: bool = False


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _int(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    from services.verification_ladder import _cell_text

    return _cell_text(value)


def request_of(job: dict[str, Any]) -> dict[str, Any]:
    return _dict(job.get("transfer_request"))


def mappings_of(job: dict[str, Any]) -> list[dict[str, Any]]:
    req = request_of(job)
    maps = req.get("mappings") or _dict(job.get("decision_artifact")).get("mappings") or []
    return [m for m in maps if isinstance(m, dict) and str(m.get("source") or "").strip()]


def sync_mode_of(job: dict[str, Any]) -> str:
    from services.sync_cursor import normalize_sync_mode

    dest = _dict(job.get("destination_summary"))
    raw = dest.get("sync_mode") or request_of(job).get("sync_mode") or job.get("sync_mode")
    return normalize_sync_mode(str(raw or ""))


def dest_engine_of(job: dict[str, Any]) -> str:
    dst = _dict(request_of(job).get("destination"))
    return str(
        dst.get("format") or job.get("destination_type")
        or _dict(job.get("destination_summary")).get("type") or ""
    ).lower()


def table_name_of(job: dict[str, Any]) -> str:
    dest = _dict(job.get("destination_summary"))
    dst = _dict(request_of(job).get("destination"))
    return str(
        dest.get("table") or dest.get("collection") or dst.get("table") or dst.get("collection")
        or job.get("destination_collection") or "destination"
    )


def key_columns_of(job: dict[str, Any]) -> list[str]:
    """Destination key columns: the run's own record of them, first found wins."""
    dest = _dict(job.get("destination_summary"))
    for name in ("primary_key_columns", "conflict_columns", "pagination_key_columns"):
        cols = [str(c) for c in dest.get(name) or [] if str(c or "").strip()]
        if cols:
            return cols
    # A stream contract names its key in source columns: read it in destination naming, as
    # every other key source is (a renamed protected key is matched by its target name).
    s2t = {s: t for t, s in target_to_source(job).items()}
    for contract in request_of(job).get("stream_contracts") or []:
        pk = _dict(contract).get("primary_key")
        cols = [pk] if isinstance(pk, str) else list(pk or [])
        cols = [s2t.get(str(c), str(c)) for c in cols if str(c or "").strip()]
        if cols:
            return cols
    recon = _dict(job.get("reconciliation"))
    l5 = _dict(_dict(_dict(recon.get("verification_ladder")).get("layers")).get("L5"))
    pk = str(_dict(l5.get("details")).get("pk_column") or "")
    if pk and "," not in pk:
        return [pk]
    seed = _dict(_dict(recon.get("sample_compare")).get("sample_seed"))
    sort_key = str(seed.get("sort_key") or "")
    return [sort_key] if sort_key else []


def target_to_source(job: dict[str, Any]) -> dict[str, str]:
    return {
        str(m.get("target") or m.get("source")): str(m.get("source"))
        for m in mappings_of(job)
        if not m.get("omit") and not m.get("omitted")
    }


def column_transforms(job: dict[str, Any]) -> dict[str, tuple[str, ...]]:
    """Target column -> the run's own transforms on it (Map transform, recipe steps)."""
    from services.transform_engine import normalize_engine_transform

    t2s = target_to_source(job)
    s2t = {s: t for t, s in t2s.items()}
    out: dict[str, list[str]] = {}
    for m in mappings_of(job):
        canon = normalize_engine_transform(str(m.get("transform") or "")) or ""
        if canon and canon.lower() not in {"none", "identity"}:
            out.setdefault(str(m.get("target") or m.get("source")), []).append(canon)
    recipe = _dict(request_of(job).get("shape_recipe"))
    for step in recipe.get("steps") or []:
        step = _dict(step)
        col = str(step.get("column") or _dict(step.get("options")).get("column") or "")
        if col and step.get("enabled") is not False:
            out.setdefault(s2t.get(col, col), []).append(str(step.get("op") or "step"))
    return {k: tuple(v) for k, v in out.items()}


def protected_targets(job: dict[str, Any]) -> set[str]:
    """Columns whose values are never shown: redact / hash_pii / mask_pii / omit."""
    from src.transfer.protection_proof import protected_columns

    req = request_of(job)
    out = {t for _s, t, _tf in protected_columns(mappings_of(job), _dict(req.get("shape_recipe")))}
    for m in mappings_of(job):
        transform = str(m.get("transform") or "").strip().lower()
        if m.get("omit") or m.get("omitted") or transform == "omit":
            out.add(str(m.get("target") or m.get("source")))
    return out


def dest_types_of(job: dict[str, Any]) -> dict[str, str]:
    types: dict[str, str] = {}
    for m in mappings_of(job):
        if m.get("target_type"):
            types[str(m.get("target") or m.get("source"))] = str(m["target_type"])
    ddl = _dict(_dict(job.get("decision_artifact")).get("ddl")).get("column_ddl")
    types.update({str(k): str(v) for k, v in _dict(ddl).items() if v})
    digest = _dict(job.get("destination_summary")).get("gate8_digest_types")
    types.update({str(k): str(v) for k, v in _dict(digest).items() if v})
    return types


def source_types_of(job: dict[str, Any]) -> dict[str, str]:
    """Target column -> the source column's declared type, as the run's mappings record it."""
    return {
        str(m.get("target") or m.get("source")): str(m["source_type"])
        for m in mappings_of(job)
        if m.get("source_type")
    }


def quarantined_keys_of(job: dict[str, Any], key_cols: list[str]) -> set[str]:
    from src.transfer.reconcile_step import _rejected_key

    dest = _dict(job.get("destination_summary"))
    details = dest.get("rejected_details") or job.get("rejected_details") or []
    out: set[str] = set()
    for detail in details if isinstance(details, list) else []:
        found = _rejected_key(_dict(detail), key_cols) if isinstance(detail, dict) else None
        if found:
            out.add(found[0] if len(found) == 1 else ", ".join(
                f"{c}={v}" for c, v in zip(key_cols, found)
            ))
    return out


def _render_key(key: Any, key_cols: list[str]) -> str | None:
    if isinstance(key, dict):
        parts = [key.get(c) for c in key_cols] if key_cols else list(key.values())
        if len(parts) == 1:
            return _text(parts[0])
        return ", ".join(f"{c}={_text(v)}" for c, v in zip(key_cols or list(key), parts))
    return _text(key)


def _drift(recon: dict[str, Any], key_cols: list[str]) -> tuple[set[str], int, list[str]]:
    from services.keyed_write_witness import DRIFT_KEY

    record = _dict(recon.get(DRIFT_KEY))
    keys: list[str] = []
    for name in ("sample_keys", "deleted_sample_keys", "moved_out_of_window_sample_keys"):
        keys += [k for k in (_render_key(x, key_cols) for x in record.get(name) or []) if k]
    rows = sum(int(record.get(n) or 0) for n in ("count", "deleted_count", "moved_out_of_window_count"))
    return set(keys), rows, keys


def _check_kind(recon: dict[str, Any]) -> str:
    """How the run compared values, as the proof classifier files its Gate-8 report.

    One authority with the signed proof pack and the Gate-8 tile
    (:func:`services.signed_proof_pack.classify_post_write_assurance` and the
    :mod:`services.reconcile_coverage` scopes): a pre-write preview compared
    nothing after the write; a writer acknowledgement or a CDC count scope is a
    count, not a value comparison, whatever sample it carries.
    """
    from services.reconcile_coverage import CDC_SOURCE_IMAGE_COUNT
    from services.signed_proof_pack import classify_post_write_assurance

    if not recon:
        return "none"
    claim = str(classify_post_write_assurance(recon).get("claim_level") or "none")
    if claim == "pre_write_only":
        return "none"
    if claim == "full_checksum":
        return "full_checksum"
    counts_only = claim == "writer_ack" or str(recon.get("checksum_scope") or "") == CDC_SOURCE_IMAGE_COUNT
    ladder = _dict(recon.get("verification_ladder"))
    l5 = _dict(_dict(ladder.get("layers")).get("L5"))
    proof = _dict(recon.get("transform_applied"))
    keyed_population = (
        bool(recon.get("culprit_keys")) or recon.get("culprit_census_clean") is True
        or (bool(l5) and not _dict(l5.get("details")).get("skipped"))
        or (proof.get("status") in {"verified", "failed"} and "exact" in str(proof.get("compare_mode") or ""))
    )
    if keyed_population and not counts_only:
        return "key_diff"
    if not counts_only and _int(_dict(recon.get("sample_compare")).get("rows_compared")):
        return "sampled"
    if _int(recon.get("source_rows")) is not None and _int(recon.get("target_rows")) is not None:
        return "count_only"
    return "none"


#: Gate-8 sample methods that draw uniformly at random from the whole population, so a
#: Clopper-Pearson bound on the population's difference rate holds. None does today:
#: Gate-8 samples from the first ``_RECONCILE_SAMPLE_CAP`` rows the run read (stream.py),
#: then keeps the lowest keys (``keyed_sorted``), the first rows (``positional_sorted``)
#: or a seeded per-stratum pick among them (``stratified``) — none is a random draw over
#: the population. Add a method here only when Gate-8 records one that is.
UNIFORM_RANDOM_SAMPLE_METHODS: frozenset[str] = frozenset()


def sample_bound_valid(method: str) -> bool:
    """Whether a sample of this method supports a population bound (see above)."""
    return method in UNIFORM_RANDOM_SAMPLE_METHODS


def describe_sample(sample_compare: dict[str, Any]) -> tuple[str, str]:
    """(method, how the rows were chosen, in words) for Gate-8's sample compare."""
    seed = _dict(sample_compare.get("sample_seed"))
    method = str(seed.get("method") or "")
    sort_key = str(seed.get("sort_key") or "").strip()
    stratum = str(seed.get("stratify_by") or "").strip()
    if method == "keyed_sorted":
        how = f"the lowest {sort_key or 'key'} values among the first rows the run read"
    elif method == "positional_sorted":
        how = "the first rows the run read"
    elif method == "stratified":
        how = f"a pick across {stratum or 'one column'}'s values among the first rows the run read"
    else:
        how = "rows Gate-8 picked, not at random"
    return method, how


def evidence_capped(recon: dict[str, Any]) -> bool:
    """True when Gate-8 named only the first differing keys, so more rows may differ."""
    ladder = _dict(recon.get("verification_ladder"))
    l5 = _dict(_dict(_dict(ladder.get("layers")).get("L5")).get("details"))
    named = len(_dict(ladder.get("localization")).get("rows") or [])
    if l5.get("capped") or (_int(l5.get("mismatch_count")) or 0) > named:
        return True
    # Gate-8 names at most this many culprit keys per kind: a full list may stop short.
    from src.transfer.reconcile_step import _CULPRIT_KEYS_LIMIT

    for holder in (recon, _dict(recon.get("transform_applied"))):
        if any(len(keys or []) >= _CULPRIT_KEYS_LIMIT for keys in _dict(holder.get("culprit_keys")).values()):
            return True
    return False


def _value_observations(recon: dict[str, Any], table: str) -> list[Observation]:
    out: list[Observation] = []
    for m in _dict(recon.get("sample_compare")).get("mismatches") or []:
        m = _dict(m)
        out.append(Observation(
            table=table, kind="value", column=str(m.get("target") or m.get("source") or "") or None,
            key=_text(m.get("key")) or None, expected=_text(m.get("source_value")),
            dest=_text(m.get("target_value")), origin="sample_compare", sampled=True,
        ))
    localization = _dict(_dict(recon.get("verification_ladder")).get("localization"))
    for row in localization.get("rows") or []:
        row = _dict(row)
        key = _text(row.get("pk"))
        if row.get("reason") == "missing_side":
            kind = "missing_in_destination" if row.get("source_present") else "extra_in_destination"
            out.append(Observation(table=table, kind=kind, key=key, origin="ladder_l5"))
        elif row.get("column"):
            out.append(Observation(
                table=table, kind="value", column=str(row["column"]), key=key,
                expected=_text(row.get("source_value")), dest=_text(row.get("target_value")),
                origin="ladder_l5",
            ))
    culprits: dict[str, list[Any]] = {}
    for holder in (recon, _dict(recon.get("transform_applied"))):
        for label, keys in _dict(holder.get("culprit_keys")).items():
            culprits.setdefault(label, []).extend(keys or [])
    kinds = {"changed": "value", "missing_in_dest": "missing_in_destination",
             "extra_in_dest": "extra_in_destination"}
    with_columns = {o.key for o in out if o.kind == "value" and o.column}
    for label, keys in culprits.items():
        kind = kinds.get(label)
        for key in dict.fromkeys(_text(k) for k in keys):
            # A changed key the sample already named by column needs no second, vaguer entry.
            if kind and key and not (kind == "value" and key in with_columns):
                out.append(Observation(table=table, kind=kind, key=key, origin="key_census"))
    return out


#: Observations that localize a Gate-8 failure (rows, keys or columns that differ).
#: Recipe effects, protected columns and out-of-scope rows exist by design and
#: account for no failure.
LOCALIZING_ORIGINS = frozenset({
    "sample_compare", "ladder_l5", "key_census", "write_witness", "write_witness_loss",
    "ladder_aggregate", "unattributed", "row_count", "quarantine", "rule_checks",
    "dedupe_checks", "batch_undo", "protection_proof", "reread",
})


def _failure_observations(recon: dict[str, Any], table: str, key_cols: list[str],
                          named: list[Observation]) -> list[Observation]:
    """What Gate-8 proved differs without naming every row: witnessed losses,
    column aggregates (L2/L4, the engine-profile ladder) and an unattributed
    discrepancy. Each is unexplained: no rule can explain a value it never saw.
    """
    from services.keyed_write_witness import DRIFT_KEY, UNATTRIBUTED_KEY
    from services.parity_classifier import UNEXPLAINED, cause

    out: list[Observation] = []
    drift = _dict(recon.get(DRIFT_KEY))
    loss_count = max(_int(drift.get("loss_count")) or 0, 0)
    if loss_count:
        lost = cause(
            UNEXPLAINED,
            "The write witness found this key missing from the destination or different from what "
            "this run wrote, and the source no longer holds it: a destination loss.",
            explained=False,
        )
        keys = [k for k in (_render_key(x, key_cols) for x in drift.get("losses") or []) if k]
        keys = list(dict.fromkeys(keys))[:loss_count]
        out += [Observation(table=table, kind="missing_in_destination", key=k,
                            origin="write_witness_loss", cause=lost) for k in keys]
        if loss_count > len(keys):
            out.append(Observation(table=table, kind="missing_in_destination", rows=loss_count - len(keys),
                                   origin="write_witness_loss", cause=lost))
    columns: dict[str, str] = {}
    ladder = _dict(recon.get("verification_ladder"))
    layers = _dict(ladder.get("layers"))
    for layer in ("L2", "L4"):
        info = _dict(layers.get(layer))
        if info.get("passed") is False:
            for col in _dict(info.get("details")).get("mismatched_columns") or []:
                columns.setdefault(str(col), f"Gate-8's column check ({layer})")
    if any(_dict(layers.get(x)).get("passed") is False for x in ("L2", "L4")):
        for col in _dict(ladder.get("localization")).get("columns") or []:
            columns.setdefault(str(col), "Gate-8's column localization")
    unattributed = _dict(recon.get(UNATTRIBUTED_KEY))
    if unattributed.get("l2") == "failed":
        for col in unattributed.get("columns") or [None]:
            columns.setdefault(str(col) if col else "", "The re-read of the keys this run wrote (L2)")
    if any(o.key and o.origin == "key_census" for o in named):
        # The key census hashes whole rows over the whole population: its named
        # rows localize the aggregate failure of every column, including rows
        # that move a column's min/max/sum without a value of their own changing.
        columns = {}
    elif any(o.key and o.origin == "ladder_l5" for o in named):
        # L5 compares only its focus columns (L4's typed mismatches); L2's untyped
        # aggregates can fail on a column L5 never compared (re-review 4, finding 1).
        compared = {str(c) for c in _dict(_dict(layers.get("L5")).get("details")).get("focus_columns") or []}
        columns = {col: where for col, where in columns.items() if col not in compared}
    by_row = {o.column for o in named if o.kind == "value" and o.column}
    for col, where in columns.items():
        if col and col in by_row:
            continue  # the rows that differ in this column are already named
        out.append(Observation(
            table=table, kind="value", column=col or None, rows=1, at_least=True,
            origin="unattributed" if where.startswith("The re-read") else "ladder_aggregate",
            cause=cause(
                UNEXPLAINED,
                f"{where} found that {('column ' + col) if col else 'one or more columns'} differs between "
                "the source and the destination; the rows were not named, so at least one row differs.",
                explained=False,
            ),
        ))
    return out


def gate8_failed(recon: dict[str, Any]) -> bool:
    return recon.get("passed") is False


def failed_layers(recon: dict[str, Any]) -> list[str]:
    layers = _dict(_dict(recon.get("verification_ladder")).get("layers"))
    return sorted(name for name, info in layers.items() if _dict(info).get("passed") is False)


def _effect_observations(job: dict[str, Any], table: str) -> list[Observation]:
    """Cells the run's own recipe changed, and protected columns: explained by design."""
    from services.parity_classifier import TRANSFORM_APPLIED, UNEXPLAINED, cause

    dest = _dict(job.get("destination_summary"))
    recon = _dict(job.get("reconciliation"))
    s2t = {s: t for t, s in target_to_source(job).items()}
    proof = _dict(recon.get("transform_applied"))
    out: list[Observation] = []
    effect = _dict(_dict(dest.get("shape_proof")).get("effect"))
    steps = {
        int(_dict(s).get("step") or i + 1): _dict(s)
        for i, s in enumerate(effect.get("steps") or [])
    }
    recipe_steps = _dict(request_of(job).get("shape_recipe")).get("steps") or _dict(
        dest.get("shape_proof")
    ).get("steps") or []
    for i, step in enumerate(recipe_steps):
        tally = steps.get(i + 1) or {}
        changed = _int(tally.get("cells_changed")) or 0
        col = str(_dict(step).get("column") or "")
        if changed <= 0 or not col:
            continue
        label = str(tally.get("label") or _dict(step).get("op") or "a recipe step")
        status = str(proof.get("status") or "")
        out.append(Observation(
            table=table, kind="value", column=s2t.get(col, col), rows=changed, origin="recipe_effect",
            cause=cause(
                TRANSFORM_APPLIED,
                f"{label} changed {changed} cell(s) as configured"
                + (f" (transform proof: {status})." if status else "."),
                explained=True,
            ),
        ))
    protection = _dict(recon.get("protection_proof"))
    written = _int(dest.get("rows_written")) or _int(recon.get("target_rows")) or 0
    leaked = set(protection.get("leaked_columns") or [])
    for src, tgt, tf in _protected_triples(job):
        if tgt in leaked:
            out.append(Observation(
                table=table, kind="value", column=tgt, rows=1, at_least=True, origin="protection_proof",
                cause=cause(UNEXPLAINED, f"The protection proof found source values in clear despite {tf}.",
                            explained=False),
            ))
        elif written:
            out.append(Observation(
                table=table, kind="value", column=tgt, rows=written, origin="protection",
                cause=cause(
                    TRANSFORM_APPLIED,
                    f"Every value is protected by {tf}, as configured"
                    + (f"; protection proof {protection.get('status')}." if protection else "."),
                    explained=True,
                ),
            ))
    return out


def _protected_triples(job: dict[str, Any]) -> list[tuple[str, str, str]]:
    from src.transfer.protection_proof import protected_columns

    return protected_columns(mappings_of(job), _dict(request_of(job).get("shape_recipe")))


def _skip_policy(dest: dict[str, Any]) -> str:
    """The duplicate handling the run recorded for its skipped rows, or "" when none was."""
    policy = str(dest.get("duplicate_policy") or "").strip()
    if policy and policy.lower() not in {"append", "none", "allow"}:
        return policy
    if str(dest.get("in_file_dedupe") or "").strip():
        return f"in-file dedupe, {dest['in_file_dedupe']}"
    if (_int(dest.get("collapsed_duplicate_rows")) or 0) > 0 or dest.get("skipped_identity_keys"):
        return "duplicate keys collapsed"
    return ""


#: Origins that name a missing or extra row by key inside this run's count gap.
_COUNTED_ORIGINS = frozenset({"sample_compare", "ladder_l5", "key_census"})
_KEYED_MODES = frozenset({"upsert", "incremental_deduped", "cdc", "scd2"})


def _count_observations(job: dict[str, Any], ev: TableEvidence) -> list[Observation]:
    """Rows the counts say are missing or extra beyond the keyed evidence.

    Gate-8's ``source_rows`` is the run's reader count and the destination
    count holds what the run wrote, so a source row the write witness saw
    updated, deleted or moved out of the window *after* the write changes
    neither count: no witness category moves this gap (subtracting them hid
    real losses). Named keys count once (L5 and the census may both name one),
    and a key already counted as quarantined or witnessed is not subtracted
    again. Witnessed losses are left out: the source no longer holds them, so
    they are not in the reader-count gap either.
    """
    from services.parity_classifier import (
        OUT_OF_SCOPE_POPULATION, QUARANTINED, TRANSFORM_APPLIED, UNEXPLAINED, cause,
    )

    recon = _dict(job.get("reconciliation"))
    dest = _dict(job.get("destination_summary"))
    src_n, dst_n = ev.rows_source, ev.rows_destination
    if src_n is None or dst_n is None:
        return []
    out: list[Observation] = []
    quarantined = max(_int(recon.get("rejected_rows")) or _int(dest.get("rejected_rows")) or 0, 0)
    quarantined -= max(_int(recon.get("coerced_null_rows")) or 0, 0)
    filtered = sum(max(_int(dest.get(k)) or 0, 0) for k in ("rows_shape_filtered", "rows_shape_diverted"))
    skipped = max(_int(recon.get("rows_skipped")) or _int(dest.get("rows_skipped")) or 0, 0)
    if quarantined > 0:
        out.append(Observation(
            table=ev.name, kind="missing_in_destination", rows=quarantined, origin="quarantine",
            keys_sample=sorted(ev.quarantined_keys)[:MAX_KEYS_SAMPLE],
            cause=cause(QUARANTINED, "Rows that failed validation were held in quarantine.", explained=True),
        ))
    if filtered > 0:
        out.append(Observation(
            table=ev.name, kind="missing_in_destination", rows=filtered, origin="recipe_effect",
            cause=cause(TRANSFORM_APPLIED, "Rows your recipe filtered or diverted, as configured.", explained=True),
        ))
    if skipped > 0:
        policy = _skip_policy(dest)
        out.append(Observation(
            table=ev.name, kind="missing_in_destination", rows=skipped, origin="writer_policy",
            cause=cause(TRANSFORM_APPLIED, f"Rows the run's duplicate policy ({policy}) skipped, as configured.",
                        explained=True) if policy else cause(
                UNEXPLAINED,
                f"The writer skipped {skipped:,} row(s) and the run recorded no duplicate policy or other "
                "reason for it.",
                explained=False,
            ),
        ))
    counted = [o for o in ev.observations if o.key and o.origin in _COUNTED_ORIGINS]
    already = ev.quarantined_keys | ev.drifted_keys
    keyed_missing = len({o.key for o in counted if o.kind == "missing_in_destination" and o.key not in already})
    keyed_extra = len({o.key for o in counted if o.kind == "extra_in_destination" and o.key not in already})
    before = max(_int(recon.get("target_rows_before")) or _int(dest.get("target_rows_before")) or 0, 0)
    mode = sync_mode_of(job)
    landed_expected = src_n - quarantined - filtered - skipped
    # An append or upsert into an occupied table writes only source rows: a
    # destination-only key it names was there before — outside this run.
    ev.outside_run = mode not in _OVERWRITE_MODES and before > 0
    shortfall = False
    if not ev.outside_run:
        # Overwrite or a first load: the destination is exactly this run's rows.
        unnamed = landed_expected - dst_n - keyed_missing + keyed_extra
        outside = 0
    elif mode in _KEYED_MODES:
        # A keyed write may update rows that were already there, so the overlap
        # is unknown — but every row this run wrote must be there now: holding
        # fewer than that is a certain shortfall.
        unnamed = max(0, landed_expected - dst_n - keyed_missing)
        shortfall = unnamed > 0
        outside = max(0, dst_n - (landed_expected - keyed_missing)) - keyed_extra
    else:
        unnamed = landed_expected - (dst_n - before) - keyed_missing
        outside = before - keyed_extra
    if unnamed > 0:
        out.append(Observation(
            table=ev.name, kind="missing_in_destination", rows=unnamed, origin="row_count",
            at_least=shortfall,
            cause=cause(
                UNEXPLAINED,
                f"The destination holds {dst_n:,} row(s), fewer than the {landed_expected:,} this run "
                "wrote: at least this many never landed.",
                explained=False,
            ) if shortfall else None,
        ))
    elif unnamed < 0:
        out.append(Observation(
            table=ev.name, kind="extra_in_destination", rows=-unnamed, origin="row_count",
            # Into an occupied table only ``before`` rows were there already (``outside`` below):
            # these are beyond them — rows written twice, or by something else (re-review 4, finding 8).
            cause=cause(
                UNEXPLAINED,
                f"The destination holds {-unnamed:,} row(s) more than the {before:,} it held before plus the "
                f"{landed_expected:,} this run wrote: rows written twice, or by something else.",
                explained=False,
            ) if ev.outside_run else None,
        ))
    if outside > 0:
        out.append(Observation(
            table=ev.name, kind="extra_in_destination", rows=outside, origin="row_count",
            cause=cause(
                OUT_OF_SCOPE_POPULATION,
                f"The destination already held {before} row(s) this run did not write "
                f"({mode.replace('_', ' ')} into an occupied table).",
                explained=True,
            ),
        ))
    return out


def _duplicate_observations(recon: dict[str, Any], table: str) -> list[Observation]:
    out = []
    for name in ("rule_checks", "dedupe_checks"):
        for check in recon.get(name) or []:
            check = _dict(check)
            dups = _int(check.get("duplicates")) or 0
            if check.get("status") == "failed" and dups > 0:
                out.append(Observation(
                    table=table, kind="duplicate_key", column=str(check.get("column") or "") or None,
                    rows=dups, origin=name,
                ))
    return out


def _batch_undone(ev: TableEvidence, recon: dict[str, Any], dest: dict[str, Any]) -> None:
    """A failed run whose batch was removed: none of its rows are in the destination now."""
    from services.parity_classifier import UNEXPLAINED, cause

    before = _int(recon.get("target_rows_before")) or _int(dest.get("target_rows_before")) or 0
    ev.rows_destination = before
    ev.observations = [o for o in ev.observations if o.kind == "missing_in_destination" and o.cause]
    written = (ev.rows_source or 0) - sum(o.rows for o in ev.observations)
    if written > 0:
        ev.observations.append(Observation(
            table=ev.name, kind="missing_in_destination", rows=written, origin="batch_undo",
            cause=cause(
                UNEXPLAINED,
                "The run failed verification and its batch was taken out of the destination, so "
                "this run's rows are not there. Fix the cause, then re-run.",
                explained=False,
            ),
        ))


def extract_evidence(job: dict[str, Any]) -> list[TableEvidence]:
    """Tables and the differences the stored evidence names. Reads no database."""
    from services.parity_classifier import CONCURRENT_SOURCE_UPDATE, cause

    recon = _dict(job.get("reconciliation"))
    dest = _dict(job.get("destination_summary"))
    name = table_name_of(job)
    key_cols = key_columns_of(job)
    ev = TableEvidence(
        name=name,
        check=_check_kind(recon),
        rows_source=_int(recon.get("source_rows")),
        rows_destination=_int(recon.get("target_rows")),
        sampled_rows=_int(_dict(recon.get("sample_compare")).get("rows_compared")) or 0,
        key_columns=key_cols,
        dest_types=dest_types_of(job),
        source_types=source_types_of(job),
        transforms=column_transforms(job),
        protected=protected_targets(job),
    )
    ev.sample_method, ev.sample_description = describe_sample(_dict(recon.get("sample_compare")))
    ev.quarantined_keys = quarantined_keys_of(job, key_cols)
    ev.capped = evidence_capped(recon)
    if ev.capped:
        ev.note = (
            f"Gate-8 named only the first differing keys of {name}; more rows may differ. "
            "Re-run the job to verify the rest."
        )
    ev.drifted_keys, drift_rows, drift_sample = _drift(recon, key_cols)
    ev.observations = _value_observations(recon, name)
    # Drifted keys the sample, L5 or the census already name are counted there.
    drift_rows -= len({o.key for o in ev.observations if o.key in ev.drifted_keys})
    if drift_rows > 0:
        ev.observations.append(Observation(
            table=name, kind="value", rows=drift_rows, origin="write_witness",
            keys_sample=drift_sample[:MAX_KEYS_SAMPLE],
            cause=cause(
                CONCURRENT_SOURCE_UPDATE,
                "The write witness proved the source changed these rows after this run wrote them.",
                explained=True,
            ),
        ))
    ev.observations += _failure_observations(recon, name, key_cols, ev.observations)
    ev.observations += _count_observations(job, ev)
    ev.observations += _effect_observations(job, name)
    ev.observations += _duplicate_observations(recon, name)
    if dest.get("partial_batch_removed") or str(dest.get("partial_batch_undo") or "") == "restored":
        _batch_undone(ev, recon, dest)
    if gate8_failed(recon) and not any(localizes(o) for o in ev.observations):
        mark_unaccounted(ev, recon)
    tables = [ev]
    streams = dest.get("streams") if is_multi_stream(dest) else None
    for row in streams if isinstance(streams, list) else []:
        row = _dict(row)
        stream = str(row.get("name") or row.get("table") or "").strip()
        ledger = _dict(row.get("row_accounting"))
        if not stream or stream == name:
            continue
        tables.append(TableEvidence(
            name=stream,
            check="count_only" if ledger else "none",
            rows_source=_int(ledger.get("source_count") or ledger.get("source_rows")),
            rows_destination=_int(ledger.get("dest_count")),
            note=f"Only the row count of {stream} was recorded on the run; its values were not compared.",
        ))
    for extra in tables[1:]:
        if extra.rows_source is not None and extra.rows_destination is not None:
            gap = extra.rows_source - extra.rows_destination
            if gap:
                extra.observations.append(Observation(
                    table=extra.name,
                    kind="missing_in_destination" if gap > 0 else "extra_in_destination",
                    rows=abs(gap), origin="row_count",
                ))
    return tables


def is_multi_stream(dest: dict[str, Any]) -> bool:
    """A run that wrote several tables: flagged, or (multi-table CDC writes no flag) two or more streams."""
    streams = dest.get("streams")
    return bool(dest.get("multi_stream")) or (isinstance(streams, list) and len(streams) >= 2)


def _out_of_scope(ob: Observation) -> bool:
    return bool(ob.cause) and ob.cause.get("code") == "out_of_scope_population"


def localizes(ob: Observation) -> bool:
    """Whether this observation names a row or column that can account for a Gate-8 failure."""
    return ob.origin in LOCALIZING_ORIGINS and not _out_of_scope(ob)


def mark_unaccounted(ev: TableEvidence, recon: dict[str, Any]) -> None:
    """Gate-8 failed and nothing left in the report accounts for it: the table is unverifiable."""
    ev.gate8_unaccounted = True
    layers = failed_layers(recon)
    ev.note = (
        f"Gate-8 failed this run{' (' + ', '.join(layers) + ')' if layers else ''}, but its "
        f"evidence names no row or column of {ev.name} that accounts for the failure, so parity "
        "cannot be shown. Re-run the job to localize the difference."
    )


def job_is_terminal(job: dict[str, Any]) -> bool:
    """A finished job (completed, with quarantine or warnings, failed, cancelled): the trust card's rule."""
    from services.job_trust import is_terminal_status

    return is_terminal_status(job.get("status"))


# ── bounded keyed re-read ────────────────────────────────────────────────────


class RereadImpossible(LookupError):
    """The evidence cannot be re-read by key at all; its message is fixed text, never a value."""


def _endpoints(job: dict[str, Any]) -> tuple[Any, Any, Any]:
    from src.transfer.models import transfer_request_from_dict

    payload = request_of(job)
    if not payload.get("source") or not payload.get("destination"):
        raise RereadImpossible("the job has no saved route to re-read")
    req = transfer_request_from_dict(payload)
    try:
        from src.transfer.engine import get_transfer_engine

        get_transfer_engine()._resolve_saved_connectors(req)
    except (LookupError, ValueError, RuntimeError) as exc:
        logger.info("parity re-read: saved connectors not refreshed (%s)", type(exc).__name__)
    return req, req.source, req.destination


def _read_by_keys(endpoint: Any, *, columns: list[str] | None, key_col: str, keys: list[str]) -> list[dict]:
    from services.dialect_profiles import schema_from_cfg
    from services.target_sample import read_target_sample
    from src.transfer.adapters import resolve_connector_config
    from src.transfer.connector_capabilities import resolve_driver_type

    cfg = resolve_connector_config(endpoint)
    db_type = resolve_driver_type(str(cfg.get("type") or endpoint.format or "")).lower()
    table = str(endpoint.table or endpoint.collection or "")
    rows = read_target_sample(
        db_type, cfg, schema=schema_from_cfg(db_type, cfg), table_name=table,
        columns=columns, limit=max(1, len(keys)), sort_key=key_col, key_values=list(keys),
    )
    return [r for r in rows if isinstance(r, dict)]


def _index(rows: list[dict], key_col: str) -> dict[str, dict]:
    from services.verification_ladder import _index_by_pk

    return _index_by_pk(rows, key_col)


def reread_cells(job: dict[str, Any], ev: TableEvidence, keys: list[str]) -> dict[str, dict[str, Any]]:
    """Current ``{key: {raw, expected, dest}}`` rows for at most MAX_REREAD_KEYS keys.

    ``raw`` is the source row as stored, ``expected`` its image after the run's
    recipe and Map stage (what the writer would write), ``dest`` the destination
    row. A side that no longer holds the key is ``None``. Raises when a side
    cannot be read; the caller keeps the stored evidence then.
    """
    if len(ev.key_columns) != 1:
        raise RereadImpossible("re-reading by key needs a single-column key")
    keys = list(dict.fromkeys(k for k in keys if k is not None))[:MAX_REREAD_KEYS]
    if not keys:
        return {}
    req, source, destination = _endpoints(job)
    key_tgt = ev.key_columns[0]
    t2s = target_to_source(job)
    key_src = t2s.get(key_tgt, key_tgt)
    targets = list(t2s)
    raw_rows = _index(_read_by_keys(source, columns=None, key_col=key_src, keys=keys), key_src)
    dest_rows = _index(
        _read_by_keys(destination, columns=targets or None, key_col=key_tgt, keys=keys), key_tgt
    )
    expected = _expected_images(job, req, list(raw_rows.values()), targets, ev.dest_types)
    by_key_expected = dict(zip(raw_rows.keys(), expected))
    out: dict[str, dict[str, Any]] = {}
    for key in keys:
        raw = raw_rows.get(key)
        out[key] = {
            "raw": {t: _text(raw.get(s)) for t, s in t2s.items()} if raw else None,
            "expected": by_key_expected.get(key),
            "dest": {t: _text(v) for t, v in dest_rows[key].items()} if key in dest_rows else None,
            "held_out": raw is not None and by_key_expected.get(key) is None,
        }
    return out


def _expected_images(
    job: dict[str, Any], req: Any, raw_rows: list[dict], targets: list[str], dest_types: dict[str, str]
) -> list[dict[str, str | None] | None]:
    """Each raw source row as the run would write it: recipe, then the writer's Map stage."""
    from src.transfer.reconcile_step import _shaped_rows_as_written

    rows = list(raw_rows)
    recipe = getattr(req, "shape_recipe", None) or {}
    if recipe and rows:
        from services.shape_engine import ShapeEngine
        from services.shape_models import ShapeRecipe

        from services.run_clock import recorded_run_started_at

        # A run-start column is re-derived with the value the run recorded.
        engine = ShapeEngine(
            ShapeRecipe.parse(recipe),
            run_started_at=recorded_run_started_at(job)
            or recorded_run_started_at(job.get("destination_summary") or {}),
        )
        rows = [engine.apply_row(r) for r in rows]
    present = [r for r in rows if r is not None]
    mapped = _shaped_rows_as_written(
        present,
        mappings=list(getattr(req, "mappings", None) or mappings_of(job)),
        target_cols=targets,
        dest_types=dest_types,
        dest_type=dest_engine_of(job),
        validation_mode=str(getattr(req, "validation_mode", "") or "strict"),
    ) if present else []
    it = iter(mapped)
    out: list[dict[str, str | None] | None] = []
    for row in rows:
        image = next(it) if row is not None else None
        out.append({k: _text(v) for k, v in image.items()} if isinstance(image, dict) else None)
    return out
