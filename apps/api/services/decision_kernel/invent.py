"""Context-aware invent (Phase C5).

Same ConversionClass does **not** imply the same DDL in every execution mode.
Create-new may widen safely; bind-existing / CDC sparse / append must never
invent destination capacity the live schema did not prove.
"""

from __future__ import annotations

from enum import Enum
from typing import Any

from services.column_case import column_type_or_none
from services.mapping_constraints import is_intentional_omit


class InventContext(str, Enum):
    """Why invent is being asked — drives DDL authority."""

    CREATE_NEW = "create_new"
    BIND_EXISTING = "bind_existing"
    CDC_SPARSE = "cdc_sparse"
    APPEND = "append"
    FULL_REFRESH = "full_refresh"


class InventRefused(Exception):
    """Fail-closed invent refusal for bind/CDC/append without a proven stamp."""

    def __init__(self, message: str, *, context: InventContext):
        super().__init__(message)
        self.context = context


def invent_dest_type(
    source_type: str,
    *,
    dest_db: str,
    context: InventContext | str,
    existing_dest_type: str = "",
    samples: list[Any] | None = None,
) -> str:
    """Return the destination type stamp allowed for ``context``.

    * ``create_new`` / ``full_refresh`` (empty dest): width-safe invent via
      ``create_new_mapping_target_type`` / ``ddl_type``.
    * ``bind_existing`` / ``append`` / ``cdc_sparse``: require
      ``existing_dest_type``; never invent capacity from the source alone.
    """
    from services.decision_kernel.types import (
        create_new_mapping_target_type,
        ddl_type,
        materialize_dest_ddl,
    )

    ctx = (
        context
        if isinstance(context, InventContext)
        else InventContext(str(context or InventContext.CREATE_NEW.value))
    )
    src = (source_type or "").strip()
    existing = (existing_dest_type or "").strip()
    db = (dest_db or "").strip().lower()

    if ctx in {
        InventContext.BIND_EXISTING,
        InventContext.CDC_SPARSE,
        InventContext.APPEND,
    }:
        if not existing:
            raise InventRefused(
                f"Invent refused in {ctx.value} — destination type must come from "
                "live schema / Map stamp (never invent capacity from source alone).",
                context=ctx,
            )
        # Live schema is physical authority. Ambiguous SQL keywords (INT/INTEGER/
        # FLOAT) on an existing column mean the engine's native wire already
        # present — never rewrite to the create-new 64-bit invent floor.
        from services.type_system import strip_identity_qualifier

        live = strip_identity_qualifier(existing).strip()
        live_u = live.upper()
        if live_u in {"INT", "INTEGER", "SIGNED", "FLOAT"} or live.lower() in {
            "integer",
            "float",
        }:
            return str(existing)
        # Materialize for dialect wire; do not widen beyond the proven stamp.
        return str(materialize_dest_ddl(db, existing) or existing)

    # CREATE_NEW / FULL_REFRESH — safe widen invent.
    if existing and ctx is InventContext.FULL_REFRESH:
        # Refreshing into an existing object: bind authority wins.
        return str(materialize_dest_ddl(db, existing) or existing)

    # CREATE_NEW invent authority is create_new_mapping_target_type alone
    # (width-preserving + bare-logical 64-bit floor). Never re-widen here —
    # a second BIGINT floor made Map INT/SMALLINT disagree with Validate stamp.
    # Source engine (when a transfer bound one) only widens the stamp: a source
    # that can emit any code point must not land on a code-page VARCHAR.
    from services.source_engine_scope import active_source_engine

    stamped = create_new_mapping_target_type(
        src, db, samples=samples, source_db=active_source_engine()
    )
    if stamped:
        return str(stamped)
    return str(ddl_type(db, src) or src or "TEXT")


def invent_context_from_sync_mode(
    sync_mode: str,
    *,
    create_new: bool = False,
    table_exists: bool | None = None,
    cdc: bool = False,
) -> InventContext:
    """Derive InventContext from transfer sync vocabulary + object state."""
    if cdc:
        return InventContext.CDC_SPARSE
    if create_new or table_exists is False:
        return InventContext.CREATE_NEW
    mode = (sync_mode or "").strip().lower()
    if "append" in mode:
        return InventContext.APPEND
    if "cdc" in mode or "incremental" in mode:
        return InventContext.CDC_SPARSE
    if table_exists is True:
        return InventContext.BIND_EXISTING
    if "overwrite" in mode or "full_refresh" in mode:
        return InventContext.FULL_REFRESH
    return InventContext.BIND_EXISTING if table_exists else InventContext.CREATE_NEW


def _is_source_as_dest_bootstrap(stamped: str, src: str) -> bool:
    """True when Map copied source type onto target (FE bootstrap), not invent.

    Exact string match always counts. Case-insensitive match counts only when
    both sides have the same *known* integer/float width — never when either
    side is bare logical ``integer`` / ``float`` (width unknown → invent 64-bit).
    """
    a = (stamped or "").strip()
    b = (src or "").strip()
    if not a or not b:
        return False
    if a == b:
        return True
    if a.upper().replace(" ", "") != b.upper().replace(" ", ""):
        return False
    from services.type_system import (
        LOGICAL_FLOAT,
        LOGICAL_INTEGER,
        float_mantissa_bits,
        integer_bit_width,
        normalize_logical_type,
    )

    la = normalize_logical_type(a)
    lb = normalize_logical_type(b)
    if la == LOGICAL_INTEGER and lb == LOGICAL_INTEGER:
        wa, wb = integer_bit_width(a), integer_bit_width(b)
        if wa is None or wb is None:
            return False
        return wa == wb
    if la == LOGICAL_FLOAT and lb == LOGICAL_FLOAT:
        fa, fb = float_mantissa_bits(a), float_mantissa_bits(b)
        if fa is None or fb is None:
            return False
        return fa == fb
    # Non numeric: casefold identity still means FE copied the token.
    return True


# Marks a ``target_type`` this Kernel invented (vs. an operator/Studio stamp).
_KERNEL_INVENT = "kernel_invent"


def _kernel_invent_is_stale(row: dict[str, Any], src: str) -> bool:
    """True when our own earlier invent used a now-superseded source type.

    Validate stamps before profiling has typed the payload, so every column of
    a CSV/Parquet/SaaS source looks like TEXT and invents VARCHAR. When the
    profiled type later arrives (DECIMAL(9,4)), that stale VARCHAR would create
    the destination column *and* then be reported as a DECIMAL→VARCHAR fidelity
    collapse — a blocker the product inflicted on itself. Only Kernel-invented
    stamps are refreshed; an operator/Studio stamp is never overridden.
    """
    if str(row.get("target_type_provenance") or "") != _KERNEL_INVENT:
        return False
    prior = str(row.get("target_type_invented_from") or "").strip()
    now = (src or "").strip()
    if not prior or not now or prior == now:
        return False
    from services.type_system import normalize_logical_type

    changed: bool = normalize_logical_type(prior) != normalize_logical_type(now)
    return changed


def _capacity_promoted_stamp(
    row: dict[str, Any],
    stamped: str,
    src_types: dict[str, str],
    dest_db: str,
) -> str:
    """A wider create-new stamp when the judged source no longer fits it, else "".

    A file/document source has no declared DDL, so its type is *profiled*, and
    every stage profiles a different amount of it: Map sees eight sample rows
    and projects ``DECIMAL(6,4)``, Validate profiles fifty and calls the source
    ``DECIMAL(7,4)``, Execute reads the whole file and calls it ``DECIMAL(8,4)``.
    The stamp is frozen at Map, so the gates then compare the source against a
    carrier Dataflow itself projected too narrow and refuse the run for a
    fidelity collapse of its own making — with no destination DDL to protect
    (the table does not exist yet) and no remap the operator could make that is
    more correct than the one we would write.

    So the stamp is re-projected from the type this stage is judging against and
    only ever widened: a fresh projection that is itself lossy leaves the stamp
    alone, and an operator-chosen carrier is never touched.
    """
    if row.get("user_override") or row.get("risk_acknowledged"):
        return ""
    judged = (
        column_type_or_none(src_types, str(row.get("source") or ""))
        or str(row.get("source_type") or "").strip()
    )
    if not judged:
        return ""
    from services.decision_kernel.type_invent import promote_create_new_capacity_stamp

    promoted = promote_create_new_capacity_stamp(judged, stamped, dest_db)
    return promoted if promoted and promoted.upper() != stamped.upper() else ""


def _backfill_widened_type(
    live_type: str,
    row: dict[str, Any],
    src_types: dict[str, str],
    *,
    dest_db: str,
    backfill_new_fields: bool,
) -> str:
    """The source's type when backfill should widen an existing column, else "".

    Binding an existing column to its live carrier is the right default: the
    destination's declared type is fact, and inventing over it would silently
    re-shape a table the operator did not ask to change. ``backfill_new_fields``
    *is* that request, though, and a source column that has grown — MySQL
    ``DECIMAL(8,2)`` widened to ``DECIMAL(12,2)`` upstream — otherwise leaves the
    destination narrow, so the rows that needed the extra digits are quarantined
    as overflow on every subsequent run.

    Only a strictly wider source passes, so this can never narrow a live column,
    and the writer's widen pass still applies its own dialect refusals before any
    ALTER runs.
    """
    if not backfill_new_fields:
        return ""
    source_type = (
        str(row.get("source_type") or "").strip()
        or column_type_or_none(src_types, str(row.get("source") or ""))
        or ""
    )
    if not source_type:
        return ""
    # Function-local: connectors.schema_drift imports the type layer, so a
    # module-level import here would close an import cycle.
    try:
        from connectors.schema_drift import is_wider_type
    except ImportError:
        return ""
    try:
        if is_wider_type(str(live_type), source_type, dest_db=dest_db):
            return source_type
    except (ValueError, TypeError, KeyError):
        return ""
    return ""


def stamp_additive_mapping_types(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str,
    live_dest_types: dict[str, str] | None = None,
    source_types: dict[str, str] | None = None,
    samples_by_source: dict[str, list[Any]] | None = None,
    backfill_new_fields: bool = False,
    dest_table_exists: bool | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Stamp Map ``target_type`` for additive / create-new columns (Decision Kernel).

    Fail-closed honesty:
    * ``pending_dest_schema`` — never invent (Studio must reload).
    * Live dest carrier present — bind that stamp; never invent from source.
    * Column absent from live types + (``create_new`` / create strategies /
      ``backfill_new_fields`` / ``dest_table_exists is False``) — invent via
      :func:`invent_dest_type` ``CREATE_NEW``.
    * Column absent + no create/backfill authority — leave empty (Execute
      refuses Map VARCHAR ADD). Not reported as ``unstamped`` — that list is
      invent-required-but-failed only (Property 2: legitimate create-new must
      not be blocked by empty-stamp noise).

    Returns ``(stamped_mappings, unstamped_additive_targets)``.
    """
    rows = [dict(m) for m in (mappings or []) if isinstance(m, dict)]
    live = {
        str(k): str(v)
        for k, v in (live_dest_types or {}).items()
        if k and str(v or "").strip()
    }
    live_fold = {k.lower(): v for k, v in live.items()}
    src_types = source_types or {}
    samples = samples_by_source or {}
    unstamped: list[str] = []
    db = (dest_db or "").strip()
    # Missing destination object → CREATE TABLE invent (never-narrower stamps).
    create_table_authority = dest_table_exists is False

    for row in rows:
        strategy = str(row.get("assignment_strategy") or "").strip()
        if strategy == "pending_dest_schema":
            # Honesty: clear any invented create-new residue until Studio loads.
            row["target_type"] = ""
            row.pop("dest_type", None)
            row["create_new"] = False
            continue
        if is_intentional_omit(row):
            continue
        tgt = str(row.get("target") or "").strip()
        if not tgt:
            continue
        live_hit = live.get(tgt) or live_fold.get(tgt.lower())
        stamped = str(row.get("target_type") or row.get("dest_type") or "").strip()
        if live_hit:
            # Bind-existing authority — live carrier always wins over Map invent.
            widened = _backfill_widened_type(
                live_hit,
                row,
                src_types,
                dest_db=db,
                backfill_new_fields=backfill_new_fields,
            )
            row["target_type"] = widened or str(live_hit)
            row["create_new"] = False
            if widened:
                # The ALTER is planned, not performed here: the writer's
                # widen pass owns the DDL and keeps its own refusals.
                row["assignment_strategy"] = "backfill_widen_existing"
            elif strategy in {"create_compatible_new", "identity_passthrough"}:
                row["assignment_strategy"] = "bind_existing"
            continue
        is_create = bool(
            row.get("create_new")
            or row.get("createNew")
            or strategy
            in {
                "create_compatible_new",
                "identity_passthrough",
            }
            or backfill_new_fields
            or create_table_authority
        )
        # Case-tolerant: Oracle/Snowflake catalogs fold to upper case while the
        # mapping carries the operator's case — an exact-key miss used to fall
        # through to "TEXT" and invent a text column for live NUMBER(12,2).
        src = (
            str(row.get("source_type") or "").strip()
            or column_type_or_none(src_types, str(row.get("source") or ""))
            or "TEXT"
        )
        # Source-as-dest FE bootstrap (target_type == source_type) is NOT Kernel
        # invent — must still run invent_dest_type (BQ UUID→STRING, etc.).
        # Casefold alone is unsafe: bare logical ``integer`` uppercases to the
        # same token as physical INT32 ``INTEGER``, which would re-invent a
        # 32-bit column and undo never-narrower invent (audit ITEM 1).
        source_identity_stamp = _is_source_as_dest_bootstrap(stamped, src)
        stale_invent = _kernel_invent_is_stale(row, src)
        if stamped and not (is_create and (source_identity_stamp or stale_invent)):
            if is_create and not row.get("create_new"):
                row["create_new"] = True
            if is_create:
                promoted = _capacity_promoted_stamp(row, stamped, src_types, db)
                if promoted:
                    row["target_type"] = promoted
            continue
        if not is_create:
            # No invent authority — leave blank; do not report as stamp failure.
            continue
        src_key = str(row.get("source") or "")
        col_samples = list(samples.get(src_key) or [])[:32] or None
        try:
            invented = invent_dest_type(
                src,
                dest_db=db,
                context=InventContext.CREATE_NEW,
                samples=col_samples,
            )
        except InventRefused:
            unstamped.append(tgt)
            continue
        if not str(invented or "").strip():
            unstamped.append(tgt)
            continue
        row["target_type"] = str(invented)
        # Provenance so a later pass with a richer source type can re-invent
        # (first Validate pass often sees every file column as TEXT).
        row["target_type_provenance"] = _KERNEL_INVENT
        row["target_type_invented_from"] = src
        row["create_new"] = True
        if not strategy:
            row["assignment_strategy"] = "create_compatible_new"
    return rows, unstamped


__all__ = [
    "InventContext",
    "InventRefused",
    "invent_context_from_sync_mode",
    "invent_dest_type",
    "stamp_additive_mapping_types",
]
