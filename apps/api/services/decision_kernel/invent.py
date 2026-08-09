"""Context-aware invent (Phase C5).

Same ConversionClass does **not** imply the same DDL in every execution mode.
Create-new may widen safely; bind-existing / CDC sparse / append must never
invent destination capacity the live schema did not prove.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


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
        # Materialize for dialect wire; do not widen beyond the proven stamp.
        return str(materialize_dest_ddl(db, existing) or existing)

    # CREATE_NEW / FULL_REFRESH — safe widen invent.
    if existing and ctx is InventContext.FULL_REFRESH:
        # Refreshing into an existing object: bind authority wins.
        return str(materialize_dest_ddl(db, existing) or existing)

    from services.decision_kernel.types import normalize_logical_type
    from services.type_system import integer_bit_width

    stamped = create_new_mapping_target_type(src, db, samples=samples)
    logical = normalize_logical_type(src)
    # Bare INTEGER/INT32 invent must never undercut the 64-bit create-new floor
    # (audit §2.1 / Phase A). Prefer ddl_type(logical) when stamp is narrower.
    if logical == "integer":
        safe = str(ddl_type(db, "integer") or "BIGINT")
        if not stamped:
            return safe
        sw = integer_bit_width(stamped)
        ww = integer_bit_width(safe)
        if sw is not None and ww is not None and sw < ww:
            return safe
        if str(stamped).upper().replace(" ", "") in {
            "INTEGER",
            "INT",
            "INT32",
            "SIGNED",
        }:
            return safe
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


def stamp_additive_mapping_types(
    mappings: list[dict[str, Any]] | None,
    *,
    dest_db: str,
    live_dest_types: dict[str, str] | None = None,
    source_types: dict[str, str] | None = None,
    samples_by_source: dict[str, list[Any]] | None = None,
    backfill_new_fields: bool = False,
) -> tuple[list[dict[str, Any]], list[str]]:
    """Stamp Map ``target_type`` for additive / create-new columns (Decision Kernel).

    Fail-closed honesty:
    * ``pending_dest_schema`` — never invent (Studio must reload).
    * Live dest carrier present — bind that stamp; never invent from source.
    * Column absent from live types + (``create_new`` / create strategies /
      ``backfill_new_fields``) — invent via :func:`invent_dest_type` ``CREATE_NEW``.
    * Column absent + no create/backfill authority — leave empty (Validate/write
      refuse Map VARCHAR ADD invent).

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

    for row in rows:
        strategy = str(row.get("assignment_strategy") or "").strip()
        if strategy == "pending_dest_schema":
            # Honesty: clear any invented create-new residue until Studio loads.
            row["target_type"] = ""
            row.pop("dest_type", None)
            row["create_new"] = False
            continue
        if row.get("intentional_omit") or row.get("intentionalOmit"):
            continue
        if str(row.get("transform") or "").lower() in {
            "omit",
            "intentional_omit",
            "drop",
            "exclude",
        }:
            continue
        tgt = str(row.get("target") or "").strip()
        if not tgt:
            continue
        live_hit = live.get(tgt) or live_fold.get(tgt.lower())
        stamped = str(row.get("target_type") or row.get("dest_type") or "").strip()
        if live_hit:
            # Bind-existing authority — live carrier always wins over Map invent.
            row["target_type"] = str(live_hit)
            row["create_new"] = False
            if strategy in {"create_compatible_new", "identity_passthrough"}:
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
        )
        if stamped:
            if is_create and not row.get("create_new"):
                row["create_new"] = True
            continue
        if not is_create:
            unstamped.append(tgt)
            continue
        src = (
            str(row.get("source_type") or "").strip()
            or str(src_types.get(str(row.get("source") or "")) or "").strip()
            or "TEXT"
        )
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
