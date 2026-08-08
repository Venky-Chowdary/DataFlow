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
    from services.decision_kernel.types import ddl_type, materialize_dest_ddl
    from services.type_system import create_new_mapping_target_type

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


__all__ = [
    "InventContext",
    "InventRefused",
    "invent_context_from_sync_mode",
    "invent_dest_type",
]
