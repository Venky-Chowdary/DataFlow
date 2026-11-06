"""Remove destination keys a complete CDC snapshot no longer presents (E3-006).

A CDC run that (re-)snapshots into a destination that already holds rows —
first CDC run after a plain overwrite, a restart whose slot is gone, a
``when_needed`` gap recovery — upserted the current source image and left
every destination row whose source row was deleted while no stream was
running. The run then reported success with a row the source no longer has
(PG -> MySQL "source 54, dest 55"; SQL Server CT -> PG "delete of id 2 never
applied"). The delete event is gone, so only the snapshot can prove it: a
key the destination holds that a *complete* snapshot did not present was
deleted at the source.

The sweep runs after the snapshot finished and before streaming starts, so
the destination equals the source image at the handoff position; rows the
stream adds later are not on the destination yet. It never runs after a
resumed (partial) dump, whose earlier chunks this run did not see. The delete
itself is the mirror kernel (``mirror_engine.apply_inferred_soft_deletes``,
hard mode) — one implementation of "destination keys absent from a snapshot".
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

#: Destinations whose rows the mirror kernel can delete set-based.
SWEEP_FAMILIES = frozenset({"postgresql", "mysql", "sqlserver", "oracle", "sqlite"})


def sweep_eligible(plan: dict[str, Any] | None, dest_type: str, *, dest_absent_before: bool | None) -> bool:
    """Whether this run's snapshot is a complete image of a populated destination."""
    from src.transfer.engine_family import engine_family

    if not isinstance(plan, dict) or not plan.get("run_snapshot"):
        return False
    if plan.get("snapshot_dump_open") or plan.get("reason") == "snapshot_in_progress":
        return False
    if dest_absent_before is not False:
        # A table this run created holds only snapshot rows; unknown is not "populated".
        return False
    return engine_family(dest_type) in SWEEP_FAMILIES


class SnapshotKeyCensus:
    """Primary keys a CDC snapshot presented, spooled (disk past the spill cap)."""

    def __init__(self, pk_sources: list[str]):
        from connectors.source_row_spool import SourceRowSpool

        self.pk_sources = [str(c) for c in pk_sources if str(c or "").strip()]
        self.spool = SourceRowSpool()
        self.spool.headers = list(self.pk_sources)
        self.complete = bool(self.pk_sources)

    def add(self, change: Any) -> None:
        from services.mirror_engine import complete_mirror_pk_tuple

        if not self.complete:
            return
        for rec in list(getattr(change, "inserts", None) or []) + list(
            getattr(change, "updates", None) or []
        ):
            if not isinstance(rec, dict):
                continue
            tup = complete_mirror_pk_tuple(rec.get(c) for c in self.pk_sources)
            if tup is None:
                # A snapshot row without a full key cannot vouch for anything:
                # sweeping on a partial key set would delete real rows.
                self.complete = False
                return
            self.spool.append_row(list(tup))

    def sweep(
        self,
        destination: Any,
        *,
        mappings: list[dict[str, Any]] | None,
        columns: list[str],
        schema: dict[str, str] | None,
        pk_target_cols: list[str],
    ) -> dict[str, Any] | None:
        """Delete destination keys absent from the snapshot; ``None`` when not proven."""
        if not self.complete or self.spool.row_count <= 0 or not pk_target_cols:
            return None
        from services.mirror_engine import apply_inferred_soft_deletes

        census = apply_inferred_soft_deletes(
            destination,
            [],
            columns,
            schema,
            mappings,
            list(pk_target_cols),
            source_key_spool=self.spool,
            pk_sources=self.pk_sources,
            delete_mode="hard",
        )
        return {
            "deleted": int(census.get("hard_deleted") or 0),
            "snapshot_keys": int(census.get("source_key_rows") or 0),
            "dest_rows_after": census.get("active_rows"),
        }

    def close(self) -> None:
        try:
            self.spool.close()
        except Exception:  # noqa: BLE001
            logger.debug("snapshot key spool close failed", exc_info=True)
