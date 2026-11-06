"""Destination keys a complete CDC snapshot did not present (E3-006).

A CDC run that (re-)snapshots into a destination that already holds rows —
first CDC run after a plain overwrite, a restart whose slot is gone, a
``when_needed`` gap recovery — upserts the current source image and left every
destination row whose source row was deleted while no stream ran. The delete
event is gone, so only the snapshot can prove it: a key the destination holds
that a *complete* snapshot did not present was deleted at the source.

Removing those keys is a destructive inference, so it runs only when it is
provably the same table seen twice:

* a table-mode source (a query / procedure extract yields the query's rows,
  not the table's — every destination key outside the query would go);
* no Map transform on a primary-key column (the census spools the cells the
  writer binds; a transformed key could not be matched);
* the destination table written by one stream of this request;
* a recovery snapshot (no position, purged log) — ``snapshot_mode=always``
  re-snapshots every run and does not imply removal unless the contract says
  so.

The stream contract's mirror vocabulary decides the rest: ``delete_mode``
``hard`` (default when a sweep is safe; CDC applies DELETE events as hard
deletes), ``soft`` (``_deleted`` tombstone) or ``off`` (keep, report). When
the sweep does not run, the run still measures and *discloses* the count:
"N keys at the destination are not in the source; not removed because ...",
with the next action. The delete itself is the mirror kernel
(``mirror_engine.apply_inferred_soft_deletes``) — one implementation of
"destination keys absent from a snapshot" — with its active-checksum pass off.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

#: Destinations whose rows the mirror kernel can delete set-based.
SWEEP_FAMILIES = frozenset({"postgresql", "mysql", "sqlserver", "oracle", "sqlite"})
#: Snapshot-plan reasons that mean "the position was lost, this dump is the
#: recovery image". ``always`` and ``initial_only`` are not recoveries.
RECOVERY_REASONS = frozenset(
    {"watermark_missing", "retention_gap", "retention_gap_when_needed"}
)
GATE_ID = "g18b_cdc_snapshot_leftover"
#: Plan-time key census cap: the destination key list is bounded by
#: ``dest_precount._KEYSET_CENSUS_MAX`` (unlisted above it) and the source
#: walk reads at most one key more than that; beyond either side the count is
#: "unmeasured". The source walk runs under its own ``bounded_step`` budget.
PLAN_PROBE_SECONDS = 20.0
#: What no probe can prove: another job or route writing the same table.
SOLE_WRITER_ASSUMPTION = (
    "This assumes no other route writes this table; a key another job adds "
    "between the snapshot and the sweep would be removed."
)


@dataclass(frozen=True)
class SweepPlan:
    """What this run does with destination keys the snapshot does not present."""

    mode: str  # hard | soft | off
    declared: bool
    allowed: bool
    reason: str = ""
    next_action: str = ""

    def label(self) -> str:
        return f"delete_mode={self.mode}" + ("" if self.declared else " (default)")


def key_value_rewritten(transform: Any) -> bool:
    """True when a Map transform changes a key's *value*, not only its carrier type.

    The engine's own classification: identity spellings and typed carriers
    (``integer``, ``decimal``, ``uuid`` ...) parse the cell into a concrete type
    the destination engine compares as equal to the stored key. Anything else
    (``trim``, ``upper``, ``hash_pii``, ``replace:``, ``default:`` ...) binds a
    cell the raw snapshot key cannot be matched to.
    """
    from services.transform_engine import (
        _IDENTITY_TRANSFORMS,
        _TYPED_TRANSFORMS,
        normalize_engine_transform,
    )

    raw = str(transform or "").strip()
    if not raw:
        return False
    canonical = normalize_engine_transform(raw) or raw.lower()
    return canonical not in _IDENTITY_TRANSFORMS and canonical not in _TYPED_TRANSFORMS


def pk_transforms(mappings: list[dict[str, Any]] | None, pk_sources: list[str]) -> list[str]:
    """Primary-key source columns a Map transform rewrites before the write."""
    wanted = {str(c).strip().lower() for c in pk_sources if str(c or "").strip()}
    hit: list[str] = []
    for m in mappings or []:
        if not isinstance(m, dict):
            continue
        source = str(m.get("source") or "").strip()
        if source.lower() not in wanted:
            continue
        if key_value_rewritten(m.get("transform")) and source not in hit:
            hit.append(source)
    return hit


def plan_snapshot_sweep(
    plan: dict[str, Any] | None,
    *,
    dest_type: str,
    dest_absent_before: bool | None,
    source_cfg: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None,
    pk_sources: list[str],
    delete_mode: str | None,
) -> SweepPlan | None:
    """The sweep decision, or ``None`` when there is nothing to measure.

    ``None``: this run does not take a complete snapshot into a populated SQL
    table, so the leftover question does not arise. Otherwise the keys are
    always censused; ``allowed`` says whether they may be removed. An unknown
    ``delete_mode`` raises :class:`services.transfer_policy_values.PolicyValueError`.
    """
    from services.procedure_source import is_callable_source
    from services.transfer_policy_values import delete_mode_for
    from src.transfer.engine_family import engine_family

    if not isinstance(plan, dict) or not plan.get("run_snapshot"):
        return None
    if plan.get("snapshot_dump_open") or plan.get("reason") == "snapshot_in_progress":
        return None
    if dest_absent_before is not False:
        # A table this run created holds only snapshot rows; unknown is not "populated".
        return None
    if engine_family(dest_type) not in SWEEP_FAMILIES:
        return None
    declared = delete_mode_for(delete_mode, "cdc")
    mode = declared or "hard"
    keep = "declare delete_mode=off on the stream contract to keep them"
    if mode == "off":
        return SweepPlan(
            mode, True, False,
            reason="delete_mode=off is declared on the stream contract",
            next_action="declare delete_mode=hard (or soft) to remove them",
        )
    if not pk_sources:
        return SweepPlan(mode, bool(declared), False, reason="the stream declares no primary key")
    if is_callable_source(source_cfg):
        return SweepPlan(
            mode, bool(declared), False,
            reason=(
                "the source is a query/procedure extract, not a table, so its "
                "rows are not the whole table"
            ),
            next_action=f"read the table itself to remove them, or {keep}",
        )
    rewritten = pk_transforms(mappings, pk_sources)
    if rewritten:
        return SweepPlan(
            mode, bool(declared), False,
            reason=(
                f"a Map transform rewrites primary key column(s) {', '.join(rewritten)}, "
                "so snapshot keys cannot be matched to destination keys"
            ),
            next_action=f"remove the transform from the key column(s), or {keep}",
        )
    if not declared and str(plan.get("reason") or "") not in RECOVERY_REASONS:
        return SweepPlan(
            mode, False, False,
            reason=(
                f"snapshot_mode={plan.get('snapshot_mode')} re-snapshots every run "
                "and does not imply removal"
            ),
            next_action=(
                "declare delete_mode=hard (or soft) on the stream contract to remove "
                f"them on every snapshot, or {keep}"
            ),
        )
    return SweepPlan(mode, bool(declared), True)


class SnapshotKeyCensus:
    """Primary keys a CDC snapshot presented, spooled (disk past the spill cap)."""

    def __init__(self, pk_sources: list[str], plan: SweepPlan):
        from connectors.source_row_spool import SourceRowSpool

        self.pk_sources = [str(c) for c in pk_sources if str(c or "").strip()]
        self.plan = plan
        self.spool = SourceRowSpool()
        self.spool.headers = list(self.pk_sources)
        self.complete = bool(self.pk_sources)

    def add(self, change: Any) -> None:
        """Spool the keys of one snapshot batch — the cells the writer binds.

        A key column has no Map transform here (``plan_snapshot_sweep`` refuses
        otherwise), so the raw cell is the bound cell; the destination engine
        compares it against the stored key.
        """
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

    def settle(
        self,
        destination: Any,
        *,
        mappings: list[dict[str, Any]] | None,
        pk_target_cols: list[str],
    ) -> dict[str, Any]:
        """Remove — or only count — destination keys absent from the snapshot.

        Returns the finding the run records: ``leftover`` (keys at the
        destination the snapshot did not present), ``removed``, the delete
        mode, and the operator text.
        """
        from services.mirror_engine import COUNT_ONLY, apply_inferred_soft_deletes

        base = {
            "delete_mode": self.plan.mode,
            "declared": self.plan.declared,
            "reason": self.plan.reason,
            "next_action": self.plan.next_action,
        }
        if not self.complete or self.spool.row_count <= 0 or not pk_target_cols:
            why = (
                "a snapshot row carried no complete key"
                if not self.complete
                else "the snapshot presented no keys"
            )
            return {
                **base, "status": "unmeasured", "leftover": None, "removed": 0,
                "finding": (
                    "Destination keys the source no longer has were not measured: "
                    f"{why}; nothing was removed."
                ),
            }
        mode = self.plan.mode if self.plan.allowed else COUNT_ONLY
        census = apply_inferred_soft_deletes(
            destination,
            [],
            list(pk_target_cols),
            None,
            mappings,
            list(pk_target_cols),
            source_key_spool=self.spool,
            pk_sources=self.pk_sources,
            delete_mode=mode,
            active_checksum=False,
        )
        if mode == COUNT_ONLY:
            leftover = int(census.get("would_delete") or 0)
            removed = 0
        else:
            removed = int(census.get("hard_deleted") or census.get("soft_deleted") or 0)
            leftover = removed
        if leftover == 0:
            finding = "Every destination key is in the source snapshot; nothing to remove."
            status = "none"
        elif removed:
            verb = "removed" if mode == "hard" else "tombstoned"
            finding = (
                f"CDC snapshot {verb} {removed} destination row(s) whose key the source "
                f"no longer has (deleted while the stream was stopped; {self.plan.label()}). "
                f"{SOLE_WRITER_ASSUMPTION}"
            )
            status = "removed"
        else:
            finding = (
                f"{leftover} key(s) at the destination are not in the source; not removed "
                f"because {self.plan.reason}."
                + (f" Next: {self.plan.next_action}." if self.plan.next_action else "")
            )
            status = "kept"
        return {**base, "status": status, "leftover": leftover, "removed": removed, "finding": finding}

    def close(self) -> None:
        try:
            self.spool.close()
        except Exception:  # noqa: BLE001
            logger.debug("snapshot key spool close failed", exc_info=True)


def _as_endpoint(endpoint: Any) -> Any:
    """An ``EndpointConfig`` for a plan-time endpoint that may still be a dict.

    REST Validate, MCP plan_transfer and transfer_plan_service hand the policy
    gates plain dicts; the sibling continuity gate coerces the same way.
    """
    if endpoint is None or not isinstance(endpoint, dict):
        return endpoint
    from src.transfer.models import EndpointConfig

    return EndpointConfig.from_dict(str(endpoint.get("kind") or "database"), endpoint)


def plan_probe_key_cap() -> int:
    from services.dest_precount import _KEYSET_CENSUS_MAX

    return int(_KEYSET_CENSUS_MAX)


def _plan_time_leftover(
    source_endpoint: Any,
    destination_endpoint: Any,
    *,
    pk_sources: list[str],
    pk_targets: list[str],
    dest_type: str,
) -> int | None:
    """Destination keys not in the source, counted at plan time; ``None`` = unmeasured.

    Bounded on both sides by :func:`plan_probe_key_cap` (the destination list
    is unlisted above it; the source walk stops one key past it) and in time
    by ``bounded_step`` inside the caller's tool deadline. Keys compare typed
    (``type_system.unique_equality_key``), not as text.
    """
    try:
        from services.dest_precount import destination_key_list
        from services.dialect_profiles import schema_from_cfg
        from services.type_system import unique_equality_key
        from src.ai.copilot.tool_deadline import bounded_step
        from src.transfer.adapters import resolve_connector_config, resolve_dest_table
        from src.transfer.source_peek import iter_stream_source_column_rows

        cap = plan_probe_key_cap()
        dest_cfg = resolve_connector_config(destination_endpoint)
        table = resolve_dest_table(dest_type, destination_endpoint, "")
        if not table:
            return None

        def _typed(values: Any) -> tuple[str, ...]:
            return tuple(str(unique_equality_key(v)) for v in values)

        def _census() -> int | None:
            dest_keys = destination_key_list(
                dest_type, dest_cfg, schema=str(schema_from_cfg(dest_type, dest_cfg) or ""),
                table_name=str(table), key_columns=pk_targets,
            )
            if dest_keys is None:  # unlisted above the cap
                return None
            remaining = {_typed(k) for k in dest_keys}
            seen = 0
            for row in iter_stream_source_column_rows(
                source_endpoint, list(pk_sources), limit=cap + 1
            ):
                seen += 1
                if seen > cap:
                    return None
                remaining.discard(_typed(row.get(c) for c in pk_sources))
            if seen == 0:
                return None  # cursor-paged source: the walk yields nothing, not "no rows"
            return len(remaining)

        step = bounded_step("cdc snapshot leftover census", _census, seconds=PLAN_PROBE_SECONDS)
        value = getattr(step, "value", None)
        return int(value) if isinstance(value, int) else None
    except Exception as exc:  # noqa: BLE001 — a plan-time probe never blocks
        logger.debug("snapshot leftover plan probe skipped: %s", exc)
        return None


def build_snapshot_leftover_preflight_gate(
    *,
    snapshot_gate: dict[str, Any] | None,
    stream_contracts: list[dict[str, Any]] | None,
    source_endpoint: Any,
    destination_endpoint: Any,
    source_config: dict[str, Any] | None,
    mappings: list[dict[str, Any]] | None,
    dest_type: str,
    catalog_primary_key_columns: list[str] | None = None,
) -> dict[str, Any] | None:
    """Plan-time: say what the snapshot will do with keys the source no longer has.

    Runs only when the g18 snapshot gate says a snapshot will run and the
    destination is an existing SQL table. ``warn`` names the count (when
    measurable), whether it will be removed, and the next action — before the
    job, not after it.

    The key is the contract's, else the source catalog's (X-29: an API-created
    CDC schedule declares none and the run binds the catalog key). With neither
    there is no census to plan; the CDC capture gate names that refusal.
    """
    details = (snapshot_gate or {}).get("details") if isinstance(snapshot_gate, dict) else None
    if not isinstance(details, dict) or not details.get("run_snapshot"):
        return None
    if details.get("snapshot_dump_open"):
        return None  # Execute never sweeps after a resumed dump; Validate says nothing either
    from services.cdc_snapshot_window import _pk_columns
    from services.sync_cursor import map_source_to_target, resolve_selected_sync_contracts

    source_endpoint = _as_endpoint(source_endpoint)
    destination_endpoint = _as_endpoint(destination_endpoint)
    contracts = resolve_selected_sync_contracts(stream_contracts)
    if len(contracts) != 1 or destination_endpoint is None:
        return None
    contract = contracts[0]
    declared = getattr(contract, "primary_key", None) or ""
    catalog = [str(c).strip() for c in (catalog_primary_key_columns or []) if str(c).strip()]
    if not (declared if isinstance(declared, (list, tuple)) else str(declared).strip()) and not catalog:
        return None
    pk_sources = _pk_columns(declared or catalog)
    maps = list(mappings or [])
    pk_targets = [map_source_to_target(c, maps) or c for c in pk_sources]
    try:
        from services.preflight_cursor_gate import dest_object_missing
        from src.transfer.adapters import resolve_connector_config, resolve_dest_table

        dest_cfg = resolve_connector_config(destination_endpoint)
        table = resolve_dest_table(dest_type, destination_endpoint, "")
        missing = dest_object_missing(dest_type, dest_cfg, str(table or "")) if table else None
    except Exception:  # noqa: BLE001
        missing = None
    if missing is not False:
        return None
    # Validate ≡ Execute: the same kernel verdict the run will take
    # (g18 stamps classify_snapshot_plan's reason), never a hardcoded one.
    plan = {
        "run_snapshot": True,
        "reason": str(details.get("snapshot_plan_reason") or ""),
        "snapshot_mode": details.get("snapshot_mode"),
        "snapshot_dump_open": bool(details.get("snapshot_dump_open")),
    }
    try:
        sweep = plan_snapshot_sweep(
            plan, dest_type=dest_type, dest_absent_before=False, source_cfg=source_config,
            mappings=maps, pk_sources=pk_sources,
            delete_mode=str(getattr(contract, "delete_mode", "") or ""),
        )
    except ValueError as exc:
        return {"id": GATE_ID, "status": "block", "message": str(exc), "duration_ms": 0, "details": {}}
    if sweep is None:
        return None
    count = _plan_time_leftover(
        source_endpoint, destination_endpoint,
        pk_sources=pk_sources, pk_targets=pk_targets, dest_type=dest_type,
    )
    n = (
        f"an unmeasured number of (census bounded to {plan_probe_key_cap():,} keys a side)"
        if count is None
        else str(count)
    )
    if count == 0:
        status, message = "pass", "Every destination key is in the source; the CDC snapshot removes nothing."
    elif sweep.allowed:
        status = "warn"
        message = (
            f"The CDC snapshot will remove {n} destination key(s) the source no longer has "
            f"({sweep.label()}). Declare delete_mode=off on the stream contract to keep them. "
            f"{SOLE_WRITER_ASSUMPTION}"
        )
    else:
        status = "warn"
        message = (
            f"{n} destination key(s) are not in the source and will NOT be removed by the "
            f"CDC snapshot because {sweep.reason}."
            + (f" Next: {sweep.next_action}." if sweep.next_action else "")
        )
    return {
        "id": GATE_ID,
        "status": status,
        "message": message,
        "duration_ms": 0,
        "details": {
            "leftover_keys": count,
            "will_remove": bool(sweep.allowed and count != 0),
            "delete_mode": sweep.mode,
            "delete_mode_declared": sweep.declared,
            "reason": sweep.reason,
            "next_action": sweep.next_action,
        },
    }
