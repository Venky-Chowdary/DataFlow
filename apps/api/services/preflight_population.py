from __future__ import annotations

from typing import Any

from preflight.models import GateStatus
from services.db_type_utils import normalize_dest_kind

def _cdc_log_capture_gates(
    contracts: list[dict[str, Any]],
    *,
    source_type: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    catalog_primary_key_columns: list[str] | None,
    mappings: list[dict[str, Any]] | None,
) -> list[dict[str, Any]]:
    """One g9c gate per selected stream: can the run read the source change log."""
    from services.cdc_log_capture_probe import build_log_capture_gate, probe_log_capture
    from services.preflight_cursor_gate import (
        catalog_identity_applies,
        resolved_stream_identity_columns,
    )

    if not source_config:
        return []
    streams = contracts or [{"name": source_table}]
    gates: list[dict[str, Any]] = []
    for contract in streams[:5]:
        table = str(
            (source_table if len(streams) == 1 else "")
            or contract.get("name")
            or contract.get("stream")
            or ""
        ).strip()
        keys = resolved_stream_identity_columns(
            contract,
            catalog_primary_key_columns=(
                catalog_primary_key_columns
                if catalog_identity_applies(contract, source_table, len(streams))
                else None
            ),
            mappings=mappings,
        )
        if not table or not keys:
            continue
        probe = probe_log_capture(
            source_type,
            dict(source_config),
            table=table,
            schema=str(source_config.get("schema") or ""),
            primary_key=",".join(keys),
        )
        gate = build_log_capture_gate(
            probe,
            cursor_field=str(contract.get("cursor_field") or contract.get("cursor") or ""),
            cursor_semantics=str(contract.get("cursor_semantics") or ""),
            primary_key_columns=keys,
            pass_status=GateStatus.PASS.value,
            block_status=GateStatus.BLOCK.value,
        )
        if gate is not None:
            gates.append({**gate, "details": {**gate.get("details", {}), "stream": table}})
    if len(gates) <= 1:
        return gates
    blocked = [g for g in gates if g.get("status") == GateStatus.BLOCK.value]
    chosen = blocked or [g for g in gates if g.get("severity") == "warn"] or gates
    return [
        {
            **chosen[0],
            "message": " ".join(
                f"{g['details'].get('stream')}: {g['message']}" for g in chosen
            ),
            "details": {"streams": [g["details"] for g in gates]},
        }
    ]


def _iter_table_population_for_preflight(
    *,
    source_kind: str,
    source_format: str,
    source_connector_id: str,
    source_config: dict[str, Any] | None,
    source_table: str,
    mappings: list[dict[str, Any]],
    column_types: dict[str, str],
    dest_types: dict[str, str],
    dest_db: str,
    sync_mode: str = "",
    read_scope: Any = None,
    source_filter: dict[str, Any] | None = None,
):
    """Same projected table walk Execute uses — or None when it cannot run.

    Studio/plan/Pilot used to scan only the preview. A late DECIMAL overflow
    then passed Validate and blocked Execute. Callable extracts, row filters,
    and file sources stay out: those populations are not this walk.
    """
    kind = (source_kind or "").strip().lower()
    if kind not in {"database", "cloud"}:
        return None
    table = str(source_table or "").strip()
    if not table:
        return None
    if not (source_connector_id or source_config):
        return None
    from services.procedure_source import is_callable_source

    if is_callable_source(source_config):
        return None
    cfg = dict(source_config or {})
    extra = cfg.get("extra") if isinstance(cfg.get("extra"), dict) else {}
    from services.sync_cursor import normalize_sync_mode

    if normalize_sync_mode(sync_mode, default="") == "cdc":
        # CDC writes a changelog, not a table rescan. Walking the table
        # judges the wrong population — preview stays, never exact.
        return None
    try:
        from src.transfer.models import EndpointConfig
        from src.transfer.source_peek import iter_bounded_table_population_rows
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from transfer.models import EndpointConfig
        from transfer.source_peek import iter_bounded_table_population_rows

    data = dict(cfg)
    if source_connector_id:
        data["connector_id"] = source_connector_id
    data.setdefault("table", table)
    data.setdefault("collection", table)
    if source_format:
        data.setdefault("format", source_format)
        data.setdefault("type", source_format)
    endpoint = EndpointConfig.from_dict(
        kind if kind in {"database", "cloud"} else "database",
        data,
    )
    return iter_bounded_table_population_rows(
        endpoint,
        mappings,
        column_types=column_types,
        dest_types=dest_types,
        dest_db=dest_db,
        source_kind=source_kind,
        source_format=source_format or str(data.get("format") or ""),
        read_scope=read_scope,
        source_filter=source_filter
        or (cfg.get("source_filter") if isinstance(cfg.get("source_filter"), dict) else None)
        or (extra.get("source_filter") if isinstance(extra.get("source_filter"), dict) else None),
    )


def _apply_overwrite_emptied_gate(
    out: dict[str, Any],
    *,
    sync_mode: str,
    destination_table_exists: bool | None,
    live_dest_columns: list[str],
    mappings: list[dict[str, Any]] | None,
    regenerated: list[str],
    schema_policy: str,
    acknowledged: bool,
    dest_kind: str,
    validation_mode: str,
) -> None:
    """Name the destination values an overwrite will empty (DEF-C-025).

    MySQL and PostgreSQL keep a destination column no mapping writes (other
    engines recreate without it), but either way the reload replaces every row
    and the values it holds now are gone. That needs no schema history to see:
    the live table and the mapping already say it. Under review policies it is a decision, otherwise a
    warning — never a silent loss.
    """
    from services.overwrite_keep import overwrite_emptied_columns
    from services.sync_cursor import is_overwrite_sync

    from services.db_type_utils import overwrite_replaces_rows

    if (
        destination_table_exists is not True
        or not is_overwrite_sync(sync_mode)
        or not overwrite_replaces_rows(dest_kind)
    ):
        return
    skip = {str(c).casefold() for c in regenerated}
    if normalize_dest_kind(dest_kind) == "redis":
        from connectors.redis_reader import REDIS_ENVELOPE_COLUMNS

        # The reader's key/type envelope is not stored data: an overwrite
        # rebuilds every key under the prefix, so there is no value to lose.
        skip |= set(REDIS_ENVELOPE_COLUMNS)
    emptied = [
        c for c in overwrite_emptied_columns(live_dest_columns, mappings)
        if c.casefold() not in skip
    ]
    if not emptied:
        return
    shown = ", ".join(emptied[:8]) + (f" (+{len(emptied) - 8} more)" if len(emptied) > 8 else "")
    message = (
        f"Overwrite empties {len(emptied)} destination column(s) no mapping writes "
        f"({shown}): every row is replaced and nothing writes these columns, so "
        "the values they hold now are lost"
    )
    details = {
        "columns": emptied,
        "sync_mode": sync_mode,
        "schema_policy": schema_policy,
        "rule_id": "schema_drift.overwrite_emptied_columns",
    }
    policy = (schema_policy or "manual_review").strip().lower()
    if acknowledged or policy not in {"manual_review", "pause_on_change", "type_locked"}:
        out.setdefault("warnings", []).append(
            {"id": "overwrite_emptied_columns", "message": message, "details": details}
        )
        return
    message += " — map a source column to it, or acknowledge the schema change to empty it"
    details.update({"remediation_kind": "acknowledge_schema_drift", "ack_required": True})
    gate = {
        "id": "overwrite_emptied_columns",
        "status": "block",
        "message": message,
        "duration_ms": 0,
        "details": details,
    }
    from services.preflight_rules import enrich_blockers

    out["gates"] = [*out.get("gates", []), gate]
    out["blockers"] = [
        *out.get("blockers", []),
        *enrich_blockers(
            [{"id": gate["id"], "message": message, "details": details}],
            dest_kind=dest_kind,
            validation_mode=validation_mode,
        ),
    ]
    out["passed"] = False
    out["passed_count"] = sum(1 for g in out["gates"] if g.get("status") == "pass")
    out["total_gates"] = len(out["gates"])
    out["readiness_score"] = round(out["passed_count"] / max(out["total_gates"], 1) * 100, 1)
