from __future__ import annotations

from typing import Any
from src.transfer.engine import (
    TransferRequest,
    logger,
)

def _execute_preflight_parity_kwargs(
    request: TransferRequest,
    *,
    destination_connected: bool,
    destination_table_exists_fallback: bool | None = None,
) -> dict[str, Any]:
    """Validate≡Execute preflight kwargs — privilege, FK, identity, operator acks.

    Never invent ``can_create``/``can_write`` from connectivity alone when a
    privilege probe is available. Contract PK prefers the destination stream name
    (same as ``preflight_router``).

    Owns ``destination_table_exists`` so callers must not also pass that kwarg
    into ``run_file_preflight`` (duplicate keyword → hard Execute failure).
    """
    from services.primary_key import extract_contract_primary_key_columns
    from services.preflight_service import inspect_destination_for_preflight

    dest = request.destination
    dest_table = str(dest.table or dest.collection or "")
    stream_contracts = list(getattr(request, "stream_contracts", None) or [])
    # Full composite contract PK — Validate≡Execute must not truncate to first col.
    # Prefer exact stream match; only fall back to first when a single contract is selected.
    contract_pk_cols = extract_contract_primary_key_columns(
        stream_contracts, stream_name=dest_table, fallback_first=False
    )
    if not contract_pk_cols:
        selected = [
            c
            for c in stream_contracts
            if isinstance(c, dict) and c.get("selected", True)
        ]
        if len(selected) == 1:
            contract_pk_cols = extract_contract_primary_key_columns(selected)
    contract_pk = ",".join(contract_pk_cols) if contract_pk_cols else ""

    extra = dict(getattr(dest, "extra", None) or {})
    dest_meta: dict[str, Any] = {}
    try:
        dest_meta = inspect_destination_for_preflight(
            connector_id=dest.connector_id or None,
            dest_type=dest.format or "",
            dest_host=dest.host or None,
            dest_port=int(dest.port or 0) or None,
            dest_database=dest.database or None,
            dest_table=dest.table or None,
            dest_collection=dest.collection or None,
            dest_schema=getattr(dest, "schema", None) or None,
            dest_username=dest.username or None,
            dest_password=dest.password or None,
            dest_connection_string=dest.connection_string or None,
            dest_warehouse=getattr(dest, "warehouse", None) or None,
            dest_auth_source=getattr(dest, "auth_source", None) or None,
            dest_auth_mode=getattr(dest, "auth_mode", None) or None,
            dest_auth_role=getattr(dest, "auth_role", None) or None,
            dest_api_key=getattr(dest, "api_key", None) or None,
            dest_service_account=getattr(dest, "service_account", None) or None,
            dest_kind=dest.kind or "database",
            dest_extra=dict(getattr(dest, "extra", None) or {}),
        )
    except Exception as exc:
        logger.warning(
            "Execute destination inspect for preflight parity failed: %s",
            exc,
            exc_info=exc,
        )

    pk_cols = list(
        dest_meta.get("primary_key_columns")
        or dest_meta.get("pk_columns")
        or extra.get("primary_key_columns")
        or []
    )
    unique_keys = list(dest_meta.get("unique_keys") or extra.get("unique_keys") or [])
    foreign_keys = list(
        dest_meta.get("foreign_keys")
        or extra.get("foreign_keys")
        or []
    )
    privilege_probe = dict(
        dest_meta.get("privilege_probe") or extra.get("privilege_probe") or {}
    )

    can_create = dest_meta.get("can_create_table")
    if can_create is None and privilege_probe:
        if "can_create_table" in privilege_probe:
            can_create = privilege_probe.get("can_create_table")
        elif "create" in privilege_probe:
            can_create = privilege_probe.get("create")
    if can_create is None:
        # Fail-closed: unknown privilege must not invent create-new DDL.
        can_create = False

    can_write = dest_meta.get("can_write")
    if can_write is None and privilege_probe:
        if "can_write" in privilege_probe:
            can_write = privilege_probe.get("can_write")
        elif "write" in privilege_probe:
            can_write = privilege_probe.get("write")
    if can_write is None:
        # Connectivity ≠ write grant. Unknown → assume writeable only for
        # append paths that still hit live driver errors; never invent create.
        can_write = bool(destination_connected)

    table_exists = dest_meta.get("table_exists")
    if table_exists is None:
        table_exists = destination_table_exists_fallback

    # Keep destination.extra stamped for later gates / theater honesty.
    extra["primary_key_columns"] = pk_cols
    extra["dynamo_key_schema"] = [
        dict(row)
        for row in (
            dest_meta.get("dynamo_key_schema")
            or extra.get("dynamo_key_schema")
            or []
        )
        if isinstance(row, dict)
    ]
    extra["dynamo_index_attributes"] = dict(
        dest_meta.get("dynamo_index_attributes")
        or extra.get("dynamo_index_attributes")
        or {}
    )
    extra["unique_keys"] = unique_keys
    extra["foreign_keys"] = foreign_keys
    if isinstance(table_exists, bool):
        extra["table_exists"] = table_exists
    if privilege_probe:
        extra["privilege_probe"] = privilege_probe
    dest.extra = extra

    return {
        "destination_pk_columns": pk_cols,
        "destination_unique_keys": unique_keys,
        "destination_foreign_keys": foreign_keys,
        # D112 — the live CHECK constraints, so Execute names offending rows
        # before the write instead of surfacing the driver's raw violation.
        "destination_check_constraints": list(dest_meta.get("check_constraints") or []),
        "destination_config": dest_meta.get("_probe_cfg") or None,
        "contract_primary_key": contract_pk,
        "stream_contracts": stream_contracts,
        "privilege_probe": privilege_probe or None,
        "redshift_staging_probe": dest_meta.get("redshift_staging_probe") or None,
        "destination_can_create": bool(can_create),
        "destination_can_write": bool(can_write),
        # Always owned here — never also pass at the call site with **parity.
        "destination_table_exists": table_exists,
        "compliance_acknowledged": bool(
            getattr(request, "compliance_acknowledged", False)
        ),
        "schema_drift_acknowledged": bool(
            getattr(request, "schema_drift_acknowledged", False)
        ),
        "fk_risk_acknowledged": bool(getattr(request, "fk_risk_acknowledged", False)),
        # The DDL Validate disclosed and the operator approved (D101): without
        # it Execute's preflight re-blocked the run Validate had cleared.
        "ddl_changes_acknowledged": bool(
            getattr(request, "ddl_changes_acknowledged", False)
        ),
        "empty_source_overwrite_acknowledged": bool(
            getattr(request, "empty_source_overwrite_acknowledged", False)
        ),
        "acknowledgment_actor": str(
            getattr(request, "acknowledgment_actor", "") or ""
        ).strip(),
        "acknowledgment_reason": str(
            getattr(request, "acknowledgment_reason", "") or ""
        ).strip(),
        "preflight_scope": str(getattr(request, "preflight_scope", "") or "").strip(),
    }


def _catalog_pk_for_policy(
    request: TransferRequest,
    source_table: str,
) -> list[str] | None:
    """Catalog key when this mode needs one and the contract did not name it.

    ``None`` means the contract already carries the key, so Validate must not
    replace it. An empty list means the catalog was read and has no key —
    the same refusal Execute raises.
    """
    from services.preflight_cursor_gate import (
        MODES_REQUIRING_PRIMARY_KEY,
        contract_declares_primary_key,
    )
    from services.sync_cursor import normalize_sync_mode

    sync = normalize_sync_mode(str(getattr(request, "sync_mode", "") or ""), default="")
    if sync not in MODES_REQUIRING_PRIMARY_KEY:
        return None
    selected = [
        c
        for c in (getattr(request, "stream_contracts", None) or [])
        if isinstance(c, dict) and c.get("selected", True)
    ]
    if selected and all(contract_declares_primary_key(c) for c in selected):
        return None
    if not source_table:
        return []
    from services.source_schema_authority import endpoint_primary_key_columns

    return endpoint_primary_key_columns(getattr(request, "source", None))


def _execute_policy_gates_for_request(
    request: TransferRequest,
    *,
    source_columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate≡Execute policy gates — never default source_kind to file.

    Studio Validate passes dest_type / source_type / source_kind / write_via_staging.
    Execute must match or CDC/SCD2/staging falsely block (or skip) after Approve.
    """
    from services.preflight_cursor_gate import read_scope_for_transfer_request
    from services.preflight_service import run_transfer_policy_gates

    dest = getattr(request, "destination", None)
    src = getattr(request, "source", None)
    src_extra = getattr(src, "extra", None) or {}
    mappings = list(getattr(request, "mappings", None) or [])
    source_table = str(
        getattr(src, "table", None) or getattr(src, "collection", None) or ""
    )
    catalog_pk = _catalog_pk_for_policy(request, source_table)
    source_config = None
    if src is not None:
        try:
            from src.transfer.adapters import resolve_connector_config

            source_config = resolve_connector_config(src)
        except Exception as exc:
            logger.debug("CDC policy source config unread: %s", exc)
            source_config = None
    return run_transfer_policy_gates(
        sync_mode=str(getattr(request, "sync_mode", "") or ""),
        schema_policy=str(getattr(request, "schema_policy", "") or "manual_review"),
        validation_mode=str(getattr(request, "validation_mode", "") or "strict"),
        stream_contracts=list(getattr(request, "stream_contracts", None) or []),
        backfill_new_fields=bool(getattr(request, "backfill_new_fields", False)),
        source_columns=list(source_columns or []),
        dest_type=str(getattr(dest, "format", None) or getattr(dest, "kind", None) or ""),
        source_type=str(getattr(src, "format", None) or getattr(src, "kind", None) or ""),
        source_kind=str(getattr(src, "kind", None) or "file"),
        write_via_staging=bool(getattr(request, "write_via_staging", False)),
        source_read_mode=str(src_extra.get("source_read_mode") or ""),
        # Best-effort here; the read side refuses authoritatively with the exact
        # cursor key. Both refuse a watermark measured on another column.
        read_scope=read_scope_for_transfer_request(request),
        # Advanced write knobs — Validate already emits g17_row_cap. Execute
        # must name the same cap/sort or Approve hides a leftover filter.
        priority_column=str(getattr(request, "priority_column", "") or ""),
        priority_direction=str(getattr(request, "priority_direction", "") or "desc"),
        row_limit=max(0, int(getattr(request, "limit", 0) or 0)),
        source_endpoint=src,
        destination_endpoint=dest,
        catalog_primary_key_columns=catalog_pk,
        mappings=mappings,
        source_table=source_table,
        source_config=source_config if isinstance(source_config, dict) else None,
    )
