"""Stage an uploaded dataset into a saved connector.

The file is the one ``resolve_dataset`` already chose. Mapping is
``run_mapping_pipeline``. Preflight is the same 9 gates a connector transfer
runs, with ``source_kind=file``. Confirm is the same ack the connector path
uses, so a file transfer does not grow a second engine or a second approval.
"""

from __future__ import annotations

import logging
from typing import Any

from services.dataset_file import assert_dataset_file
from services.timezone_policy import (
    apply_declared_source_timezone,
    validate_source_timezone,
)

from .query_tools import _tool_result

_LOG = logging.getLogger(__name__)


def _column_dicts(parsed_columns: list[Any], sample_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Columns the parser named, with samples in the transfer wire spelling.

    A bare column name is not a declared type. ``TEXT`` here is the same
    placeholder a header scan emits, and Map is allowed to replace it from
    the samples. A parser that already inferred a type (CSV/Parquet) keeps
    that type as the starting declaration.

    Samples use ``cell_to_string`` so a nested object is JSON, a bool is
    ``true``/``false``, and Map profiles the same spelling preflight does.
    ``str(dict)`` is a Python repr, which is not JSON and is not what the
    writer stores.
    """
    from services.value_serializer import cell_to_string

    rows: list[dict[str, Any]] = []
    for col in parsed_columns:
        if isinstance(col, str):
            name = col.strip()
            inferred = "TEXT"
            nullable = True
        elif isinstance(col, dict):
            name = str(col.get("name") or "").strip()
            inferred = str(col.get("inferred_type") or "TEXT")
            nullable = bool(col.get("nullable", True))
        else:
            continue
        if not name:
            continue
        samples = []
        for row in sample_rows:
            if not isinstance(row, dict) or row.get(name) is None:
                continue
            text = cell_to_string(row.get(name))
            if text:
                samples.append(text)
            if len(samples) >= 20:
                break
        rows.append({
            "name": name,
            "inferred_type": inferred,
            "nullable": nullable,
            "samples": samples,
        })
    return rows


def _blocked(preflight: dict[str, Any]) -> str:
    blockers = preflight.get("blockers") or []
    listed = "; ".join(
        f"{b.get('id')}: {b.get('message')}" for b in blockers[:4] if isinstance(b, dict) and b.get("message")
    )
    fixes = "; ".join(
        fix
        for fix in (
            str((b.get("details") or {}).get("recommended_fix") or "").strip()
            for b in blockers[:4]
            if isinstance(b, dict)
        )
        if fix
    )
    err = "Preflight blocked this transfer, so I won't start it"
    if listed:
        err += f" — {listed}"
    detail_lines: list[str] = []
    for blocker in blockers[:4]:
        if not isinstance(blocker, dict):
            continue
        for issue in (blocker.get("details") or {}).get("issues") or []:
            text = str(issue).strip()
            if text and text not in detail_lines:
                detail_lines.append(text)
        if detail_lines:
            break
    if detail_lines:
        err += " " + " ".join(detail_lines[:2])
    run_id = preflight.get("run_id")
    if run_id:
        err += f" (run {run_id})."
    else:
        err += "."
    if "naive wall-clock" in err:
        err += (
            " Pass source_timezone (an IANA zone such as UTC) on "
            "start_dataset_transfer to declare the zone for values that have "
            "none. Values that already carry an offset stay as written."
        )
    if fixes:
        err += f" To proceed: {fixes}"
    return err


def stage_dataset_transfer(
    dataset_name: str = "",
    dest_connector_id: str = "",
    dest_connector_name: str = "",
    dest_table: str = "",
    sync_mode: str = "",
    schema_policy: str = "manual_review",
    validation_mode: str = "balanced",
    limit: int = 0,
    contract_id: str = "",
    require_signed_contract: Any = None,
    source_timezone: str = "",
    pii_acknowledgement: dict[str, Any] | None = None,
    schema_drift_acknowledged: bool = False,
    upsert_key: str = "",
    primary_key: str = "",
    dedupe_key: str = "",
) -> Any:
    """Stage a file→connector transfer. Returns a tool result. Does not write."""
    tool = "start_dataset_transfer"
    hint = (dataset_name or "").strip()
    if not hint:
        return _tool_result(tool, success=False, error="Which uploaded file should I move?")
    zone, zone_error = validate_source_timezone(source_timezone)
    if zone_error:
        return _tool_result(tool, success=False, error=zone_error)
    from .transfer_tools import _pii_acknowledgement

    try:
        pii_ack = _pii_acknowledgement(pii_acknowledgement)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))

    from .data_analyst import get_data_analyst

    analyst = get_data_analyst()
    # A bare stem binds the measured sibling (more rows, then more columns).
    # Naming the file type selects that sibling. Profile still lists every
    # match; a transfer must not refuse the bind pick_dataset already made.
    schema = analyst.resolve_dataset(hint)
    if schema is None or getattr(schema, "source", "") != "upload" or not getattr(schema, "path", ""):
        found = getattr(schema, "name", "") if schema is not None else ""
        if found and getattr(schema, "source", "") != "upload":
            return _tool_result(
                tool,
                success=False,
                error=(
                    f"“{hint}” matches {found}, which is a template with no file. "
                    "Name the uploaded file."
                ),
            )
        return _tool_result(
            tool,
            success=False,
            error=f"Uploaded dataset '{hint}' was not found.",
        )

    try:
        path = assert_dataset_file(schema.path)
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))

    from services.file_parser import FileParser

    parsed = FileParser.parse(path.read_bytes(), path.name)
    if not getattr(parsed, "success", False) or not getattr(parsed, "columns", None):
        return _tool_result(
            tool,
            success=False,
            error=f"Could not read {path.name}.",
        )

    from .transfer_tools import (
        _dest_table_exists_tri_state,
        _identity_stream_contract,
        _introspect,
        _is_execute_cleared,
        _primary_key_csv,
        _run_preflight,
        _safe_connector,
        _schema_rows,
        _stage_bound_contract,
        normalize_sync_mode,
    )
    from .schema_tools import AmbiguousConnectorError

    try:
        dst_conn, err = _safe_connector(dest_connector_id, dest_connector_name, tool)
        if err:
            return err
    except AmbiguousConnectorError as exc:
        return _tool_result(tool, success=False, error=exc.message)

    from src.transfer.connector_capabilities import source_only_destination_refusal

    not_a_destination = source_only_destination_refusal(
        str(dst_conn.get("type") or dst_conn.get("format") or "")
    )
    if not_a_destination:
        return _tool_result(tool, success=False, error=not_a_destination)

    table = (dest_table or schema.name).strip()
    if not table:
        return _tool_result(tool, success=False, error="Which destination table should I write?")
    from connectors.sql_identifiers import unsafe_destination_identifier_reason

    ident_err = unsafe_destination_identifier_reason(
        str(dst_conn.get("type") or dst_conn.get("format") or ""),
        table,
    )
    if ident_err:
        return _tool_result(tool, success=False, error=ident_err)

    try:
        dst_info = _introspect(dst_conn, table, purpose="destination")
    except Exception as exc:
        _LOG.warning("dataset transfer dest introspect failed: %s", exc, exc_info=True)
        dst_info = {"ok": False, "error": str(exc), "columns": [], "db_type": "", "cfg": {}}

    sample_rows = [row for row in (parsed.data or [])[:20] if isinstance(row, dict)]
    src_rows = _schema_rows(_column_dicts(list(parsed.columns), sample_rows))
    if not src_rows:
        return _tool_result(tool, success=False, error=f"{path.name} has no columns.")
    dst_rows = _schema_rows(dst_info.get("columns") or [])
    dest_exists = _dest_table_exists_tri_state(dst_info)
    mode = normalize_sync_mode(sync_mode)
    file_type = str(getattr(parsed, "file_type", "") or schema.file_type or "csv")

    from services.data_profiler import source_types_are_authoritative
    from services.mapping_pipeline import run_mapping_pipeline

    # A file does not declare DDL. ``source_types_are_authoritative`` is the
    # owner of that fact: placeholder TEXT must not outrank the samples, or
    # Map stamps TEXT and preflight then blocks the TIMESTAMPTZ/BINARY it
    # just read from those same samples.
    mapping = run_mapping_pipeline(
        [r["name"] for r in src_rows],
        [r["name"] for r in dst_rows],
        source_schemas=src_rows,
        target_schemas=dst_rows,
        source_samples={r["name"]: r["samples"] for r in src_rows},
        validation_mode=validation_mode,
        # The file format is the source engine. Unbound, a CSV's text read as
        # code-page VARCHAR and SQL Server's NVARCHAR carrier was graded a
        # fidelity collapse on every text column (D204).
        file_format=file_type,
        source_db_type=file_type,
        destination_db_type=str(dst_info.get("db_type") or dst_conn.get("type") or ""),
        schema_policy=schema_policy,
        sync_mode=mode,
        destination_table_exists=dest_exists,
        source_types_authoritative=source_types_are_authoritative("file", file_type),
        use_llm=False,
    )
    mappings = list(mapping.get("mappings") or [])
    if not mappings:
        return _tool_result(
            tool,
            success=False,
            error="No column mapping was produced, so there is nothing safe to run.",
        )
    # Preflight and the engine read column_types off these rows. They have to
    # be the carriers Map judged, not the placeholder the parser left behind.
    upgraded = {
        str(m.get("source") or ""): str(m.get("source_type") or "").strip()
        for m in mappings
        if isinstance(m, dict) and str(m.get("source") or "").strip()
    }
    for row in src_rows:
        stamped = upgraded.get(row["name"])
        if stamped:
            row["inferred_type"] = stamped
    dest_db = str(dst_info.get("db_type") or dst_conn.get("type") or "")
    if zone:
        mappings = apply_declared_source_timezone(src_rows, mappings, zone, dest_db)

    # D58: the caller's key is the stream contract preflight AND Execute read,
    # built by the same helper start_transfer uses. It used to be stamped onto
    # the job payload only, after preflight had already refused the keyed load
    # for carrying no contract.
    operator_key = ",".join(
        part.strip()
        for part in str(upsert_key or primary_key or dedupe_key or "").split(",")
        if part.strip()
    )
    contracts, identity_error = _identity_stream_contract(
        mode=mode,
        source_table=schema.name,
        source_columns=[r["name"] for r in src_rows],
        mappings=mappings,
        operator_key=operator_key,
        catalog_key="",
    )
    if identity_error:
        return _tool_result(tool, success=False, error=identity_error)
    for contract in contracts:
        # The file is the snapshot (preflight_cursor_gate exempts keyed file
        # upserts from a watermark): an updated_at the helper would infer for
        # a table must not start filtering a re-uploaded file.
        if contract.pop("cursor_inferred", False):
            contract.pop("cursor_field", None)
            contract.pop("cursor_semantics", None)

    preflight = _run_preflight(
        src_conn={"id": "", "name": schema.name, "type": file_type, "schema": ""},
        dst_conn=dst_conn,
        src_table=schema.name,
        dst_table=table,
        src_rows=src_rows,
        sample_rows=sample_rows,
        mappings=mappings,
        mode=mode,
        schema_policy=schema_policy,
        validation_mode=validation_mode,
        src_db_type=file_type,
        source_config={"path": str(path), "format": file_type},
        dest_db_type=dest_db,
        dest_exists=dest_exists,
        source_kind="file",
        known_row_count=int(getattr(parsed, "row_count", 0) or 0),
        source_primary_key=_primary_key_csv(contracts[0].get("primary_key")) if contracts else "",
        stream_contracts=contracts or None,
        pii_ack=pii_ack,
        schema_drift_acknowledged=bool(schema_drift_acknowledged),
    )
    if not _is_execute_cleared(preflight):
        # The decision reason names what holds Confirm (e.g. which columns a
        # PII review flagged and how to acknowledge it) — never refuse blind.
        reason = str(
            ((preflight.get("proof_bundle") or {}).get("transfer_decision") or {}).get("reason")
            or ""
        ).strip()
        return _tool_result(
            tool,
            success=False,
            output={"preflight": preflight, "dataset": schema.name},
            error=_blocked(preflight) if not preflight.get("passed") else (
                "Preflight is not approve — Confirm is blocked until Studio Execute would unlock."
                + (f" Reason: {reason}" if reason else "")
            ),
        )

    payload = {
        "source": {
            "kind": "file",
            "format": file_type,
            "table": schema.name,
        },
        "destination": {
            "kind": "database",
            "format": dst_conn.get("type") or "",
            "connector_id": dst_conn["id"],
            "schema": dst_conn.get("schema") or "",
            "table": table,
        },
        "mappings": mappings,
        "column_types": {r["name"]: r["inferred_type"] for r in src_rows},
        "sync_mode": mode,
        "schema_policy": schema_policy,
        "validation_mode": validation_mode,
        "limit": max(0, int(limit or 0)),
        "source_filter": {},
        "stream_contracts": contracts,
        "skip_preflight": False,
        "preflight_run_id": preflight.get("run_id"),
        "source_path": str(path),
        "source_filename": path.name,
        "schema_drift_acknowledged": bool(schema_drift_acknowledged),
    }
    if payload["schema_drift_acknowledged"]:
        payload["acknowledgment_actor"] = "operator"
        payload["acknowledgment_reason"] = "Accept drift for this run"
    try:
        payload.update(_stage_bound_contract(contract_id, require_signed_contract))
    except ValueError as exc:
        return _tool_result(tool, success=False, error=str(exc))
    payload["skip_preflight"] = False
    if pii_ack:
        # Execute re-runs Validate: the same ack and its trail ride on the job.
        payload["compliance_acknowledged"] = True
        payload["acknowledgment_actor"] = pii_ack["approved_by"]
        payload["acknowledgment_reason"] = pii_ack["reason"]

    preview = {
        "dataset": schema.name,
        "file": path.name,
        "rows": int(getattr(parsed, "row_count", 0) or 0),
        "destination": f"{dst_conn.get('name')}.{table}",
        "sync_mode": mode,
        "mappings": len(mappings),
        "preflight_run_id": preflight.get("run_id") or "",
    }
    if pii_ack:
        preview["pii_acknowledgement"] = dict(pii_ack)
    from .ack_ledger import get_ack_ledger

    ack_id = get_ack_ledger().put(kind="start_transfer", payload=payload, preview=preview)
    return _tool_result(
        tool,
        success=True,
        output={
            "action": "start_dataset_transfer",
            "label": f"Load {schema.name} into {dst_conn.get('name')}.{table}",
            "risk": "mutate",
            "requires_confirm": True,
            "ack_id": ack_id,
            "preview": preview,
        },
    )
