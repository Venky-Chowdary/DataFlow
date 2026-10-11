from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)


class CdcDriftRequiresFencedRepair(RuntimeError):
    """Exactly-once CDC found source-image drift that needs operator repair."""

    def __init__(self, evidence: dict[str, Any]) -> None:
        self.evidence = evidence
        super().__init__(
            "Exactly-once CDC found destination drift. Re-copy the evidenced keys "
            "with the confirmed repair action, then resume. This is state "
            "convergence / reconciliation, not log replay."
        )


def _build_drift_evidence(
    *,
    cursor_key: str,
    source_table: str,
    dest_table: str,
    pk_fields: list[str],
    committed_position: Any,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    from services.audit_log import _platform_hmac_secret

    secret = _platform_hmac_secret()
    keys: list[dict[str, Any]] = []
    for row in rows[:100]:
        values = []
        display: dict[str, str] = {}
        for field in pk_fields:
            value = row.get(field)
            if field not in row:
                value = next(
                    (v for k, v in row.items() if str(k).casefold() == field.casefold()),
                    None,
                )
            values.append(value)
            display[field] = "••••" if value is not None else "(null)"
        canonical = json.dumps(values, sort_keys=True, separators=(",", ":"), default=str)
        message = f"{cursor_key}|{source_table}|{canonical}".encode("utf-8")
        keys.append(
            {
                "key_id": hmac.new(secret, message, hashlib.sha256).hexdigest(),
                "display": display,
            }
        )
    return {
        "action": "operator_fenced_repair_required",
        "code": "cdc_drift_requires_fenced_repair",
        "cursor_key": cursor_key,
        "source_table": source_table,
        "dest_table": dest_table,
        "pk_fields": list(pk_fields),
        "committed_position": committed_position,
        "drift_count": len(rows),
        "keys": keys,
        "truncated": len(rows) > len(keys),
        "semantics": "state convergence / reconciliation, not log replay",
    }


def run_fenced_drift_repair(
    job_id: str,
    key_ids: list[str],
    *,
    actor: str,
    workspace_id: str,
) -> dict[str, Any]:
    """Reconcile confirmed CDC drift under the route's lease and EOS fence."""
    from connectors.cdc_eos_sql import open_eos_session
    from services.cdc_lease import (
        CdcLeaseConflict,
        acquire_lease,
        lease_view,
        new_holder_id,
        release_lease,
    )
    from services.mongodb_service import get_mongodb_service
    from src.transfer.models import transfer_request_from_dict

    store = get_mongodb_service()
    job = store.get_job(str(job_id))
    if not job:
        raise ValueError("CDC drift repair job was not found.")
    if str(job.get("workspace_id") or "") != str(workspace_id or ""):
        raise PermissionError("CDC drift repair job is outside the caller's workspace.")
    extras = job.get("extras") if isinstance(job.get("extras"), dict) else {}
    evidence = extras.get("cdc_drift_repair")
    if not isinstance(evidence, dict) or evidence.get("action") != "operator_fenced_repair_required":
        raise ValueError("This job has no operator-fenced CDC drift evidence.")
    saved = job.get("transfer_request") or {}
    source_data = saved.get("source") or {}
    dest_data = saved.get("destination") or {}
    if (
        str(source_data.get("table") or source_data.get("collection") or "")
        != str(evidence.get("source_table") or "")
        or str(dest_data.get("table") or dest_data.get("collection") or "")
        != str(evidence.get("dest_table") or "")
        or str(job.get("cdc_lease_cursor_key") or evidence.get("cursor_key") or "")
        != str(evidence.get("cursor_key") or "")
    ):
        raise ValueError("CDC drift evidence does not match the saved job route.")
    evidence_keys = {
        str(item.get("key_id") or "")
        for item in evidence.get("keys", [])
        if isinstance(item, dict) and item.get("key_id")
    }
    selected = {str(value) for value in key_ids} if key_ids else evidence_keys
    if not selected or not selected.issubset(evidence_keys):
        raise ValueError("Selected CDC drift keys are not present in job evidence.")

    from services.cdc_exactly_once import require_batch_lsn

    lsn = require_batch_lsn(evidence.get("committed_position"))
    request = transfer_request_from_dict(saved)
    from src.transfer.adapters import resolve_connector_config, write_destination_database
    from src.transfer.connector_capabilities import resolve_driver_type
    from services.cdc_value_digest import (
        _scan_table,
        identity_rows_missing_on_dest,
        rows_absent_by_primary_key,
    )

    source_type = resolve_driver_type(request.source.format or "")
    dest_type = resolve_driver_type(request.destination.format or "")
    source_cfg = resolve_connector_config(request.source, workspace_id=workspace_id)
    dest_cfg = resolve_connector_config(request.destination, workspace_id=workspace_id)
    source_table = str(evidence["source_table"])
    dest_table = str(evidence["dest_table"])
    cursor_key = str(evidence["cursor_key"])
    contract_name = "stream"
    for contract in saved.get("stream_contracts") or []:
        if (
            isinstance(contract, dict)
            and str(contract.get("name") or "") == source_table
            and contract.get("selected", True)
        ):
            contract_name = str(contract.get("name") or "stream")
            break
    from src.transfer.cdc_transfer import _single_stream_cursor_key

    expected_cursor_key = _single_stream_cursor_key(
        src_type=source_type,
        src_cfg=source_cfg,
        table_name=source_table,
        dest_type=dest_type,
        dest_cfg=dest_cfg,
        dest_table=dest_table,
        stream_name=contract_name,
    )
    if expected_cursor_key != cursor_key:
        raise ValueError("CDC drift evidence cursor does not match the saved job route.")
    if dest_type not in {"sqlite", "postgresql", "mysql"}:
        raise ValueError(
            f"Inclusive _df_lsn compare-and-set is not supported for {dest_type}; "
            "it is not supported for SQL Server, Oracle, MongoDB, Snowflake, "
            "BigQuery, Redshift, object stores, or other unguarded writers. "
            "Supported repair dialects are SQLite, PostgreSQL, and MySQL."
        )

    from connectors.cdc_eos_sql import dest_watermark_view
    if dest_type == "sqlite":
        fence_view = dest_watermark_view(dest_cfg, cursor_key)
    else:
        from connectors.cdc_eos_sa import sa_dest_watermark_view

        fence_view = sa_dest_watermark_view(dest_cfg, cursor_key, dest_type)
    min_generation = int(getattr(fence_view, "fence_epoch", 0) or 0)
    existing = lease_view(cursor_key) or {}
    resource = str(existing.get("resource") or evidence.get("cdc_lease_resource") or "")
    if not resource:
        if source_type == "postgresql":
            from connectors.postgresql_change_stream import _slot_name

            slot = _slot_name(
                source_cfg.get("database") or "postgres", source_table, cursor_key
            )
            resource = f"pg_slot:{slot}"
        elif source_type == "mysql":
            import hashlib

            digest = hashlib.sha1(
                f"{source_cfg.get('host')}|{source_cfg.get('database')}|{source_table.lower()}|{cursor_key}".encode(),
                usedforsecurity=False,
            ).hexdigest()
            resource = f"mysql_server_id:{10_000 + (int(digest[:6], 16) % 1_000_000)}"
        elif source_type == "mongodb":
            resource = f"mongo_cs:{source_cfg.get('database') or 'admin'}:{source_table}"
        elif source_type == "sqlserver":
            from services.cdc_lease import mssql_cdc_resource

            resource = mssql_cdc_resource(
                str(source_cfg.get("database") or "master"),
                str(request.source.schema or "dbo"),
                source_table,
                mode="ct",
            )
        elif source_type == "oracle":
            from services.cdc_lease import oracle_cdc_resource

            resource = oracle_cdc_resource(
                str(request.source.schema or ""),
                source_table,
                mode="logminer",
                host=str(source_cfg.get("host") or ""),
            )
        else:
            resource = cursor_key
    holder_id = new_holder_id(str(job_id))
    lease = None
    counts = {
        "repaired": 0,
        "refused_newer_event": 0,
        "source_row_absent": 0,
        "already_converged": 0,
        "failed": 0,
        "verified": 0,
    }
    from services.audit_log import append_audit_event

    audit = append_audit_event
    audit(
        action="cdc_drift_repair_started",
        resource=str(job_id),
        actor=actor or "operator",
        workspace_id=workspace_id,
        details={
            "job_id": str(job_id),
            "table": dest_table,
            "key_count": len(selected),
            "outcomes": dict(counts),
        },
    )
    try:
        try:
            lease = acquire_lease(
                cursor_key,
                resource=resource,
                holder_id=holder_id,
                min_generation=min_generation,
                meta={"job_id": str(job_id), "purpose": "confirmed_cdc_drift_repair"},
            )
        except CdcLeaseConflict as exc:
            raise CdcLeaseConflict(
                f"CDC repair refused: stop the stream first ({exc}).",
                holder_id=exc.holder_id,
                resource=exc.resource or resource,
                cursor_key=cursor_key,
            ) from exc
        opened = open_eos_session(
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            stream_key=cursor_key,
            incoming_fence=int(lease.generation),
        )
        committed = evidence.get("committed_position")
        mappings = list(saved.get("mappings") or [])
        pk_fields = [str(col) for col in evidence.get("pk_fields") or []]
        pk_mapping = {
            str(mapping.get("source") or "").casefold(): str(
                mapping.get("target") or mapping.get("source") or ""
            )
            for mapping in mappings
            if isinstance(mapping, dict)
        }
        pk_targets = [pk_mapping.get(field.casefold(), "") for field in pk_fields]
        if not pk_fields or any(not item for item in pk_targets):
            raise ValueError(
                "CDC drift repair requires destination mappings for every primary key."
            )
        drift_rows = identity_rows_missing_on_dest(
            source_type=source_type,
            source_cfg=source_cfg,
            source_table=source_table,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            mappings=mappings,
            dest_types=saved.get("column_types") or {},
        )
        if drift_rows is None:
            drift_rows = rows_absent_by_primary_key(
                source_type=source_type,
                source_cfg=source_cfg,
                source_table=source_table,
                dest_type=dest_type,
                dest_cfg=dest_cfg,
                dest_table=dest_table,
                mappings=mappings,
                primary_key=",".join(pk_fields),
            )
        if drift_rows is None:
            raise ValueError("CDC drift rescan could not prove selected source rows.")
        source_rows = _scan_table(
            source_type,
            source_cfg,
            source_table,
            list(dict.fromkeys(pk_fields)),
        )
        if source_rows is None:
            raise ValueError("CDC drift rescan could not prove current source keys.")
        source_ids: set[str] = set()
        for row in source_rows:
            one = _build_drift_evidence(
                cursor_key=cursor_key,
                source_table=source_table,
                dest_table=dest_table,
                pk_fields=pk_fields,
                committed_position=committed,
                rows=[row],
            )
            source_ids.update(item["key_id"] for item in one["keys"])
        keyed_rows: dict[str, dict[str, Any]] = {}
        for row in drift_rows:
            single = _build_drift_evidence(
                cursor_key=cursor_key,
                source_table=source_table,
                dest_table=dest_table,
                pk_fields=pk_fields,
                committed_position=committed,
                rows=[row],
            )
            if single["keys"]:
                keyed_rows[single["keys"][0]["key_id"]] = row
        not_drift = selected - set(keyed_rows)
        absent_source = not_drift - source_ids
        counts["source_row_absent"] = len(absent_source)
        counts["already_converged"] = len(not_drift & source_ids)
        for key_id in selected & set(keyed_rows):
            row = dict(keyed_rows[key_id])
            row["_df_lsn"] = lsn
            writer_mappings = list(mappings)
            if not any(
                str(item.get("source") or "") == "_df_lsn"
                for item in writer_mappings
                if isinstance(item, dict)
            ):
                writer_mappings.append(
                    {"source": "_df_lsn", "target": "_df_lsn", "confidence": 1.0}
                )
            try:
                _written, _columns, writer_summary = write_destination_database(
                    request.destination,
                    [row],
                    list(row),
                    saved.get("column_types") or {},
                    writer_mappings,
                    write_mode="upsert",
                    conflict_columns=pk_targets,
                    sync_mode="cdc",
                    allow_equal_lsn=True,
                )
                skipped_rows = int(writer_summary.get("rows_skipped") or 0)
                if skipped_rows:
                    counts["refused_newer_event"] += 1
                elif int(_written or 0) > 0:
                    counts["repaired"] += 1
                else:
                    counts["failed"] += 1
            except Exception as exc:
                logger.warning(
                    "CDC drift repair key failed cursor_key=%s key_id=%s error_type=%s",
                    cursor_key,
                    key_id,
                    type(exc).__name__,
                )
                counts["failed"] += 1
        after = identity_rows_missing_on_dest(
            source_type=source_type,
            source_cfg=source_cfg,
            source_table=source_table,
            dest_type=dest_type,
            dest_cfg=dest_cfg,
            dest_table=dest_table,
            mappings=mappings,
            dest_types=saved.get("column_types") or {},
        )
        if after is None:
            raise ValueError("CDC drift reconciliation could not prove destination state.")
        after_ids: set[str] = set()
        for row in after:
            single = _build_drift_evidence(
                cursor_key=cursor_key,
                source_table=source_table,
                dest_table=dest_table,
                pk_fields=pk_fields,
                committed_position=committed,
                rows=[row],
            )
            after_ids.update(item["key_id"] for item in single["keys"])
        counts["verified"] = len((selected - after_ids) & source_ids)
        resolved = (
            not bool(evidence.get("truncated"))
            and evidence.get("drift_count") == len(evidence_keys)
            and not (evidence_keys & after_ids)
            and not (evidence_keys - source_ids)
            and counts["failed"] == 0
            and counts["refused_newer_event"] == 0
        )
        summary = {
            "actor": actor,
            "job_id": str(job_id),
            "table": dest_table,
            "key_count": len(selected),
            "outcomes": counts,
            "verified": counts["verified"],
            "remaining_keys": len(
                (evidence_keys & after_ids) | (evidence_keys - source_ids)
            ),
            "resolved": resolved,
            "fence_generation": int(opened.fence_epoch),
            "semantics": "state convergence / reconciliation, not log replay",
        }
        updated_extras = dict(extras)
        updated_evidence = dict(evidence)
        updated_evidence["last_repair"] = summary
        updated_evidence["resolved"] = resolved
        updated_extras["cdc_drift_repair"] = updated_evidence
        store.update_job_fields(
            str(job_id),
            {
                "extras": updated_extras,
                "cdc_drift_repair": updated_evidence,
            },
        )
        audit(
            action="cdc_drift_repair_completed",
            resource=str(job_id),
            actor=actor or "operator",
            workspace_id=workspace_id,
            details={"job_id": str(job_id), "table": dest_table, "key_count": len(selected), "outcomes": counts},
        )
        return summary
    finally:
        if lease is not None:
            release_lease(
                cursor_key,
                holder_id=holder_id,
                generation=int(lease.generation),
            )
