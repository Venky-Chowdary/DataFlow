"""Load a glob or wait-for-file pickup as one transfer.

Each file gets a fresh chunk cursor. The destination manifest, not a shared
checkpoint, decides which files already landed. Fingerprints from every file
become one order-independent digest so Gate-8 can grade the whole pickup.
"""

from __future__ import annotations

import logging
from typing import Any

from services.dest_precount import PRECOUNT_KEY
from services.row_conservation import CENSUS_KEY, KEY_ADDRESSED_KEY


def merge_file_census(summary: dict[str, Any], file_census: dict[str, Any]) -> None:
    """Add one file's key census onto the pickup summary.

    Files are merged in deterministic order, so a later file's upsert is
    last-wins. Each file's census was probed after the previous file
    committed, and ``inserts = unique_keys - dest_hits``. Summing those
    counts is the pickup's net COUNT(*) delta even when files share keys.
    """
    from services.row_conservation import KeyCensus

    incoming = KeyCensus.from_mapping(file_census)
    if incoming is None:
        return
    prior = KeyCensus.from_mapping(summary.get(CENSUS_KEY))
    if prior is None:
        summary[CENSUS_KEY] = incoming.to_dict()
        return
    events = None
    if prior.events_read is not None or incoming.events_read is not None:
        events = int(prior.events_read or 0) + int(incoming.events_read or 0)
    merged = KeyCensus(
        unique_batch_keys=prior.unique_batch_keys + incoming.unique_batch_keys,
        dest_preexisting=prior.dest_preexisting + incoming.dest_preexisting,
        tombstones=prior.tombstones + incoming.tombstones,
        unique_tombstone_keys=prior.unique_tombstone_keys + incoming.unique_tombstone_keys,
        events_read=events,
    )
    summary[CENSUS_KEY] = merged.to_dict()


def load_picked_files(
    *,
    src_type: str,
    src_cfg: dict[str, Any],
    bucket: str,
    key: str,
    dest_type: str,
    dest_cfg: dict[str, Any],
    source: Any,
    destination: Any,
    mappings: list[dict],
    schema: dict[str, str],
    effective_sync: str,
    stream_contracts: list[dict] | None,
    job_id: str | None,
    checkpoint: Any,
    checkpoint_service: Any,
    retry_budget: Any,
    backfill_new_fields: bool,
    validation_mode: str,
    source_filter: dict[str, Any] | None,
    skip_preflight: bool,
    shape_runner: Any,
    pre_write_rows_before: int | None,
) -> tuple[int, list[str], dict[str, Any], list[str]]:
    """List, select, and load new or changed files. Raises FileNotArrived."""
    from services.duplicate_policy import normalize_duplicate_policy
    from services.file_load_ledger import file_bytes_digest, manifest_entries, record_manifest_file
    from services.file_pickup import FileNotArrived, is_file_pattern, list_remote_files, plan_pickup
    from services.fingerprint_accumulator import FingerprintAccumulator
    from services.object_streaming import (
        download_for_object_store,
        download_object,
        stream_spilled_file_to_database,
    )

    extra = getattr(source, "extra", None) or {}
    wait_seconds = int(extra.get("file_wait_seconds") or 0)
    route_key = f"{src_type}:{bucket}:{key}"
    connector_key = str(getattr(source, "connector_id", "") or "") or route_key
    manifest = manifest_entries(route_key)
    if dest_type in {"postgresql", "snowflake"}:
        try:
            schema_name = str(
                getattr(destination, "schema", "") or dest_cfg.get("schema") or ""
            )
            if dest_type == "snowflake":
                from services.snowflake_file_manifest import read_loaded_manifest

                manifest = read_loaded_manifest(
                    dest_cfg, connector=connector_key, schema=schema_name or "PUBLIC"
                )
            else:
                from services.file_manifest import read_loaded_manifest

                manifest = read_loaded_manifest(
                    dest_cfg,
                    connector=connector_key,
                    schema=schema_name or "public",
                )
        except Exception:
            logging.getLogger(__name__).warning(
                "destination file manifest unread; using the local ledger",
                exc_info=True,
            )
    plan = plan_pickup(
        pattern=key,
        files=list_remote_files(src_type, src_cfg, bucket, key),
        manifest=manifest,
        wait_seconds=wait_seconds,
        waited_seconds=int(extra.get("file_waited_seconds") or 0),
    )
    if plan.action == "unchanged":
        unchanged = {
            "file_pickup": "unchanged",
            "files_picked": [],
            "source_row_count": 0,
            "source_row_count_source": "file_manifest",
            "rows_written": 0,
            "rejected_rows": 0,
            "sync_mode": effective_sync,
        }
        if isinstance(pre_write_rows_before, int):
            unchanged[PRECOUNT_KEY] = int(pre_write_rows_before)
        return 0, [], unchanged, []
    if plan.action != "load":
        raise FileNotArrived(plan.message, retryable=plan.retryable, pattern=key)

    total_rows = 0
    ddl_log: list[str] = []
    summary: dict[str, Any] = {
        "files_picked": [],
        "source_row_count": 0,
        "rows_written": 0,
        "rejected_rows": 0,
        "sync_mode": effective_sync,
        "source_row_count_source": "file_pickup",
    }
    if isinstance(pre_write_rows_before, int):
        summary[PRECOUNT_KEY] = int(pre_write_rows_before)
    pickup_policy = normalize_duplicate_policy(str(extra.get("duplicate_policy") or ""))
    if pickup_policy in {"upsert", "dedupe_skip"}:
        # Explicit keyed policy. COUNT(*) does not grow when a key is
        # updated, so conservation must use the key census.
        summary[KEY_ADDRESSED_KEY] = True
    population = FingerprintAccumulator()
    columns: list[str] = []
    for item in plan.files:
        # Each file is its own load. A shared chunk cursor would skip the
        # next file's only batch, and a manifest rollback means a crashed
        # file must be read again from the first row.
        if checkpoint is not None:
            checkpoint.chunk_index = 0
            checkpoint.rows_processed = 0
            checkpoint.offset = 0
            checkpoint.file_offset = 0
            checkpoint.checksum = ""
        cache_key = f"{src_type}:{bucket}:{item.name}"
        path = download_object(
            cache_key,
            lambda p, item_name=item.name: download_for_object_store(
                src_type, p, src_cfg, bucket, item_name
            ),
            force=True,
        )
        checksum = item.checksum or file_bytes_digest(path)
        rows, ddl, one_summary, columns = stream_spilled_file_to_database(
            path=path,
            filename=item.name,
            destination=destination,
            mappings=mappings,
            schema=schema,
            sync_mode=effective_sync,
            stream_contracts=stream_contracts,
            job_id=job_id,
            checkpoint=checkpoint,
            checkpoint_service=checkpoint_service,
            retry_budget=retry_budget,
            backfill_new_fields=backfill_new_fields,
            validation_mode=validation_mode,
            source_filter=source_filter,
            skip_preflight=skip_preflight,
            shape_runner=shape_runner,
            duplicate_policy=str(extra.get("duplicate_policy") or ""),
            fingerprint_sink=population,
            file_identity={
                "connector": connector_key,
                "path": item.name,
                "size": item.size,
                "mtime": item.mtime,
                "sha256": checksum,
                "job_id": job_id or "",
            },
        )
        total_rows += int(rows or 0)
        ddl_log.extend(ddl or [])
        if summary.get(PRECOUNT_KEY) is None and one_summary.get(PRECOUNT_KEY) is not None:
            summary[PRECOUNT_KEY] = one_summary[PRECOUNT_KEY]
        summary["source_row_count"] = int(summary.get("source_row_count") or 0) + int(
            one_summary.get("source_row_count") or 0
        )
        summary["rows_written"] = int(summary.get("rows_written") or 0) + int(
            one_summary.get("rows_written") or rows or 0
        )
        summary["rejected_rows"] = int(summary.get("rejected_rows") or 0) + int(
            one_summary.get("rejected_rows") or 0
        )
        sample = one_summary.get("reconcile_sample")
        if isinstance(sample, list) and sample:
            held = summary.setdefault("reconcile_sample", [])
            if isinstance(held, list):
                held.extend(sample)
                del held[50:]
        written_ids = one_summary.get("written_ids")
        if isinstance(written_ids, list):
            ids = summary.setdefault("written_ids", [])
            if isinstance(ids, list):
                for item_id in written_ids:
                    if item_id not in ids:
                        ids.append(item_id)
        file_census = one_summary.get(CENSUS_KEY)
        if isinstance(file_census, dict):
            merge_file_census(summary, file_census)
        for key_name in ("primary_key_columns", "conflict_columns"):
            if not summary.get(key_name) and one_summary.get(key_name):
                summary[key_name] = list(one_summary[key_name])
        summary["files_picked"].append(
            {
                "name": item.name,
                "size": item.size,
                "checksum": checksum,
                "mtime": item.mtime,
                "rows": int(rows or 0),
            }
        )
        loaded = str(one_summary.get("file_manifest_status") or "") == "loaded"
        if int(rows or 0) > 0 or loaded:
            record_manifest_file(
                route_key,
                name=item.name,
                size=item.size,
                checksum=checksum,
                mtime=item.mtime,
            )
    if population.total:
        summary["checksum"] = population.digest()
        summary["checksum_mode"] = "inline_write_pass"
    summary["file_pickup"] = "glob" if is_file_pattern(key) else "wait"
    return total_rows, ddl_log, summary, columns
