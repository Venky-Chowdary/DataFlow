"""Load a glob or wait-for-file pickup as one transfer.

Each file gets a fresh chunk cursor. The destination manifest, not a shared
checkpoint, decides which files already landed. Fingerprints from every file
become one order-independent digest so Gate-8 can grade the whole pickup.
"""

from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Any

from services.dest_precount import PRECOUNT_KEY
from services.row_conservation import CENSUS_KEY, KEY_ADDRESSED_KEY


# Source-digest fields one load's summary carries for Gate-8.
_LOAD_PROOF_KEYS = (
    "checksum",
    "checksum_mode",
    "checksum_note",
    "source_snapshot",
    "source_independently_reread",
    "identity_alignment",
    "identity_hash_aligned",
)


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
    from services.file_pickup import (
        FileNotArrived,
        is_file_pattern,
        list_remote_files,
        matches_pattern,
        ordered_files,
        plan_pickup,
    )
    from services.fingerprint_accumulator import FingerprintAccumulator
    from services.object_streaming import (
        download_for_object_store,
        download_object,
        stream_spilled_file_to_database,
    )
    from services.sync_cursor import is_overwrite_sync

    try:
        from src.transfer.file_stream import combine_file_set
    except ImportError:  # pragma: no cover - api root on PYTHONPATH
        from transfer.file_stream import combine_file_set

    extra = getattr(source, "extra", None) or {}
    wait_seconds = int(extra.get("file_wait_seconds") or 0)
    route_key = f"{src_type}:{bucket}:{key}"
    connector_key = str(getattr(source, "connector_id", "") or "") or route_key
    dest_table_name = str(getattr(destination, "table", "") or dest_cfg.get("table") or "")
    schedule_id = str(extra.get("schedule_id") or "")
    manifest = manifest_entries(route_key)
    if dest_type in {"postgresql", "snowflake"}:
        try:
            schema_name = str(
                getattr(destination, "schema", "") or dest_cfg.get("schema") or ""
            )
            if dest_type == "snowflake":
                from services.snowflake_file_manifest import read_loaded_manifest

                manifest = read_loaded_manifest(
                    dest_cfg,
                    connector=connector_key,
                    schema=schema_name or "PUBLIC",
                    dest_table=dest_table_name,
                    schedule_id=schedule_id,
                )
            else:
                from services.file_manifest import read_loaded_manifest

                manifest = read_loaded_manifest(
                    dest_cfg,
                    connector=connector_key,
                    schema=schema_name or "public",
                    dest_table=dest_table_name,
                    schedule_id=schedule_id,
                )
        except Exception:
            logging.getLogger(__name__).warning(
                "destination file manifest unread; using the local ledger",
                exc_info=True,
            )
    listed = list_remote_files(src_type, src_cfg, bucket, key)
    plan = plan_pickup(
        pattern=key,
        files=listed,
        manifest=manifest,
        wait_seconds=wait_seconds,
        waited_seconds=int(extra.get("file_waited_seconds") or 0),
    )
    if plan.action in {"unchanged", "warn"}:
        unchanged = {
            "file_pickup": plan.action,
            "files_picked": [],
            "source_row_count": 0,
            "source_row_count_source": "file_manifest",
            "rows_written": 0,
            "rejected_rows": 0,
            "sync_mode": effective_sync,
            "status": plan.status,
            "warnings": [plan.message] if plan.message else [],
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
    files = list(plan.files)
    whole_set = is_overwrite_sync(effective_sync)
    if whole_set:
        # D34: an overwrite replaces the destination with the source, and the
        # source is every matched file. Loading only the files the manifest
        # calls new would replace the table with a fraction of the set.
        files = ordered_files([f for f in listed if matches_pattern(f.name, key)]) or files

    def _reset_checkpoint() -> None:
        # A load starts at its first row: a shared chunk cursor would skip the
        # next file's only batch, and a manifest rollback means a crashed load
        # is read again from the start.
        if checkpoint is not None:
            checkpoint.chunk_index = 0
            checkpoint.rows_processed = 0
            checkpoint.offset = 0
            checkpoint.file_offset = 0
            checkpoint.checksum = ""

    def _download(item: Any) -> tuple[Path, str]:
        path = download_object(
            f"{src_type}:{bucket}:{item.name}",
            lambda p, item_name=item.name: download_for_object_store(
                src_type, p, src_cfg, bucket, item_name
            ),
            force=True,
        )
        return path, item.checksum or file_bytes_digest(path)

    def _load(path: Path, filename: str, identity: dict[str, Any] | None) -> tuple[int, dict]:
        nonlocal total_rows, columns
        rows, ddl, one_summary, columns = stream_spilled_file_to_database(
            path=path,
            filename=filename,
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
            file_identity=identity,
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
        return int(rows or 0), one_summary

    def _record(item: Any, checksum: str, rows: int, loaded: bool) -> None:
        summary["files_picked"].append(
            {
                "name": item.name,
                "size": item.size,
                "checksum": checksum,
                "mtime": item.mtime,
                "rows": rows,
            }
        )
        if rows > 0 or loaded:
            record_manifest_file(
                route_key,
                name=item.name,
                size=item.size,
                checksum=checksum,
                mtime=item.mtime,
            )

    if whole_set and len(files) > 1:
        # One logical source, one load: the writer's own staged overwrite
        # (one PostgreSQL transaction, one SQLite stage, one MySQL retire)
        # covers the whole set, so a failure on any file leaves the pre-run
        # destination intact and Gate-8 counts every file. A set has no
        # single file identity to claim in a destination manifest.
        downloads = [(item, *_download(item)) for item in files]
        _reset_checkpoint()
        with tempfile.TemporaryDirectory(prefix="df-pickup-set-") as tmp:
            combined = Path(tmp) / "set"
            name, file_rows = combine_file_set(
                [(path, item.name) for item, path, _checksum in downloads], combined
            )
            rows, one_summary = _load(combined, name, None)
        loads = [one_summary]
        for (item, _path, checksum), item_rows in zip(downloads, file_rows, strict=True):
            _record(item, checksum, item_rows if rows else 0, False)
    else:
        loads = []
        for item in files:
            _reset_checkpoint()
            path, checksum = _download(item)
            rows, one_summary = _load(
                path,
                item.name,
                {
                    "connector": connector_key,
                    "path": item.name,
                    "size": item.size,
                    "mtime": item.mtime,
                    "sha256": checksum,
                    "job_id": job_id or "",
                    "dest_table": dest_table_name,
                    "schedule_id": schedule_id,
                },
            )
            loads.append(one_summary)
            loaded = str(one_summary.get("file_manifest_status") or "") == "loaded"
            _record(item, checksum, rows, loaded)
    if len(loads) == 1 and not population.total:
        # A COPY fast-path load proves itself by its own write-pass or re-read
        # digest and feeds no row fingerprints to the shared sink; one load is
        # the whole pickup, so its proof is the pickup's.
        for key_name in _LOAD_PROOF_KEYS:
            if key_name in loads[0]:
                summary[key_name] = loads[0][key_name]
    if population.total:
        summary["checksum"] = population.digest()
        summary["checksum_mode"] = "inline_write_pass"
    summary["file_pickup"] = "glob" if is_file_pattern(key) else "wait"
    if plan.action == "load" and plan.message:
        held = [
            str(item).strip()
            for item in (summary.get("warnings") or [])
            if str(item).strip()
        ]
        if plan.message not in held:
            held.append(plan.message)
        summary["warnings"] = held
    return total_rows, ddl_log, summary, columns


def pickup_job_outcome(
    summary: dict[str, Any] | None,
    rows_written: int = 0,
) -> dict[str, Any] | None:
    """Job status for a dated series whose file for today has not arrived.

    ``unchanged`` ("no new or changed files") stays a normal completion.
    A dated load that wrote rows still carries the missing-today warning.
    """
    if not isinstance(summary, dict):
        return None
    warnings = []
    for item in summary.get("warnings") or []:
        text = item.get("message") if isinstance(item, dict) else item
        text = str(text or "").strip()
        if text:
            warnings.append(text)
    today_missing = [
        item
        for item in warnings
        if item.startswith("expected file for ") and item.endswith(" not arrived")
    ]
    action = str(summary.get("file_pickup") or "")
    if action == "warn" and int(rows_written or 0) == 0 and warnings:
        return {
            "status": "completed_with_warnings",
            "message": warnings[0],
            "warnings": warnings,
        }
    if today_missing:
        return {
            "status": "completed_with_warnings",
            "message": today_missing[0],
            "warnings": today_missing,
        }
    return None


def apply_pickup_job_outcome(
    recon: dict[str, Any] | None,
    summary: dict[str, Any] | None,
    rows_written: int = 0,
) -> tuple[dict[str, Any], str | None, str | None]:
    """Prefer the pickup warning over a 0-row reconcile failure."""
    outcome = pickup_job_outcome(summary, rows_written)
    current = dict(recon or {})
    if outcome is None:
        return current, None, None
    # A real reconcile failure on rows that landed stays a failure.
    # The 0-row "file not arrived" poll is the case that must not look like
    # a checksum miss.
    if int(rows_written or 0) > 0 and current.get("passed") is False:
        return current, None, None
    if int(rows_written or 0) == 0:
        current["passed"] = True
    current["message"] = outcome["message"]
    current["pickup_warning"] = outcome["message"]
    return current, str(outcome["status"]), str(outcome["message"])
