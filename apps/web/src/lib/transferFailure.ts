/**
 * Client-side operator guidance for known destination failures.
 * Mirrors apps/api/services/error_handling.humanize_transfer_failure.
 *
 * Honesty: only high/medium confidence patterns get concrete checks.
 * Copy must say "likely checks", never imply a guaranteed one-click fix.
 */

export type TransferFailureHint = {
  code: string;
  title: string;
  fix: string;
  confidence: "high" | "medium" | "low";
};

export function inferTransferFailureHint(
  error?: string | null,
  errorCode?: string | null,
  errorTitle?: string | null,
  errorFix?: string | null,
  errorConfidence?: string | null,
): TransferFailureHint | null {
  const conf = (errorConfidence === "high" || errorConfidence === "medium" || errorConfidence === "low")
    ? errorConfidence
    : null;

  if (errorCode && errorTitle && errorFix && conf) {
    return { code: errorCode, title: errorTitle, fix: errorFix, confidence: conf };
  }

  const text = String(error || "").toLowerCase();
  if (!text) return null;

  if (
    text.includes("cdc_lease_conflict")
    || text.includes("cdc lease conflict")
    || text.includes("refuse concurrent consumer")
  ) {
    return {
      code: errorCode || "cdc_lease_conflict",
      title: errorTitle || "CDC lease conflict",
      confidence: "high",
      fix:
        errorFix
        || "Another worker holds this CDC resource. Stop the holder, wait for TTL, or Force-release the lease in Job Theater (fencing generation advances), then Resume. Do not run two consumers on the same slot or server_id.",
    };
  }
  if (
    text.includes("cdc_lsn_gap")
    || text.includes("cdc_scn_gap")
    || text.includes("cdc_binlog_gap")
    || text.includes("cdc_slot_gap")
    || text.includes("cdc_ct_gap")
    || text.includes("cdc_cursor_gap")
    || text.includes("wal_status=lost")
    || text.includes("before capture retention")
    || text.includes("before available redo")
    || text.includes("min_lsn")
    || text.includes("min_valid_version")
    || text.includes("last_sync_version")
    || text.includes("oldest_available")
    || text.includes("ora-01291")
    || text.includes("ora-01292")
  ) {
    return {
      code: errorCode || "cdc_cursor_gap",
      title: errorTitle || "CDC cursor gap (retention / failover)",
      confidence: "high",
      fix:
        errorFix
        || "If snapshot_mode=when_needed, Resume — the engine snapshots current source keys then streams from the new tip. initial/never stay fail-closed until you change mode or reset the watermark. Purged-window events are gone. Not continuous CDC.",
    };
  }
  if (
    text.includes("allow_append_only")
    || text.includes("append-only")
    || text.includes("cdc_append_only_sink")
  ) {
    return {
      code: errorCode || "cdc_append_only_sink",
      title: errorTitle || "Append-only CDC sink blocked",
      confidence: "high",
      fix:
        errorFix
        || "Use a PK upsert destination, or enable Allow append-only CDC in Destination Advanced (acknowledges duplicates on redelivery).",
    };
  }
  if (text.includes("table is full") || text.includes("(1114") || text.includes("er_record_file_full")) {
    return {
      code: errorCode || "destination_table_full",
      title: errorTitle || "Destination table is full (MySQL 1114)",
      confidence: "high",
      fix:
        errorFix
        || "MySQL ER_RECORD_FILE_FULL (1114) means the engine could not allocate more space for this table. Common verified causes: host disk full, InnoDB tablespace limit, MEMORY/HEAP max size, or MyISAM max_rows. Confirm which applies on your host, free or expand capacity, then Resume. Resume alone will fail again until capacity is available.",
    };
  }
  if (text.includes("tablespace is full") || text.includes("innodb: error: tablespace")) {
    return {
      code: errorCode || "destination_tablespace_full",
      title: errorTitle || "Destination tablespace is full",
      confidence: "high",
      fix:
        errorFix
        || "InnoDB tablespace is exhausted. Expand the tablespace / data file or free space inside it, then Resume.",
    };
  }
  if (text.includes("disk full") || text.includes("no space left") || text.includes("enospc")) {
    return {
      code: errorCode || "destination_disk_full",
      title: errorTitle || "Destination reported no free disk space",
      confidence: "high",
      fix:
        errorFix
        || "Free space on the destination host (or expand the volume), confirm the write path mount, then Resume.",
    };
  }
  if (text.includes("too many connections") || text.includes("max_connections")) {
    return {
      code: errorCode || "destination_connection_limit",
      title: errorTitle || "Destination connection limit reached",
      confidence: "medium",
      fix:
        errorFix
        || "Likely max_connections saturation. Reduce concurrent jobs or raise the destination limit, then retry.",
    };
  }
  if (
    text.includes("duplicate redis key")
    || text.includes("duplicate primary key")
    || text.includes("duplicate key values")
    || (text.includes("conflict on") && text.includes("duplicate"))
    || text.includes("keys repeat")
  ) {
    return {
      code: errorCode || "duplicate_primary_key",
      title: errorTitle || "Duplicate identity-key values in a write batch",
      confidence: "high",
      fix:
        errorFix
        || "Open Map and set Primary key to a unique column (code, id, iso, name — not capital/city). Or set stream-contract primary_key. If the source truly duplicates that key, dedupe upstream before Resume.",
    };
  }
  if (
    text.includes("json file must be an array")
    || text.includes("json must be an array of objects")
    || text.includes("json array must contain objects")
    || text.includes("json file has no object rows")
  ) {
    return {
      code: errorCode || "json_shape_unsupported",
      title: errorTitle || "JSON source shape is not tabular",
      confidence: "high",
      fix:
        errorFix
        || 'Datawrap needs object rows: [{...}], a wrapper like {"data":[{...}]} / {"countries":[{...}]}, GeoJSON features, or one object as a single row. Re-export, re-upload, then re-run from Source — Resume will not help if extract never started.',
    };
  }
  if (
    text.includes('."public"')
    || text.includes('schema "public"')
    || text.includes("schema 'public'")
    || (text.includes("002003") && text.includes("public"))
  ) {
    return {
      code: errorCode || "snowflake_schema_not_found",
      title: errorTitle || "Snowflake schema not found (check PUBLIC vs public)",
      confidence: "high",
      fix:
        errorFix
        || 'Snowflake treats quoted "public" differently from PUBLIC. Set connector schema to PUBLIC, confirm role USAGE, then reload sample preview.',
    };
  }
  if (
    text.includes("were quarantined")
    || text.includes("row(s) were quarantined")
    || text.includes("all rows were quarantined")
    || (text.includes("quarantined") && text.includes("nothing was written"))
  ) {
    return {
      code: errorCode || "all_rows_quarantined",
      title: errorTitle || "Every row was quarantined — nothing landed",
      confidence: "high",
      fix:
        errorFix
        || "Open the Quarantine tab on this job, read the per-row reason, fix the Map transform or destination type, then Replay. This is not an empty source — the engine held the rows so nothing was silently dropped.",
    };
  }
  if (
    text.includes("full_refresh could not clear")
    || text.includes("refusing to append onto rows that should have been replaced")
    || text.includes("full_refresh_drop_failed")
  ) {
    return {
      code: errorCode || "full_refresh_drop_failed",
      title: errorTitle || "Could not clear the destination for full refresh",
      confidence: "high",
      fix:
        errorFix
        || "Grant DROP (or DELETE) on the destination table, confirm no lock is holding it, then re-run. Datawrap refused to continue as an append — that would have silently doubled the destination row count.",
    };
  }
  if (
    text.includes("no durable checkpoint to resume")
    || text.includes("restart-from-zero would duplicate")
  ) {
    return {
      code: errorCode || "resume_without_checkpoint",
      title: errorTitle || "Resume needs a committed checkpoint",
      confidence: "high",
      fix:
        errorFix
        || "This job never saved durable progress (0 rows). Do not Resume — re-run from Validate, or start a new transfer. After a deploy/restart, claim workers now restart zero-progress jobs from the beginning instead of false-failing Resume.",
    };
  }
  if (
    text.includes("ambiguous_write_outcome")
    || text.includes("cannot be safely retried")
    || text.includes("unknown outcome")
  ) {
    return {
      code: errorCode || "ambiguous_write_outcome",
      title: errorTitle || "Write interrupted with an unknown outcome",
      confidence: "high",
      fix:
        errorFix
        || "Resume this job from the last committed chunk. Datawrap stopped instead of re-sending the batch because this destination cannot deduplicate a replay. To make retries automatic, switch the sync mode to upsert with a primary key.",
    };
  }
  if (
    text.includes("duplicate_transfer")
    || text.includes("equivalent transfer is already")
  ) {
    return {
      code: errorCode || "duplicate_transfer",
      title: errorTitle || "An equivalent transfer is already running",
      confidence: "high",
      fix:
        errorFix
        || "Open the in-flight job instead of starting another writer against the same table. Cancel it first if you need a fresh run.",
    };
  }
  if (errorTitle && errorFix) {
    return {
      code: errorCode || "transfer_failed",
      title: errorTitle,
      fix: errorFix,
      confidence: conf || "low",
    };
  }
  return null;
}

export function isDestinationCapacityFailure(hint: TransferFailureHint | null, error?: string | null): boolean {
  if (hint?.code.includes("full") || hint?.code.includes("capacity") || hint?.code.includes("tablespace")) {
    return true;
  }
  return /table is full|disk full|no space left|tablespace is full|1114/i.test(String(error || ""));
}

/** Classify a log line for terminal coloring (no invented semantics). */
export type JobLogTone = "default" | "ok" | "warn" | "error" | "meta" | "progress";

export function classifyJobLogLine(line: string): JobLogTone {
  const t = line.toLowerCase();
  if (/failed|error|exception|traceback|1114|table is full|denied|overflow|conflict/.test(t)) return "error";
  if (/warn|quarantine|retry|stale|slow|attention/.test(t)) return "warn";
  if (/completed|success|passed|reconcile ok|written|applied/.test(t)) return "ok";
  if (/batch\s+\d|rows?\s+(processed|written|moved)|progress|%/.test(t)) return "progress";
  if (/connecting|entered|phase|queued|started|stream|lease|snapshot|ddl/.test(t)) return "meta";
  return "default";
}
