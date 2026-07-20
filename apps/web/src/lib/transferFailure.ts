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
        || 'DataFlow needs object rows: [{...}], a wrapper like {"data":[{...}]} / {"countries":[{...}]}, GeoJSON features, or one object as a single row. Re-export, re-upload, then re-run from Source — Resume will not help if extract never started.',
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
