/**
 * The job's failure hint, read from the API's classification (error_code,
 * error_title, error_fix). Wording lives in apps/api/services/error_taxonomy.py.
 */
import { errorTitle as errorTitleForCode, isCapacityError } from "./errorCopy";

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
  // The API classifies every failure (services/error_taxonomy.py) and sends its
  // code, title and one action on the job. The page no longer re-guesses a
  // cause from the error text: a text fallback titled a PostgreSQL schema
  // error "Snowflake schema not found" (ERR-6).
  void error;
  const conf = (errorConfidence === "high" || errorConfidence === "medium" || errorConfidence === "low")
    ? errorConfidence
    : null;
  if (!errorTitle && !errorFix) return null;
  return {
    code: errorCode || "unclassified",
    title: errorTitle || errorTitleForCode(errorCode, "Transfer failed"),
    fix: errorFix || "",
    confidence: conf || "low",
  };
}

/** A failure more disk / tablespace / connections fixes — decided on the code. */
export function isDestinationCapacityFailure(hint: TransferFailureHint | null, _error?: string | null): boolean {
  const code = hint?.code || "";
  return isCapacityError(code) || code === "destination_table_full" || code === "destination_disk_full"
    || code === "destination_tablespace_full";
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
