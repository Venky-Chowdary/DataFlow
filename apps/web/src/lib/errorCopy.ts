/**
 * One reader for the API's error envelope (services/error_taxonomy.py).
 *
 * The API owns the wording: every failure carries
 * `{error: {code, message, action: {label, ui, api}, object, retriable, reference}, detail}`
 * and a failed job carries `error_code` plus `error_details.{message, action}`.
 * The web decides presentation — which title, which button, whether a failure is a
 * capacity or a missing-table case — from `code`, never by searching the text.
 */

export type ErrorAction = { label: string; ui: string; api?: string };

export type ErrorCopy = {
  code: string;
  title: string;
  message: string;
  action: ErrorAction | null;
  reference: string;
};

const REFERENCE_RE = /\s*\b(?:Reference|Ref):\s*([A-Za-z0-9][A-Za-z0-9._-]{5,})\.?\s*$/;

/** Split a trailing "Reference: <id>" off a message so it renders as its own chip. */
export function splitReference(text: string | null | undefined): { text: string; reference: string } {
  const raw = String(text ?? "");
  const match = raw.match(REFERENCE_RE);
  if (!match) return { text: raw.trim(), reference: "" };
  return { text: raw.slice(0, match.index).trim(), reference: match[1] };
}

/** Family titles, used when the envelope carries no title of its own. */
const FAMILY_TITLES: Record<string, string> = {
  auth: "Sign-in rejected",
  access: "Not permitted",
  network: "Can't reach the database",
  object: "Not found",
  query: "Query error",
  value: "A value didn't fit",
  timeout: "Timed out",
  capacity: "Out of capacity",
  config: "Not available on this server",
  platform: "Datawrap couldn't finish this",
  unclassified: "Cause not identified",
};

export function errorFamily(code: string | null | undefined): string {
  const key = String(code || "");
  return key.includes(".") ? key.split(".", 1)[0] : key;
}

export function errorTitle(code: string | null | undefined, fallback = "Something went wrong"): string {
  return FAMILY_TITLES[errorFamily(code)] || fallback;
}

/** A failure that more disk / tablespace / connections fixes (capacity.*). */
export function isCapacityError(code: string | null | undefined): boolean {
  return errorFamily(code) === "capacity";
}

/** The object the run named is absent or invisible to the role. */
export function isMissingObjectError(code: string | null | undefined): boolean {
  return (
    code === "object.table_not_found"
    || code === "object.table_not_visible"
    || code === "object.collection_not_found"
    || code === "object.schema_not_found"
  );
}

/** The connection itself failed (sign-in, network, server, driver). */
export function isConnectionError(code: string | null | undefined): boolean {
  const family = errorFamily(code);
  return (
    family === "auth"
    || family === "network"
    || family === "config"
    || code === "object.database_not_found"
    || code === "access.database_not_granted"
  );
}

/** A key the destination already holds, or a batch that repeats its own key. */
export function isDuplicateKeyError(code: string | null | undefined): boolean {
  return code === "value.duplicate_key" || code === "duplicate_primary_key";
}

/** The run's upload is gone; only a new run from Transfer Studio can continue. */
export function isUploadExpired(code: string | null | undefined): boolean {
  return code === "object.upload_expired";
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === "object" && !Array.isArray(value) ? (value as Record<string, unknown>) : null;
}

function readAction(value: unknown): ErrorAction | null {
  const rec = asRecord(value);
  if (!rec || typeof rec.label !== "string" || !rec.label.trim()) return null;
  return {
    label: rec.label,
    ui: typeof rec.ui === "string" ? rec.ui : "",
    api: typeof rec.api === "string" ? rec.api : undefined,
  };
}

/** The envelope of an API response body, or null when the body has none. */
export function readErrorEnvelope(body: unknown): ErrorCopy | null {
  const rec = asRecord(body);
  if (!rec) return null;
  const err = asRecord(rec.error) ?? asRecord(rec.error_envelope);
  const detail = typeof rec.detail === "string" ? rec.detail : "";
  if (!err || typeof err.code !== "string" || !err.code) return null;
  const message = typeof err.message === "string" && err.message ? err.message : splitReference(detail).text;
  return {
    code: err.code,
    title: typeof err.title === "string" && err.title ? err.title : errorTitle(err.code),
    message,
    action: readAction(err.action),
    reference: typeof err.reference === "string" && err.reference ? err.reference : splitReference(detail).reference,
  };
}

/** The copy a failed job shows: its taxonomy message and one action, or its legacy text. */
export function jobFailureCopy(job: {
  _id?: string;
  error?: string | null;
  message?: string | null;
  error_code?: string | null;
  error_title?: string | null;
  error_fix?: string | null;
  error_details?: Record<string, unknown> | null;
}): ErrorCopy {
  const details = asRecord(job.error_details) ?? {};
  const code = String(job.error_code || details.code || "");
  const action = readAction(details.action)
    ?? (job.error_fix ? { label: String(job.error_fix), ui: "" } : null);
  const structured = Boolean(readAction(details.action)) && typeof details.message === "string" && details.message;
  const fullText = String(job.error || job.message || "");
  const { text, reference } = splitReference(fullText);
  return {
    code,
    title: String(job.error_title || details.title || "") || errorTitle(code, "Transfer failed"),
    // A structured failure's job text is message + action; show the message once.
    message: structured ? String(details.message) : text,
    action,
    reference: reference || String(job._id || ""),
  };
}

/** The taxonomy code an API error carries (ApiError.code), or "". */
export function errorCodeOf(error: unknown): string {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" ? code : "";
}
