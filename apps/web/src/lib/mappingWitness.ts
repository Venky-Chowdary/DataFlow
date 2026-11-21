/**
 * Mapping Witness client (`/api/v1/transfer/mapping-witness*`, `/api/v1/jobs/{id}/mapping-witness`).
 *
 * The server judges each mapped pair against a bounded sample of the data and
 * returns evidence with a reason. Nothing here changes a mapping: the Map step
 * shows the flag and offers one action, Review. A job carries the evidence
 * snapshot it was submitted with; the Trust Report reads only that.
 */
import { apiFetch, parseApiError } from "./api";
import { isCallableSourceMode } from "./sourceReadMode";
import { API_BASE } from "./types";

export type WitnessStatus =
  | "witnessed"
  | "consistent"
  | "flagged"
  | "differs"
  | "not_comparable"
  | "not_read"
  | "no_evidence";
export type WitnessFlagKind = "swap" | "wrong_target" | "unit_mismatch" | "format_mismatch";

export interface WitnessFlag {
  kind: WitnessFlagKind;
  reason: string;
  suggested_target: string | null;
  basis: "keyed" | "profile" | "source_only";
  action: "review";
  swapped_with?: string;
  /** Names exactly this flag on this check: "Keep this mapping" answers it. */
  id?: string;
}

export interface WitnessPair {
  source: string;
  target: string;
  status: WitnessStatus;
  reason: string;
  flag: WitnessFlag | null;
  evidence: Record<string, unknown>;
}

export interface WitnessReport {
  version: string;
  mode: "keyed" | "profile" | "source_only";
  generated_at: string;
  key: { source: string; target: string } | null;
  sample: { source_rows: number; destination_rows: number; matching_keys: number };
  pairs: WitnessPair[];
  summary: {
    pairs: number;
    witnessed: number;
    consistent: number;
    flagged: number;
    differs: number;
    not_comparable?: number;
    not_read?: number;
    no_evidence: number;
  };
  inputs_sha256: string;
  notes?: string[];
  trigger?: "auto" | "explicit";
}

/** What POST /transfer/mapping-witness answers: the report, appended for this mapping and destination. */
export interface WitnessResult {
  mapping_digest: string;
  session?: string | null;
  requested_by?: string;
  report: WitnessReport;
  report_sha256: string;
  recorded_at: string;
}

export interface WitnessReview {
  source: string;
  flagged_target: string;
  flag_kind: WitnessFlagKind | null;
  flag_id?: string | null;
  decision: "kept" | "changed" | "continued";
  new_target: string | null;
  by: string;
  at: string;
}

/** The evidence frozen onto a job at submit (GET /jobs/{id}/mapping-witness). */
export interface WitnessSnapshot {
  status: "ok" | "unavailable";
  reason?: string;
  taken_at: string;
  report: WitnessReport | null;
  report_sha256?: string | null;
  report_recorded_at?: string | null;
  requested_by?: string | null;
  session?: string | null;
  not_checked_reason?: string;
  flag_reviews?: WitnessReview[];
  map_decisions?: Record<string, { target: string; approved: boolean; changed: boolean; by: string; at: string }>;
}

export const FLAG_LABEL: Record<WitnessFlagKind, string> = {
  swap: "Looks swapped",
  wrong_target: "Values match another column",
  unit_mismatch: "Unit mismatch",
  format_mismatch: "Format mismatch",
};

export const MODE_LABEL: Record<WitnessReport["mode"], string> = {
  keyed: "Compared on matching keys",
  profile: "Compared by value profile",
  source_only: "Source values only (before load)",
};

export const STATUS_LABEL: Record<WitnessStatus, string> = {
  witnessed: "Witnessed",
  consistent: "Consistent",
  flagged: "Review",
  differs: "Differs",
  not_comparable: "Not comparable",
  not_read: "Not read",
  no_evidence: "No evidence",
};

/** Reading these costs a query or a scan: the witness reads them only on "Check mapping". */
export const WAREHOUSE_ENGINES = new Set(["bigquery", "snowflake", "redshift", "databricks", "synapse", "athena"]);

export function isWarehouse(destType: string | null | undefined): boolean {
  return WAREHOUSE_ENGINES.has(String(destType || "").toLowerCase());
}

export interface WitnessRow {
  source: string;
  target?: string | null;
  createNew?: boolean;
  destType?: string;
  omitted?: boolean;
}

export interface WitnessRequest {
  mappings: Array<{ source: string; target: string; create_new?: boolean; target_type?: string }>;
  source_rows: Record<string, unknown>[];
  destination?: Record<string, unknown> | null;
  target_key_columns?: string[];
  candidate_columns?: string[];
  destination_table_exists?: boolean | null;
  date_locale?: string;
  trigger?: "auto" | "explicit";
  session_id?: string;
  source_ref?: WitnessSourceRef;
}

/** What the sample came from, named the way the run names its source (the server folds both the same way). */
export interface WitnessSourceRef {
  kind: string;
  name: string;
  /** A query or procedure source: its statement (two extracts of one stream are two sources). */
  statement?: string;
}

/**
 * The Map step's source, from the same state the run is submitted from: a file by
 * its name; a cloud object by its path; a table by the stream Map shows (the run
 * submits the primary stream, so evidence for another stream never attaches);
 * a query or procedure by its statement, as callableSourceExtra sends it.
 */
export function witnessSourceRef(state: {
  sourceKind: string;
  fileName?: string | null;
  cloudPath?: string | null;
  activeStream?: string | null;
  primaryStream?: string | null;
  readMode?: string | null;
  statement?: string | null;
}): WitnessSourceRef {
  if (state.sourceKind === "file") return { kind: "file", name: state.fileName || "", statement: "" };
  if (state.sourceKind === "cloud") return { kind: "database", name: state.cloudPath || "", statement: "" };
  const callable = state.sourceKind === "database" && isCallableSourceMode(state.readMode || undefined);
  return {
    kind: "database",
    name: state.activeStream || state.primaryStream || "",
    statement: callable ? String(state.statement || "").trim() : "",
  };
}

/** The rows sent: what Map already shows, capped (the server caps again). */
export const WITNESS_MAX_ROWS = 500;
/** Destination columns offered as alternatives; the server reads at most 64 named columns. */
export const WITNESS_MAX_CANDIDATES = 60;

/** The pairs a mapping writes, as the server counts them: omitted rows are not pairs. */
export function writtenPairs(rows: WitnessRow[]): WitnessRow[] {
  return rows.filter((m) => m.source && m.target && !m.omitted);
}

export function buildWitnessRequest(input: {
  rows: WitnessRow[];
  sampleRows?: Record<string, unknown>[];
  destEndpoint?: Record<string, unknown> | null;
  targetKeyColumns?: string[];
  candidateColumns?: string[];
  destTableExists?: boolean | null;
  dateLocale?: string;
  trigger: "auto" | "explicit";
  sessionId?: string;
  source?: WitnessSourceRef;
}): WitnessRequest {
  return {
    mappings: writtenPairs(input.rows).map((m) => ({
      source: m.source,
      target: String(m.target),
      create_new: Boolean(m.createNew),
      target_type: m.destType || undefined,
    })),
    source_rows: (input.sampleRows ?? []).slice(0, WITNESS_MAX_ROWS),
    // Always sent: it names the destination the evidence belongs to, even when it is not read.
    destination: input.destEndpoint ?? null,
    target_key_columns: input.targetKeyColumns ?? [],
    candidate_columns: (input.candidateColumns ?? []).slice(0, WITNESS_MAX_CANDIDATES),
    destination_table_exists: input.destTableExists ?? null,
    date_locale: input.dateLocale || "",
    trigger: input.trigger,
    session_id: input.sessionId || "",
    source_ref: input.source ?? { kind: "", name: "" },
  };
}

/** Everything a check depends on: a different value means an earlier result no longer stands. */
export function witnessInputsKey(request: WitnessRequest): string {
  return JSON.stringify({ ...request, source_rows: request.source_rows.length, trigger: undefined });
}

/**
 * Whether Map may run the witness by itself now: once the sample is loaded and
 * the destination catalog has answered (without its keys the read could not
 * join). A run Map starts by itself is "auto": the server then reads no
 * warehouse — only "Check mapping" (``explicit``) does.
 */
export function witnessAutoReady(input: { rowsReady: boolean; pairsKey: string; catalogReady: boolean }): boolean {
  return input.rowsReady && Boolean(input.pairsKey) && input.catalogReady;
}

export async function runMappingWitness(body: WitnessRequest, signal?: AbortSignal): Promise<WitnessResult> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping-witness`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "The mapping check could not run"));
  return res.json();
}

export async function reviewWitnessFlag(body: {
  mapping_digest: string;
  session_id: string;
  destination: Record<string, unknown> | null;
  source: string;
  decision: "kept" | "changed" | "continued";
  flag_id?: string;
  new_target?: string;
}): Promise<WitnessReview> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping-witness/review`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "The review could not be recorded"));
  return (await res.json()).decision;
}

/** Who approved or changed each pair in Map; the server records the signed-in person, never a name we send. */
export async function recordMapDecisions(
  sessionId: string,
  destination: Record<string, unknown> | null,
  mappings: Array<{ source: string; target: string; approved: boolean; changed: boolean }>,
  source?: WitnessSourceRef,
): Promise<number> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping-witness/decisions`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ session_id: sessionId, destination, mappings, source_ref: source ?? { kind: "", name: "" } }),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "Map decisions could not be recorded"));
  return (await res.json()).recorded;
}

export async function fetchJobMappingWitness(jobId: string): Promise<WitnessSnapshot | null> {
  const res = await apiFetch(`${API_BASE}/jobs/${encodeURIComponent(jobId)}/mapping-witness`);
  if (!res.ok) throw new Error(await parseApiError(res, "Mapping evidence could not be loaded"));
  const body = (await res.json()) as { witness?: WitnessSnapshot | null } | null;
  const witness = body?.witness ?? null;
  if (witness && witness.status !== "ok" && witness.status !== "unavailable") {
    throw new Error("Mapping evidence could not be read");
  }
  if (witness && witness.status === "ok" && witness.report && !Array.isArray(witness.report.pairs)) {
    throw new Error("Mapping evidence could not be read");
  }
  return witness;
}

/** Flagged pairs by lower-cased source name. */
export function flagsBySource(report: WitnessReport | null | undefined): Map<string, WitnessPair> {
  const out = new Map<string, WitnessPair>();
  for (const p of report?.pairs ?? []) {
    if (p.status === "flagged" && p.flag) out.set(p.source.toLowerCase(), p);
  }
  return out;
}

/** Stable identity of the pairs the witness judges (order-independent, omitted rows excluded). */
export function witnessPairsKey(rows: WitnessRow[]): string {
  return writtenPairs(rows)
    .map((m) => `${m.source.toLowerCase()}␟${String(m.target).toLowerCase()}`)
    .sort()
    .join("␞");
}
