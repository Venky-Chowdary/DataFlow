/**
 * Mapping Witness client (`/api/v1/transfer/mapping-witness`, `/api/v1/jobs/{id}/mapping-witness`).
 *
 * The server judges each mapped pair against a bounded sample of the data and
 * returns evidence with a reason. Nothing here changes a mapping: the Map step
 * shows the flag and offers one action, Review.
 */
import { apiFetch, parseApiError } from "./api";
import { API_BASE } from "./types";

export type WitnessStatus = "witnessed" | "consistent" | "flagged" | "differs" | "no_evidence";
export type WitnessFlagKind = "swap" | "wrong_target" | "unit_mismatch" | "format_mismatch";

export interface WitnessFlag {
  kind: WitnessFlagKind;
  reason: string;
  suggested_target: string | null;
  basis: "keyed" | "profile" | "source_only";
  action: "review";
  swapped_with?: string;
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
  summary: { pairs: number; witnessed: number; consistent: number; flagged: number; differs: number; no_evidence: number };
  inputs_sha256: string;
  notes?: string[];
}

export interface WitnessDecision {
  source: string;
  target: string;
  flag_kind: WitnessFlagKind | null;
  decision: "kept" | "changed";
  new_target: string | null;
  by: string;
  at: string;
}

export interface WitnessRecord {
  mapping_digest: string;
  report: WitnessReport;
  decisions: WitnessDecision[];
  updated_at?: string;
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
  no_evidence: "No evidence",
};

export interface WitnessRequest {
  mappings: Array<{ source: string; target: string; create_new?: boolean; target_type?: string }>;
  source_rows: Record<string, unknown>[];
  destination?: Record<string, unknown> | null;
  target_key_columns?: string[];
  destination_table_exists?: boolean | null;
  date_locale?: string;
}

/** The rows sent: what Map already shows, capped (the server caps again). */
export const WITNESS_MAX_ROWS = 500;

export async function runMappingWitness(body: WitnessRequest, signal?: AbortSignal): Promise<WitnessRecord> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping-witness`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ ...body, source_rows: body.source_rows.slice(0, WITNESS_MAX_ROWS) }),
    signal,
  });
  if (!res.ok) throw new Error(await parseApiError(res, "The mapping check could not run"));
  return res.json();
}

export async function reviewWitnessFlag(body: {
  mapping_digest: string;
  source: string;
  decision: "kept" | "changed";
  new_target?: string;
}): Promise<WitnessDecision> {
  const res = await apiFetch(`${API_BASE}/transfer/mapping-witness/review`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(await parseApiError(res, "The review could not be recorded"));
  return (await res.json()).decision;
}

export async function fetchJobMappingWitness(jobId: string): Promise<WitnessRecord | null> {
  const res = await apiFetch(`${API_BASE}/jobs/${encodeURIComponent(jobId)}/mapping-witness`);
  if (!res.ok) throw new Error(await parseApiError(res, "Mapping evidence could not be loaded"));
  const body = (await res.json()) as { witness?: WitnessRecord | null } | null;
  const witness = body?.witness ?? null;
  if (witness && !Array.isArray(witness.report?.pairs)) throw new Error("Mapping evidence could not be read");
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

/** Stable identity of the mapped pairs the witness judges (order-independent). */
export function witnessPairsKey(mappings: Array<{ source: string; target?: string | null }>): string {
  return mappings
    .filter((m) => m.source && m.target)
    .map((m) => `${m.source.toLowerCase()}␟${String(m.target).toLowerCase()}`)
    .sort()
    .join("␞");
}
