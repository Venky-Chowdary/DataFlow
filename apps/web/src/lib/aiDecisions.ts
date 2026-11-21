/**
 * AI decisions ledger client (`/api/v1/jobs/{id}/ai-decisions`).
 *
 * One row per AI involvement on a job: what was proposed, by a deterministic
 * engine or which model, what a person decided, and which check verified it.
 * The same rows are sealed into the signed proof pack and the signed Trust Report.
 */
import { apiFetch, parseApiError } from "./api";
import { API_BASE } from "./types";

export type AiOrigin =
  | { type: "deterministic"; engine: string; method?: string }
  | { type: "model"; provider: string; model: string };

export interface AiDecisionEntry {
  id: string;
  kind:
    | "mapping_proposal"
    | "model_mapping_suggestion"
    | "witness_flag"
    | "parity_explanation"
    | "parity_fix"
    | "model_call"
    | string;
  subject: string;
  origin: AiOrigin;
  inputs_sha256: string;
  proposal: Record<string, unknown>;
  decision: { status: string; by: string | null; at: string | null; how: string };
  verification: { check: string; status: string; detail?: string };
}

export interface AiDecisionLedger {
  version: string;
  job_id: string;
  status?: "unavailable";
  reason?: string;
  entries: AiDecisionEntry[];
  counts?: { total: number; by_kind: Record<string, number>; by_origin: Record<string, number>; model_calls: number };
  ledger_sha256?: string;
}

export const KIND_LABEL: Record<string, string> = {
  mapping_proposal: "Mapping",
  model_mapping_suggestion: "Model suggestion",
  witness_flag: "Mapping check",
  parity_explanation: "Explanation",
  parity_fix: "Fix",
  model_call: "Model call",
};

const DECISION_LABEL: Record<string, string> = {
  accepted: "Accepted",
  changed: "Changed by a person",
  rejected: "Not adopted",
  held: "Held for review",
  kept: "Kept",
  unreviewed: "Not reviewed",
  informational: "No decision needed",
};

const CHECK_LABEL: Record<string, string> = {
  mapping_witness: "Mapping check",
  parity_recheck: "Re-check",
  parity_classifier: "Cause classifier",
  ai_egress_gate: "Egress gate",
  human_judgement: "Person's judgement",
  none: "Not checked",
};

const ENGINE_LABEL: Record<string, string> = {
  semantic_mapper: "the Datawrap mapper",
  parity_fixes: "Parity Studio",
  parity_template: "a template",
};

/** Who proposed it: the provider and model, or the deterministic engine by its product name. */
export function originLabel(origin: AiOrigin): string {
  if (origin.type === "model") {
    return `${origin.provider}${origin.model && origin.model !== "unrecorded" ? ` · ${origin.model}` : ""}`;
  }
  if (origin.engine.startsWith("mapping_witness")) return "the mapping check";
  return ENGINE_LABEL[origin.engine] ?? "Datawrap (deterministic)";
}

export function decisionLabel(entry: AiDecisionEntry): string {
  const base = DECISION_LABEL[entry.decision.status] ?? entry.decision.status.replace(/_/g, " ");
  return entry.decision.by ? `${base} · ${entry.decision.by}` : base;
}

export function checkLabel(entry: AiDecisionEntry): string {
  const check = CHECK_LABEL[entry.verification.check] ?? entry.verification.check.replace(/_/g, " ");
  if (entry.verification.check === "none") return check;
  return `${check}: ${entry.verification.status.replace(/_/g, " ")}`;
}

export type AiTone = "ok" | "warn" | "muted";

/** Green only where a check confirmed it; amber where a check disagreed or nobody looked. */
export function checkTone(entry: AiDecisionEntry): AiTone {
  const s = entry.verification.status;
  if (["witnessed", "passed", "supported", "metadata_only", "causes_decided_deterministically"].includes(s)) return "ok";
  if (["flagged", "contradicted", "no_change"].includes(s)) return "warn";
  return "muted";
}

export async function fetchAiDecisions(jobId: string): Promise<AiDecisionLedger> {
  const res = await apiFetch(`${API_BASE}/jobs/${encodeURIComponent(jobId)}/ai-decisions`);
  if (!res.ok) throw new Error(await parseApiError(res, "AI decisions could not be loaded"));
  const body = (await res.json()) as Partial<AiDecisionLedger> | null;
  // A body that is not a ledger is an error, never an empty ledger ("no AI involved").
  if (!body || !Array.isArray(body.entries)) throw new Error("AI decisions could not be read");
  return body as AiDecisionLedger;
}
