/**
 * Reversibility Proof client: the exit inventory, rehearsals, the Exit Evidence
 * Pack and the reversibility gate reads (contract sections 1, 3-5).
 *
 * Every refusal keeps its HTTP status (ApiError) so a page can tell a 409
 * ("a rehearsal is running", "sign-off blocked", stale version) from a 422 or
 * a transport failure.
 */
import { apiErrorFrom, apiFetch } from "./api";
import { API_BASE } from "./types";
import {
  normalizeRehearsal,
  type ExitReadiness,
  type ExitRehearsal,
  type ExitSystem,
  type ExitSystemInput,
  type ExitVerifyResult,
  type RehearsalSummary,
  type Trend,
} from "./exitReadiness";
import type { ReversibilityReport } from "./reversibility";

type Init = { signal?: AbortSignal };

async function json<T>(res: Response, fallback: string): Promise<T> {
  if (!res.ok) throw await apiErrorFrom(res, fallback);
  return (await res.json()) as T;
}

const enc = encodeURIComponent;

/* ── Exit inventory (Page A) ─────────────────────────────────── */

export async function fetchExitReadiness(init: Init = {}): Promise<ExitReadiness> {
  const res = await apiFetch(`${API_BASE}/exit/readiness`, { signal: init.signal });
  return json(res, "Could not load exit readiness.");
}

export async function fetchExitSystems(init: Init = {}): Promise<ExitSystem[]> {
  const res = await apiFetch(`${API_BASE}/exit/systems`, { signal: init.signal });
  const body = await json<{ systems: ExitSystem[] }>(res, "Could not load the declared systems.");
  return body.systems ?? [];
}

export async function fetchExitSystem(systemId: string, init: Init = {}): Promise<ExitSystem> {
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}`, { signal: init.signal });
  return json(res, "Could not load this system.");
}

/** Declare a system (201). 422 names the field the API refused. */
export async function declareExitSystem(input: ExitSystemInput): Promise<ExitSystem> {
  const res = await apiFetch(`${API_BASE}/exit/systems`, { method: "POST", body: JSON.stringify(input) });
  return json(res, "Could not declare this system.");
}

/** Change a system; `expectedVersion` makes a stale edit a 409 instead of a silent overwrite. */
export async function updateExitSystem(
  systemId: string,
  patch: Partial<ExitSystemInput>,
  expectedVersion?: number,
): Promise<ExitSystem> {
  const body = { ...patch, ...(typeof expectedVersion === "number" ? { expected_version: expectedVersion } : {}) };
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}`, { method: "PATCH", body: JSON.stringify(body) });
  return json(res, "Could not save this system.");
}

/** Remove a system (204). Its rehearsals and evidence are kept; 409 while a rehearsal runs. */
export async function deleteExitSystem(systemId: string): Promise<void> {
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}`, { method: "DELETE" });
  if (!res.ok) throw await apiErrorFrom(res, "Could not remove this system.");
}

/* ── PII review (contract 11.1.7) ────────────────────────────── */

/**
 * Acknowledge the PII a system's tables hold: its rehearsal extracts then pass the PII gate as an acknowledged
 * transfer. The API records the caller as the actor (never the body); 422 for a reason under 8 characters.
 */
export async function acknowledgeExitPii(systemId: string, reason: string, expectedVersion?: number): Promise<ExitSystem> {
  const body = { reason: reason.trim(), ...(typeof expectedVersion === "number" ? { expected_version: expectedVersion } : {}) };
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}/pii-acknowledgement`, { method: "POST", body: JSON.stringify(body) });
  return json(res, "Could not record the PII acknowledgement.");
}

/** Withdraw it: the next rehearsal's extracts are held by the PII gate again. */
export async function withdrawExitPii(systemId: string): Promise<ExitSystem> {
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}/pii-acknowledgement`, { method: "DELETE" });
  return json(res, "Could not withdraw the PII acknowledgement.");
}

/* ── Rehearsals (Page B) ─────────────────────────────────────── */

/** Start a rehearsal now (202). 409 while one already runs for the system. */
export async function startExitRehearsal(systemId: string): Promise<ExitRehearsal> {
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}/rehearsals`, { method: "POST", body: "{}" });
  return normalizeRehearsal(await json<ExitRehearsal>(res, "Could not start the rehearsal."));
}

export async function fetchSystemRehearsals(
  systemId: string,
  limit = 20,
  init: Init = {},
): Promise<{ rehearsals: RehearsalSummary[]; trend: Trend }> {
  // The route accepts 1..100 (422 outside it).
  const safe = Math.max(1, Math.min(100, Math.floor(limit) || 20));
  const res = await apiFetch(`${API_BASE}/exit/systems/${enc(systemId)}/rehearsals?limit=${safe}`, { signal: init.signal });
  return json(res, "Could not load the rehearsals of this system.");
}

export async function fetchExitRehearsal(rehearsalId: string, init: Init = {}): Promise<ExitRehearsal> {
  const res = await apiFetch(`${API_BASE}/exit/rehearsals/${enc(rehearsalId)}`, { signal: init.signal });
  return normalizeRehearsal(await json<ExitRehearsal>(res, "Could not load this rehearsal."));
}

/** Sign as one required role. 409 unless the rehearsal is proven, or on a stale version. */
export async function signExitRehearsal(
  rehearsalId: string,
  role: string,
  note?: string,
  expectedVersion?: number,
): Promise<ExitRehearsal> {
  const body = {
    role,
    ...(note?.trim() ? { note: note.trim() } : {}),
    ...(typeof expectedVersion === "number" ? { expected_version: expectedVersion } : {}),
  };
  const res = await apiFetch(`${API_BASE}/exit/rehearsals/${enc(rehearsalId)}/signoff`, { method: "POST", body: JSON.stringify(body) });
  return normalizeRehearsal(await json<ExitRehearsal>(res, "Could not record your sign-off."));
}

/** The readable evidence report (Markdown), signed or not. */
export async function downloadExitEvidenceMarkdown(rehearsalId: string): Promise<Blob> {
  const res = await apiFetch(`${API_BASE}/exit/rehearsals/${enc(rehearsalId)}/evidence?format=markdown`);
  if (!res.ok) throw await apiErrorFrom(res, "Could not export the evidence report.");
  return res.blob();
}

/** The signed Exit Evidence Pack (JSON); 409 until every required role has signed. */
export async function fetchExitEvidencePack(rehearsalId: string): Promise<Record<string, unknown>> {
  const res = await apiFetch(`${API_BASE}/exit/rehearsals/${enc(rehearsalId)}/evidence?format=json`);
  return json(res, "Could not download the signed evidence pack.");
}

/** Verify a pack: signature, Merkle root over its parts, and its anchor read back now. */
export async function verifyExitEvidence(pack: unknown): Promise<ExitVerifyResult> {
  // A file saved as `{pack: …}` is sent as it is; a bare pack is wrapped (the route accepts both).
  const wrapped = pack && typeof pack === "object" && "pack" in (pack as Record<string, unknown>) ? pack : { pack };
  const res = await apiFetch(`${API_BASE}/exit/evidence/verify`, { method: "POST", body: JSON.stringify(wrapped) });
  return json(res, "Could not verify this evidence pack.");
}

/* ── Reversibility gate (Page C) ─────────────────────────────── */

export async function fetchPlanReversibility(planId: string, init: Init = {}): Promise<ReversibilityReport> {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${enc(planId)}/reversibility`, { signal: init.signal });
  return json(res, "Could not analyse the reversibility of this plan.");
}

export async function fetchJobReversibility(jobId: string, init: Init = {}): Promise<ReversibilityReport> {
  const res = await apiFetch(`${API_BASE}/jobs/${enc(jobId)}/reversibility`, { signal: init.signal });
  return json(res, "Could not analyse the reversibility of this run.");
}

/** Set the plan policy `require_reversibility` (policies merge on the server). */
export async function setPlanRequireReversibility(planId: string, require: boolean): Promise<void> {
  const res = await apiFetch(`${API_BASE}/transfer/plans/${enc(planId)}`, {
    method: "PATCH",
    body: JSON.stringify({ policies: { require_reversibility: require } }),
  });
  if (!res.ok) throw await apiErrorFrom(res, "Could not change the reversibility requirement.");
}
