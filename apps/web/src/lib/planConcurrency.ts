/**
 * Transfer-plan writes that cannot silently overwrite each other (PS-1).
 *
 * The API refuses a mapping save based on an older plan revision (409
 * `plan_version_conflict`), answers 503 `plan_store_busy` (with Retry-After)
 * when another writer holds the plan store, and 503 `plan_store_unreadable`
 * when the store file is damaged and an operator has to restore it. This module
 * turns those answers into typed errors the Studio can show plainly, and
 * retries a busy store once.
 */

export const PLAN_CHANGED_MESSAGE = "This plan changed since you opened it — reload to see the latest";

/** The plan gained a newer revision (or an approval/revert) since it was loaded. */
export class PlanVersionConflictError extends Error {
  readonly code = "plan_version_conflict";
  readonly baseVersion: number | null;
  readonly currentVersion: number | null;
  /** "head" (newest revision) or "selection" (active / approved revision). */
  readonly compared: string;
  readonly serverMessage: string;

  constructor(baseVersion: number | null, currentVersion: number | null, compared = "head", serverMessage = "") {
    super(PLAN_CHANGED_MESSAGE);
    this.name = "PlanVersionConflictError";
    this.baseVersion = baseVersion;
    this.currentVersion = currentVersion;
    this.compared = compared;
    this.serverMessage = serverMessage;
  }
}

/** Another request held the plan store; one retry did not help. */
export class PlanStoreBusyError extends Error {
  readonly code = "plan_store_busy";

  constructor(detail = "") {
    super(detail || "The plan is being saved by another request. Try again in a moment.");
    this.name = "PlanStoreBusyError";
  }
}

/** The server's plan store file is damaged: an operator must repair or restore it. */
export class PlanStoreUnreadableError extends Error {
  readonly code = "plan_store_unreadable";

  constructor(detail = "") {
    const fallback = "Transfer plans cannot be loaded: the server's plan store is damaged. "
      + "An operator must repair it or restore its backup; nothing was saved.";
    super(
      !detail
        ? fallback
        : /operator/i.test(detail)
          ? detail
          : `${detail} An operator must repair or restore the plan store; nothing was saved.`,
    );
    this.name = "PlanStoreUnreadableError";
  }
}

export function isPlanVersionConflict(err: unknown): err is PlanVersionConflictError {
  return err instanceof PlanVersionConflictError;
}

/** The newest `revisions[].version` of a plan (0 for none): what `base_version` names. */
export function headVersion(plan: { revisions?: Array<{ version?: number | null }> | null } | null | undefined): number {
  let head = 0;
  for (const rev of plan?.revisions ?? []) {
    const v = Number(rev?.version);
    if (Number.isFinite(v) && v > head) head = v;
  }
  return head;
}

const DEFAULT_RETRY_MS = 2000;
const MAX_RETRY_MS = 10_000;

/** Retry-After in milliseconds (seconds form), bounded; a default when absent or invalid. */
export function retryAfterMs(res: Response): number {
  const raw = res.headers.get("Retry-After");
  const seconds = raw == null ? NaN : Number(raw.trim());
  if (!Number.isFinite(seconds) || seconds < 0) return DEFAULT_RETRY_MS;
  return Math.min(seconds * 1000, MAX_RETRY_MS);
}

async function body(res: Response): Promise<Record<string, unknown> | null> {
  try {
    const data = await res.clone().json();
    return data && typeof data === "object" ? (data as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

function num(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

const defaultSleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

/**
 * Send a plan write and interpret the plan-store answers.
 *
 * - 409 `plan_version_conflict` → {@link PlanVersionConflictError} (never retried);
 * - 503 `plan_store_busy` → wait Retry-After, send once more; still busy → {@link PlanStoreBusyError};
 * - 503 `plan_store_unreadable` → {@link PlanStoreUnreadableError} (no retry: an operator has to act).
 *
 * Any other response is returned for the caller's own handling.
 */
export async function planWriteResponse(
  send: () => Promise<Response>,
  options: { sleep?: (ms: number) => Promise<void> } = {},
): Promise<Response> {
  const sleep = options.sleep ?? defaultSleep;
  let res = await send();
  for (let attempt = 0; ; attempt += 1) {
    if (res.status === 409) {
      const data = await body(res);
      const detail = data?.detail as Record<string, unknown> | undefined;
      if (detail && typeof detail === "object" && detail.code === "plan_version_conflict") {
        throw new PlanVersionConflictError(
          num(detail.base_version),
          num(detail.current_version),
          typeof detail.compared === "string" ? detail.compared : "head",
          typeof detail.message === "string" ? detail.message : "",
        );
      }
      return res;
    }
    if (res.status !== 503) return res;
    const data = await body(res);
    const detail = typeof data?.detail === "string" ? data.detail : "";
    if (data?.error === "plan_store_unreadable" || data?.error === "plan_store_lock_unavailable") {
      // An operator has to repair or restore the store / its lock file: no retry.
      throw new PlanStoreUnreadableError(detail);
    }
    if (data?.error !== "plan_store_busy") return res;
    if (attempt >= 1) throw new PlanStoreBusyError(detail);
    await sleep(retryAfterMs(res));
    res = await send();
  }
}

/** One mapping row as the plan API stores it (a revision's `mappings[]`). */
export interface PlanMappingRow {
  source: string;
  target: string;
  confidence?: number;
  transform?: string;
  reason?: string;
  user_override?: boolean;
  intentional_omit?: boolean;
  omit_reason?: string;
  omit_reason_text?: string;
  [extra: string]: unknown;
}

/** One destination column compared between the latest revision and the user's unsaved mappings. */
export interface PlanMappingDiffRow {
  /** Destination column as written (an omitted source shows as "<source> (not written)"). */
  target: string;
  /** Joined for display ("a + b" when a side maps two sources); null when that side maps nothing. */
  latestSource: string | null;
  mineSource: string | null;
  latestSources: string[];
  mineSources: string[];
  /** Either side maps more than one source to this column (a duplicate the Map step flags). */
  duplicate: boolean;
  differs: boolean;
}

/** A revision of a persisted plan as GET /transfer/plans/{id} returns it. */
export interface PlanRevisionView {
  version: number;
  mappings?: PlanMappingRow[];
  validation?: { source?: string } | null;
  agents_used?: string[];
  plan_summary?: { client_request_id?: string } | null;
}

export interface PlanView {
  id: string;
  active_version?: number;
  revisions?: PlanRevisionView[];
}

/** The revision Execute would run: `active_version`, else the newest. */
export function activeRevision(plan: PlanView | null | undefined): PlanRevisionView | null {
  const revisions = plan?.revisions ?? [];
  if (!revisions.length) return null;
  return revisions.find((r) => r.version === plan?.active_version) ?? revisions[revisions.length - 1];
}

function isOmit(row: PlanMappingRow): boolean {
  return Boolean(row.intentional_omit) || row.transform === "omit";
}

/**
 * Sources per destination column, keyed case-insensitively; every source is
 * kept (a duplicate is two sources, not the last one). Omitted columns have
 * no destination: they are keyed by source and shown as "omitted".
 */
function sourcesByTarget(rows: PlanMappingRow[]): Map<string, { label: string; sources: string[] }> {
  const out = new Map<string, { label: string; sources: string[] }>();
  for (const row of rows) {
    if (!row) continue;
    const omitted = isOmit(row);
    const target = (row.target ?? "").trim();
    if (!omitted && !target) continue;
    const key = omitted ? `omit:${(row.source ?? "").toLowerCase()}` : target.toLowerCase();
    const label = omitted ? `${row.source} (not written)` : target;
    const value = omitted
      ? "omitted"
      : row.transform ? `${row.source} (${row.transform})` : (row.source ?? "");
    const entry = out.get(key) ?? { label, sources: [] };
    entry.sources.push(value);
    out.set(key, entry);
  }
  return out;
}

/**
 * Per destination column: which source(s) each side maps, and whether they
 * differ. Columns only one side maps appear with `null` on the other side;
 * a column a side maps from two sources shows both (`duplicate`). For
 * display only: silent adoption uses {@link sameFullMappings}.
 */
export function diffPlanMappings(latest: PlanMappingRow[], mine: PlanMappingRow[]): PlanMappingDiffRow[] {
  const theirs = sourcesByTarget(latest);
  const ours = sourcesByTarget(mine);
  const keys = [...new Set([...theirs.keys(), ...ours.keys()])];
  return keys.map((key) => {
    const a = theirs.get(key);
    const b = ours.get(key);
    const latestSources = [...(a?.sources ?? [])].sort();
    const mineSources = [...(b?.sources ?? [])].sort();
    return {
      target: (a ?? b)!.label,
      latestSource: latestSources.length ? latestSources.join(" + ") : null,
      mineSource: mineSources.length ? mineSources.join(" + ") : null,
      latestSources,
      mineSources,
      duplicate: latestSources.length > 1 || mineSources.length > 1,
      differs: latestSources.join("\u0000") !== mineSources.join("\u0000"),
    };
  });
}

/** True when both sides map every destination column from the same source(s) (display level). */
export function sameMappings(a: PlanMappingRow[], b: PlanMappingRow[]): boolean {
  return diffPlanMappings(a, b).every((row) => !row.differs);
}

function canonicalRows(rows: PlanMappingRow[]): string {
  const canon = rows.map((r) => ({
    t: (r.target ?? "").trim(),
    s: r.source ?? "",
    x: r.transform ?? "",
    u: Boolean(r.user_override),
    o: isOmit(r),
    r: r.omit_reason ?? "",
    rt: r.omit_reason_text ?? "",
    c: Math.round((typeof r.confidence === "number" ? r.confidence : 1) * 1000) / 1000,
  }));
  canon.sort((p, q) => (p.t + "\u0000" + p.s + "\u0000" + p.x).localeCompare(q.t + "\u0000" + q.s + "\u0000" + q.x));
  return JSON.stringify(canon);
}

/**
 * Whether two mapping sets are the same decision, row for row: target,
 * source, transform, user override, intentional omit and its reasons, and
 * confidence. Used before adopting a revision silently as "this page's own";
 * the per-column diff ignores most of these and is for display only.
 */
export function sameFullMappings(a: PlanMappingRow[], b: PlanMappingRow[]): boolean {
  return canonicalRows(a) === canonicalRows(b);
}

/**
 * After this page's Map failed on the client (timed out, aborted, network)
 * while the server may still have committed it: the head this page may
 * adopt, or null.
 *
 * Only the revision that carries this page's own `client_request_id` (sent
 * with the Map, stored as `plan_summary.client_request_id`) and is the
 * plan's newest revision proves the Map was ours. Anything else — another
 * user's Map, a UI save, a later save on top — leaves the head alone, so the
 * next save meets the comparison dialog.
 */
export function headAfterOwnMap(
  plan: PlanView | null | undefined,
  knownHead: number | null,
  requestId: string,
): number | null {
  if (knownHead == null || !requestId) return null;
  const revisions = plan?.revisions ?? [];
  if (!revisions.length) return null;
  const head = revisions.reduce((a, b) => (b.version > a.version ? b : a));
  if (head.version <= knownHead) return null;
  return head.plan_summary?.client_request_id === requestId ? head.version : null;
}

/** True when the request failed before any server answer (timeout, abort, network). */
export function isClientSideFailure(err: unknown): boolean {
  if (err instanceof TypeError) return true;
  const e = err as { name?: string; message?: string } | null;
  return e?.name === "AbortError" || e?.message === "Request timed out";
}

/** A fresh id for one Map request (matches the API's `client_request_id` pattern). */
export function newMapRequestId(): string {
  const c = (globalThis as { crypto?: { randomUUID?: () => string } }).crypto;
  const raw = c?.randomUUID?.() ?? `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
  return `map-${raw}`;
}

/**
 * After this page's own write failed without a conflict (the backend may
 * still have stored it, e.g. Execute's merge_plan_into_run recovery): the
 * plan's head if its active revision holds exactly `mine`, else null.
 */
export function headIfOurs(plan: PlanView | null | undefined, mine: PlanMappingRow[]): number | null {
  const latest = activeRevision(plan);
  if (!latest || !sameFullMappings(latest.mappings ?? [], mine)) return null;
  return headVersion(plan);
}
