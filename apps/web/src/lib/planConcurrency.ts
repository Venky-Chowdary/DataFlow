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
}

/** One target column compared between the latest revision and the user's unsaved mappings. */
export interface PlanMappingDiffRow {
  target: string;
  latestSource: string | null;
  mineSource: string | null;
  differs: boolean;
}

/** A revision of a persisted plan as GET /transfer/plans/{id} returns it. */
export interface PlanRevisionView {
  version: number;
  mappings?: PlanMappingRow[];
  validation?: { source?: string } | null;
  agents_used?: string[];
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

function sourceByTarget(rows: PlanMappingRow[]): Map<string, string> {
  const out = new Map<string, string>();
  for (const row of rows) {
    if (!row?.target) continue;
    const src = row.transform ? `${row.source} (${row.transform})` : row.source;
    out.set(row.target, src ?? "");
  }
  return out;
}

/**
 * Per destination column: which source each side maps, and whether they differ.
 * Columns only one side maps appear with `null` on the other side.
 */
export function diffPlanMappings(latest: PlanMappingRow[], mine: PlanMappingRow[]): PlanMappingDiffRow[] {
  const theirs = sourceByTarget(latest);
  const ours = sourceByTarget(mine);
  const targets = [...new Set([...theirs.keys(), ...ours.keys()])];
  return targets.map((target) => {
    const latestSource = theirs.get(target) ?? null;
    const mineSource = ours.get(target) ?? null;
    return { target, latestSource, mineSource, differs: latestSource !== mineSource };
  });
}

/** True when both sides map every destination column from the same source. */
export function sameMappings(a: PlanMappingRow[], b: PlanMappingRow[]): boolean {
  return diffPlanMappings(a, b).every((row) => !row.differs);
}

/**
 * After a Map that failed on the client (e.g. timed out) while the server may
 * still have committed it: the head this page may adopt, or null.
 *
 * Every revision newer than `knownHead` must be a Map result (not a UI save,
 * whose validation source is "ui_sync"), and there may be at most one: that
 * is this page's own Map landing. Anything else is someone else's work, and
 * the head stays as it was, so the next save meets the conflict dialog.
 */
export function headAfterOwnMap(plan: PlanView | null | undefined, knownHead: number | null): number | null {
  if (knownHead == null) return null;
  const newer = (plan?.revisions ?? []).filter((r) => r.version > knownHead);
  if (newer.length !== 1) return null;
  const [rev] = newer;
  if (rev.validation?.source === "ui_sync") return null;
  return rev.version;
}

/**
 * After this page's own write failed without a conflict (the backend may
 * still have stored it, e.g. Execute's merge_plan_into_run recovery): the
 * plan's head if its active revision holds exactly `mine`, else null.
 */
export function headIfOurs(plan: PlanView | null | undefined, mine: PlanMappingRow[]): number | null {
  const latest = activeRevision(plan);
  if (!latest || !sameMappings(latest.mappings ?? [], mine)) return null;
  return headVersion(plan);
}
