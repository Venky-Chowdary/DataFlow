/** Shared ring math — pathLength 100 so 100% is a closed circle, not a mid-gap. */

export function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

/** Dasharray for an SVG circle with pathLength=100. Complete uses 100 0 (no gap). */
export function ringDasharray(pct: number, opts?: { indeterminate?: boolean; exact?: boolean }): string {
  if (opts?.indeterminate) return "28 72";
  const n = opts?.exact
    ? Math.max(0, Math.min(100, Number.isFinite(pct) ? pct : 0))
    : clampPercent(pct);
  return n >= 100 ? "100 0" : `${n} 100`;
}

/** Jobs-list bar during checksum. Tenths from reconcile_pct, never rounded to 99. */
export function reconcileBarPercent(
  progressPct: number | null | undefined,
  reconcilePct: number | null | undefined,
  phase?: string | null,
): number {
  const reconciling = String(phase || "").toLowerCase() === "reconcile";
  const scan = Number(reconcilePct);
  if (reconciling && Number.isFinite(scan) && scan >= 0) {
    return Math.min(98.99, 98 + Math.min(99, scan) / 100);
  }
  return clampPercent(progressPct ?? 0);
}

/**
 * passed / applicable as a whole percent that reaches 100 only when every
 * applicable check passed: 199 of 200 is 99%, never a rounded-up 100%.
 */
export function ratioPercent(passed: number, applicable: number): number {
  if (!(applicable > 0)) return 0;
  const p = Math.max(0, Math.min(passed, applicable));
  if (p >= applicable) return 100;
  return Math.min(99, clampPercent((p / applicable) * 100));
}

/**
 * Validate hero ring: passed ÷ applicable, where applicable is every gate
 * except the skipped ones (N/A — they must not leave a finished run stuck at
 * 88%). Gates in review or blocked count against the ring, so "16/22 passed ·
 * 5 skipped · 1 review" reads 94%, not 100%. The decision (approve / review)
 * never fills the ring on its own; the decision chip says that.
 */
export function validateRingPercent(opts: {
  running: boolean;
  passed?: boolean;
  decision?: string;
  passedCount: number;
  blockedCount: number;
  /** Every gate in the run, skipped included. */
  totalCount?: number;
  skippedCount?: number;
  /** Gates that ended in review (not passed, not blocked). */
  reviewCount?: number;
  readinessScore?: number;
}): { pct: number; indeterminate: boolean } {
  if (opts.running) return { pct: 0, indeterminate: true };
  const passed = Math.max(0, opts.passedCount);
  const total = Math.max(0, opts.totalCount ?? 0);
  const applicable = total > 0
    ? Math.max(passed, total - Math.max(0, opts.skippedCount ?? 0))
    : passed + Math.max(0, opts.blockedCount) + Math.max(0, opts.reviewCount ?? 0);
  if (applicable > 0) {
    return { pct: ratioPercent(passed, applicable), indeterminate: false };
  }
  return { pct: clampPercent(opts.readinessScore ?? 0), indeterminate: false };
}

/** Discrete stage bar — completed / total, never invented 8/22/42. */
export function stagePercent(completed: number, total: number): number {
  if (total <= 0) return 0;
  return clampPercent((Math.max(0, completed) / total) * 100);
}

/** Launch-chip state from real stage progress — 100% marks every chip done. */
export function launchStageState(
  progress: number,
  index: number,
  total: number,
): "done" | "active" | "pending" {
  if (total <= 0) return "pending";
  if (clampPercent(progress) >= 100) return "done";
  const doneAt = stagePercent(index + 1, total);
  const startAt = stagePercent(index, total);
  if (progress >= doneAt) return "done";
  if (progress >= startAt) return "active";
  return "pending";
}
