/** Shared ring math — pathLength 100 so 100% is a closed circle, not a mid-gap. */

export function clampPercent(value: number): number {
  if (!Number.isFinite(value)) return 0;
  return Math.max(0, Math.min(100, Math.round(value)));
}

/** Dasharray for an SVG circle with pathLength=100. Complete uses 100 0 (no gap). */
export function ringDasharray(pct: number, opts?: { indeterminate?: boolean }): string {
  if (opts?.indeterminate) return "28 72";
  const n = clampPercent(pct);
  return n >= 100 ? "100 0" : `${n} 100`;
}

/**
 * Validate hero ring. Skipped gates are N/A — they must not leave a finished
 * approve run looking stuck at 88%. Blocked runs show passed / actionable.
 */
export function validateRingPercent(opts: {
  running: boolean;
  passed?: boolean;
  decision?: string;
  passedCount: number;
  blockedCount: number;
  readinessScore?: number;
}): { pct: number; indeterminate: boolean } {
  if (opts.running) return { pct: 0, indeterminate: true };
  if (opts.passed && (opts.decision === "approve" || opts.decision === "review")) {
    return { pct: 100, indeterminate: false };
  }
  const actionable = Math.max(0, opts.passedCount) + Math.max(0, opts.blockedCount);
  if (actionable > 0) {
    return { pct: clampPercent((opts.passedCount / actionable) * 100), indeterminate: false };
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
