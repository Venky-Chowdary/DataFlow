/** Honest Theater % — row ratio while writing; engine phase % before first write. */

export function theaterProgressPct(input: {
  phase?: string | null;
  status?: string | null;
  progress_pct?: number | null;
  total_rows?: number | null;
  records_processed?: number | null;
  progress_indeterminate?: boolean;
  reconciling?: boolean;
  isComplete?: boolean;
  isRunning?: boolean;
}): number {
  const total = Number(input.total_rows ?? 0);
  const processed = Number(input.records_processed ?? 0);
  const reported = Number(input.progress_pct ?? 0);
  const phase = String(input.phase || "").toLowerCase();
  const writing = phase === "writing" || phase === "load";
  const derived = total > 0 ? (processed / Math.max(total, 1)) * 100 : null;
  const indeterminate = Boolean(input.progress_indeterminate) && !(total > 0);

  let raw: number;
  if (input.reconciling) {
    raw = Math.max(reported || 99, 99);
  } else if (writing && derived != null) {
    raw = derived;
  } else if (derived != null && processed > 0) {
    raw = derived;
  } else if (indeterminate) {
    raw = Math.min(reported || 5, 5);
  } else {
    // Pre-write: 0/N must not floor to 1% and fight the engine 2/5 confirm %.
    raw = reported;
  }

  if (input.isComplete) return 100;
  return Math.min(99, Math.max(input.isRunning ? 1 : 0, Math.round(raw)));
}
