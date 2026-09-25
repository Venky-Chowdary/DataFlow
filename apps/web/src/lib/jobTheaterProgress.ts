/** Honest Theater % — row ratio while writing; engine phase % before first write. */

function parseEpochMs(value?: string | null): number | null {
  if (!value) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) ? ms : null;
}

/** Job clock: never prefer a heartbeat-reset `started_at` over an earlier `created_at`. */
export function earliestJobStartMs(input: {
  startedAt?: string | null;
  createdAt?: string | null;
  fallbackMs?: number | null;
  nowMs?: number;
}): number {
  const now = input.nowMs ?? Date.now();
  const fallback =
    typeof input.fallbackMs === "number" && Number.isFinite(input.fallbackMs) && input.fallbackMs > 0
      ? input.fallbackMs
      : null;
  const candidates = [parseEpochMs(input.startedAt), parseEpochMs(input.createdAt), fallback].filter(
    (ms): ms is number => ms != null && ms > 0 && ms <= now + 2_000,
  );
  if (!candidates.length) return now;
  return Math.min(...candidates);
}

/** Wall clock for Theater Elapsed. Terminal jobs freeze — never Date.now() after done. */
export function theaterElapsedMs(input: {
  startedAt?: string | null;
  createdAt?: string | null;
  completedAt?: string | null;
  fallbackStartMs?: number | null;
  nowMs?: number;
  terminal?: boolean;
  frozenEndMs?: number | null;
}): number {
  const now = input.nowMs ?? Date.now();
  const start = earliestJobStartMs({
    startedAt: input.startedAt,
    createdAt: input.createdAt,
    fallbackMs: input.fallbackStartMs,
    nowMs: now,
  });
  const completed = parseEpochMs(input.completedAt);
  const frozen =
    typeof input.frozenEndMs === "number" && Number.isFinite(input.frozenEndMs) && input.frozenEndMs > 0
      ? input.frozenEndMs
      : null;
  const end = completed ?? (input.terminal ? (frozen ?? now) : now);
  return Math.max(0, end - start);
}

/** Job-average rows/s. Refuse a reconnect-window invent (460k / 0.5s). */
export function jobAverageRowsPerSecond(processed: number, elapsedMs: number): number {
  if (!(processed > 0) || !(elapsedMs >= 5_000)) return 0;
  return Math.round(processed / (elapsedMs / 1000));
}

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
