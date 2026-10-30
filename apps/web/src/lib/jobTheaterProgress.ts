/** Honest Theater % — row ratio while writing; engine phase % before first write. */

/** ISO date-time with no ``Z`` / ``±hh:mm`` designator. */
const NAIVE_ISO = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?$/;

/**
 * Epoch ms of a server timestamp. The job store returns UTC wall time without
 * an offset (pymongo hands back naive UTC datetimes). ``Date.parse`` reads such
 * a string as *local* time, so west of UTC a 55 s run finished "4 h in the
 * future" and Theater showed "240m 55s" (UTC-4). A server timestamp without an
 * offset is UTC.
 */
export function parseServerTimestampMs(value?: string | null): number | null {
  if (!value) return null;
  const text = String(value).trim();
  const ms = Date.parse(NAIVE_ISO.test(text) ? `${text.replace(" ", "T")}Z` : text);
  return Number.isFinite(ms) ? ms : null;
}

const parseEpochMs = parseServerTimestampMs;

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

/**
 * Wall clock for Theater Elapsed. Terminal and parked jobs freeze — never
 * Date.now() after the run stopped. A terminal job without ``completed_at``,
 * or a job parked on a decision, stops at its last update (``updated_at``).
 */
export function theaterElapsedMs(input: {
  startedAt?: string | null;
  createdAt?: string | null;
  completedAt?: string | null;
  updatedAt?: string | null;
  fallbackStartMs?: number | null;
  nowMs?: number;
  terminal?: boolean;
  parked?: boolean;
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
  const stopped = Boolean(input.terminal || input.parked);
  const lastUpdate = parseEpochMs(input.updatedAt);
  const stoppedAt = lastUpdate != null && lastUpdate <= now + 2_000 ? lastUpdate : null;
  const end = input.parked && !input.terminal
    ? (stoppedAt ?? frozen ?? now)
    : (completed ?? (stopped ? (stoppedAt ?? frozen ?? now) : now));
  return Math.max(0, end - start);
}

/**
 * Engine monotonic seconds published on the finished run.
 * Null when the engine did not measure, so Theater keeps the live wall clock.
 */
export function publishedEngineElapsedSeconds(value: unknown): number | null {
  if (typeof value === "boolean" || value == null || value === "") return null;
  const n = typeof value === "number" ? value : Number(value);
  if (!Number.isFinite(n) || n < 0) return null;
  return n;
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
  reconcile_pct?: number | null;
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
    const scan = Number(input.reconcile_pct);
    if (Number.isFinite(scan) && scan >= 0) {
      raw = 98 + Math.min(99, scan) / 100;
    } else if (reported > 98 && reported < 99) {
      raw = reported;
    } else {
      raw = Math.max(reported || 99, 99);
    }
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
  if (input.reconciling && raw < 99) {
    const painted = Math.min(98.99, Math.max(0, raw));
    return Math.round(painted * 100) / 100;
  }
  return Math.min(99, Math.max(input.isRunning ? 1 : 0, Math.round(raw)));
}
