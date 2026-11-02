/**
 * Live Validate progress — wall clock and rows scanned, never a looping stage show.
 *
 * The old ticker cycled G1–G9 every 1.1s (`index % 9`). A 1M CSV that sat
 * in GET /preflight for 300s looked like the same nine steps repeating.
 * Progress is whatever the worker published; missing counts stay a single
 * honest "engine is running" line.
 */

export type ValidateProgress = {
  elapsed_ms?: number;
  rows_scanned?: number;
  rows_estimate?: number;
  phase?: string;
  status?: string;
};

function formatElapsed(ms: number): string {
  const s = ms / 1000;
  return s < 10 ? `${s.toFixed(1)}s` : `${Math.round(s)}s`;
}

export function engineProgressCopy(progress: ValidateProgress | null | undefined, elapsedMs: number): {
  count: string;
  name: string;
} {
  const scanned = Math.max(0, Number(progress?.rows_scanned) || 0);
  const total = Math.max(0, Number(progress?.rows_estimate) || 0);
  const elapsed = formatElapsed(elapsedMs);
  const phase = String(progress?.phase || "").trim();

  if (scanned > 0 && total > 0) {
    return {
      count: `${scanned.toLocaleString()} / ${total.toLocaleString()} · ${elapsed}`,
      name: "Scanning population fit",
    };
  }
  if (scanned > 0) {
    return {
      count: `${scanned.toLocaleString()} · ${elapsed}`,
      name: "Scanning population fit",
    };
  }
  if (phase === "scanning_population_fit") {
    return {
      count: elapsed,
      name: "Scanning population fit — live row counts arrive as the worker heartbeats",
    };
  }
  return {
    count: elapsed,
    name: "Engine running — waiting for a live gate result, not a repeating stage list",
  };
}
