/**
 * Presentation helpers for the engine's per-phase timing breakdown.
 *
 * The engine reports where a transfer's wall time actually went — reading the
 * source, transforming and writing, or re-reading to verify the checksum.
 * Before this, a slow transfer was a single number and an operator had no way
 * to tell an under-provisioned source from a slow destination from an
 * expensive verification step.
 *
 * The arithmetic lives here rather than in the component so it can be tested
 * without a DOM, matching the other pure helpers in this folder.
 */

import type { PhaseProfileReport, PhaseTiming } from "./types";

export interface PhaseRow {
  phase: string;
  label: string;
  seconds: number;
  /** Percentage of busy time, already rounded for display. */
  percent: number;
  rows: number;
  rowsPerSecond: number;
  secondsLabel: string;
  throughputLabel: string;
  /** True for the phase that consumed the most time. */
  dominant: boolean;
}

export interface PhaseProfileView {
  rows: PhaseRow[];
  busySeconds: number;
  elapsedSeconds: number;
  dominantLabel: string;
  /** Plain-language summary of where the time went. */
  headline: string;
  /** Caveat shown when phases overlapped, so percentages are not of wall time. */
  overlapNote: string;
}

/** Human-readable duration. Sub-second work is common on small transfers. */
export function formatSeconds(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds < 0.001) return "<1ms";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds % 60);
  if (minutes < 60) return `${minutes}m ${rest}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

/** Compact row throughput, e.g. `1.2M rows/s`. */
export function formatThroughput(rowsPerSecond: number): string {
  if (!Number.isFinite(rowsPerSecond) || rowsPerSecond <= 0) return "—";
  if (rowsPerSecond >= 1_000_000) return `${(rowsPerSecond / 1_000_000).toFixed(1)}M rows/s`;
  if (rowsPerSecond >= 1_000) return `${(rowsPerSecond / 1_000).toFixed(1)}K rows/s`;
  return `${Math.round(rowsPerSecond)} rows/s`;
}

function toRow(timing: PhaseTiming, busy: number, dominant: string): PhaseRow {
  const seconds = Number(timing.seconds) || 0;
  // Recompute the share rather than trusting share_of_busy: an older engine
  // build may not send it, and a stale value would contradict the seconds
  // shown right beside it.
  const percent = busy > 0 ? Math.round((seconds / busy) * 1000) / 10 : 0;
  const rows = Number(timing.rows) || 0;
  const rowsPerSecond = Number(timing.rows_per_second) || (seconds > 0 ? rows / seconds : 0);
  return {
    phase: timing.phase,
    label: timing.label || timing.phase,
    seconds,
    percent,
    rows,
    rowsPerSecond,
    secondsLabel: formatSeconds(seconds),
    throughputLabel: rows > 0 ? formatThroughput(rowsPerSecond) : "—",
    dominant: timing.phase === dominant,
  };
}

/**
 * Turn the raw report into rows ready to render, or `null` when there is
 * nothing worth showing.
 *
 * Returning `null` rather than an empty view lets the caller omit the whole
 * section instead of rendering an empty card, which is the difference between
 * a dense dashboard and a ragged one.
 */
export function buildPhaseProfileView(
  report: PhaseProfileReport | null | undefined
): PhaseProfileView | null {
  if (!report || !Array.isArray(report.phases) || report.phases.length === 0) return null;

  const busy = Number(report.busy_seconds) || report.phases.reduce((sum, p) => sum + (Number(p.seconds) || 0), 0);
  if (busy <= 0) return null;

  const dominant = report.dominant_phase || "";
  const rows = report.phases
    .map((p) => toRow(p, busy, dominant))
    .sort((a, b) => b.seconds - a.seconds);

  // Fall back to the slowest row when the engine did not name a dominant
  // phase, so the headline is never blank.
  const lead = rows.find((r) => r.dominant) ?? rows[0];
  const elapsed = Number(report.elapsed_seconds) || 0;
  const overlap = Number(report.overlap_factor) || 0;

  return {
    rows,
    busySeconds: busy,
    elapsedSeconds: elapsed,
    dominantLabel: lead?.label ?? "",
    headline: lead
      ? `${lead.label} took ${lead.secondsLabel}, ${lead.percent}% of engine time.`
      : "",
    overlapNote:
      overlap > 1.15
        ? `Phases ran concurrently (${overlap.toFixed(1)}× overlap), so shares are of engine time, not of the ${formatSeconds(
            elapsed
          )} wall clock.`
        : "",
  };
}
