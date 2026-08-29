import type { TransferJob } from "./types";
import { destProvenCount } from "./conservationLedger";
import type { JobHistory } from "./jobHistory";
import { jobFilterCounts } from "./jobHistory";

export interface DayThroughput {
  label: string;
  rows: number;
  jobs: number;
}

export interface JobStatusSlice {
  key: string;
  label: string;
  count: number;
  color: string;
}

export function buildThroughputSeries(jobs: TransferJob[], days = 7): DayThroughput[] {
  const now = new Date();
  const buckets: DayThroughput[] = [];

  for (let i = days - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    d.setHours(0, 0, 0, 0);
    const next = new Date(d);
    next.setDate(next.getDate() + 1);

    const dayJobs = jobs.filter((j) => {
      const t = new Date(j.created_at).getTime();
      return t >= d.getTime() && t < next.getTime();
    });
    const completed = dayJobs.filter((j) => j.status === "completed" || j.status === "completed_with_quarantine");
    const rows = completed.reduce((s, j) => {
      const dest = destProvenCount(j);
      return dest == null ? s : s + dest;
    }, 0);

    buckets.push({
      label: d.toLocaleDateString(undefined, { weekday: "short" }),
      rows,
      jobs: dayJobs.length,
    });
  }
  return buckets;
}

export function buildStatusDistribution(jobs: TransferJob[]): JobStatusSlice[] {
  const completed = jobs.filter((j) => j.status === "completed").length;
  const quarantine = jobs.filter((j) => j.status === "completed_with_quarantine").length;
  const failed = jobs.filter((j) => j.status === "failed").length;
  const running = jobs.filter((j) => j.status === "running" || j.status === "pending").length;
  const other = Math.max(0, jobs.length - completed - quarantine - failed - running);
  return statusSlices(completed, quarantine, running, failed, other);
}

/**
 * Mix over the whole scoped history — not the most-recent page of rows.
 * Counting the page made Overview read "50 jobs" for a 90-job workspace.
 */
export function buildStatusDistributionFromHistory(history: JobHistory): JobStatusSlice[] {
  const sc = history.statusCounts || {};
  const completed = Number(sc.completed || 0);
  const quarantine = Number(sc.completed_with_quarantine || 0);
  const failed = Number(sc.failed || 0);
  const running = Number(sc.running || 0) + Number(sc.pending || 0);
  const named = completed + quarantine + failed + running;
  const other = Math.max(0, history.total - named);
  return statusSlices(completed, quarantine, running, failed, other);
}

function statusSlices(
  completed: number,
  quarantine: number,
  running: number,
  failed: number,
  other: number,
): JobStatusSlice[] {
  return [
    { key: "completed", label: "Completed", count: completed, color: "#10b981" },
    { key: "quarantine", label: "Completed with quarantine", count: quarantine, color: "#d97706" },
    { key: "running", label: "Running", count: running, color: "#0ea5e9" },
    { key: "failed", label: "Failed", count: failed, color: "#ef4444" },
    { key: "other", label: "Other", count: other, color: "#94a3b8" },
  ].filter((s) => s.count > 0);
}

/** Operator-visible Overview totals. Same owner as Jobs filter chips. */
export function buildOverviewJobStats(history: JobHistory): {
  total: number;
  completed: number;
  quarantine: number;
  failed: number;
  running: number;
  successRate: number | null;
  windowLoaded: number;
  isWindow: boolean;
} {
  const counts = jobFilterCounts(history);
  return {
    total: counts.all,
    completed: counts.completed,
    quarantine: counts.quarantine,
    failed: counts.failed,
    running: counts.running,
    successRate: counts.all ? Math.round((counts.completed / counts.all) * 100) : null,
    windowLoaded: history.jobs.length,
    isWindow: counts.all > history.jobs.length,
  };
}

export function sparklineFromThroughput(series: DayThroughput[]): number[] {
  const vals = series.map((s) => s.rows);
  const max = Math.max(...vals, 1);
  return vals.map((v) => v / max);
}
