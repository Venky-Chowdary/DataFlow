import { useEffect, useState } from "react";
import { fetchJobs, fetchUsageSummary } from "../../lib/api";
import type { MetricItem } from "../ui/PageMetricsRow";

interface WindowCounts {
  completed: number;
  failed: number;
}

/** The 7 UTC calendar days ending today, as the list's inclusive date filter reads them. */
export function sevenDayWindowStart(now = new Date()): string {
  const start = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate() - 6));
  return start.toISOString().slice(0, 10);
}

/** 48,213,004 → "48.2M". Never rounds a small count away. */
export function compactCount(value: number): string {
  if (!Number.isFinite(value)) return "—";
  if (Math.abs(value) < 10_000) return value.toLocaleString();
  return new Intl.NumberFormat(undefined, { notation: "compact", maximumFractionDigits: 1 }).format(value);
}

export function successRateMetric(window: WindowCounts | null | undefined): MetricItem {
  const finished = window ? window.completed + window.failed : 0;
  if (!window || finished === 0) {
    return {
      label: "Success rate · 7 days",
      value: "—",
      sub: window ? "No finished runs in 7 days" : "Not available",
      tone: "green",
    };
  }
  const rate = (window.completed / finished) * 100;
  return {
    label: "Success rate · 7 days",
    value: `${rate >= 99.95 || rate === 0 ? rate.toFixed(0) : rate.toFixed(1)}%`,
    sub: `${window.completed.toLocaleString()} succeeded · ${window.failed.toLocaleString()} failed`,
    tone: "green",
  };
}

export function needsAttentionMetric(attention: number | undefined, failed: number): MetricItem {
  if (attention == null) {
    return { label: "Need attention", value: "—", sub: "Not counted by this server", tone: "red" };
  }
  const verificationFailed = Math.max(0, attention - failed);
  return {
    label: "Need attention",
    value: attention.toLocaleString(),
    sub: attention === 0 ? "Nothing failed" : `${failed.toLocaleString()} failed · ${verificationFailed.toLocaleString()} failed verification`,
    tone: attention > 0 ? "red" : "default",
  };
}

interface UseJobsKpisArgs {
  running: number;
  failed: number;
  attention?: number;
  /** Bumped by Refresh; the window counts re-read with it, not on every poll. */
  refreshKey: number;
}

/**
 * KPI tiles for the Jobs page. Running and attention come from the list's
 * whole-history counts; the 7-day success rate is two counted list queries
 * (limit 1); rows written today is the usage meter. Anything the server could
 * not answer shows "—", never an estimate.
 */
export function useJobsKpis({ running, failed, attention, refreshKey }: UseJobsKpisArgs): MetricItem[] {
  const [window7, setWindow7] = useState<WindowCounts | null | undefined>(undefined);
  const [usage, setUsage] = useState<{ rows: number; runs: number } | null | undefined>(undefined);

  useEffect(() => {
    let cancelled = false;
    const dateFrom = sevenDayWindowStart();
    Promise.all([
      fetchJobs({ limit: 1, offset: 0, status: "completed", dateFrom }),
      fetchJobs({ limit: 1, offset: 0, status: "failed", dateFrom }),
    ])
      .then(([completed, failedRuns]) => {
        if (!cancelled) setWindow7({ completed: completed.filteredTotal, failed: failedRuns.filteredTotal });
      })
      .catch(() => {
        if (!cancelled) setWindow7(null);
      });
    fetchUsageSummary(1)
      .then((summary) => {
        if (cancelled) return;
        const rows = summary.totals?.rows_written ?? summary.rows_written;
        const runs = summary.totals?.event_count ?? summary.event_count;
        setUsage(typeof rows === "number" ? { rows, runs: Number(runs ?? 0) } : null);
      })
      .catch(() => {
        if (!cancelled) setUsage(null);
      });
    return () => {
      cancelled = true;
    };
  }, [refreshKey]);

  return [
    {
      label: "Running now",
      value: running.toLocaleString(),
      sub: running > 0 ? "Refreshes every 5 s" : "No live runs",
      tone: "blue",
    },
    window7 === undefined
      ? { label: "Success rate · 7 days", value: "—", sub: "Loading…", tone: "green" }
      : successRateMetric(window7),
    usage === undefined
      ? { label: "Rows written · today (UTC)", value: "—", sub: "Loading…", tone: "teal" }
      : usage === null
        ? { label: "Rows written · today (UTC)", value: "—", sub: "Usage meter not available", tone: "teal" }
        : {
          label: "Rows written · today (UTC)",
          value: compactCount(usage.rows),
          sub: `across ${usage.runs.toLocaleString()} metered run${usage.runs === 1 ? "" : "s"}`,
          tone: "teal",
        },
    needsAttentionMetric(attention, failed),
  ];
}
