/**
 * The verdicts of Trust Reports read in this session (review UI2 #2).
 * Every parity report response (open, explain, approve, dismiss, sign-off)
 * publishes its verdict here; the Jobs grid and drawer header read it, so a
 * change in the Trust Report shows on the row without a reload.
 */
import { useSyncExternalStore } from "react";
import { isParityVerdict, type ListVerdict } from "./verdict";

let reports: ReadonlyMap<string, { verdict: ListVerdict; at: number }> = new Map();
const listeners = new Set<() => void>();

export function publishReportVerdict(
  report: { job_id?: string; verdict?: unknown; parity?: { unexplained?: number | null } | null } | null | undefined,
  now: number = Date.now(),
): void {
  const jobId = String(report?.job_id || "");
  if (!jobId || !isParityVerdict(report?.verdict)) return;
  const next = new Map(reports);
  next.set(jobId, { verdict: { verdict: report!.verdict as ListVerdict["verdict"], unexplained: Number(report?.parity?.unexplained ?? 0) || 0 }, at: now });
  reports = next;
  for (const listener of listeners) listener();
}

export function useReportVerdicts(): ReadonlyMap<string, { verdict: ListVerdict; at: number }> {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => reports,
    () => reports,
  );
}

export function reportVerdictsSnapshot(): ReadonlyMap<string, { verdict: ListVerdict; at: number }> {
  return reports;
}
