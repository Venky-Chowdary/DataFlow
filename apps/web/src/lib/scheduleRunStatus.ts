/**
 * What a schedule shows about its runs — derived from the run records the API
 * returns, never from a separately maintained flag.
 *
 * The API derives `last_status` / `last_status_reason` from the latest run
 * record, and returns the run in flight (`current_run`) and the newest skipped
 * beat (`last_skip`). These helpers pick the badge and decide how often the
 * page re-reads, so a run that ends (or dies) is shown without a manual reload.
 */
import type { PipelineSchedule, ScheduleRun } from "./types";
import { jobStatusBadgeClass, jobStatusLabel } from "./uiUtils";

/** Re-read while a run is pending/running. */
export const ACTIVE_POLL_MS = 5_000;
/** Re-read at rest: a cadence beat, a skip or a sweep can change the status. */
export const IDLE_POLL_MS = 30_000;

const OPEN = new Set(["pending", "running"]);

export function isOpenRun(status: string | null | undefined): boolean {
  return OPEN.has(String(status || "").toLowerCase());
}

export interface ScheduleStatusBadge {
  status: string;
  label: string;
  className: string;
  /** Hover text: why the run ended as it did (or why the beat was skipped). */
  title: string;
}

/** The single status badge for a schedule card / drawer. */
export function scheduleStatusBadge(sched: PipelineSchedule): ScheduleStatusBadge | null {
  if (sched.needs_approval) {
    return {
      status: "needs_approval",
      label: jobStatusLabel("needs_approval"),
      className: jobStatusBadgeClass("needs_approval"),
      title: sched.approval_finding || "Waiting on an approval.",
    };
  }
  const current = sched.current_run;
  if (current && isOpenRun(current.status)) {
    const label = current.status === "pending" ? "Starting" : "Running";
    return {
      status: current.status,
      label,
      className: jobStatusBadgeClass("running"),
      title: current.job_id ? `${label}: job ${current.job_id}` : `${label}…`,
    };
  }
  if (sched.running) {
    return { status: "running", label: "Running", className: jobStatusBadgeClass("running"), title: "Running" };
  }
  if (!sched.last_status) return null;
  const reason = String(sched.last_status_reason || "").trim();
  return {
    status: sched.last_status,
    label: jobStatusLabel(sched.last_status),
    className: jobStatusBadgeClass(sched.last_status),
    title: reason ? `Last run: ${reason}` : `Last run: ${jobStatusLabel(sched.last_status)}`,
  };
}

/** The newest skipped beat worth showing, with its reason. */
export function skippedBeatNote(sched: PipelineSchedule): string {
  const skip = sched.last_skip;
  if (!skip || !skip.reason) return "";
  const times = Number(skip.occurrences || 1);
  return times > 1 ? `Skipped ${times}×: ${skip.reason}` : `Skipped: ${skip.reason}`;
}

/** Why a history row ended — the record's reason, falling back to its error. */
export function runReason(run: ScheduleRun): string {
  return String(run.reason || run.error || "").trim();
}

/** Status label for a history row (a skip names how often it repeated). */
export function runLabel(run: ScheduleRun): string {
  if (run.status === "skipped") {
    const times = Number(run.occurrences || 1);
    return times > 1 ? `Skipped ×${times}` : "Skipped";
  }
  return jobStatusLabel(run.status);
}

export function historyPollMs(runs: readonly ScheduleRun[] | null | undefined): number {
  return (runs || []).some((r) => isOpenRun(r.status)) ? ACTIVE_POLL_MS : IDLE_POLL_MS;
}

export function schedulesPollMs(rows: readonly PipelineSchedule[] | null | undefined): number {
  return (rows || []).some((s) => s.running || isOpenRun(s.current_run?.status))
    ? ACTIVE_POLL_MS
    : IDLE_POLL_MS;
}
