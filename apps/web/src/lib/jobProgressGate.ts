import type { JobProgress } from "./types";
import { isJobTerminal } from "./uiUtils";

/** Why a read did not return a document. The screen must not keep the previous job. */
export type ProgressAbsence = "missing" | "unauthorized" | "forbidden";

/**
 * One observation from SSE or from a document read.
 * `seq` is assigned when the read starts, or when an SSE frame arrives.
 * A slower read that started earlier has a smaller seq and must not land last.
 */
export type ProgressDelivery = {
  seq: number;
  jobId: string;
  /** Workspace selected when this read was issued. */
  boundWorkspaceId: string;
  job: JobProgress | null;
  absence?: ProgressAbsence;
};

export type ShownProgress = {
  seq: number;
  jobId: string;
  status?: string;
};

export type ProgressDecision =
  | { action: "apply"; job: JobProgress }
  | { action: "drop" }
  | { action: "clear" };

/** All-workspaces may display a job that names its own tenant. A concrete workspace may not. */
function workspaceAllowsDocument(activeWorkspaceId: string, documentWorkspaceId: string): boolean {
  const active = activeWorkspaceId.trim();
  const doc = documentWorkspaceId.trim();
  if (!doc) return true;
  if (active === "__all__") return true;
  return Boolean(active) && doc === active;
}

/**
 * Whether this delivery may replace what the theater is showing.
 * Ordering is the read's seq, not a max of row counters: a reconcile message
 * can move while the writer-ack count stays put. A terminal status is not
 * walked back to running.
 */
export function decideProgressDelivery(input: {
  subscribedJobId: string;
  activeWorkspaceId: string;
  shown: ShownProgress | null;
  delivery: ProgressDelivery;
}): ProgressDecision {
  const { subscribedJobId, activeWorkspaceId, shown, delivery } = input;
  if (delivery.jobId !== subscribedJobId) return { action: "drop" };
  if (delivery.boundWorkspaceId !== activeWorkspaceId) return { action: "drop" };
  if (shown && shown.jobId === subscribedJobId && delivery.seq < shown.seq) return { action: "drop" };
  if (delivery.absence || !delivery.job) return { action: "clear" };
  const job = delivery.job;
  if (job._id !== subscribedJobId) return { action: "drop" };
  if (!workspaceAllowsDocument(activeWorkspaceId, job.workspace_id || "")) return { action: "clear" };
  if (
    shown
    && shown.jobId === subscribedJobId
    && isJobTerminal(shown.status)
    && !isJobTerminal(job.status)
  ) {
    return { action: "drop" };
  }
  return { action: "apply", job };
}
