/**
 * Job history counts — one owner for "how many jobs do I have?".
 *
 * The Jobs header used to count the rows it had loaded, so a 90-job history read
 * "All (50) · Failed (10)" while Pilot (which counts in the store) said 90 and 29.
 * The server now returns whole-history counts next to the page of rows; every
 * operator-visible total reads them from here.
 */

import type { TransferJob } from "./types";
import { isJobSuccess } from "./uiUtils";

export type JobStatusCounts = Record<string, number>;

export interface JobHistory {
  /** Most recent page of jobs — what the table can show. */
  jobs: TransferJob[];
  /** Jobs in the whole scoped history, not just the page above. */
  total: number;
  /** Per-status counts over that same whole history. */
  statusCounts: JobStatusCounts;
}

export interface JobFilterCounts {
  all: number;
  running: number;
  completed: number;
  quarantine: number;
  failed: number;
}

export const EMPTY_JOB_HISTORY: JobHistory = { jobs: [], total: 0, statusCounts: {} };

const RUNNING_STATUSES = new Set(["running", "pending"]);

/** History from a jobs response, falling back to the rows when a server never counted. */
export function jobHistoryFromResponse(data: {
  jobs?: TransferJob[];
  total?: number;
  status_counts?: JobStatusCounts;
}): JobHistory {
  const jobs = data.jobs || [];
  const counted = Number(data.total || 0);
  const statusCounts = data.status_counts && Object.keys(data.status_counts).length
    ? data.status_counts
    : statusCountsFromJobs(jobs);
  return { jobs, total: counted || jobs.length, statusCounts };
}

/** Per-status counts of the rows in hand — only a fallback for an uncounted response. */
export function statusCountsFromJobs(jobs: TransferJob[]): JobStatusCounts {
  const counts: JobStatusCounts = {};
  for (const job of jobs) {
    const key = job.status || "unknown";
    counts[key] = (counts[key] || 0) + 1;
  }
  return counts;
}

/** Filter-chip counts over the whole history, using the same buckets as the filters. */
export function jobFilterCounts(history: JobHistory): JobFilterCounts {
  const counts: JobFilterCounts = { all: history.total, running: 0, completed: 0, quarantine: 0, failed: 0 };
  for (const [status, n] of Object.entries(history.statusCounts)) {
    const count = Number(n) || 0;
    if (RUNNING_STATUSES.has(status)) counts.running += count;
    if (isJobSuccess(status)) counts.completed += count;
    if (status === "completed_with_quarantine") counts.quarantine += count;
    if (status === "failed") counts.failed += count;
  }
  return counts;
}
