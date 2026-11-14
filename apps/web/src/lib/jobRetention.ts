/**
 * General -> Job retention (QA enterprise #4, Security 23): retention_days is
 * applied by an admin-run purge, dry run first — and, once a platform admin
 * turns on "Purge automatically" (retention_auto_purge, off by default), by
 * the same purge every day for every workspace.
 */
export type JobRetentionDryRun = { would_remove: number; cutoff: string; retention_days: number; confirm_token: string };

export const RETENTION_HINT =
  "Retention applies when a workspace admin runs the purge below (dry run first): finished jobs older than this many days are deleted from this workspace's job history. Nothing is deleted on a schedule.";

export const RETENTION_SCHEDULED_HINT =
  "Purge automatically is on: once a day, finished jobs older than this many days are deleted from every workspace's job history (audited per workspace). Running and queued jobs, audit records and quarantine are never touched. A workspace admin can still run the purge below.";

/**
 * The "Purge automatically" field hint (UX-24). It used to say "deleted
 * daily" whatever the switch said, beside "Nothing is deleted on a schedule":
 * the two read as a contradiction. It now describes the switch's state.
 */
export function autoPurgeHint(autoPurge: boolean): string {
  return autoPurge
    ? "On: once a day, finished jobs older than the retention window are deleted in every workspace."
    : "Off: nothing is deleted on a schedule. Old jobs go only when an admin runs Purge old jobs below.";
}

export function retentionHint(autoPurge: boolean): string {
  return autoPurge ? RETENTION_SCHEDULED_HINT : RETENTION_HINT;
}

export function purgeConfirmCopy(dry: JobRetentionDryRun): { title: string; message: string; nothing: boolean } {
  const day = dry.cutoff.slice(0, 10);
  if (dry.would_remove === 0) {
    return { title: "Nothing to purge", message: `No finished jobs older than ${dry.retention_days} days (before ${day}).`, nothing: true };
  }
  const noun = dry.would_remove === 1 ? "job" : "jobs";
  return {
    title: `Delete ${dry.would_remove} finished ${noun}?`,
    message: `Finished jobs older than ${dry.retention_days} days (before ${day}) in this workspace will be deleted. Running and queued jobs are kept. This cannot be undone.`,
    nothing: false,
  };
}
