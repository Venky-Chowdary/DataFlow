/**
 * General -> Job retention (QA enterprise #4): retention_days is applied by an
 * admin-run purge, dry run first. Nothing deletes jobs on a schedule.
 */
export type JobRetentionDryRun = { would_remove: number; cutoff: string; retention_days: number; confirm_token: string };

export const RETENTION_HINT =
  "Retention applies when a workspace admin runs the purge below (dry run first): finished jobs older than this many days are deleted from this workspace's job history. Nothing is deleted on a schedule.";

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
