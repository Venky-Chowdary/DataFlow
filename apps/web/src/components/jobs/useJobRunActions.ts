import { useCallback, useState } from "react";
import { cancelJob } from "../../lib/api";
import { downloadBlob } from "../../lib/downloadBlob";
import { jobsCsv, jobsCsvFilename, LIVE_JOB_STATUSES, shortJobId } from "../../lib/jobGrid";
import { PERMISSIONS, useWriteGate } from "../../lib/PermissionsContext";
import type { TransferJob } from "../../lib/types";
import { isJobSuccess, presentedJobStatus } from "../../lib/uiUtils";
import { useToast } from "../Toast";
import { useConfirm } from "../ui/ConfirmDialog";
import type { JobsMenuItem } from "./JobsMenu";
import type { JobsToolbarAction } from "./JobsToolbar";

interface UseJobRunActionsArgs {
  /** The selected row (the grid's cursor). */
  selected: TransferJob | undefined;
  /** Rows on screen, in display order — what Export CSV writes. */
  rows: TransferJob[];
  displayName: (job: TransferJob) => string;
  /** The page's existing "Retry from start" for a failed run. */
  retry: (jobId: string) => Promise<void> | void;
  /** The page's existing "Sync now" for the selected successful run. */
  syncNow: () => Promise<void> | void;
  /** True while the page's retry / sync is in flight. */
  busy: boolean;
  open: (jobId: string) => void;
  onRefresh?: () => void;
}

export interface JobRunActions {
  rerun: JobsToolbarAction;
  cancel: JobsToolbarAction;
  exportCsv: JobsToolbarAction;
  rowMenuItems: (job: TransferJob) => JobsMenuItem[];
}

/**
 * Re-run, Cancel and Export CSV for the Jobs command bar and row menu.
 *
 * Re-run reuses the page's own actions: a failed or cancelled run is retried
 * from the start, a successful run is synced again on its signed contract
 * (with that action's confirm). The API gates both on connector.write, so the
 * controls are disabled with the refusal sentence for a role without it.
 */
export function useJobRunActions({
  selected,
  rows,
  displayName,
  retry,
  syncNow,
  busy,
  open,
  onRefresh,
}: UseJobRunActionsArgs): JobRunActions {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const writeGate = useWriteGate(PERMISSIONS.connectorWrite, "re-run or cancel jobs");
  const [cancelling, setCancelling] = useState<string | null>(null);

  const cancelRun = useCallback(async (job: TransferJob) => {
    const accepted = await confirm({
      title: "Cancel this job?",
      message: "The job stops at the next checkpoint. Rows already committed stay in the destination.",
      confirmLabel: "Cancel job",
      cancelLabel: "Keep running",
      tone: "danger",
      facts: [
        { label: "Job", value: displayName(job) },
        { label: "Job ID", value: shortJobId(job._id) },
      ],
    });
    if (!accepted) return;
    setCancelling(job._id);
    try {
      await cancelJob(job._id);
      toast({ title: "Cancellation requested", message: "The job will stop at the next checkpoint.", tone: "info" });
      onRefresh?.();
    } catch (e) {
      toast({ title: "Could not cancel job", message: (e as Error).message, tone: "error" });
    } finally {
      setCancelling(null);
    }
  }, [confirm, displayName, onRefresh, toast]);

  const status = selected ? presentedJobStatus(selected) : "";
  const live = LIVE_JOB_STATUSES.has(status);
  const retryable = status === "failed" || status === "cancelled";
  const syncable = isJobSuccess(status);

  const rerun: JobsToolbarAction = {
    visible: Boolean(selected) && (retryable || syncable),
    disabled: !selected || busy || !writeGate.allowed || !(retryable || syncable),
    busy: busy && Boolean(selected),
    title: !selected
      ? "Select a job to re-run it"
      : !writeGate.allowed
        ? writeGate.reason
        : retryable
          ? "Retry from start: starts a new job and re-reads the source from the beginning."
          : syncable
            ? "Sync now: reuses the signed contract. One confirm; a second click does not start another run."
            : "A running job cannot be re-run until it finishes.",
    onClick: () => {
      if (!selected) return;
      if (retryable) void retry(selected._id);
      else if (syncable) void syncNow();
    },
  };

  const cancel: JobsToolbarAction = {
    visible: Boolean(selected) && live,
    disabled: !selected || !live || !writeGate.allowed || cancelling === selected._id,
    busy: Boolean(selected && cancelling === selected._id),
    title: !selected
      ? "Select a running job to cancel it"
      : !writeGate.allowed
        ? writeGate.reason
        : live
          ? "Stops the selected job at its next checkpoint."
          : "Only a running or queued job can be cancelled.",
    onClick: () => {
      if (selected && live) void cancelRun(selected);
    },
  };

  const exportCsv: JobsToolbarAction = {
    disabled: rows.length === 0,
    title: rows.length
      ? `Download the ${rows.length.toLocaleString()} job${rows.length === 1 ? "" : "s"} on this page as CSV`
      : "No jobs on this page to export",
    onClick: () => {
      if (!rows.length) return;
      downloadBlob(jobsCsvFilename(), new Blob([jobsCsv(rows, { name: displayName })], { type: "text/csv;charset=utf-8" }));
    },
  };

  const rowMenuItems = (job: TransferJob): JobsMenuItem[] => {
    const rowStatus = presentedJobStatus(job);
    const items: JobsMenuItem[] = [{ id: "open", label: "Open details", onSelect: () => open(job._id) }];
    if (rowStatus === "failed" || rowStatus === "cancelled") {
      items.push({
        id: "retry",
        label: "Retry from start",
        disabled: busy || !writeGate.allowed,
        title: writeGate.reason || "Starts a new job and re-reads the source from the beginning.",
        onSelect: () => void retry(job._id),
      });
    }
    if (LIVE_JOB_STATUSES.has(rowStatus)) {
      items.push({
        id: "cancel",
        label: "Cancel job",
        disabled: !writeGate.allowed || cancelling === job._id,
        title: writeGate.reason || "Stops the job at its next checkpoint.",
        onSelect: () => void cancelRun(job),
      });
    }
    items.push({
      id: "copy",
      label: "Copy job ID",
      onSelect: () => {
        void navigator.clipboard?.writeText(job._id).then(
          () => toast({ title: "Job ID copied", message: job._id, tone: "success" }),
          () => toast({ title: "Could not copy", message: job._id, tone: "warning" }),
        );
      },
    });
    return items;
  };

  return { rerun, cancel, exportCsv, rowMenuItems };
}
