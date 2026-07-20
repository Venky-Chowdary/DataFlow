import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { fetchCdcLease, forceReleaseCdcLease } from "../../lib/api";
import { JobProgress } from "../../lib/types";
import { useToast } from "../Toast";
import { useConfirm } from "../ui/ConfirmDialog";

interface CdcLeaseConflictPanelProps {
  job: JobProgress;
  onResume?: () => void;
  resuming?: boolean;
  onOpenJob?: (jobId: string) => void;
}

function parseHolderJobId(holder: string | null | undefined): string | null {
  const parts = String(holder || "")
    .split(":")
    .filter(Boolean);
  if (parts.length < 3) return null;
  const jid = parts[parts.length - 2]?.trim();
  if (!jid || jid === "job") return null;
  return jid;
}

/**
 * Closed-loop Next step when a CDC job failed on lease conflict.
 * Force-release advances fencing generation; it does not kill the holder process.
 */
export function CdcLeaseConflictPanel({
  job,
  onResume,
  resuming,
  onOpenJob,
}: CdcLeaseConflictPanelProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const cursorKey = job.cdc_lease_cursor_key || "";
  const [busy, setBusy] = useState(false);
  const [liveGen, setLiveGen] = useState<number | null>(job.cdc_lease_generation ?? null);
  const [holderJobId, setHolderJobId] = useState<string | null>(
    () => parseHolderJobId(job.cdc_lease_holder),
  );
  const [released, setReleased] = useState(false);

  useEffect(() => {
    if (!cursorKey) return;
    let cancelled = false;
    void fetchCdcLease(cursorKey)
      .then((snap) => {
        if (cancelled) return;
        if (snap.holder_job_id) setHolderJobId(String(snap.holder_job_id));
        if (snap.lease?.generation != null) setLiveGen(Number(snap.lease.generation));
        if (!snap.found) setReleased(true);
      })
      .catch(() => {
        /* optional */
      });
    return () => {
      cancelled = true;
    };
  }, [cursorKey]);

  if (!job.cdc_lease_conflict) return null;

  const handleForceRelease = async () => {
    if (!cursorKey) {
      toast({
        title: "No lease cursor on this job",
        message: "Re-run after upgrading — older failures may lack cdc_lease_cursor_key.",
        tone: "error",
      });
      return;
    }
    const ok = await confirm({
      title: "Force-release CDC lease?",
      message:
        "This clears the lease so another consumer can acquire it. The prior holder is not stopped — it will fail on the next heartbeat renew. Prefer cancelling the holder job when it is still running.",
      confirmLabel: "Force-release",
      cancelLabel: "Keep lease",
      tone: "danger",
    });
    if (!ok) return;
    setBusy(true);
    try {
      const result = await forceReleaseCdcLease({
        cursor_key: cursorKey,
        expected_generation: liveGen,
        reason: `operator break from job ${job._id || (job as { id?: string }).id || "unknown"}`,
      });
      if (result.released || result.reason === "not_found") {
        setReleased(true);
        toast({
          title: result.released ? "Lease released" : "Lease already free",
          message: "You can Resume or re-run this CDC job now.",
          tone: "success",
        });
      } else if (result.reason === "generation_mismatch") {
        toast({
          title: "Fence moved",
          message: "Another steal already advanced the generation. Refresh and retry if still blocked.",
          tone: "error",
        });
      } else {
        toast({
          title: "Could not release lease",
          message: result.reason,
          tone: "error",
        });
      }
    } catch (e) {
      toast({
        title: "Force-release failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="df2-theater-v3-next df2-theater-lease-next" role="region" aria-label="CDC lease conflict next steps">
      <div className="df2-theater-v3-next-copy">
        <strong>Next step · CDC lease</strong>
        <span>
          {released
            ? "Lease is free. Resume or re-run — delivery remains at-least-once upsert."
            : `Held by ${job.cdc_lease_holder || "another worker"}${
                job.cdc_lease_resource ? ` · ${job.cdc_lease_resource}` : ""
              }${liveGen != null ? ` · gen ${liveGen}` : ""}. Force-release only after you accept fencing the prior holder.`}
        </span>
      </div>
      <div className="df2-theater-v3-next-actions">
        {!released && cursorKey && (
          <Button
            size="sm"
            variant="danger"
            loading={busy}
            loadingLabel="Releasing…"
            onClick={() => void handleForceRelease()}
            leadingIcon={<DtIcon name="alert" size={14} />}
          >
            Force-release lease
          </Button>
        )}
        {holderJobId && onOpenJob && (
          <Button
            size="sm"
            variant="ghost"
            onClick={() => onOpenJob(holderJobId)}
            leadingIcon={<DtIcon name="jobs" size={14} />}
          >
            Open holder job
          </Button>
        )}
        {released && onResume && (job.chunk_current != null || job.checkpoint) && (
          <Button
            size="sm"
            variant="primary"
            loading={resuming}
            loadingLabel="Resuming…"
            onClick={onResume}
            leadingIcon={<DtIcon name="play" size={14} />}
          >
            Resume from checkpoint
          </Button>
        )}
      </div>
    </div>
  );
}
