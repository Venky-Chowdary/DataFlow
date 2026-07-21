import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { clearCdcCursor, fetchCdcCursor } from "../../lib/api";
import { JobProgress } from "../../lib/types";
import { useToast } from "../Toast";
import { useConfirm } from "../ui/ConfirmDialog";

interface CdcCursorGapPanelProps {
  job: JobProgress;
  onResume?: () => void;
  resuming?: boolean;
}

/**
 * Closed-loop Next step when CDC failed because resume LSN/SCN is before
 * retained redo (AG failover, archive purge, CDC cleanup).
 */
export function CdcCursorGapPanel({ job, onResume, resuming }: CdcCursorGapPanelProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const cursorKey = job.cdc_lease_cursor_key || "";
  const [busy, setBusy] = useState(false);
  const [cleared, setCleared] = useState(false);
  const [liveWatermark, setLiveWatermark] = useState<string | null>(job.watermark ?? null);

  const isGap =
    Boolean(job.cdc_cursor_gap)
    || job.error_code === "cdc_cursor_gap"
    || job.error_code === "cdc_lsn_gap"
    || job.error_code === "cdc_scn_gap"
    || /before capture retention|before available redo|min_lsn|oldest_available|ora-01291/i.test(
      String(job.error || ""),
    );

  useEffect(() => {
    if (!cursorKey || !isGap) return;
    let cancelled = false;
    void fetchCdcCursor(cursorKey)
      .then((snap) => {
        if (cancelled) return;
        setLiveWatermark(snap.watermark);
        if (!snap.found) setCleared(true);
      })
      .catch(() => {
        /* optional */
      });
    return () => {
      cancelled = true;
    };
  }, [cursorKey, isGap]);

  if (!isGap) return null;

  const dialect = job.cdc_cursor_gap_dialect || "source";
  const resume = job.cdc_cursor_gap_resume || "—";
  const retained = job.cdc_cursor_gap_retained || "—";

  const handleClear = async () => {
    if (!cursorKey) {
      toast({
        title: "No CDC cursor key on this job",
        message: "Re-run after upgrading, or clear the watermark from Ops if you know the key.",
        tone: "error",
      });
      return;
    }
    const ok = await confirm({
      title: "Reset CDC watermark?",
      message:
        "Clears the resume cursor so the next run re-snapshots (when_needed / initial). Destination rows are not rolled back — at-least-once upsert may re-apply. Continuous CDC across the gap is not claimed.",
      confirmLabel: "Reset watermark",
      cancelLabel: "Keep cursor",
      tone: "danger",
    });
    if (!ok) return;
    setBusy(true);
    try {
      const result = await clearCdcCursor({
        cursor_key: cursorKey,
        reason: `operator gap recovery from job ${job._id || "unknown"}`,
      });
      setCleared(true);
      setLiveWatermark(null);
      toast({
        title: result.reason === "not_found" ? "Watermark already clear" : "Watermark reset",
        message: "Re-run with snapshot mode when_needed or initial. Delivery remains at-least-once.",
        tone: "success",
      });
    } catch (e) {
      toast({
        title: "Could not clear watermark",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="df2-theater-v3-next df2-theater-cursor-gap-next" role="region" aria-label="CDC cursor gap next steps">
      <div className="df2-theater-v3-next-copy">
        <strong>Next step · CDC cursor gap</strong>
        <span>
          {cleared
            ? "Watermark cleared. Re-run with snapshot when_needed or initial — do not claim continuous CDC across the gap."
            : `${dialect} resume ${resume} is before retained ${retained}${
                liveWatermark ? ` · live cursor ${liveWatermark}` : ""
              }. Reset the watermark, then re-snapshot.`}
        </span>
      </div>
      <div className="df2-theater-v3-next-actions">
        {!cleared && (
          <Button
            size="sm"
            variant="danger"
            loading={busy}
            loadingLabel="Resetting…"
            onClick={() => void handleClear()}
            leadingIcon={<DtIcon name="alert" size={14} />}
          >
            Reset watermark
          </Button>
        )}
        {cleared && onResume && (
          <Button
            size="sm"
            variant="primary"
            loading={resuming}
            loadingLabel="Resuming…"
            onClick={onResume}
            leadingIcon={<DtIcon name="play" size={14} />}
          >
            Resume / re-run
          </Button>
        )}
      </div>
    </div>
  );
}
