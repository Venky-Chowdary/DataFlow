import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { clearCdcCursor, fetchCdcCursor } from "../../lib/api";
import { snapshotModeRecoversGap } from "../../lib/jobTrustScore";
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
 *
 * when_needed / always / initial_only: Resume is primary — the engine blocking-
 * snapshots current source keys, then streams from the new tip.
 * initial: Reset watermark (or set when_needed).
 * never: change mode; Reset would leave never without a cursor.
 */
export function CdcCursorGapPanel({ job, onResume, resuming }: CdcCursorGapPanelProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const cursorKey = job.cdc_lease_cursor_key || "";
  const [busy, setBusy] = useState(false);
  const [cleared, setCleared] = useState(false);
  const [liveWatermark, setLiveWatermark] = useState<string | null>(job.watermark ?? null);

  const snapshotMode = String(job.snapshot_mode || job.snapshot_plan?.snapshot_mode || "")
    .trim()
    .toLowerCase()
    .replace(/-/g, "_");
  const engineSnapshots = snapshotModeRecoversGap(snapshotMode);
  const neverMode = snapshotMode === "never";

  const isGap =
    Boolean(job.cdc_cursor_gap)
    || job.error_code === "cdc_cursor_gap"
    || job.error_code === "cdc_lsn_gap"
    || job.error_code === "cdc_scn_gap"
    || job.error_code === "cdc_binlog_gap"
    || job.error_code === "cdc_slot_gap"
    || /before capture retention|before available redo|min_lsn|oldest_available|ora-01291|wal_status=lost|replication slot/i.test(
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
        "Clears the resume cursor so the next run re-snapshots under initial. Destination rows are not rolled back — at-least-once upsert may re-apply. Purged-window events are gone. Not continuous CDC.",
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
        message: "Re-run with snapshot mode initial or when_needed. Delivery remains at-least-once.",
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

  const copy = cleared
    ? "Watermark cleared. Re-run with snapshot when_needed or initial — do not claim continuous CDC across the gap."
    : engineSnapshots
      ? `${dialect} resume ${resume} is before retained ${retained}${
          liveWatermark ? ` · live cursor ${liveWatermark}` : ""
        }. Resume re-upserts current source keys, then streams from the new tip. Purged-window events are gone — not continuous CDC, not migration_proven.`
      : neverMode
        ? `${dialect} resume ${resume} is before retained ${retained}. snapshot_mode=never forbids a recovery snapshot. Set when_needed, then Resume. Purged-window events are gone.`
        : `${dialect} resume ${resume} is before retained ${retained}${
            liveWatermark ? ` · live cursor ${liveWatermark}` : ""
          }. snapshot_mode=initial will not snapshot again. Reset the watermark or set when_needed.`;

  const showResume = Boolean(onResume) && (cleared || engineSnapshots) && !neverMode;
  const showReset = !cleared && !neverMode;

  return (
    <div className="df2-theater-v3-next df2-theater-cursor-gap-next" role="region" aria-label="CDC cursor gap next steps">
      <div className="df2-theater-v3-next-copy">
        <strong>Next step · CDC cursor gap</strong>
        <span>{copy}</span>
      </div>
      <div className="df2-theater-v3-next-actions">
        {showResume && (
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
        {showReset && (
          <Button
            size="sm"
            variant={engineSnapshots ? "secondary" : "danger"}
            loading={busy}
            loadingLabel="Resetting…"
            onClick={() => void handleClear()}
            leadingIcon={<DtIcon name="alert" size={14} />}
          >
            Reset watermark
          </Button>
        )}
      </div>
    </div>
  );
}
