import { useEffect, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { clearCdcCursor, probeCdcRetention, type CdcRetentionProbe } from "../../lib/api";
import { useToast } from "../Toast";
import { useConfirm } from "../ui/ConfirmDialog";

export interface CdcRetentionPanelProps {
  /** When set, show proactive status from a job or Validate probe. */
  status?: string | null;
  resume?: string | null;
  retained?: string | null;
  message?: string | null;
  dialect?: string | null;
  cursorKey?: string | null;
  /** Optional live re-probe (Validate). */
  probeRequest?: {
    type: string;
    host?: string;
    port?: number;
    database?: string;
    username?: string;
    password?: string;
    schema?: string;
    connection_string?: string;
    table?: string;
    cursor_key?: string;
    multi_subnet_failover?: boolean;
  } | null;
  onResume?: () => void;
  resuming?: boolean;
}

/**
 * Proactive CDC retention health — at_risk / gap before or after poll failure.
 * Gap recovery: reset watermark + re-snapshot (at-least-once; not continuous CDC).
 */
export function CdcRetentionPanel({
  status: statusProp,
  resume: resumeProp,
  retained: retainedProp,
  message: messageProp,
  dialect: dialectProp,
  cursorKey: cursorKeyProp,
  probeRequest,
  onResume,
  resuming,
}: CdcRetentionPanelProps) {
  const { toast } = useToast();
  const { confirm } = useConfirm();
  const [busy, setBusy] = useState(false);
  const [cleared, setCleared] = useState(false);
  const [live, setLive] = useState<CdcRetentionProbe | null>(null);

  const status = (live?.cdc_retention_status || statusProp || "").toLowerCase();
  const resume = live?.cdc_retention_resume || resumeProp || "—";
  const retained = live?.cdc_retention_retained || retainedProp || "—";
  const message = live?.cdc_retention_message || messageProp || "";
  const dialect = live?.cdc_retention_dialect || dialectProp || "source";
  const cursorKey = live?.retention?.cursor_key || cursorKeyProp || probeRequest?.cursor_key || "";

  const notable = status === "gap" || status === "at_risk";

  useEffect(() => {
    setCleared(false);
    setLive(null);
  }, [statusProp, cursorKeyProp]);

  if (!notable && !probeRequest) return null;
  if (!notable && probeRequest && !live) {
    // Show a compact Check CTA only when Validate passes a probe request.
  }

  const handleProbe = async () => {
    if (!probeRequest) return;
    setBusy(true);
    try {
      const result = await probeCdcRetention(probeRequest);
      setLive(result);
      toast({
        title: `CDC retention · ${result.cdc_retention_status}`,
        message: result.cdc_retention_message || result.retention?.message,
        tone:
          result.cdc_retention_status === "gap"
            ? "error"
            : result.cdc_retention_status === "at_risk"
              ? "warning"
              : "success",
      });
    } catch (e) {
      toast({
        title: "Retention probe failed",
        message: e instanceof Error ? e.message : undefined,
        tone: "error",
      });
    } finally {
      setBusy(false);
    }
  };

  const handleClear = async () => {
    if (!cursorKey) {
      toast({
        title: "No CDC cursor key",
        message: "Re-run after upgrading, or clear the watermark from Ops if you know the key.",
        tone: "error",
      });
      return;
    }
    const ok = await confirm({
      title: "Reset CDC watermark?",
      message:
        "Clears the resume cursor so the next run re-snapshots. Destination rows are not rolled back — at-least-once upsert may re-apply. Continuous CDC across a retention gap is not claimed.",
      confirmLabel: "Reset watermark",
      cancelLabel: "Keep cursor",
      tone: "danger",
    });
    if (!ok) return;
    setBusy(true);
    try {
      await clearCdcCursor({
        cursor_key: cursorKey,
        reason: "operator retention gap recovery",
      });
      setCleared(true);
      toast({
        title: "Watermark reset",
        message: "Re-run with snapshot when_needed or initial.",
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

  const showPanel = notable || Boolean(live) || Boolean(probeRequest);

  if (!showPanel) return null;

  return (
    <div
      className={`df2-theater-v3-next df2-theater-retention-next${status === "gap" ? " is-danger" : status === "at_risk" ? " is-warn" : ""}`}
      role="region"
      aria-label="CDC retention health"
    >
      <div className="df2-theater-v3-next-copy">
        <strong>
          {status === "gap"
            ? "Next step · CDC retention gap"
            : status === "at_risk"
              ? "Watch · CDC retention at risk"
              : "CDC retention"}
        </strong>
        <span>
          {cleared
            ? "Watermark cleared. Re-run with snapshot when_needed or initial."
            : message
              || (status
                ? `${dialect} resume ${resume} · retained ${retained}`
                : "Probe the source watermark against live retention before Execute.")}
        </span>
      </div>
      <div className="df2-theater-v3-next-actions">
        {probeRequest && (
          <Button
            size="sm"
            variant="secondary"
            loading={busy}
            loadingLabel="Probing…"
            onClick={() => void handleProbe()}
            leadingIcon={<DtIcon name="activity" size={14} />}
          >
            Check retention
          </Button>
        )}
        {(status === "gap" || status === "at_risk") && !cleared && (
          <Button
            size="sm"
            variant={status === "gap" ? "danger" : "secondary"}
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
