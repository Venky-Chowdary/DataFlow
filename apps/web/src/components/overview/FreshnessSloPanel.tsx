import { useEffect, useMemo, useState } from "react";
import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";
import { dismissBanner, isBannerDismissed } from "../../lib/dismissibleBanner";

export type FreshnessAlert = {
  severity: string;
  code: string;
  title: string;
  detail: string;
  schedule_id?: string | null;
  job_id?: string | null;
  stream?: string | null;
  lag_seconds?: number | null;
  lag_bytes?: number | null;
};

interface FreshnessSloPanelProps {
  sloStatus?: string | null;
  warnSeconds?: number;
  criticalSeconds?: number;
  worstLagSeconds?: number | null;
  staleCount?: number;
  criticalCount?: number;
  alerts?: FreshnessAlert[];
  scheduleNames?: Record<string, string>;
  onOpenPipeline?: (scheduleId: string) => void;
  onOpenJob?: (jobId: string) => void;
}

function formatLagBytes(n: number): string {
  if (n >= 1_048_576) return `${(n / 1_048_576).toFixed(1)} MB`;
  if (n >= 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${n} B`;
}

/**
 * Closed-loop freshness SLO surface for Overview — lag/heartbeat alerts with
 * Open schedule / Open job CTAs (same pattern as quarantine / lease next steps).
 *
 * Heartbeat alone never claims catch-up; unknown SLO stays visible as honesty
 * until the operator dismisses. Hidden when there is no CDC activity (n_a).
 */
export function FreshnessSloPanel({
  sloStatus,
  warnSeconds = 60,
  criticalSeconds,
  worstLagSeconds,
  staleCount = 0,
  criticalCount = 0,
  alerts = [],
  scheduleNames = {},
  onOpenPipeline,
  onOpenJob,
}: FreshnessSloPanelProps) {
  const signature = useMemo(
    () =>
      [
        sloStatus || "",
        String(staleCount),
        String(criticalCount),
        String(worstLagSeconds ?? ""),
        alerts.map((a) => `${a.severity}:${a.schedule_id || ""}:${a.job_id || ""}`).join("|"),
      ].join("::"),
    [sloStatus, staleCount, criticalCount, worstLagSeconds, alerts],
  );
  const [dismissed, setDismissed] = useState(false);
  useEffect(() => {
    setDismissed(isBannerDismissed("overview.freshness", signature));
  }, [signature]);

  if (!sloStatus || sloStatus === "n_a") return null;
  if (dismissed) return null;

  const onDismiss = () => {
    dismissBanner("overview.freshness", signature);
    setDismissed(true);
  };

  if (sloStatus === "unknown") {
    return (
      <div className="df2-freshness-slo is-warn" role="status" aria-label="CDC freshness not measured">
        <DtIcon name="alert" size={16} />
        <div className="df2-freshness-slo-body">
          <strong>CDC lag not measured yet</strong>
          <p>
            Freshness SLO tracks how far behind a live CDC pipeline is (seconds from
            the last source commit, or WAL/binlog bytes still to catch up). Right now
            we only see that a consumer heartbeated — that proves the worker is alive,
            not that data is caught up. Open a CDC job in Theater and check Slot /
            Binlog / Capture chips. Batch transfers (Excel → MySQL, etc.) do not use
            this ribbon.
          </p>
        </div>
        <button
          type="button"
          className="df2-banner-dismiss"
          onClick={onDismiss}
          aria-label="Dismiss CDC lag notice"
          title="Dismiss until this status changes"
        >
          <DtIcon name="x" size={14} />
        </button>
      </div>
    );
  }
  if (sloStatus === "ok" && alerts.length === 0) {
    return (
      <div className="df2-freshness-slo is-ok" role="status" aria-label="CDC freshness SLO">
        <DtIcon name="check" size={16} />
        <div className="df2-freshness-slo-body">
          <strong>CDC freshness OK</strong>
          <p>
            {worstLagSeconds != null
              ? `Worst proven CDC lag ${worstLagSeconds.toFixed(1)}s (warn at ${warnSeconds}s).`
              : `Pipelines within warn (${warnSeconds}s) / WAL catch-up band.`}
          </p>
        </div>
        <button
          type="button"
          className="df2-banner-dismiss"
          onClick={onDismiss}
          aria-label="Dismiss freshness OK notice"
          title="Dismiss until this status changes"
        >
          <DtIcon name="x" size={14} />
        </button>
      </div>
    );
  }

  const top = alerts.slice(0, 4);
  return (
    <div
      className={`df2-freshness-slo ${sloStatus === "critical" ? "is-critical" : "is-warn"}`}
      role="status"
      aria-label="CDC freshness SLO alerts"
    >
      <DtIcon name="alert" size={16} />
      <div className="df2-freshness-slo-body">
        <strong>
          {sloStatus === "critical" ? "CDC lag critical" : "CDC lag above SLO"}
          {criticalCount > 0 ? ` · ${criticalCount} critical` : ""}
          {staleCount > 0 ? ` · ${staleCount} stale` : ""}
        </strong>
        <p>
          Warn {warnSeconds}s
          {criticalSeconds != null ? ` · critical ${criticalSeconds.toFixed(0)}s` : ""}
          {worstLagSeconds != null ? ` · worst ${worstLagSeconds.toFixed(1)}s` : ""}.
          Open the schedule or job, then check lease / quarantine if the consumer stalled.
        </p>
        {top.length > 0 && (
          <ul className="df2-freshness-slo-list">
            {top.map((a, i) => {
              const name =
                (a.schedule_id && scheduleNames[a.schedule_id])
                || a.stream
                || a.schedule_id
                || a.job_id
                || "Pipeline";
              return (
                <li key={`${a.schedule_id || ""}-${a.job_id || ""}-${i}`}>
                  <span>
                    <em className={a.severity === "critical" ? "df2-text-warn" : undefined}>
                      {a.title}
                    </em>
                    {" · "}
                    {name}
                    {a.lag_seconds != null ? ` · ${a.lag_seconds.toFixed(1)}s` : ""}
                    {a.lag_bytes != null ? ` · ${formatLagBytes(Number(a.lag_bytes))}` : ""}
                  </span>
                  <span className="df2-freshness-slo-actions">
                    {a.schedule_id && onOpenPipeline && (
                      <Button size="sm" variant="ghost" onClick={() => onOpenPipeline(a.schedule_id!)}>
                        Open pipeline
                      </Button>
                    )}
                    {a.job_id && onOpenJob && (
                      <Button size="sm" variant="ghost" onClick={() => onOpenJob(a.job_id!)}>
                        Open job
                      </Button>
                    )}
                  </span>
                </li>
              );
            })}
          </ul>
        )}
      </div>
      <button
        type="button"
        className="df2-banner-dismiss"
        onClick={onDismiss}
        aria-label="Dismiss freshness alert"
        title="Dismiss until this status changes"
      >
        <DtIcon name="x" size={14} />
      </button>
    </div>
  );
}
