import { DtIcon } from "../DtIcon";
import { Button } from "../ui/Button";

export type FreshnessAlert = {
  severity: string;
  code: string;
  title: string;
  detail: string;
  schedule_id?: string | null;
  job_id?: string | null;
  stream?: string | null;
  lag_seconds?: number;
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

/**
 * Closed-loop freshness SLO surface for Overview — lag/heartbeat alerts with
 * Open pipeline / Open job CTAs (same pattern as quarantine / lease next steps).
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
  if (!sloStatus || sloStatus === "unknown") return null;
  if (sloStatus === "ok" && alerts.length === 0) {
    return (
      <div className="df2-freshness-slo is-ok" role="status" aria-label="CDC freshness SLO">
        <DtIcon name="check" size={16} />
        <div>
          <strong>Freshness SLO met</strong>
          <p>
            {worstLagSeconds != null
              ? `Worst CDC lag ${worstLagSeconds.toFixed(1)}s (warn ${warnSeconds}s).`
              : `No pipelines above the ${warnSeconds}s warn threshold.`}
          </p>
        </div>
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
          {sloStatus === "critical" ? "Freshness SLO critical" : "Freshness SLO warn"}
          {criticalCount > 0 ? ` · ${criticalCount} critical` : ""}
          {staleCount > 0 ? ` · ${staleCount} stale` : ""}
        </strong>
        <p>
          Warn {warnSeconds}s
          {criticalSeconds != null ? ` · critical ${criticalSeconds.toFixed(0)}s` : ""}
          {worstLagSeconds != null ? ` · worst ${worstLagSeconds.toFixed(1)}s` : ""}.
          Open the pipeline or job, then check lease / quarantine if the consumer stalled.
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
    </div>
  );
}
