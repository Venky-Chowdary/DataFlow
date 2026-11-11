/**
 * Page B: Dashboard → Migration health.
 *
 * Parity over recent runs and the table that keeps drifting, from
 * GET /parity/summary. Workspace-scoped, so it re-reads when the workspace
 * changes.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { Button } from "../ui/Button";
import { DtIcon } from "../DtIcon";
import { ParityTrendChart, TableRunHeatmap } from "../charts/ParityCharts";
import { fetchParitySummary } from "../../lib/api";
import { formatParityPct, parityErrorMessage, type ParitySummary } from "../../lib/parity";
import { WORKSPACE_CHANGED_EVENT } from "../../lib/workspace";

export interface MigrationHealthViewProps {
  state: "loading" | "error" | "ready";
  summary: ParitySummary | null;
  error?: string;
  limit: number;
  onRetry?: () => void;
  onOpenJob?: (jobId: string) => void;
  onStartTransfer?: () => void;
}

/** The section body for each state; the data fetch lives in MigrationHealthSection. */
export function MigrationHealthView({ state, summary, error, limit, onRetry, onOpenJob, onStartTransfer }: MigrationHealthViewProps) {
  const runs = summary?.runs ?? [];
  const latest = runs[runs.length - 1];
  const empty = state === "ready" && runs.length === 0;

  return (
    <section className="df2-overview-v3-card df2-parity-health" aria-labelledby="parity-health-title">
      <header className="df2-overview-v3-card-head">
        <div>
          <h2 id="parity-health-title" className="df2-overview-v3-card-title">Migration health</h2>
          <p className="df2-overview-v3-card-sub">
            Does each destination match its source, and which table keeps drifting?
          </p>
        </div>
        <span className="df2-overview-v3-card-badge">Last {limit} runs</span>
      </header>
      <div className="df2-overview-v3-card-body">
        {state === "loading" && (
          <p className="df2-parity-muted" role="status">Loading migration health…</p>
        )}

        {state === "error" && (
          <div className="df2-parity-notice is-danger" role="alert">
            <DtIcon name="alert" size={14} />
            <span>{error || "Could not load migration health."}</span>
            {onRetry && (
              <Button size="sm" variant="ghost" onClick={onRetry}>
                Try again
              </Button>
            )}
          </div>
        )}

        {empty && (
          <div className="df2-overview-v3-table-empty df2-parity-health-empty">
            <p>
              <b>Run a transfer to see parity here.</b> Each completed run is checked against its source, and the
              results build up into a trend.
            </p>
            {onStartTransfer && (
              <Button size="sm" variant="primary" onClick={onStartTransfer}>
                Start a transfer
              </Button>
            )}
          </div>
        )}

        {state === "ready" && summary && runs.length > 0 && (
          <>
            <dl className="df2-parity-kpis">
              <div className="df2-parity-kpi">
                <dt>runs fully proven</dt>
                <dd>{formatParityPct(summary.kpis.fully_proven_pct)}</dd>
              </div>
              <div className="df2-parity-kpi">
                <dt>runs with unexplained differences</dt>
                <dd>{summary.kpis.runs_with_unexplained.toLocaleString("en-US")}</dd>
              </div>
              <div className="df2-parity-kpi">
                <dt>runs that failed verification</dt>
                <dd>{summary.kpis.runs_failed_verification.toLocaleString("en-US")}</dd>
              </div>
              <div className="df2-parity-kpi">
                <dt>sign-offs</dt>
                <dd>{summary.kpis.signoffs.toLocaleString("en-US")}</dd>
              </div>
              <div className="df2-parity-kpi">
                <dt>most-drifting table</dt>
                <dd className={summary.kpis.most_drifting_table ? "df2-mono" : undefined}>
                  {summary.kpis.most_drifting_table ?? "None"}
                </dd>
              </div>
            </dl>
            <div className="df2-parity-grid2">
              <div className="df2-parity-card">
                <h3 className="df2-parity-h3">Parity trend</h3>
                <ParityTrendChart runs={runs} />
                {latest && onOpenJob && (
                  <button type="button" className="df2-overview-v3-link" onClick={() => onOpenJob(latest.job_id)}>
                    Open the latest run's trust report →
                  </button>
                )}
              </div>
              <div className="df2-parity-card">
                <h3 className="df2-parity-h3">Tables × runs</h3>
                {summary.heatmap.tables.length ? (
                  <TableRunHeatmap heatmap={summary.heatmap} runs={runs} />
                ) : (
                  <p className="df2-parity-muted">No per-table checks in these runs yet.</p>
                )}
              </div>
            </div>
          </>
        )}
      </div>
    </section>
  );
}

export interface MigrationHealthSectionProps {
  limit?: number;
  onOpenJob?: (jobId: string) => void;
  onStartTransfer?: () => void;
}

export function MigrationHealthSection({ limit = 30, onOpenJob, onStartTransfer }: MigrationHealthSectionProps) {
  const [summary, setSummary] = useState<ParitySummary | null>(null);
  const [state, setState] = useState<"loading" | "error" | "ready">("loading");
  const [error, setError] = useState("");
  const seq = useRef(0);

  const load = useCallback(() => {
    const id = ++seq.current;
    setState((s) => (s === "ready" ? s : "loading"));
    fetchParitySummary(limit)
      .then((next) => {
        if (seq.current !== id) return;
        setSummary(next);
        setError("");
        setState("ready");
      })
      .catch((err: unknown) => {
        if (seq.current !== id) return;
        setError(parityErrorMessage(err, "Could not load migration health."));
        setState("error");
      });
  }, [limit]);

  useEffect(() => {
    load();
    const onWorkspace = () => {
      setSummary(null);
      setState("loading");
      load();
    };
    window.addEventListener(WORKSPACE_CHANGED_EVENT, onWorkspace);
    return () => {
      seq.current += 1;
      window.removeEventListener(WORKSPACE_CHANGED_EVENT, onWorkspace);
    };
  }, [load]);

  return (
    <MigrationHealthView
      state={state}
      summary={summary}
      error={error}
      limit={limit}
      onRetry={load}
      onOpenJob={onOpenJob}
      onStartTransfer={onStartTransfer}
    />
  );
}
