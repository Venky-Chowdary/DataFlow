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
import { Sparkline, StatTile, StatTileRow } from "../ui/StatTile";
import { parityAxis } from "../../lib/chartScale";
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

function runsWord(n: number): string {
  return `${n.toLocaleString("en-US")} ${n === 1 ? "run" : "runs"}`;
}

/** The section body for each state; the data fetch lives in MigrationHealthSection. */
export function MigrationHealthView({ state, summary, error, limit, onRetry, onOpenJob, onStartTransfer }: MigrationHealthViewProps) {
  const runs = summary?.runs ?? [];
  const latest = runs[runs.length - 1];
  const empty = state === "ready" && runs.length === 0;
  // The most-drifting table's row in the heatmap: how often, and its latest drifting run.
  const driftRow = summary?.kpis.most_drifting_table
    ? summary.heatmap.tables.indexOf(summary.kpis.most_drifting_table)
    : -1;
  const driftCells = driftRow >= 0 ? summary!.heatmap.cells[driftRow] ?? [] : [];
  const isDrift = (c: string) => c === "unexplained_small" || c === "unexplained_large";
  const driftRuns = driftCells.filter(isDrift).length;
  let driftJob = "";
  for (let i = driftCells.length - 1; i >= 0; i -= 1) {
    if (isDrift(driftCells[i])) {
      driftJob = summary!.heatmap.job_ids[i] ?? "";
      break;
    }
  }

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
            <StatTileRow label="Migration health over the last runs" className="df2-parity-health-kpis">
              <StatTile
                label="Runs fully proven"
                value={formatParityPct(summary.kpis.fully_proven_pct)}
                tone={(summary.kpis.fully_proven_pct ?? 0) >= 100 ? "ok" : "neutral"}
                spark={<Sparkline values={runs.map((r) => r.proven_equal_pct)} domain={parityAxis(runs.map((r) => r.proven_equal_pct))?.domain} />}
                caption={`latest ${formatParityPct(latest?.proven_equal_pct ?? null)}`}
              />
              <StatTile
                label="Unexplained differences"
                value={summary.kpis.runs_with_unexplained.toLocaleString("en-US")}
                tone={summary.kpis.runs_with_unexplained > 0 ? "warn" : "ok"}
                spark={<Sparkline kind="bars" values={runs.map((r) => r.unexplained)} />}
                caption={`${runsWord(summary.kpis.runs_with_unexplained)} of ${runs.length}`}
              />
              <StatTile
                label="Failed verification"
                value={summary.kpis.runs_failed_verification.toLocaleString("en-US")}
                tone={summary.kpis.runs_failed_verification > 0 ? "danger" : "ok"}
                caption={`${runsWord(summary.kpis.runs_failed_verification)} of ${runs.length}`}
              />
              <StatTile
                label="Sign-offs"
                value={summary.kpis.signoffs.toLocaleString("en-US")}
                caption={`across ${runs.length.toLocaleString("en-US")} ${runs.length === 1 ? "run" : "runs"}`}
              />
            </StatTileRow>
            {summary.kpis.most_drifting_table ? (
              <p className="df2-parity-drift-callout">
                <span className="df2-parity-drift-dot" aria-hidden="true" />
                <span>
                  Most-drifting table: <b className="df2-mono">{summary.kpis.most_drifting_table}</b>
                  {driftRuns > 0 ? ` · unexplained differences in ${runsWord(driftRuns)}` : ""}
                </span>
                {driftJob && onOpenJob ? (
                  <button type="button" className="df2-overview-v3-link" onClick={() => onOpenJob(driftJob)}>
                    Open its latest drifting run →
                  </button>
                ) : null}
              </p>
            ) : (
              <p className="df2-parity-drift-callout is-calm">
                <span className="df2-parity-drift-dot" aria-hidden="true" />
                <span>No table drifted in these runs.</span>
              </p>
            )}
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
                <h3 className="df2-parity-h3">Tables × runs <span className="df2-parity-muted">oldest → newest</span></h3>
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
