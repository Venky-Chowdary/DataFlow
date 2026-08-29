import { useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { fetchOpsDlq, fetchOpsFreshness, fetchOpenScheduleApprovals } from "../lib/api";
import { formatRelativeTime } from "../lib/connectionWorkbench";
import type { JobHistory } from "../lib/jobHistory";
import { jobHistoryFromResponse } from "../lib/jobHistory";
import {
  buildOverviewJobStats,
  buildStatusDistributionFromHistory,
  buildThroughputSeries,
} from "../lib/overviewAnalytics";
import { destProvenCount, formatJobRowMetric } from "../lib/conservationLedger";
import { isJobSuccess, jobStatusBadgeClass, jobStatusLabel } from "../lib/uiUtils";
import { DtIcon } from "../components/DtIcon";
import { DataPlaneFlow } from "../components/overview/DataPlaneFlow";
import {
  StatusDonut,
  ThroughputChart,
  ThroughputChartPlaceholder,
} from "../components/overview/OverviewCharts";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { ProgressCell } from "../components/ui/ProgressCell";
import { CopyIdChip } from "../components/ui/CopyIdChip";
import {
  FreshnessSloPanel,
  type FreshnessAlert,
} from "../components/overview/FreshnessSloPanel";
import { dismissBanner, isBannerDismissed } from "../lib/dismissibleBanner";
import { buildDataPlaneTopology, countSavedConnectionRoutes } from "../lib/topologyUtils";
import {
  connectorPassedProbe,
  connectorTestHealth,
} from "../lib/connectorHealth";

interface DashboardPageProps {
  connectors: Connector[];
  jobs: TransferJob[];
  /** Whole-history counts. Overview must not count the recent page of rows. */
  history?: JobHistory;
  /** True while the first scoped workspace lists are still in flight. */
  listsLoading?: boolean;
  schedules?: PipelineSchedule[];
  onOpenConnectors?: () => void;
  onOpenJobs?: () => void;
  onOpenJob?: (jobId: string) => void;
  onOpenPipeline?: (scheduleId: string) => void;
  onOpenSchedules?: () => void;
}

const JOB_LIMIT = 10;

function HealthRing({ score }: { score: number }) {
  const r = 38;
  const c = 2 * Math.PI * r;
  const pct = Math.max(0, Math.min(100, score));
  return (
    <div className="df2-overview-v3-score" aria-label={`Workspace health ${score}%`}>
      <svg viewBox="0 0 88 88" aria-hidden>
        <circle className="df2-overview-v3-score-track" cx="44" cy="44" r={r} />
        <circle
          className="df2-overview-v3-score-fill"
          cx="44"
          cy="44"
          r={r}
          strokeDasharray={`${(pct / 100) * c} ${c}`}
          transform="rotate(-90 44 44)"
        />
      </svg>
      <div className="df2-overview-v3-score-label">
        <strong>{score}</strong>
        <span>health</span>
      </div>
    </div>
  );
}

export function DashboardPage({
  connectors,
  jobs,
  history,
  listsLoading = false,
  schedules = [],
  onOpenConnectors,
  onOpenJobs,
  onOpenJob,
  onOpenPipeline,
  onOpenSchedules,
}: DashboardPageProps) {
  const [opsLagSeconds, setOpsLagSeconds] = useState<number | null>(null);
  const [dlqCount, setDlqCount] = useState<number | null>(null);
  /** Inbox SSOT. null = unknown (failed fetch) — never invent 0 parked. */
  const [parkedCount, setParkedCount] = useState<number | null>(null);
  const [freshness, setFreshness] = useState<{
    slo_status?: string;
    warn_threshold_seconds?: number;
    critical_threshold_seconds?: number;
    stale_count?: number;
    critical_count?: number;
    worst_lag_seconds?: number | null;
    alerts?: FreshnessAlert[];
  } | null>(null);

  useEffect(() => {
    fetchOpsFreshness(60)
      .then((f) => {
        setOpsLagSeconds(f.worst_lag_seconds);
        setFreshness({
          slo_status: f.slo_status,
          warn_threshold_seconds: f.warn_threshold_seconds,
          critical_threshold_seconds: f.critical_threshold_seconds,
          stale_count: f.stale_count,
          critical_count: f.critical_count,
          worst_lag_seconds: f.worst_lag_seconds,
          alerts: f.alerts,
        });
      })
      .catch(() => {
        setOpsLagSeconds(null);
        setFreshness(null);
      });
    fetchOpsDlq(50)
      .then((d) => setDlqCount(d.count))
      .catch(() => setDlqCount(null));
    fetchOpenScheduleApprovals()
      .then((rows) => setParkedCount(rows.length))
      .catch(() => setParkedCount(null));
  }, []);

  const jobHistory = useMemo(
    () => history ?? jobHistoryFromResponse({ jobs }),
    [history, jobs],
  );
  const stats = useMemo(() => buildOverviewJobStats(jobHistory), [jobHistory]);
  const completed = jobs.filter((j) => isJobSuccess(j.status));
  const running = jobs.filter((j) => j.status === "running" || j.status === "pending");
  const destMeasured = completed
    .map((j) => destProvenCount(j))
    .filter((n): n is number => n != null);
  const totalRecords = destMeasured.reduce((sum, n) => sum + n, 0);
  const successRate = stats.successRate;
  const failedCount = stats.failed;
  const runningCount = stats.running;
  const healthyConnectors = connectors.filter((c) => connectorPassedProbe(c)).length;
  const untestedConnectors = connectors.filter((c) => connectorTestHealth(c) === "never_tested").length;
  const failedConnectors = connectors.filter((c) => connectorTestHealth(c) === "failed").length;
  const enabledPipelines = schedules.filter((s) => s.enabled).length;
  const cdcLagSeconds = useMemo(() => {
    const lags = [...running, ...completed]
      .map((j) => j.cdc_lag_seconds)
      .filter((v): v is number => typeof v === "number" && Number.isFinite(v));
    const fromJobs = lags.length ? Math.max(...lags) : null;
    if (opsLagSeconds != null && fromJobs != null) return Math.max(opsLagSeconds, fromJobs);
    return opsLagSeconds ?? fromJobs;
  }, [running, completed, opsLagSeconds]);

  const throughputSeries = useMemo(() => buildThroughputSeries(jobs), [jobs]);
  const statusSlices = useMemo(
    () => buildStatusDistributionFromHistory(jobHistory),
    [jobHistory],
  );

  const topology = useMemo(
    () => buildDataPlaneTopology(connectors, jobs, schedules),
    [connectors, jobs, schedules],
  );
  const routeCount = countSavedConnectionRoutes(topology);

  const healthScore = useMemo(() => {
    if (connectors.length === 0 && stats.total === 0) return null;
    let score = 100;
    if (connectors.length) {
      score -= (failedConnectors / connectors.length) * 35;
      score -= (untestedConnectors / connectors.length) * 12;
    }
    if (stats.total) {
      score -= ((stats.total - stats.completed) / stats.total) * 25;
    }
    if (failedCount) score -= Math.min(15, failedCount * 4);
    if (runningCount) score = Math.min(score + 2, 100);
    return Math.round(Math.max(0, Math.min(100, score)));
  }, [connectors.length, failedConnectors, untestedConnectors, stats.total, stats.completed, failedCount, runningCount]);

  const hasThroughput = throughputSeries.some((d) => d.rows > 0);
  const hasJobs = stats.total > 0;
  const pausedPipelines = schedules.filter((s) => !s.enabled).length;
  const scheduleNames = useMemo(() => {
    const map: Record<string, string> = {};
    for (const s of schedules) map[s.id] = s.name;
    return map;
  }, [schedules]);
  const freshnessStale = (freshness?.stale_count ?? 0) > 0
    || freshness?.slo_status === "warn"
    || freshness?.slo_status === "critical";
  const attentionItems = [
    failedCount > 0 ? `${failedCount} failed job${failedCount === 1 ? "" : "s"}` : null,
    dlqCount != null && dlqCount > 0 ? `${dlqCount} DLQ event${dlqCount === 1 ? "" : "s"}` : null,
    freshnessStale
      ? `CDC lag ${freshness?.slo_status === "critical" ? "critical" : "above SLO"}${
          freshness?.stale_count ? ` · ${freshness.stale_count} pipeline${freshness.stale_count === 1 ? "" : "s"}` : ""
        }`
      : cdcLagSeconds != null && cdcLagSeconds > 60
        ? `CDC lag ${cdcLagSeconds.toFixed(0)}s`
        : null,
    pausedPipelines > 0 ? `${pausedPipelines} paused schedule${pausedPipelines === 1 ? "" : "s"}` : null,
    parkedCount != null && parkedCount > 0
      ? `${parkedCount} schedule${parkedCount === 1 ? "" : "s"} parked on a decision`
      : null,
  ].filter(Boolean) as string[];
  const attentionSignature = attentionItems.join(" · ");
  const [attentionDismissed, setAttentionDismissed] = useState(false);
  useEffect(() => {
    setAttentionDismissed(
      attentionSignature ? isBannerDismissed("overview.attention", attentionSignature) : false,
    );
  }, [attentionSignature]);

  return (
    <PageShell
      wide
      className="df2-page-overview-v3"
      title="Overview"
      kicker="Workspace"
      description="Live health, throughput, and recent migrations for this workspace."
    >
      <PageFrame className="df2-overview-v3">
        {attentionItems.length > 0 && !attentionDismissed && (
          <div className="df2-overview-attention" role="status">
            <DtIcon name="alert" size={16} />
            <div>
              <strong>Needs attention</strong>
              <p>{attentionSignature}</p>
            </div>
            {failedCount > 0 && onOpenJobs && (
              <button type="button" className="df2-overview-attention-action" onClick={onOpenJobs}>
                Open jobs
              </button>
            )}
            {failedCount === 0 && dlqCount != null && dlqCount > 0 && onOpenJobs && (
              <button type="button" className="df2-overview-attention-action" onClick={onOpenJobs}>
                Open quarantine
              </button>
            )}
            {freshnessStale && !failedCount && onOpenPipeline && freshness?.alerts?.[0]?.schedule_id && (
              <button
                type="button"
                className="df2-overview-attention-action"
                onClick={() => onOpenPipeline(freshness.alerts![0].schedule_id!)}
              >
                Open pipeline
              </button>
            )}
            {parkedCount != null && parkedCount > 0 && failedCount === 0 && onOpenSchedules && (
              <button type="button" className="df2-overview-attention-action" onClick={onOpenSchedules}>
                Open Pipelines inbox
              </button>
            )}
            <button
              type="button"
              className="df2-banner-dismiss"
              aria-label="Dismiss needs attention"
              title="Dismiss until these counts change"
              onClick={() => {
                dismissBanner("overview.attention", attentionSignature);
                setAttentionDismissed(true);
              }}
            >
              <DtIcon name="x" size={14} />
            </button>
          </div>
        )}
        <FreshnessSloPanel
          sloStatus={freshness?.slo_status}
          warnSeconds={freshness?.warn_threshold_seconds}
          criticalSeconds={freshness?.critical_threshold_seconds}
          worstLagSeconds={freshness?.worst_lag_seconds ?? opsLagSeconds}
          staleCount={freshness?.stale_count}
          criticalCount={freshness?.critical_count}
          alerts={freshness?.alerts}
          scheduleNames={scheduleNames}
          onOpenPipeline={onOpenPipeline}
          onOpenJob={onOpenJob}
        />
        <section className="df2-overview-v3-analytics" aria-label="Analytics">
          <article className="df2-overview-v3-card df2-overview-v3-card--chart">
            <header className="df2-overview-v3-card-head">
              <div>
                <h2 className="df2-overview-v3-card-title">Throughput</h2>
                <p className="df2-overview-v3-card-sub">Conserved dest rows per day · append dest Δ, not dest after · unmeasured omitted</p>
              </div>
              <span className="df2-overview-v3-card-badge">{totalRecords.toLocaleString()} conserved</span>
            </header>
            <div className="df2-overview-v3-card-body df2-overview-v3-chart-body">
              {hasThroughput ? (
                <ThroughputChart series={throughputSeries} />
              ) : (
                <ThroughputChartPlaceholder series={throughputSeries} />
              )}
            </div>
          </article>

          <article className="df2-overview-v3-card df2-overview-v3-card--chart">
            <header className="df2-overview-v3-card-head">
              <div>
                <h2 className="df2-overview-v3-card-title">Migration mix</h2>
                <p className="df2-overview-v3-card-sub">
                  {listsLoading
                    ? "Loading workspace history…"
                    : stats.isWindow
                      ? `Whole history · table shows the ${stats.windowLoaded.toLocaleString()} most recent`
                      : "Job status breakdown"}
                </p>
              </div>
              <span className="df2-overview-v3-card-badge">
                {listsLoading ? "…" : `${stats.total.toLocaleString()} jobs`}
              </span>
            </header>
            <div className="df2-overview-v3-card-body df2-overview-v3-chart-body">
              {hasJobs ? (
                <StatusDonut slices={statusSlices} centerLabel="success" centerValue={`${successRate ?? 0}%`} />
              ) : listsLoading ? (
                <div className="df2-overview-v3-donut-empty">
                  <p>Loading workspace history…</p>
                </div>
              ) : (
                <div className="df2-overview-v3-donut-empty" aria-hidden>
                  <svg viewBox="0 0 120 120" className="df2-overview-v3-donut-empty-svg">
                    <circle cx="60" cy="60" r="44" className="df2-overview-v3-donut-empty-track" />
                    <circle cx="60" cy="60" r="28" className="df2-overview-v3-donut-empty-hole" />
                  </svg>
                  <p>No jobs yet — distribution appears after your first transfer.</p>
                </div>
              )}
            </div>
          </article>
        </section>

        <div className="df2-overview-v3-workspace">
          <div className="df2-overview-v3-main">
            <article className="df2-overview-v3-card df2-overview-v3-card--plane">
              <header className="df2-overview-v3-card-head">
                <div>
                  <h2 className="df2-overview-v3-card-title">Data plane</h2>
                  <p className="df2-overview-v3-card-sub">
                    {routeCount
                      ? `${routeCount} active route${routeCount > 1 ? "s" : ""}`
                      : connectors.length
                        ? `${connectors.length} connection${connectors.length === 1 ? "" : "s"} · no routes yet`
                        : "Connect sources and destinations to map your topology"}
                  </p>
                </div>
                {onOpenConnectors && (
                  <button type="button" className="df2-overview-v3-link" onClick={onOpenConnectors}>
                    Connectors →
                  </button>
                )}
              </header>
              <div className="df2-overview-v3-card-body df2-overview-v3-plane-body">
                <DataPlaneFlow
                  nodes={topology.nodes}
                  edges={topology.edges}
                  connectionCount={connectors.length}
                  onOpenConnectors={onOpenConnectors}
                />
              </div>
            </article>

            <article className="df2-overview-v3-card">
              <header className="df2-overview-v3-card-head">
                <div>
                  <h2 className="df2-overview-v3-card-title">Recent migrations</h2>
                  <p className="df2-overview-v3-card-sub">Latest governed transfers</p>
                </div>
                {onOpenJobs && jobs.length > 0 && (
                  <button type="button" className="df2-overview-v3-link" onClick={onOpenJobs}>
                    Job Theater →
                  </button>
                )}
              </header>
              {jobs.length === 0 ? (
                <div className="df2-overview-v3-table-empty">
                  <DtIcon name="transfer" size={22} />
                  <p>No migrations yet. Use <strong>Transfer Studio</strong> from the sidebar when you are ready.</p>
                </div>
              ) : (
                <div className="df2-overview-v3-card-body df2-overview-v3-card-body--flush">
                  <div className="df2-table-wrap df2-overview-table-wrap">
                    <table className="df2-table df2-overview-table" aria-label="Recent migrations">
                      <thead>
                        <tr>
                          <th>Route</th>
                          <th>Job ID</th>
                          <th>Status</th>
                          <th className="df2-col-progress">Progress</th>
                          <th>Rows</th>
                          <th>Quarantine</th>
                        </tr>
                      </thead>
                      <tbody>
                        {jobs.slice(0, JOB_LIMIT).map((job) => (
                          <tr key={job._id} className={job.status === "failed" ? "df2-row-error" : job.status === "completed_with_quarantine" ? "df2-row-warn" : ""}>
                            <td>
                              <div className="df2-cell-title" title={job.source_name}>{job.source_name}</div>
                              <div className="df2-cell-meta" title={`${job.source_type} → ${job.destination_type}`}>
                                {job.source_type} → {job.destination_type}
                              </div>
                            </td>
                            <td><CopyIdChip id={job._id} label="Job" compact /></td>
                            <td><span className={jobStatusBadgeClass(job.status)}>{jobStatusLabel(job.status)}</span></td>
                            <td className="df2-col-progress"><JobProgressCell job={job} /></td>
                            <td className="df2-overview-rows" title={formatJobRowMetric(job).title}>
                              {formatJobRowMetric(job).value}
                            </td>
                            <td className="df2-overview-rows">
                              {(job.rejected_rows ?? 0) > 0 ? (
                                <span className="df2-badge df2-badge-warn" title="Open Jobs → Inspect quarantine for row-level findings">
                                  {(job.rejected_rows ?? 0).toLocaleString()}
                                </span>
                              ) : (
                                "—"
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              )}
            </article>
          </div>

          <aside className="df2-overview-v3-rail">
            <article className="df2-overview-v3-card df2-overview-v3-health">
              <div className="df2-overview-v3-health-inner">
                {healthScore != null ? (
                  <HealthRing score={healthScore} />
                ) : (
                  <div className="df2-overview-v3-score df2-overview-v3-score--idle" aria-hidden>
                    <DtIcon name="dashboard" size={28} />
                  </div>
                )}
                <div>
                  <h2 className="df2-overview-v3-card-title">Workspace</h2>
                  <p className="df2-overview-v3-card-sub">
                    {listsLoading
                      ? "Loading workspace counts…"
                      : healthScore != null
                      ? failedCount > 0
                        ? `${failedCount} failed job${failedCount === 1 ? "" : "s"} affecting score`
                        : runningCount > 0
                          ? `${runningCount} job${runningCount === 1 ? "" : "s"} in progress`
                          : "All systems nominal"
                      : "Metrics populate once you connect and transfer"}
                  </p>
                </div>
              </div>
            </article>

            <article className="df2-overview-v3-card">
              <header className="df2-overview-v3-card-head">
                <div>
                  <h2 className="df2-overview-v3-card-title">Connections</h2>
                  <p className="df2-overview-v3-card-sub">
                    {connectors.length
                      ? [
                          `${healthyConnectors} healthy`,
                          untestedConnectors ? `${untestedConnectors} never tested` : "",
                          failedConnectors ? `${failedConnectors} need attention` : "",
                        ].filter(Boolean).join(" · ")
                      : "No connections yet"}
                  </p>
                </div>
                {onOpenConnectors && (
                  <button type="button" className="df2-overview-v3-link" onClick={onOpenConnectors}>
                    Manage →
                  </button>
                )}
              </header>
              <div className="df2-overview-v3-card-body">
                {connectors.length === 0 ? (
                  <p className="df2-overview-v3-inline-empty">No saved connections.</p>
                ) : (
                  <ul className="df2-overview-conn-list">
                    {connectors.slice(0, 8).map((c) => (
                      <li key={c.id}>
                        <span className={`df2-health-dot ${connectorTestHealth(c) === "failed" ? "err" : connectorTestHealth(c) === "passed" ? "ok" : "warn"}`} />
                        <ConnectorIcon id={c.type} size={18} />
                        <span className="df2-overview-conn-body">
                          <span className="df2-overview-conn-name" title={c.name}>{c.name}</span>
                          <span className="df2-overview-conn-meta" title={`${c.type}${c.database ? ` · ${c.database}` : c.host ? ` · ${c.host}` : ""}`}>
                            {c.type}{c.database ? ` · ${c.database}` : c.host ? ` · ${c.host}` : ""}
                          </span>
                        </span>
                      </li>
                    ))}
                    {connectors.length > 8 && (
                      <li className="df2-overview-conn-more">+{connectors.length - 8} more connections</li>
                    )}
                  </ul>
                )}
              </div>
            </article>

            <article className="df2-overview-v3-card">
              <header className="df2-overview-v3-card-head">
                <h2 className="df2-overview-v3-card-title">Schedules</h2>
              </header>
              <div className="df2-overview-v3-card-body">
                {schedules.length === 0 ? (
                  <p className="df2-overview-v3-inline-empty">No schedules yet.</p>
                ) : (
                  <>
                    <ul className="df2-overview-pipeline-list">
                      {schedules.slice(0, 6).map((s) => (
                        <li key={s.id}>
                          <strong title={s.name}>{s.name}</strong>
                          <span className="df2-cell-meta">
                            {s.interval}{!s.enabled && " · paused"}
                            {s.last_run_at && ` · ${formatRelativeTime(s.last_run_at)}`}
                          </span>
                        </li>
                      ))}
                    </ul>
                    <p className="df2-overview-v3-rail-meta">
                      {/* Name the whole set the Schedules page lists, not just
                          the enabled slice — the two screens counted different
                          things under the same word. */}
                      {schedules.length} total · {enabledPipelines} enabled
                      {pausedPipelines > 0 ? ` · ${pausedPipelines} paused` : ""}
                    </p>
                  </>
                )}
              </div>
            </article>
          </aside>
        </div>
      </PageFrame>
    </PageShell>
  );
}

function JobProgressCell({ job }: { job: TransferJob }) {
  if (job.status === "completed") {
    return <ProgressCell value={100} done />;
  }
  if (job.status === "completed_with_quarantine") {
    return (
      <span className="df2-cell-meta df2-progress-warn" title="Completed, but rows were rejected or values coerced to NULL">
        <DtIcon name="alert" size={12} /> Landed, not full fidelity
      </span>
    );
  }
  if ((job.status === "running" || job.status === "pending") && job.progress_pct != null) {
    return <ProgressCell value={job.progress_pct} />;
  }
  if (job.status === "failed" && job.error) {
    return (
      <span className="df2-cell-meta df2-text-error df2-progress-error" title={job.error}>
        {job.error.length > 40 ? `${job.error.slice(0, 40)}…` : job.error}
      </span>
    );
  }
  return <span className="df2-cell-meta">—</span>;
}
