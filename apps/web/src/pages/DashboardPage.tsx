import { useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { fetchCatalogStats } from "../lib/api";
import { formatRelativeTime } from "../lib/connectionWorkbench";
import {
  buildStatusDistribution,
  buildThroughputSeries,
  sparklineFromThroughput,
} from "../lib/overviewAnalytics";
import { jobStatusBadgeClass } from "../lib/uiUtils";
import { DtIcon } from "../components/DtIcon";
import { DataPlaneFlow } from "../components/overview/DataPlaneFlow";
import {
  MetricGlassTile,
  StatusDonut,
  ThroughputChart,
  ThroughputChartPlaceholder,
} from "../components/overview/OverviewCharts";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { ProgressCell } from "../components/ui/ProgressCell";
import { buildDataPlaneTopology } from "../lib/topologyUtils";

interface DashboardPageProps {
  connectors: Connector[];
  jobs: TransferJob[];
  schedules?: PipelineSchedule[];
  onNewTransfer: () => void;
  onOpenPilot?: () => void;
  onOpenConnectors?: () => void;
  onOpenJobs?: () => void;
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
  schedules = [],
  onOpenConnectors,
  onOpenJobs,
}: DashboardPageProps) {
  const [catalogStats, setCatalogStats] = useState<{ live: number; total: number; transfer_live?: number } | null>(null);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setCatalogStats({ live: s.live, total: s.total, transfer_live: s.transfer_live }))
      .catch(() => setCatalogStats(null));
  }, []);

  const completed = jobs.filter((j) => j.status === "completed");
  const failed = jobs.filter((j) => j.status === "failed");
  const running = jobs.filter((j) => j.status === "running" || j.status === "pending");
  const totalRecords = completed.reduce((sum, j) => sum + (j.records_processed || 0), 0);
  const successRate = jobs.length ? Math.round((completed.length / jobs.length) * 100) : null;
  const healthyConnectors = connectors.filter((c) => c.status !== "error" && c.last_test_ok !== false).length;
  const enabledPipelines = schedules.filter((s) => s.enabled).length;

  const throughputSeries = useMemo(() => buildThroughputSeries(jobs), [jobs]);
  const statusSlices = useMemo(() => buildStatusDistribution(jobs), [jobs]);
  const throughputSpark = useMemo(() => sparklineFromThroughput(throughputSeries), [throughputSeries]);

  const topology = useMemo(
    () => buildDataPlaneTopology(connectors, jobs, schedules),
    [connectors, jobs, schedules],
  );
  const routeCount = topology.edges.length;

  const healthScore = useMemo(() => {
    if (connectors.length === 0 && jobs.length === 0) return null;
    let score = 100;
    if (connectors.length) {
      score -= ((connectors.length - healthyConnectors) / connectors.length) * 35;
    }
    if (jobs.length) {
      score -= ((jobs.length - completed.length) / jobs.length) * 25;
    }
    if (failed.length) score -= Math.min(15, failed.length * 4);
    if (running.length) score = Math.min(score + 2, 100);
    return Math.round(Math.max(0, Math.min(100, score)));
  }, [connectors.length, healthyConnectors, jobs.length, completed.length, failed.length, running.length]);

  const hasThroughput = throughputSeries.some((d) => d.rows > 0);
  const hasJobs = jobs.length > 0;

  return (
    <PageShell wide className="df2-page-overview-v3" title="Overview">
      <PageFrame className="df2-overview-v3">
        {(failed.length > 0 || running.length > 0) && (
          <section className="df2-overview-v3-ops" aria-label="Live operations">
            {running.length > 0 && (
              <button
                type="button"
                className="df2-overview-v3-ops-chip is-live"
                onClick={() => onOpenJobs?.()}
                disabled={!onOpenJobs}
              >
                <DtIcon name="activity" size={14} />
                <strong>{running.length}</strong>
                <span>transfer{running.length === 1 ? "" : "s"} running</span>
              </button>
            )}
            {failed.length > 0 && (
              <button
                type="button"
                className="df2-overview-v3-ops-chip is-fail"
                onClick={() => onOpenJobs?.()}
                disabled={!onOpenJobs}
              >
                <DtIcon name="alert" size={14} />
                <strong>{failed.length}</strong>
                <span>need review in Job Theater</span>
              </button>
            )}
          </section>
        )}

        <section className="df2-overview-v3-kpis" aria-label="Key metrics">
          <MetricGlassTile
            label="Rows moved"
            value={totalRecords.toLocaleString()}
            sub="Completed transfers"
            icon="trend"
            tone="teal"
            sparkline={throughputSpark}
          />
          <MetricGlassTile
            label="Success rate"
            value={successRate != null ? `${successRate}%` : "—"}
            sub={jobs.length ? `${completed.length} of ${jobs.length} jobs` : "No jobs yet"}
            icon="check"
            tone="green"
          />
          <MetricGlassTile
            label="Connections"
            value={connectors.length}
            sub={`${healthyConnectors} healthy`}
            icon="connectors"
            tone={healthyConnectors < connectors.length ? "amber" : "default"}
          />
          <MetricGlassTile
            label="Catalog live"
            value={catalogStats?.transfer_live ?? catalogStats?.live ?? "—"}
            sub={
              catalogStats?.total
                ? `${catalogStats.total} drivers · ${enabledPipelines} pipelines`
                : enabledPipelines
                  ? `${enabledPipelines} pipelines enabled`
                  : "Loading…"
            }
            icon="activity"
            tone="teal"
          />
        </section>

        <section className="df2-overview-v3-analytics" aria-label="Analytics">
          <article className="df2-overview-v3-card df2-overview-v3-card--chart">
            <header className="df2-overview-v3-card-head">
              <div>
                <h2 className="df2-overview-v3-card-title">Throughput</h2>
                <p className="df2-overview-v3-card-sub">Rows moved per day · last 7 days</p>
              </div>
              <span className="df2-overview-v3-card-badge">{totalRecords.toLocaleString()} total rows</span>
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
                <p className="df2-overview-v3-card-sub">Job status breakdown</p>
              </div>
              <span className="df2-overview-v3-card-badge">{jobs.length} jobs</span>
            </header>
            <div className="df2-overview-v3-card-body df2-overview-v3-chart-body">
              {hasJobs ? (
                <StatusDonut slices={statusSlices} centerLabel="success" centerValue={`${successRate ?? 0}%`} />
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
                          <th>Status</th>
                          <th className="df2-col-progress">Progress</th>
                          <th>Rows</th>
                        </tr>
                      </thead>
                      <tbody>
                        {jobs.slice(0, JOB_LIMIT).map((job) => (
                          <tr key={job._id} className={job.status === "failed" ? "df2-row-error" : ""}>
                            <td>
                              <div className="df2-cell-title" title={job.source_name}>{job.source_name}</div>
                              <div className="df2-cell-meta" title={`${job.source_type} → ${job.destination_type}`}>
                                {job.source_type} → {job.destination_type}
                              </div>
                            </td>
                            <td><span className={jobStatusBadgeClass(job.status)}>{job.status}</span></td>
                            <td className="df2-col-progress"><JobProgressCell job={job} /></td>
                            <td className="df2-overview-rows">{job.records_processed?.toLocaleString() ?? "—"}</td>
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
                    {healthScore != null
                      ? failed.length > 0
                        ? `${failed.length} failed job${failed.length === 1 ? "" : "s"} affecting score`
                        : running.length > 0
                          ? `${running.length} job${running.length === 1 ? "" : "s"} in progress`
                          : "All systems nominal"
                      : "Metrics populate once you connect and transfer"}
                  </p>
                </div>
              </div>
            </article>

            <article className="df2-overview-v3-card">
              <header className="df2-overview-v3-card-head">
                <h2 className="df2-overview-v3-card-title">Connections</h2>
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
                    {connectors.slice(0, 6).map((c) => (
                      <li key={c.id}>
                        <span className={`df2-health-dot ${c.status === "error" || c.last_test_ok === false ? "err" : "ok"}`} />
                        <ConnectorIcon id={c.type} size={18} />
                        <span className="df2-overview-conn-name" title={c.name}>{c.name}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </article>

            <article className="df2-overview-v3-card">
              <header className="df2-overview-v3-card-head">
                <h2 className="df2-overview-v3-card-title">Pipelines</h2>
              </header>
              <div className="df2-overview-v3-card-body">
                {schedules.length === 0 ? (
                  <p className="df2-overview-v3-inline-empty">No scheduled pipelines.</p>
                ) : (
                  <>
                    <ul className="df2-overview-pipeline-list">
                      {schedules.slice(0, 5).map((s) => (
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
                      {enabledPipelines} enabled
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
