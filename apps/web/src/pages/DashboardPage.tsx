import { useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { fetchCatalogStats } from "../lib/api";
import { formatRelativeTime } from "../lib/connectionWorkbench";
import {
  buildStatusDistribution,
  buildThroughputSeries,
} from "../lib/overviewAnalytics";
import { jobStatusBadgeClass } from "../lib/uiUtils";
import { DtIcon } from "../components/DtIcon";
import { EmptyState } from "../components/EmptyState";
import { DataPlaneFlow } from "../components/overview/DataPlaneFlow";
import { StatusDonut, ThroughputChart } from "../components/overview/OverviewCharts";
import { PageFrame } from "../components/ui/PageFrame";
import { PageInsightStrip } from "../components/ui/PageInsightStrip";
import { PageMetricsRow } from "../components/ui/PageMetricsRow";
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

const JOB_LIMIT = 6;

export function DashboardPage({
  connectors,
  jobs,
  schedules = [],
  onNewTransfer,
  onOpenPilot,
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
  const alertCount = failed.length + connectors.filter((c) => c.status === "error").length;
  const enabledPipelines = schedules.filter((s) => s.enabled).length;

  const throughputSeries = useMemo(() => buildThroughputSeries(jobs), [jobs]);
  const statusSlices = useMemo(() => buildStatusDistribution(jobs), [jobs]);

  const insight = !connectors.length
    ? "Connect your first source or destination to activate the data plane."
    : failed.length
      ? `${failed.length} migration${failed.length > 1 ? "s" : ""} need attention — open Job Theater for logs.`
      : running.length
        ? `${running.length} live migration${running.length > 1 ? "s" : ""} streaming to Job Theater.`
        : jobs.length === 0
          ? "Run your first governed transfer to populate topology and analytics."
          : "Data plane healthy — throughput and routes updating from live jobs.";

  const topology = useMemo(
    () => buildDataPlaneTopology(connectors, jobs, schedules),
    [connectors, jobs, schedules],
  );
  const routeCount = topology.edges.length;
  const healthTone = alertCount > 0 ? "warn" : running.length > 0 ? "live" : "ok";

  return (
    <PageShell
      wide
      className="df2-page-overview-enterprise"
      kicker="Control plane"
      title="Overview"
      description="Enterprise data plane — live analytics, pipelines, and migration health."
      actions={
        <div className="df2-overview-toolbar-actions">
          {onOpenPilot && (
            <button type="button" className="df2-btn df2-btn-ghost df2-btn-icon" onClick={onOpenPilot} title="Data Pilot">
              <DtIcon name="sparkle" size={18} />
            </button>
          )}
          <button type="button" className="df2-btn df2-btn-primary" onClick={onNewTransfer}>
            <DtIcon name="plus" size={16} /> New transfer
          </button>
        </div>
      }
    >
      <PageFrame className="df2-overview-page df2-overview-enterprise" showHonesty>
        <PageInsightStrip
          tone={healthTone}
          pill={
            healthTone === "warn"
              ? `${alertCount} alert${alertCount > 1 ? "s" : ""}`
              : healthTone === "live"
                ? `${running.length} live`
                : "Operational"
          }
          message={insight}
          actions={
            failed.length > 0 && onOpenJobs ? (
              <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenJobs}>
                <DtIcon name="alert" size={14} /> Open Job Theater
              </button>
            ) : undefined
          }
        />

        <PageMetricsRow
          metrics={[
            { label: "Rows moved", value: totalRecords.toLocaleString(), tone: "teal", icon: "trend", sub: "7-day total" },
            { label: "Success rate", value: successRate != null ? `${successRate}%` : "—", tone: "green", icon: "check", sub: jobs.length ? `${completed.length} completed` : "No jobs yet" },
            { label: "Connections", value: connectors.length, tone: healthyConnectors < connectors.length ? "red" : undefined, icon: "connectors", sub: `${healthyConnectors} healthy` },
            { label: "Active routes", value: routeCount, tone: "teal", icon: "activity", sub: enabledPipelines ? `${enabledPipelines} pipelines` : "No schedules" },
          ]}
        />

        <section className="df2-overview-analytics" aria-label="Analytics">
          <article className="df2-glass-panel df2-chart-panel df2-chart-panel-throughput">
            <header className="df2-glass-panel-head">
              <div>
                <h2 className="df2-glass-title">Throughput</h2>
                <p className="df2-glass-sub">Rows moved per day</p>
              </div>
              <span className="df2-glass-badge">{totalRecords.toLocaleString()} total</span>
            </header>
            <div className="df2-glass-panel-body df2-chart-panel-body">
              {jobs.length === 0 ? (
                <EmptyState
                  compact
                  icon="trend"
                  title="No throughput data"
                  description="Completed transfers populate the 7-day trend."
                />
              ) : (
                <ThroughputChart series={throughputSeries} />
              )}
            </div>
          </article>

          <article className="df2-glass-panel df2-chart-panel df2-chart-panel-donut-wrap">
            <header className="df2-glass-panel-head">
              <div>
                <h2 className="df2-glass-title">Migration mix</h2>
                <p className="df2-glass-sub">Job status distribution</p>
              </div>
              <span className="df2-glass-badge">{jobs.length} jobs</span>
            </header>
            <div className="df2-glass-panel-body df2-chart-panel-body df2-chart-panel-donut">
              {jobs.length === 0 ? (
                <EmptyState
                  compact
                  icon="jobs"
                  title="No jobs yet"
                  description="Run a transfer to see status distribution."
                  action={
                    <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onNewTransfer}>
                      New transfer
                    </button>
                  }
                />
              ) : (
                <StatusDonut slices={statusSlices} centerLabel="success" centerValue={`${successRate}%`} />
              )}
            </div>
          </article>
        </section>

        <div className="df2-overview-workspace">
          <div className="df2-overview-primary">
            <article className="df2-glass-panel df2-glass-panel-deep df2-overview-flow">
              <header className="df2-glass-panel-head">
                <div>
                  <h2 className="df2-glass-title">Data plane</h2>
                  <p className="df2-glass-sub">
                    {routeCount
                      ? `${routeCount} pipeline route${routeCount > 1 ? "s" : ""} across your workspace`
                      : `${connectors.length} connection${connectors.length === 1 ? "" : "s"} ready`}
                  </p>
                </div>
                {onOpenConnectors && (
                  <button type="button" className="df2-overview-text-link" onClick={onOpenConnectors}>
                    Connectors →
                  </button>
                )}
              </header>
              <div className="df2-glass-panel-body df2-overview-flow-body">
                <DataPlaneFlow
                  nodes={topology.nodes}
                  edges={topology.edges}
                  connectionCount={connectors.length}
                  onOpenConnectors={onOpenConnectors}
                />
              </div>
            </article>

            <article className="df2-glass-panel df2-overview-migrations">
              <header className="df2-glass-panel-head">
                <div>
                  <h2 className="df2-glass-title">Recent migrations</h2>
                  <p className="df2-glass-sub">Latest activity</p>
                </div>
                {onOpenJobs && jobs.length > 0 && (
                  <button type="button" className="df2-overview-text-link" onClick={onOpenJobs}>
                    View all →
                  </button>
                )}
              </header>
              {jobs.length === 0 ? (
                <div className="df2-glass-panel-body">
                  <EmptyState
                    compact
                    icon="transfer"
                    title="No migrations yet"
                    description="Start a governed transfer from Transfer Studio."
                    action={
                      <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onNewTransfer}>
                        New transfer
                      </button>
                    }
                  />
                </div>
              ) : (
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
              )}
            </article>
          </div>

          <aside className="df2-overview-rail">
            <article className="df2-glass-panel">
              <header className="df2-glass-panel-head">
                <h2 className="df2-glass-title">Connections</h2>
                {onOpenConnectors && (
                  <button type="button" className="df2-overview-text-link" onClick={onOpenConnectors}>
                    Manage →
                  </button>
                )}
              </header>
              <div className="df2-glass-panel-body">
                {connectors.length === 0 ? (
                  <EmptyState
                    compact
                    icon="connectors"
                    title="No connections"
                    description="Add a source or destination from Connectors."
                    action={
                      onOpenConnectors ? (
                        <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenConnectors}>
                          Open Connectors
                        </button>
                      ) : undefined
                    }
                  />
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

            <article className="df2-glass-panel">
              <header className="df2-glass-panel-head">
                <h2 className="df2-glass-title">Pipelines</h2>
              </header>
              <div className="df2-glass-panel-body">
                {schedules.length === 0 ? (
                  <EmptyState compact icon="activity" title="No pipelines" description="Schedule recurring syncs from Pipelines." />
                ) : (
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
                )}
                <p className="df2-overview-rail-meta">
                  {catalogStats?.transfer_live ?? catalogStats?.live ?? "120+"} transfer-ready · {enabledPipelines} enabled
                </p>
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
