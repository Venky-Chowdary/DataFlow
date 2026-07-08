import { useEffect, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { Connector, TransferJob } from "../lib/types";
import { fetchCatalogStats } from "../lib/api";
import { DtIcon } from "../components/DtIcon";
import { ConnectionHub } from "../components/ConnectionHub";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";

interface DashboardPageProps {
  connectors: Connector[];
  jobs: TransferJob[];
  onNewTransfer: () => void;
  onOpenPilot?: () => void;
  onOpenConnectors?: () => void;
  onOpenJobs?: () => void;
}

export function DashboardPage({
  connectors,
  jobs,
  onNewTransfer,
  onOpenPilot,
  onOpenConnectors,
  onOpenJobs,
}: DashboardPageProps) {
  const [catalogStats, setCatalogStats] = useState<{ live: number; beta: number; total: number } | null>(null);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setCatalogStats({ live: s.live, beta: s.beta, total: s.total }))
      .catch(() => setCatalogStats(null));
  }, []);

  const completed = jobs.filter((j) => j.status === "completed");
  const failed = jobs.filter((j) => j.status === "failed");
  const running = jobs.filter((j) => j.status === "running" || j.status === "pending");
  const totalRecords = completed.reduce((sum, j) => sum + (j.records_processed || 0), 0);
  const successRate = jobs.length ? Math.round((completed.length / jobs.length) * 100) : 100;
  const healthyConnectors = connectors.filter((c) => c.status !== "error").length;
  const connectionSummary = connectors.length ? `${healthyConnectors}/${connectors.length}` : "0";
  const connectionSub = connectors.length ? "healthy connections" : "No connections";
  const alertCount = failed.length + connectors.filter((c) => c.status === "error").length;
  const latestJob = jobs[0];
  const nextAction = !connectors.length
    ? {
        title: "Connect your first source or destination",
        body: "Add a saved connector so Transfer Studio can reuse credentials and validate routes.",
        action: "Open connectors",
        icon: "connectors",
        onClick: onOpenConnectors ?? onNewTransfer,
      }
    : failed.length
      ? {
          title: "Resolve failed migrations",
          body: "Open Job Theater to inspect the failed run, retry when possible, or return to Transfer Studio.",
          action: "Open Job Theater",
          icon: "jobs",
          onClick: onOpenJobs ?? onNewTransfer,
        }
      : jobs.length === 0
        ? {
            title: "Run your first governed transfer",
            body: "Upload a file or select a database source, map schema, run preflight, and execute.",
            action: "Start transfer",
            icon: "transfer",
            onClick: onNewTransfer,
          }
        : {
            title: "Monitor the data plane",
            body: "Review throughput, schema assurance, and recent job history from one control surface.",
            action: "Watch jobs",
            icon: "activity",
            onClick: onOpenJobs ?? onNewTransfer,
          };

  const flowNodes = connectors.map((c) => ({
    id: c.id,
    label: c.name,
    type: c.type,
    active: c.status !== "error",
  }));

  return (
    <PageShell
      wide
      title="Overview"
      description="Enterprise data plane — sync health, throughput, and pipeline activity."
      actions={
        <>
          {onOpenPilot && (
            <button type="button" className="df2-btn" onClick={onOpenPilot}>
              <DtIcon name="sparkle" size={16} /> Data Pilot
            </button>
          )}
          <button type="button" className="df2-btn df2-btn-primary" onClick={onNewTransfer}>
            <DtIcon name="plus" size={16} /> New transfer
          </button>
        </>
      }
    >
      {failed.length > 0 && (
        <div className="df2-alert df2-alert-error" role="alert">
          <DtIcon name="alert" size={18} />
          <div>
            <strong>{failed.length} failed migration{failed.length > 1 ? "s" : ""} need attention</strong>
            <p>Check Job Theater for error details and retry from Transfer Studio.</p>
          </div>
          {onOpenJobs && (
            <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenJobs}>
              Open Job Theater
            </button>
          )}
        </div>
      )}

      {running.length > 0 && (
        <div className="df2-alert df2-alert-info" role="status">
          <span className="df2-pulse-dot" />
          <div>
            <strong>{running.length} migration{running.length > 1 ? "s" : ""} in progress</strong>
            <p>Live batch progress available in Job Theater.</p>
          </div>
          {onOpenJobs && (
            <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={onOpenJobs}>
              Watch live
            </button>
          )}
        </div>
      )}

      <section className="df2-dashboard-command" aria-label="Overview command center">
        <div className="df2-dashboard-next">
          <span className="df2-rail-kicker">Next best action</span>
          <h2>{nextAction.title}</h2>
          <p>{nextAction.body}</p>
          <div className="df2-dashboard-next-actions">
            <button type="button" className="df2-btn df2-btn-primary" onClick={nextAction.onClick}>
              <DtIcon name={nextAction.icon} size={16} /> {nextAction.action}
            </button>
            {onOpenPilot && (
              <button type="button" className="df2-btn" onClick={onOpenPilot}>
                <DtIcon name="sparkle" size={16} /> Ask Data Pilot
              </button>
            )}
          </div>
        </div>
        <div className="df2-dashboard-matrix" aria-label="Control plane summary">
          <button type="button" onClick={onOpenConnectors ?? onNewTransfer}>
            <span>Connection mesh</span>
            <strong>{connectors.length ? `${healthyConnectors}/${connectors.length} ready` : "Not connected"}</strong>
            <small>Sources and destinations</small>
          </button>
          <button type="button" onClick={onNewTransfer}>
            <span>Transfer readiness</span>
            <strong>{prettifyPercent(successRate)}</strong>
            <small>Success posture</small>
          </button>
          <button type="button" onClick={onOpenJobs ?? onNewTransfer}>
            <span>Runtime theater</span>
            <strong>{running.length ? `${running.length} live` : "Idle"}</strong>
            <small>Jobs and reconciliation</small>
          </button>
          <button type="button" onClick={failed.length ? onOpenJobs : onNewTransfer}>
            <span>Risk queue</span>
            <strong>{alertCount ? `${alertCount} alert${alertCount > 1 ? "s" : ""}` : "Clean"}</strong>
            <small>Failures and connector errors</small>
          </button>
        </div>
      </section>

      <div className="df2-stats df2-stats-hero">
        <StatCard label="Records moved" value={totalRecords.toLocaleString()} tone="blue" />
        <StatCard label="Success rate" value={`${successRate}%`} tone={successRate >= 90 ? "green" : "red"} />
        <StatCard label="Connections" value={connectionSummary} sub={connectionSub} tone={connectors.length && healthyConnectors === connectors.length ? "green" : undefined} />
        <StatCard
          label="Connectors live"
          value={catalogStats ? `${catalogStats.live + catalogStats.beta}` : "—"}
          sub={catalogStats ? `${catalogStats.total}+ catalog` : undefined}
        />
        <StatCard label="Active jobs" value={running.length} tone={running.length > 0 ? "blue" : undefined} />
      </div>

      <section className="df2-ops-board" aria-label="Operational controls">
        <div className={`df2-ops-card ${alertCount ? "warn" : "ok"}`}>
          <span>System health</span>
          <strong>{alertCount ? `${alertCount} alert${alertCount > 1 ? "s" : ""}` : "Clean"}</strong>
          <p>{healthyConnectors} connectors ready · {running.length} live job{running.length === 1 ? "" : "s"}</p>
        </div>
        <div className="df2-ops-card">
          <span>Schema drift</span>
          <strong>Detect before sync</strong>
          <p>Breaking cursor/key changes pause the route for review.</p>
        </div>
        <div className="df2-ops-card">
          <span>Mapping assurance</span>
          <strong>Optimal assignment</strong>
          <p>Exact match, semantic graph, type guard, review threshold.</p>
        </div>
        <div className="df2-ops-card">
          <span>Last activity</span>
          <strong>{latestJob ? latestJob.status : "No jobs"}</strong>
          <p>{latestJob ? `${latestJob.source_type} to ${latestJob.destination_type}` : "Start a transfer to populate history."}</p>
        </div>
      </section>

      <div className="df2-grid-2 df2-dashboard-grid">
        <div className="df2-stack">
          <div className="df2-card df2-card-elevated">
            <div className="df2-card-head">
              <div>
                <h2 className="df2-card-title">Data plane topology</h2>
                <p className="df2-card-sub">Live routing across your connections</p>
              </div>
              <span className="df2-badge df2-badge-live">{healthyConnectors} healthy</span>
            </div>
            <div className="df2-card-body">
              <ConnectionHub
                nodes={flowNodes}
                centerLabel="DataFlow"
                emptyHint="Add connectors to activate the data plane"
                variant="hero"
              />
            </div>
          </div>

          <div className="df2-card">
            <div className="df2-card-head">
              <h2 className="df2-card-title">Recent migrations</h2>
              <div style={{ display: "flex", gap: 8 }}>
                {failed.length > 0 && <span className="df2-badge df2-badge-error">{failed.length} failed</span>}
                {running.length > 0 && <span className="df2-badge df2-badge-run">{running.length} live</span>}
              </div>
            </div>
            {jobs.length === 0 ? (
              <div className="df2-empty">
                <DtIcon name="transfer" size={28} />
                <h3 className="df2-empty-title">No migrations yet</h3>
                <p className="df2-empty-desc">Start your first transfer — batch progress streams to Job Theater.</p>
                <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={onNewTransfer}>
                  Start transfer
                </button>
              </div>
            ) : (
              <div className="df2-table-wrap df2-card-body-flush">
                <table className="df2-table" aria-label="Recent migrations">
                  <thead>
                    <tr>
                      <th>Route</th>
                      <th>Status</th>
                      <th>Progress</th>
                      <th>Rows</th>
                    </tr>
                  </thead>
                  <tbody>
                    {jobs.slice(0, 10).map((job) => (
                      <tr key={job._id} className={job.status === "failed" ? "df2-row-error" : ""}>
                        <td>
                          <div className="df2-cell-title">{job.source_name}</div>
                          <div className="df2-cell-meta">{job.source_type} → {job.destination_type} · {job.destination_collection || job.destination_database}</div>
                        </td>
                        <td>
                          <span className={`df2-badge ${
                            job.status === "completed" ? "df2-badge-live"
                              : job.status === "failed" ? "df2-badge-error"
                              : "df2-badge-run"
                          }`}>
                            {job.status}
                          </span>
                        </td>
                        <td>
                          {(job.status === "running" || job.status === "pending") && job.progress_pct != null ? (
                            <div className="df2-inline-bar">
                              <div className="df2-inline-fill" style={{ width: `${job.progress_pct}%` }} />
                              <span>{job.progress_pct}%</span>
                            </div>
                          ) : job.status === "failed" && job.error ? (
                            <span className="df2-cell-meta df2-text-error" title={job.error}>
                              {job.error.slice(0, 40)}…
                            </span>
                          ) : "—"}
                        </td>
                        <td>{job.records_processed?.toLocaleString() ?? "—"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>

        <aside className="df2-stack">
          <div className="df2-card">
            <div className="df2-card-head">
              <h2 className="df2-card-title">Connection health</h2>
              {onOpenConnectors && (
                <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={onOpenConnectors}>
                  Manage
                </button>
              )}
            </div>
            <div className="df2-card-body">
              {connectors.length === 0 ? (
                <div className="df2-compact-empty">
                  <p>No connections configured.</p>
                  {onOpenConnectors && (
                    <button type="button" className="df2-btn df2-btn-sm" onClick={onOpenConnectors}>
                      Add connector
                    </button>
                  )}
                </div>
              ) : (
                connectors.slice(0, 10).map((c) => (
                  <div key={c.id} className="df2-health-item">
                    <span className={`df2-health-dot ${c.status === "error" ? "err" : "ok"}`} />
                    <div className="df2-cell-icon">
                      <ConnectorIcon id={c.type} size={18} />
                    </div>
                    <div style={{ minWidth: 0, flex: 1 }}>
                      <div className="df2-cell-title">{c.name}</div>
                      <div className="df2-cell-meta">{c.type} · {c.host}{c.port ? `:${c.port}` : ""}</div>
                    </div>
                  </div>
                ))
              )}
            </div>
          </div>

          <div className="df2-card">
            <div className="df2-card-head">
              <h2 className="df2-card-title">Platform capabilities</h2>
            </div>
            <div className="df2-card-body df2-cap-list">
              <div className="df2-cap-item"><DtIcon name="shield" size={16} /> 8 preflight validation gates</div>
              <div className="df2-cap-item"><DtIcon name="sparkle" size={16} /> Semantic mapping (1M+ schematics)</div>
              <div className="df2-cap-item"><DtIcon name="activity" size={16} /> Batch writes · 5K rows/commit</div>
              <div className="df2-cap-item"><DtIcon name="check" size={16} /> Post-transfer reconciliation</div>
              <div className="df2-cap-item"><DtIcon name="connectors" size={16} /> {catalogStats?.total ?? "620"}+ connector catalog</div>
            </div>
          </div>

          <div className="df2-card">
            <div className="df2-card-head">
              <h2 className="df2-card-title">Quick actions</h2>
            </div>
            <div className="df2-card-body df2-stack" style={{ gap: 8 }}>
              <button type="button" className="df2-btn df2-btn-block" onClick={onNewTransfer}>
                <DtIcon name="upload" size={16} /> File → Database
              </button>
              <button type="button" className="df2-btn df2-btn-block" onClick={onNewTransfer}>
                <DtIcon name="connectors" size={16} /> Database → Database
              </button>
              {onOpenJobs && (
                <button type="button" className="df2-btn df2-btn-block" onClick={onOpenJobs}>
                  <DtIcon name="jobs" size={16} /> Job Theater
                </button>
              )}
            </div>
          </div>
        </aside>
      </div>
    </PageShell>
  );
}

function prettifyPercent(value: number) {
  return `${Math.max(0, Math.min(100, value))}%`;
}
