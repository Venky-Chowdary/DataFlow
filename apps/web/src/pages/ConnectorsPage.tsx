import { useEffect, useMemo, useState } from "react";
import { ConnectorCatalogPanel } from "../components/ConnectorCatalogPanel";
import { EmptyState } from "../components/EmptyState";
import { PipelineTopology } from "../components/PipelineTopology";
import { DtIcon } from "../components/DtIcon";
import { ConnectorCard } from "../components/ui/ConnectorCard";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageFrame } from "../components/ui/PageFrame";
import { PageInsightStrip } from "../components/ui/PageInsightStrip";
import { PageMetricsRow } from "../components/ui/PageMetricsRow";
import { PageShell } from "../components/ui/PageShell";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { fetchCatalogStats, testSavedConnector, type CatalogConnector } from "../lib/api";
import { resolveCatalogIdToType } from "../lib/connectorTypes";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { buildConnectionWorkbenchContext, formatRelativeTime } from "../lib/connectionWorkbench";
import { connectorHealthLabel, jobStatusBadgeClass } from "../lib/uiUtils";
import { buildDataPlaneTopology } from "../lib/topologyUtils";

interface ConnectorsPageProps {
  connectors: Connector[];
  jobs?: TransferJob[];
  schedules?: PipelineSchedule[];
  onAdd: (type?: string) => void;
  onEdit: (connector: Connector) => void;
  onDelete: (id: string) => void;
  onRefresh?: () => void | Promise<void>;
  showConnectionsTab?: number;
  highlightConnectorId?: string;
}

function catalogType(id: string) {
  return resolveCatalogIdToType(id);
}

const CONNECTION_TABS = ["Status", "Streams", "Schema", "Mappings", "Sync History", "Settings"] as const;

export function ConnectorsPage({ connectors, jobs = [], schedules = [], onAdd, onEdit, onDelete, onRefresh, showConnectionsTab, highlightConnectorId }: ConnectorsPageProps) {
  const { toast } = useToast();
  const [tab, setTab] = useState<"connections" | "catalog">("connections");
  const [role, setRole] = useState<"all" | "source" | "destination">("all");
  const [testingId, setTestingId] = useState<string | null>(null);
  const [testingAll, setTestingAll] = useState(false);
  const [stats, setStats] = useState<{ total: number; live: number; beta: number; transfer_live?: number; connect_only?: number; roadmap?: number; planned?: number } | null>(null);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "ready" | "error">("all");
  const [selectedConnectionId, setSelectedConnectionId] = useState("");
  const [connectionTab, setConnectionTab] = useState<(typeof CONNECTION_TABS)[number]>("Status");

  useEffect(() => {
    if (!highlightConnectorId) return;
    if (!connectors.some((c) => c.id === highlightConnectorId)) return;
    setTab("connections");
    setSelectedConnectionId(highlightConnectorId);
    window.requestAnimationFrame(() => {
      document.getElementById(`connector-card-${highlightConnectorId}`)?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    });
  }, [highlightConnectorId, connectors]);

  useEffect(() => {
    if (showConnectionsTab != null && showConnectionsTab > 0) {
      setTab("connections");
    }
  }, [showConnectionsTab]);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setStats({
        total: s.total,
        live: s.live,
        beta: s.beta,
        transfer_live: s.transfer_live,
        connect_only: s.connect_only,
        roadmap: s.roadmap,
        planned: s.planned,
      }))
      .catch(() => setStats(null));
  }, []);

  useEffect(() => {
    if (!selectedConnectionId && connectors.length > 0) {
      setSelectedConnectionId(connectors[0].id);
    } else if (selectedConnectionId && !connectors.some((c) => c.id === selectedConnectionId)) {
      setSelectedConnectionId(connectors[0]?.id ?? "");
    }
  }, [connectors, selectedConnectionId]);

  const topology = useMemo(
    () => buildDataPlaneTopology(connectors, jobs, schedules),
    [connectors, jobs, schedules],
  );

  const filteredConnectors = useMemo(() => {
    const q = query.trim().toLowerCase();
    return connectors.filter((c) => {
      const statusOk = statusFilter === "all"
        || (statusFilter === "error" ? c.status === "error" : c.status !== "error");
      if (!statusOk) return false;
      if (!q) return true;
      return [c.name, c.type, c.host, c.database]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(q));
    });
  }, [connectors, query, statusFilter]);
  const healthyCount = connectors.filter((c) => c.status !== "error" && c.last_test_ok !== false).length;
  const errorCount = connectors.filter((c) => c.status === "error" || c.last_test_ok === false).length;
  const selectedConnection = connectors.find((c) => c.id === selectedConnectionId) ?? connectors[0];
  const workbench = useMemo(
    () => (selectedConnection ? buildConnectionWorkbenchContext(selectedConnection, jobs, schedules) : null),
    [selectedConnection, jobs, schedules],
  );

  const handleTest = async (id: string) => {
    setTestingId(id);
    try {
      const result = await testSavedConnector(id);
      toast({
        title: result.success ? "Connection OK" : "Connection failed",
        message: result.message,
        tone: result.success ? "success" : "error",
      });
      await onRefresh?.();
    } catch {
      toast({ title: "Test failed", tone: "error" });
    }
    setTestingId(null);
  };

  const handleTestAll = async () => {
    if (!connectors.length) return;
    setTestingAll(true);
    let passed = 0;
    let failed = 0;
    for (const c of connectors) {
      try {
        const result = await testSavedConnector(c.id);
        if (result.success) passed += 1;
        else failed += 1;
      } catch {
        failed += 1;
      }
    }
    toast({
      title: "Connection tests finished",
      message: `${passed} passed · ${failed} failed · ${connectors.length} total`,
      tone: failed > 0 ? "warning" : "success",
    });
    await onRefresh?.();
    setTestingAll(false);
  };

  const handleCatalogSelect = (item: CatalogConnector) => {
    if (item.effective_status === "planned" || (!item.transfer_ready && !item.connect_only)) {
      toast({
        title: "Connector not available yet",
        message: `${item.name} is on the roadmap. Choose a transfer-ready or test-only connector.`,
        tone: "info",
      });
      return;
    }
    if (item.connect_only) {
      toast({
        title: "Connection test only",
        message: `${item.name} supports credential test — transfer routes coming soon.`,
        tone: "warning",
      });
    }
    onAdd(catalogType(item.id));
  };

  const pageInsight = useMemo(() => {
    if (tab === "catalog") {
      return {
        tone: "info" as const,
        pill: "Catalog",
        message: `${stats?.transfer_live ?? stats?.live ?? "—"} connectors support full transfer today. Browse ${stats?.total ?? "—"} integrations — transfer-ready, test-only, and roadmap tiers are labeled in the grid.`,
      };
    }
    return {
      tone: (errorCount > 0 ? "warn" : connectors.length ? "live" : "info") as "info" | "live" | "warn",
      pill:
        connectors.length === 0
          ? "No connections"
          : errorCount > 0
            ? `${errorCount} need attention`
            : `${healthyCount} healthy`,
      message:
        connectors.length === 0
          ? "Browse the catalog to add your first source or destination — credentials are saved once and reused."
          : errorCount
            ? "Re-test failing connections or update credentials before running transfers."
            : `${topology.edges.length} route${topology.edges.length === 1 ? "" : "s"} on the data plane — topology updates as jobs complete.`,
    };
  }, [tab, stats, errorCount, connectors.length, healthyCount, topology.edges.length]);

  return (
    <PageShell
      wide
      className="df2-page-connectors"
      kicker="Integration hub"
      title="Connectors"
      description="Manage saved connections and browse the connector catalog."
      actions={
        <>
          {connectors.length > 0 && (
            <button
              type="button"
              className="df2-btn"
              disabled={testingAll}
              onClick={() => void handleTestAll()}
            >
              <DtIcon name="activity" size={16} />
              {testingAll ? "Testing…" : "Test all"}
            </button>
          )}
          <button type="button" className="df2-btn df2-btn-primary" onClick={() => onAdd()}>
            <DtIcon name="plus" size={16} /> New connection
          </button>
        </>
      }
    >
      <PageFrame className="df2-connectors-page" showHonesty>
      <PageInsightStrip tone={pageInsight.tone} pill={pageInsight.pill} message={pageInsight.message} />
      <PageMetricsRow
        compact
        columns={4}
        metrics={[
          { label: "Transfer ready", value: stats?.transfer_live ?? stats?.live ?? "—", tone: "green", icon: "check" },
          { label: "Test only", value: stats?.connect_only ?? 0, icon: "connectors" },
          { label: "Roadmap", value: stats?.roadmap ?? stats?.planned ?? "—" },
          { label: "Saved", value: connectors.length, icon: "database" },
        ]}
      />

      <FilterTabs
        ariaLabel="Connector views"
        value={tab}
        onChange={setTab}
        items={[
          { id: "connections" as const, label: "My connections", count: connectors.length },
          { id: "catalog" as const, label: "Add connector" },
        ]}
      />

      <div className="df2-connectors-workspace">
      <div className="df2-connectors-pane">
      {tab === "connections" ? (
        <div className="df2-stack">
          <div className="df2-card df2-card-elevated df2-topology-card">
            <div className="df2-card-head">
              <div>
                <h2 className="df2-card-title">Topology</h2>
                <p className="df2-card-sub">Live view of your data plane</p>
              </div>
              <span className="df2-badge df2-badge-live">
                {connectors.filter((c) => c.status !== "error").length} healthy
              </span>
            </div>
            <div className="df2-card-body df2-topology-card-body">
              <PipelineTopology
                nodes={topology.nodes}
                edges={topology.edges}
                emptyHint="Add a connector from the catalog tab, then run a transfer to see live routes."
              />
            </div>
          </div>

          <section className="df2-connection-workbench" aria-label="Connection operations workbench">
            <div className="df2-connection-workbench-head">
              <div>
                <span className="df2-rail-kicker">Connection workbench</span>
                <h2>{selectedConnection?.name ?? "Connection workbench preview"}</h2>
                <p>
                  {selectedConnection
                    ? `${selectedConnection.type} · ${selectedConnection.host || "managed endpoint"}${selectedConnection.port ? `:${selectedConnection.port}` : ""}`
                    : "Preview the production controls every saved connection receives: streams, schema drift, mappings, sync history, and policy settings."}
                </p>
              </div>
              <div className="df2-connection-picker">
                <label className="df2-label" htmlFor="connection-workbench-picker">Connection</label>
                <select
                  id="connection-workbench-picker"
                  className="df2-input df2-select"
                  value={selectedConnection?.id ?? ""}
                  onChange={(e) => setSelectedConnectionId(e.target.value)}
                  disabled={connectors.length === 0}
                >
                  {connectors.length === 0 ? (
                    <option value="">No saved connections</option>
                  ) : connectors.map((c) => (
                    <option key={c.id} value={c.id}>{c.name} · {c.type}</option>
                  ))}
                </select>
              </div>
            </div>

            <FilterTabs
              ariaLabel="Connection sections"
              className="df2-filter-tabs--wrap"
              value={connectionTab}
              onChange={setConnectionTab}
              items={CONNECTION_TABS.map((item) => ({ id: item, label: item }))}
            />

            <div className="df2-connection-panel">
              {connectionTab === "Status" && (
                <div className="df2-connection-status-grid">
                  <div>
                    <span>Health</span>
                    <strong>
                      {selectedConnection
                        ? connectorHealthLabel(selectedConnection.status, selectedConnection.last_test_ok)
                        : "Not configured"}
                    </strong>
                  </div>
                  <div>
                    <span>Last sync</span>
                    <strong>
                      {workbench?.lastJob
                        ? `${workbench.lastJob.status} · ${formatRelativeTime(workbench.lastJob.created_at)}`
                        : "No runs yet"}
                    </strong>
                  </div>
                  <div>
                    <span>Schedule</span>
                    <strong>{workbench?.scheduleLabel ?? "Manual"}</strong>
                  </div>
                  <div>
                    <span>Activity</span>
                    <strong>
                      {workbench
                        ? `${workbench.completedCount} done · ${workbench.runningCount} live · ${workbench.failedCount} failed`
                        : "—"}
                    </strong>
                  </div>
                </div>
              )}
              {connectionTab === "Streams" && (
                selectedConnection ? (
                  workbench && workbench.streams.length > 0 ? (
                    <div className="df2-stream-list">
                      {workbench.streams.map((stream) => (
                        <div key={stream.name} className="df2-stream-row">
                          <strong>{stream.name}</strong>
                          <span className="df2-badge df2-badge-muted">
                            {stream.source === "schedule" ? "Scheduled pipeline" : "Transfer job"}
                          </span>
                        </div>
                      ))}
                    </div>
                  ) : (
                    <EmptyState compact icon="activity" title="No streams yet" description={`Streams appear when you run a transfer or enable a pipeline from ${selectedConnection.name}.`} />
                  )
                ) : (
                  <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view streams." />
                )
              )}
              {connectionTab === "Schema" && (
                selectedConnection ? (
                  <div className="df2-policy-console df2-policy-console-flush">
                    <div className="df2-policy-head">
                      <div>
                        <span className="df2-rail-kicker">Schema contract</span>
                        <h4>{selectedConnection.name}</h4>
                      </div>
                      <span className={`df2-badge ${workbench?.lastJob?.status === "failed" ? "df2-badge-error" : "df2-badge-live"}`}>
                        {workbench?.relatedJobs.length ? "Observed from jobs" : "Awaiting first run"}
                      </span>
                    </div>
                    <div className="df2-schema-review-grid">
                      <div><span>Connector</span><strong>{selectedConnection.type}</strong><p>{selectedConnection.database || selectedConnection.host}</p></div>
                      <div><span>Preflight</span><strong>8-gate validation</strong><p>Schema contract enforced in Transfer Studio before write.</p></div>
                      <div><span>Jobs</span><strong>{workbench?.relatedJobs.length ?? 0}</strong><p>Historical migrations involving this connection.</p></div>
                      <div className="block"><span>Last success</span><strong>{formatRelativeTime(workbench?.lastSuccessAt ?? null)}</strong><p>From completed transfer jobs.</p></div>
                    </div>
                  </div>
                ) : (
                  <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to review schema status." />
                )
              )}
              {connectionTab === "Mappings" && (
                selectedConnection ? (
                  <div className="df2-mapping-policy-grid">
                    <div><strong>Recent routes</strong><span>{workbench?.relatedJobs.length ?? 0} job(s) reference this connector.</span></div>
                    <div><strong>Pipelines</strong><span>{workbench?.relatedSchedules.length ?? 0} schedule(s) · {workbench?.enabledScheduleCount ?? 0} enabled.</span></div>
                    <div><strong>Role</strong><span>{selectedConnection.role ?? "source or destination"} · inferred from usage.</span></div>
                    <div><strong>Review</strong><span>Open Transfer Studio to edit column mappings with live type intelligence.</span></div>
                  </div>
                ) : (
                  <EmptyState compact icon="sparkle" title="Select a connection" description="Choose a saved connection above to view mapping activity." />
                )
              )}
              {connectionTab === "Sync History" && (
                selectedConnection ? (
                  workbench && workbench.relatedJobs.length > 0 ? (
                    <div className="df2-table-wrap df2-card-body-flush">
                      <table className="df2-table" aria-label="Sync history">
                        <thead>
                          <tr>
                            <th>Route</th>
                            <th>Status</th>
                            <th>Rows</th>
                            <th>When</th>
                          </tr>
                        </thead>
                        <tbody>
                          {workbench.relatedJobs.slice(0, 8).map((job) => (
                            <tr key={job._id}>
                              <td>
                                <div className="df2-cell-title">{job.source_name}</div>
                                <div className="df2-cell-meta">{job.source_type} → {job.destination_type}</div>
                              </td>
                              <td><span className={jobStatusBadgeClass(job.status)}>{job.status}</span></td>
                              <td>{job.records_processed?.toLocaleString() ?? "—"}</td>
                              <td className="df2-cell-meta">{formatRelativeTime(job.created_at)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <EmptyState compact icon="jobs" title="No sync history" description={`Run a transfer or enable a pipeline to populate history for ${selectedConnection.name}.`} />
                  )
                ) : (
                  <EmptyState compact icon="connectors" title="Select a connection" description="Choose a saved connection above to view sync history." />
                )
              )}
              {connectionTab === "Settings" && (
                selectedConnection ? (
                  <div className="df2-settings-mini-grid">
                    <div><span>Sync frequency</span><strong>{workbench?.scheduleLabel ?? "Manual"}</strong></div>
                    <div><span>Database / bucket</span><strong>{selectedConnection.database || "—"}</strong></div>
                    <div><span>Endpoint</span><strong>{selectedConnection.host || "managed"}{selectedConnection.port ? `:${selectedConnection.port}` : ""}</strong></div>
                    <div><span>Pipelines</span><strong>{workbench?.relatedSchedules.length ?? 0} configured</strong></div>
                  </div>
                ) : (
                  <EmptyState compact icon="settings" title="Select a connection" description="Choose a saved connection above to view settings." />
                )
              )}
            </div>
          </section>

          {connectors.length === 0 ? (
            <EmptyState
              icon="connectors"
              title="No connections yet"
              description="Browse the catalog, enter credentials once, and reuse connections across every transfer."
              action={
                <button type="button" className="df2-btn df2-btn-primary" onClick={() => setTab("catalog")}>
                  Browse catalog
                </button>
              }
            />
          ) : (
            <>
            <FilterTabs
              ariaLabel="Filter connection status"
              value={statusFilter}
              onChange={setStatusFilter}
              items={[
                { id: "all", label: "All", count: connectors.length },
                { id: "ready", label: "Healthy", count: healthyCount },
                { id: "error", label: "Errors", count: errorCount },
              ]}
            />
            <PageToolbar
              searchValue={query}
              onSearchChange={setQuery}
              searchPlaceholder="Search saved connections…"
            />
            <div className="df2-connector-card-grid" role="list" aria-label="Saved connections">
              {filteredConnectors.map((c) => (
                <ConnectorCard
                  key={c.id}
                  connector={c}
                  index={connectors.findIndex((x) => x.id === c.id)}
                  selected={selectedConnectionId === c.id}
                  highlighted={highlightConnectorId === c.id}
                  testing={testingId === c.id}
                  onSelect={() => setSelectedConnectionId(c.id)}
                  onTest={() => void handleTest(c.id)}
                  onEdit={() => onEdit(c)}
                  onDelete={() => onDelete(c.id)}
                />
              ))}
            </div>
            {filteredConnectors.length === 0 && (
              <EmptyState
                compact
                icon="search"
                title="No matches"
                description="No saved connections match the current search or filters."
              />
            )}
            </>
          )}
        </div>
      ) : (
        <div className="df2-stack">
          <FilterTabs
            ariaLabel="Filter catalog by role"
            value={role}
            onChange={setRole}
            items={[
              { id: "all", label: "All" },
              { id: "source", label: "Sources" },
              { id: "destination", label: "Destinations" },
            ]}
          />
          <div className="df2-card">
            <div className="df2-card-body">
              <ConnectorCatalogPanel role={role} onSelect={handleCatalogSelect} initialStatus="live" requireAvailable />
            </div>
          </div>
        </div>
      )}
      </div>
      </div>
      </PageFrame>
    </PageShell>
  );
}
