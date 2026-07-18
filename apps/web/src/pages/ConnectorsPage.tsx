import { useEffect, useMemo, useState } from "react";
import { ConnectorCatalogPanel } from "../components/ConnectorCatalogPanel";
import { CONNECTION_TABS } from "../components/ConnectionWorkbench";
import { ConnectorDetailDrawer } from "../components/ConnectorDetailDrawer";
import { EmptyState } from "../components/ui/EmptyState";
import { DtIcon } from "../components/DtIcon";
import { Button } from "../components/ui/Button";
import { ConnectorCard } from "../components/ui/ConnectorCard";
import { FilterBar } from "../components/ui/FilterBar";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageContextBar } from "../components/ui/PageContextBar";
import { PageToolbar } from "../components/ui/PageToolbar";
import { LoadingBlock } from "../components/LoadingState";
import { useToast } from "../components/Toast";
import { testSavedConnector, type CatalogConnector } from "../lib/api";
import { resolveCatalogIdToType } from "../lib/connectorTypes";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { buildConnectionWorkbenchContext, lastUsedAtForConnector } from "../lib/connectionWorkbench";

interface ConnectorsPageProps {
  connectors: Connector[];
  /** True while the first connectors fetch has not settled yet. */
  connectorsLoading?: boolean;
  jobs?: TransferJob[];
  schedules?: PipelineSchedule[];
  onAdd: (type?: string) => void;
  onEdit: (connector: Connector) => void;
  onDelete: (id: string) => void;
  onRefresh?: () => void | Promise<void>;
  onOpenTransfer?: () => void;
  onOpenJob?: (jobId: string) => void;
  showConnectionsTab?: number;
  highlightConnectorId?: string;
}

function catalogType(id: string) {
  return resolveCatalogIdToType(id);
}

export function ConnectorsPage({
  connectors,
  connectorsLoading = false,
  jobs = [],
  schedules = [],
  onAdd,
  onEdit,
  onDelete,
  onRefresh,
  onOpenTransfer,
  onOpenJob,
  showConnectionsTab,
  highlightConnectorId,
}: ConnectorsPageProps) {
  const { toast } = useToast();
  const [tab, setTab] = useState<"connections" | "catalog">("connections");
  const [role, setRole] = useState<"all" | "source" | "destination">("all");
  const [testingId, setTestingId] = useState<string | null>(null);
  const [testingAll, setTestingAll] = useState(false);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "ready" | "error">("all");
  const [selectedConnectionId, setSelectedConnectionId] = useState("");
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [connectionTab, setConnectionTab] = useState<(typeof CONNECTION_TABS)[number]>("Status");

  const openDrawer = (id: string) => {
    setSelectedConnectionId(id);
    setConnectionTab("Status");
    setDrawerOpen(true);
  };

  useEffect(() => {
    if (!highlightConnectorId) return;
    if (!connectors.some((c) => c.id === highlightConnectorId)) return;
    setTab("connections");
    setSelectedConnectionId(highlightConnectorId);
    setDrawerOpen(true);
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
    if (selectedConnectionId && !connectors.some((c) => c.id === selectedConnectionId)) {
      setSelectedConnectionId("");
      setDrawerOpen(false);
    }
  }, [connectors, selectedConnectionId]);

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
  const connectorsInUse = useMemo(() => {
    const ids = new Set<string>();
    schedules.forEach((s) => {
      if (s.source_connector_id) ids.add(s.source_connector_id);
      if (s.dest_connector_id) ids.add(s.dest_connector_id);
    });
    return connectors.filter((c) => ids.has(c.id)).length;
  }, [connectors, schedules]);
  const distinctTypes = useMemo(
    () => new Set(connectors.map((c) => c.type)).size,
    [connectors],
  );
  const selectedConnection = connectors.find((c) => c.id === selectedConnectionId) ?? null;
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
    const tier = item.certification_tier || "";
    const isPlanned =
      tier === "planned" ||
      item.effective_status === "planned" ||
      (!item.transfer_ready &&
        !item.connect_only &&
        tier !== "source_only" &&
        item.effective_status !== "live");
    if (isPlanned) {
      toast({
        title: "Connector not available yet",
        message: `${item.name} is on the roadmap. Choose a Certified or Source-only connector.`,
        tone: "info",
      });
      return;
    }
    if (item.connect_only || tier === "source_only" || (!item.transfer_ready && item.effective_status === "live")) {
      toast({
        title: "Source only",
        message: `${item.name} can be saved and tested as a source — full R/W transfer may be limited.`,
        tone: "warning",
      });
    }
    onAdd(catalogType(item.id));
  };

  return (
    <PageShell
      wide
      className="df2-page-connectors"
      title="Connectors"
      description="Saved connections and the transfer-ready catalog."
    >
      <PageFrame className="df2-connectors-page">
        {connectors.length > 0 && (
          <PageContextBar
            ariaLabel="Connections summary"
            stats={[
              { label: "Connections", value: connectors.length, icon: "connectors" },
              { label: "Healthy", value: healthyCount, icon: "check", tone: healthyCount > 0 ? "ok" : "muted" },
              {
                label: "Needs attention",
                value: errorCount,
                icon: "alert",
                tone: errorCount > 0 ? "danger" : "muted",
                title: errorCount > 0 ? "Connections failing their last test" : "All connections passing",
              },
              { label: "In pipelines", value: connectorsInUse, icon: "activity", tone: "muted", title: "Connections referenced by scheduled pipelines" },
              { label: "Data systems", value: distinctTypes, icon: "layers", tone: "muted", title: "Distinct connector types in use" },
            ]}
          />
        )}
        <PageToolbar
          searchValue={tab === "connections" && connectors.length > 0 ? query : undefined}
          onSearchChange={tab === "connections" && connectors.length > 0 ? setQuery : undefined}
          searchPlaceholder="Search saved connections…"
          filters={
            <FilterBar variant="inline" ariaLabel="Connector page filters">
              <FilterTabs
                ariaLabel="Connector views"
                value={tab}
                onChange={setTab}
                items={[
                  { id: "connections" as const, label: "Connections", count: connectors.length },
                  { id: "catalog" as const, label: "Catalog" },
                ]}
              />
              {tab === "connections" && connectors.length > 0 && (
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
              )}
              {tab === "catalog" && (
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
              )}
            </FilterBar>
          }
          actions={
            <>
              {connectors.length > 0 && tab === "connections" && (
                <Button
                  size="sm"
                  loading={testingAll}
                  loadingLabel="Testing…"
                  disabled={testingAll}
                  onClick={() => void handleTestAll()}
                  leadingIcon={<DtIcon name="activity" size={14} />}
                >
                  Test all
                </Button>
              )}
              <Button
                size="sm"
                variant="primary"
                onClick={() => onAdd()}
              >
                New connection
              </Button>
            </>
          }
        />

        <div className="df2-connectors-workspace">
          {tab === "connections" ? (
            connectorsLoading && connectors.length === 0 ? (
              <div className="df2-connectors-empty" aria-busy="true">
                <LoadingBlock
                  title="Loading connections"
                  hint="Fetching saved connectors from your workspace…"
                  size="md"
                />
              </div>
            ) : connectors.length === 0 ? (
              <div className="df2-connectors-empty">
                <EmptyState
                  page
                  icon="connectors"
                  title="No connections yet"
                  description="Browse the catalog, enter credentials once, and reuse connections across Transfer Studio, Pipelines, and Data Pilot."
                  action={
                    <div className="df2-empty-actions-row">
                      <button type="button" className="df2-btn df2-btn-primary" onClick={() => setTab("catalog")}>
                        <DtIcon name="search" size={14} /> Browse catalog
                      </button>
                      <button type="button" className="df2-btn df2-btn-ghost" onClick={() => onAdd()}>
                        Add connection
                      </button>
                    </div>
                  }
                />
                <div className="df2-connectors-empty-features" aria-label="Connection capabilities">
                  {[
                    { icon: "activity" as const, title: "Streams & sync", desc: "Track pipelines and transfer jobs per connection." },
                    { icon: "sparkle" as const, title: "Schema drift", desc: "Review column changes before they hit production." },
                    { icon: "gate" as const, title: "Policy settings", desc: "Residency, RBAC, and audit for enterprise workspaces." },
                  ].map((item) => (
                    <article key={item.title} className="df2-connectors-feature-card">
                      <span className="df2-connectors-feature-icon" aria-hidden>
                        <DtIcon name={item.icon} size={18} />
                      </span>
                      <h3>{item.title}</h3>
                      <p>{item.desc}</p>
                    </article>
                  ))}
                </div>
              </div>
            ) : (
            <div className="df2-connectors-list-full">
              {filteredConnectors.length === 0 ? (
                <EmptyState
                  compact
                  icon="search"
                  title="No matches"
                  description="No saved connections match the current search or filters."
                />
              ) : (
                <div className="df2-connector-rows" role="list" aria-label="Saved connections">
                  <div className="df2-connector-rows-head" aria-hidden>
                    <span className="df2-connector-rows-head-name">Connection</span>
                    <span className="df2-connector-rows-head-role">Role</span>
                    <span className="df2-connector-rows-head-test">Last test</span>
                    <span className="df2-connector-rows-head-used">Last used</span>
                    <span className="df2-connector-rows-head-actions" />
                  </div>
                  {filteredConnectors.map((c) => (
                    <ConnectorCard
                      key={c.id}
                      compact
                      connector={c}
                      index={connectors.findIndex((x) => x.id === c.id)}
                      selected={selectedConnectionId === c.id && drawerOpen}
                      highlighted={highlightConnectorId === c.id}
                      testing={testingId === c.id}
                      lastUsedAt={lastUsedAtForConnector(c, jobs)}
                      onSelect={() => openDrawer(c.id)}
                      onTest={() => void handleTest(c.id)}
                    />
                  ))}
                </div>
              )}
            </div>
            )
          ) : (
            <div className="df2-connectors-pane df2-connectors-catalog">
              <ConnectorCatalogPanel
                role={role}
                onSelect={handleCatalogSelect}
                initialStatus="live"
                requireAvailable={false}
                limit={200}
              />
            </div>
          )}
        </div>

        <ConnectorDetailDrawer
          open={drawerOpen && !!selectedConnection}
          connector={selectedConnection}
          workbench={workbench}
          connectors={connectors}
          connectionTab={connectionTab}
          setConnectionTab={setConnectionTab}
          testing={testingId === selectedConnection?.id}
          onClose={() => setDrawerOpen(false)}
          onTest={() => selectedConnection && void handleTest(selectedConnection.id)}
          onEdit={() => {
            if (!selectedConnection) return;
            setDrawerOpen(false);
            onEdit(selectedConnection);
          }}
          onDelete={() => {
            if (!selectedConnection) return;
            setDrawerOpen(false);
            onDelete(selectedConnection.id);
          }}
          onOpenTransfer={onOpenTransfer}
          onSelectConnection={setSelectedConnectionId}
          onOpenJob={onOpenJob}
        />
      </PageFrame>
    </PageShell>
  );
}
