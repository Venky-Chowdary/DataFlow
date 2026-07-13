import { useEffect, useMemo, useState } from "react";
import { ConnectorCatalogPanel } from "../components/ConnectorCatalogPanel";
import { ConnectionWorkbench, CONNECTION_TABS } from "../components/ConnectionWorkbench";
import { EmptyState } from "../components/EmptyState";
import { DtIcon } from "../components/DtIcon";
import { ConnectorCard } from "../components/ui/ConnectorCard";
import { FilterTabs } from "../components/ui/FilterTabs";
import { PageFrame } from "../components/ui/PageFrame";
import { PageShell } from "../components/ui/PageShell";
import { PageToolbar } from "../components/ui/PageToolbar";
import { useToast } from "../components/Toast";
import { testSavedConnector, type CatalogConnector } from "../lib/api";
import { resolveCatalogIdToType } from "../lib/connectorTypes";
import { Connector, PipelineSchedule, TransferJob } from "../lib/types";
import { buildConnectionWorkbenchContext } from "../lib/connectionWorkbench";

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

export function ConnectorsPage({ connectors, jobs = [], schedules = [], onAdd, onEdit, onDelete, onRefresh, showConnectionsTab, highlightConnectorId }: ConnectorsPageProps) {
  const { toast } = useToast();
  const [tab, setTab] = useState<"connections" | "catalog">("connections");
  const [role, setRole] = useState<"all" | "source" | "destination">("all");
  const [testingId, setTestingId] = useState<string | null>(null);
  const [testingAll, setTestingAll] = useState(false);
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
    if (!selectedConnectionId && connectors.length > 0) {
      setSelectedConnectionId(connectors[0].id);
    } else if (selectedConnectionId && !connectors.some((c) => c.id === selectedConnectionId)) {
      setSelectedConnectionId(connectors[0]?.id ?? "");
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

  return (
    <PageShell wide className="df2-page-connectors" title="Connectors">
      <PageFrame className="df2-connectors-page">
        <PageToolbar
          searchValue={tab === "connections" && connectors.length > 0 ? query : undefined}
          onSearchChange={tab === "connections" && connectors.length > 0 ? setQuery : undefined}
          searchPlaceholder="Search saved connections…"
          filters={
            <>
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
            </>
          }
          actions={
            <>
              {connectors.length > 0 && tab === "connections" && (
                <button
                  type="button"
                  className="df2-btn df2-btn-sm"
                  disabled={testingAll}
                  onClick={() => void handleTestAll()}
                >
                  <DtIcon name="activity" size={14} />
                  {testingAll ? "Testing…" : "Test all"}
                </button>
              )}
              <button type="button" className="df2-btn df2-btn-sm df2-btn-primary" onClick={() => onAdd()}>
                <DtIcon name="plus" size={14} /> New connection
              </button>
            </>
          }
        />

        <div className="df2-connectors-workspace">
          {tab === "connections" ? (
            <>
            <div className="df2-connectors-layout">
              <aside className="df2-connectors-list">
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
                    {filteredConnectors.length === 0 && (
                      <EmptyState
                        compact
                        icon="search"
                        title="No matches"
                        description="No saved connections match the current search or filters."
                      />
                    )}
                  </div>
                )}
              </aside>
              <section className="df2-connectors-detail">
                <ConnectionWorkbench
                  selectedConnection={selectedConnection}
                  workbench={workbench}
                  connectionTab={connectionTab}
                  setConnectionTab={setConnectionTab}
                  connectors={connectors}
                  onSelectConnection={setSelectedConnectionId}
                />
              </section>
            </div>
            </>
          ) : (
            <div className="df2-connectors-pane">
              <div className="df2-card">
                <div className="df2-card-body">
                  <ConnectorCatalogPanel role={role} onSelect={handleCatalogSelect} initialStatus="live" requireAvailable={false} limit={200} />
                </div>
              </div>
            </div>
          )}
        </div>
      </PageFrame>
    </PageShell>
  );
}
