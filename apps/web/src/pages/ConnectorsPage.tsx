import { useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { ConnectionHub } from "../components/ConnectionHub";
import { ConnectorCatalogPanel } from "../components/ConnectorCatalogPanel";
import { DtIcon } from "../components/DtIcon";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
import { useToast } from "../components/Toast";
import { fetchCatalogStats, testSavedConnector, type CatalogConnector } from "../lib/api";
import { resolveCatalogIdToType } from "../lib/connectorTypes";
import { Connector } from "../lib/types";

interface ConnectorsPageProps {
  connectors: Connector[];
  onAdd: (type?: string) => void;
  onEdit: (connector: Connector) => void;
  onDelete: (id: string) => void;
}

function catalogType(id: string) {
  return resolveCatalogIdToType(id);
}

const CONNECTION_TABS = ["Status", "Streams", "Schema", "Mappings", "Sync History", "Settings"] as const;

export function ConnectorsPage({ connectors, onAdd, onEdit, onDelete }: ConnectorsPageProps) {
  const { toast } = useToast();
  const [tab, setTab] = useState<"connections" | "catalog">("connections");
  const [role, setRole] = useState<"all" | "source" | "destination">("all");
  const [testingId, setTestingId] = useState<string | null>(null);
  const [stats, setStats] = useState<{ total: number; live: number; beta: number } | null>(null);
  const [query, setQuery] = useState("");
  const [statusFilter, setStatusFilter] = useState<"all" | "ready" | "error">("all");
  const [selectedConnectionId, setSelectedConnectionId] = useState("");
  const [connectionTab, setConnectionTab] = useState<(typeof CONNECTION_TABS)[number]>("Status");

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setStats({ total: s.total, live: s.live, beta: s.beta }))
      .catch(() => setStats(null));
  }, []);

  useEffect(() => {
    if (!selectedConnectionId && connectors.length > 0) {
      setSelectedConnectionId(connectors[0].id);
    } else if (selectedConnectionId && !connectors.some((c) => c.id === selectedConnectionId)) {
      setSelectedConnectionId(connectors[0]?.id ?? "");
    }
  }, [connectors, selectedConnectionId]);

  const flowNodes = connectors.map((c) => ({
    id: c.id,
    label: c.name,
    type: c.type,
    active: c.status !== "error",
  }));

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
  const selectedConnection = connectors.find((c) => c.id === selectedConnectionId) ?? connectors[0];

  const handleTest = async (id: string) => {
    setTestingId(id);
    try {
      const result = await testSavedConnector(id);
      toast({
        title: result.success ? "Connection OK" : "Connection failed",
        message: result.message,
        tone: result.success ? "success" : "error",
      });
    } catch {
      toast({ title: "Test failed", tone: "error" });
    }
    setTestingId(null);
  };

  const handleCatalogSelect = (item: CatalogConnector) => {
    onAdd(catalogType(item.id));
  };

  return (
    <PageShell
      wide
      title="Connectors"
      description="Manage saved connections and browse the connector catalog."
      actions={
        <button type="button" className="df2-btn df2-btn-primary" onClick={() => onAdd()}>
          <DtIcon name="plus" size={16} /> New connection
        </button>
      }
    >
      {stats && (
        <div className="df2-stats">
          <StatCard label="Catalog" value={`${stats.total}+`} tone="blue" />
          <StatCard label="Live" value={stats.live} tone="green" />
          <StatCard label="Beta" value={stats.beta} />
          <StatCard label="Saved" value={connectors.length} />
        </div>
      )}

      <div className="df2-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          className={`df2-tab ${tab === "connections" ? "active" : ""}`}
          onClick={() => setTab("connections")}
        >
          My connections ({connectors.length})
        </button>
        <button
          type="button"
          role="tab"
          className={`df2-tab ${tab === "catalog" ? "active" : ""}`}
          onClick={() => setTab("catalog")}
        >
          Add connector
        </button>
      </div>

      {tab === "connections" ? (
        <div className="df2-stack">
          <div className="df2-card">
            <div className="df2-card-head">
              <div>
                <h2 className="df2-card-title">Topology</h2>
                <p className="df2-card-sub">Live view of your data plane</p>
              </div>
              <span className="df2-badge df2-badge-live">
                {connectors.filter((c) => c.status !== "error").length} healthy
              </span>
            </div>
            <div className="df2-card-body">
              <ConnectionHub
                nodes={flowNodes}
                centerLabel="DataFlow"
                emptyHint="Add a connector from the catalog tab"
                variant="hero"
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

            <div className="df2-connection-tabs" role="tablist" aria-label="Connection sections">
              {CONNECTION_TABS.map((item) => (
                <button
                  key={item}
                  type="button"
                  role="tab"
                  aria-selected={connectionTab === item}
                  className={connectionTab === item ? "active" : ""}
                  onClick={() => setConnectionTab(item)}
                >
                  {item}
                </button>
              ))}
            </div>

            <div className="df2-connection-panel">
              {connectionTab === "Status" && (
                <div className="df2-connection-status-grid">
                  <div><span>Health</span><strong>{selectedConnection ? (selectedConnection.status === "error" ? "Action needed" : "Ready") : "Ready to configure"}</strong></div>
                  <div><span>Last sync</span><strong>{selectedConnection ? "Awaiting run" : "No run yet"}</strong></div>
                  <div><span>Schedule</span><strong>Manual</strong></div>
                  <div><span>Schema drift</span><strong>Detect before sync</strong></div>
                </div>
              )}
              {connectionTab === "Streams" && (
                <div className="df2-table-wrap">
                  <table className="df2-table">
                    <thead><tr><th>Stream</th><th>Sync mode</th><th>Cursor</th><th>Primary key</th><th>Status</th></tr></thead>
                    <tbody>
                      {["orders", "customers", "events"].map((stream, i) => (
                        <tr key={stream}>
                          <td><span className="df2-cell-title">{stream}</span></td>
                          <td>{i === 2 ? "Incremental append" : "Full refresh overwrite"}</td>
                          <td>{i === 2 ? "updated_at" : "Not required"}</td>
                          <td>{stream.slice(0, -1)}_id</td>
                          <td><span className="df2-badge df2-badge-live">Selected</span></td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
              {connectionTab === "Schema" && (
                <div className="df2-schema-review-grid">
                  {["New column", "Removed field", "Type change", "Cursor removed"].map((item, i) => (
                    <div key={item} className={i === 3 ? "block" : ""}>
                      <span>{item}</span>
                      <strong>{i === 3 ? "Pause for review" : "Policy controlled"}</strong>
                      <p>{i === 3 ? "Breaking drift requires manual cursor selection." : "Non-breaking changes follow the selected propagation policy."}</p>
                    </div>
                  ))}
                </div>
              )}
              {connectionTab === "Mappings" && (
                <div className="df2-mapping-policy-grid">
                  <div><strong>Rename</strong><span>Normalize source names to warehouse contracts.</span></div>
                  <div><strong>Hash</strong><span>Protect sensitive fields before write.</span></div>
                  <div><strong>Encrypt</strong><span>Apply policy transforms to PII columns.</span></div>
                  <div><strong>Filter</strong><span>Drop rows failing quality or compliance rules.</span></div>
                </div>
              )}
              {connectionTab === "Sync History" && (
                <div className="df2-connection-timeline">
                  {["Connection created", "Schema discovered", "Preflight ready"].map((item) => (
                    <div key={item}><span /><strong>{item}</strong><p>Waiting for first production sync.</p></div>
                  ))}
                </div>
              )}
              {connectionTab === "Settings" && (
                <div className="df2-settings-mini-grid">
                  <div><span>Sync frequency</span><strong>Manual / scheduled</strong></div>
                  <div><span>Destination namespace</span><strong>Source-defined</strong></div>
                  <div><span>Schema propagation</span><strong>Manual approval</strong></div>
                  <div><span>Backfill</span><strong>Off by default</strong></div>
                </div>
              )}
            </div>
          </section>

          {connectors.length === 0 ? (
            <div className="df2-empty">
              <DtIcon name="connectors" size={32} />
              <h3 className="df2-empty-title">No connections yet</h3>
              <p className="df2-empty-desc">
                Browse the catalog, enter credentials once, and reuse connections across every transfer.
              </p>
              <button type="button" className="df2-btn df2-btn-primary" onClick={() => setTab("catalog")}>
                Browse catalog
              </button>
            </div>
          ) : (
            <>
            <div className="df2-table-toolbar">
              <label className="df2-table-search" aria-label="Search saved connections">
                <DtIcon name="search" size={15} />
                <input
                  type="search"
                  placeholder="Search saved connections..."
                  value={query}
                  onChange={(e) => setQuery(e.target.value)}
                />
              </label>
              <div className="df2-segment" role="group" aria-label="Filter connection status">
                {(["all", "ready", "error"] as const).map((item) => (
                  <button
                    key={item}
                    type="button"
                    className={statusFilter === item ? "active" : ""}
                    onClick={() => setStatusFilter(item)}
                  >
                    {item === "all" ? "All" : item === "ready" ? "Ready" : "Errors"}
                  </button>
                ))}
              </div>
            </div>
            <div className="df2-table-wrap">
              <table className="df2-table" aria-label="Saved connections">
                <thead>
                  <tr>
                    <th>Connection</th>
                    <th>Type</th>
                    <th>Endpoint</th>
                    <th>Status</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {filteredConnectors.map((c) => (
                    <tr key={c.id}>
                      <td>
                        <div className="df2-cell-main">
                          <div className="df2-cell-icon">
                            <ConnectorIcon id={c.type} size={22} />
                          </div>
                          <div>
                            <div className="df2-cell-title">{c.name}</div>
                            <div className="df2-cell-meta">{c.database || "—"}</div>
                          </div>
                        </div>
                      </td>
                      <td><span className="df2-badge df2-badge-muted">{c.type}</span></td>
                      <td><span className="df2-cell-meta">{c.host}{c.port ? `:${c.port}` : ""}</span></td>
                      <td>
                        <span className={`df2-badge ${c.status === "error" ? "df2-badge-error" : "df2-badge-live"}`}>
                          {c.status || "ready"}
                        </span>
                      </td>
                      <td style={{ textAlign: "right" }}>
                        <button
                          type="button"
                          className="df2-btn df2-btn-ghost df2-btn-sm"
                          disabled={testingId === c.id}
                          onClick={() => void handleTest(c.id)}
                        >
                          {testingId === c.id ? "Testing…" : "Test"}
                        </button>
                        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm" onClick={() => onEdit(c)}>
                          Edit
                        </button>
                        <button type="button" className="df2-btn df2-btn-ghost df2-btn-sm df2-btn-danger" onClick={() => onDelete(c.id)}>
                          Remove
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {filteredConnectors.length === 0 && (
              <div className="df2-empty df2-empty-compact">
                <p className="df2-empty-desc">No saved connections match the current filters.</p>
              </div>
            )}
            </>
          )}
        </div>
      ) : (
        <div className="df2-stack">
          <div style={{ display: "flex", justifyContent: "flex-end" }}>
            <div className="df2-segment" role="group" aria-label="Connector role">
              {(["all", "source", "destination"] as const).map((r) => (
                <button
                  key={r}
                  type="button"
                  className={role === r ? "active" : ""}
                  onClick={() => setRole(r)}
                >
                  {r === "all" ? "All" : r === "source" ? "Sources" : "Destinations"}
                </button>
              ))}
            </div>
          </div>
          <div className="df2-card">
            <div className="df2-card-body">
              <ConnectorCatalogPanel role={role} onSelect={handleCatalogSelect} />
            </div>
          </div>
        </div>
      )}
    </PageShell>
  );
}
