import { useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { ConnectorMarketplace } from "../components/ConnectorMarketplace";
import { DataFlowGraph } from "../components/DataFlowGraph";
import { DtIcon } from "../components/DtIcon";
import { CONNECTOR_CATALOG, Connector } from "../lib/types";

interface ConnectorsPageProps {
  connectors: Connector[];
  onAdd: (type?: string) => void;
  onDelete: (id: string) => void;
}

export function ConnectorsPage({ connectors, onAdd, onDelete }: ConnectorsPageProps) {
  const [setupMode, setSetupMode] = useState<"none" | "source" | "destination">("none");
  const [tab, setTab] = useState<"connections" | "catalog">("connections");

  const flowNodes = connectors.map((c) => ({
    id: c.id,
    label: c.name,
    type: c.type,
    active: c.status !== "error",
  }));

  return (
    <div className="dt-content">
      <div className="dt-page-header">
        <div className="dt-page-header-row">
          <div>
            <h1 className="dt-page-title">Connectors</h1>
            <p className="dt-page-subtitle">
              Configure sources and destinations here — transfers pick from saved connections only.
            </p>
          </div>
          <div className="dt-flex dt-gap-2">
            <button type="button" className="dt-btn" onClick={() => setSetupMode("source")}>
              <DtIcon name="plus" size={16} /> New Source
            </button>
            <button type="button" className="dt-btn dt-btn-primary" onClick={() => onAdd()}>
              <DtIcon name="connectors" size={18} /> Configure Connector
            </button>
          </div>
        </div>
      </div>

      <div className="dt-card dt-flow-card dt-mb-6">
        <div className="dt-card-header">
          <div>
            <h3 className="dt-card-title">Live Data Topology</h3>
            <p className="dt-text-sm dt-text-muted">Animated flow from connected systems into the platform hub</p>
          </div>
          <span className="dt-badge dt-badge-success">
            <span className="dt-badge-dot" /> {connectors.length} connected
          </span>
        </div>
        <div className="dt-card-body">
          <DataFlowGraph
            nodes={flowNodes}
            emptyHint="Add your first connector to visualize data flow"
          />
        </div>
      </div>

      {setupMode !== "none" && (
        <div className="dt-card dt-mb-6">
          <div className="dt-card-body">
            <ConnectorMarketplace
              role={setupMode}
              title={setupMode === "source" ? "Set up a new source" : "Set up a new destination"}
              onSelect={(c) => {
                const type = c.id.replace(/___/g, "_").split("_")[0];
                onAdd(type === "csv" ? "csv" : type);
                setSetupMode("none");
              }}
            />
            <button type="button" className="dt-btn dt-btn-ghost dt-mt-4" onClick={() => setSetupMode("none")}>
              Cancel
            </button>
          </div>
        </div>
      )}

      <div className="dt-tabs dt-mb-6">
        <button type="button" className={tab === "connections" ? "active" : ""} onClick={() => setTab("connections")}>
          Saved Connections ({connectors.length})
        </button>
        <button type="button" className={tab === "catalog" ? "active" : ""} onClick={() => setTab("catalog")}>
          Connector Catalog
        </button>
      </div>

      {tab === "connections" ? (
        connectors.length === 0 ? (
          <div className="dt-card dt-connector-onboard">
            <div className="dt-card-body dt-text-center">
              <h2 className="dt-font-semibold dt-mb-2">Configure Your First Connection</h2>
              <p className="dt-text-muted dt-mb-6" style={{ maxWidth: 520, margin: "0 auto 24px" }}>
                All database credentials and host settings live here. New Transfer will reference these saved connectors — nothing to re-enter per job.
              </p>
              <button type="button" className="dt-btn dt-btn-primary" onClick={() => onAdd()}>
                <DtIcon name="plus" size={18} /> Configure Connector
              </button>
            </div>
          </div>
        ) : (
          <div className="dt-connectors-grid">
            {connectors.map((connector, i) => (
              <div key={connector.id} className="dt-connector-card dt-connector-card-live" style={{ animationDelay: `${i * 60}ms` }}>
                <div className="dt-connector-header">
                  <div className="dt-connector-icon dt-connector-icon-framed">
                    <ConnectorIcon id={connector.type} size={32} />
                  </div>
                  <div>
                    <div className="dt-connector-name">{connector.name}</div>
                    <div className="dt-connector-type-label">{connector.type}</div>
                  </div>
                  <span className="dt-connector-live-dot" title="Connected" />
                </div>
                <div className="dt-connector-meta">
                  <div className="dt-connector-meta-row">
                    <span className="dt-text-muted">Host</span>
                    <span className="dt-mono">{connector.host}:{connector.port}</span>
                  </div>
                  {connector.database && (
                    <div className="dt-connector-meta-row">
                      <span className="dt-text-muted">Database</span>
                      <span className="dt-mono">{connector.database}</span>
                    </div>
                  )}
                </div>
                <div className="dt-connector-footer">
                  <span className="dt-badge dt-badge-success">
                    <span className="dt-badge-dot" /> {connector.status || "configured"}
                  </span>
                  <button type="button" className="dt-btn dt-btn-ghost dt-btn-sm dt-btn-danger" onClick={() => onDelete(connector.id)}>
                    Remove
                  </button>
                </div>
              </div>
            ))}
          </div>
        )
      ) : (
        <div className="dt-connectors-catalog">
          {CONNECTOR_CATALOG.map((item, i) => (
            <button
              key={item.id}
              type="button"
              className="dt-connector-type"
              style={{ animationDelay: `${i * 40}ms` }}
              onClick={() => onAdd(item.id)}
            >
              <div className="dt-connector-icon-framed sm">
                <ConnectorIcon id={item.id} size={32} />
              </div>
              <span className="dt-connector-type-name">{item.label}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
