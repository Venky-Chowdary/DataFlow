import { useEffect, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { fetchCatalogConnectors } from "../lib/api";

export interface CatalogConnector {
  id: string;
  name: string;
  category: string;
  status: string;
  description: string;
}

interface ConnectorMarketplaceProps {
  role: "source" | "destination" | "all";
  title: string;
  onSelect: (connector: CatalogConnector) => void;
}

const STATUS_CLASS: Record<string, string> = {
  live: "dt-badge-success",
  beta: "dt-badge-info",
  planned: "dt-badge-default",
  enterprise: "dt-badge-warning",
};

export function ConnectorMarketplace({ role, title, onSelect }: ConnectorMarketplaceProps) {
  const [query, setQuery] = useState("");
  const [tab, setTab] = useState<"catalog" | "marketplace">("catalog");
  const [connectors, setConnectors] = useState<CatalogConnector[]>([]);
  const [suggested, setSuggested] = useState<CatalogConnector[]>([]);
  const [total, setTotal] = useState(620);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    setLoading(true);
    fetchCatalogConnectors({ q: query, role, limit: 59 })
      .then((data) => {
        setConnectors(data.connectors || []);
        setSuggested(data.suggested || []);
        setTotal(data.total || 620);
      })
      .catch(() => {})
      .finally(() => setLoading(false));
  }, [query, role]);

  const grid = query ? connectors : suggested.length ? suggested : connectors;

  return (
    <div className="dt-marketplace">
      <div className="dt-marketplace-header">
        <h2 className="dt-font-semibold">{title}</h2>
        <div className="dt-marketplace-search">
          <DtIcon name="search" size={18} />
          <input
            className="dt-input"
            placeholder="Search 620+ connectors…"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
          />
        </div>
      </div>

      <div className="dt-marketplace-tabs">
        <button type="button" className={tab === "catalog" ? "active" : ""} onClick={() => setTab("catalog")}>
          DataTransfer Connectors
        </button>
        <button type="button" className={tab === "marketplace" ? "active" : ""} onClick={() => setTab("marketplace")}>
          Marketplace
        </button>
      </div>

      <div className="dt-marketplace-meta">
        <span>Suggested {role === "source" ? "sources" : role === "destination" ? "destinations" : "connectors"}</span>
        <span className="dt-text-muted">{grid.length} of {total} connectors</span>
      </div>

      {loading ? (
        <p className="dt-text-muted dt-text-center dt-p-8">Loading catalog…</p>
      ) : (
        <div className="dt-marketplace-grid">
          {grid.map((c) => (
            <button key={c.id} type="button" className="dt-marketplace-card" onClick={() => onSelect(c)}>
              <ConnectorIcon id={c.id.replace(/___/g, "_").split("_")[0]} size={32} />
              <div className="dt-marketplace-card-body">
                <span className="dt-marketplace-card-name">{c.name}</span>
                <span className={`dt-badge dt-badge-sm ${STATUS_CLASS[c.status] || "dt-badge-default"}`}>
                  {c.status}
                </span>
              </div>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
