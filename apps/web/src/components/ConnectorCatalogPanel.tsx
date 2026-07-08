import { useCallback, useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Skeleton } from "./LoadingState";
import { fetchCatalogConnectors, type CatalogConnector } from "../lib/api";
import { resolveCatalogIdToType } from "../lib/connectorTypes";

const CATEGORY_LABELS: Record<string, string> = {
  database: "Databases",
  warehouse: "Data Warehouses",
  file: "Files & Formats",
  cloud_storage: "Cloud Storage",
  saas: "SaaS Applications",
  api: "APIs & Webhooks",
  finance: "Finance",
  healthcare: "Healthcare",
  marketing: "Marketing",
  logistics: "Logistics",
};

const STATUS_FILTERS = [
  { id: "", label: "All" },
  { id: "live", label: "Live" },
  { id: "beta", label: "Beta" },
  { id: "planned", label: "Planned" },
];

function catalogType(id: string) {
  return resolveCatalogIdToType(id);
}

function highlightMatch(text: string, query: string) {
  if (!query.trim()) return text;
  const idx = text.toLowerCase().indexOf(query.toLowerCase());
  if (idx < 0) return text;
  return (
    <>
      {text.slice(0, idx)}
      <mark className="df2-catalog-highlight">{text.slice(idx, idx + query.length)}</mark>
      {text.slice(idx + query.length)}
    </>
  );
}

interface ConnectorCatalogPanelProps {
  role?: "source" | "destination" | "all";
  onSelect: (connector: CatalogConnector) => void;
  limit?: number;
  /** Hide category sidebar — use in modals */
  compact?: boolean;
}

export function ConnectorCatalogPanel({
  role = "all",
  onSelect,
  limit = 96,
  compact = false,
}: ConnectorCatalogPanelProps) {
  const [query, setQuery] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [category, setCategory] = useState("");
  const [status, setStatus] = useState("");
  const [connectors, setConnectors] = useState<CatalogConnector[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [filtered, setFiltered] = useState(0);
  const [total, setTotal] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const t = setTimeout(() => setDebouncedQ(query), 250);
    return () => clearTimeout(t);
  }, [query]);

  const load = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchCatalogConnectors({
        q: debouncedQ,
        role: role === "all" ? undefined : role,
        category: category || undefined,
        status: status || undefined,
        limit,
      });
      setConnectors(data.connectors || []);
      setCategories(data.categories || []);
      setFiltered(data.filtered ?? data.connectors?.length ?? 0);
      setTotal(data.total ?? 0);
    } catch {
      setConnectors([]);
      setError("Could not load connector catalog. Start the API with npm run dev:api and refresh.");
    }
    setLoading(false);
  }, [debouncedQ, role, category, status, limit]);

  useEffect(() => {
    void load();
  }, [load]);

  const categoryNav = useMemo(() => {
    const items = [{ id: "", label: "All categories" }];
    for (const c of categories) {
      items.push({ id: c, label: CATEGORY_LABELS[c] || c.replace(/_/g, " ") });
    }
    return items;
  }, [categories]);

  return (
    <div className={`df2-catalog-layout ${compact ? "df2-catalog-compact" : ""}`}>
      {!compact && (
        <nav className="df2-catalog-nav" aria-label="Categories">
          {categoryNav.map((item) => (
            <button
              key={item.id || "all"}
              type="button"
              className={category === item.id ? "active" : ""}
              onClick={() => setCategory(item.id)}
            >
              {item.label}
            </button>
          ))}
        </nav>
      )}

      <div>
        <div className="df2-search">
          <span className="df2-search-icon"><DtIcon name="search" size={16} /></span>
          <input
            type="search"
            placeholder={total ? `Search ${total}+ connectors…` : "Search connectors…"}
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            aria-label="Search connector catalog"
            autoFocus={compact}
          />
        </div>

        <div className="df2-chips" role="group" aria-label="Filter by status">
          {STATUS_FILTERS.map((f) => (
            <button
              key={f.id || "all"}
              type="button"
              className={`df2-chip ${status === f.id ? "active" : ""}`}
              onClick={() => setStatus(f.id)}
            >
              {f.label}
            </button>
          ))}
        </div>

        {compact && categoryNav.length > 1 && (
          <div className="df2-chips" style={{ marginTop: 10 }} role="group" aria-label="Filter by category">
            {categoryNav.slice(0, 8).map((item) => (
              <button
                key={item.id || "all-cat"}
                type="button"
                className={`df2-chip ${category === item.id ? "active" : ""}`}
                onClick={() => setCategory(item.id)}
              >
                {item.label}
              </button>
            ))}
          </div>
        )}

        <p style={{ fontSize: 13, color: "#64748b", margin: "0 0 16px" }}>
          {loading ? "Loading…" : `${connectors.length} of ${filtered} · ${total} total`}
        </p>

        {error ? (
          <div className="df2-empty" style={{ padding: "24px 0" }}>
            <p className="df2-empty-desc">{error}</p>
            <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={() => void load()}>
              Retry
            </button>
          </div>
        ) : loading ? (
          <div className="df2-connector-grid" aria-busy="true">
            {Array.from({ length: 12 }, (_, i) => (
              <Skeleton key={i} className="df2-skeleton-tile" />
            ))}
          </div>
        ) : connectors.length === 0 ? (
          <p style={{ color: "#64748b" }}>No connectors match. Try a different search or filter.</p>
        ) : (
          <div className="df2-connector-grid">
            {connectors.map((item) => (
              <button
                key={item.id}
                type="button"
                className="df2-connector-tile"
                onClick={() => onSelect(item)}
                title={item.description}
              >
                <div className="df2-connector-tile-icon">
                  <ConnectorIcon id={catalogType(item.id)} size={28} />
                </div>
                <span className="df2-connector-tile-name">{highlightMatch(item.name, debouncedQ)}</span>
                {item.description && (
                  <span className="df2-connector-tile-desc">{item.description}</span>
                )}
                <span className={`df2-badge df2-badge-${item.status === "live" ? "live" : item.status === "beta" ? "beta" : "muted"}`}>
                  {item.status || "planned"}
                </span>
              </button>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
