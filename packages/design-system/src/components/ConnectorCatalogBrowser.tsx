export interface CatalogConnector {
  id: string;
  name: string;
  category: string;
  status: "live" | "beta" | "planned" | "ai";
  description: string;
}

interface ConnectorCatalogBrowserProps {
  connectors: CatalogConnector[];
  total: number;
  catalogTotal: number;
  liveCount: number;
  categories: string[];
  query: string;
  category: string;
  loading?: boolean;
  onQueryChange: (q: string) => void;
  onCategoryChange: (category: string) => void;
  onLoadMore?: () => void;
  hasMore?: boolean;
}

const STATUS_LABEL: Record<CatalogConnector["status"], string> = {
  live: "Live",
  beta: "Beta",
  planned: "Planned",
  ai: "AI Factory",
};

const CATEGORY_LABEL: Record<string, string> = {
  database: "Database",
  warehouse: "Warehouse",
  file: "File",
  api: "API",
  saas: "SaaS",
  cloud_storage: "Cloud storage",
  marketing: "Marketing",
  finance: "Finance",
  healthcare: "Healthcare",
  logistics: "Logistics",
};

export function ConnectorCatalogBrowser({
  connectors,
  total,
  catalogTotal,
  liveCount,
  categories,
  query,
  category,
  loading,
  onQueryChange,
  onCategoryChange,
  onLoadMore,
  hasMore,
}: ConnectorCatalogBrowserProps) {
  return (
    <div className="df-catalog-browser">
      <div className="df-catalog-browser-toolbar">
        <input
          className="df-input df-catalog-search"
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          placeholder="Search connectors (certified + roadmap)…"
        />
        <select className="df-select df-catalog-filter" value={category} onChange={(e) => onCategoryChange(e.target.value)}>
          <option value="">All categories</option>
          {categories.map((c) => (
            <option key={c} value={c}>
              {CATEGORY_LABEL[c] ?? c}
            </option>
          ))}
        </select>
        <span className="df-catalog-stats">
          {liveCount} live · {catalogTotal} total
          {query || category ? ` · ${total} matching` : ""}
        </span>
      </div>

      {loading && connectors.length === 0 ? (
        <p className="df-empty-inline">Loading connector catalog…</p>
      ) : (
        <div className="df-connector-grid df-connector-grid--catalog">
          {connectors.map((c) => (
            <div key={c.id} className="df-connector-card df-connector-card--static">
              <div className="df-connector-card-top">
                <span className="df-connector-name">{c.name}</span>
                <span className={["df-connector-status", `df-connector-status--${c.status}`].join(" ")}>
                  {STATUS_LABEL[c.status]}
                </span>
              </div>
              <div className="df-connector-category">{CATEGORY_LABEL[c.category] ?? c.category}</div>
              <div className="df-connector-desc">{c.description}</div>
            </div>
          ))}
        </div>
      )}

      {hasMore && onLoadMore && (
        <div className="df-catalog-load-more">
          <button type="button" className="df-btn df-btn--ghost" disabled={loading} onClick={onLoadMore}>
            {loading ? "Loading…" : `Load more (${connectors.length} of ${total})`}
          </button>
        </div>
      )}
    </div>
  );
}
