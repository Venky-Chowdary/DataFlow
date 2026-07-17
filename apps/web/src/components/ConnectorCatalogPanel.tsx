import { useCallback, useEffect, useMemo, useState } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Skeleton } from "./LoadingState";
import { FilterTabs } from "./ui/FilterTabs";
import { FilterBar } from "./ui/FilterBar";
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

const STATIC_CATEGORIES = Object.keys(CATEGORY_LABELS);

const STATUS_FILTERS = [
  { id: "live", label: "Transfer ready" },
  { id: "connect_only", label: "Test only" },
  { id: "", label: "All catalog" },
  { id: "planned", label: "Roadmap" },
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

function statusBadge(item: CatalogConnector) {
  const eff = item.effective_status || item.status;
  if (item.transfer_ready || eff === "live") return { cls: "df2-badge-live", label: item.capability_label || "Full transfer" };
  if (item.connect_only || eff === "connect_only") return { cls: "df2-badge-run", label: item.capability_label || "Test only" };
  return { cls: "df2-badge-muted", label: "Roadmap" };
}

interface ConnectorCatalogPanelProps {
  role?: "source" | "destination" | "all";
  onSelect: (connector: CatalogConnector) => void;
  limit?: number;
  compact?: boolean;
  /** Block planned + connect-only when picking for transfer */
  transferOnly?: boolean;
  /** When true, only live transfer connectors are clickable */
  requireAvailable?: boolean;
  initialStatus?: string;
}

export function ConnectorCatalogPanel({
  role = "all",
  onSelect,
  limit = 96,
  compact = false,
  transferOnly = false,
  requireAvailable = false,
  initialStatus = "",
}: ConnectorCatalogPanelProps) {
  const [query, setQuery] = useState("");
  const [debouncedQ, setDebouncedQ] = useState("");
  const [category, setCategory] = useState("");
  const [status, setStatus] = useState(initialStatus);
  const [connectors, setConnectors] = useState<CatalogConnector[]>([]);
  const [categories, setCategories] = useState<string[]>([]);
  const [filtered, setFiltered] = useState(0);
  const [total, setTotal] = useState(0);
  const [transferLive, setTransferLive] = useState(0);
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
        transferOnly: transferOnly || (requireAvailable && status === "live"),
      });
      setConnectors(data.connectors || []);
      setCategories(data.categories || []);
      setFiltered(data.filtered ?? data.connectors?.length ?? 0);
      setTotal(data.total ?? 0);
      setTransferLive(data.transfer_live ?? 0);
    } catch {
      setConnectors([]);
      setError("Could not load connector catalog. Start the API with npm run dev:api and refresh.");
    }
    setLoading(false);
  }, [debouncedQ, role, category, status, limit, transferOnly, requireAvailable]);

  useEffect(() => {
    void load();
  }, [load]);

  const categoryNav = useMemo(() => {
    const items = [{ id: "", label: "All categories" }];
    const source = categories.length > 0 ? categories : STATIC_CATEGORIES;
    for (const c of source) {
      items.push({ id: c, label: CATEGORY_LABELS[c] || c.replace(/_/g, " ") });
    }
    return items;
  }, [categories]);

  const showSidebar = !compact;

  return (
    <div className={`df2-catalog-layout ${compact ? "df2-catalog-compact" : ""}`}>
      {showSidebar && (
        <nav className="df2-catalog-nav" aria-label="Connector categories">
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

      <div className="df2-catalog-main">
        {compact && (
          <div className="df2-chips" role="group" aria-label="Connector categories">
            {categoryNav.map((item) => (
              <button
                key={item.id || "all"}
                type="button"
                className={`df2-chip ${category === item.id ? "active" : ""}`}
                onClick={() => setCategory(item.id)}
              >
                {item.label}
              </button>
            ))}
          </div>
        )}

        <div className="df2-catalog-filter-row">
          <label className="df2-toolbar-search df2-catalog-search" aria-label="Search connector catalog">
            <DtIcon name="search" size={15} />
            <input
              type="search"
              placeholder="Search connectors…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              autoFocus={compact}
            />
          </label>

          <FilterBar ariaLabel="Catalog capability filters" className="df2-catalog-filter-bar">
            <FilterTabs
              ariaLabel="Filter by capability"
              value={status}
              onChange={setStatus}
              items={STATUS_FILTERS.map((f) => ({ id: f.id, label: f.label }))}
            />
          </FilterBar>
        </div>

        {!error && !loading && (
          <p className="df2-catalog-meta">
            {filtered.toLocaleString()} shown
            {total > 0 && ` · ${total.toLocaleString()} in catalog`}
            {transferLive > 0 && ` · ${transferLive.toLocaleString()} transfer-ready`}
          </p>
        )}

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
          <p className="df2-catalog-empty">No connectors match. Try &quot;Transfer ready&quot; or a different search.</p>
        ) : (
          <div className="df2-connector-grid">
            {connectors.map((item) => {
              const badge = statusBadge(item);
              const clickable = item.transfer_ready || (!requireAvailable && (item.connect_only || item.status === "beta"));
              const blocked = requireAvailable && !item.transfer_ready && !item.connect_only;
              return (
                <button
                  key={item.id}
                  type="button"
                  className={`df2-connector-tile ${!item.transfer_ready ? "is-planned" : ""} ${item.transfer_ready ? "is-live" : ""}`}
                  onClick={() => {
                    if (blocked && !item.connect_only) return;
                    onSelect(item);
                  }}
                  disabled={blocked && !item.connect_only}
                  title={
                    item.transfer_ready
                      ? `${item.name} — ${item.description}`
                      : item.connect_only
                        ? `${item.name} — connection test only, transfer not yet supported`
                        : `${item.name} — on roadmap`
                  }
                >
                  <div className="df2-connector-tile-icon">
                    <ConnectorIcon id={catalogType(item.id)} size={28} />
                  </div>
                  <span className="df2-connector-tile-name">{highlightMatch(item.name, debouncedQ)}</span>
                  {item.description && (
                    <span className="df2-connector-tile-desc">{item.description}</span>
                  )}
                  <span className={`df2-badge ${badge.cls}`}>{badge.label}</span>
                  {!clickable && !item.connect_only && (
                    <span className="df2-connector-tile-lock">Roadmap</span>
                  )}
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
