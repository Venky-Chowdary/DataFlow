import { useMemo, useRef, useState } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { Connector } from "../../lib/types";
import { getConnectorDefaults } from "../../lib/connectorTypes";

/** Curated engines shown first — matches TransferPage FALLBACK_DEST_TYPES. */
const FEATURED_DEST_IDS = ["postgresql", "mongodb", "mysql", "snowflake", "bigquery"] as const;

interface DestinationPickerProps {
  connectors: Connector[];
  connectorId: string;
  destType: string;
  liveDestTypes: { id: string; label: string }[];
  onSelectConnector: (id: string) => void;
  onSelectManual: () => void;
  onSelectType: (type: string) => void;
}

function matchesEngineQuery(d: { id: string; label: string }, q: string): boolean {
  if (!q) return true;
  const hay = `${d.id} ${d.label}`.toLowerCase();
  return hay.includes(q);
}

export function DestinationPicker({
  connectors,
  connectorId,
  destType,
  liveDestTypes,
  onSelectConnector,
  onSelectManual,
  onSelectType,
}: DestinationPickerProps) {
  const typeOptions = useMemo(() => {
    const fromConnectors = [...new Set(connectors.map((c) => c.type))].sort();
    return fromConnectors.map((id) => ({
      id,
      label: liveDestTypes.find((d) => d.id === id)?.label ?? getConnectorDefaults(id).label,
    }));
  }, [connectors, liveDestTypes]);

  const [typeFilter, setTypeFilter] = useState("all");
  const [connectorQuery, setConnectorQuery] = useState("");
  const [engineQuery, setEngineQuery] = useState("");
  const listRef = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    const cq = connectorQuery.trim().toLowerCase();
    return connectors.filter((c) => {
      if (typeFilter !== "all" && c.type !== typeFilter) return false;
      if (!cq) return true;
      const hay = `${c.name} ${c.type} ${c.database || ""} ${c.host || ""}`.toLowerCase();
      return hay.includes(cq);
    });
  }, [connectors, typeFilter, connectorQuery]);

  const manualActive = !connectorId;
  const query = engineQuery.trim().toLowerCase();

  const featuredEngines = useMemo(() => {
    const byId = new Map(liveDestTypes.map((d) => [d.id, d]));
    return FEATURED_DEST_IDS
      .map((id) => byId.get(id) ?? { id, label: getConnectorDefaults(id).label })
      .filter((d) => matchesEngineQuery(d, query));
  }, [liveDestTypes, query]);

  const otherEngines = useMemo(() => {
    const featuredSet = new Set<string>(FEATURED_DEST_IDS);
    return liveDestTypes
      .filter((d) => !featuredSet.has(d.id) && matchesEngineQuery(d, query))
      .sort((a, b) => a.label.localeCompare(b.label));
  }, [liveDestTypes, query]);

  const allEnginesForSelect = useMemo(() => {
    const featuredSet = new Set<string>(FEATURED_DEST_IDS);
    const featured = FEATURED_DEST_IDS.map((id) => {
      const hit = liveDestTypes.find((d) => d.id === id);
      return hit ?? { id, label: getConnectorDefaults(id).label };
    });
    const rest = liveDestTypes
      .filter((d) => !featuredSet.has(d.id))
      .sort((a, b) => a.label.localeCompare(b.label));
    return [...featured, ...rest];
  }, [liveDestTypes]);

  const selectedOutsideFeatured = Boolean(
    destType && !FEATURED_DEST_IDS.includes(destType as (typeof FEATURED_DEST_IDS)[number]),
  );

  const showMoreEngines = otherEngines.length > 0;
  /** When searching, surface matches as a compact list; otherwise only a short peek. */
  const moreEnginesVisible = query ? otherEngines : otherEngines.slice(0, 6);

  const focusSavedConnections = () => {
    setEngineQuery("");
    listRef.current?.scrollTo({ top: 0, behavior: "smooth" });
    const first = listRef.current?.querySelector<HTMLButtonElement>(
      ".df2-dest-connector-card:not(.df2-dest-connector-manual)",
    );
    first?.focus();
  };

  return (
    <div className={`df2-dest-picker${manualActive ? " is-custom-engine" : ""}`}>
      <div className="df2-dest-picker-toolbar">
        <label className="df2-dest-connector-search">
          <DtIcon name="search" size={13} />
          <input
            type="search"
            value={connectorQuery}
            onChange={(e) => setConnectorQuery(e.target.value)}
            placeholder="Search saved connections…"
            aria-label="Search destination connections"
            disabled={connectors.length === 0}
          />
        </label>
        {typeOptions.length > 1 && (
          <label className="df2-dest-type-filter">
            <span className="df2-sr-only">Filter by engine</span>
            <select
              value={typeFilter}
              onChange={(e) => setTypeFilter(e.target.value)}
              aria-label="Filter connections by engine"
            >
              <option value="all">All engines</option>
              {typeOptions.map((t) => (
                <option key={t.id} value={t.id}>{t.label}</option>
              ))}
            </select>
          </label>
        )}
      </div>

      <div
        ref={listRef}
        className="df2-dest-connector-list"
        role="radiogroup"
        aria-label="Destination connectors"
      >
        {filtered.map((c) => (
          <button
            key={c.id}
            type="button"
            role="radio"
            aria-checked={connectorId === c.id}
            className={`df2-dest-connector-card${connectorId === c.id ? " active" : ""}`}
            onClick={() => onSelectConnector(c.id)}
          >
            <span className="df2-dest-connector-card-icon" aria-hidden>
              <ConnectorIcon id={c.type} size={18} />
            </span>
            <span className="df2-dest-connector-card-text">
              <span className="df2-dest-connector-card-name" title={c.name}>{c.name}</span>
              <span
                className="df2-dest-connector-card-meta"
                title={[
                  getConnectorDefaults(c.type).label,
                  c.database || c.host || "",
                ].filter(Boolean).join(" · ")}
              >
                {getConnectorDefaults(c.type).label}
                {c.database ? ` · ${c.database}` : c.host ? ` · ${c.host}` : ""}
              </span>
            </span>
            {c.last_test_ok === true && (
              <span className="df2-dest-connector-card-status ok">Tested</span>
            )}
          </button>
        ))}

        <button
          type="button"
          role="radio"
          aria-checked={manualActive}
          className={`df2-dest-connector-card df2-dest-connector-manual${manualActive ? " active" : ""}`}
          onClick={onSelectManual}
        >
          <span className="df2-dest-connector-card-icon" aria-hidden>
            <DtIcon name="connectors" size={18} />
          </span>
          <span className="df2-dest-connector-card-text">
            <span className="df2-dest-connector-card-name">Custom connection</span>
            <span className="df2-dest-connector-card-meta">Enter host & credentials</span>
          </span>
        </button>
      </div>

      {connectors.length === 0 && (
        <p className="df2-label-hint df2-dest-picker-empty">
          No saved connectors — choose Custom, or add one under Connectors.
        </p>
      )}
      {connectors.length > 0 && filtered.length === 0 && (
        <p className="df2-label-hint df2-dest-picker-empty" role="status">
          No matches — clear search or choose Custom.
        </p>
      )}

      {manualActive && (
        <div className="df2-dest-engine-panel" aria-label="Destination engine">
          <div className="df2-dest-engine-panel-head">
            <span className="df2-label">Engine</span>
            <label className="df2-dest-engine-select-wrap">
              <span className="df2-sr-only">Select destination engine</span>
              <select
                className="df2-dest-engine-select"
                value={destType || ""}
                onChange={(e) => {
                  const next = e.target.value;
                  if (next) onSelectType(next);
                }}
                aria-label="Select destination engine"
              >
                <option value="">Select engine…</option>
                {allEnginesForSelect.map((d) => (
                  <option key={d.id} value={d.id}>{d.label}</option>
                ))}
              </select>
            </label>
            {connectors.length > 0 && (
              <button
                type="button"
                className="df2-dest-engine-back"
                onClick={focusSavedConnections}
                title="Back to saved connections"
              >
                <DtIcon name="chevron-left" size={12} />
                <span>Saved</span>
              </button>
            )}
          </div>

          <label className="df2-dest-engine-search">
            <DtIcon name="search" size={12} />
            <input
              type="search"
              value={engineQuery}
              onChange={(e) => setEngineQuery(e.target.value)}
              placeholder="Search engines…"
              aria-label="Search destination engines"
            />
            {engineQuery ? (
              <button
                type="button"
                className="df2-dest-engine-search-clear"
                aria-label="Clear engine search"
                onClick={() => setEngineQuery("")}
              >
                <DtIcon name="x" size={11} />
              </button>
            ) : null}
          </label>

          <div className="df2-dest-engine-grid" role="radiogroup" aria-label="Featured destination engines">
            {featuredEngines.map((d) => (
              <button
                key={d.id}
                type="button"
                role="radio"
                aria-checked={destType === d.id}
                className={`df2-dest-engine-tile${destType === d.id ? " active" : ""}`}
                onClick={() => onSelectType(d.id)}
                title={d.label}
              >
                <ConnectorIcon id={d.id} size={15} />
                <span>{d.label}</span>
              </button>
            ))}
          </div>

          {featuredEngines.length === 0 && query ? (
            <p className="df2-label-hint df2-dest-engine-selected-hint" role="status">
              No featured matches — try the list below or the engine dropdown.
            </p>
          ) : null}

          {showMoreEngines && (
            <div className="df2-dest-engine-more" aria-label="More destination engines">
              <div className="df2-dest-engine-more-head">
                <span className="df2-dest-engine-hint">
                  {query ? `Matches (${otherEngines.length})` : "More engines"}
                </span>
                {!query && otherEngines.length > moreEnginesVisible.length ? (
                  <span className="df2-label-hint">
                    +{otherEngines.length - moreEnginesVisible.length} in dropdown
                  </span>
                ) : null}
              </div>
              <div className="df2-dest-engine-more-grid" role="list">
                {moreEnginesVisible.map((d) => (
                  <button
                    key={d.id}
                    type="button"
                    role="listitem"
                    className={`df2-dest-engine-tile${destType === d.id ? " active" : ""}`}
                    onClick={() => onSelectType(d.id)}
                    title={d.label}
                  >
                    <ConnectorIcon id={d.id} size={14} />
                    <span>{d.label}</span>
                  </button>
                ))}
              </div>
            </div>
          )}

          {selectedOutsideFeatured && destType && (
            <p className="df2-label-hint df2-dest-engine-selected-hint">
              Selected: {liveDestTypes.find((d) => d.id === destType)?.label
                ?? getConnectorDefaults(destType).label}
            </p>
          )}
        </div>
      )}
    </div>
  );
}
