import { useMemo, useState } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { FilterTabs } from "../ui/FilterTabs";
import { FilterBar } from "../ui/FilterBar";
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
  // Filter tabs only reflect saved connector types — never dump the full catalog.
  const typeFilters = useMemo(() => {
    const fromConnectors = [...new Set(connectors.map((c) => c.type))].sort();
    return [
      { id: "all", label: "All" },
      ...fromConnectors.map((id) => ({
        id,
        label: liveDestTypes.find((d) => d.id === id)?.label ?? getConnectorDefaults(id).label,
      })),
    ];
  }, [connectors, liveDestTypes]);

  const [filter, setFilter] = useState("all");
  const [connectorQuery, setConnectorQuery] = useState("");
  const [engineQuery, setEngineQuery] = useState("");

  const filtered = useMemo(() => {
    const cq = connectorQuery.trim().toLowerCase();
    return connectors.filter((c) => {
      if (filter !== "all" && c.type !== filter) return false;
      if (!cq) return true;
      const hay = `${c.name} ${c.type} ${c.database || ""} ${c.host || ""}`.toLowerCase();
      return hay.includes(cq);
    });
  }, [connectors, filter, connectorQuery]);

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

  const selectedOutsideFeatured = Boolean(
    destType && !FEATURED_DEST_IDS.includes(destType as (typeof FEATURED_DEST_IDS)[number]),
  );

  return (
    <div className="df2-dest-picker">
      <div className="df2-dest-picker-head">
        <div>
          <label className="df2-label">Connection</label>
          <p className="df2-label-hint">
            Saved connector or custom host credentials.
          </p>
        </div>
        {connectors.length > 0 && typeFilters.length > 1 && (
          <FilterBar ariaLabel="Filter destinations by type">
            <FilterTabs
              ariaLabel="Filter destinations by type"
              value={filter}
              onChange={setFilter}
              items={typeFilters}
            />
          </FilterBar>
        )}
      </div>

      {connectors.length > 3 && (
        <label className="df2-dest-connector-search">
          <DtIcon name="search" size={13} />
          <input
            type="search"
            value={connectorQuery}
            onChange={(e) => setConnectorQuery(e.target.value)}
            placeholder="Search connections…"
            aria-label="Search destination connections"
          />
        </label>
      )}

      <div className="df2-dest-connector-grid" role="listbox" aria-label="Destination connectors">
        {filtered.map((c) => (
          <button
            key={c.id}
            type="button"
            role="option"
            aria-selected={connectorId === c.id}
            className={`df2-dest-connector-card${connectorId === c.id ? " active" : ""}`}
            onClick={() => onSelectConnector(c.id)}
          >
            <ConnectorIcon id={c.type} size={18} />
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
            {c.last_test_ok === true && (
              <span className="df2-dest-connector-card-status ok">Tested</span>
            )}
          </button>
        ))}

        <button
          type="button"
          className={`df2-dest-connector-card df2-dest-connector-manual${manualActive ? " active" : ""}`}
          onClick={onSelectManual}
        >
          <DtIcon name="connectors" size={18} />
          <span className="df2-dest-connector-card-name">Custom connection</span>
          <span className="df2-dest-connector-card-meta">Host & credentials</span>
        </button>
      </div>

      {manualActive && (
        <div className="df2-dest-manual-types">
          <div className="df2-dest-engine-toolbar">
            <span className="df2-dest-manual-types-label">Engine</span>
            <label className="df2-dest-engine-search">
              <DtIcon name="search" size={13} />
              <input
                type="search"
                value={engineQuery}
                onChange={(e) => setEngineQuery(e.target.value)}
                placeholder="Search…"
                aria-label="Search destination engines"
              />
            </label>
          </div>

          <div className="df2-dest-type-chips" role="listbox" aria-label="Featured destination engines">
            {featuredEngines.map((d) => (
              <button
                key={d.id}
                type="button"
                role="option"
                aria-selected={destType === d.id}
                className={`df2-dest-type-chip${destType === d.id ? " active" : ""}`}
                onClick={() => onSelectType(d.id)}
              >
                <ConnectorIcon id={d.id} size={14} />
                {d.label}
              </button>
            ))}
            {featuredEngines.length === 0 && (
              <span className="df2-label-hint">No featured engines match.</span>
            )}
          </div>

          {(otherEngines.length > 0 || selectedOutsideFeatured) && (
            <label className="df2-field" style={{ margin: 0 }}>
              <span className="df2-label">More engines</span>
              <select
                className="df2-dest-engine-select"
                value={selectedOutsideFeatured ? destType : ""}
                onChange={(e) => {
                  const next = e.target.value;
                  if (next) onSelectType(next);
                }}
                aria-label="Select other destination engine"
              >
                <option value="">
                  {otherEngines.length
                    ? `Choose from ${otherEngines.length} more…`
                    : "Select engine…"}
                </option>
                {(selectedOutsideFeatured
                  && !otherEngines.some((d) => d.id === destType)
                  ? [
                      {
                        id: destType,
                        label: liveDestTypes.find((d) => d.id === destType)?.label
                          ?? getConnectorDefaults(destType).label,
                      },
                      ...otherEngines,
                    ]
                  : otherEngines
                ).map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.label}
                  </option>
                ))}
              </select>
            </label>
          )}
        </div>
      )}

      {connectors.length === 0 && (
        <p className="df2-label-hint df2-dest-picker-empty">
          No saved connectors yet — use Custom connection or add one under Connectors.
        </p>
      )}

      {manualActive && !destType && (
        <p className="df2-label-hint">
          Select an engine to continue.
        </p>
      )}
    </div>
  );
}
