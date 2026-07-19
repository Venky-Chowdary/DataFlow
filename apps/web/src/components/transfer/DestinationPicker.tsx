import { useMemo, useState } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../DtIcon";
import { FilterTabs } from "../ui/FilterTabs";
import { FilterBar } from "../ui/FilterBar";
import { Connector } from "../../lib/types";
import { getConnectorDefaults } from "../../lib/connectorTypes";

interface DestinationPickerProps {
  connectors: Connector[];
  connectorId: string;
  destType: string;
  liveDestTypes: { id: string; label: string }[];
  onSelectConnector: (id: string) => void;
  onSelectManual: () => void;
  onSelectType: (type: string) => void;
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
  const typeFilters = useMemo(() => {
    const fromConnectors = [...new Set(connectors.map((c) => c.type))];
    const fromLive = liveDestTypes.map((d) => d.id);
    const merged = [...new Set([...fromConnectors, ...fromLive])].sort();
    return [
      { id: "all", label: "All" },
      ...merged.map((id) => ({
        id,
        label: liveDestTypes.find((d) => d.id === id)?.label ?? getConnectorDefaults(id).label,
      })),
    ];
  }, [connectors, liveDestTypes]);

  const [filter, setFilter] = useState("all");

  const filtered = useMemo(
    () => connectors.filter((c) => filter === "all" || c.type === filter),
    [connectors, filter],
  );

  const manualActive = !connectorId;

  return (
    <div className="df2-dest-picker">
      <div className="df2-dest-picker-head">
        <div>
          <label className="df2-label">Choose destination</label>
          <p className="df2-label-hint" style={{ margin: "2px 0 0" }}>
            Pick a saved connection — the route updates to that system, not a default engine.
          </p>
        </div>
        <FilterBar ariaLabel="Filter destinations by type">
          <FilterTabs
            ariaLabel="Filter destinations by type"
            value={filter}
            onChange={setFilter}
            items={typeFilters}
          />
        </FilterBar>
      </div>

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
            <span className="df2-dest-connector-card-name">{c.name}</span>
            <span className="df2-dest-connector-card-meta">
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
          <span className="df2-dest-connector-card-meta">Enter host & credentials</span>
        </button>
      </div>

      {manualActive && (
        <div className="df2-dest-manual-types">
          <span className="df2-dest-manual-types-label">Engine</span>
          <div className="df2-dest-type-chips">
            {liveDestTypes.map((d) => (
              <button
                key={d.id}
                type="button"
                className={`df2-dest-type-chip${destType === d.id ? " active" : ""}`}
                onClick={() => onSelectType(d.id)}
              >
                <ConnectorIcon id={d.id} size={14} />
                {d.label}
              </button>
            ))}
          </div>
        </div>
      )}

      {connectors.length === 0 && (
        <p className="df2-label-hint df2-dest-picker-empty">
          No saved connectors yet — use Custom connection or add connectors in the Connectors page.
        </p>
      )}
    </div>
  );

}
