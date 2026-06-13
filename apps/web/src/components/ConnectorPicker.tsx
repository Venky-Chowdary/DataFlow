import { ConnectorIcon } from "../app/brand-icons";
import { DtIcon } from "./DtIcon";
import { Connector } from "../lib/types";

interface ConnectorPickerProps {
  connectors: Connector[];
  value: string;
  onChange: (id: string) => void;
  filterType?: string;
  label?: string;
  onConfigure?: () => void;
  emptyMessage?: string;
}

export function ConnectorPicker({
  connectors,
  value,
  onChange,
  filterType,
  label = "Select connector",
  onConfigure,
  emptyMessage = "No saved connectors yet.",
}: ConnectorPickerProps) {
  const list = filterType ? connectors.filter((c) => c.type === filterType) : connectors;

  if (list.length === 0) {
    return (
      <div className="dt-connector-picker-empty">
        <DtIcon name="connectors" size={28} />
        <p>{emptyMessage}</p>
        {onConfigure && (
          <button type="button" className="dt-btn dt-btn-primary dt-btn-sm" onClick={onConfigure}>
            <DtIcon name="plus" size={16} /> Configure in Connectors
          </button>
        )}
      </div>
    );
  }

  return (
    <div className="dt-connector-picker">
      {label && <div className="dt-label dt-mb-2">{label}</div>}
      <div className="dt-connector-picker-grid">
        {list.map((c) => (
          <button
            key={c.id}
            type="button"
            className={`dt-connector-picker-card ${value === c.id ? "selected" : ""}`}
            onClick={() => onChange(c.id)}
          >
            <div className="dt-connector-picker-icon">
              <ConnectorIcon id={c.type} size={32} />
            </div>
            <div className="dt-connector-picker-body">
              <span className="dt-connector-picker-name">{c.name}</span>
              <span className="dt-connector-picker-meta">{c.type} · {c.host}:{c.port}</span>
              {c.database && <span className="dt-connector-picker-meta">{c.database}</span>}
            </div>
            {value === c.id && <span className="dt-connector-picker-check"><DtIcon name="check" size={16} /></span>}
          </button>
        ))}
      </div>
      {onConfigure && (
        <button type="button" className="dt-btn dt-btn-ghost dt-btn-sm dt-mt-3" onClick={onConfigure}>
          <DtIcon name="plus" size={16} /> Add or manage connectors
        </button>
      )}
    </div>
  );
}
