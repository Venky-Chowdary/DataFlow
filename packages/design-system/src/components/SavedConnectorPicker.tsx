import { Button } from "./Button";

export interface SavedConnectorOption {
  id: string;
  name: string;
  type: string;
  role: string;
  last_test_ok?: boolean;
}

interface SavedConnectorPickerProps {
  label: string;
  hint?: string;
  connectors: SavedConnectorOption[];
  value: string;
  onChange: (connectorId: string) => void;
  onRefresh?: () => void;
  loading?: boolean;
  accent?: "orange" | "mint";
}

export function SavedConnectorPicker({
  label,
  hint,
  connectors,
  value,
  onChange,
  onRefresh,
  loading,
  accent = "orange",
}: SavedConnectorPickerProps) {
  return (
    <div className="df-saved-connector-picker">
      <div className="df-saved-connector-picker-head">
        <span className="df-label">{label}</span>
        {onRefresh && (
          <Button variant="ghost" onClick={onRefresh} disabled={loading}>
            Refresh
          </Button>
        )}
      </div>
      {hint && <p className="df-field-hint">{hint}</p>}
      <select
        className={`df-select df-saved-connector-select df-saved-connector-select--${accent}`}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={loading}
      >
        <option value="">Paste connection string or pick saved connector…</option>
        {connectors.map((c) => (
          <option key={c.id} value={c.id}>
            {c.name} ({c.type})
            {c.last_test_ok ? " · verified" : ""}
          </option>
        ))}
      </select>
    </div>
  );
}
