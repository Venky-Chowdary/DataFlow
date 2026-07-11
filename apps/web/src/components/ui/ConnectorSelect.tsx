import { ConnectorIcon } from "../../app/brand-icons";
import { Connector } from "../../lib/types";

interface ConnectorSelectProps {
  id?: string;
  label: string;
  value: string;
  onChange: (id: string) => void;
  connectors: Connector[];
  placeholder?: string;
  required?: boolean;
  disabled?: boolean;
  hint?: string;
}

export function ConnectorSelect({
  id,
  label,
  value,
  onChange,
  connectors,
  placeholder = "Select connector…",
  required,
  disabled,
  hint,
}: ConnectorSelectProps) {
  const selected = connectors.find((c) => c.id === value);

  return (
    <div className="df2-field">
      <label className="df2-label" htmlFor={id}>{label}</label>
      <div className="df2-connector-select-row">
        <span className="df2-connector-select-icon" aria-hidden>
          {selected ? (
            <ConnectorIcon id={selected.type} size={22} />
          ) : (
            <span className="df2-connector-select-placeholder-icon" />
          )}
        </span>
        <select
          id={id}
          className="df2-input df2-select df2-connector-select"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          required={required}
          disabled={disabled || connectors.length === 0}
        >
          <option value="">{connectors.length === 0 ? "No connectors available" : placeholder}</option>
          {connectors.map((c) => (
            <option key={c.id} value={c.id}>
              {c.name} — {c.type}{c.last_test_ok === false ? " (untested)" : ""}
            </option>
          ))}
        </select>
      </div>
      {hint && <p className="df2-label-hint">{hint}</p>}
    </div>
  );
}
