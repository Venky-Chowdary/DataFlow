import { ConnectorIcon } from "../../app/brand-icons";
import { Connector } from "../../lib/types";
import { connectorDisplayName } from "../../lib/connectorTypes";

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
          title={selected ? connectorOptionLabel(selected) : undefined}
        >
          <option value="">{connectors.length === 0 ? "No connectors available" : placeholder}</option>
          {connectors.map((c) => (
            <option key={c.id} value={c.id} title={connectorOptionLabel(c)}>
              {connectorOptionLabel(c)}
            </option>
          ))}
        </select>
      </div>
      {hint && <p className="df2-label-hint">{hint}</p>}
    </div>
  );
}

/**
 * UX-40: the connection's name first, then its engine's own name, then a test
 * warning only when there is one, so a narrow picker keeps what tells
 * connections apart. The full text is the option's and the select's title.
 */
export function connectorOptionLabel(c: Pick<Connector, "name" | "type" | "last_test_ok">): string {
  const status = c.last_test_ok === false ? " · test failed" : c.last_test_ok == null ? " · not tested" : "";
  return `${c.name} (${connectorDisplayName(c.type)})${status}`;
}
