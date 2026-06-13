import type { ReactNode } from "react";
import { Button } from "./Button";

export interface DestinationOption {
  id: string;
  label: string;
  type: "saved" | "engine";
  engine?: string;
  status?: "live" | "beta" | "planned";
}

interface DestinationPickerProps {
  label: string;
  options: DestinationOption[];
  value: string;
  onChange: (id: string) => void;
  accent?: "orange" | "mint";
}

export function DestinationPicker({
  label,
  options,
  value,
  onChange,
  accent = "mint",
}: DestinationPickerProps) {
  const saved = options.filter((o) => o.type === "saved");
  const engines = options.filter((o) => o.type === "engine");

  return (
    <div className={`df-dest-picker df-dest-picker--${accent}`}>
      <span className="df-label">{label}</span>
      {saved.length > 0 && (
        <div className="df-dest-picker-group">
          <span className="df-dest-picker-group-label">Saved connectors</span>
          <div className="df-dest-picker-options">
            {saved.map((o) => (
              <button
                key={o.id}
                type="button"
                className={["df-dest-option", value === o.id ? "df-dest-option--active" : ""].filter(Boolean).join(" ")}
                onClick={() => onChange(o.id)}
              >
                {o.label}
              </button>
            ))}
          </div>
        </div>
      )}
      <div className="df-dest-picker-group">
        <span className="df-dest-picker-group-label">Database engine</span>
        <div className="df-dest-picker-options">
          {engines.map((o) => (
            <button
              key={o.id}
              type="button"
              className={["df-dest-option", value === o.id ? "df-dest-option--active" : ""].filter(Boolean).join(" ")}
              onClick={() => onChange(o.id)}
            >
              {o.label}
              {o.status === "live" && <span className="df-dest-option-badge">Live</span>}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

interface SourceTypePickerProps {
  value: string;
  onChange: (id: string) => void;
  options: { id: string; label: string }[];
}

export function SourceTypePicker({ value, onChange, options }: SourceTypePickerProps) {
  return (
    <div className="df-source-type-picker">
      <span className="df-label">Source database</span>
      <div className="df-dest-picker-options">
        {options.map((o) => (
          <button
            key={o.id}
            type="button"
            className={["df-dest-option", value === o.id ? "df-dest-option--active" : ""].filter(Boolean).join(" ")}
            onClick={() => onChange(o.id)}
          >
            {o.label}
          </button>
        ))}
      </div>
    </div>
  );
}

interface TransferSelectLayoutProps {
  templates: { id: string; label: string }[];
  activeTemplateId: string;
  onTemplateChange: (id: string) => void;
  sourcePanel: ReactNode;
  destinationPanel: ReactNode;
  onContinue: () => void;
  continueDisabled?: boolean;
}

export function TransferSelectLayout({
  templates,
  activeTemplateId,
  onTemplateChange,
  sourcePanel,
  destinationPanel,
  onContinue,
  continueDisabled,
}: TransferSelectLayoutProps) {
  return (
    <div className="df-transfer-select">
      <div className="df-transfer-select-templates" role="tablist">
        {templates.map((t) => (
          <button
            key={t.id}
            type="button"
            role="tab"
            aria-selected={activeTemplateId === t.id}
            className={["df-transfer-select-tab", activeTemplateId === t.id ? "df-transfer-select-tab--active" : ""]
              .filter(Boolean)
              .join(" ")}
            onClick={() => onTemplateChange(t.id)}
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="df-transfer-select-panels">
        <section className="df-transfer-select-panel">
          <header className="df-transfer-select-panel-head">Source</header>
          {sourcePanel}
        </section>
        <div className="df-transfer-select-arrow" aria-hidden>
          →
        </div>
        <section className="df-transfer-select-panel">
          <header className="df-transfer-select-panel-head">Destination</header>
          {destinationPanel}
        </section>
      </div>

      <footer className="df-transfer-select-footer">
        <Button variant="primary" disabled={continueDisabled} onClick={onContinue}>
          Continue
        </Button>
      </footer>
    </div>
  );
}
