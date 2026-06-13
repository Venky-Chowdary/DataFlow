import type { ReactNode } from "react";
import { Button } from "./Button";
import { DatabaseIcon, FileIcon, ApiIcon } from "./DatabaseIcons";

export interface DestinationOption {
  id: string;
  label: string;
  type: "saved" | "engine";
  engine?: string;
  status?: "live" | "beta" | "planned";
}

export interface TemplateOption {
  id: string;
  label: string;
  icon: ReactNode;
  description?: string;
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

const TEMPLATE_ICONS: Record<string, ReactNode> = {
  "file-db": <FileIcon size={24} />,
  "db-db": <DatabaseIcon type="postgresql" size={24} />,
  "db-file": <DatabaseIcon type="postgresql" size={24} />,
  "api-db": <ApiIcon size={24} />,
  "file-file": <FileIcon size={24} />,
};

const TEMPLATE_DESCRIPTIONS: Record<string, string> = {
  "file-db": "Upload CSV, Excel, JSON, or any file to your database",
  "db-db": "Migrate data between any two databases",
  "db-file": "Export database tables to CSV, Excel, or JSON",
  "api-db": "Import data from REST APIs to your warehouse",
  "file-file": "Convert between file formats",
};

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
      <div className="df-transfer-select-hero">
        <div className="df-transfer-select-hero-badge">Choose transfer type</div>
        <div className="df-transfer-select-templates" role="tablist">
          {templates.map((t) => (
            <button
              key={t.id}
              type="button"
              role="tab"
              aria-selected={activeTemplateId === t.id}
              className={["df-template-card", activeTemplateId === t.id ? "df-template-card--active" : ""]
                .filter(Boolean)
                .join(" ")}
              onClick={() => onTemplateChange(t.id)}
            >
              <span className="df-template-card-icon">
                {TEMPLATE_ICONS[t.id] ?? <FileIcon size={24} />}
              </span>
              <span className="df-template-card-content">
                <span className="df-template-card-label">{t.label}</span>
                <span className="df-template-card-desc">
                  {TEMPLATE_DESCRIPTIONS[t.id] ?? "Transfer data"}
                </span>
              </span>
              {activeTemplateId === t.id && (
                <span className="df-template-card-check">✓</span>
              )}
            </button>
          ))}
        </div>
      </div>

      <div className="df-transfer-select-panels">
        <section className="df-transfer-select-panel df-transfer-select-panel--source">
          <header className="df-transfer-select-panel-head">
            <span className="df-transfer-select-panel-tag">Source</span>
            <span className="df-transfer-select-panel-hint">Where's your data coming from?</span>
          </header>
          <div className="df-transfer-select-panel-body">
            {sourcePanel}
          </div>
        </section>
        
        <div className="df-transfer-select-bridge" aria-hidden>
          <div className="df-transfer-select-bridge-line" />
          <div className="df-transfer-select-bridge-icon">
            <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
              <path d="M4 10H16M16 10L12 6M16 10L12 14" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
            </svg>
          </div>
          <div className="df-transfer-select-bridge-line" />
        </div>
        
        <section className="df-transfer-select-panel df-transfer-select-panel--dest">
          <header className="df-transfer-select-panel-head">
            <span className="df-transfer-select-panel-tag">Destination</span>
            <span className="df-transfer-select-panel-hint">Where should data go?</span>
          </header>
          <div className="df-transfer-select-panel-body">
            {destinationPanel}
          </div>
        </section>
      </div>

      <footer className="df-transfer-select-footer">
        <Button variant="primary" disabled={continueDisabled} onClick={onContinue}>
          Continue to mapping
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M6 12L10 8L6 4" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </Button>
      </footer>
    </div>
  );
}
