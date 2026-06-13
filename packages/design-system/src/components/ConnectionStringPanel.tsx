interface ConnectionStringFieldProps {
  label: string;
  hint: string;
  value: string;
  onChange: (value: string) => void;
  dbTypeLabel?: string;
  status?: "idle" | "connecting" | "connected" | "error";
  statusMessage?: string;
  placeholder?: string;
  accent?: "orange" | "mint";
}

export function ConnectionStringField({
  label,
  hint,
  value,
  onChange,
  dbTypeLabel,
  status = "idle",
  statusMessage,
  placeholder = "postgresql://user:password@host:5432/database",
  accent = "orange",
}: ConnectionStringFieldProps) {
  return (
    <div className={["df-conn-field", accent === "mint" ? "df-conn-field--mint" : ""].filter(Boolean).join(" ")}>
      <div className="df-conn-field-head">
        <div>
          <div className="df-conn-field-label">{label}</div>
          <div className="df-conn-field-hint">{hint}</div>
        </div>
        {dbTypeLabel && <span className="df-conn-field-badge">{dbTypeLabel}</span>}
      </div>
      <textarea
        className="df-conn-field-input"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={3}
        spellCheck={false}
        autoComplete="off"
      />
      {status !== "idle" && (
        <div className={["df-conn-field-status", `df-conn-field-status--${status}`].join(" ")}>
          {status === "connecting" && <span className="df-conn-field-spinner" aria-hidden />}
          <span>{statusMessage}</span>
        </div>
      )}
    </div>
  );
}

interface DualConnectionPanelProps {
  source: ConnectionStringFieldProps;
  destination: ConnectionStringFieldProps;
}

export function DualConnectionPanel({ source, destination }: DualConnectionPanelProps) {
  return (
    <div className="df-dual-conn">
      <ConnectionStringField {...source} accent="orange" />
      <div className="df-dual-conn-arrow" aria-hidden>
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
          <path d="M5 12H17M17 12L13 8M17 12L13 16" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
        </svg>
      </div>
      <ConnectionStringField {...destination} accent="mint" />
    </div>
  );
}
