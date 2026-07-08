import { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

interface PageShellProps {
  title: string;
  description?: string;
  actions?: ReactNode;
  wide?: boolean;
  children: ReactNode;
}

const PAGE_SIGNALS: Record<string, { label: string; value: string; icon: string }[]> = {
  Overview: [
    { label: "Data plane", value: "Live topology", icon: "activity" },
    { label: "Quality loop", value: "Reconciled", icon: "check" },
    { label: "Risk posture", value: "Fail-closed", icon: "shield" },
  ],
  "Transfer Studio": [
    { label: "Schema", value: "Contracted", icon: "gate" },
    { label: "Mapping", value: "Confidence gated", icon: "sparkle" },
    { label: "Execution", value: "Preflight first", icon: "shield" },
  ],
  Connectors: [
    { label: "Catalog", value: "623+ entries", icon: "connectors" },
    { label: "Drivers", value: "Live certified", icon: "check" },
    { label: "Secrets", value: "Vaulted", icon: "key" },
  ],
  "Scheduled pipelines": [
    { label: "Cadence", value: "Policy aware", icon: "clock" },
    { label: "State", value: "Retryable", icon: "activity" },
    { label: "Guardrails", value: "Preflight", icon: "gate" },
  ],
  "Job Theater": [
    { label: "Runtime", value: "Streaming", icon: "activity" },
    { label: "Batches", value: "Checkpointed", icon: "database" },
    { label: "Proof", value: "Row + checksum", icon: "check" },
  ],
  "MCP Server": [
    { label: "Agents", value: "Tool-backed", icon: "sparkle" },
    { label: "Surface", value: "Cursor + Claude", icon: "zap" },
    { label: "Access", value: "Governed", icon: "shield" },
  ],
  Settings: [
    { label: "Security", value: "Enterprise", icon: "shield" },
    { label: "Audit", value: "Always on", icon: "activity" },
    { label: "Access", value: "Role-ready", icon: "key" },
  ],
};

export function PageShell({ title, description, actions, wide, children }: PageShellProps) {
  const signals = PAGE_SIGNALS[title] ?? [
    { label: "Control", value: "Ready", icon: "check" },
    { label: "Schema", value: "Protected", icon: "shield" },
    { label: "Agent", value: "Available", icon: "sparkle" },
  ];

  return (
    <div className={`df2-page ${wide ? "df2-page-wide" : ""}`}>
      <header className="df2-page-head df2-page-hero">
        <div className="df2-page-copy">
          <span className="df2-page-kicker">
            <DtIcon name="sparkle" size={14} />
            DataFlow command center
          </span>
          <h1 className="df2-page-title">{title}</h1>
          {description && <p className="df2-page-desc">{description}</p>}
          <div className="df2-page-signals" aria-label={`${title} operating signals`}>
            {signals.map((signal) => (
              <div key={signal.label} className="df2-page-signal">
                <DtIcon name={signal.icon} size={15} />
                <span>{signal.label}</span>
                <strong>{signal.value}</strong>
              </div>
            ))}
          </div>
        </div>
        {actions && <div className="df2-page-actions">{actions}</div>}
      </header>
      {children}
    </div>
  );
}
