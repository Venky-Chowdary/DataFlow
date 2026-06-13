import type { ReactNode } from "react";

interface ConnectionCardProps {
  title: string;
  subtitle: string;
  connected: boolean;
  onTest?: () => void;
  children?: ReactNode;
}

export function ConnectionCard({ title, subtitle, connected, onTest, children }: ConnectionCardProps) {
  return (
    <div className="df-card df-connection-card">
      <div className="df-card-header">
        <div>
          <div className="df-label">{title}</div>
          <div className="df-card-title">{subtitle}</div>
        </div>
        <span
          className={[
            "df-status-dot",
            connected ? "df-status-dot--connected" : "df-status-dot--disconnected",
          ].join(" ")}
        >
          {connected ? "Connected" : "Offline"}
        </span>
      </div>
      {children}
      {onTest && (
        <button type="button" className="df-btn df-btn-secondary" onClick={onTest} style={{ marginTop: 20 }}>
          Test connection
        </button>
      )}
    </div>
  );
}
