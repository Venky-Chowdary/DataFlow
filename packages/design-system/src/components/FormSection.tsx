import type { ReactNode } from "react";
import { LoadingState } from "./LoadingState";

interface FormSectionProps {
  title: string;
  subtitle?: string;
  connected?: boolean;
  loading?: boolean;
  loadingLabel?: string;
  onTest?: () => void;
  children?: ReactNode;
}

export function FormSection({
  title,
  subtitle,
  connected,
  loading = false,
  loadingLabel = "Working…",
  onTest,
  children,
}: FormSectionProps) {
  return (
    <div className="df-form-section">
      <div className="df-form-section-head">
        <div>
          <div className="df-form-section-title">{title}</div>
          {subtitle && <div className="df-form-section-sub">{subtitle}</div>}
        </div>
        <div className="df-form-section-head-actions">
          {connected !== undefined && !loading && (
            <span className={["df-status-pill", connected ? "df-status-pill--ok" : "df-status-pill--off"].join(" ")}>
              <span className="df-status-pill-dot" aria-hidden />
              {connected ? "Ready" : "Pending"}
            </span>
          )}
          {onTest && (
            <button type="button" className="df-btn df-btn-secondary df-btn-sm" onClick={onTest} disabled={loading}>
              {loading ? "Testing…" : "Test connection"}
            </button>
          )}
        </div>
      </div>
      <div className="df-form-section-body">
        {loading ? <LoadingState label={loadingLabel} compact /> : children}
      </div>
    </div>
  );
}
