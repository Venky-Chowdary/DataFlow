import { ReactNode } from "react";

type InsightTone = "ok" | "live" | "warn" | "info";

interface PageInsightStripProps {
  tone?: InsightTone;
  pill: string;
  message: string;
  actions?: ReactNode;
  className?: string;
}

/** Status strip below page header — operational context at a glance */
export function PageInsightStrip({
  tone = "ok",
  pill,
  message,
  actions,
  className = "",
}: PageInsightStripProps) {
  return (
    <div className={`df2-page-insight ${className}`.trim()} role="status">
      <span className={`df2-page-insight-pill df2-page-insight-pill--${tone}`}>{pill}</span>
      <p className="df2-page-insight-message"> {message}</p>
      {actions && <div className="df2-page-insight-actions">{actions}</div>}
    </div>
  );
}
