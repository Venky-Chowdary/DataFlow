import { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

export type ContextStatTone = "default" | "ok" | "warn" | "danger" | "muted";

export interface ContextStat {
  label: string;
  value: string | number;
  icon?: string;
  tone?: ContextStatTone;
  title?: string;
}

interface PageContextBarProps {
  /** High-signal, precisely formatted KPIs shown where the page title used to be. */
  stats: ContextStat[];
  /** Optional right-aligned primary/secondary actions. */
  actions?: ReactNode;
  className?: string;
  ariaLabel?: string;
}

/**
 * Reclaims the space freed by removing the duplicated page title with a slim,
 * enterprise summary strip. Reused across workspace pages for one visual language.
 */
export function PageContextBar({
  stats,
  actions,
  className = "",
  ariaLabel = "Page summary",
}: PageContextBarProps) {
  return (
    <div className={`df2-context-bar ${className}`.trim()} role="group" aria-label={ariaLabel}>
      <div className="df2-context-bar-stats">
        {stats.map((s) => (
          <div
            key={s.label}
            className={`df2-context-stat df2-context-stat--${s.tone ?? "default"}`}
            title={s.title}
          >
            {s.icon ? (
              <span className="df2-context-stat-icon" aria-hidden>
                <DtIcon name={s.icon} size={15} />
              </span>
            ) : null}
            <span className="df2-context-stat-body">
              <span className="df2-context-stat-value">{s.value}</span>
              <span className="df2-context-stat-label">{s.label}</span>
            </span>
          </div>
        ))}
      </div>
      {actions ? <div className="df2-context-bar-actions">{actions}</div> : null}
    </div>
  );
}
