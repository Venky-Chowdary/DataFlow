import { ReactNode } from "react";
import { DtIcon } from "./DtIcon";

interface EmptyStateProps {
  icon?: string;
  title: string;
  description?: string;
  action?: ReactNode;
  compact?: boolean;
  /** Center in the page viewport — Job Theater, Connectors zero-state */
  page?: boolean;
}

/** Standard empty state — icon, title, description, optional CTA */
export function EmptyState({ icon = "activity", title, description, action, compact, page }: EmptyStateProps) {
  return (
    <div
      className={[
        "df2-empty",
        compact ? "df2-empty-compact" : "",
        page ? "df2-empty-page" : "",
      ].filter(Boolean).join(" ")}
      role="status"
      aria-label={title}
    >
      <div className="df2-empty-icon" aria-hidden>
        <DtIcon name={icon} size={compact ? 22 : 28} />
      </div>
      <h3 className="df2-empty-title">{title}</h3>
      {description && <p className="df2-empty-desc">{description}</p>}
      {action && <div className="df2-empty-action">{action}</div>}
    </div>
  );
}
