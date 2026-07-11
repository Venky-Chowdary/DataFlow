import { ReactNode } from "react";
import { DtIcon } from "./DtIcon";

interface EmptyStateProps {
  icon?: string;
  title: string;
  description?: string;
  action?: ReactNode;
  compact?: boolean;
}

/** Standard empty state — icon, title, description, optional CTA */
export function EmptyState({ icon = "activity", title, description, action, compact }: EmptyStateProps) {
  return (
    <div
      className={`df2-empty ${compact ? "df2-empty-compact" : ""}`}
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
