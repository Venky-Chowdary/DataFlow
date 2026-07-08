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
      className={`dt-empty ${compact ? "dt-empty-compact" : ""}`}
      role="status"
      aria-label={title}
    >
      <div className="dt-empty-icon">
        <DtIcon name={icon} size={compact ? 22 : 28} />
      </div>
      <h3 className="dt-empty-title">{title}</h3>
      {description && <p className="dt-empty-text">{description}</p>}
      {action}
    </div>
  );
}
