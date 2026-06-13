import type { ReactNode } from "react";

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  eyebrow?: string;
  action?: ReactNode;
  compact?: boolean;
}

export function PageHeader({ title, subtitle, eyebrow, action, compact }: PageHeaderProps) {
  return (
    <header
      className={["df-page-header", compact ? "df-page-header--compact" : ""].filter(Boolean).join(" ")}
    >
      <div className="df-page-header-row">
        <div>
          {eyebrow && <p className="df-page-eyebrow">{eyebrow}</p>}
          <h1 className="df-display">{title}</h1>
          {subtitle && <p className="df-subtitle">{subtitle}</p>}
        </div>
        {action}
      </div>
    </header>
  );
}
