import { ReactNode } from "react";

interface PageHeaderProps {
  title: string;
  subtitle?: string;
  actions?: ReactNode;
  badge?: ReactNode;
  eyebrow?: string;
}

export function PageHeader({ title, subtitle, actions, badge, eyebrow }: PageHeaderProps) {
  return (
    <header className="dt-page-header">
      <div className="dt-page-header-row">
        <div className="dt-page-header-main">
          {eyebrow && <p className="dt-page-eyebrow">{eyebrow}</p>}
          <div className="dt-page-title-row">
            <h1 className="dt-page-title">{title}</h1>
            {badge}
          </div>
          {subtitle && <p className="dt-page-subtitle">{subtitle}</p>}
        </div>
        {actions && <div className="dt-page-actions">{actions}</div>}
      </div>
    </header>
  );
}
