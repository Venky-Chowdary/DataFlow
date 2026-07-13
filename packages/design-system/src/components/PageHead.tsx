import type { ReactNode } from "react";

interface PageHeadProps {
  title?: string;
  description?: string;
  action?: ReactNode;
}

export function PageHead({ title, description, action }: PageHeadProps) {
  if (!title && !description && !action) return null;

  return (
    <header className="df-page-head" aria-label={title ? `${title} page header` : "Page header"}>
      <div className="df-page-head-row">
        <div className="df-page-head-text">
          {title && <h1 className="df-page-title">{title}</h1>}
          {description && <p className="df-page-desc">{description}</p>}
        </div>
        {action}
      </div>
    </header>
  );
}
