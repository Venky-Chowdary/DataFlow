import type { ReactNode } from "react";

interface PageHeadProps {
  title?: string;
  description?: string;
  action?: ReactNode;
}

export function PageHead({ description, action }: PageHeadProps) {
  if (!description && !action) return null;

  return (
    <header className="df-page-head">
      <div className="df-page-head-row">
        {description && <p className="df-page-desc">{description}</p>}
        {action}
      </div>
    </header>
  );
}
