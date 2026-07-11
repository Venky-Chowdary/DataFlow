import { ReactNode } from "react";

interface PageSectionProps {
  title?: string;
  subtitle?: string;
  actions?: ReactNode;
  children: ReactNode;
  className?: string;
  asCard?: boolean;
  elevated?: boolean;
}

/** Consistent page section — optional card wrapper with aligned header */
export function PageSection({
  title,
  subtitle,
  actions,
  children,
  className = "",
  asCard = true,
  elevated = false,
}: PageSectionProps) {
  const body = (
    <>
      {(title || actions) && (
        <header className="df2-card-head df2-section-head">
          <div className="df2-section-head-copy">
            {title && <h2 className="df2-card-title">{title}</h2>}
            {subtitle && <p className="df2-card-sub">{subtitle}</p>}
          </div>
          {actions && <div className="df2-section-head-actions">{actions}</div>}
        </header>
      )}
      <div className={title ? "df2-card-body" : undefined}>{children}</div>
    </>
  );

  if (!asCard) {
    return (
      <section className={`df2-page-section ${className}`.trim()}>
        {body}
      </section>
    );
  }

  return (
    <section
      className={`df2-card df2-page-section-card ${elevated ? "df2-card-elevated" : ""} ${className}`.trim()}
    >
      {body}
    </section>
  );
}
