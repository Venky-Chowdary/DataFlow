/**
 * Page chrome with visible title — enterprise ops pages need identity in-content,
 * not only in the topbar breadcrumb.
 */
import { ReactNode } from "react";

interface PageShellProps {
  title: string;
  description?: string;
  kicker?: string;
  actions?: ReactNode;
  className?: string;
  /** @deprecated All pages are full-width; kept for API compatibility */
  wide?: boolean;
  /** Fit content in one viewport — scroll only inside .df2-scroll-pane regions */
  fit?: boolean;
  /**
   * When false, no header chrome (Transfer Studio / Pilot immersive).
   * When true, show a compact page header with title + optional description.
   */
  showHeader?: boolean;
  children: ReactNode;
}

export function PageShell({
  title,
  description,
  kicker,
  actions,
  className,
  fit,
  showHeader = true,
  children,
}: PageShellProps) {
  const hasActions = Boolean(actions);

  return (
    <div
      className={[
        "df2-page",
        "df2-page-fluid",
        "df2-page-compact",
        "df2-page-titled",
        fit ? "df2-page-fit" : "",
        !showHeader ? "df2-page-no-header" : "",
        !hasActions ? "df2-page-no-actions" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {showHeader ? (
        <header className="df2-page-header">
          <div className="df2-page-header-text">
            {kicker ? <span className="df2-page-kicker">{kicker}</span> : null}
            <h1 className="df2-page-title" id="df2-page-title">{title}</h1>
            {description ? (
              <p className="df2-page-description" id="df2-page-description">{description}</p>
            ) : null}
          </div>
          {hasActions ? (
            <div className="df2-page-actions" aria-label={`${title} actions`}>
              {actions}
            </div>
          ) : null}
        </header>
      ) : (
        <h1 className="df2-sr-only" id="df2-page-title">{title}</h1>
      )}

      {fit ? <div className="df2-page-body">{children}</div> : children}
    </div>
  );
}
