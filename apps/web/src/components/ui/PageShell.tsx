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
   * When false, no header chrome at all (Transfer Studio / Pilot).
   * When true, title stays screen-reader only — topbar breadcrumb is the visible page name.
   * Actions render in a slim bar so content gets the vertical space.
   */
  showHeader?: boolean;
  children: ReactNode;
}

/**
 * Enterprise page chrome: no duplicate H1/description under the topbar.
 * Page identity lives in the breadcrumb; this shell only hosts actions + content.
 */
export function PageShell({
  title,
  description,
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
        fit ? "df2-page-fit" : "",
        !showHeader ? "df2-page-no-header" : "",
        !hasActions ? "df2-page-no-actions" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {/* Always expose title for a11y / document outline; never visually duplicate topbar */}
      <h1 className="df2-sr-only" id="df2-page-title">{title}</h1>
      {description ? (
        <p className="df2-sr-only" id="df2-page-description">{description}</p>
      ) : null}

      {showHeader && hasActions && (
        <div className="df2-page-actionbar" aria-label={`${title} actions`}>
          <div className="df2-page-actions">{actions}</div>
        </div>
      )}

      {fit ? <div className="df2-page-body">{children}</div> : children}
    </div>
  );
}
