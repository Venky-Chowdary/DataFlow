/**
 * Page chrome — the visible page name lives in the topbar breadcrumb
 * (`Workspace > {page}`), so this shell intentionally does NOT render a
 * duplicated in-content title. The reclaimed vertical space is handed back
 * to each page for high-signal content (KPIs, status, context, actions).
 *
 * An `sr-only` <h1> is always emitted so the document keeps one accessible,
 * SEO-friendly page heading. When a page supplies `actions`, they render in a
 * slim right-aligned action bar (no title beside them).
 */
import { ReactNode } from "react";

interface PageShellProps {
  /** Accessible page name — rendered sr-only (mirrors the topbar breadcrumb). */
  title: string;
  /** Accessible page summary — rendered sr-only for context/SEO. */
  description?: string;
  /** @deprecated The breadcrumb owns page identity; kicker is no longer shown. */
  kicker?: string;
  actions?: ReactNode;
  className?: string;
  /** @deprecated All pages are full-width; kept for API compatibility */
  wide?: boolean;
  /** Fit content in one viewport — scroll only inside .df2-scroll-pane regions */
  fit?: boolean;
  /**
   * When false, no chrome at all (Transfer Studio / Pilot immersive) — only the
   * sr-only heading is kept. When true, a slim action bar renders if actions exist.
   */
  showHeader?: boolean;
  children: ReactNode;
}

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
        "df2-page-untitled",
        fit ? "df2-page-fit" : "",
        !showHeader ? "df2-page-no-header" : "",
        !hasActions ? "df2-page-no-actions" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      <h1 className="df2-sr-only" id="df2-page-title">{title}</h1>
      {description ? (
        <p className="df2-sr-only" id="df2-page-description">{description}</p>
      ) : null}

      {showHeader && hasActions ? (
        <div className="df2-page-actionbar">
          <div className="df2-page-actions" aria-label={`${title} actions`}>
            {actions}
          </div>
        </div>
      ) : null}

      {fit ? <div className="df2-page-body">{children}</div> : children}
    </div>
  );
}
