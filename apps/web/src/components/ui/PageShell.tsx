import { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

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
  /** When false, rely on topbar breadcrumb (e.g. Transfer Studio chrome) */
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
  return (
    <div
      className={[
        "df2-page",
        "df2-page-fluid",
        fit ? "df2-page-fit" : "",
        !showHeader ? "df2-page-no-header" : "",
        className ?? "",
      ]
        .filter(Boolean)
        .join(" ")}
    >
      {showHeader && (
        <header className="df2-page-head df2-page-head-enterprise">
          <div className="df2-page-copy">
            {kicker && (
              <span className="df2-page-kicker">
                <DtIcon name="sparkle" size={12} />
                {kicker}
              </span>
            )}
            <h1 className="df2-page-title">{title}</h1>
            {description && <p className="df2-page-desc">{description}</p>}
          </div>
          {actions && <div className="df2-page-actions">{actions}</div>}
        </header>
      )}
      {fit ? <div className="df2-page-body">{children}</div> : children}
    </div>
  );
}
