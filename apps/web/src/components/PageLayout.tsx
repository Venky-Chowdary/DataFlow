import { ReactNode } from "react";

export type PageWidth = "default" | "wide" | "full" | "fluid";

interface PageLayoutProps {
  children: ReactNode;
  width?: PageWidth;
  flush?: boolean;
  /** Fill main pane height — scroll only inside .dt-page-body panels */
  viewport?: boolean;
  className?: string;
}

/** Unified middle-pane shell — responsive padding, scroll, vertical rhythm */
export function PageLayout({
  children,
  width = "default",
  flush = false,
  viewport = false,
  className = "",
}: PageLayoutProps) {
  return (
    <div
      className={`dt-page dt-page-enter ${flush ? "dt-page-flush" : ""} ${viewport ? "dt-page-viewport" : ""} ${className}`.trim()}
      data-width={width}
    >
      <div
        className={`dt-page-stack dt-page-inner dt-stagger dt-page-inner--${width} ${viewport ? "dt-page-stack-viewport" : ""}`.trim()}
      >
        {children}
      </div>
    </div>
  );
}
