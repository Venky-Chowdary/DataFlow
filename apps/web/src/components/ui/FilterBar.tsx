import { ReactNode } from "react";

interface FilterBarProps {
  children: ReactNode;
  className?: string;
  /**
   * standalone — full oval pill (Transfer Studio, catalog, workbench).
   * inline — row of tab groups inside PageToolbar (toolbar adds outer pill).
   */
  variant?: "standalone" | "inline";
  ariaLabel?: string;
}

/** Shared oval filter shell — same visual language as Job Theater toolbar filters. */
export function FilterBar({
  children,
  className = "",
  variant = "standalone",
  ariaLabel = "Filters",
}: FilterBarProps) {
  if (variant === "inline") {
    return (
      <div className={`df2-filter-bar-inline ${className}`.trim()} role="group" aria-label={ariaLabel}>
        {children}
      </div>
    );
  }

  return (
    <div className={`df2-filter-bar ${className}`.trim()} role="group" aria-label={ariaLabel}>
      {children}
    </div>
  );
}
