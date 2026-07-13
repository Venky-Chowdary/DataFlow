import { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

interface PageToolbarProps {
  searchValue?: string;
  onSearchChange?: (value: string) => void;
  searchPlaceholder?: string;
  /** Center zone — FilterTabs or other segmented controls */
  filters?: ReactNode;
  /** Right zone — primary / secondary actions */
  actions?: ReactNode;
  className?: string;
}

/**
 * Enterprise list toolbar — three balanced zones in ONE row:
 * search (left) · filters (center) · actions (right)
 */
export function PageToolbar({
  searchValue,
  onSearchChange,
  searchPlaceholder = "Search…",
  filters,
  actions,
  className = "",
}: PageToolbarProps) {
  const showSearch = onSearchChange != null;

  return (
    <div
      className={`df2-toolbar ${className}`.trim()}
      role="toolbar"
      aria-label="Page toolbar"
    >
      <div className="df2-toolbar-start">
        {showSearch ? (
          <label className="df2-toolbar-search" aria-label="Search">
            <DtIcon name="search" size={15} />
            <input
              type="search"
              placeholder={searchPlaceholder}
              value={searchValue ?? ""}
              onChange={(e) => onSearchChange(e.target.value)}
            />
            {searchValue ? (
              <button
                type="button"
                className="df2-toolbar-clear"
                onClick={() => onSearchChange("")}
                aria-label="Clear search"
              >
                ×
              </button>
            ) : null}
          </label>
        ) : null}
      </div>

      <div className="df2-toolbar-center">
        {filters ? <div className="df2-toolbar-filters">{filters}</div> : null}
      </div>

      <div className="df2-toolbar-end">{actions}</div>
    </div>
  );
}
