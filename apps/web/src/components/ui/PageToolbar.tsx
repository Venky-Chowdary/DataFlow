import { ReactNode } from "react";
import { DtIcon } from "../DtIcon";

interface PageToolbarProps {
  searchValue?: string;
  onSearchChange?: (value: string) => void;
  searchPlaceholder?: string;
  actions?: ReactNode;
  className?: string;
}

/** Search + action bar — shared layout for list pages */
export function PageToolbar({
  searchValue,
  onSearchChange,
  searchPlaceholder = "Search…",
  actions,
  className = "",
}: PageToolbarProps) {
  const showSearch = onSearchChange != null;

  return (
    <div className={`df2-page-toolbar ${className}`.trim()}>
      {showSearch && (
        <label className="df2-command-search df2-page-toolbar-search" aria-label="Search">
          <DtIcon name="search" size={15} />
          <input
            type="search"
            placeholder={searchPlaceholder}
            value={searchValue ?? ""}
            onChange={(e) => onSearchChange(e.target.value)}
          />
          {searchValue && (
            <button
              type="button"
              className="df2-page-toolbar-clear"
              onClick={() => onSearchChange("")}
              aria-label="Clear search"
            >
              ×
            </button>
          )}
        </label>
      )}
      {actions && <div className="df2-page-toolbar-actions">{actions}</div>}
    </div>
  );
}
