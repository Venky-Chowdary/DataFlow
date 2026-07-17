import { ReactNode } from "react";

export interface FilterTabItem<T extends string = string> {
  id: T;
  label: string;
  count?: number;
  icon?: ReactNode;
}

interface FilterTabsProps<T extends string = string> {
  items: FilterTabItem<T>[];
  value: T;
  onChange: (id: T) => void;
  className?: string;
  ariaLabel?: string;
  disabled?: boolean;
}

/** Consistent segmented tab control used on Connectors, Jobs, Settings, etc. */
export function FilterTabs<T extends string = string>({
  items,
  value,
  onChange,
  className = "",
  ariaLabel = "Filter",
  disabled = false,
}: FilterTabsProps<T>) {
  return (
    <div
      className={`df2-tabs df2-filter-tabs ${disabled ? "is-disabled" : ""} ${className}`.trim()}
      role="tablist"
      aria-label={ariaLabel}
      aria-disabled={disabled || undefined}
    >
      {items.map((item) => (
        <button
          key={item.id}
          type="button"
          role="tab"
          aria-selected={value === item.id}
          className={`df2-tab ${value === item.id ? "active" : ""}`}
          disabled={disabled}
          onClick={() => !disabled && onChange(item.id)}
        >
          {item.icon}
          {item.label}
          {item.count != null && ` (${item.count})`}
        </button>
      ))}
    </div>
  );
}
