import type { ReactNode } from "react";
import { IconConnector, IconHome, IconJobs, IconTransfer } from "../icons";

export type NavItemId = "home" | "transfer" | "connectors" | "jobs";

export interface NavItem {
  id: NavItemId;
  label: string;
}

interface EnterpriseNavProps {
  active: NavItemId;
  onNavigate: (id: NavItemId) => void;
  variant?: "top" | "sidebar";
  onNewTransfer?: () => void;
}

const NAV_ITEMS: NavItem[] = [
  { id: "home", label: "Home" },
  { id: "transfer", label: "Transfer" },
  { id: "jobs", label: "Operations" },
  { id: "connectors", label: "Connectors" },
];

const NAV_ICONS: Record<NavItemId, ReactNode> = {
  home: <IconHome />,
  transfer: <IconTransfer />,
  connectors: <IconConnector />,
  jobs: <IconJobs />,
};

export function EnterpriseNav({ active, onNavigate, variant = "top" }: EnterpriseNavProps) {
  const isTop = variant === "top";

  return (
    <nav
      className={["df-nav", isTop ? "df-nav--top" : ""].filter(Boolean).join(" ")}
      aria-label="Platform navigation"
    >
      <ul className="df-nav-list">
        {NAV_ITEMS.map((item) => (
          <li key={item.id}>
            <button
              type="button"
              className={["df-nav-item", active === item.id ? "df-nav-item--active" : ""].filter(Boolean).join(" ")}
              onClick={() => onNavigate(item.id)}
              aria-current={active === item.id ? "page" : undefined}
            >
              {!isTop && (
                <span className="df-nav-icon" aria-hidden>
                  {NAV_ICONS[item.id]}
                </span>
              )}
              {item.label}
            </button>
          </li>
        ))}
      </ul>
    </nav>
  );
}
