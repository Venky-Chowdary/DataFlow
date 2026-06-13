import type { ReactNode } from "react";
import { IconConnector, IconHome, IconJobs, IconTransfer } from "../icons";
import type { NavItemId } from "./EnterpriseNav";

interface NavPaneProps {
  active: NavItemId;
  onNavigate: (id: NavItemId) => void;
}

const ITEMS: { id: NavItemId; label: string; icon: ReactNode }[] = [
  { id: "home", label: "Home", icon: <IconHome size={20} /> },
  { id: "transfer", label: "Transfer", icon: <IconTransfer size={20} /> },
  { id: "jobs", label: "Operations", icon: <IconJobs size={20} /> },
  { id: "connectors", label: "Connectors", icon: <IconConnector size={20} /> },
];

export function NavPane({ active, onNavigate }: NavPaneProps) {
  return (
    <ul className="df-nav-list">
      {ITEMS.map((item) => (
        <li key={item.id}>
          <button
            type="button"
            className={["df-nav-item", active === item.id ? "df-nav-item--active" : ""].filter(Boolean).join(" ")}
            onClick={() => onNavigate(item.id)}
            aria-current={active === item.id ? "page" : undefined}
          >
            <span className="df-nav-item-icon">{item.icon}</span>
            <span className="df-nav-item-label">{item.label}</span>
          </button>
        </li>
      ))}
    </ul>
  );
}

export const RailNav = NavPane;
export type { NavItemId };
