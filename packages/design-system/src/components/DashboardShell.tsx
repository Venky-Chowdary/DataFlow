import type { ReactNode } from "react";
import { BrandLogo } from "./BrandLogo";
import { EnterpriseNav, type NavItemId } from "./EnterpriseNav";
import { ThemeToggle } from "./ThemeToggle";

interface DashboardShellProps {
  activeNav: NavItemId;
  onNavigate: (id: NavItemId) => void;
  onNewTransfer?: () => void;
  children: ReactNode;
}

export function DashboardShell({ activeNav, onNavigate, onNewTransfer, children }: DashboardShellProps) {
  return (
    <div className="df-app df-app--dashboard">
      <header className="df-app-header">
        <BrandLogo />
        <span className="df-badge-enterprise">Enterprise</span>
        <div className="df-header-spacer" />
        <div className="df-header-actions">
          <ThemeToggle />
        </div>
      </header>

      <div className="df-app-body">
        <aside className="df-sidebar df-sidebar--nav">
          <EnterpriseNav active={activeNav} onNavigate={onNavigate} onNewTransfer={onNewTransfer} />
        </aside>
        <main className="df-main df-main--dashboard">
          <div className="df-main-inner df-main-inner--wide">{children}</div>
        </main>
      </div>
    </div>
  );
}
