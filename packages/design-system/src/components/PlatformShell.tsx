import type { ReactNode } from "react";
import { BrandLogo } from "./BrandLogo";
import { EnterpriseNav, type NavItemId } from "./EnterpriseNav";
import { PipelineSteps, type PipelineStepItem } from "./PipelineSteps";
import { ThemeToggle } from "./ThemeToggle";

interface PlatformShellProps {
  activeNav: NavItemId;
  onNavigate: (id: NavItemId) => void;
  onNewTransfer: () => void;
  children: ReactNode;
  narrow?: boolean;
  pipeline?: {
    steps: PipelineStepItem[];
    currentIndex: number;
    onExit: () => void;
  };
}

export function PlatformShell({
  activeNav,
  onNavigate,
  onNewTransfer,
  children,
  narrow = false,
  pipeline,
}: PlatformShellProps) {
  return (
    <div className="df-platform">
      <header className="df-topbar">
        <BrandLogo />
        <div className="df-topbar-nav">
          <EnterpriseNav active={activeNav} onNavigate={onNavigate} variant="top" />
        </div>
        <div className="df-topbar-actions">
          {!pipeline && (
            <button type="button" className="df-btn-run" onClick={onNewTransfer}>
              <span className="df-btn-run-icon" aria-hidden>
                <svg width="11" height="13" viewBox="0 0 14 16" fill="currentColor">
                  <path d="M2 1.5L12 8L2 14.5V1.5Z" />
                </svg>
              </span>
              Run transfer
            </button>
          )}
          <ThemeToggle />
        </div>
      </header>

      {pipeline && (
        <div className="df-wizard-strip">
          <div className="df-wizard-strip-inner">
            <PipelineSteps steps={pipeline.steps} currentIndex={pipeline.currentIndex} />
            <button type="button" className="df-wizard-exit" onClick={pipeline.onExit}>
              Exit
            </button>
          </div>
        </div>
      )}

      <main className="df-canvas">
        <div className={["df-canvas-inner", narrow ? "df-canvas-inner--narrow" : ""].filter(Boolean).join(" ")}>
          {children}
        </div>
      </main>
    </div>
  );
}
