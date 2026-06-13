import type { ReactNode } from "react";
import { BrandLogo } from "./BrandLogo";
import { NavPane, type NavItemId } from "./NavPane";
import { PipelineSteps, type PipelineStepItem } from "./PipelineSteps";
import { ThemeToggle } from "./ThemeToggle";

const PAGE_TITLES: Record<NavItemId, string> = {
  home: "Home",
  transfer: "Transfer",
  jobs: "Operations",
  connectors: "Connectors",
};

interface AppLayoutProps {
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
  topAction?: ReactNode;
}

export function AppLayout({
  activeNav,
  onNavigate,
  onNewTransfer,
  children,
  narrow = false,
  pipeline,
  topAction,
}: AppLayoutProps) {
  const title = pipeline
    ? `${pipeline.steps[pipeline.currentIndex]?.label ?? "Transfer"}`
    : PAGE_TITLES[activeNav];

  return (
    <div className="df-app">
      <aside className="df-nav-pane df-glass" aria-label="Navigation">
        <div className="df-nav-pane-head">
          <BrandLogo />
        </div>

        <div className="df-nav-pane-body">
          <NavPane active={activeNav} onNavigate={onNavigate} />
        </div>

        <div className="df-nav-pane-foot">
          <ThemeToggle />
        </div>
      </aside>

      <div className="df-workspace">
        <header className="df-title-bar df-glass">
          <h1 className="df-title-bar-heading">{title}</h1>
          <div className="df-title-bar-actions">
            {topAction}
            {!pipeline && activeNav !== "transfer" && (
              <button type="button" className="df-btn df-btn-primary" onClick={onNewTransfer}>
                New transfer
              </button>
            )}
          </div>
        </header>

        {pipeline && (
          <div className="df-workspace-chrome df-glass">
            <PipelineSteps steps={pipeline.steps} currentIndex={pipeline.currentIndex} />
            <button type="button" className="df-pipeline-exit" onClick={pipeline.onExit}>
              Exit
            </button>
          </div>
        )}

        <main className="df-workspace-scroll">
          <div
            className={["df-workspace-inner", narrow ? "df-workspace-inner--narrow" : ""]
              .filter(Boolean)
              .join(" ")}
          >
            <div className="df-page">{children}</div>
          </div>
        </main>
      </div>
    </div>
  );
}

export const PlatformShell = AppLayout;
