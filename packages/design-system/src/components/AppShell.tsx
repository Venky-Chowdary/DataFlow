import type { ReactNode } from "react";
import { BrandLogo } from "./BrandLogo";
import { StepWizard, type WizardStepItem } from "./StepWizard";
import { ThemeToggle } from "./ThemeToggle";

interface AppShellProps {
  steps: WizardStepItem[];
  currentStepIndex: number;
  children: ReactNode;
  breadcrumb?: string[];
  onExit?: () => void;
}

export function AppShell({ steps, currentStepIndex, children, breadcrumb, onExit }: AppShellProps) {
  const trail = breadcrumb ?? ["Transfer", steps[currentStepIndex]?.label ?? ""];

  return (
    <div className="df-app df-app--wizard">
      <header className="df-app-header">
        <BrandLogo />
        <nav className="df-breadcrumb" aria-label="Breadcrumb">
          {trail.map((part, i) => (
            <span key={`${part}-${i}`} style={{ display: "contents" }}>
              {i > 0 && <span className="df-breadcrumb-sep" aria-hidden>/</span>}
              <span className={i === trail.length - 1 ? "df-breadcrumb-current" : undefined}>{part}</span>
            </span>
          ))}
        </nav>
        <div className="df-header-actions">
          {onExit && (
            <button type="button" className="df-btn df-btn-ghost df-btn--header" onClick={onExit}>
              Exit wizard
            </button>
          )}
          <ThemeToggle />
        </div>
      </header>

      <div className="df-app-body">
        <aside className="df-sidebar">
          <p className="df-wizard-rail-title">Transfer pipeline</p>
          <StepWizard steps={steps} currentIndex={currentStepIndex} variant="sidebar" />
          <div className="df-wizard-rail-meta">
            <strong>Eight-gate preflight</strong>
            Every transfer runs source, schema, mapping, dry-run, capacity, and reconciliation checks before a single row moves.
          </div>
        </aside>
        <main className="df-main">
          <div className="df-main-inner">{children}</div>
        </main>
      </div>
    </div>
  );
}
