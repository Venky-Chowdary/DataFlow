import { DtIcon } from "../DtIcon";

interface Step {
  n: number;
  label: string;
  icon: string;
}

interface WizardStepsProps {
  steps: Step[];
  current: number;
  onStepClick?: (n: number) => void;
  canGoTo?: (n: number) => boolean;
}

export function WizardSteps({ steps, current, onStepClick, canGoTo }: WizardStepsProps) {
  return (
    <nav className="df2-wizard" aria-label="Transfer steps">
      {steps.map((s, i) => {
        const done = current > s.n;
        const active = current === s.n;
        const clickable = done && onStepClick && (!canGoTo || canGoTo(s.n));
        return (
          <div key={s.n} className="df2-wizard-item">
            {i > 0 && <div className={`df2-wizard-line ${done ? "done" : ""}`} aria-hidden />}
            <button
              type="button"
              className={`df2-wizard-step ${active ? "active" : ""} ${done ? "done" : ""}`}
              disabled={!clickable}
              onClick={() => clickable && onStepClick?.(s.n)}
              aria-current={active ? "step" : undefined}
              aria-label={`${s.n}. ${s.label}`}
              title={`${s.n}. ${s.label}`}
            >
              <span className="df2-wizard-num">
                {done ? <DtIcon name="check" size={12} /> : <DtIcon name={s.icon} size={14} />}
              </span>
              <span className="df2-wizard-label">{s.label}</span>
            </button>
          </div>
        );
      })}
    </nav>
  );
}
