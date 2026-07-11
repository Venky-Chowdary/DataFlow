import { DtIcon } from "../DtIcon";

interface Step {
  n: number;
  label: string;
  icon: string;
  shortLabel?: string;
}

interface WizardStepsProps {
  steps: Step[];
  current: number;
  onStepClick?: (n: number) => void;
  canGoTo?: (n: number) => boolean;
  /** default = full card · compact = tiny pills · studio = proportional Transfer Studio rail */
  variant?: "default" | "compact" | "studio";
}

export function WizardSteps({ steps, current, onStepClick, canGoTo, variant = "default" }: WizardStepsProps) {
  const lastN = steps.length ? steps[steps.length - 1].n : 0;
  const progressPct = steps.length ? ((current - 1) / Math.max(steps.length - 1, 1)) * 100 : 0;

  const variantClass =
    variant === "studio" ? "df2-wizard-studio" :
    variant === "compact" ? "df2-wizard-compact" : "";

  return (
    <nav
      className={`df2-wizard ${variantClass}`}
      aria-label="Transfer steps"
      data-current-step={current}
    >
      <div className="df2-wizard-track">
        {steps.map((s) => {
          const done = current > s.n;
          const active = current === s.n;
          const clickable = done && onStepClick && (!canGoTo || canGoTo(s.n));
          const isLast = s.n === lastN;
          return (
            <div key={s.n} className="df2-wizard-item">
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
                  {done ? <DtIcon name="check" size={14} /> : <DtIcon name={s.icon} size={16} />}
                </span>
                <span className="df2-wizard-label">{s.label}</span>
                {s.shortLabel && (
                  <span className="df2-wizard-label-short" aria-hidden>{s.shortLabel}</span>
                )}
              </button>
              {!isLast && <div className={`df2-wizard-line ${done ? "done" : ""}`} aria-hidden />}
            </div>
          );
        })}
      </div>
      {variant === "studio" && (
        <div className="df2-wizard-progress" aria-hidden>
          <div className="df2-wizard-progress-fill" style={{ width: `${progressPct}%` }} />
        </div>
      )}
    </nav>
  );
}
