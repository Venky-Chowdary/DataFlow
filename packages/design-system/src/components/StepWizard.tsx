export interface WizardStepItem {
  id: string;
  label: string;
  description: string;
}

interface StepWizardProps {
  steps: WizardStepItem[];
  currentIndex: number;
  variant?: "sidebar" | "header" | "modern";
}

export function StepWizard({ steps, currentIndex, variant = "sidebar" }: StepWizardProps) {
  if (variant === "modern" || variant === "header") {
    return (
      <div className="df-steps-header" role="navigation" aria-label="Transfer steps">
        {steps.map((step, i) => (
          <div key={step.id} style={{ display: "contents" }}>
            {i > 0 && <div className="df-step-connector" aria-hidden />}
            <div
              className={[
                "df-step-pill",
                i === currentIndex ? "df-step-pill--active" : "",
                i < currentIndex ? "df-step-pill--done" : "",
              ]
                .filter(Boolean)
                .join(" ")}
            >
              <span className="df-step-pill-num">{i < currentIndex ? "✓" : i + 1}</span>
              <span>{step.label}</span>
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <nav aria-label="Transfer wizard">
      <ul className="df-steps">
        {steps.map((step, i) => (
          <li
            key={step.id}
            className={[
              "df-step",
              i === currentIndex ? "df-step--active" : "",
              i < currentIndex ? "df-step--done" : "",
            ]
              .filter(Boolean)
              .join(" ")}
          >
            <span className="df-step-indicator">{i < currentIndex ? "✓" : i + 1}</span>
            <div>
              <div className="df-step-label">{step.label}</div>
              <div className="df-step-desc">{step.description}</div>
            </div>
          </li>
        ))}
      </ul>
    </nav>
  );
}
