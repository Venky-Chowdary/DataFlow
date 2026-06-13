interface WizardPanelProps {
  title: string;
  subtitle?: string;
  step?: number;
  action?: React.ReactNode;
  children: React.ReactNode;
}

export function WizardPanel({ title, subtitle, step, action, children }: WizardPanelProps) {
  return (
    <section className="df-wizard-panel">
      <div className="df-wizard-panel-header">
        <div>
          {step !== undefined && <span className="df-wizard-panel-step">Step {step}</span>}
          <h2 className="df-wizard-panel-title">{title}</h2>
          {subtitle && <p className="df-wizard-panel-subtitle">{subtitle}</p>}
        </div>
        {action}
      </div>
      <div className="df-wizard-panel-body">{children}</div>
    </section>
  );
}
