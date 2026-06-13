export interface PipelineStepItem {
  id: string;
  label: string;
}

interface PipelineStepsProps {
  steps: PipelineStepItem[];
  currentIndex: number;
}

export function PipelineSteps({ steps, currentIndex }: PipelineStepsProps) {
  return (
    <>
      {steps.map((step, i) => (
        <div
          key={step.id}
          className={[
            "df-pipeline-step",
            i === currentIndex ? "df-pipeline-step--active" : "",
            i < currentIndex ? "df-pipeline-step--done" : "",
          ]
            .filter(Boolean)
            .join(" ")}
          aria-current={i === currentIndex ? "step" : undefined}
        >
          <span className="df-pipeline-step-num">{i < currentIndex ? "✓" : i + 1}</span>
          <span>{step.label}</span>
        </div>
      ))}
    </>
  );
}
