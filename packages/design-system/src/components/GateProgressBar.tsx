import { ProgressBar } from "./ProgressBar";

interface GateProgressBarProps {
  passed: number;
  total: number;
  label?: string;
}

export function GateProgressBar({ passed, total, label = "Preflight gates" }: GateProgressBarProps) {
  const pct = total > 0 ? Math.round((passed / total) * 100) : 0;
  const tone = passed === total ? "mint" : passed > 0 ? "brand" : "neutral";

  return (
    <div className="df-gate-progress">
      <ProgressBar
        value={pct}
        label={label}
        sublabel={`${passed}/${total} passed`}
        tone={tone}
      />
      <div className="df-gate-progress-segments" aria-hidden>
        {Array.from({ length: total }, (_, i) => (
          <span
            key={i}
            className={[
              "df-gate-progress-seg",
              i < passed ? "df-gate-progress-seg--pass" : "",
              i === passed && passed < total ? "df-gate-progress-seg--active" : "",
            ]
              .filter(Boolean)
              .join(" ")}
          />
        ))}
      </div>
    </div>
  );
}
