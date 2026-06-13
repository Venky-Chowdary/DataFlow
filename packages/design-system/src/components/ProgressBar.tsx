interface ProgressBarProps {
  value?: number;
  indeterminate?: boolean;
  label?: string;
  sublabel?: string;
  tone?: "brand" | "mint" | "danger" | "neutral";
  size?: "sm" | "md";
}

export function ProgressBar({
  value = 0,
  indeterminate = false,
  label,
  sublabel,
  tone = "brand",
  size = "md",
}: ProgressBarProps) {
  const pct = indeterminate ? undefined : Math.min(100, Math.max(0, value));

  return (
    <div className={["df-progress", size === "sm" ? "df-progress--sm" : ""].filter(Boolean).join(" ")}>
      {(label || sublabel) && (
        <div className="df-progress-head">
          {label && <span className="df-progress-label">{label}</span>}
          {sublabel && <span className="df-progress-sublabel">{sublabel}</span>}
        </div>
      )}
      <div
        className={[
          "df-progress-rail",
          indeterminate ? "df-progress-rail--indeterminate" : "",
          `df-progress-rail--${tone}`,
        ]
          .filter(Boolean)
          .join(" ")}
        role="progressbar"
        aria-valuenow={indeterminate ? undefined : pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div className="df-progress-fill" style={pct !== undefined ? { width: `${pct}%` } : undefined} />
      </div>
    </div>
  );
}
