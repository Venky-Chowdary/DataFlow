interface ProgressCellProps {
  value: number;
  done?: boolean;
  className?: string;
  /** What is progressing; the bar's accessible name (UX-16). */
  label?: string;
}

/** Enterprise progress — label never overlaps the bar */
export function ProgressCell({ value, done = false, className = "", label = "Job progress" }: ProgressCellProps) {
  const pct = Math.max(0, Math.min(100, Math.round(value)));
  return (
    <div className={`df2-progress-cell ${done ? "is-done" : ""} ${className}`.trim()}>
      <div
        className="df2-progress-track"
        role="progressbar"
        aria-label={label}
        aria-valuenow={pct}
        aria-valuemin={0}
        aria-valuemax={100}
      >
        <div className="df2-progress-fill" style={{ width: `${pct}%` }} />
      </div>
      <span className="df2-progress-pct">{pct}%</span>
    </div>
  );
}
