interface ProgressCellProps {
  value: number;
  done?: boolean;
  className?: string;
}

/** Enterprise progress — label never overlaps the bar */
export function ProgressCell({ value, done = false, className = "" }: ProgressCellProps) {
  const pct = Math.max(0, Math.min(100, Math.round(value)));
  return (
    <div className={`df2-progress-cell ${done ? "is-done" : ""} ${className}`.trim()}>
      <div
        className="df2-progress-track"
        role="progressbar"
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
