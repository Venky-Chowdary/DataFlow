import type { ReactNode } from "react";
import { clampPercent, ringDasharray } from "../../lib/progressRing";

interface ProgressRingProps {
  value: number;
  indeterminate?: boolean;
  size?: number;
  radius?: number;
  tone?: "ok" | "warn" | "danger" | "live" | "idle";
  children?: ReactNode;
  className?: string;
}

/** One SVG ring — 100% is a closed circle. Live mode spins; it does not invent %. */
export function ProgressRing({
  value,
  indeterminate = false,
  size = 56,
  radius,
  tone = "ok",
  children,
  className = "",
}: ProgressRingProps) {
  const r = radius ?? Math.max(8, size / 2 - 4);
  const c = size / 2;
  const pct = clampPercent(value);
  const complete = !indeterminate && pct >= 100;
  return (
    <div
      className={`df2-progress-ring tone-${tone}${indeterminate ? " is-indeterminate" : ""}${complete ? " is-complete" : ""} ${className}`.trim()}
      style={{ width: size, height: size }}
      aria-hidden
    >
      <svg viewBox={`0 0 ${size} ${size}`}>
        <circle cx={c} cy={c} r={r} className="track" />
        <circle
          cx={c}
          cy={c}
          r={r}
          className="fill"
          pathLength={100}
          strokeDasharray={ringDasharray(pct, { indeterminate })}
          transform={`rotate(-90 ${c} ${c})`}
        />
      </svg>
      <div className="df2-progress-ring-label">{children}</div>
    </div>
  );
}
