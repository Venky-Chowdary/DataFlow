import type { CSSProperties } from "react";

interface ConfidenceBarProps {
  value: number;
  showLabel?: boolean;
}

function barColor(value: number): string {
  if (value >= 0.95) return "var(--df-success)";
  if (value >= 0.85) return "var(--df-warning)";
  return "var(--df-danger)";
}

export function ConfidenceBar({ value, showLabel = true }: ConfidenceBarProps) {
  const pct = Math.round(value * 100);
  const fillStyle: CSSProperties = {
    width: `${pct}%`,
    background: barColor(value),
  };

  return (
    <div className="df-confidence">
      <div className="df-confidence-track">
        <div className="df-confidence-fill" style={fillStyle} />
      </div>
      {showLabel && <span className="df-confidence-label df-mono">{pct}%</span>}
    </div>
  );
}
