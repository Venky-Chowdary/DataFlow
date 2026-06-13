interface StatCardProps {
  label: string;
  value: string | number;
  delta?: string;
  trend?: "up" | "down" | "neutral";
  accent?: "steel" | "teal" | "success" | "warning";
}

export function StatCard({ label, value, delta, trend = "neutral", accent = "steel" }: StatCardProps) {
  return (
    <div className={["df-stat-card", `df-stat-card--${accent}`].join(" ")}>
      <div className="df-stat-label">{label}</div>
      <div className="df-stat-value">{typeof value === "number" ? value.toLocaleString() : value}</div>
      {delta && (
        <div className={["df-stat-delta", `df-stat-delta--${trend}`].join(" ")}>{delta}</div>
      )}
    </div>
  );
}
