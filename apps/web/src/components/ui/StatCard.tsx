interface StatCardProps {
  label: string;
  value: string | number;
  tone?: "default" | "blue" | "green" | "red";
  sub?: string;
}

export function StatCard({ label, value, tone = "default", sub }: StatCardProps) {
  const toneClass = tone === "blue" ? "blue" : tone === "green" ? "green" : tone === "red" ? "red" : "";
  return (
    <div className="df2-stat">
      <div className="df2-stat-label">{label}</div>
      <div className={`df2-stat-value ${toneClass}`}>{value}</div>
      {sub && <div className="df2-stat-sub">{sub}</div>}
    </div>
  );
}
