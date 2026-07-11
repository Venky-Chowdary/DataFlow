import { DtIcon } from "../DtIcon";

interface StatCardProps {
  label: string;
  value: string | number;
  tone?: "default" | "blue" | "green" | "red" | "teal";
  sub?: string;
  icon?: string;
}

export function StatCard({ label, value, tone = "default", sub, icon }: StatCardProps) {
  const toneClass =
    tone === "blue" ? "blue"
    : tone === "green" ? "green"
    : tone === "red" ? "red"
    : tone === "teal" ? "teal"
    : "";
  return (
    <div className="df2-stat">
      {icon && (
        <div className="df2-stat-icon">
          <DtIcon name={icon} size={16} />
        </div>
      )}
      <div className="df2-stat-label">{label}</div>
      <div className={`df2-stat-value ${toneClass}`}>{value}</div>
      {sub && <div className="df2-stat-sub">{sub}</div>}
    </div>
  );
}
