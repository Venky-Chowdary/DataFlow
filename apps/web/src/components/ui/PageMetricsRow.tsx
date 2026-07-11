import { StatCard } from "./StatCard";

export interface MetricItem {
  label: string;
  value: string | number;
  tone?: "default" | "blue" | "green" | "red" | "teal";
  sub?: string;
  icon?: string;
}

interface PageMetricsRowProps {
  metrics: MetricItem[];
  compact?: boolean;
  columns?: 3 | 4 | 5;
  className?: string;
}

/** Standardized KPI row — equal-height tiles on every page */
export function PageMetricsRow({ metrics, compact, columns = 4, className = "" }: PageMetricsRowProps) {
  const colClass = columns === 5 ? "df2-stats--5" : columns === 3 ? "df2-stats--3" : "";
  return (
    <section
      className={`df2-stats ${compact ? "df2-stats--compact" : ""} ${colClass} ${className}`.trim()}
      aria-label="Key metrics"
    >
      {metrics.map((m) => (
        <StatCard key={m.label} {...m} />
      ))}
    </section>
  );
}
