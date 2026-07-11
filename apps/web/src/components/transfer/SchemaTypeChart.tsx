import type { ColumnAnalysis } from "../../lib/types";

interface SchemaTypeChartProps {
  columns: ColumnAnalysis[];
}

const TYPE_COLORS: Record<string, string> = {
  string: "#6366f1",
  varchar: "#6366f1",
  text: "#6366f1",
  integer: "#0ea5e9",
  int: "#0ea5e9",
  bigint: "#0284c7",
  decimal: "#8b5cf6",
  numeric: "#8b5cf6",
  float: "#a855f7",
  double: "#a855f7",
  boolean: "#14b8a6",
  date: "#f59e0b",
  timestamp: "#f97316",
  datetime: "#f97316",
  json: "#ec4899",
  unknown: "#94a3b8",
};

function normalizeType(t: string): string {
  const lower = (t || "string").toLowerCase();
  if (lower.includes("int")) return "integer";
  if (lower.includes("dec") || lower.includes("num")) return "decimal";
  if (lower.includes("date") || lower.includes("time")) return "date";
  if (lower.includes("bool")) return "boolean";
  if (lower.includes("json")) return "json";
  if (lower.includes("char") || lower.includes("text") || lower.includes("str")) return "string";
  return lower;
}

export function SchemaTypeChart({ columns }: SchemaTypeChartProps) {
  const buckets = new Map<string, number>();
  for (const col of columns) {
    const t = normalizeType(col.inferred_type ?? col.semantic_type ?? "string");
    buckets.set(t, (buckets.get(t) ?? 0) + 1);
  }
  const entries = [...buckets.entries()].sort((a, b) => b[1] - a[1]);
  const total = columns.length || 1;

  if (!entries.length) return null;

  return (
    <div className="df2-type-chart" aria-label="Column type distribution">
      <div className="df2-type-chart-bar">
        {entries.map(([type, count], i) => (
          <span
            key={type}
            className="df2-type-chart-segment"
            style={{
              width: `${(count / total) * 100}%`,
              background: TYPE_COLORS[type] ?? TYPE_COLORS.unknown,
              animationDelay: `${i * 0.08}s`,
            }}
            title={`${type}: ${count}`}
          />
        ))}
      </div>
      <ul className="df2-type-chart-legend">
        {entries.slice(0, 6).map(([type, count]) => (
          <li key={type}>
            <span className="df2-type-chart-swatch" style={{ background: TYPE_COLORS[type] ?? TYPE_COLORS.unknown }} />
            <span>{type}</span>
            <strong>{count}</strong>
          </li>
        ))}
      </ul>
    </div>
  );
}
