interface MetricCardProps {
  label: string;
  value: string | number;
  tone?: "default" | "mint" | "orange";
}

interface MetricRowProps {
  items: MetricCardProps[];
}

export function MetricRow({ items }: MetricRowProps) {
  return (
    <div className="df-metric-row" role="group" aria-label="Metrics">
      {items.map((item) => (
        <div key={item.label} className="df-metric-card">
          <div className="df-metric-card-label">{item.label}</div>
          <div
            className={[
              "df-metric-card-value",
              item.tone === "mint" ? "df-metric-card-value--mint" : "",
              item.tone === "orange" ? "df-metric-card-value--orange" : "",
            ]
              .filter(Boolean)
              .join(" ")}
          >
            {typeof item.value === "number" ? item.value.toLocaleString() : item.value}
          </div>
        </div>
      ))}
    </div>
  );
}
