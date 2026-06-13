interface StatStripItem {
  label: string;
  value: string | number;
  tone?: "default" | "emerald" | "amber";
}

interface StatStripProps {
  items: StatStripItem[];
}

export function StatStrip({ items }: StatStripProps) {
  return (
    <div className="df-stat-strip" role="group" aria-label="Metrics">
      {items.map((item) => (
        <div key={item.label} className="df-stat-item">
          <span className="df-stat-item-label">{item.label}</span>
          <span
            className={[
              "df-stat-item-value",
              item.tone === "emerald" ? "df-stat-item-value--emerald" : "",
              item.tone === "amber" ? "df-stat-item-value--amber" : "",
            ]
              .filter(Boolean)
              .join(" ")}
          >
            {typeof item.value === "number" ? item.value.toLocaleString() : item.value}
          </span>
        </div>
      ))}
    </div>
  );
}
