export interface ConnectorItem {
  id: string;
  name: string;
  category: "database" | "file" | "api" | "warehouse";
  status: "live" | "beta" | "planned" | "ai";
  description: string;
}

interface ConnectorCatalogProps {
  connectors: ConnectorItem[];
  onSelect?: (id: string) => void;
}

const CATEGORY_LABEL: Record<ConnectorItem["category"], string> = {
  database: "Database",
  file: "File format",
  api: "API",
  warehouse: "Warehouse",
};

const STATUS_LABEL: Record<ConnectorItem["status"], string> = {
  live: "Live",
  beta: "Beta",
  planned: "Planned",
  ai: "AI Factory",
};

export function ConnectorCatalog({ connectors, onSelect }: ConnectorCatalogProps) {
  return (
    <div className="df-connector-grid">
      {connectors.map((c) => (
        <button
          key={c.id}
          type="button"
          className="df-connector-card"
          onClick={() => onSelect?.(c.id)}
          disabled={!onSelect}
        >
          <div className="df-connector-card-top">
            <span className="df-connector-name">{c.name}</span>
            <span className={["df-connector-status", `df-connector-status--${c.status}`].join(" ")}>
              {STATUS_LABEL[c.status]}
            </span>
          </div>
          <div className="df-connector-category">{CATEGORY_LABEL[c.category]}</div>
          <div className="df-connector-desc">{c.description}</div>
        </button>
      ))}
    </div>
  );
}
