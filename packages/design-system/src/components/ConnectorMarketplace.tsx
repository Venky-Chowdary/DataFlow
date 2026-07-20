import { useState, useMemo } from "react";

interface ConnectorDef {
  id: string;
  name: string;
  category: string;
  icon: string;
  color: string;
  status: "live" | "beta" | "coming";
  description: string;
}

const CONNECTORS: ConnectorDef[] = [
  // Databases
  { id: "postgresql", name: "PostgreSQL", category: "Databases", icon: "🐘", color: "#336791", status: "live", description: "Open-source relational database" },
  { id: "mysql", name: "MySQL", category: "Databases", icon: "🐬", color: "#00758F", status: "live", description: "Popular open-source RDBMS" },
  { id: "mongodb", name: "MongoDB", category: "Databases", icon: "🍃", color: "#13AA52", status: "live", description: "NoSQL document database" },
  { id: "oracle", name: "Oracle", category: "Databases", icon: "🔴", color: "#F80000", status: "live", description: "Enterprise database — LogMiner/Flashback CDC" },
  { id: "sqlserver", name: "SQL Server", category: "Databases", icon: "📊", color: "#CC2927", status: "live", description: "Enterprise database — native CDC / CT" },
  { id: "redis", name: "Redis", category: "Databases", icon: "⚡", color: "#DC382D", status: "live", description: "In-memory data store" },
  { id: "cassandra", name: "Cassandra", category: "Databases", icon: "👁", color: "#1287B1", status: "beta", description: "Distributed NoSQL database" },
  { id: "dynamodb", name: "DynamoDB", category: "Databases", icon: "⚙️", color: "#4053D6", status: "live", description: "AWS managed NoSQL" },

  // Data Warehouses
  { id: "snowflake", name: "Snowflake", category: "Data Warehouses", icon: "❄️", color: "#29B5E8", status: "live", description: "Cloud data platform" },
  { id: "bigquery", name: "BigQuery", category: "Data Warehouses", icon: "📈", color: "#4285F4", status: "live", description: "Google cloud analytics" },
  { id: "redshift", name: "Redshift", category: "Data Warehouses", icon: "🔴", color: "#8C4FFF", status: "live", description: "AWS data warehouse" },
  { id: "databricks", name: "Databricks", category: "Data Warehouses", icon: "🧱", color: "#FF3621", status: "coming", description: "Unified analytics platform" },
  { id: "synapse", name: "Azure Synapse", category: "Data Warehouses", icon: "🔷", color: "#0078D4", status: "coming", description: "Microsoft analytics service" },
  { id: "iceberg", name: "Apache Iceberg", category: "Data Warehouses", icon: "🧊", color: "#1B7A9E", status: "live", description: "Lakehouse table format writer" },

  // Files
  { id: "csv", name: "CSV", category: "Files", icon: "📄", color: "#4CAF50", status: "live", description: "Comma-separated values" },
  { id: "excel", name: "Excel", category: "Files", icon: "📊", color: "#217346", status: "live", description: "Microsoft spreadsheet" },
  { id: "json", name: "JSON", category: "Files", icon: "{ }", color: "#F7DF1E", status: "live", description: "JavaScript object notation" },
  { id: "xml", name: "XML", category: "Files", icon: "</>", color: "#E34F26", status: "live", description: "Extensible markup language" },
  { id: "parquet", name: "Parquet", category: "Files", icon: "🗂", color: "#50ABF1", status: "live", description: "Columnar storage format" },
  { id: "avro", name: "Avro", category: "Files", icon: "🔄", color: "#FF6B35", status: "live", description: "Data serialization system" },

  // Cloud Storage
  { id: "s3", name: "Amazon S3", category: "Cloud Storage", icon: "🪣", color: "#FF9900", status: "live", description: "AWS object storage" },
  { id: "gcs", name: "Google Cloud Storage", category: "Cloud Storage", icon: "☁️", color: "#4285F4", status: "live", description: "GCP object storage" },
  { id: "azure-blob", name: "Azure Blob", category: "Cloud Storage", icon: "📦", color: "#0078D4", status: "live", description: "Microsoft cloud storage" },
  { id: "dropbox", name: "Dropbox", category: "Cloud Storage", icon: "📁", color: "#0061FF", status: "beta", description: "File hosting service" },
  { id: "google-drive", name: "Google Drive", category: "Cloud Storage", icon: "🔺", color: "#4285F4", status: "live", description: "Cloud file storage" },

  // APIs
  { id: "rest", name: "REST API", category: "APIs", icon: "🔌", color: "#7B61FF", status: "live", description: "RESTful web services" },
  { id: "graphql", name: "GraphQL", category: "APIs", icon: "◈", color: "#E10098", status: "live", description: "Query language for APIs" },
  { id: "webhook", name: "Webhooks", category: "APIs", icon: "🎣", color: "#00D4FF", status: "live", description: "HTTP callbacks" },

  // SaaS Platforms
  { id: "salesforce", name: "Salesforce", category: "SaaS", icon: "☁️", color: "#00A1E0", status: "live", description: "CRM platform" },
  { id: "hubspot", name: "HubSpot", category: "SaaS", icon: "🧡", color: "#FF7A59", status: "live", description: "Marketing & CRM" },
  { id: "zendesk", name: "Zendesk", category: "SaaS", icon: "🎧", color: "#03363D", status: "beta", description: "Customer service" },
  { id: "stripe", name: "Stripe", category: "SaaS", icon: "💳", color: "#635BFF", status: "live", description: "Payment processing" },
  { id: "shopify", name: "Shopify", category: "SaaS", icon: "🛒", color: "#96BF48", status: "coming", description: "E-commerce platform" },

  // Event Streams
  { id: "kafka", name: "Apache Kafka", category: "Streams", icon: "📨", color: "#231F20", status: "live", description: "JSON produce destination (optional Schema Registry)" },
  { id: "kinesis", name: "AWS Kinesis", category: "Streams", icon: "🌊", color: "#FF9900", status: "coming", description: "Real-time data streaming" },
  { id: "pubsub", name: "Google Pub/Sub", category: "Streams", icon: "📡", color: "#4285F4", status: "coming", description: "Messaging service" },
  { id: "rabbitmq", name: "RabbitMQ", category: "Streams", icon: "🐰", color: "#FF6600", status: "coming", description: "Message broker" },

  // Enterprise
  { id: "sap", name: "SAP", category: "Enterprise", icon: "💎", color: "#0FAAFF", status: "coming", description: "Enterprise software" },
  { id: "workday", name: "Workday", category: "Enterprise", icon: "👔", color: "#0072CE", status: "coming", description: "HR & finance platform" },
  { id: "netsuite", name: "NetSuite", category: "Enterprise", icon: "📋", color: "#0B0B0B", status: "coming", description: "ERP system" },
  { id: "servicenow", name: "ServiceNow", category: "Enterprise", icon: "🎫", color: "#81B5A1", status: "coming", description: "IT service management" },
];

const CATEGORIES = [
  { id: "all", name: "All Connectors", icon: "🔗" },
  { id: "Databases", name: "Databases", icon: "🗄️" },
  { id: "Data Warehouses", name: "Data Warehouses", icon: "🏢" },
  { id: "Files", name: "Files", icon: "📁" },
  { id: "Cloud Storage", name: "Cloud Storage", icon: "☁️" },
  { id: "APIs", name: "APIs", icon: "🔌" },
  { id: "SaaS", name: "SaaS Platforms", icon: "💼" },
  { id: "Streams", name: "Event Streams", icon: "📨" },
  { id: "Enterprise", name: "Enterprise Apps", icon: "🏛️" },
];

interface ConnectorCardProps {
  connector: ConnectorDef;
  selected?: boolean;
  onSelect?: (id: string) => void;
}

function ConnectorCard({ connector, selected, onSelect }: ConnectorCardProps) {
  return (
    <button
      type="button"
      className={`dt-connector-card ${selected ? "dt-connector-card--selected" : ""}`}
      onClick={() => onSelect?.(connector.id)}
      style={{ "--connector-color": connector.color } as React.CSSProperties}
    >
      <div className="dt-connector-card-glow" />
      <div className="dt-connector-card-icon">
        <span>{connector.icon}</span>
      </div>
      <div className="dt-connector-card-content">
        <span className="dt-connector-card-name">{connector.name}</span>
        <span className="dt-connector-card-desc">{connector.description}</span>
      </div>
      <div className="dt-connector-card-status">
        <span className={`dt-badge dt-badge--${connector.status === "live" ? "success" : connector.status === "beta" ? "info" : "purple"}`}>
          {connector.status === "live" && <span className="dt-badge-dot" />}
          {connector.status}
        </span>
      </div>
      {selected && (
        <div className="dt-connector-card-check">
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M3 8L6.5 11.5L13 5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </div>
      )}
    </button>
  );
}

interface ConnectorMarketplaceProps {
  mode?: "source" | "destination";
  selectedId?: string;
  onSelect?: (id: string) => void;
}

export function ConnectorMarketplace({ mode = "source", selectedId, onSelect }: ConnectorMarketplaceProps) {
  const [search, setSearch] = useState("");
  const [category, setCategory] = useState("all");

  const filtered = useMemo(() => {
    return CONNECTORS.filter((c) => {
      const matchesSearch = c.name.toLowerCase().includes(search.toLowerCase()) ||
                           c.description.toLowerCase().includes(search.toLowerCase());
      const matchesCategory = category === "all" || c.category === category;
      return matchesSearch && matchesCategory;
    });
  }, [search, category]);

  const grouped = useMemo(() => {
    if (category !== "all") return { [category]: filtered };
    return filtered.reduce((acc, c) => {
      if (!acc[c.category]) acc[c.category] = [];
      acc[c.category].push(c);
      return acc;
    }, {} as Record<string, ConnectorDef[]>);
  }, [filtered, category]);

  return (
    <div className="dt-marketplace">
      <div className="dt-marketplace-header">
        <div className="dt-marketplace-header-text">
          <h2 className="dt-marketplace-title">
            Select {mode === "source" ? "Source" : "Destination"}
          </h2>
          <p className="dt-marketplace-subtitle">
            Certified transfer drivers plus roadmap catalog tiles — honest status labels
          </p>
        </div>

        <div className="dt-marketplace-search">
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
            <circle cx="8" cy="8" r="5.5" stroke="currentColor" strokeWidth="1.5"/>
            <path d="M12 12L16 16" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round"/>
          </svg>
          <input
            type="text"
            className="dt-input"
            placeholder="Search any source system..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
      </div>

      <div className="dt-marketplace-categories">
        {CATEGORIES.map((cat) => (
          <button
            key={cat.id}
            type="button"
            className={`dt-category-chip ${category === cat.id ? "dt-category-chip--active" : ""}`}
            onClick={() => setCategory(cat.id)}
          >
            <span>{cat.icon}</span>
            <span>{cat.name}</span>
          </button>
        ))}
      </div>

      <div className="dt-marketplace-content">
        {Object.entries(grouped).map(([cat, connectors]) => (
          <div key={cat} className="dt-connector-group">
            <h3 className="dt-connector-group-title">
              {CATEGORIES.find((c) => c.id === cat || c.name === cat)?.icon} {cat}
              <span className="dt-connector-group-count">{connectors.length}</span>
            </h3>
            <div className="dt-connector-grid">
              {connectors.map((connector) => (
                <ConnectorCard
                  key={connector.id}
                  connector={connector}
                  selected={selectedId === connector.id}
                  onSelect={onSelect}
                />
              ))}
            </div>
          </div>
        ))}

        {filtered.length === 0 && (
          <div className="dt-marketplace-empty">
            <span className="dt-marketplace-empty-icon">🔍</span>
            <h3>No connectors found</h3>
            <p>Try adjusting your search or browse categories</p>
          </div>
        )}
      </div>
    </div>
  );
}

export function ConnectorMarketplaceStyles() {
  return (
    <style>{`
      .dt-marketplace {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-6);
      }

      .dt-marketplace-header {
        display: flex;
        align-items: flex-start;
        justify-content: space-between;
        gap: var(--dt-space-6);
        flex-wrap: wrap;
      }

      .dt-marketplace-title {
        font-size: var(--dt-text-2xl);
        font-weight: 700;
        color: var(--dt-text);
        margin-bottom: var(--dt-space-2);
      }

      .dt-marketplace-subtitle {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
      }

      .dt-marketplace-search {
        position: relative;
        min-width: 300px;
      }

      .dt-marketplace-search svg {
        position: absolute;
        left: 16px;
        top: 50%;
        transform: translateY(-50%);
        color: var(--dt-text-muted);
        pointer-events: none;
      }

      .dt-marketplace-search .dt-input {
        padding-left: 48px;
        width: 100%;
      }

      .dt-marketplace-categories {
        display: flex;
        flex-wrap: wrap;
        gap: var(--dt-space-2);
        padding: var(--dt-space-4);
        background: rgba(255, 255, 255, 0.02);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
      }

      .dt-category-chip {
        display: inline-flex;
        align-items: center;
        gap: var(--dt-space-2);
        padding: var(--dt-space-2) var(--dt-space-4);
        font-family: inherit;
        font-size: var(--dt-text-sm);
        font-weight: 500;
        color: var(--dt-text-secondary);
        background: transparent;
        border: 1px solid transparent;
        border-radius: var(--dt-radius-full);
        cursor: pointer;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-category-chip:hover {
        background: rgba(255, 255, 255, 0.05);
        color: var(--dt-text);
      }

      .dt-category-chip--active {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
        color: var(--dt-electric);
      }

      .dt-connector-group {
        margin-bottom: var(--dt-space-8);
      }

      .dt-connector-group-title {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin-bottom: var(--dt-space-4);
      }

      .dt-connector-group-count {
        padding: 2px 10px;
        font-size: var(--dt-text-xs);
        font-weight: 600;
        color: var(--dt-text-tertiary);
        background: rgba(255, 255, 255, 0.05);
        border-radius: var(--dt-radius-full);
      }

      .dt-connector-grid {
        display: grid;
        grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
        gap: var(--dt-space-4);
      }

      .dt-connector-card {
        position: relative;
        display: flex;
        align-items: flex-start;
        gap: var(--dt-space-4);
        padding: var(--dt-space-5);
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-xl);
        cursor: pointer;
        font-family: inherit;
        text-align: left;
        overflow: hidden;
        transition: all var(--dt-duration-normal) var(--dt-ease);
      }

      .dt-connector-card:hover {
        border-color: var(--dt-border-strong);
        transform: translateY(-2px);
        box-shadow: var(--dt-shadow-lg);
      }

      .dt-connector-card--selected {
        border-color: var(--dt-electric);
        background: var(--dt-electric-dim);
      }

      .dt-connector-card--selected:hover {
        border-color: var(--dt-electric);
      }

      .dt-connector-card-glow {
        position: absolute;
        top: 0;
        left: 0;
        width: 100%;
        height: 3px;
        background: var(--connector-color);
        opacity: 0;
        transition: opacity var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-connector-card:hover .dt-connector-card-glow,
      .dt-connector-card--selected .dt-connector-card-glow {
        opacity: 1;
      }

      .dt-connector-card-icon {
        width: 48px;
        height: 48px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: rgba(255, 255, 255, 0.05);
        border-radius: var(--dt-radius-lg);
        font-size: 24px;
        flex-shrink: 0;
      }

      .dt-connector-card-content {
        flex: 1;
        min-width: 0;
      }

      .dt-connector-card-name {
        display: block;
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin-bottom: var(--dt-space-1);
      }

      .dt-connector-card-desc {
        display: block;
        font-size: var(--dt-text-xs);
        color: var(--dt-text-tertiary);
        line-height: 1.4;
      }

      .dt-connector-card-status {
        flex-shrink: 0;
      }

      .dt-connector-card-check {
        position: absolute;
        top: var(--dt-space-3);
        right: var(--dt-space-3);
        width: 24px;
        height: 24px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-electric);
        color: var(--dt-black);
        border-radius: 50%;
      }

      .dt-marketplace-empty {
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: center;
        padding: var(--dt-space-16);
        text-align: center;
      }

      .dt-marketplace-empty-icon {
        font-size: 48px;
        margin-bottom: var(--dt-space-4);
      }

      .dt-marketplace-empty h3 {
        font-size: var(--dt-text-lg);
        font-weight: 600;
        color: var(--dt-text);
        margin-bottom: var(--dt-space-2);
      }

      .dt-marketplace-empty p {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-tertiary);
      }
    `}</style>
  );
}
