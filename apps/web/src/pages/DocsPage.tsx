import { useEffect, useState } from "react";
import { DtIcon } from "../components/DtIcon";
import { PageFrame } from "../components/ui/PageFrame";
import { PageSection } from "../components/ui/PageSection";
import { PageShell } from "../components/ui/PageShell";
import { StatCard } from "../components/ui/StatCard";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import { fetchCatalogStats } from "../lib/api";

interface CatalogStats {
  total: number;
  live: number;
  beta: number;
  planned: number;
  categories: number;
  transfer_live?: number;
  connect_only?: number;
  roadmap?: number;
}

function RevealSection({ children, className = "" }: { children: React.ReactNode; className?: string }) {
  const { ref, visible } = useRevealOnScroll();
  return (
    <div ref={ref} className={`df2-docs-reveal ${visible ? "df2-docs-reveal--in" : ""} ${className}`.trim()}>
      {children}
    </div>
  );
}

function ArchitectureDiagram() {
  const width = 1200;
  const height = 320;
  const y = 160;
  const nodes = [
    { x: 80, w: 150, label: "Sources", sub: "Files · Databases · Warehouses · SaaS" },
    { x: 280, w: 120, label: "Ingestion", sub: "Parse · Profile · Normalize" },
    { x: 440, w: 120, label: "Canonical", sub: "Schema · Types · Keys" },
    { x: 600, w: 120, label: "Mapper", sub: "AI · Semantic · Rules" },
    { x: 760, w: 120, label: "Preflight", sub: "Gates · Evidence · Decision" },
    { x: 920, w: 120, label: "Execution", sub: "Chunk · Transform · Write" },
    { x: 1080, w: 150, label: "Targets", sub: "Any database · File · Warehouse" },
  ];

  const path = nodes
    .map((n, i) => {
      const x1 = n.x + (i === 0 ? n.w / 2 : 0);
      const x2 = n.x + (i === 0 ? n.w / 2 : n.w / 2);
      return i === 0 ? `M ${x1} ${y}` : `L ${x2} ${y}`;
    })
    .join(" ");

  return (
    <div className="df2-docs-architecture" aria-label="DataFlow architecture diagram">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="df2-docs-architecture-svg"
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="Data pipeline flow"
      >
        <defs>
          <marker id="df2-docs-arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="var(--df-brand, #2563eb)" />
          </marker>
        </defs>

        {nodes.map((n, i) => (
          <g key={n.label}>
            <rect
              x={n.x}
              y={y - 50}
              width={n.w}
              height={100}
              rx={12}
              className="df2-docs-arch-node"
            />
            <text
              x={n.x + n.w / 2}
              y={y - 24}
              className="df2-docs-arch-title"
              textAnchor="middle"
              dominantBaseline="middle"
            >
              {n.label}
            </text>
            <text
              x={n.x + n.w / 2}
              y={y - 4}
              className="df2-docs-arch-sub"
              textAnchor="middle"
              dominantBaseline="middle"
            >
              {n.sub.split(" · ").map((line, idx) => (
                <tspan key={line} x={n.x + n.w / 2} dy={idx === 0 ? 0 : "1.2em"}>
                  {line}
                </tspan>
              ))}
            </text>
            {n.label === "Preflight" && (
              <circle
                cx={n.x + n.w / 2}
                cy={y - 50}
                r={6}
                className="df2-docs-pulse-ring"
              />
            )}
          </g>
        ))}

        <path d={path} className="df2-docs-arch-flow" markerEnd="url(#df2-docs-arrow)" />

        <circle r="7" fill="var(--df-brand, #2563eb)" className="df2-docs-flow-particle">
          <animateMotion dur="3.5s" repeatCount="indefinite" path={path} />
        </circle>
        <circle r="5" fill="#06b6d4" className="df2-docs-flow-particle df2-docs-flow-particle--lag">
          <animateMotion dur="3.5s" begin="0.7s" repeatCount="indefinite" path={path} />
        </circle>
        <circle r="5" fill="#f59e0b" className="df2-docs-flow-particle df2-docs-flow-particle--lag2">
          <animateMotion dur="3.5s" begin="1.4s" repeatCount="indefinite" path={path} />
        </circle>
      </svg>

      <div className="df2-docs-arch-legend">
        <div className="df2-docs-legend-item">
          <span className="df2-docs-legend-dot df2-docs-legend-dot--blue" />
          <span>Structured data</span>
        </div>
        <div className="df2-docs-legend-item">
          <span className="df2-docs-legend-dot df2-docs-legend-dot--cyan" />
          <span>Semi-structured</span>
        </div>
        <div className="df2-docs-legend-item">
          <span className="df2-docs-legend-dot df2-docs-legend-dot--amber" />
          <span>Streaming / CDC</span>
        </div>
      </div>
    </div>
  );
}

function ConnectorOrbit() {
  const categories = [
    { label: "Relational", icon: "database", examples: "PostgreSQL · MySQL · SQL Server · Oracle · SQLite" },
    { label: "Document / NoSQL", icon: "layers", examples: "MongoDB · DynamoDB · Cassandra · Redis · Elasticsearch" },
    { label: "Cloud Warehouses", icon: "cloud", examples: "Snowflake · BigQuery · Redshift · Databricks · ClickHouse" },
    { label: "Object Storage", icon: "server", examples: "Amazon S3 · GCS · Azure Blob / ADLS · MinIO · R2" },
    { label: "Files & Streams", icon: "file", examples: "CSV · JSON · Parquet · Excel · Kafka · Kinesis" },
    { label: "SaaS & APIs", icon: "globe", examples: "Salesforce · HubSpot · Stripe · REST · GraphQL · gRPC" },
  ];

  return (
    <div className="df2-docs-orbit">
      {categories.map((c, i) => (
        <div
          key={c.label}
          className="df2-docs-orbit-card"
          style={{ animationDelay: `${i * 80}ms` }}
        >
          <div className="df2-docs-orbit-head">
            <DtIcon name={c.icon} size={18} />
            <strong>{c.label}</strong>
          </div>
          <p>{c.examples}</p>
        </div>
      ))}
    </div>
  );
}

const PREFLIGHT_RULES = [
  {
    id: "G1",
    title: "File Integrity",
    icon: "upload",
    desc: "Can we parse the source? Headers, encodings, malformed rows, and empty files are caught before any data moves.",
  },
  {
    id: "G2",
    title: "Schema Contract",
    icon: "database",
    desc: "Are the source types compatible with the destination? Lossy casts (e.g. VARCHAR → INTEGER) are flagged unless reversible.",
  },
  {
    id: "G3",
    title: "Type Fidelity",
    icon: "activity",
    desc: "Do values fit the target precision? Decimals, timestamps, booleans, and JSON are validated against the target contract.",
  },
  {
    id: "G4",
    title: "Mapping Confidence",
    icon: "sparkle",
    desc: "AI must be confident before auto-mapping. Low-confidence fields are escalated to the user for review.",
  },
  {
    id: "G5",
    title: "Dry-Run Integrity",
    icon: "check",
    desc: "A sample is run through the real pipeline. Duplicates, missing keys, and invalid transforms are surfaced.",
  },
  {
    id: "G6",
    title: "Target DDL Compatibility",
    icon: "shield",
    desc: "Will the target table or collection accept the data? Existing schemas, PKs, and required fields are checked.",
  },
];

function PreflightRules() {
  return (
    <div className="df2-docs-rules">
      {PREFLIGHT_RULES.map((rule, i) => (
        <div key={rule.id} className="df2-docs-rule-card" style={{ animationDelay: `${i * 70}ms` }}>
          <div className="df2-docs-rule-id">{rule.id}</div>
          <div className="df2-docs-rule-body">
            <div className="df2-docs-rule-title">
              <DtIcon name={rule.icon} size={16} />
              <strong>{rule.title}</strong>
            </div>
            <p>{rule.desc}</p>
          </div>
        </div>
      ))}
    </div>
  );
}

function TrustPillars() {
  const pillars = [
    {
      icon: "shield",
      title: "Zero data loss",
      desc: "Every field is tracked from source to target. Rejected rows are quarantined, not silently dropped.",
    },
    {
      icon: "key",
      title: "Type-safe transfers",
      desc: "Numeric, timestamp, boolean, and JSON values are normalized using a canonical type system before writing.",
    },
    {
      icon: "trend",
      title: "End-to-end lineage",
      desc: "Source path, target path, transform, confidence, and evidence are recorded for every mapping.",
    },
    {
      icon: "users",
      title: "Compliance ready",
      desc: "PII detection, hashing, de-identification, and audit logging are built in, not bolted on.",
    },
  ];

  return (
    <div className="df2-docs-pillars">
      {pillars.map((p, i) => (
        <div key={p.title} className="df2-docs-pillar" style={{ animationDelay: `${i * 80}ms` }}>
          <div className="df2-docs-pillar-icon">
            <DtIcon name={p.icon} size={22} />
          </div>
          <strong>{p.title}</strong>
          <p>{p.desc}</p>
        </div>
      ))}
    </div>
  );
}

function UseCase({ title, body }: { title: string; body: string }) {
  return (
    <div className="df2-docs-use-case">
      <strong>{title}</strong>
      <p>{body}</p>
    </div>
  );
}

export function DocsPage() {
  const [stats, setStats] = useState<CatalogStats | null>(null);
  const [statsError, setStatsError] = useState(false);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setStats(s))
      .catch(() => setStatsError(true));
  }, []);

  const transferLive = stats?.transfer_live ?? stats?.live ?? 130;
  const total = stats?.total ?? 734;
  const planned = stats?.planned ?? 600;

  return (
    <PageShell
      title="Docs"
      description="How DataFlow plans, maps, validates, and proves every transfer."
      fit={false}
      className="df2-page-docs"
    >
      <PageFrame className="df2-docs-workspace">
      <div className="df2-docs dt-stagger">
        <RevealSection>
          <PageSection title="Architecture" subtitle="One canonical pipeline for every source and destination" asCard>
            <ArchitectureDiagram />
          </PageSection>
        </RevealSection>

        <RevealSection>
          <div className="df2-docs-stats">
            <StatCard label="Connector catalog" value={total.toLocaleString()} sub={statsError ? "Catalog offline" : "Source and destination connectors"} icon="database" tone="blue" />
            <StatCard label="Live routes" value="10,000+" sub="Any source to any destination" icon="transfer" tone="teal" />
            <StatCard label="Preflight gates" value="6" sub="Hard and soft validation gates" icon="shield" tone="green" />
            <StatCard label="Test coverage" value="753" sub="API tests + preflight suite" icon="check" tone="default" />
          </div>
        </RevealSection>

        <RevealSection>
          <PageSection title="Data flow" subtitle="From any source to any verified target">
            <img
              src="/docs/pipeline.png"
              alt="DataFlow pipeline illustration"
              className="df2-docs-illustration"
              loading="lazy"
            />
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="Connector coverage" subtitle="A single platform for the systems you already use">
            <ConnectorOrbit />
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="Preflight rules" subtitle="Why transfers are blocked or approved">
            <PreflightRules />
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="Trust pillars" subtitle="Built for data governance and compliance">
            <TrustPillars />
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="How it works" subtitle="From raw source to verified target">
            <div className="df2-docs-steps">
              <div className="df2-docs-step">
                <div className="df2-docs-step-number">1</div>
                <strong>Ingest & profile</strong>
                <p>CSV, JSON, databases, and SaaS APIs are parsed, profiled, and reduced to a clean sample with inferred types.</p>
              </div>
              <div className="df2-docs-step">
                <div className="df2-docs-step-number">2</div>
                <strong>Build the canonical model</strong>
                <p>Every column is normalized into a logical type (VARCHAR, INTEGER, DECIMAL, TIMESTAMP, BOOLEAN, JSON, BINARY, etc.).</p>
              </div>
              <div className="df2-docs-step">
                <div className="df2-docs-step-number">3</div>
                <strong>Map with AI + rules</strong>
                <p>Exact matches, aliases, type compatibility, semantic similarity, and historical lineage generate ranked mappings.</p>
              </div>
              <div className="df2-docs-step">
                <div className="df2-docs-step-number">4</div>
                <strong>Run preflight gates</strong>
                <p>Dry-run, type coercion, duplicate checks, DDL compatibility, and confidence thresholds produce a go/no-go decision.</p>
              </div>
              <div className="df2-docs-step">
                <div className="df2-docs-step-number">5</div>
                <strong>Execute & validate</strong>
                <p>Rows are chunked, transformed, written, and reconciled. Rejected rows are quarantined and reported.</p>
              </div>
            </div>
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="Real-world use cases" subtitle="From logistics to warehouses">
            <div className="df2-docs-use-cases">
              <UseCase
                title="Logistics CSV → Snowflake"
                body="A 3PL uploads CSV shipment manifests with mixed date formats, currency strings, and empty cells. DataFlow profiles every column, maps to the warehouse schema, validates numbers, and writes clean rows without losing the original records."
              />
              <UseCase
                title="MongoDB → PostgreSQL"
                body="Nested documents are flattened, arrays are handled, ObjectId values are preserved, and JSONB fields are typed. Duplicate keys are caught in preflight before any write."
              />
              <UseCase
                title="S3 JSONL → BigQuery"
                body="Streaming JSON Lines are batched, timestamps are normalized, nullable structs are mapped to BigQuery RECORD columns, and the job emits lineage for every field."
              />
              <UseCase
                title="ERP → CRM sync"
                body="Schedules keep Salesforce or HubSpot in sync with an ERP or data warehouse. Incremental cursors, upsert semantics, and soft deletes are supported."
              />
            </div>
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="Security & compliance" subtitle="Defense in depth for sensitive data">
            <ul className="df2-docs-list">
              <li><strong>Encryption in transit and at rest</strong> — all cloud connectors use TLS by default; credentials are encrypted at rest.</li>
              <li><strong>Credential isolation</strong> — connection secrets are scoped to the connector, never logged in plain text, and masked in UI samples.</li>
              <li><strong>PII / PHI detection</strong> — columns are scanned for sensitive patterns and can be hashed, masked, or redacted before leaving the source.</li>
              <li><strong>Quarantine and audit</strong> — malformed rows, type violations, and transform failures are quarantined with row-level evidence and written to lineage logs.</li>
              <li><strong>Least-privilege access</strong> — connectors use read-only or write-only credentials where possible; the platform does not require database superuser rights.</li>
            </ul>
          </PageSection>
        </RevealSection>

        <RevealSection>
          <PageSection title="What to do when a transfer is blocked" subtitle="Reading the preflight result">
            <div className="df2-docs-faq">
              <p><strong>Mapping confidence too low?</strong> Review the map step, pin the correct target column, and the gate will re-score.</p>
              <p><strong>Target DDL incompatible?</strong> Check the target schema. DataFlow can create the target for you or report a type/precision conflict.</p>
              <p><strong>Dry-run integrity failed?</strong> Look for duplicate primary keys, 100% null columns, or values that cannot be cast to the target type.</p>
              <p><strong>Data type mismatch in CSV?</strong> Use a transform (date, decimal, integer, boolean) or change the inferred column type in the mapping step.</p>
            </div>
          </PageSection>
        </RevealSection>
      </div>
      </PageFrame>
    </PageShell>
  );
}
