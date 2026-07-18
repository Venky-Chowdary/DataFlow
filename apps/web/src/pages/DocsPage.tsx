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

function RevealSection({
  children,
  className = "",
  id,
}: {
  children: React.ReactNode;
  className?: string;
  id?: string;
}) {
  const { ref, visible } = useRevealOnScroll();
  return (
    <section
      id={id}
      ref={ref}
      className={`df2-docs-reveal ${visible ? "df2-docs-reveal--in" : ""} ${className}`.trim()}
    >
      {children}
    </section>
  );
}

const DOC_SECTIONS = [
  { id: "architecture", label: "Architecture", icon: "layers" },
  { id: "coverage", label: "Connector coverage", icon: "connectors" },
  { id: "pipeline", label: "How it works", icon: "transfer" },
  { id: "preflight", label: "Preflight gates", icon: "shield" },
  { id: "trust", label: "Trust & fidelity", icon: "check" },
  { id: "use-cases", label: "Use cases", icon: "database" },
  { id: "security", label: "Security & compliance", icon: "lock" },
  { id: "troubleshooting", label: "Troubleshooting", icon: "alert" },
] as const;

function DocsHero({
  transferLive,
  total,
  activeSection,
  onJump,
}: {
  transferLive: number;
  total: number;
  activeSection: string;
  onJump: (id: string) => void;
}) {
  return (
    <header className="df2-docs-hero">
      <div className="df2-docs-hero-copy">
        <span className="df2-docs-hero-kicker">
          <DtIcon name="book" size={14} /> Product documentation
        </span>
        <h2 className="df2-docs-hero-title">Move any data, prove every row.</h2>
        <p className="df2-docs-hero-sub">
          DataFlow runs one canonical pipeline — profile, map, validate, execute, reconcile —
          so any source reaches any destination with zero silent data loss. Everything below is
          how it works, end to end.
        </p>
        <div className="df2-docs-hero-actions">
          <button type="button" className="df2-btn df2-btn-primary df2-btn-sm" onClick={() => onJump("pipeline")}>
            <DtIcon name="transfer" size={14} /> How a transfer runs
          </button>
          <button type="button" className="df2-btn df2-btn-sm" onClick={() => onJump("preflight")}>
            <DtIcon name="shield" size={14} /> Preflight gates
          </button>
        </div>
        <div className="df2-docs-hero-stats">
          <div className="df2-docs-hero-stat">
            <strong>{transferLive.toLocaleString()}</strong>
            <span>Transfer-ready connectors</span>
          </div>
          <div className="df2-docs-hero-stat">
            <strong>{total.toLocaleString()}</strong>
            <span>Catalog connectors</span>
          </div>
          <div className="df2-docs-hero-stat">
            <strong>8</strong>
            <span>Preflight gates</span>
          </div>
          <div className="df2-docs-hero-stat">
            <strong>10k+</strong>
            <span>Any-to-any routes</span>
          </div>
        </div>
      </div>
      <nav className="df2-docs-hero-progress" aria-label="On this page">
        <span className="df2-docs-hero-progress-label">On this page</span>
        <ol>
          {DOC_SECTIONS.map((s) => (
            <li key={s.id}>
              <button
                type="button"
                className={activeSection === s.id ? "is-active" : ""}
                onClick={() => onJump(s.id)}
              >
                <span className="df2-docs-hero-progress-dot" aria-hidden />
                {s.label}
              </button>
            </li>
          ))}
        </ol>
      </nav>
    </header>
  );
}

function ArchitectureDiagram() {
  const width = 1200;
  const height = 380;
  const y = 168;
  const nodes = [
    { x: 48, w: 142, label: "Sources", sub: "Files · DBs · Warehouses · SaaS", tone: "edge" },
    { x: 230, w: 118, label: "Ingestion", sub: "Parse · Profile · Normalize", tone: "core" },
    { x: 384, w: 118, label: "Canonical", sub: "Schema · Types · Keys", tone: "core" },
    { x: 538, w: 118, label: "Mapper", sub: "AI · Semantic · Rules", tone: "core" },
    { x: 692, w: 118, label: "Preflight", sub: "8 gates · Evidence", tone: "gate" },
    { x: 846, w: 118, label: "Execution", sub: "Chunk · Transform · Write", tone: "core" },
    { x: 1004, w: 148, label: "Targets", sub: "DB · File · Warehouse", tone: "edge" },
  ];

  const path = nodes
    .map((n, i) => {
      const x = n.x + n.w / 2;
      return i === 0 ? `M ${x} ${y}` : `L ${x} ${y}`;
    })
    .join(" ");

  const surfaces = [
    { label: "Transfer Studio", x: 180 },
    { label: "Data Pilot", x: 420 },
    { label: "MCP Agents", x: 640 },
    { label: "Pipelines API", x: 880 },
  ];

  return (
    <div className="df2-docs-architecture" aria-label="DataFlow architecture diagram">
      <div className="df2-docs-arch-planes">
        <span className="df2-docs-arch-plane-tag">Control surfaces</span>
        <span className="df2-docs-arch-plane-tag is-data">Canonical data plane</span>
      </div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="df2-docs-architecture-svg"
        preserveAspectRatio="xMidYMid meet"
        role="img"
        aria-label="Data pipeline flow from sources through preflight to targets"
      >
        <defs>
          <linearGradient id="df2-docs-flow-grad" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stopColor="#14b8a6" />
            <stop offset="55%" stopColor="#0d9488" />
            <stop offset="100%" stopColor="#0f766e" />
          </linearGradient>
          <marker id="df2-docs-arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="#0d9488" />
          </marker>
          <filter id="df2-docs-soft" x="-20%" y="-20%" width="140%" height="140%">
            <feDropShadow dx="0" dy="4" stdDeviation="6" floodColor="#0f172a" floodOpacity="0.08" />
          </filter>
        </defs>

        <rect x="36" y="36" width="1128" height="64" rx="14" className="df2-docs-arch-surface-band" />
        {surfaces.map((s) => (
          <g key={s.label}>
            <rect x={s.x} y="48" width="140" height="40" rx="10" className="df2-docs-arch-surface" />
            <text x={s.x + 70} y="72" textAnchor="middle" className="df2-docs-arch-surface-label">{s.label}</text>
          </g>
        ))}

        <path d={path} className="df2-docs-arch-flow" markerEnd="url(#df2-docs-arrow)" />

        {nodes.map((n) => (
          <g key={n.label} filter="url(#df2-docs-soft)">
            <rect
              x={n.x}
              y={y - 52}
              width={n.w}
              height={104}
              rx={14}
              className={`df2-docs-arch-node df2-docs-arch-node--${n.tone}`}
            />
            <text
              x={n.x + n.w / 2}
              y={y - 22}
              className="df2-docs-arch-title"
              textAnchor="middle"
              dominantBaseline="middle"
            >
              {n.label}
            </text>
            <text
              x={n.x + n.w / 2}
              y={y + 2}
              className="df2-docs-arch-sub"
              textAnchor="middle"
              dominantBaseline="middle"
            >
              {n.sub.split(" · ").map((line, idx) => (
                <tspan key={line} x={n.x + n.w / 2} dy={idx === 0 ? 0 : "1.25em"}>
                  {line}
                </tspan>
              ))}
            </text>
            {n.tone === "gate" && (
              <circle cx={n.x + n.w / 2} cy={y - 52} r={6} className="df2-docs-pulse-ring" />
            )}
          </g>
        ))}

        <circle r="7" fill="url(#df2-docs-flow-grad)" className="df2-docs-flow-particle">
          <animateMotion dur="3.5s" repeatCount="indefinite" path={path} />
        </circle>
        <circle r="5" fill="#06b6d4" className="df2-docs-flow-particle df2-docs-flow-particle--lag">
          <animateMotion dur="3.5s" begin="0.7s" repeatCount="indefinite" path={path} />
        </circle>
        <circle r="5" fill="#f59e0b" className="df2-docs-flow-particle df2-docs-flow-particle--lag2">
          <animateMotion dur="3.5s" begin="1.4s" repeatCount="indefinite" path={path} />
        </circle>

        <text x="600" y="320" textAnchor="middle" className="df2-docs-arch-footnote">
          One engine under every surface — profile → map → validate → execute → reconcile
        </text>
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
        <div className="df2-docs-legend-item">
          <span className="df2-docs-legend-dot df2-docs-legend-dot--teal" />
          <span>Preflight gate</span>
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
    title: "Nullability & Keys",
    icon: "key",
    desc: "Required fields, primary keys, and uniqueness constraints are checked against sampled values before write.",
  },
  {
    id: "G6",
    title: "Destination Probe",
    icon: "server",
    desc: "Connectivity, privileges, and target object presence are verified with a live probe — not assumed.",
  },
  {
    id: "G7",
    title: "Capacity & Dry-Run",
    icon: "check",
    desc: "A sample runs through the real pipeline. Volume estimates, duplicates, and invalid transforms are surfaced.",
  },
  {
    id: "G8",
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
  const [activeSection, setActiveSection] = useState<string>(DOC_SECTIONS[0].id);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setStats(s))
      .catch(() => setStatsError(true));
  }, []);

  useEffect(() => {
    const els = DOC_SECTIONS
      .map((s) => document.getElementById(s.id))
      .filter((el): el is HTMLElement => Boolean(el));
    if (els.length === 0) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => b.intersectionRatio - a.intersectionRatio);
        if (visible[0]) setActiveSection(visible[0].target.id);
      },
      { rootMargin: "-18% 0px -68% 0px", threshold: [0, 0.25, 0.5] },
    );
    els.forEach((el) => observer.observe(el));
    return () => observer.disconnect();
  }, []);

  const jumpTo = (id: string) => {
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
    setActiveSection(id);
  };

  const transferLive = stats?.transfer_live ?? stats?.live ?? 130;
  const total = stats?.total ?? 734;

  return (
    <PageShell
      title="Help & documentation"
      description="How DataFlow plans, maps, validates, and proves every transfer."
      fit={false}
      className="df2-page-docs"
    >
      <PageFrame className="df2-docs-workspace">
      <DocsHero transferLive={transferLive} total={total} activeSection={activeSection} onJump={jumpTo} />

      <div className="df2-docs-shell">
        <aside className="df2-docs-toc" aria-label="Documentation sections">
          <span className="df2-docs-toc-label">Contents</span>
          <nav>
            {DOC_SECTIONS.map((s) => (
              <button
                key={s.id}
                type="button"
                className={`df2-docs-toc-item ${activeSection === s.id ? "is-active" : ""}`}
                onClick={() => jumpTo(s.id)}
                aria-current={activeSection === s.id ? "true" : undefined}
              >
                <DtIcon name={s.icon} size={15} />
                <span>{s.label}</span>
              </button>
            ))}
          </nav>
          <div className="df2-docs-toc-card">
            <StatCard
              label="Connector catalog"
              value={total.toLocaleString()}
              sub={statsError ? "Catalog offline" : `${transferLive.toLocaleString()} transfer-ready`}
              icon="database"
              tone="blue"
            />
          </div>
        </aside>

        <div className="df2-docs dt-stagger">
        <RevealSection id="architecture">
          <PageSection title="Architecture" subtitle="One canonical pipeline for every source and destination" asCard>
            <ArchitectureDiagram />
            <img
              src="/docs/pipeline.png"
              alt="DataFlow pipeline illustration — source ingestion through verified target write"
              className="df2-docs-illustration"
              loading="lazy"
            />
          </PageSection>
        </RevealSection>

        <RevealSection id="coverage">
          <PageSection title="Connector coverage" subtitle="A single platform for the systems you already use">
            <ConnectorOrbit />
          </PageSection>
        </RevealSection>

        <RevealSection id="pipeline">
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

        <RevealSection id="preflight">
          <PageSection title="Preflight gates" subtitle="Why transfers are blocked or approved before any write">
            <PreflightRules />
          </PageSection>
        </RevealSection>

        <RevealSection id="trust">
          <PageSection title="Trust & fidelity" subtitle="Built for data governance and compliance">
            <TrustPillars />
          </PageSection>
        </RevealSection>

        <RevealSection id="use-cases">
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

        <RevealSection id="security">
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

        <RevealSection id="troubleshooting">
          <PageSection title="Troubleshooting a blocked transfer" subtitle="Reading the preflight result">
            <div className="df2-docs-faq">
              <p><strong>Mapping confidence too low?</strong> Review the map step, pin the correct target column, and the gate will re-score.</p>
              <p><strong>Target DDL incompatible?</strong> Check the target schema. DataFlow can create the target for you or report a type/precision conflict.</p>
              <p><strong>Dry-run integrity failed?</strong> Look for duplicate primary keys, 100% null columns, or values that cannot be cast to the target type.</p>
              <p><strong>Data type mismatch in CSV?</strong> Use a transform (date, decimal, integer, boolean) or change the inferred column type in the mapping step.</p>
            </div>
          </PageSection>
        </RevealSection>
        </div>
      </div>
      </PageFrame>
    </PageShell>
  );
}
