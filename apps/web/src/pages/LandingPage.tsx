import { useState, type ReactNode } from "react";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import {
  MappingCinema,
  ProofCinema,
} from "../components/landing/AlgorithmCinema";
import { TrustSection } from "../components/landing/TrustSection";
import { ProofEvidenceSection } from "../components/landing/ProofEvidenceSection";
import { ProductShot } from "../components/marketing/ProductShot";
import { ObservabilityInAction } from "../components/landing/ObservabilityInAction";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import { EVIDENCE_AS_OF, TRANSFER_READY_DRIVERS, catalogHonestyLead } from "../lib/provenEvidence";
import { connectorDisplayName } from "../lib/connectorTypes";
import type { PublicRoute } from "../lib/publicNavigation";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

const STACK_IDS = [
  "postgresql",
  "snowflake",
  "bigquery",
  "sqlserver",
  "oracle",
  "mongodb",
  "mysql",
  "kafka",
  "s3",
];

/** Marquee rows — transfer-ready drivers only (transfer_live_driver_types()). */
const CONNECTOR_MARQUEE_IDS = [
  "postgresql",
  "mysql",
  "sqlserver",
  "oracle",
  "mongodb",
  "snowflake",
  "bigquery",
  "iceberg",
  "s3",
  "adls",
  "gcs",
  "kafka",
  "pgvector",
  "qdrant",
  "elasticsearch",
  "dynamodb",
  "redis",
  "salesforce",
  "hubspot",
  "stripe",
  "parquet",
  "csv",
  "json",
  "sftp",
];

const CONNECTOR_MARQUEE_ROW_B = [
  "snowflake",
  "kafka",
  "postgresql",
  "bigquery",
  "pinecone",
  "sqlserver",
  "redis",
  "mongodb",
  "gcs",
  "oracle",
  "weaviate",
  "mysql",
  "s3",
  "neo4j",
  "salesforce",
  "iceberg",
  "milvus",
  "couchbase",
  "elasticsearch",
  "adls",
  "stripe",
  "dynamodb",
  "sqlite",
  "hubspot",
];

function ConnectorMarquee({
  ids,
  reverse = false,
  duration = 42,
}: {
  ids: string[];
  reverse?: boolean;
  duration?: number;
}) {
  const loop = [...ids, ...ids];
  return (
    <div className={`lp-conn-marquee${reverse ? " is-reverse" : ""}`} aria-hidden>
      <div className="lp-conn-marquee-track" style={{ ["--lp-marquee-dur" as string]: `${duration}s` }}>
        {loop.map((id, i) => (
          <span key={`${id}-${i}`} className="lp-conn-marquee-tile">
            <ConnectorIcon id={id} size={30} />
            <em>{connectorDisplayName(id)}</em>
          </span>
        ))}
      </div>
    </div>
  );
}

const SURFACE_SHOTS: Record<string, string> = {
  studio: "/docs/screenshots/app-transfer-source.png",
  theater: "/docs/screenshots/app-jobs.png",
  pipelines: "/docs/screenshots/app-pipelines.png",
  mcp: "/docs/screenshots/app-mcp.png",
  query: "/docs/screenshots/app-query.png",
};

const SURFACES: {
  id: string;
  label: string;
  title: string;
  body: string;
  route: PublicRoute;
  cta: string;
}[] = [
  {
    id: "studio",
    label: "Studio",
    title: "Transfer Studio plans every load",
    body: "Connect source and destination, review semantic maps with confidence scores, clear nine preflight gates, then write with quarantine and value-by-value verification. Pilot and MCP run the exact same path.",
    route: "product-transfer",
    cta: "Open Transfer Studio",
  },
  {
    id: "theater",
    label: "Theater",
    title: "Job Theater proves what ran",
    body: "Live phases, quarantine samples with reasons, and a Gate-8 verification report read back from the destination. Every finished job comes with proof you can export.",
    route: "product-jobs",
    cta: "See Job Theater",
  },
  {
    id: "pipelines",
    label: "Pipelines",
    title: "Schedules that run every gate",
    body: "Typed watermarks, upsert, append, mirror with soft delete, or native CDC — every tick is a real, verified job. Schema drift waits for your review in Map before the next run.",
    route: "product-pipelines",
    cta: "Explore Pipelines",
  },
  {
    id: "mcp",
    label: "MCP",
    title: "Agents inherit the same gates",
    body: "Agents work through secret references, under workspace RBAC and API keys capped at their creator's role. They map, preflight, and verify on the same engine as the UI.",
    route: "product-mcp",
    cta: "Read MCP docs",
  },
  {
    id: "query",
    label: "Query",
    title: "Explore live data safely, then load it in Studio",
    body: "Query Playground runs read-only SELECT and WITH against saved connectors. When a slice is ready, Transfer Studio writes it — table, destination INSERT/MERGE, or a stored procedure call, all with binds and quarantine.",
    route: "product-query",
    cta: "Open Query Playground",
  },
];

function Reveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  const reveal = useRevealOnScroll();
  return (
    <div ref={reveal.ref} className={`${reveal.className} ${className}`.trim()}>
      {children}
    </div>
  );
}

function SurfaceTabs({ onNavigate, onGetStarted }: Pick<LandingHomeProps, "onNavigate" | "onGetStarted">) {
  const [active, setActive] = useState(SURFACES[0].id);
  const current = SURFACES.find((s) => s.id === active) ?? SURFACES[0];

  return (
    <section className="lp-home-surfaces" id="product" aria-label="Product surfaces">
      <div className="lp-home-surfaces-inner">
        <Reveal className="lp-home-section-head">
          <p className="lp-section-kicker">For operators &amp; agents</p>
          <h2>Your control plane. Our governed engine.</h2>
          <p>
            Use Transfer Studio, Job Theater, Pipelines, or MCP. Datawrap manages mapping, gates,
            quarantine, and proof underneath every surface.
          </p>
        </Reveal>

        <div className="lp-home-tabs" role="tablist" aria-label="Product surfaces">
          {SURFACES.map((s) => (
            <button
              key={s.id}
              type="button"
              role="tab"
              aria-selected={s.id === active}
              className={`lp-home-tab ${s.id === active ? "is-active" : ""}`}
              onClick={() => setActive(s.id)}
            >
              {s.label}
            </button>
          ))}
        </div>

        <div className="lp-home-tab-panel" role="tabpanel">
          <div className="lp-home-tab-copy">
            <h3>{current.title}</h3>
            <p>{current.body}</p>
            <div className="lp-home-tab-actions">
              <button
                type="button"
                className="lp-btn lp-btn--brand"
                onClick={() => (current.id === "studio" ? onGetStarted() : onNavigate(current.route))}
              >
                {current.cta}
              </button>
              <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate(current.route)}>
                Learn more →
              </button>
            </div>
          </div>
          <div className="lp-home-tab-visual">
            <ProductShot
              src={SURFACE_SHOTS[current.id] ?? "/docs/screenshots/app-overview.png"}
              alt={`${current.title} in the Datawrap workspace`}
              surface={current.label}
            />
          </div>
        </div>
      </div>
    </section>
  );
}

/** Home — Airbyte-class composition, Datawrap product truth. */
export function LandingHome({ onLogin: _onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  return (
    <>
      {/* 1) Hero — brand-first, one headline, one visual */}
      <section className="lp-hero lp-hero--home lp-hero--home-ink">
        <div className="lp-hero-home-bg" aria-hidden />
        <div className="lp-hero-home-grid">
          <div className="lp-hero-copy">
            <p className="lp-hero-eyebrow">
              <span className="lp-hero-eyebrow-dot" aria-hidden />
              Universal data movement with proof
            </p>
            <h1 className="lp-hero-title">
              <span className="lp-hero-title-a">Move any schema</span>
              <span className="lp-hero-title-b">anywhere — proven.</span>
            </h1>
            <p className="lp-hero-sub">
              Datawrap maps your columns by meaning, clears nine preflight gates before it writes,
              then reads every load back from the destination and verifies it value by value. If
              anything doesn&apos;t match, Datawrap tells you instead of reporting success. One
              governed path for Studio, Pipelines, Pilot, and MCP.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Try Datawrap free
                <DtIcon name="arrow-right" size={16} />
              </button>
              <button
                type="button"
                className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink"
                onClick={() => onNavigate("contact")}
              >
                Contact sales
              </button>
            </div>
            <p className="lp-hero-meta">
              {TRANSFER_READY_DRIVERS} transfer-ready drivers
              <span aria-hidden>·</span>
              G1–G9 preflight
              <span aria-hidden>·</span>
              Gate-8 read-back
              <span aria-hidden>·</span>
              4 native CDC engines
            </p>
          </div>
          <div className="lp-hero-visual lp-hero-visual--stage">
            <ProductShot
              src="/docs/screenshots/app-jobs.png"
              alt="Datawrap Job Theater — whole-history counts, destination population, and run proof"
              surface="Job Theater · workspace"
              route={{ source: "PostgreSQL · public.orders", dest: "Snowflake · ANALYTICS.ORDERS" }}
            />
          </div>
        </div>
      </section>

      {/* 2) Stack strip */}
      <section className="lp-home-stack" aria-label="Works with your stack">
        <p className="lp-home-stack-label">Works with the stacks you already run</p>
        <div className="lp-home-stack-row">
          {STACK_IDS.map((id) => (
            <span key={id} className="lp-home-stack-item" title={connectorDisplayName(id)}>
              <ConnectorIcon id={id} size={28} />
              <em>{connectorDisplayName(id)}</em>
            </span>
          ))}
        </div>
      </section>

      {/* 3) Problem — Airbyte-style pain list, our product framing */}
      <section className="lp-home-problem" id="why">
        <div className="lp-home-problem-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Built for the questions operators get asked</p>
            <h2>Did every row land, exactly as it left?</h2>
            <p>
              Datawrap answers with evidence: which column went where, which values were held back
              and why, and a destination read-back that matches the source value for value.
            </p>
          </Reveal>
          <ol className="lp-home-pain-list lp-home-pain-rail">
            <li className="is-ink">
              <span className="lp-home-pain-num">01</span>
              <div>
                <h3>Mapping by meaning</h3>
                <p>
                  <code>order_amt</code> pairs with <code>total_amount</code> because the order
                  qualifier matches — and stays clear of <code>payment_amount</code>. Same-role
                  collisions wait for your review in Map.
                </p>
              </div>
            </li>
            <li className="is-soft">
              <span className="lp-home-pain-num">02</span>
              <div>
                <h3>Exact types, end to end</h3>
                <p>
                  Decimals stay exact, timestamps keep every microsecond (and DATETIME2&apos;s
                  seventh digit), and Unicode text lands on NVARCHAR. A value that can&apos;t fit
                  goes to quarantine with its reason and a replay path.
                </p>
              </div>
            </li>
            <li className="is-outline">
              <span className="lp-home-pain-num">03</span>
              <div>
                <h3>Proof you can archive</h3>
                <p>
                  Row counts, value and key census, and a signed proof pack linked into a
                  tamper-evident audit chain — ready for finance and compliance.
                </p>
              </div>
            </li>
          </ol>
        </div>
      </section>

      {/* 4) Architecture — governed path */}
      <section className="lp-home-arch" id="platform">
        <div className="lp-home-arch-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">The architecture</p>
            <h2>One governed layer between source and destination.</h2>
            <p>
              Datawrap connects your systems, maps with roles and type fit, gates every write, and
              verifies every load — for humans and agents alike.
            </p>
          </Reveal>

          <ol className="lp-home-arch-flow" aria-label="Governed transfer path">
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">01</span>
                <span className="lp-home-arch-step-tag">Map</span>
              </div>
              <h3>Semantic mapping</h3>
              <p>
                Roles, synonyms, qualifiers, and type fit score every edge. Same-role collisions
                wait for your review in Map before anything is written.
              </p>
              <ul>
                <li>Role-aware column matching</li>
                <li>Confidence thresholds</li>
                <li>Manual review on schema drift</li>
              </ul>
            </li>
            <li className="lp-home-arch-step lp-home-arch-step--connector" aria-hidden>
              <span className="lp-home-arch-arrow">→</span>
            </li>
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">02</span>
                <span className="lp-home-arch-step-tag">Preflight</span>
              </div>
              <h3>Nine fail-fast gates</h3>
              <p>
                G1–G9 run before any write: schema, types, capacity, privileges, collation-aware
                key checks, and a dry run that sends coerce failures to quarantine with column,
                value, and reason.
              </p>
              <ul>
                <li>Schema &amp; type contracts</li>
                <li>Capacity &amp; privilege probes</li>
                <li>Dry-run coerce samples</li>
              </ul>
            </li>
            <li className="lp-home-arch-step lp-home-arch-step--connector" aria-hidden>
              <span className="lp-home-arch-arrow">→</span>
            </li>
            <li className="lp-home-arch-step">
              <div className="lp-home-arch-step-top">
                <span className="lp-home-arch-step-num">03</span>
                <span className="lp-home-arch-step-tag">Prove</span>
              </div>
              <h3>Write, then verify</h3>
              <p>
                Clean rows land and held-back rows quarantine in the open. Gate-8 reads the
                destination back and shows MATCH when every value and key agrees.
              </p>
              <ul>
                <li>Quarantine with replay</li>
                <li>Value &amp; key census</li>
                <li>Signed proof pack</li>
              </ul>
            </li>
          </ol>

          <div className="lp-home-arch-cta">
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Meet Transfer Studio
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
              See the algorithms →
            </button>
          </div>
        </div>
      </section>

      <ObservabilityInAction />

      <section className="lp-home-market" id="vs-market" aria-label="What sets Datawrap apart">
        <div className="lp-home-market-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">What sets Datawrap apart</p>
            <h2>Every load verified. Every exception explained.</h2>
            <p>
              Semantic maps you review, nine gates before the write, quarantine with replay, and a
              read-back from the destination engine itself. Every load is verified value by value;
              if anything doesn&apos;t match, Datawrap tells you instead of reporting success.
            </p>
          </Reveal>
          <div className="lp-home-market-grid">
            <article>
              <span>Verify</span>
              <h3>Gate-8 read-back</h3>
              <p>
                After the write, Datawrap reads the destination back and runs a value and key
                census against the source. The job shows MATCH only when they agree.
              </p>
            </article>
            <article>
              <span>Govern</span>
              <h3>Signed risk contracts</h3>
              <p>
                An intentional cast or truncation is a signed, immutable contract that states what
                may change and how the writer behaves — recorded with the job.
              </p>
            </article>
            <article>
              <span>Recover</span>
              <h3>Quarantine with replay</h3>
              <p>
                Held-back rows keep column, value, and reason in a durable, workspace-scoped queue.
                Fix the source or the map and replay them into the same destination.
              </p>
            </article>
            <article className="is-ours">
              <span>Datawrap</span>
              <h3>Map · G1–G9 · quarantine · MATCH</h3>
              <p>
                One engine behind Transfer Studio, Pipelines, Pilot, and MCP. Destination INSERT,
                MERGE, and stored-procedure writes use binds, with quarantine on every path.
              </p>
            </article>
          </div>
        </div>
      </section>

      <section className="lp-home-depth" id="engine-depth" aria-label="Engine capabilities">
        <div className="lp-home-depth-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">What the engine does</p>
            <h2>Every surface runs the same algorithms.</h2>
            <p>
              Studio, Pipelines, Pilot, and MCP share one engine. Each item below is a shipped
              product capability.
            </p>
          </Reveal>
          <ul className="lp-home-depth-list">
            <li>
              <strong>Semantic map</strong>
              <p>Role, qualifier, and type fit score every edge. Ambiguous pairs wait for review.</p>
            </li>
            <li>
              <strong>Nine preflight gates</strong>
              <p>G1–G9 clear before every write, from Studio, schedules, chat, and agents alike.</p>
            </li>
            <li>
              <strong>Post-write verification</strong>
              <p>Gate-8 reads the destination back: row counts plus a value and key census.</p>
            </li>
            <li>
              <strong>Quarantine &amp; replay</strong>
              <p>Column, value, and reason for every held-back row, with replay into the same target.</p>
            </li>
            <li>
              <strong>Signed risk contracts</strong>
              <p>Intentional fidelity changes are signed, immutable, and enforced by the writer.</p>
            </li>
            <li>
              <strong>Schema drift review</strong>
              <p>A new or renamed source column waits for approval in Map; the next run picks it up.</p>
            </li>
            <li>
              <strong>Native CDC</strong>
              <p>SQL Server native CDC, PostgreSQL logical decoding, MySQL binlog, and Oracle LogMiner.</p>
            </li>
            <li>
              <strong>Schedules &amp; watermarks</strong>
              <p>Incremental cursors bound in the column&apos;s own type, zone-aware, read exactly once.</p>
            </li>
            <li>
              <strong>Unicode &amp; collation safety</strong>
              <p>Unicode text lands on NVARCHAR; keys the destination collation would merge are caught by name.</p>
            </li>
            <li>
              <strong>Timestamp precision</strong>
              <p>Every microsecond carried, DATETIME2(7)&apos;s seventh digit included, and compared by Gate-8.</p>
            </li>
            <li>
              <strong>Encoding detection</strong>
              <p>Legacy CSV encodings are detected, shown, and recorded with the job; a declared encoding is strict.</p>
            </li>
            <li>
              <strong>Streams &amp; vectors</strong>
              <p>Kafka topics as a source; typed pgvector tables plus Pinecone, Qdrant, Weaviate, and Milvus.</p>
            </li>
          </ul>
        </div>
      </section>

      <section className="lp-home-measured" id="measured" aria-label="Measured evidence">
        <div className="lp-home-measured-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Measured {EVIDENCE_AS_OF}</p>
            <h2>Numbers from named, re-runnable fixtures.</h2>
            <p>
              Every figure here traces to a recorded run against real engines, with the destination
              read back afterwards.
            </p>
          </Reveal>
          <dl className="lp-home-measured-grid">
            <div>
              <dt>{TRANSFER_READY_DRIVERS}</dt>
              <dd>Transfer-ready drivers across databases, warehouses, files, streams, and vectors</dd>
            </div>
            <div>
              <dt>4</dt>
              <dd>Native CDC engines: SQL Server, PostgreSQL, MySQL, Oracle</dd>
            </div>
            <div>
              <dt>48</dt>
              <dd>Live schema-drift cases on PostgreSQL, MySQL, SQL Server, Oracle</dd>
            </div>
            <div>
              <dt>9</dt>
              <dd>Core preflight gates G1–G9 before every write</dd>
            </div>
          </dl>
        </div>
      </section>

      {/* 5) Product surfaces — tabbed like Airbyte CLI/SDK/API/MCP */}
      <SurfaceTabs onNavigate={onNavigate} onGetStarted={onGetStarted} />

      {/* 6) Mapping proof — clear chapters + live stage */}
      <section className="lp-home-proof-band" aria-label="Semantic mapping">
        <div className="lp-home-proof-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Semantic mapping</p>
            <h2>Every column earns a confidence score</h2>
            <p>
              Format, role, qualifier, and type fit outrank string similarity. Ambiguous edges wait
              for review before they pin, and your decisions are remembered by the workspace.
            </p>
          </Reveal>
          <div className="lp-home-proof-grid">
            <div className="lp-home-proof-chapters">
              <article>
                <span>01</span>
                <h3>Role-aware matching</h3>
                <p>
                  <code>order_amt</code> pairs with <code>total_amount</code> because the order
                  qualifier matches. <code>payment_amount</code> keeps its own mapping, even though
                  both are NUMERIC amounts.
                </p>
              </article>
              <article>
                <span>02</span>
                <h3>Continuous confidence</h3>
                <p>
                  Each edge gets a score from format, role, and type fit. Only confident edges pin
                  automatically.
                </p>
              </article>
              <article>
                <span>03</span>
                <h3>Human review gate</h3>
                <p>
                  Ambiguous maps pause for confirmation. Operators decide; the workspace remembers.
                </p>
              </article>
            </div>
            <div className="lp-home-proof-stage">
              <MappingCinema />
            </div>
          </div>
        </div>
      </section>

      {/* 7) Runtime proof */}
      <section className="lp-home-proof-band lp-home-proof-band--alt" aria-label="Runtime proof">
        <div className="lp-home-proof-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Runtime proof</p>
            <h2>Read back, verified, then MATCH</h2>
            <p>
              The engine hashes the mapped source rows, reads the destination back, and runs a value
              and key census. MATCH appears when they agree — with quarantine counts alongside.
            </p>
          </Reveal>
          <div className="lp-home-proof-grid lp-home-proof-grid--reverse">
            <div className="lp-home-proof-chapters">
              <article>
                <span>01</span>
                <h3>Write clean rows</h3>
                <p>Only rows that clear coerce land in the destination table.</p>
              </article>
              <article>
                <span>02</span>
                <h3>Quarantine the rest</h3>
                <p>
                  Held-back values keep column, sample, and reason, ready to fix and replay.
                </p>
              </article>
              <article>
                <span>03</span>
                <h3>Verify value by value</h3>
                <p>Row counts, content hashes, and the key census agree before MATCH is shown.</p>
              </article>
            </div>
            <div className="lp-home-proof-stage">
              <ProofCinema />
            </div>
          </div>
        </div>
      </section>

      {/* 8) Use cases */}
      <section className="lp-home-usecases" id="usecases">
        <div className="lp-home-usecases-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Use cases</p>
            <h2>What operators run</h2>
            <p>Migration, recurring sync, and warehouse loading — the same gates and verification every time.</p>
          </Reveal>
          <div className="lp-home-usecase-list lp-home-usecase-bento">
            <article className="lp-home-usecase is-featured">
              <span>Migration</span>
              <h3>Cross-schema cutover with dual-run proof</h3>
              <p>Pilot a subset, verify it, then cut over with identity, keys, and constraints carried.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-migrations")}>
                Migration path →
              </button>
            </article>
            <article className="lp-home-usecase is-soft">
              <span>Sync</span>
              <h3>Recurring sync with every gate</h3>
              <p>Native CDC or typed watermarks; every tick is a verified job and drift waits for review.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-sync")}>
                Sync path →
              </button>
            </article>
            <article className="lp-home-usecase is-outline">
              <span>Warehouse</span>
              <h3>Bulk loads finance can archive</h3>
              <p>Snowflake, BigQuery, Iceberg, and cloud storage with capacity probes and verified MERGE.</p>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("solution-warehouse")}>
                Warehouse path →
              </button>
            </article>
          </div>
        </div>
      </section>

      {/* 9) Connectors honesty */}
      <section className="lp-home-connectors" id="tools">
        <div className="lp-home-connectors-inner">
          <Reveal className="lp-home-section-head">
            <p className="lp-section-kicker">Connectors</p>
            <h2>Load Snowflake, BigQuery, and your lake</h2>
            <p>
              Native drivers for warehouses, object storage, databases, streams, vector stores, and
              apps. Map once, validate before write, quarantine bad rows, and verify every load —
              one catalog, one governed path, <strong>{TRANSFER_READY_DRIVERS} transfer-ready drivers</strong>.
            </p>
          </Reveal>
        </div>
        <div className="lp-home-connectors-marquee" aria-hidden>
          <ConnectorMarquee ids={CONNECTOR_MARQUEE_IDS} duration={48} />
          <ConnectorMarquee ids={CONNECTOR_MARQUEE_ROW_B} reverse duration={56} />
        </div>
        <div className="lp-home-connectors-inner">
          <p className="lp-home-connectors-note">
            {catalogHonestyLead()} Open the catalog to pick a source and destination.
          </p>
          <div className="lp-home-arch-cta">
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("integrations")}>
              Browse connectors
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        </div>
      </section>

      <ProofEvidenceSection onNavigate={onNavigate} />
      <TrustSection />

      {/* 10) Final CTA — Airbyte-style close */}
      <section className="lp-home-final">
        <div className="lp-home-final-inner">
          <h2>Ship a governed transfer today</h2>
          <p>
            Start free on the engine enterprises run behind SSO, SCIM, BYOK, and a tamper-evident
            audit chain. Your first load ends the way every load should: verified, with a proof
            pack you can hand to finance.
          </p>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Try Datawrap free
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("pricing")}>
              See pricing
            </button>
            <button type="button" className="lp-btn lp-btn--ghost lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
              Enterprise
            </button>
          </div>
        </div>
      </section>
    </>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
