import type { ReactNode } from "react";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import { TrustSection } from "../components/landing/TrustSection";
import { MarketingFaq } from "../components/marketing/MarketingFaq";
import { ProductShot } from "../components/marketing/ProductShot";
import { VideoPlayer, videoThumb } from "../components/marketing/VideoPlayer";
import { FEATURED_VIDEO_ID, MARKETING_VIDEOS } from "../lib/videoCatalog";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import {
  PENDING_DRIVERS,
  PROVEN_CDC_SOURCES,
  PROVEN_DRIVERS,
  catalogHonestyLead,
  provenDriversPhrase,
} from "../lib/provenEvidence";
import { connectorDisplayName } from "../lib/connectorTypes";
import type { PublicRoute } from "../lib/publicNavigation";
import { DEMO_ROUTE, WORKSPACE_SHOT } from "../lib/workspaceShots";

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

const CONNECTOR_WALL = [...new Set([...CONNECTOR_MARQUEE_IDS, ...CONNECTOR_MARQUEE_ROW_B])];

type Chapter = {
  step: string;
  name: string;
  title: string;
  body: string;
  facts: string[];
  shot: string;
  alt: string;
  surface: string;
};

const CHAPTERS: Chapter[] = [
  {
    step: "01",
    name: "Connect",
    title: "Point at a source and a destination",
    body: "Pick a saved connection, then a table, a SQL query, or a stored procedure. Datawrap profiles a sample before anything moves, so you see real types and values first.",
    facts: [
      "Databases, warehouses, object storage, files, and streams",
      "Table, SQL query, or stored procedure as the source",
      "A profiled sample before the first write",
    ],
    shot: WORKSPACE_SHOT.transferSource,
    alt: "Transfer Studio source step: Orders Postgres (demo), table sales_orders, with a profiled sample of 100 rows",
    surface: "Transfer Studio · Source",
  },
  {
    step: "02",
    name: "Map",
    title: "Columns pair by meaning, not only by name",
    body: "Every pairing is scored on the column's role, its qualifier, and type fit. Confident pairs map themselves; ambiguous ones wait for a person. Open any row to see why it was paired.",
    facts: [
      "11 of 11 columns mapped on this route",
      "JSONB attributes land in Snowflake as VARIANT",
      "A written reason on every pairing",
    ],
    shot: WORKSPACE_SHOT.transferMapWhyStage,
    alt: "Transfer Studio map: the attributes column explained, JSONB to VARIANT through Parse JSON",
    surface: "Transfer Studio · Map",
  },
  {
    step: "03",
    name: "Validate",
    title: "Checks run before a single row is written",
    body: "Nine core checks run on every route, from source access to dry-run coercion, plus the policy checks the route needs. A failed check stops the write, names the cause, and links the control that fixes it.",
    facts: [
      "23 checks on this route, none blocking",
      "New tables wait for DDL sign-off",
      "Execute stays locked until the verdict is approve",
    ],
    shot: WORKSPACE_SHOT.transferValidateGates,
    alt: "Validation rules grid for the Postgres to Snowflake route: 23 checks, 17 passed and 6 not applicable",
    surface: "Transfer Studio · Validate",
  },
  {
    step: "04",
    name: "Run",
    title: "Clean rows land. Bad rows wait in quarantine.",
    body: "Rows are written in batches with bound parameters. A value that cannot fit the destination is held back with its column, value, and reason, and replays into the same table once the source or the map is fixed.",
    facts: [
      "Insert, merge, or stored-procedure writes",
      "Quarantine with replay into the same table",
      "A live log for every batch",
    ],
    shot: WORKSPACE_SHOT.transferRunComplete,
    alt: "Transfer Studio run complete: 5,100 rows appended to Snowflake, append delta balanced",
    surface: "Transfer Studio · Run",
  },
  {
    step: "05",
    name: "Prove",
    title: "The destination is read back and compared",
    body: "After the write, Datawrap reads the destination and compares it with the source: row counts, a checksum, and a value census by key. When values differ by design, each difference is listed with its reason.",
    facts: [
      "Source and destination counts side by side",
      "Differences explained per column, not hidden",
      "A report you can export and sign off",
    ],
    shot: WORKSPACE_SHOT.jobTrustExplained,
    alt: "Trust Report for a MongoDB load: 5,100 source rows, 3,756 in the destination, 1,344 differences, every one explained",
    surface: "Job space · Trust Report",
  },
];

type Feature = {
  id: string;
  title: string;
  body: string;
  shot: string;
  alt: string;
  route: PublicRoute;
  link: string;
  size: "wide" | "narrow" | "third";
  icon: string;
};

const FEATURES: Feature[] = [
  {
    id: "schedules",
    title: "Recurring syncs that stop when they should",
    body: "Incremental, upsert, append, mirror, or CDC on your cadence. Every run is a full job with every check. When a run needs a decision, the schedule parks and tells you exactly what to change.",
    shot: WORKSPACE_SHOT.schedulesCard,
    alt: "Schedules: one schedule parked on a finding, upsert needs a business key, with the fix and an Open Transfer Studio button",
    route: "product-pipelines",
    link: "Explore Schedules",
    size: "wide",
    icon: "jobs",
  },
  {
    id: "query",
    title: "Look at the data on both sides",
    body: "Read-only SQL against any saved connection, with a schema browser and a row limit. Statements that write are refused.",
    shot: WORKSPACE_SHOT.queryCard,
    alt: "Query on Warehouse Snowflake (demo): revenue by region, 5 rows in 3.1 seconds",
    route: "product-query",
    link: "Open Query",
    size: "narrow",
    icon: "search",
  },
  {
    id: "reversibility",
    title: "Know whether you can get the source back",
    body: "Column by column: can the destination value reproduce the source value? Hashes, masks, and narrowing casts are named.",
    shot: WORKSPACE_SHOT.jobReversibilityCard,
    alt: "Reversibility tab: every mapping lossless, 11 columns invertible, rollback by restoring from the destination",
    route: "product-jobs",
    link: "See Job space",
    size: "third",
    icon: "eye",
  },
  {
    id: "contracts",
    title: "Pin the shape a route promises",
    body: "Columns, types, and nullability, versioned and signed. Drift waits for review instead of changing the destination.",
    shot: WORKSPACE_SHOT.contractsCard,
    alt: "Contract for Orders Postgres to SALES_ORDERS_REPORTING: 11 columns with types and nullability",
    route: "product-transfer",
    link: "How contracts work",
    size: "third",
    icon: "lock",
  },
  {
    id: "agents",
    title: "Ask in plain English, or bring your agent",
    body: "Pilot answers with exact server-side SQL. MCP gives Cursor, Claude, and other clients the same tools, scoped by role, with every change held for confirmation.",
    shot: WORKSPACE_SHOT.pilotCard,
    alt: "Pilot answering total order value by region on Orders Postgres, with the SQL it ran",
    route: "product-pilot",
    link: "Meet Pilot",
    size: "third",
    icon: "sparkle",
  },
];

const QUESTIONS: { q: string; a: string; where: string }[] = [
  {
    q: "Did every row arrive?",
    a: "Rows in the source, rows in the destination, and rows that differ, from a read-back of the destination itself.",
    where: "Trust Report",
  },
  {
    q: "Why did this column change type?",
    a: "The pairing, its confidence, and the type on both sides, recorded before the write.",
    where: "Map",
  },
  {
    q: "What stopped the load?",
    a: "The check that failed, the reason, and the control that fixes it. Nothing was written.",
    where: "Validate",
  },
  {
    q: "Where did the bad rows go?",
    a: "Quarantine keeps the column, the value, and the reason. Replay sends them to the same table.",
    where: "Quarantine",
  },
  {
    q: "Could we rebuild the source from the copy?",
    a: "Each column is marked invertible or not, with the transform that made it lossy.",
    where: "Reversibility",
  },
  {
    q: "Who approved this, and when?",
    a: "Sign-off on the job and a hash-chained audit log you can export.",
    where: "Sign-off",
  },
];

const USE_CASES: { id: PublicRoute; tag: string; title: string; body: string; points: string[] }[] = [
  {
    id: "solution-migrations",
    tag: "Migration",
    title: "Cut over to a new system with evidence",
    body: "Move between schemas that were never one-to-one. Run a subset, compare it, then cut over with keys and constraints carried.",
    points: ["Cross-schema mapping you can review", "Identity, keys, and constraints carried", "A signed report for the cutover meeting"],
  },
  {
    id: "solution-sync",
    tag: "Sync",
    title: "Keep two systems in step",
    body: "Native CDC or typed watermarks on a schedule. Every run is a checked job, and schema drift waits for approval.",
    points: [
      PROVEN_CDC_SOURCES.length
        ? `CDC proven on ${PROVEN_CDC_SOURCES.join(", ")}`
        : "Native CDC and typed watermarks",
      "Drift held for review, never applied silently",
      "Schedules that park instead of retrying blindly",
    ],
  },
  {
    id: "solution-warehouse",
    tag: "Warehouse",
    title: "Load the warehouse finance relies on",
    body: "Snowflake, BigQuery, Iceberg, and cloud storage, with capacity probes before the write and a read-back after it.",
    points: ["Exact decimals and timestamps", "Append, overwrite, or merge by key", "Every load verified against the source"],
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

function SectionHead({ kicker, title, children }: { kicker: string; title: string; children?: ReactNode }) {
  return (
    <Reveal className="lp-home-section-head">
      <p className="lp-section-kicker">{kicker}</p>
      <h2>{title}</h2>
      {children ? <p>{children}</p> : null}
    </Reveal>
  );
}

function StepBand({
  step,
  flip,
}: {
  step: (typeof CHAPTERS)[number];
  flip: boolean;
}) {
  const { ref, className } = useRevealOnScroll<HTMLLIElement>(0.18);
  return (
    <li
      ref={ref}
      id={`step-${step.name.toLowerCase()}`}
      className={`${className}${flip ? " is-flip" : ""}`}
    >
      <div>
        <p className="lp-home-steps-name">
          {step.step} · {step.name}
        </p>
        <h3>{step.title}</h3>
        <p>{step.body}</p>
        <ul className="lp-home-steps-facts">
          {step.facts.map((fact) => (
            <li key={fact}>{fact}</li>
          ))}
        </ul>
      </div>
      <ProductShot src={step.shot} alt={step.alt} surface={step.surface} />
    </li>
  );
}

function ProductSteps() {
  return (
    <ol className="lp-home-steps">
      {CHAPTERS.map((c, i) => (
        <StepBand key={c.step} step={c} flip={i % 2 === 1} />
      ))}
    </ol>
  );
}

export function LandingHome({ onLogin: _onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  const featured = MARKETING_VIDEOS.find((v) => v.id === FEATURED_VIDEO_ID);
  return (
    <>
      <section className="lp-home-hero" aria-labelledby="lp-home-hero-title">
        <div className="lp-home-hero-bg" aria-hidden />
        <div className="lp-home-hero-grid">
        <div className="lp-home-hero-copy">
          <p className="lp-home-hero-eyebrow">Data transfer platform</p>
          <h1 id="lp-home-hero-title">
            Move data anywhere.
            <span> Prove every row arrived.</span>
          </h1>
          <p className="lp-home-hero-sub">
            Map columns across systems that were never designed to match. Stop a bad write before
            it starts, then read the destination back and see which rows differ, and why.
          </p>
          <div className="lp-home-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Start free
              <DtIcon name="arrow-right" size={16} />
            </button>
            <button
              type="button"
              className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink"
              onClick={() => onNavigate("contact")}
            >
              Talk to an engineer
            </button>
          </div>
          <ul className="lp-home-hero-facts">
            <li>
              <strong>11 of 11</strong>
              <span>columns mapped</span>
            </li>
            <li>
              <strong>5,100</strong>
              <span>rows read back</span>
            </li>
            <li>
              <strong>0 differ</strong>
              <span>full checksum</span>
            </li>
          </ul>
          <p className="lp-home-hero-meta">{provenDriversPhrase()}</p>
        </div>
        <div className="lp-home-hero-stage">
          <ProductShot
            src={WORKSPACE_SHOT.transferMapStage}
            alt="Transfer Studio map step: 11 of 11 columns from public.sales_orders mapped to SALES_ORDERS_REPORTING"
            surface="Transfer Studio · Map"
            route={{ source: DEMO_ROUTE.source, dest: DEMO_ROUTE.dest }}
          />
        </div>
        </div>
      </section>

      <section className="lp-home-stack" aria-label="Works with your stack">
        <p className="lp-home-stack-label">Works with the systems you already run</p>
        <div className="lp-conn-marquee lp-home-stack-marquee">
          <div className="lp-conn-marquee-track lp-home-stack-track">
            {[0, 1].map((copy) => (
              <div key={copy} className="lp-home-stack-row" aria-hidden={copy === 1 || undefined}>
                {STACK_IDS.map((id) => (
                  <span key={`${copy}-${id}`} className="lp-home-stack-item" title={connectorDisplayName(id)}>
                    <ConnectorIcon id={id} size={28} />
                    <em>{connectorDisplayName(id)}</em>
                  </span>
                ))}
              </div>
            ))}
          </div>
        </div>
      </section>

      <section className="lp-home-promises" aria-label="What a transfer guarantees">
        <div className="lp-home-inner">
          <Reveal className="lp-home-promises-grid">
            {[
              {
                n: "01",
                title: "Map by meaning",
                body: "Columns pair on role and type, not on spelling. An ambiguous pair waits for a person.",
              },
              {
                n: "02",
                title: "Stop a bad write",
                body: "Checks run before the first row. One failure locks the write and names the control that fixes it.",
              },
              {
                n: "03",
                title: "Read it back",
                body: "The destination is counted and checksummed against the source. Every difference stays listed.",
              },
            ].map((item, i) => (
              <article key={item.n} className="lp-scene-card" style={{ ["--reveal-i" as string]: i }}>
                <span>{item.n}</span>
                <h2>{item.title}</h2>
                <p>{item.body}</p>
              </article>
            ))}
          </Reveal>
        </div>
      </section>

      <section className="lp-home-measured" aria-label="Measured on one run">
        <div className="lp-home-inner">
          <SectionHead kicker="One named run" title="Postgres to Snowflake, counted on both sides">
            These figures are from the load on this page: 5,100 orders, public.sales_orders into
            DW_DEMO.SALES_ORDERS_REPORTING. They are not a catalog-wide claim.
          </SectionHead>
          <Reveal className="lp-home-measured-grid">
            {[
              ["11 / 11", "Columns mapped, none flagged"],
              ["5,100", "Rows in the source and in Snowflake"],
              ["0", "Rows that differ on the checksum"],
              ["23", "Gates checked before the write"],
            ].map(([value, label], i) => (
              <div key={label} style={{ ["--reveal-i" as string]: i }}>
                <strong>{value}</strong>
                <span>{label}</span>
              </div>
            ))}
          </Reveal>
        </div>
      </section>

      <section className="lp-home-flow" id="how-it-works" aria-label="How a transfer runs">
        <div className="lp-home-inner">
          <SectionHead kicker="How it works" title="Five steps from connection to proof">
            The same path runs whether a person starts the transfer in Studio, a schedule fires,
            or an agent calls it through MCP. No surface can skip a check or the read-back.
          </SectionHead>
          <ProductSteps />
          <div className="lp-home-actions">
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Open Transfer Studio
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
              How mapping and checks work
            </button>
          </div>
        </div>
      </section>

      <section className="lp-home-features" id="product" aria-label="After the first load">
        <div className="lp-home-inner">
          <SectionHead kicker="After the first load" title="Everything you need to run it every day">
            Schedules, Query, Reversibility, Contracts, and agents sit on the same engine as Studio,
            so a recurring sync is held to the same standard as the first transfer.
          </SectionHead>
          <Reveal className="lp-home-bento">
            {FEATURES.map((f, i) => (
              <article key={f.id} className={`lp-home-bento-cell is-${f.size}`} style={{ ["--reveal-i" as string]: i }}>
                <div className="lp-home-bento-copy">
                  <span className="lp-home-bento-mark" aria-hidden>
                    <DtIcon name={f.icon} size={18} chrome="product" />
                  </span>
                  <h3>{f.title}</h3>
                  <p>{f.body}</p>
                  <button type="button" className="lp-home-link" onClick={() => onNavigate(f.route)}>
                    {f.link}
                    <DtIcon name="arrow-right" size={14} />
                  </button>
                </div>
              </article>
            ))}
          </Reveal>
        </div>
      </section>

      <section className="lp-home-questions" aria-label="Questions Datawrap answers">
        <div className="lp-home-inner">
          <SectionHead kicker="Built for the review meeting" title="Answers your team will be asked for">
            Moving the data is half the job. The other half is showing it arrived intact. Each of
            these has a screen in Datawrap, not a spreadsheet someone assembles afterwards.
          </SectionHead>
          <Reveal>
            <MarketingFaq items={QUESTIONS} />
          </Reveal>
        </div>
      </section>

      <section className="lp-home-cases" id="usecases" aria-label="Use cases">
        <div className="lp-home-inner">
          <SectionHead kicker="Use cases" title="Three jobs teams bring to Datawrap" />
          <Reveal className="lp-home-cases-grid">
            {USE_CASES.map((u, i) => (
              <article key={u.id} className="lp-home-case" style={{ ["--reveal-i" as string]: i }}>
                <span className="lp-home-case-tag">{u.tag}</span>
                <h3>{u.title}</h3>
                <p>{u.body}</p>
                <ul>
                  {u.points.map((p) => (
                    <li key={p}>
                      <DtIcon name="check" size={14} />
                      {p}
                    </li>
                  ))}
                </ul>
                <button type="button" className="lp-home-link" onClick={() => onNavigate(u.id)}>
                  {u.tag} guide
                  <DtIcon name="arrow-right" size={14} />
                </button>
              </article>
            ))}
          </Reveal>
        </div>
      </section>

      <section className="lp-home-connectors" id="tools" aria-label="Connectors">
        <div className="lp-home-inner lp-home-connectors-head">
          <SectionHead kicker="Connectors" title="Databases, warehouses, lakes, and streams">
            Native drivers, each one held to the same mapping, checks, and read-back. A driver is
            listed as proven when its CI jobs ran green with no skipped test.
          </SectionHead>
          <dl className="lp-home-connectors-stats">
            <div>
              <dt>{PROVEN_DRIVERS}</dt>
              <dd>drivers proven in CI</dd>
            </div>
            {PENDING_DRIVERS > 0 ? (
              <div>
                <dt>{PENDING_DRIVERS}</dt>
                <dd>more running their CI proof</dd>
              </div>
            ) : null}
            <div>
              <dt>{PROVEN_CDC_SOURCES.length}</dt>
              <dd>CDC sources proven</dd>
            </div>
          </dl>
        </div>
        <div className="lp-home-inner">
          <Reveal className="lp-home-connector-grid">
            {CONNECTOR_WALL.map((id, i) => (
              <span key={id} className="lp-conn-marquee-tile" style={{ ["--reveal-i" as string]: i }}>
                <ConnectorIcon id={id} size={30} />
                <em>{connectorDisplayName(id)}</em>
              </span>
            ))}
          </Reveal>
        </div>
        <div className="lp-home-inner">
          <p className="lp-home-connectors-note">{catalogHonestyLead()}</p>
          <div className="lp-home-actions">
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("integrations")}>
              Browse connectors
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help")}>
              Read the docs
            </button>
          </div>
        </div>
      </section>

      <section className="lp-home-videos" id="demos" aria-label="Video demos">
        <div className="lp-home-videos-inner">
          <SectionHead kicker="Watch it work" title="Real runs, recorded end to end">
            Quarantine and replay, schema drift, and native CDC on the production engine. Each
            video ends where every transfer should: with the read-back.
          </SectionHead>
          <div className="lp-home-videos-feature">
            <VideoPlayer videoId={FEATURED_VIDEO_ID} title={featured?.title ?? "Datawrap demo"} />
          </div>
          <Reveal className="lp-home-videos-row">
            {MARKETING_VIDEOS.filter((v) => v.id !== FEATURED_VIDEO_ID)
              .slice(0, 3)
              .map((video, i) => (
                <button
                  key={video.id}
                  type="button"
                  className="lp-home-videos-card"
                  style={{ ["--reveal-i" as string]: i }}
                  onClick={() => onNavigate("demos")}
                >
                  <img src={videoThumb(video.id, "hq")} alt="" loading="lazy" />
                  <span className="lp-home-videos-card-title">{video.title}</span>
                </button>
              ))}
          </Reveal>
          <div className="lp-home-actions">
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("demos")}>
              All demos
            </button>
          </div>
        </div>
      </section>

      <TrustSection />

      <section className="lp-home-final" aria-labelledby="lp-home-final-title">
        <div className="lp-home-final-inner">
          <div className="lp-home-final-panel">
            <div>
              <h2 id="lp-home-final-title">Run your first verified transfer today</h2>
              <p>
                Start free with the full Transfer Studio path, checks and read-back included. When
                you&apos;re ready for schedules, CDC, and SSO, we&apos;ll scope it with you.
              </p>
            </div>
            <div className="lp-home-final-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Start free
                <DtIcon name="arrow-right" size={16} />
              </button>
              <button
                type="button"
                className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink"
                onClick={() => onNavigate("pricing")}
              >
                See pricing
              </button>
            </div>
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
