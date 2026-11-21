import { Children, isValidElement, type CSSProperties, type ReactNode } from "react";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import { ProductShot } from "../../components/marketing/ProductShot";
import type { PublicRoute } from "../../lib/publicNavigation";
import {
  AlgoBlock,
  GateTable,
  PRODUCT_FRAMES,
  ProofCallout,
  SOLUTION_FRAMES,
  WORKSPACE_SHOT,
  DEMO_ROUTE,
  DEMO_SCHEDULE_ROUTE,
} from "./productPageShared";

type Nav = (r: PublicRoute) => void;

function ShotBand({
  kicker,
  title,
  lead,
  src,
  alt,
  surface,
}: {
  kicker: string;
  title: string;
  lead: string;
  src: string;
  alt: string;
  surface: string;
  /** Kept so existing call sites still type-check. Every band is a split row. */
  beside?: boolean;
}) {
  return (
    <section className="lp-proof-beside" aria-label={title}>
      <div className="lp-proof-beside-copy">
        <p className="lp-mkt-kicker">{kicker}</p>
        <h2>{title}</h2>
        <p>{lead}</p>
      </div>
      <ProductShot src={src} alt={alt} surface={surface} />
    </section>
  );
}

function StatsStrip({ items }: { items: { value: string; label: string }[] }) {
  return (
    <div className="lp-mkt-stats-strip" role="list">
      {items.map((item) => (
        <div key={item.label} className="lp-mkt-stats-item" role="listitem">
          <strong>{item.value}</strong>
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
}

function PacketFlow({
  nodes,
}: {
  nodes: { label: string; sub: string; accent?: boolean }[];
}) {
  return (
    <div className="lp-mkt-packet-flow" aria-label="Data movement flow">
      {nodes.map((n, i) => (
        <div key={n.label} className="lp-mkt-packet-step" style={{ "--i": i } as CSSProperties}>
          <div className={`lp-mkt-packet-node${n.accent ? " is-accent" : ""}`}>
            <strong>{n.label}</strong>
            <span>{n.sub}</span>
          </div>
          {i < nodes.length - 1 ? (
            <span className="lp-mkt-packet-wire" aria-hidden>
              <span className="lp-mkt-packet-dot" />
            </span>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function SurfaceShell({
  kicker,
  title,
  lead,
  ctaPrimary,
  ctaSecondary,
  onPrimary,
  onSecondary,
  heroVisual,
  stats,
  liveFrames: _liveFrames,
  liveTitle: _liveTitle,
  liveSurface: _liveSurface,
  children,
  next,
  nextLabel,
  onNavigate,
}: {
  kicker: string;
  title: string;
  lead: string;
  ctaPrimary: string;
  ctaSecondary: string;
  onPrimary: () => void;
  onSecondary: () => void;
  heroVisual: ReactNode;
  stats: { value: string; label: string }[];
  liveFrames?: readonly { src: string; alt: string; caption?: string }[];
  liveTitle?: string;
  liveSurface?: string;
  children: ReactNode;
  next: PublicRoute;
  nextLabel: string;
  onNavigate: Nav;
}) {
  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-mkt-surface">
      <MarketingHeroBand
        tone="ink"
        kicker={kicker}
        title={title}
        lead={lead}
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onPrimary}>
              {ctaPrimary}
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={onSecondary}>
              {ctaSecondary}
            </button>
          </div>
        }
        visual={heroVisual}
      />
      <MarketingReveal>
        <StatsStrip items={stats} />
      </MarketingReveal>
      {children}
      <MarketingReveal>
        <section className="lp-mkt-next-band">
          <MarketingSectionFooter align="center">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate(next)}>
              Next: {nextLabel} →
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("help")}>
              Read the docs
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function Chapter({
  id,
  kicker,
  title,
  children,
}: {
  id: string;
  kicker: string;
  title: string;
  children: ReactNode;
}) {
  const nodes = Children.toArray(children);
  const shots = nodes.filter((node) => isValidElement(node) && node.type === ProductShot);
  const copy = nodes.filter((node) => !(isValidElement(node) && node.type === ProductShot));
  const band = shots.length === 1;
  return (
    <MarketingReveal>
      <section id={id} className={`lp-mkt-body lp-mkt-chapter${band ? " lp-mkt-chapter--band" : ""}`}>
        <div className="lp-mkt-chapter-copy">
          <p className="lp-mkt-kicker">{kicker}</p>
          <h2>{title}</h2>
          {copy}
        </div>
        {band ? shots : null}
      </section>
    </MarketingReveal>
  );
}

/* ─── Product pages ─────────────────────────────────────────────── */

export function TransferStudioPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · Transfer Studio"
      title="Plan the load. Check it. Then write it."
      lead="Pick a source and a destination, review how every column will land, and clear preflight before a single row is written. After the load, Datawrap reads the destination back and compares it with the source. Every screenshot on this page is one real run: 5,100 orders from Postgres into a new Snowflake table."
      ctaPrimary="Open Transfer Studio"
      ctaSecondary="See Job space"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-jobs")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.transferMap}
          alt="Transfer Studio Map step: 11 of 11 sales_orders columns mapped to Snowflake types, all safe"
          surface="Transfer Studio · Map"
          route={DEMO_ROUTE}
        />
      }
      liveFrames={PRODUCT_FRAMES.transfer}
      liveTitle="One load, step by step"
      liveSurface="Transfer Studio"
      stats={[
        { value: "11 / 11", label: "Columns mapped, none flagged" },
        { value: "23", label: "Gates checked before the write" },
        { value: "5,100", label: "Rows written on the Snowflake run" },
        { value: "5,100 = 5,100", label: "Source and Snowflake, read back" },
      ]}
      next="product-jobs"
      nextLabel="Job space"
      onNavigate={onNavigate}
    >
      <Chapter id="ts-what" kicker="What it is" title="Transfer Studio is the control surface for every load">
        <div className="lp-mkt-prose">
          <p>
            Transfer Studio is where humans plan transfers. You pick a source and destination (or upload CSV, JSONL, Parquet),
            inspect the proposed semantic map, run preflight, and only then authorize write. The same path is shared with
            Datawrap Pilot and MCP — so UI, chat, and agents always run the same governed path.
          </p>
          <p>
            Studio keeps evidence with the job: mapping decisions, gate results, quarantined rows,
            and reconciliation hashes. That evidence is what Job space displays after you click run.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Connect", sub: "Drivers · files" },
            { label: "Profile", sub: "Types · keys" },
            { label: "Map", sub: "Semantics · confidence", accent: true },
            { label: "Preflight", sub: "G1–G9", accent: true },
            { label: "Write", sub: "Quarantine" },
            { label: "Prove", sub: "Checksums" },
          ]}
        />
      </Chapter>

      <Chapter id="ts-algo-map" kicker="Algorithm" title="How semantic mapping actually scores columns">
        <AlgoBlock
          title="Mapping pipeline"
          lead="The mapper classifies format and domain, enriches each column with a semantic role, then scores every source→target edge. Ambiguous edges stay pinned for human review."
          steps={[
            {
              name: "Format + domain classify",
              detail:
                "Column names and a sample of the values are classified — amount, email, timestamp, identifier, currency. That classification, not the spelling of the name, is what the pairing is scored on.",
            },
            {
              name: "Column enrichment",
              detail:
                "Each column is analyzed for semantic role — amount, email, identifier, timestamp, currency — using name + inferred type + sample values.",
            },
            {
              name: "Edge scoring",
              detail:
                "Candidates ranked by exact name → synonym dictionary → role match → type compatibility. Confidence is continuous (0–1).",
            },
            {
              name: "Threshold gate",
              detail:
                "Workspace validation mode sets the floor (strict ≈ 0.85; engine floor 0.72). Pairs at or above the floor are ready to approve.",
            },
            {
              name: "Transform attach",
              detail:
                "Date and decimal transforms attach when the types line up, and the writer records each one on the plan.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Why meaning beats names.</strong>
          <p>
            Source <code>order_amt</code> (NUMERIC) → destination <code>total_amount</code> scores on
            the order qualifier + type. <code>payment_amount</code>, a different money column,
            keeps its own mapping even though both are amounts. A currency string stays on its own
            mapping until the types agree.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="ts-gates" kicker="Preflight" title="Nine core gates, plus the ones your route needs">
        <div className="lp-mkt-prose">
          <p>
            G1 to G9 run on every route. Validate adds policy gates for the route in front of it: a
            new table adds <em>Create destination table</em>, a sync mode with keys adds{" "}
            <em>Sync contract</em>, a dropped column adds <em>Field reduction governance</em>. The
            Snowflake load on this page checked 23 gates in 107.7 seconds.
          </p>
          <p>
            On this route Validate approved the plan, including the <code>CREATE TABLE</code> for{" "}
            <code>SALES_ORDERS_REPORTING</code>, and Execute opened.
          </p>
        </div>
        <GateTable />
      </Chapter>

      <Chapter id="ts-write" kicker="Execute" title="Write path: land the rows, then prove them">
        <AlgoBlock
          title="Load + reconcile"
          lead="After the checks pass, the engine chunks rows, applies mapped transforms, and writes with the chosen mode. On this run the read-back is 5,100 and 5,100."
          steps={[
            {
              name: "Chunk + transform",
              detail: "Rows are batched. Each field follows its mapped coerce path.",
            },
            {
              name: "Destination write",
              detail: "Table upsert or append, a destination query with bound parameters, or a destination stored procedure, one call per row.",
            },
            {
              name: "Every row named",
              detail: "The job records rows read, rows written, and the destination count. A held row stays on the job and replays into the same table.",
            },
            {
              name: "Read the destination back",
              detail:
                "Post-load, the engine reads the destination back and compares row counts, checksums, and a value and key census with the mapped source. The verdict is recorded on the job.",
            },
          ]}
        />
      </Chapter>

      <MarketingReveal>
        <ShotBand
          kicker="Mapping"
          title="Every pairing shows why it was made"
          lead="Open a row in Map. It shows the source type, the destination type, the confidence, and the reason the two columns were paired."
          src={WORKSPACE_SHOT.transferMapWhyStage}
          alt="Map row for the attributes column: JSONB to VARIANT through Parse JSON, with confidence and a sample"
          surface="Transfer Studio · Map"
        />
      </MarketingReveal>

      <Chapter id="ts-scenario" kicker="Worked example" title="sales_orders, Postgres to Snowflake">
        <div className="lp-mkt-scenario">
          <ol>
            <li>
              Source: <code>public.sales_orders</code> on Orders Postgres, 5,100 rows and 11 columns, including{" "}
              <code>DECIMAL(10,2)</code> prices, a <code>TIMESTAMPTZ</code>, and a <code>JSONB</code> attributes column.
            </li>
            <li>
              Destination: a new <code>DW_DEMO.SALES_ORDERS_REPORTING</code> table in Snowflake, full append.
            </li>
            <li>
              Map: all 11 columns pair by name and type. <code>TIMESTAMPTZ</code> becomes <code>TIMESTAMP_LTZ</code>,{" "}
              <code>JSONB</code> becomes <code>VARIANT</code> through Parse JSON (93% confidence, sample-validated).
            </li>
            <li>
              Validate: approved. 17 gates passed, and 6 were not part of this route.
            </li>
            <li>
              Run: 5,100 read, 5,100 written, destination count 0 before and 5,100 after. Proof scope is a full
              checksum with an independent re-read of the source.
            </li>
          </ol>
        </div>
      </Chapter>

      <Chapter id="ts-contract" kicker="Contracts" title="Keep the plan you approved">
        <div className="lp-mkt-prose">
          <p>
            <em>Save contract</em> on Validate stores the approved shape as a versioned contract: 11 columns, 11
            mappings, and the 23 gate results behind them. Once signed, a schedule can refuse to run when the
            source or destination no longer matches it.
          </p>
        </div>
        <ProductShot
          src={WORKSPACE_SHOT.contractsColumns}
          alt="Contract v1 for Orders Postgres to DW_DEMO.SALES_ORDERS_REPORTING, draft, 11 columns with Snowflake types"
          surface="Contracts"
          route={DEMO_ROUTE}
        />
      </Chapter>
    </SurfaceShell>
  );
}

export function JobTheaterPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · Job space"
      title="Every row landed, and you can get it back"
      lead="Open a finished run in Job space. The Trust Report reads both sides: 5,100 rows in the source, 5,100 in Snowflake, full checksum match. Reversibility shows 11 of 11 columns round-trip. Both are ready for sign-off."
      ctaPrimary="Open the app"
      ctaSecondary="Transfer Studio"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-transfer")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.jobTrust}
          alt="Trust Report for the Snowflake run: 5,100 rows in source and destination, full checksum match"
          surface="Job space · Trust Report"
          route={DEMO_ROUTE}
        />
      }
      liveFrames={PRODUCT_FRAMES.jobs}
      liveTitle="The Snowflake run, read back"
      liveSurface="Job space"
      stats={[
        { value: "5,100", label: "Rows in source and in Snowflake" },
        { value: "Match", label: "Full checksum on every table" },
        { value: "11 / 11", label: "Columns that round-trip" },
        { value: "3", label: "Roles that sign the report" },
      ]}
      next="product-pipelines"
      nextLabel="Schedules"
      onNavigate={onNavigate}
    >
      <Chapter id="jt-what" kicker="What it is" title="The record of what actually happened">
        <div className="lp-mkt-prose">
          <p>
            Transfer Studio plans a load; Job space is where you check it. The Trust Report reads both systems:
            rows in the source, rows in the destination, and a full checksum per table. On the Snowflake run both
            sides are 5,100, and every table matches.
          </p>
          <p>
            Reversibility answers the cutover question: the destination can give the source data back. On this
            run all 11 columns round-trip, so a restore reads them from the destination.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Queue", sub: "Accepted plan" },
            { label: "Preflight", sub: "G1–G9", accent: true },
            { label: "Write", sub: "Batches", accent: true },
            { label: "Account", sub: "Every row" },
            { label: "Reconcile", sub: "Checksums" },
            { label: "Complete", sub: "Proof report" },
          ]}
        />
      </Chapter>

      <Chapter id="jt-algo" kicker="Algorithm" title="How a load is proven">
        <AlgoBlock
          title="Post-load proof"
          lead="After the write, Datawrap reads the destination back and compares it with the mapped source: row counts, content checksums, and a value and key census. The Snowflake run's proof scope is a full checksum with an independent re-read of the source."
          steps={[
            {
              name: "Map source sample",
              detail: "Apply the same mappings/transforms used at write time to produce the expected target matrix.",
            },
            {
              name: "Checksum source view",
              detail: "Hash ordered mapped rows (writer checksum when available, else recompute from mapped records).",
            },
            {
              name: "Read the destination",
              detail: "Read back what landed — engine COUNT plus the values and keys of the written set, aligned on a stable key.",
            },
            {
              name: "Compare",
              detail: "Row counts, checksums, and the census agree. On this run, 5,100 and 5,100.",
            },
            {
              name: "Surface in Job space",
              detail: "Timeline, counters, quarantine samples, and proof report become the operator’s single source of truth.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Every row accounted for.</strong>
          <p>
            The Snowflake run read 5,100 and wrote 5,100. The destination count and the checksum are on the job.
          </p>
        </ProofCallout>
      </Chapter>

      <MarketingReveal>
        <ShotBand
          kicker="Proof"
          title="The read-back sits on the job"
          lead="Job space reads the destination after the write and puts the counts next to the source. On this run, 5,100 and 5,100, full checksum match."
          src={WORKSPACE_SHOT.jobTrust}
          alt="Trust Report for the Snowflake run: 5,100 rows in source and destination, full checksum match"
          surface="Job space · Trust Report"
        />
      </MarketingReveal>

      <Chapter id="jt-signoff" kicker="Sign-off" title="Hand the report to the people who answer for it">
        <div className="lp-mkt-prose">
          <p>
            The report lists what it is based on: reconciliation passed, assurance level full checksum, trust
            score 100, and whether a protection or transform proof applied. Engineering, the data owner, and
            compliance each sign the same version. Every approval goes to the audit log with the signer&apos;s role,
            and once all required roles have signed, the evidence pack is signed with the workspace&apos;s own
            evidence signing key (separate from login). Anyone with access to the workspace can verify it.
          </p>
        </div>
        <ProductShot
          src={WORKSPACE_SHOT.jobTrustEvidence}
          alt="Evidence behind the Trust Report: reconciliation passed, assurance full checksum, trust score 100"
          surface="Job space · Evidence"
          route={DEMO_ROUTE}
        />
      </Chapter>
    </SurfaceShell>
  );
}

export function PipelinesPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · Schedules"
      title="Recurring sync, with the same proof"
      lead="Run a route you have already approved every few minutes, hourly, daily, or on a cron. Each tick is a full job with the same gates and the same read-back as a load you start by hand."
      ctaPrimary="Create a schedule"
      ctaSecondary="Recurring sync guide"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("solution-sync")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.schedulesDrawer}
          alt="Orders feed schedule: every 2 minutes, incremental deduped on order_id"
          surface="Schedules"
          route={DEMO_SCHEDULE_ROUTE}
        />
      }
      liveFrames={PRODUCT_FRAMES.pipelines}
      liveTitle="Schedules, including the one that refused to run"
      liveSurface="Schedules"
      stats={[
        { value: "7", label: "Sync modes, CDC included" },
        { value: "4", label: "Native CDC engines" },
        { value: "2 min", label: "Orders feed cadence" },
        { value: "Same", label: "Gates as Transfer Studio" },
      ]}
      next="product-query"
      nextLabel="Query Playground"
      onNavigate={onNavigate}
    >
      <Chapter id="pl-what" kicker="What it is" title="A clock on top of an approved plan">
        <div className="lp-mkt-prose">
          <p>
            A schedule takes a route you have already mapped and validated in Transfer Studio and runs it on a
            cadence. Every run is a real job in Job space, with the same gates and the same read-back as a run
            you start by hand.
          </p>
          <p>
            A signed contract and reversibility can be required on every tick, so each run uses the map you
            approved and a destination that can give the data back.
          </p>
          <p>
            Choose watermark incremental for change data, upsert for slowly changing dimensions, append for events,
            overwrite for full refresh, or mirror to keep the destination in step — rows gone from the source are
            soft-deleted with <code>_deleted</code> and <code>_deleted_at</code>, and the physical row is kept. Native
            CDC streams changes from SQL Server, PostgreSQL, MySQL, and Oracle. A new or renamed column waits
            in Map until you approve it, and the next tick uses that map.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Plan", sub: "Studio map" },
            { label: "Schedule", sub: "Cron · cadence", accent: true },
            { label: "Tick", sub: "Enqueue job" },
            { label: "Preflight", sub: "G1–G9", accent: true },
            { label: "Sync", sub: "Mode · watermark" },
            { label: "Proof", sub: "Job space" },
          ]}
        />
      </Chapter>

      <Chapter id="pl-algo" kicker="Algorithm" title="How a pipeline tick executes">
        <AlgoBlock
          title="Tick lifecycle"
          lead="A schedule is just a clock. The work is still a governed transfer — identical to clicking Run in Studio."
          steps={[
            {
              name: "Wake on cadence",
              detail: "Cron / interval fires; runner loads the saved plan (map, mode, connectors, quarantine policy).",
            },
            {
              name: "Resolve watermark",
              detail: "For incremental mode, read the last high-water mark and bind it in the cursor column’s own type; zone-bearing cursors compare by instant, so every new row is read once.",
            },
            {
              name: "Run G1–G9",
              detail: "Full preflight every tick. Schema drift and confidence changes wait for review before anything is written.",
            },
            {
              name: "Write + quarantine",
              detail: "Same chunked write path as Studio. Every row is in the written set or named on the job.",
            },
            {
              name: "Prove in Job space",
              detail: "The job record gets its read-back and its counts. Operators and agents see the same artifact.",
            },
          ]}
        />
      </Chapter>

      <MarketingReveal>
        <ShotBand
          kicker="Sync modes"
          title="CDC is a mode on the same schedule"
          lead="Full refresh, incremental, CDC, slowly changing dimensions, or mirror. CDC is log-based change with a cursor and a key, on the same checks as every other mode."
          src={WORKSPACE_SHOT.transferSyncModesCard}
          alt="Advanced settings listing sync modes, including CDC: log-based changes with a cursor and a key"
          surface="Transfer Studio · Sync mode"
          beside
        />
      </MarketingReveal>

      <Chapter id="pl-scenario" kicker="Worked example" title="Orders feed, every two minutes">
        <div className="lp-mkt-scenario">
          <ol>
            <li>
              Route: <code>orders_feed</code> on Orders Postgres into Analytics Postgres, incremental deduped on{" "}
              <code>order_id</code>, cron <code>*/2 * * * *</code> in America/New_York.
            </li>
            <li>
              Schema policy is manual approval. A new or renamed source column waits in Map until you approve
              it, and the next tick uses that map.
            </li>
            <li>
              Each run lands in History with rows written and a link to its job.
            </li>
          </ol>
        </div>
      </Chapter>
    </SurfaceShell>
  );
}

export function QueryPlaygroundPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · Query Playground"
      title="Look at the data on both sides"
      lead="Run read-only SQL against any saved connection. Check a source before you map it, then run the same query on the destination after the load and compare. Open a tab per system, see column types from the driver, and copy results as CSV, JSON, or Markdown."
      ctaPrimary="Open Query Playground"
      ctaSecondary="Connectors"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("integrations")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.query}
          alt="Query on Snowflake: orders, units, revenue, and average discount by region from SALES_ORDERS_REPORTING"
          surface="Query · Snowflake"
          route={DEMO_ROUTE}
        />
      }
      liveFrames={PRODUCT_FRAMES.query}
      liveTitle="The same question, asked of both systems"
      liveSurface="Query"
      stats={[
        { value: "5 / 5", label: "Regions identical on both sides" },
        { value: "3.1 s", label: "Snowflake aggregate, 5,100 rows" },
        { value: "1,000", label: "Default row limit" },
        { value: "Read-only", label: "DML refused before it runs" },
      ]}
      next="product-pilot"
      nextLabel="Datawrap Pilot"
      onNavigate={onNavigate}
    >
      <Chapter id="qy-what" kicker="What it is" title="Exploration on the connectors you already trust">
        <div className="lp-mkt-prose">
          <p>
            Query runs on the connections you already saved, with the same encrypted credentials Transfer Studio
            uses. The screenshots show one aggregate run twice: orders, units, revenue, and average discount by
            region for paid, shipped, and delivered orders. On Snowflake, LATAM comes back with 767 orders, 3,845
            units, and 182,648.65 in revenue. The Postgres source returns the same numbers for every region.
          </p>
          <p>
            Only read and metadata statements run: SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, and
            VALUES. A <code>DELETE</code> is refused in the editor before it reaches the database. On PostgreSQL,
            MySQL, SQLite, and Oracle the statement also runs in a transaction the server itself refuses writes
            in. Writes belong in Transfer Studio, where they get gates and quarantine.
          </p>
          <p>
            When a query describes the slice you want to move, paste the same SELECT into Transfer Studio&apos;s{" "}
            <em>SQL query</em> source. From there it is mapped, validated, and verified like any other load.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Connect", sub: "Saved connection" },
            { label: "Write", sub: "SQL", accent: true },
            { label: "Check", sub: "Read-only filter", accent: true },
            { label: "Run", sub: "Row limit" },
            { label: "Compare", sub: "Source vs destination" },
            { label: "Move", sub: "Studio SQL source" },
          ]}
        />
      </Chapter>

      <Chapter id="qy-algo" kicker="How it runs" title="What happens when you press Run">
        <AlgoBlock
          title="Query path"
          lead="Every statement is checked before it reaches the database, runs under a row limit, and comes back with the column types the driver reported."
          steps={[
            {
              name: "Use the saved connection",
              detail: "The query runs with the connection's own driver and encrypted credentials, the same ones Transfer Studio uses.",
            },
            {
              name: "Check the statement",
              detail: "One statement, a read-only head, no write keyword, and no function with side effects outside the transaction. Anything else is refused in the editor.",
            },
            {
              name: "Open a read-only session",
              detail: "On PostgreSQL, MySQL, SQLite, and Oracle the server itself refuses writes for the duration of the query.",
            },
            {
              name: "Return typed rows",
              detail: "Results come back under the row limit (1,000 by default) with the types the driver reported, such as DECIMAL(24,2).",
            },
            {
              name: "Move it properly",
              detail: "To load the slice, use the same SELECT as a Transfer Studio SQL query source. Map and Validate still apply.",
            },
          ]}
        />
      </Chapter>

      <Chapter id="qy-caps" kicker="Capabilities" title="What you can do in the playground">
        <div className="lp-mkt-prose">
          <p>
            Write SQL against PostgreSQL, MySQL, Snowflake, BigQuery, and other SQL engines you have saved. Keep a
            tab per system, run one statement or all of them, filter the result, and copy it as CSV, JSON, or
            Markdown. History keeps what you ran. Kafka topics and vector stores are not queried here.
          </p>
        </div>
      </Chapter>
    </SurfaceShell>
  );
}

export function DataPilotPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · Datawrap Pilot"
      title="Ask the workspace, see the SQL"
      lead="Ask how many rows a table holds, what a column adds up to by region, which tables a connection has, or what ran recently. Pilot answers from the live systems and your job records on Datawrap’s own local engine, with no cloud AI key, and shows the SQL behind every number. Anything that would change data waits for your Confirm."
      ctaPrimary="Try Datawrap Pilot"
      ctaSecondary="MCP Server"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-mcp")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.pilot}
          alt="Pilot answering that sales_orders has 5,100 rows and listing order_total by region, with the SQL it ran"
          surface="Datawrap Pilot"
        />
      }
      liveFrames={PRODUCT_FRAMES.pilot}
      liveTitle="Exact answers from the live tables"
      liveSurface="Datawrap Pilot"
      stats={[
        { value: "5,100", label: "Rows counted server-side, not sampled" },
        { value: "SQL", label: "Shown under every number" },
        { value: "Local", label: "Engine, no cloud AI key" },
        { value: "Confirm", label: "Before anything changes" },
      ]}
      next="product-mcp"
      nextLabel="MCP Server"
      onNavigate={onNavigate}
    >
      <Chapter id="dp-what" kicker="What it is" title="Chat on the governed engine">
        <div className="lp-mkt-prose">
          <p>
            Pilot turns a question into a query against the right connection and says what it ran. Asked how many
            rows <code>sales_orders</code> holds, it answered 5,100, marked the result as an exact server-side
            aggregate rather than a sample, and printed the <code>COUNT(*)</code>. Asked for order totals by region,
            it returned all five regions with the <code>GROUP BY</code> underneath.
          </p>
          <p>
            Its own local engine runs in the workspace. Connecting a third-party model in Settings is optional and
            only changes the wording; the numbers and every Confirm still come from the engine. Every figure is
            an exact server-side result, with the SQL under it.
          </p>
        </div>
        <AlgoBlock
          title="How Pilot answers"
          lead="Pilot answers from the systems themselves, not from a model's memory of them, and it shows its work."
          steps={[
            {
              name: "Find the connection",
              detail: "The question names a table and a saved connection, such as sales_orders on Orders Postgres (demo).",
            },
            {
              name: "Build the query",
              detail: "Counts, sums, and group-bys become one SQL statement for that engine.",
            },
            {
              name: "Run it on the server",
              detail: "The aggregate runs over the whole table and is labelled exact, never a sample passed off as a total.",
            },
            {
              name: "Show the work",
              detail: "The SQL and a result reference sit under the answer so you can filter or profile it next.",
            },
            {
              name: "Confirm before change",
              detail: "Anything that would create, run, or change something becomes a request you confirm, and then goes through the same gates as Studio.",
            },
          ]}
        />
      </Chapter>
    </SurfaceShell>
  );
}

export function McpServerPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SurfaceShell
      kicker="Product · MCP Server"
      title="Agents get governed tools and secret references"
      lead="Cursor, Claude, and VS Code call Datawrap MCP tools under workspace RBAC, with API keys capped at their creator’s role. Transfers map, preflight, quarantine, and verify — with audit entries naming the key behind every agent-initiated run."
      ctaPrimary="Connect an agent"
      ctaSecondary="Security overview"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("security")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.mcp}
          alt="MCP Server page: server URL online, client setup for Cursor, Claude Desktop, VS Code, custom GPT, and other remote clients"
          surface="MCP Server"
        />
      }
      liveFrames={PRODUCT_FRAMES.mcp}
      liveTitle="Agent runs land in the same workspace"
      liveSurface="MCP Server"
      stats={[
        { value: "1 URL", label: "Streamable HTTP, any MCP client" },
        { value: "4", label: "Key roles: Viewer to Admin" },
        { value: "ack_id", label: "Then confirm_action to change" },
        { value: "Same", label: "Gates as the UI" },
      ]}
      next="integrations"
      nextLabel="Connectors"
      onNavigate={onNavigate}
    >
      <Chapter id="mcp-what" kicker="What it is" title="One governed engine for human and agent operators">
        <div className="lp-mkt-prose">
          <p>
            The MCP server exposes connectors, transfer plans, schedules, job status, and controlled run actions behind
            a deny-by-default allow-list. Tool responses carry secret references in place of credentials. If preflight
            holds a run, the agent sees the same gate evidence a human would in Job space.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Agent", sub: "Cursor · Claude" },
            { label: "MCP", sub: "Tools · auth", accent: true },
            { label: "RBAC", sub: "Workspace roles" },
            { label: "Engine", sub: "Map · G1–G9", accent: true },
            { label: "Job", sub: "Job space" },
            { label: "Audit", sub: "Immutable log" },
          ]}
        />
      </Chapter>

      <Chapter id="mcp-algo" kicker="Algorithm" title="How an agent-initiated transfer stays safe">
        <AlgoBlock
          title="MCP run path"
          lead="Tools are thin wrappers over the same engine: Auth → RBAC → plan → preflight → enqueue, exactly like a run started in the UI."
          steps={[
            {
              name: "Authenticate",
              detail: "A workspace API key with its own role and scopes, capped at its creator’s grants and named in the audit log.",
            },
            {
              name: "Authorize",
              detail: "RBAC checks the run permission, connector scope, and the tool allow-list on every call.",
            },
            {
              name: "Resolve plan",
              detail: "Agent supplies connector IDs + mode; ambiguous maps still require review flags.",
            },
            {
              name: "Preflight G1–G9",
              detail: "Identical gates as Studio. Blockers return structured evidence to the agent.",
            },
            {
              name: "Enqueue + audit",
              detail: "The job appears in Job space tagged as agent-initiated; tool payloads carry secret references only.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Secret references only.</strong>
          <p>
            MCP responses include job IDs, gate results, and quarantine summaries. Sample inspection follows an
            explicit, audited policy.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="mcp-tools" kicker="Tooling" title="Representative tool groups">
        <div className="lp-mkt-prose">
          <p>
            The tools follow the product. <strong>Connections</strong>: <code>list_connectors</code>,{" "}
            <code>test_connector</code>, <code>introspect_connector_schema</code>. <strong>Planning</strong>:{" "}
            <code>plan_transfer</code>, <code>map_connector_schemas</code>, <code>get_preflight_run</code>,{" "}
            <code>explain_mapping_assurance</code>. <strong>Running</strong>: <code>start_transfer</code>,{" "}
            <code>retry_job</code>, <code>resume_job</code>, <code>replay_quarantine</code>.{" "}
            <strong>Proof</strong>: <code>explain_parity</code>, <code>check_reversibility</code>,{" "}
            <code>compare_datasets</code>. <strong>Schedules</strong>: <code>create_schedule</code>,{" "}
            <code>run_schedule_now</code>, <code>set_schedule_enabled</code>.
          </p>
          <p>
            Anything that creates or moves something returns an <code>ack_id</code> and does nothing until{" "}
            <code>confirm_action</code>. Access follows the key: a Viewer key gets read-only tools, an Operator key
            can run and cancel jobs, an Editor key gets everything except <code>delete_connector</code>, and an
            Admin key gets every tool.
          </p>
        </div>
      </Chapter>
    </SurfaceShell>
  );
}

export function MigrationsSolutionPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SolutionShell
      kicker="Solution · Migrations"
      title="Cross-schema cutover you can prove"
      lead="Move data across schemas that were never 1:1. Semantic maps you can review, nine preflight gates (G1–G9), and a verified read-back before cutover."
      ctaPrimary="Start a migration"
      ctaSecondary="Open Transfer Studio"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-transfer")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.transferValidate}
          alt="Validate approved: 17 gates passed, Execute unlocked for the Snowflake load"
          surface="Migrations · Validate"
          route={DEMO_ROUTE}
        />
      }
      liveFrames={SOLUTION_FRAMES.migrations}
      liveTitle="The same Transfer Studio path that proves a cutover"
      liveSurface="Transfer Studio"
      outcomes={[
        {
          title: "Semantic column matching",
          body: "Roles and qualifiers outrank string names — order_amt lines up with total_amount, and payment_amount keeps its own mapping, when both money columns exist.",
        },
        {
          title: "Human review on ambiguous edges",
          body: "Low-confidence maps pause for confirmation. Nothing pins into workspace synonyms until someone accepts it.",
        },
        {
          title: "Exact on every cast",
          body: "Dry-run checks every value against the destination type before the write. The approved plan is what Execute runs.",
        },
        {
          title: "Signed proof at cutover",
          body: "Pilot a subset first, then the full write. Finance archives a signed proof pack: counts, checksums, and a census.",
        },
      ]}
      steps={[
        {
          n: "01",
          title: "Discover both sides",
          body: "Profile source and destination. Datawrap proposes role-aware maps instead of assuming name equality.",
        },
        {
          n: "02",
          title: "Review the map",
          body: "Operators confirm ambiguous edges. High-confidence matches pin; the rest wait for a human decision.",
        },
        {
          n: "03",
          title: "Clear nine core gates",
          body: "Contracts, types, capacity, and dry-run must pass before write. One block stops the load.",
        },
        {
          n: "04",
          title: "Cutover with proof",
          body: "The pilot run verifies, then the production write ships with quarantine visible and a signed proof pack.",
        },
      ]}
      caps={[
        {
          title: "Messy real-world schemas",
          body: "Amounts, emails, and identifiers align when qualifiers match. Same-role collisions (order vs payment; CRM id vs warehouse key) wait for Map review.",
        },
        {
          title: "Unicode and collation",
          body: "Unicode text lands on NVARCHAR on SQL Server, created text keys carry the source’s equality, and keys a PAD SPACE or _ci collation would merge are caught by name.",
        },
        {
          title: "Time to the last digit",
          body: "Every microsecond carries, DATETIME2(7) keeps its seventh digit, and the read-back compares timestamps at full precision.",
        },
        {
          title: "Identity, keys, constraints",
          body: "Sequences, foreign keys, CHECK constraints, and secondary indexes carry on create-new, in dependency order, proven on PostgreSQL, MySQL, SQL Server, and Oracle.",
        },
        {
          title: "Quarantine you can act on",
          body: "Held-back rows surface with column, value, and reason at write time — and replay into the same destination once fixed.",
        },
        {
          title: "Compliance-ready artifacts",
          body: "Mapping decisions, gate results, quarantine samples, and signed proof packs export straight from the job, with legacy file encodings recorded.",
        },
      ]}
      cinema={
        <ShotBand
          kicker="Mapping"
          title="The map, with the reason on the row"
          lead="Confident pairs map themselves. A row that is ambiguous waits, and the reason stays on the column so the next person can see it."
          src={WORKSPACE_SHOT.transferMap}
          alt="Transfer Studio map: 11 of 11 columns from sales_orders mapped to SALES_ORDERS_REPORTING"
          surface="Transfer Studio · Map"
        />
      }
      next="solution-warehouse"
      nextLabel="Warehouse loading"
      onNavigate={onNavigate}
    />
  );
}

export function WarehouseSolutionPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <div className="lp-mkt-page lp-wh-v3">
      <section className="lp-sol-hero lp-sol-hero--ink" aria-label="Warehouse loading">
        <div className="lp-sol-hero-inner">
          <div className="lp-sol-hero-copy">
            <p className="lp-pricing-hero-kicker">
              <span className="lp-pricing-hero-dot" aria-hidden />
              Solution · Warehouse loading
            </p>
            <h1>Bulk loads finance can archive</h1>
            <p className="lp-sol-hero-lead">
              Load Snowflake, BigQuery, Apache Iceberg, and S3, ADLS, or GCS with create-new tables,
              MERGE upserts, and a read-back verification on every load. Every warehouse load maps,
              gates, quarantines, and verifies against the destination engine.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Load a warehouse
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("product-pipelines")}>
                Explore Schedules
              </button>
            </div>
          </div>
          <div className="lp-sol-hero-visual">
            <ProductShot
              src={WORKSPACE_SHOT.transferRunComplete}
              alt="Snowflake load finished: 5,100 appended, destination 0 before and 5,100 after, checksum recorded"
              surface="Warehouse · Run"
              route={DEMO_ROUTE}
            />
          </div>
        </div>
      </section>

      <section className="lp-wh-rail" aria-label="Destinations">
        <div className="lp-shell lp-wh-rail-inner">
          <span>Snowflake</span>
          <span>BigQuery</span>
          <span>Apache Iceberg</span>
          <span>S3 · ADLS · GCS</span>
          <span>Read back and compared</span>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-wh-split">
          <div className="lp-shell lp-wh-split-grid">
            <div>
              <p className="lp-mkt-kicker">Before the bulk window</p>
              <h2>Probe the destination first</h2>
              <p>
                Reachability, privileges, and warehouse capacity clear in preflight — so every load
                window goes to a plan that is ready to land.
              </p>
              <ul className="lp-wh-list">
                <li>Write rights and role checks before any bulk path starts</li>
                <li>Slot / capacity estimates surfaced for operators</li>
                <li>A new table is created only after its DDL is approved</li>
              </ul>
            </div>
            <aside className="lp-wh-panel" aria-label="Preflight snapshot">
              <header>
                <strong>DW_DEMO.SALES_ORDERS_REPORTING</strong>
                <em>Approved</em>
              </header>
              <div><span>Destination write access</span><em>passed</em></div>
              <div><span>Create destination table</span><em>approved DDL</em></div>
              <div><span>Staging capacity</span><em>passed</em></div>
              <div><span>Gates</span><em>17 passed · Execute unlocked</em></div>
            </aside>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-wh-split lp-wh-split--alt">
          <div className="lp-shell lp-wh-split-grid is-flip">
            <div>
              <p className="lp-mkt-kicker">Write modes</p>
              <h2>Upsert, append, or overwrite — validated</h2>
              <p>
                Each driver offers the bulk modes it supports, with MERGE upserts on Snowflake and
                BigQuery. The Snowflake run wrote 5,100 of 5,100.
              </p>
              <ul className="lp-wh-list">
                <li>Driver-aware load jobs with quarantine samples</li>
                <li>Snowflake password or key-pair authentication</li>
                <li>Promote the same plan to a schedule for a recurring cadence</li>
              </ul>
            </div>
            <aside className="lp-wh-modes" aria-label="Write modes">
              <div><strong>Upsert</strong><span>Primary-key MERGE</span></div>
              <div><strong>Append</strong><span>Insert path with coerce quarantine</span></div>
              <div><strong>Overwrite</strong><span>Replace window with reconcile after</span></div>
            </aside>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-wh-flow">
          <div className="lp-shell">
            <div className="lp-wh-section-head">
              <p className="lp-mkt-kicker">How it works</p>
              <h2>Four steps from a capacity check to proof</h2>
            </div>
            <ol className="lp-wh-flow-list">
              <li className="lp-scene-card">
                <span>01</span>
                <strong>Probe</strong>
                <p>Destination rights and capacity before the window opens.</p>
              </li>
              <li className="lp-scene-card">
                <span>02</span>
                <strong>Map &amp; gate</strong>
                <p>Semantic maps and nine preflight gates (G1–G9), the same ones Studio runs.</p>
              </li>
              <li className="lp-scene-card">
                <span>03</span>
                <strong>Bulk write</strong>
                <p>The same driver path Studio uses, with the destination read back after the write.</p>
              </li>
              <li className="lp-scene-card">
                <span>04</span>
                <strong>Prove</strong>
                <p>Row counts, checksums, and a value and key census, compared with the source.</p>
              </li>
            </ol>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <ShotBand
          kicker="Proof"
          title="Read the destination back, then compare"
          lead="The engine hashes the mapped source rows, reads the destination back, and reports a match when the counts, the content, and the keys agree."
          src={WORKSPACE_SHOT.jobTrust}
          alt="Trust Report: 5,100 rows in the source, 5,100 in Snowflake, full checksum match"
          surface="Job space · Trust Report"
        />
      </MarketingReveal>

      <section className="lp-wh-cta">
        <div className="lp-shell lp-wh-cta-inner">
          <div>
            <h2>Ready to load with proof?</h2>
            <p>Start free on the same engine — or talk to us about enterprise warehouse controls.</p>
          </div>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Load a warehouse
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("solution-sync")}>
              Next: Recurring sync →
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}

export function SyncSolutionPage({
  onGetStarted,
  onNavigate,
}: {
  onGetStarted: () => void;
  onNavigate: Nav;
}) {
  return (
    <SolutionShell
      kicker="Solution · Recurring sync"
      title="Incremental schedules you can verify"
      lead="Keep systems aligned on a cadence — native CDC or typed watermarks, upsert, mirror with soft delete, schema-drift review, and Job space verification on every tick."
      ctaPrimary="Create a sync"
      ctaSecondary="Schedules"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-pipelines")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.schedulesHistory}
          alt="Run history for the Orders feed, each run linked to its job"
          surface="Recurring sync · Schedules"
          route={DEMO_SCHEDULE_ROUTE}
        />
      }
      liveFrames={SOLUTION_FRAMES.sync}
      liveTitle="Every tick is a real job in the live workspace"
      liveSurface="Schedules"
      outcomes={[
        {
          title: "Cadence you choose",
          body: "Hourly to weekly schedules — every tick is a real, verified job.",
        },
        {
          title: "Watermark incremental",
          body: "Deltas read from the last successful watermark, bound in the cursor column’s own type, so a restart resumes from the right row.",
        },
        {
          title: "Drift that waits for review",
          body: "A new or renamed column waits for approval in Map, so every write lands in the shape you approved.",
        },
        {
          title: "Job space on every run",
          body: "Phases, quarantine samples, and the verified read-back attach to each tick for operators to open later.",
        },
      ]}
      steps={[
        {
          n: "01",
          title: "Promote a proven plan",
          body: "Start from a Studio map that already cleared gates — sync inherits the same engine.",
        },
        {
          n: "02",
          title: "Set cadence and mode",
          body: "Pick schedule, watermark or upsert mode, and destination write strategy operators trust.",
        },
        {
          n: "03",
          title: "Run ticks under gates",
          body: "Each tick maps, preflights, writes with quarantine, and verifies — identical to interactive loads.",
        },
        {
          n: "04",
          title: "Watch and correct",
          body: "Job space surfaces drift reviews and quarantine. Decide once; the next tick reuses the decision.",
        },
      ]}
      caps={[
        {
          title: "CDC snapshot, then stream",
          body: "SQL Server, PostgreSQL, MySQL, and Oracle: a consistent snapshot backfills, then streaming upserts land on the primary key so redelivery is safe.",
        },
        {
          title: "Idempotent upserts",
          body: "Primary keys stay authoritative across retries, so a partial tick converges on its next run.",
        },
        {
          title: "Same policy for agents",
          body: "MCP and Pilot can trigger syncs — they inherit workspace RBAC and the nine core gates.",
        },
        {
          title: "Mirror with soft delete",
          body: "Rows gone from the source are flagged _deleted with _deleted_at and the physical row is kept. Hard delete is an explicit setting, flagged before the run.",
        },
        {
          title: "Typed, zone-aware watermarks",
          body: "The watermark binds as a value of the cursor column’s type, and zone-bearing cursors compare by instant — on every SQL engine.",
        },
        {
          title: "Run history you can trust",
          body: "One lifecycle for every scheduled run: retries, overlapping beats, and pending approvals each show their real status.",
        },
      ]}
      cinema={
        <ShotBand
          kicker="Sync modes"
          title="One list of modes, CDC included"
          lead="The schedule runs the mode you pick. CDC sits next to incremental, mirror, and slowly changing dimensions, and it goes through the same checks."
          src={WORKSPACE_SHOT.transferSyncModesCard}
          alt="Sync mode list with CDC: log-based changes with a cursor and a key"
          surface="Transfer Studio · Sync mode"
          beside
        />
      }
      next="pricing"
      nextLabel="Pricing"
      onNavigate={onNavigate}
    />
  );
}

function SolutionShell({
  kicker,
  title,
  lead,
  ctaPrimary,
  ctaSecondary,
  onPrimary,
  onSecondary,
  heroVisual,
  liveFrames: _liveFrames,
  liveTitle: _liveTitle,
  liveSurface: _liveSurface,
  outcomes,
  steps,
  caps,
  cinema,
  next,
  nextLabel,
  onNavigate,
}: {
  kicker: string;
  title: string;
  lead: string;
  ctaPrimary: string;
  ctaSecondary: string;
  onPrimary: () => void;
  onSecondary: () => void;
  heroVisual: ReactNode;
  liveFrames?: readonly { src: string; alt: string; caption?: string }[];
  liveTitle?: string;
  liveSurface?: string;
  outcomes: { title: string; body: string }[];
  steps: { n: string; title: string; body: string }[];
  caps: { title: string; body: string }[];
  cinema: ReactNode;
  next: PublicRoute;
  nextLabel: string;
  onNavigate: Nav;
}) {
  return (
    <div className="lp-mkt-page lp-sol-v2">
      <section className="lp-sol-hero lp-sol-hero--ink" aria-label={kicker}>
        <div className="lp-sol-hero-inner">
          <div className="lp-sol-hero-copy">
            <p className="lp-pricing-hero-kicker">
              <span className="lp-pricing-hero-dot" aria-hidden />
              {kicker}
            </p>
            <h1>{title}</h1>
            <p className="lp-sol-hero-lead">{lead}</p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onPrimary}>
                {ctaPrimary}
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={onSecondary}>
                {ctaSecondary}
              </button>
            </div>
          </div>
          <div className="lp-sol-hero-visual">{heroVisual}</div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-sol-outcomes" aria-label="What you get">
          {outcomes.map((item, i) => (
            <article key={item.title} className="lp-sol-outcome lp-scene-card" style={{ "--reveal-i": i } as CSSProperties}>
              <span>{String(i + 1).padStart(2, "0")}</span>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
            </article>
          ))}
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sol-section">
          <div className="lp-sol-section-head">
            <p className="lp-mkt-kicker">How it works</p>
            <h2>A clear path from discovery to proof</h2>
            <p>The same governed engine at every step.</p>
          </div>
          <ol className="lp-sol-steps">
            {steps.map((step) => (
              <li key={step.n} className="lp-sol-step lp-scene-card">
                <span>{step.n}</span>
                <div>
                  <h3>{step.title}</h3>
                  <p>{step.body}</p>
                </div>
              </li>
            ))}
          </ol>
        </section>
      </MarketingReveal>

      <MarketingReveal>{cinema}</MarketingReveal>

      <MarketingReveal>
        <section className="lp-sol-section lp-sol-section--caps">
          <div className="lp-sol-section-head">
            <p className="lp-mkt-kicker">Built for operators</p>
            <h2>Details that survive real schemas</h2>
          </div>
          <div className="lp-sol-caps">
            {caps.map((cap) => (
              <article key={cap.title} className="lp-sol-cap lp-scene-card">
                <h3>{cap.title}</h3>
                <p>{cap.body}</p>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sol-footer">
          <div className="lp-sol-footer-inner">
            <div>
              <h2>Ready to run this path?</h2>
              <p>Start free on the same engine — or talk to us about enterprise controls.</p>
            </div>
            <div className="lp-sol-footer-actions">
              <button type="button" className="lp-btn lp-btn--brand" onClick={onPrimary}>
                {ctaPrimary}
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--on-ink" onClick={() => onNavigate(next)}>
                Next: {nextLabel} →
              </button>
              <button type="button" className="lp-btn lp-btn--ghost lp-btn--on-ink" onClick={() => onNavigate("help")}>
                Read the docs
              </button>
            </div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}
