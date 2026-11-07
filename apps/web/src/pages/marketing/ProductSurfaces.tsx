import { type CSSProperties, type ReactNode } from "react";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import {
  AlgorithmCinemaBand,
  CdcCinema,
  MappingCinema,
  ProofCinema,
} from "../../components/landing/AlgorithmCinema";
import { ProductShot } from "../../components/marketing/ProductShot";
import type { PublicRoute } from "../../lib/publicNavigation";
import {
  AlgoBlock,
  GateTable,
  LiveProductReel,
  PRODUCT_FRAMES,
  ProofCallout,
  SOLUTION_FRAMES,
  WORKSPACE_SHOT,
} from "./productPageShared";

type Nav = (r: PublicRoute) => void;

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
  liveFrames,
  liveTitle,
  liveSurface,
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
      {liveFrames && liveFrames.length > 0 ? (
        <MarketingReveal>
          <section className="lp-mkt-body">
            <LiveProductReel
              frames={liveFrames}
              title={liveTitle ?? "Inside the live workspace"}
              surface={liveSurface ?? "Workspace"}
            />
          </section>
        </MarketingReveal>
      ) : null}
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
  return (
    <MarketingReveal>
      <section id={id} className="lp-mkt-body lp-mkt-chapter">
        <p className="lp-mkt-kicker">{kicker}</p>
        <h2>{title}</h2>
        {children}
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
      title="The wizard that verifies every load"
      lead="Connect any source to any destination, review semantic maps with confidence scores, clear nine preflight gates (G1–G9), then write with quarantine and a value-by-value read-back from the destination — all in one governed path."
      ctaPrimary="Open Transfer Studio"
      ctaSecondary="See Job Theater"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-jobs")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.transferSource}
          alt="Transfer Studio source step in the live Datawrap workspace"
          surface="Transfer Studio"
          route={{ source: "sample-orders.csv", dest: "File export · CSV" }}
        />
      }
      liveFrames={PRODUCT_FRAMES.transfer}
      liveTitle="Transfer Studio in the live workspace"
      liveSurface="Transfer Studio"
      stats={[
        { value: "G1–G9", label: "Real preflight gates" },
        { value: "Any→any", label: "Route coverage" },
        { value: "≥0.85", label: "Default confidence floor" },
        { value: "Verified", label: "Read back after write" },
      ]}
      next="product-jobs"
      nextLabel="Job Theater"
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
            and reconciliation hashes. That evidence is what Job Theater displays after you click run.
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
                "FormatClassifierAgent inspects column names and samples (payment tokens, domain profiles) and returns a format confidence (e.g. payment_feed vs generic_tabular).",
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
                "Workspace validation mode sets the floor (strict ≈ 0.85; engine floor 0.72). Below-threshold edges block G4 until you pin, reject, or remap.",
            },
            {
              name: "Transform attach",
              detail:
                "Safe coerce / date / decimal transforms attach when they are reversible. An intentional lossy cast is a signed risk contract, enforced by the writer.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Why meaning beats names.</strong>
          <p>
            Source <code>order_amt</code> (NUMERIC) → destination <code>total_amount</code> scores on
            the order qualifier + type. <code>payment_amount</code>, a different money column,
            keeps its own mapping even though both are amounts. A STRING with currency symbols bound
            for NUMERIC is held at G4/G5 and quarantined with its reason.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="ts-gates" kicker="Preflight · G1–G9" title="Nine core gates from the real engine, before every write">
        <div className="lp-mkt-prose">
          <p>
            Each gate maps to <code>GateId</code> in Datawrap&apos;s preflight package. A single{" "}
            <em>block</em> stops the write before any row moves, and tells you exactly which gate and why.
          </p>
        </div>
        <GateTable />
      </Chapter>

      <Chapter id="ts-write" kicker="Execute" title="Write path: quarantine, then prove">
        <AlgoBlock
          title="Load + reconcile"
          lead="After G1–G9 pass, the engine chunks rows, applies mapped transforms, writes with the chosen mode, and isolates held-back rows. Success means a verified read-back: Gate-8 row counts plus a value and key census."
          steps={[
            {
              name: "Chunk + transform",
              detail: "Rows are batched; each field follows its mapped coerce path. Failures go to quarantine with column, value, and reason.",
            },
            {
              name: "Destination write",
              detail: "Table upsert/append, destination query (one INSERT/MERGE/UPDATE with binds), or destination stored procedure (one CALL per row). Failed destination SQL quarantines the row.",
            },
            {
              name: "Quarantine isolate",
              detail: "Held-back rows stay out of the clean set and wait in a durable, workspace-scoped queue. Operators inspect and replay them from Job Theater.",
            },
            {
              name: "Gate-8 verify",
              detail:
                "Post-load, the engine reads the destination back and compares row counts, checksums, and a value and key census with the mapped source. The verdict is recorded on the job.",
            },
          ]}
        />
      </Chapter>

      <MarketingReveal>
        <AlgorithmCinemaBand
          kicker="Mapping · Preflight"
          title="Watch the semantic mapper score every edge"
          lead="Format classify, role enrichment, and continuous confidence. Ambiguous edges pause for human review before they can pin into workspace synonyms."
          compact
        >
          <MappingCinema />
        </AlgorithmCinemaBand>
      </MarketingReveal>

      <Chapter id="ts-scenario" kicker="Worked example" title="Retail orders CSV → PostgreSQL">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Upload <code>sample-orders.csv</code> and select PostgreSQL <code>public.orders</code> (or Load sample in Studio).</li>
            <li>Review map: <code>order_amt → total_amount</code> (qualifier pin), <code>cust_id → customer_key</code> (review — G4 holds; id ≠ warehouse key).</li>
            <li>Preflight: G1–G3 pass; G5 flags currency-symbol rows — quarantine policy captures them.</li>
            <li>Write clean rows; Job Theater shows the verified read-back for the written set plus quarantined rows with reasons, ready to replay.</li>
          </ol>
        </div>
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
      kicker="Product · Job Theater"
      title="See every phase from queue to proof"
      lead="Job Theater is the operations console for transfers — live batch progress, phase timeline, quarantine samples, and reconciliation reports. If it ran, you can prove it here."
      ctaPrimary="Open the app"
      ctaSecondary="Transfer Studio"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-transfer")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.jobs}
          alt="Job Theater with whole-history counts and destination population"
          surface="Job Theater"
          route={{ source: "PostgreSQL · public.orders", dest: "Snowflake · ANALYTICS.ORDERS" }}
        />
      }
      liveFrames={PRODUCT_FRAMES.jobs}
      liveTitle="Job Theater with real reconcile evidence"
      liveSurface="Job Theater"
      stats={[
        { value: "Live", label: "Batch progress" },
        { value: "Phases", label: "Queue → reconcile" },
        { value: "Quarantine", label: "Bad rows + reasons" },
        { value: "Verified", label: "Value & key census" },
      ]}
      next="product-pipelines"
      nextLabel="Pipelines"
      onNavigate={onNavigate}
    >
      <Chapter id="jt-what" kicker="What it is" title="Operations visibility for governed loads">
        <div className="lp-mkt-prose">
          <p>
            Transfer Studio plans the load. Job Theater watches it. Every job transitions through explicit phases —
            Queued, Preflight, Extract/Write, Reconciling, Complete (or Failed) — with counters that reflect what the destination holds.
            Quarantined rows appear with column, value, and reason so operators can fix the map or the source.
          </p>
          <p>
            Retries preserve the audit trail: you see which attempt failed which gate, and whether checksums matched
            after a successful rewrite. Agents and humans look at the same job record.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Queue", sub: "Accepted plan" },
            { label: "Preflight", sub: "G1–G9", accent: true },
            { label: "Write", sub: "Batches", accent: true },
            { label: "Quarantine", sub: "Bad rows" },
            { label: "Reconcile", sub: "Checksums" },
            { label: "Complete", sub: "Proof report" },
          ]}
        />
      </Chapter>

      <Chapter id="jt-algo" kicker="Algorithm" title="How reconciliation proves a load">
        <AlgoBlock
          title="Post-load proof"
          lead="After the write, Gate-8 reads the destination back and compares it with the mapped source: row counts, content checksums, and a value and key census."
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
              detail: "Row counts, checksums, and the census → MATCH, or a named mismatch showing exactly which values differ.",
            },
            {
              name: "Surface in Theater",
              detail: "Timeline, counters, quarantine samples, and proof report become the operator’s single source of truth.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Every row accounted for.</strong>
          <p>
            Quarantined rows are part of the proof: the written set verifies while N rows wait in quarantine with
            reasons. Operators fix the source or the map and replay them.
          </p>
        </ProofCallout>
      </Chapter>

      <MarketingReveal>
        <AlgorithmCinemaBand
          kicker="Proof"
          title="Read-back verification lands on every job"
          lead="The engine hashes mapped source rows, reads the destination back, runs the census — then flashes MATCH when counts, content, and keys agree."
          compact
        >
          <ProofCinema />
        </AlgorithmCinemaBand>
      </MarketingReveal>

      <Chapter id="jt-scenario" kicker="Worked example" title="A warehouse load, verified end to end">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Pipeline kicks off Snowflake upsert at 02:00 UTC — job appears in Theater as Queued.</li>
            <li>G2 Destination + G7 Capacity probe warehouse slots; policy continues with a warning.</li>
            <li>During Write, 42 rows quarantine on null PK; the written set verifies with MATCH.</li>
            <li>Operator opens the quarantine sample, fixes the source nulls, and replays — Complete with 0 quarantined.</li>
          </ol>
        </div>
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
      kicker="Product · Pipelines"
      title="Recurring sync with every gate, every run"
      lead="Hourly, daily, and weekly schedules with typed watermarks, upsert, append, overwrite, mirror with soft delete, and native CDC — every tick reuses Transfer Studio’s gates, quarantine, and verification."
      ctaPrimary="Schedule a pipeline"
      ctaSecondary="Recurring sync guide"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("solution-sync")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.pipelines}
          alt="Schedules workspace — cadence, mode, and health"
          surface="Schedules"
          route={{ source: "PostgreSQL · public.orders", dest: "BigQuery · analytics.orders" }}
        />
      }
      liveFrames={PRODUCT_FRAMES.pipelines}
      liveTitle="Schedules in the live workspace"
      liveSurface="Schedules"
      stats={[
        { value: "Hourly+", label: "Cadences" },
        { value: "4", label: "Native CDC engines" },
        { value: "Drift", label: "Manual review in Map" },
        { value: "Same", label: "Engine as Studio" },
      ]}
      next="product-query"
      nextLabel="Query Playground"
      onNavigate={onNavigate}
    >
      <Chapter id="pl-what" kicker="What it is" title="Schedules on the governed engine">
        <div className="lp-mkt-prose">
          <p>
            Pipelines turn a proven Transfer Studio plan into a cadence. Each run is a real job in Job Theater —
            with the same mapping, gates, quarantine, and post-write verification as a run you start by hand.
          </p>
          <p>
            Choose watermark incremental for change data, upsert for slowly changing dimensions, append for events,
            overwrite for full refresh, or mirror to keep the destination in step — rows gone from the source are
            soft-deleted with <code>_deleted</code> and <code>_deleted_at</code>, and the physical row is kept. Native
            CDC streams changes from SQL Server, PostgreSQL, MySQL, and Oracle. Schema drift — a new or renamed
            column — waits for your approval in Map, and the next tick picks it up.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Plan", sub: "Studio map" },
            { label: "Schedule", sub: "Cron · cadence", accent: true },
            { label: "Tick", sub: "Enqueue job" },
            { label: "Preflight", sub: "G1–G9", accent: true },
            { label: "Sync", sub: "Mode · watermark" },
            { label: "Proof", sub: "Theater" },
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
              detail: "Same chunked write path as Studio; bad rows isolate with reasons.",
            },
            {
              name: "Prove in Theater",
              detail: "The job record gets its Gate-8 verification and counts. Operators and agents see the same artifact.",
            },
          ]}
        />
      </Chapter>

      <MarketingReveal>
        <AlgorithmCinemaBand
          kicker="CDC"
          title="Snapshot handoff, then idempotent streaming upserts"
          lead="SQL Server native CDC, PostgreSQL logical decoding, MySQL binlog, and Oracle LogMiner: a consistent snapshot, a handoff at the log position, then idempotent upserts on the primary key — so a retried tick converges and deletes carry through."
          compact
        >
          <CdcCinema />
        </AlgorithmCinemaBand>
      </MarketingReveal>

      <Chapter id="pl-scenario" kicker="Worked example" title="Hourly orders into BigQuery">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Promote Studio plan Orders PG → BigQuery with watermark on <code>updated_at</code>.</li>
            <li>Set cadence every hour; first tick backfills, later ticks move only deltas.</li>
            <li>Source adds a column — drift waits for review; the operator approves it in Map and the next tick resumes.</li>
            <li>Each hour’s job shows written counts and its verified read-back in Job Theater.</li>
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
      title="Inspect live data before you move it"
      lead="Run ad-hoc SQL and document queries against connected systems, preview results, export samples, and hand validated selections into Transfer Studio — without leaving the workspace."
      ctaPrimary="Open Query Playground"
      ctaSecondary="Connectors"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("integrations")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.query}
          alt="Query Playground in the live Datawrap workspace"
          surface="Query Playground"
        />
      }
      liveFrames={PRODUCT_FRAMES.query}
      liveTitle="Query Playground in the live workspace"
      liveSurface="Query Playground"
      stats={[
        { value: "SQL", label: "Relational drivers" },
        { value: "Docs", label: "Mongo-style queries" },
        { value: "Preview", label: "Row-limited safe" },
        { value: "Handoff", label: "Into Transfer Studio" },
      ]}
      next="product-pilot"
      nextLabel="Datawrap Pilot"
      onNavigate={onNavigate}
    >
      <Chapter id="qy-what" kicker="What it is" title="Exploration on the connectors you already trust">
        <div className="lp-mkt-prose">
          <p>
            Query Playground is built for discovery and validation. It runs read-only SELECT and WITH; destination
            INSERT/MERGE and stored-procedure writes live in Transfer Studio’s destination step (Query / Stored
            procedure), with quarantine on every row. You query through the same connector credentials and RBAC as
            the rest of the workspace, with preview limits that keep exploratory queries light.
          </p>
          <p>
            When a query defines the slice you want to move, hand off to Transfer Studio to attach mapping, preflight,
            and verification. Exploration and governed load each get the surface built for them.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Connect", sub: "Live driver" },
            { label: "Author", sub: "SQL · docs", accent: true },
            { label: "Preview", sub: "Limited rows", accent: true },
            { label: "Validate", sub: "Types · nulls" },
            { label: "Handoff", sub: "Transfer Studio" },
            { label: "Prove", sub: "After load" },
          ]}
        />
      </Chapter>

      <Chapter id="qy-algo" kicker="Algorithm" title="Safe preview → Studio handoff">
        <AlgoBlock
          title="Query path"
          lead="Playground reads under connector RBAC, caps result size, and promotes a validated selection into a transfer plan."
          steps={[
            {
              name: "Bind connector",
              detail: "Use the saved connector’s driver + encrypted secret — same vault as Transfer Studio.",
            },
            {
              name: "Compile dialect",
              detail: "SQL highlighting and execution path adapt to PostgreSQL, MySQL, Snowflake, BigQuery, etc.",
            },
            {
              name: "Preview limit",
              detail: "Enforce row/time caps so exploratory queries stay safe in shared workspaces.",
            },
            {
              name: "Shape check",
              detail: "Surface column types and null ratios so you know what Studio will map.",
            },
            {
              name: "Promote",
              detail: "Handoff creates/updates a Studio plan — mapping + G1–G9 still required before write.",
            },
          ]}
        />
      </Chapter>

      <Chapter id="qy-caps" kicker="Capabilities" title="What you can do in the playground">
        <div className="lp-mkt-prose">
          <p>
            Author multi-driver SQL against PostgreSQL, MySQL, Snowflake, BigQuery, and other SQLAlchemy-backed
            engines — plus Mongo-style filters when the connector is document-native. Every result is a preview,
            capped by workspace policy, with column types surfaced so you know what Studio will map. When a slice
            is ready, promote it into a Transfer Studio plan with mapping, gates, and verification attached. Every
            query is attributed to its user in enterprise workspaces.
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
      title="A copilot on its own local engine"
      lead="Ask why a gate failed, how to fix a map, what a Job Theater run did, or how many rows a table holds. Pilot answers from the same evidence Studio and MCP use, on Datawrap’s own local engine — no cloud AI key required — and anything that changes data waits for your Confirm."
      ctaPrimary="Try Datawrap Pilot"
      ctaSecondary="MCP Server"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-mcp")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.pilot}
          alt="Datawrap Pilot in the live workspace"
          surface="Datawrap Pilot"
        />
      }
      liveFrames={PRODUCT_FRAMES.pilot}
      liveTitle="Datawrap Pilot in the live workspace"
      liveSurface="Datawrap Pilot"
      stats={[
        { value: "Local", label: "Own engine · no cloud key" },
        { value: "Gates", label: "Explain failures" },
        { value: "Maps", label: "Propose fixes" },
        { value: "Confirm", label: "Before anything moves" },
      ]}
      next="product-mcp"
      nextLabel="MCP Server"
      onNavigate={onNavigate}
    >
      <Chapter id="dp-what" kicker="What it is" title="Chat on the governed engine">
        <div className="lp-mkt-prose">
          <p>
            Datawrap Pilot is an operator copilot on the governed engine. When it proposes a mapping fix or quarantine
            policy, the change flows through Transfer Studio’s review and the nine core gates. Pilot’s own local engine
            answers from your schemas and job evidence, on the box; connecting a third-party model in Settings is
            optional and only polishes the wording — transfer facts and Confirm always come from the engine.
          </p>
        </div>
        <AlgoBlock
          title="Triage loop"
          lead="Pilot answers from job evidence — gate messages, quarantine samples, map scores — then hands you into Studio or Theater with context preserved."
          steps={[
            {
              name: "Ground on artifacts",
              detail: "Load the job’s gate results, mapping edges, and quarantine samples, and answer from them.",
            },
            {
              name: "Explain failure",
              detail: "Translate G1–G9 blockers into plain language (e.g. G4 confidence, G5 dry-run cast failures).",
            },
            {
              name: "Propose fix",
              detail: "Suggest synonym pins, coerce rules, or quarantine policy — still requiring human/agent confirmation.",
            },
            {
              name: "Handoff",
              detail: "Deep-link into Transfer Studio or Job Theater with the same job ID and map context.",
            },
            {
              name: "Re-run gates",
              detail: "Any accepted change re-enters preflight, and proof comes from the engine’s post-write verification.",
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
          alt="MCP Server page in the live workspace"
          surface="MCP Server"
        />
      }
      liveFrames={PRODUCT_FRAMES.mcp}
      liveTitle="Agent runs still land in the real workspace"
      liveSurface="MCP Server"
      stats={[
        { value: "MCP", label: "Tool surface" },
        { value: "RBAC", label: "On every call" },
        { value: "Audit", label: "Agent runs logged" },
        { value: "Same", label: "Gates as UI" },
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
            holds a run, the agent sees the same gate evidence a human would in Job Theater.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Agent", sub: "Cursor · Claude" },
            { label: "MCP", sub: "Tools · auth", accent: true },
            { label: "RBAC", sub: "Workspace roles" },
            { label: "Engine", sub: "Map · G1–G9", accent: true },
            { label: "Job", sub: "Theater" },
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
              detail: "The job appears in Theater tagged as agent-initiated; tool payloads carry secret references only.",
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
            Tools group cleanly around <strong>catalog &amp; connectors</strong> (warehouses, lakes, databases, and
            apps), <strong>transfer plans</strong> (create and update maps, with review flags kept on ambiguous
            edges), <strong>run &amp; status</strong> (enqueue governed runs, re-run jobs, and poll phases and
            proof), <strong>schedules</strong> (create and inspect recurring syncs, each confirmed by a person),
            and <strong>quarantine read</strong> (sample held-back rows for agent-assisted fixes, policy-scoped).
            The API surface is thin on purpose — the engine decides what is safe.
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
      lead="Move data across schemas that were never 1:1. Semantic maps you can review, nine preflight gates (G1–G9), quarantine with reasons, and a verified read-back before cutover."
      ctaPrimary="Start a migration"
      ctaSecondary="Open Transfer Studio"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-transfer")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.transferSource}
          alt="Transfer Studio source — profiled sample-orders before a migration write"
          surface="Migrations · Transfer Studio"
          route={{ source: "PostgreSQL · public.orders", dest: "Snowflake · ANALYTICS.ORDERS" }}
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
          body: "Dry-run sends coerce failures to quarantine with column, value, and reason. An intentional cast is a signed risk contract.",
        },
        {
          title: "Signed proof at cutover",
          body: "Pilot a subset first, then the full write. Finance archives a signed proof pack — counts, checksums, and a census that MATCH.",
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
          body: "Every microsecond carries, DATETIME2(7) keeps its seventh digit, and Gate-8 compares timestamps at full precision.",
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
        <AlgorithmCinemaBand
          kicker="Mapping"
          title="Watch every edge earn a confidence score"
          lead="Format classify, role enrichment, and continuous confidence. Ambiguous edges pause for human review before they pin."
          compact
        >
          <MappingCinema />
        </AlgorithmCinemaBand>
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
                Explore Pipelines
              </button>
            </div>
          </div>
          <div className="lp-sol-hero-visual">
            <ProductShot
              src={WORKSPACE_SHOT.jobs}
              alt="Job Theater destination population after a warehouse load"
              surface="Warehouse · Job Theater"
              route={{ source: "PostgreSQL · public.orders", dest: "Snowflake · ANALYTICS.ORDERS" }}
            />
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-mkt-body">
          <LiveProductReel
            frames={SOLUTION_FRAMES.warehouse}
            title="Warehouse loads in the live workspace"
            surface="Job Theater"
          />
        </section>
      </MarketingReveal>

      <section className="lp-wh-rail" aria-label="Destinations">
        <div className="lp-shell lp-wh-rail-inner">
          <span>Snowflake</span>
          <span>BigQuery</span>
          <span>Apache Iceberg</span>
          <span>S3 · ADLS · GCS</span>
          <span>Gate-8 MATCH</span>
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
                <li>The same G1–G9 contract as Studio migrations</li>
              </ul>
            </div>
            <aside className="lp-wh-panel" aria-label="Preflight snapshot">
              <header>
                <strong>Destination probe</strong>
                <em>Ready</em>
              </header>
              <div><span>Reachability</span><em>ok</em></div>
              <div><span>Privileges</span><em>write granted</em></div>
              <div><span>Capacity</span><em>slots available</em></div>
              <div><span>Preflight</span><em>9 / 9</em></div>
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
                BigQuery. Coerce failures quarantine in the open with their reason.
              </p>
              <ul className="lp-wh-list">
                <li>Driver-aware load jobs with quarantine samples</li>
                <li>Snowflake password or key-pair authentication</li>
                <li>Promote the same plan into Pipelines for recurring cadence</li>
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
              <h2>Four steps from probe to MATCH</h2>
            </div>
            <ol className="lp-wh-flow-list">
              <li>
                <span>01</span>
                <strong>Probe</strong>
                <p>Destination rights and capacity before the window opens.</p>
              </li>
              <li>
                <span>02</span>
                <strong>Map &amp; gate</strong>
                <p>Semantic maps and nine fail-fast gates (G1–G9) — identical to Studio.</p>
              </li>
              <li>
                <span>03</span>
                <strong>Bulk write</strong>
                <p>Driver path with quarantine for coerce failures.</p>
              </li>
              <li>
                <span>04</span>
                <strong>Prove</strong>
                <p>Row counts, checksums, and a value and key census flash MATCH.</p>
              </li>
            </ol>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <AlgorithmCinemaBand
          kicker="Proof"
          title="Read-back verification flashes MATCH"
          lead="The engine hashes mapped source rows, reads the destination back, and flashes MATCH when counts, content, and keys agree."
          compact
        >
          <ProofCinema />
        </AlgorithmCinemaBand>
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
      lead="Keep systems aligned on a cadence — native CDC or typed watermarks, upsert, mirror with soft delete, schema-drift review, and Job Theater verification on every tick."
      ctaPrimary="Create a sync"
      ctaSecondary="Pipelines product"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-pipelines")}
      heroVisual={
        <ProductShot
          src={WORKSPACE_SHOT.pipelines}
          alt="Schedules workspace for recurring sync"
          surface="Recurring sync · Schedules"
          route={{ source: "PostgreSQL · public.orders", dest: "BigQuery · analytics.orders" }}
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
          title: "Theater on every run",
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
          body: "Job Theater surfaces drift reviews and quarantine. Decide once; the next tick reuses the decision.",
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
        <AlgorithmCinemaBand
          kicker="CDC"
          title="Snapshot + LSN handoff, then streaming upserts"
          lead="Start with a snapshot window, hand off at a logical cursor, and stream idempotent upserts. Redelivery is safe by design."
          compact
        >
          <CdcCinema />
        </AlgorithmCinemaBand>
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
  liveFrames,
  liveTitle,
  liveSurface,
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

      {liveFrames && liveFrames.length > 0 ? (
        <MarketingReveal>
          <section className="lp-mkt-body">
            <LiveProductReel
              frames={liveFrames}
              title={liveTitle ?? "Inside the live workspace"}
              surface={liveSurface ?? "Workspace"}
            />
          </section>
        </MarketingReveal>
      ) : null}

      <MarketingReveal>
        <section className="lp-sol-outcomes" aria-label="What you get">
          {outcomes.map((item, i) => (
            <article key={item.title} className="lp-sol-outcome" style={{ "--reveal-i": i } as CSSProperties}>
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
              <li key={step.n} className="lp-sol-step">
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
              <article key={cap.title} className="lp-sol-cap">
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
              <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate(next)}>
                Next: {nextLabel} →
              </button>
              <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("help")}>
                Read the docs
              </button>
            </div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}
