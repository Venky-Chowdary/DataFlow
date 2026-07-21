import { useEffect, useState, type CSSProperties, type ReactNode } from "react";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";
import {
  AlgoBlock,
  GateTable,
  LiveProductReel,
  PRODUCT_FRAMES,
  ProofCallout,
  REAL_PREFLIGHT_GATES,
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

function Shot({
  label,
  caption,
  children,
  tone = "light",
}: {
  label: string;
  caption: string;
  children: ReactNode;
  tone?: "light" | "ink";
}) {
  return (
    <figure className={`lp-mkt-shot lp-mkt-shot--${tone}`}>
      <div className="lp-mkt-shot-chrome">
        <span className="lp-mkt-shot-dots" aria-hidden>
          <i /><i /><i />
        </span>
        <span className="lp-mkt-shot-label">{label}</span>
      </div>
      <div className="lp-mkt-shot-body">{children}</div>
      <figcaption>{caption}</figcaption>
    </figure>
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
            <LiveProductReel frames={liveFrames} title={liveTitle ?? "Inside the live workspace"} />
          </section>
        </MarketingReveal>
      ) : null}
      {children}
      <MarketingReveal>
        <section className="lp-mkt-body">
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate(next)}>
              Next: {nextLabel} →
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("help")}>
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

/* ─── Unique hero mocks ─────────────────────────────────────────── */

function TransferStudioMock() {
  const [gate, setGate] = useState(0);
  const gates = REAL_PREFLIGHT_GATES.map((g) => `${g.id} ${g.title}`);
  useEffect(() => {
    const id = window.setInterval(() => setGate((g) => (g + 1) % REAL_PREFLIGHT_GATES.length), 900);
    return () => window.clearInterval(id);
  }, []);

  return (
    <Shot label="Transfer Studio · Orders migration" caption="Live preflight advancing through eight fail-fast gates before write.">
      <div className="lp-mkt-ui-grid lp-mkt-ui-grid--studio">
        <div className="lp-mkt-ui-pane">
          <h4>Semantic map</h4>
          {[
            ["order_amt", "payment_amount", "0.93"],
            ["cust_email", "email", "0.91"],
            ["order_id", "order_key", "0.88"],
          ].map(([s, d, c]) => (
            <div key={s} className="lp-mkt-ui-map-row">
              <code>{s}</code>
              <span className="lp-mkt-ui-map-arrow" aria-hidden>→</span>
              <code>{d}</code>
              <em>{c}</em>
            </div>
          ))}
        </div>
        <div className="lp-mkt-ui-pane">
          <h4>Preflight · {Math.min(gate + 1, 8)}/8</h4>
          <div className="lp-mkt-ui-progress" aria-hidden>
            <i style={{ width: `${((gate + 1) / 8) * 100}%` }} />
          </div>
          <ul className="lp-mkt-ui-gates">
            {gates.map((g, i) => (
              <li key={g} className={i <= gate ? "is-pass" : "is-pending"}>
                <span>{g}</span>
                <em>{i <= gate ? "pass" : "…"}</em>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </Shot>
  );
}

function JobTheaterMock() {
  const [phase, setPhase] = useState(0);
  const phases = ["Queued", "Profiling", "Writing", "Reconciling", "Complete"];
  useEffect(() => {
    const id = window.setInterval(() => setPhase((p) => (p + 1) % phases.length), 1100);
    return () => window.clearInterval(id);
  }, [phases.length]);
  const pct = [8, 28, 62, 88, 100][phase];

  return (
    <Shot label="Job Theater · job_7f3a91" caption="Batch progress, phase timeline, and proof counters update as the engine runs.">
      <div className="lp-mkt-ui-theater">
        <div className="lp-mkt-ui-theater-head">
          <strong>CSV → PostgreSQL · orders</strong>
          <span className={`lp-mkt-ui-phase is-${phases[phase].toLowerCase()}`}>{phases[phase]}</span>
        </div>
        <div className="lp-mkt-ui-progress lp-mkt-ui-progress--lg" aria-hidden>
          <i style={{ width: `${pct}%` }} />
        </div>
        <div className="lp-mkt-ui-metrics">
          <div><strong>12,480</strong><span>Source rows</span></div>
          <div><strong>{phase >= 2 ? "12,471" : "—"}</strong><span>Written</span></div>
          <div><strong>{phase >= 2 ? "9" : "—"}</strong><span>Quarantined</span></div>
          <div><strong>{phase >= 4 ? "OK" : "…"}</strong><span>Checksum</span></div>
        </div>
        <ol className="lp-mkt-ui-timeline">
          {phases.map((p, i) => (
            <li key={p} className={i <= phase ? "is-done" : ""}>
              <span className="lp-mkt-ui-timeline-dot" />
              {p}
            </li>
          ))}
        </ol>
      </div>
    </Shot>
  );
}

function PipelinesMock() {
  const rows = [
    { name: "Orders hourly", cadence: "Every hour", mode: "Watermark", next: "12 min", status: "healthy" },
    { name: "Customers daily", cadence: "Daily 02:00 UTC", mode: "Upsert", next: "5h", status: "healthy" },
    { name: "Events → Snowflake", cadence: "Every 15 min", mode: "Append", next: "3 min", status: "drift" },
  ];
  return (
    <Shot label="Pipelines · workspace schedules" caption="Cadence, write mode, and health for every recurring sync — one glance.">
      <table className="lp-mkt-ui-table">
        <thead>
          <tr>
            <th>Pipeline</th>
            <th>Cadence</th>
            <th>Mode</th>
            <th>Next</th>
            <th>Health</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.name}>
              <td><strong>{r.name}</strong></td>
              <td>{r.cadence}</td>
              <td><code>{r.mode}</code></td>
              <td>{r.next}</td>
              <td><span className={`lp-mkt-ui-pill is-${r.status}`}>{r.status}</span></td>
            </tr>
          ))}
        </tbody>
      </table>
    </Shot>
  );
}

function QueryMock() {
  return (
    <Shot label="Query Playground · PostgreSQL / analytics" caption="Ad-hoc SQL against live connectors — preview rows, then export or hand off to Transfer Studio.">
      <div className="lp-mkt-ui-query">
        <pre className="lp-mkt-ui-sql">{`SELECT order_id, payment_amount, email
FROM orders
WHERE created_at >= NOW() - INTERVAL '7 days'
LIMIT 200;`}</pre>
        <div className="lp-mkt-ui-result-bar">
          <span>200 rows · 48 ms</span>
          <span className="lp-mkt-ui-pill is-healthy">preview</span>
        </div>
        <table className="lp-mkt-ui-table lp-mkt-ui-table--compact">
          <thead>
            <tr><th>order_id</th><th>payment_amount</th><th>email</th></tr>
          </thead>
          <tbody>
            <tr><td>ord_1842</td><td>129.00</td><td>a@retail.co</td></tr>
            <tr><td>ord_1843</td><td>48.50</td><td>b@retail.co</td></tr>
            <tr><td>ord_1844</td><td>312.10</td><td>c@retail.co</td></tr>
          </tbody>
        </table>
      </div>
    </Shot>
  );
}

function PilotMock() {
  return (
    <Shot label="Data Pilot · triage chat" caption="Natural language over the same engine — failed gates, mapping fixes, and Job Theater handoff.">
      <div className="lp-mkt-ui-chat">
        <div className="lp-mkt-ui-bubble is-user">
          Why did the Orders → BigQuery job fail preflight?
        </div>
        <div className="lp-mkt-ui-bubble is-bot">
          Gate <strong>Type coercion</strong> blocked <code>order_amt</code> (STRING) → <code>payment_amount</code> (NUMERIC).
          214 sample values contain currency symbols. Pin a coerce rule or quarantine those rows.
        </div>
        <div className="lp-mkt-ui-chat-actions">
          <span>Open Job Theater</span>
          <span>Fix mapping</span>
          <span>Quarantine policy</span>
        </div>
      </div>
    </Shot>
  );
}

function McpMock() {
  return (
    <Shot label="MCP · Cursor agent tools" caption="Agents call the same governed tools — never raw destination passwords.">
      <div className="lp-mkt-ui-mcp">
        <div className="lp-mkt-ui-mcp-tool">
          <code>dataflow.transfer.run</code>
          <span className="lp-mkt-ui-pill is-healthy">allowed</span>
        </div>
        <pre className="lp-mkt-ui-sql">{`{
  "source": "pg.public.orders",
  "destination": "bq.analytics.orders",
  "mode": "upsert",
  "preflight": true
}`}</pre>
        <ul className="lp-mkt-ui-mcp-log">
          <li className="is-pass">RBAC: transfer:execute ✓</li>
          <li className="is-pass">Preflight 8/8 ✓</li>
          <li className="is-pass">Job job_7f3a91 queued → Job Theater</li>
        </ul>
      </div>
    </Shot>
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
      title="The wizard that refuses silent data loss"
      lead="Connect any source to any destination, review semantic maps with confidence scores, pass eight preflight gates (G1–G8), then write with quarantine and checksum proof — all in one governed path."
      ctaPrimary="Open Transfer Studio"
      ctaSecondary="See Job Theater"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-jobs")}
      heroVisual={<TransferStudioMock />}
      liveFrames={PRODUCT_FRAMES.transfer}
      liveTitle="Transfer Studio in the live workspace"
      stats={[
        { value: "G1–G8", label: "Real preflight gates" },
        { value: "Any→any", label: "Route coverage" },
        { value: "≥0.85", label: "Default confidence floor" },
        { value: "Proof", label: "Checksum after write" },
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
            Data Pilot and MCP — so UI, chat, and agents never diverge into unsafe shortcuts.
          </p>
          <p>
            Unlike script-first ETL, Studio keeps evidence with the job: mapping decisions, gate results, quarantined rows,
            and reconciliation hashes. That evidence is what Job Theater displays after you click run.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Connect", sub: "Drivers · files" },
            { label: "Profile", sub: "Types · keys" },
            { label: "Map", sub: "Semantics · confidence", accent: true },
            { label: "Preflight", sub: "G1–G8", accent: true },
            { label: "Write", sub: "Quarantine" },
            { label: "Prove", sub: "Checksums" },
          ]}
        />
      </Chapter>

      <Chapter id="ts-algo-map" kicker="Algorithm" title="How semantic mapping actually scores columns">
        <AlgoBlock
          title="Mapping pipeline"
          lead="The mapper does not string-match names. It classifies format/domain, enriches each column with a semantic role, then scores every source→target edge. Ambiguous edges stay pinned for human review."
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
                "Candidates ranked by exact name → synonym dictionary → role match → type compatibility. Confidence is continuous (0–1), not a binary guess.",
            },
            {
              name: "Threshold gate",
              detail:
                "Workspace validation mode sets the floor (strict ≈ 0.85; engine floor 0.72). Below-threshold edges block G4 until you pin, reject, or remap.",
            },
            {
              name: "Transform attach",
              detail:
                "Safe coerce / date / decimal transforms are attached only when reversible or explicitly approved — never silent lossy casts.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Why this beats name matching.</strong>
          <p>
            Source <code>order_amt</code> (NUMERIC) → destination <code>payment_amount</code> scores high on role + type
            even when names never matched. A STRING with currency symbols trying to land in NUMERIC fails G4/G5 instead of writing garbage.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="ts-gates" kicker="Preflight · G1–G8" title="Eight gates from the real engine — fail-fast before write">
        <div className="lp-mkt-prose">
          <p>
            These are not marketing labels. They map to <code>GateId</code> in DataFlow&apos;s preflight package.
            A single <em>block</em> stops write — there is no “best effort” mode that drops rows quietly.
          </p>
        </div>
        <GateTable />
      </Chapter>

      <Chapter id="ts-write" kicker="Execute" title="Write path: quarantine, then prove">
        <AlgoBlock
          title="Load + reconcile"
          lead="After G1–G8 pass, the engine chunks rows, applies mapped transforms, writes with the chosen mode, and isolates bad rows. Success is not “job finished” — it is checksum-matched proof."
          steps={[
            {
              name: "Chunk + transform",
              detail: "Rows are batched; each field follows its mapped coerce path. Failures go to quarantine with column, value, and reason.",
            },
            {
              name: "Destination write",
              detail: "Append, overwrite, upsert, or watermark incremental — validated earlier by the write plan / DDL gates.",
            },
            {
              name: "Quarantine isolate",
              detail: "Rejected rows never land in the clean set and never disappear. Operators inspect them in Job Theater.",
            },
            {
              name: "Checksum reconcile",
              detail:
                "Post-load: compare source-mapped checksum vs destination sample/counts (G8 plan). Matched or failed is recorded on the job — never assumed.",
            },
          ]}
        />
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Role detection", d: "Amounts, emails, IDs, and timestamps classified so synonyms align across schemas." },
            { t: "Confidence scores", d: "Every edge shows a score. You pin, reject, or override before production write." },
            { t: "Type-aware coerce", d: "Only fail-fast-safe coercions are offered. Unsafe casts never silently apply." },
            { t: "Synonym dictionary", d: "Workspace synonyms improve over time — Pilot can propose additions from failed jobs." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
      </Chapter>

      <Chapter id="ts-scenario" kicker="Worked example" title="Retail orders CSV → PostgreSQL">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Upload <code>sample-orders.csv</code> and select PostgreSQL <code>public.orders</code> (or Load sample in Studio).</li>
            <li>Review map: <code>order_amt → payment_amount</code> (high confidence), <code>cust_email → email</code>.</li>
            <li>Preflight: G1–G3 pass; G5 flags currency-symbol rows — quarantine policy captures them.</li>
            <li>Write clean rows; Job Theater shows checksum match on written set + quarantined rows with reasons.</li>
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
      heroVisual={<JobTheaterMock />}
      liveFrames={PRODUCT_FRAMES.jobs}
      liveTitle="Job Theater with real reconcile evidence"
      stats={[
        { value: "Live", label: "Batch progress" },
        { value: "Phases", label: "Queue → reconcile" },
        { value: "Quarantine", label: "Bad rows + reasons" },
        { value: "Proof", label: "Counts · checksums" },
      ]}
      next="product-pipelines"
      nextLabel="Pipelines"
      onNavigate={onNavigate}
    >
      <Chapter id="jt-what" kicker="What it is" title="Operations visibility for governed loads">
        <div className="lp-mkt-prose">
          <p>
            Transfer Studio plans the load. Job Theater watches it. Every job transitions through explicit phases —
            Queued, Preflight, Extract/Write, Reconciling, Complete (or Failed) — with counters that never invent success.
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
            { label: "Preflight", sub: "G1–G8", accent: true },
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
          lead="Success is not “status=complete”. After write, the engine recomputes a content checksum over mapped source rows and compares it to what landed in the destination — plus row counts."
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
              name: "Read target sample",
              detail: "Probe destination with the reconcile plan (counts + content sample aligned on a stable key when present).",
            },
            {
              name: "Compare",
              detail: "Row-count delta and checksum equality → matched / mismatched. Mismatch fails the job proof — never silent OK.",
            },
            {
              name: "Surface in Theater",
              detail: "Timeline, counters, quarantine samples, and proof report become the operator’s single source of truth.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Zero silent data loss.</strong>
          <p>
            Quarantined rows are part of the proof story: written set can checksum-match while N rows remain isolated
            with reasons. Operators fix and retry without guessing what disappeared.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="jt-caps" kicker="Capabilities" title="What operators get on every run">
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Phase timeline", d: "Ordered phases with timestamps — no opaque spinner that hides where time went." },
            { t: "Batch counters", d: "Source, written, quarantined, and skipped counts update as batches commit." },
            { t: "Quarantine samples", d: "Inspect failing values without dumping the whole table into logs." },
            { t: "Proof report", d: "Row counts and content checksums after write — matched or failed, never assumed." },
            { t: "Retry with context", d: "Re-run from the failed plan with the same mapping and gate evidence attached." },
            { t: "Agent parity", d: "MCP-triggered jobs appear identically — same phases, same proof artifacts." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
      </Chapter>

      <Chapter id="jt-scenario" kicker="Worked example" title="Watching a warehouse load fail safely">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Pipeline kicks off Snowflake upsert at 02:00 UTC — job appears in Theater as Queued.</li>
            <li>G2 Destination + G7 Capacity probe warehouse slots; policy continues with a warning.</li>
            <li>During Write, 42 rows quarantine on null PK; written set reconciles checksum OK.</li>
            <li>Operator opens quarantine sample, fixes source nulls, retries — Complete with 0 quarantined.</li>
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
      title="Recurring sync that still runs preflight"
      lead="Hourly, daily, and weekly schedules with watermark incremental, upsert, append, and overwrite — every tick reuses Transfer Studio’s gates, quarantine, and Job Theater proof."
      ctaPrimary="Schedule a pipeline"
      ctaSecondary="Recurring sync guide"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("solution-sync")}
      heroVisual={<PipelinesMock />}
      liveFrames={PRODUCT_FRAMES.pipelines}
      liveTitle="Pipelines in the live workspace"
      stats={[
        { value: "Hourly+", label: "Cadences" },
        { value: "4", label: "Write modes" },
        { value: "Drift", label: "Schema blocking" },
        { value: "Same", label: "Engine as Studio" },
      ]}
      next="product-query"
      nextLabel="Query Playground"
      onNavigate={onNavigate}
    >
      <Chapter id="pl-what" kicker="What it is" title="Schedules on the governed engine — not a second product">
        <div className="lp-mkt-prose">
          <p>
            Pipelines turn a proven Transfer Studio plan into a cadence. Each run is a real job in Job Theater —
            with the same mapping, gates, and reconciliation. There is no “scheduler-only” path that skips proof
            for convenience.
          </p>
          <p>
            Choose watermark incremental for change data, upsert for slowly changing dimensions, append for events,
            or overwrite for full refresh. Schema drift blocks the next tick until you review the diff.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Plan", sub: "Studio map" },
            { label: "Schedule", sub: "Cron · cadence", accent: true },
            { label: "Tick", sub: "Enqueue job" },
            { label: "Preflight", sub: "G1–G8", accent: true },
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
              detail: "For incremental mode, read the last high-water mark; select only new/changed rows.",
            },
            {
              name: "Run G1–G8",
              detail: "Full preflight every tick. Schema drift or confidence regressions block instead of writing wrong shapes.",
            },
            {
              name: "Write + quarantine",
              detail: "Same chunked write path as Studio; bad rows isolate with reasons.",
            },
            {
              name: "Prove in Theater",
              detail: "Job record gets checksum + counts. Operators and agents see the same artifact.",
            },
          ]}
        />
      </Chapter>

      <Chapter id="pl-modes" kicker="Write modes" title="Pick the mode that matches the destination">
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Watermark incremental", d: "Advance a high-water mark column; only new/changed rows move each tick." },
            { t: "Upsert", d: "Merge on primary/business keys where the destination driver supports it." },
            { t: "Append", d: "Insert-only streams for events and immutable fact tables." },
            { t: "Overwrite", d: "Full refresh when the table must match source exactly after the run." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
      </Chapter>

      <Chapter id="pl-scenario" kicker="Worked example" title="Hourly orders into BigQuery">
        <div className="lp-mkt-scenario">
          <ol>
            <li>Promote Studio plan Orders PG → BigQuery with watermark on <code>updated_at</code>.</li>
            <li>Set cadence every hour; first tick backfills, later ticks move only deltas.</li>
            <li>Source adds a column — drift gate blocks; operator accepts map, next tick resumes.</li>
            <li>Each hour’s job shows written counts + checksum in Job Theater.</li>
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
      heroVisual={<QueryMock />}
      liveFrames={PRODUCT_FRAMES.query}
      liveTitle="Query Playground in the live workspace"
      stats={[
        { value: "SQL", label: "Relational drivers" },
        { value: "Docs", label: "Mongo-style queries" },
        { value: "Preview", label: "Row-limited safe" },
        { value: "Handoff", label: "Into Transfer Studio" },
      ]}
      next="product-pilot"
      nextLabel="Data Pilot"
      onNavigate={onNavigate}
    >
      <Chapter id="qy-what" kicker="What it is" title="Exploration that respects connector boundaries">
        <div className="lp-mkt-prose">
          <p>
            Query Playground is for discovery and validation — not a second write path. You query through the same
            connector credentials and RBAC as the rest of the workspace, with preview limits so exploratory SELECTs
            cannot accidentally become full-table pulls.
          </p>
          <p>
            When a query defines the slice you want to move, hand off to Transfer Studio to attach mapping, preflight,
            and proof. Exploration and governed load stay separate on purpose.
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
          lead="Playground never writes destinations. It reads under connector RBAC, caps result size, and only then can promote a selection into a transfer plan."
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
              detail: "Handoff creates/updates a Studio plan — mapping + G1–G8 still required before write.",
            },
          ]}
        />
      </Chapter>

      <Chapter id="qy-caps" kicker="Capabilities" title="What you can do in the playground">
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Multi-driver SQL", d: "PostgreSQL, MySQL, Snowflake, BigQuery, and other SQLAlchemy-backed engines." },
            { t: "Document queries", d: "Mongo-style filters for collections when the connector is document-native." },
            { t: "Result preview", d: "Column types and sample rows before you commit to a transfer plan." },
            { t: "Export samples", d: "CSV/JSONL samples for offline review — still scoped by preview limits." },
            { t: "Studio handoff", d: "Promote a validated query/table selection into a Transfer Studio plan." },
            { t: "Audit context", d: "Who queried what is attributable in enterprise workspaces." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
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
      kicker="Product · Data Pilot"
      title="Natural-language triage on the governed engine"
      lead="Ask why a gate failed, how to fix a map, or what a Job Theater run did — Pilot answers with the same evidence Studio and MCP use, and can hand you back into the wizard when you need controls."
      ctaPrimary="Try Data Pilot"
      ctaSecondary="MCP Server"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-mcp")}
      heroVisual={<PilotMock />}
      liveFrames={PRODUCT_FRAMES.pilot}
      liveTitle="Data Pilot in the live workspace"
      stats={[
        { value: "NL", label: "Triage chat" },
        { value: "Gates", label: "Explain failures" },
        { value: "Maps", label: "Propose fixes" },
        { value: "Handoff", label: "Studio · Theater" },
      ]}
      next="product-mcp"
      nextLabel="MCP Server"
      onNavigate={onNavigate}
    >
      <Chapter id="dp-what" kicker="What it is" title="Chat that cannot bypass preflight">
        <div className="lp-mkt-prose">
          <p>
            Data Pilot is an operator copilot, not a shadow ETL path. When it proposes a mapping fix or quarantine
            policy, the change still flows through Transfer Studio’s review and the eight gates. That is how Pilot
            stays trustworthy for production teams.
          </p>
        </div>
        <AlgoBlock
          title="Triage loop"
          lead="Pilot answers from job evidence — gate messages, quarantine samples, map scores — then hands you into Studio or Theater with context preserved."
          steps={[
            {
              name: "Ground on artifacts",
              detail: "Load the job’s gate results, mapping edges, and quarantine samples — not invented narratives.",
            },
            {
              name: "Explain failure",
              detail: "Translate G1–G8 blockers into plain language (e.g. G4 confidence, G5 dry-run cast failures).",
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
              detail: "Any accepted change re-enters preflight. Pilot never marks a load proven on its own.",
            },
          ]}
        />
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Gate explainers", d: "Plain-language reasons for G1–G8 failures with the underlying evidence." },
            { t: "Mapping proposals", d: "Suggest synonym pins and coerce rules from failed samples." },
            { t: "Job inspection", d: "Summarize Theater phases, quarantine counts, and proof status." },
            { t: "Safe handoff", d: "Deep-link into Studio or Theater with the job and map context preserved." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
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
      title="Agents get tools — never raw passwords"
      lead="Cursor, Claude, and VS Code call DataFlow MCP tools under workspace RBAC. Transfers still map, preflight, quarantine, and reconcile — with audit entries for every agent-initiated run."
      ctaPrimary="Connect an agent"
      ctaSecondary="Security overview"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("security")}
      heroVisual={<McpMock />}
      liveFrames={PRODUCT_FRAMES.mcp}
      liveTitle="Agent runs still land in the real workspace"
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
            The MCP server exposes connectors, transfer plans, job status, and controlled run actions. Agents never
            receive destination secrets in tool responses. If preflight fails, the agent sees the same gate evidence
            a human would in Job Theater.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Agent", sub: "Cursor · Claude" },
            { label: "MCP", sub: "Tools · auth", accent: true },
            { label: "RBAC", sub: "Workspace roles" },
            { label: "Engine", sub: "Map · G1–G8", accent: true },
            { label: "Job", sub: "Theater" },
            { label: "Audit", sub: "Immutable log" },
          ]}
        />
      </Chapter>

      <Chapter id="mcp-algo" kicker="Algorithm" title="How an agent-initiated transfer stays safe">
        <AlgoBlock
          title="MCP run path"
          lead="Tools are thin wrappers over the same engine. Auth → RBAC → plan → preflight → enqueue — never raw SQL write to destination."
          steps={[
            {
              name: "Authenticate",
              detail: "Workspace token / SSO-backed service account. No anonymous tool calls.",
            },
            {
              name: "Authorize",
              detail: "RBAC checks transfer:execute, connector scope, and environment (prod vs sandbox).",
            },
            {
              name: "Resolve plan",
              detail: "Agent supplies connector IDs + mode; ambiguous maps still require review flags.",
            },
            {
              name: "Preflight G1–G8",
              detail: "Identical gates as Studio. Blockers return structured evidence to the agent.",
            },
            {
              name: "Enqueue + audit",
              detail: "Job appears in Theater tagged as agent-initiated; secrets never appear in tool payloads.",
            },
          ]}
        />
        <ProofCallout>
          <strong>Never raw passwords.</strong>
          <p>
            MCP responses include job IDs, gate results, and quarantine summaries — not connector secrets or full
            row dumps unless an explicit, audited policy allows sample inspection.
          </p>
        </ProofCallout>
      </Chapter>

      <Chapter id="mcp-tools" kicker="Tooling" title="Representative tool groups">
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Catalog & connectors", d: "List transfer-ready drivers and connection health." },
            { t: "Transfer plans", d: "Create/update maps; never skip review flags on ambiguous edges." },
            { t: "Run & status", d: "Enqueue governed runs; poll phases and proof." },
            { t: "Quarantine read", d: "Sample bad rows for agent-assisted fixes — still policy-scoped." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
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
    <SurfaceShell
      kicker="Solution · Migrations"
      title="Cross-schema moves when names never matched"
      lead="Migrate databases and files across schemas that were never 1:1 — with semantic maps you can review, type-safe coercion, and checksum proof before cutover."
      ctaPrimary="Start a migration"
      ctaSecondary="Transfer Studio"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-transfer")}
      heroVisual={<TransferStudioMock />}
      stats={[
        { value: "Semantic", label: "Column matching" },
        { value: "Review", label: "Ambiguous edges" },
        { value: "Fail-fast", label: "Unsafe casts" },
        { value: "Cutover", label: "Checksum proof" },
      ]}
      next="solution-warehouse"
      nextLabel="Warehouse loading"
      onNavigate={onNavigate}
    >
      <Chapter id="mig-flow" kicker="How it works" title="Migration flow from discovery to cutover">
        <div className="lp-mkt-prose">
          <p>
            Migrations fail when teams assume name equality. DataFlow profiles both sides, proposes role-aware maps,
            and forces a review on anything ambiguous. Preflight blocks cutover until contracts, types, and capacity
            clear — then Job Theater proves the load.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Discover", sub: "Schemas" },
            { label: "Map", sub: "Semantics", accent: true },
            { label: "Pilot load", sub: "Sample proof" },
            { label: "Cutover", sub: "Full write", accent: true },
            { label: "Reconcile", sub: "Checksums" },
            { label: "Sign-off", sub: "Evidence" },
          ]}
        />
      </Chapter>
      <Chapter id="mig-caps" kicker="Capabilities" title="Built for messy real-world schemas">
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Role-aware matching", d: "Amounts, emails, and identifiers align even when column names differ." },
            { t: "Dual-run proof", d: "Pilot subset first; full cutover only after checksum confidence." },
            { t: "Quarantine cutover", d: "Bad rows never block the clean set — and never disappear." },
            { t: "Audit pack", d: "Maps, gates, and proof export for migration sign-off." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
      </Chapter>
    </SurfaceShell>
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
    <SurfaceShell
      kicker="Solution · Warehouse loading"
      title="Bulk paths finance can trust"
      lead="Load Snowflake, BigQuery, and Redshift with destination probes, capacity checks, upsert/overwrite modes, and reconciliation reports built for analytics stakeholders."
      ctaPrimary="Load a warehouse"
      ctaSecondary="Pipelines"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-pipelines")}
      heroVisual={<JobTheaterMock />}
      stats={[
        { value: "Native", label: "Warehouse drivers" },
        { value: "Probe", label: "Perms · capacity" },
        { value: "Bulk", label: "Write paths" },
        { value: "Report", label: "Reconcile export" },
      ]}
      next="solution-sync"
      nextLabel="Recurring sync"
      onNavigate={onNavigate}
    >
      <Chapter id="wh-flow" kicker="How it works" title="Warehouse-specific probes before bulk write">
        <div className="lp-mkt-prose">
          <p>
            Warehouse loads are expensive to get wrong. DataFlow probes permissions and capacity, validates the write
            mode against the driver, then executes bulk paths. Post-load row counts and content checksums become the
            report analytics and finance teams can archive.
          </p>
        </div>
        <div className="lp-mkt-capability-grid">
          {[
            { t: "Snowflake", d: "Upsert and overwrite with warehouse slot awareness." },
            { t: "BigQuery", d: "Load jobs with destination table probes and proof counts." },
            { t: "Redshift", d: "Bulk paths with capacity and permission checks." },
            { t: "Scheduled refresh", d: "Promote any warehouse plan into Pipelines for cadence." },
          ].map((c) => (
            <article key={c.t} className="lp-mkt-capability-card">
              <h3>{c.t}</h3>
              <p>{c.d}</p>
            </article>
          ))}
        </div>
      </Chapter>
    </SurfaceShell>
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
    <SurfaceShell
      kicker="Solution · Recurring sync"
      title="Incremental pipelines with quarantine, not hope"
      lead="Keep systems aligned on a cadence — watermark incremental, upsert, schema-drift blocking, and Job Theater visibility from every tick."
      ctaPrimary="Create a sync"
      ctaSecondary="Pipelines product"
      onPrimary={onGetStarted}
      onSecondary={() => onNavigate("product-pipelines")}
      heroVisual={<PipelinesMock />}
      stats={[
        { value: "Cadence", label: "Hourly → weekly" },
        { value: "Watermark", label: "Incremental" },
        { value: "Drift", label: "Blocks bad ticks" },
        { value: "Theater", label: "Every run" },
      ]}
      next="pricing"
      nextLabel="Pricing"
      onNavigate={onNavigate}
    >
      <Chapter id="sy-flow" kicker="How it works" title="From proven plan to reliable ticks">
        <div className="lp-mkt-prose">
          <p>
            Recurring sync is the Pipelines product applied to operational keep-warm loads. Promote a Studio plan,
            pick cadence and mode, and every tick inherits gates and proof. Drift stops the line instead of writing
            into a silently wrong schema.
          </p>
        </div>
        <PacketFlow
          nodes={[
            { label: "Promote", sub: "Studio plan" },
            { label: "Cadence", sub: "Schedule", accent: true },
            { label: "Delta", sub: "Watermark" },
            { label: "Gates", sub: "Each tick", accent: true },
            { label: "Sync", sub: "Upsert/append" },
            { label: "Prove", sub: "Theater" },
          ]}
        />
      </Chapter>
    </SurfaceShell>
  );
}
