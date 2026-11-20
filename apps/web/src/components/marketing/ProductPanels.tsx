import { useEffect, useId, useState, type CSSProperties, type ReactNode } from "react";
import { useInViewOnce } from "../../hooks/useInViewOnce";
import { ALL_CONNECTORS, CATALOG_TOTAL, MARKETING_STACK, PROVEN_DRIVERS, provenBadge } from "../../lib/provenEvidence";
import { EXAMPLE_READ, EXAMPLE_STATEMENT } from "./StatementHero";

/**
 * Datawrap product surfaces rendered in code for the public site.
 *
 * Every panel here is an example. Column names, types, and gate ids come from
 * a recorded run in the hosted workspace (Orders Postgres public.sales_orders
 * into Warehouse Snowflake DW_DEMO.SALES_ORDERS_REPORTING); row keys, reasons,
 * and timings are illustrative. Panels say "Example" where a visitor can see
 * it, and none of them carries a row count as a claim: the product's promise
 * is the identity read = landed + held + skipped, and that holds at any size.
 */

export const RECORDED_ROUTE = {
  source: "public.sales_orders",
  sourceEngine: "PostgreSQL",
  dest: "DW_DEMO.SALES_ORDERS_REPORTING",
  destEngine: "Snowflake",
};

/** The columns of the recorded run, as the Map step showed them. */
export const RECORDED_MAPPING: { col: string; from: string; to: string; transform?: string }[] = [
  { col: "order_id", from: "BIGINT", to: "BIGINT" },
  { col: "region", from: "TEXT", to: "VARCHAR" },
  { col: "store_code", from: "TEXT", to: "VARCHAR" },
  { col: "product_sku", from: "TEXT", to: "VARCHAR" },
  { col: "units", from: "INT4", to: "BIGINT" },
  { col: "unit_price", from: "DECIMAL(10,2)", to: "NUMBER(10,2)" },
  { col: "discount_pct", from: "DECIMAL(5,2)", to: "NUMBER(5,2)" },
  { col: "order_total", from: "DECIMAL(12,2)", to: "NUMBER(12,2)" },
  { col: "status", from: "TEXT", to: "VARCHAR" },
  { col: "ordered_at", from: "TIMESTAMPTZ", to: "TIMESTAMP_LTZ" },
  { col: "attributes", from: "JSONB", to: "VARIANT", transform: "Parse JSON" },
];

/** Validate on the recorded route, in four plain-language groups. */
export const RECORDED_CHECK_GROUPS: {
  name: string;
  checks: { name: string; does: string; state: "pass" | "na" }[];
}[] = [
  {
    name: "Can we read it and write it",
    checks: [
      { name: "Source readable", does: "The saved connection can read the source.", state: "pass" },
      { name: "Destination write access", does: "The destination accepts writes from its saved connection.", state: "pass" },
      { name: "Staging capacity", does: "There is room to stage the load.", state: "pass" },
      { name: "Staging policy", does: "Staging follows the route's rules.", state: "pass" },
    ],
  },
  {
    name: "Will every value fit",
    checks: [
      { name: "Population fit", does: "Source values fit their destination types.", state: "pass" },
      { name: "Sample dry-run", does: "Sample rows go through the real write path.", state: "pass" },
      { name: "Sample reconciliation", does: "The written sample reads back equal.", state: "pass" },
      { name: "Data integrity", does: "Keys are unique and required values are present.", state: "pass" },
      { name: "Target table definition", does: "The destination table definition is valid.", state: "pass" },
      { name: "Create destination table", does: "The new table can be created as planned.", state: "pass" },
    ],
  },
  {
    name: "Do the columns line up",
    checks: [
      { name: "Schema contract", does: "The source schema is the one the plan expects.", state: "pass" },
      { name: "Column mappings", does: "Every mapping is approved.", state: "pass" },
      { name: "Source column coverage", does: "Every source column is mapped or declared dropped.", state: "pass" },
      { name: "Dropped columns", does: "A dropped column carries its reason.", state: "pass" },
      { name: "Required destination columns", does: "Only for an existing table.", state: "na" },
      { name: "Existing table shape", does: "Only for an existing table.", state: "na" },
      { name: "Schema replacement", does: "Only when a destination schema is replaced.", state: "na" },
      { name: "Code crosswalks", does: "Only when code values are translated.", state: "na" },
    ],
  },
  {
    name: "Is it safe to write",
    checks: [
      { name: "Schema change policy", does: "Source changes follow your policy.", state: "pass" },
      { name: "Validation mode", does: "The workspace's validation mode is met.", state: "pass" },
      { name: "Sync contract", does: "The sync mode fits the destination's keys.", state: "pass" },
      { name: "Control totals", does: "Only when control totals are declared.", state: "na" },
      { name: "Destination foreign keys", does: "Only when the destination enforces them.", state: "na" },
    ],
  },
];

function prefersReducedMotion(): boolean {
  try {
    return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch {
    return true;
  }
}

/** Status glyphs, so state never rests on colour alone. */
export function StatusGlyph({ kind }: { kind: "landed" | "held" | "skipped" | "pending" }) {
  return (
    <svg className={`mk-glyph is-${kind}`} viewBox="0 0 12 12" width="12" height="12" aria-hidden focusable="false">
      {kind === "landed" ? <path d="M2.5 6.2 5 8.6l4.6-5.2" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" /> : null}
      {kind === "held" ? (
        <>
          <rect x="3" y="2.5" width="2" height="7" rx="0.6" fill="currentColor" />
          <rect x="7" y="2.5" width="2" height="7" rx="0.6" fill="currentColor" />
        </>
      ) : null}
      {kind === "skipped" ? (
        <>
          <circle cx="6" cy="6" r="4.2" fill="none" stroke="currentColor" strokeWidth="1.3" />
          <path d="M3.2 8.8 8.8 3.2" stroke="currentColor" strokeWidth="1.3" strokeLinecap="round" />
        </>
      ) : null}
      {kind === "pending" ? <circle cx="6" cy="6" r="4" fill="none" stroke="currentColor" strokeWidth="1.3" strokeDasharray="2 1.6" /> : null}
    </svg>
  );
}

function RouteLine() {
  return (
    <p className="mk-panel-route">
      <span>{RECORDED_ROUTE.sourceEngine}</span> <code>{RECORDED_ROUTE.source}</code>
      <span className="mk-panel-route-to">to</span>
      <span>{RECORDED_ROUTE.destEngine}</span> <code>{RECORDED_ROUTE.dest}</code>
    </p>
  );
}

type Share = { landed: number; held: number; skipped: number; pending?: number };

/**
 * The identity as proportions: the read bar and the outcome bar are the same
 * length, which is the whole point. No counts, so it reads the same at any
 * scale.
 */
function IdentityBars({
  share,
  settled = true,
  legend = ["landed", "held", "skipped"],
}: {
  share: Share;
  settled?: boolean;
  /** Key entries, fixed per panel so the key never re-wraps while rows arrive. */
  legend?: Array<"landed" | "held" | "skipped">;
}) {
  const pending = share.pending ?? 0;
  const total = share.landed + share.held + share.skipped + pending;
  return (
    <div className={`mk-bars${settled ? " is-settled" : ""}`} role="img" aria-label="Rows read equal rows landed, plus rows held, plus rows skipped by a rule you set">
      <div className="mk-bars-row">
        <span className="mk-bars-label">Read</span>
        <span className="mk-bars-track">
          <span className="mk-bars-seg is-read" style={{ flexGrow: total }} />
        </span>
      </div>
      <div className="mk-bars-row">
        <span className="mk-bars-label">Accounted</span>
        <span className="mk-bars-track">
          <span className="mk-bars-seg is-landed" style={{ flexGrow: share.landed }} />
          {share.held ? <span className="mk-bars-seg is-held" style={{ flexGrow: share.held }} /> : null}
          {share.skipped ? <span className="mk-bars-seg is-skipped" style={{ flexGrow: share.skipped }} /> : null}
          {pending ? <span className="mk-bars-seg is-pending" style={{ flexGrow: pending }} /> : null}
        </span>
      </div>
      <p className="mk-bars-key" aria-hidden>
        {legend.includes("landed") ? <span><i className="is-landed" />Landed</span> : null}
        {legend.includes("held") ? <span><i className="is-held" />Held with a reason</span> : null}
        {legend.includes("skipped") ? <span><i className="is-skipped" />Skipped by a rule you set</span> : null}
      </p>
    </div>
  );
}

type LedgerRow = { key: string; region: string; to: "landed" | "held" | "skipped"; note?: string };

const LEDGER_ROWS: LedgerRow[] = [
  { key: "48201", region: "EMEA", to: "landed" },
  { key: "48202", region: "APAC", to: "landed" },
  { key: "48203", region: "AMER", to: "landed" },
  { key: "48204", region: "EMEA", to: "held", note: "status is NULL; NOT NULL in the destination" },
  { key: "48205", region: "LATAM", to: "landed" },
  { key: "48206", region: "TEST", to: "skipped", note: "region = 'TEST', excluded by the run's filter" },
  { key: "48207", region: "APAC", to: "landed" },
  { key: "48208", region: "AMER", to: "landed" },
];

/** The one example run every panel shows: read = landed + held + skipped. */
export const EXAMPLE_RUN = {
  rows: LEDGER_ROWS,
  read: LEDGER_ROWS.length,
  landed: LEDGER_ROWS.filter((r) => r.to === "landed").length,
  held: LEDGER_ROWS.filter((r) => r.to === "held"),
  skipped: LEDGER_ROWS.filter((r) => r.to === "skipped"),
};

const runFmt = new Intl.NumberFormat("en-US");

/**
 * The example run's totals, the same run the home statement shows. The
 * ledger rows above are a sample of that run, not its size.
 */
export const RUN_TOTALS = {
  read: EXAMPLE_READ,
  landed: EXAMPLE_STATEMENT.landed,
  held: EXAMPLE_STATEMENT.held,
  skipped: EXAMPLE_STATEMENT.skipped,
};

/** "48,213,906 read = 48,213,871 landed + 33 held + 2 skipped". */
export function exampleIdentity(): string {
  const r = RUN_TOTALS;
  return `${runFmt.format(r.read)} read = ${runFmt.format(r.landed)} landed + ${r.held} held + ${r.skipped} skipped`;
}

const EXAMPLE_META = "Example rows";
const EXAMPLE_SHARE: Share = { landed: EXAMPLE_RUN.landed, held: EXAMPLE_RUN.held.length, skipped: EXAMPLE_RUN.skipped.length };

/**
 * The signature visual: rows leave the source and land, are held with a
 * reason, or are skipped by a rule you set; then the two bars close.
 * Plays once on load, replays on request, and shows the settled state under
 * reduced motion.
 */
export function ReconciliationLedger() {
  const titleId = useId();
  const total = LEDGER_ROWS.length;
  const [step, setStep] = useState(total);
  const [run, setRun] = useState(0);

  useEffect(() => {
    if (prefersReducedMotion()) {
      setStep(total);
      return;
    }
    setStep(0);
    let n = 0;
    let id = 0;
    const start = window.setTimeout(() => {
      id = window.setInterval(() => {
        n += 1;
        setStep(n);
        if (n >= total) window.clearInterval(id);
      }, 360);
    }, 500);
    return () => {
      window.clearTimeout(start);
      window.clearInterval(id);
    };
  }, [run, total]);

  const moved = LEDGER_ROWS.slice(0, step);
  const waiting = LEDGER_ROWS.slice(step);
  const lane = (to: LedgerRow["to"]) => moved.filter((r) => r.to === to);
  const done = step >= total;
  const share: Share = {
    landed: lane("landed").length,
    held: lane("held").length,
    skipped: lane("skipped").length,
    pending: waiting.length,
  };

  return (
    <figure className="mk-ledger2" aria-labelledby={titleId}>
      <div className="mk-ledger2-head">
        <div>
          <p id={titleId} className="mk-ledger2-title">Trust Report, reconciliation</p>
          <p className="mk-ledger2-meta">
            <code>{RECORDED_ROUTE.source}</code> to <code>{RECORDED_ROUTE.dest}</code>
          </p>
        </div>
        <span className="mk-ledger2-example">Example rows</span>
      </div>

      <div className="mk-ledger2-body">
        <div className="mk-ledger2-col mk-ledger2-source">
          <p className="mk-ledger2-colhead">Read from {RECORDED_ROUTE.sourceEngine}</p>
          <ul aria-label="Rows read from the source">
            {LEDGER_ROWS.map((r, i) => (
              <li key={r.key} className={`mk-ledger2-row mk-ledger2-src${i < step ? ` is-done is-${r.to}` : ""}`}>
                <code>{r.key}</code>
                <span>{r.region}</span>
                <StatusGlyph kind={i < step ? r.to : "pending"} />
                <span className="mk-sr-only">{i < step ? (r.to === "landed" ? "landed" : r.to === "held" ? "held" : "skipped") : "waiting"}</span>
              </li>
            ))}
          </ul>
        </div>

        <div className="mk-ledger2-flow" aria-hidden>
          <span />
        </div>

        <div className="mk-ledger2-col mk-ledger2-lanes">
          <div className="mk-ledger2-lane is-landed">
            <p className="mk-ledger2-colhead"><StatusGlyph kind="landed" />Landed in {RECORDED_ROUTE.destEngine}</p>
            <ul className="mk-ledger2-chips">
              {lane("landed").map((r) => (
                <li key={r.key} className="mk-ledger2-chip">
                  <code>{r.key}</code>
                </li>
              ))}
              {lane("landed").length === 0 ? <li className="mk-ledger2-ghost" aria-hidden /> : null}
            </ul>
          </div>
          <div className="mk-ledger2-lane is-held">
            <p className="mk-ledger2-colhead"><StatusGlyph kind="held" />Held in quarantine, with the reason</p>
            <ul>
              {lane("held").map((r) => (
                <li key={r.key} className="mk-ledger2-row">
                  <code>{r.key}</code>
                  <span>{r.note}</span>
                </li>
              ))}
              {lane("held").length === 0 ? <li className="mk-ledger2-ghost" aria-hidden /> : null}
            </ul>
          </div>
          <div className="mk-ledger2-lane is-skipped">
            <p className="mk-ledger2-colhead"><StatusGlyph kind="skipped" />Skipped by a rule you set</p>
            <ul>
              {lane("skipped").map((r) => (
                <li key={r.key} className="mk-ledger2-row">
                  <code>{r.key}</code>
                  <span>{r.note}</span>
                </li>
              ))}
              {lane("skipped").length === 0 ? <li className="mk-ledger2-ghost" aria-hidden /> : null}
            </ul>
          </div>
        </div>
      </div>

      <div className="mk-ledger2-foot">
        <IdentityBars share={share} settled={done} />
        <div className="mk-ledger2-verdict">
          <span className={`mk-status${done ? " mk-status--ok" : ""}`} aria-live="polite">
            <StatusGlyph kind={done ? "landed" : "pending"} />
            {done ? "Balanced" : "Reconciling"}
          </span>
          <p className="mk-ledger2-equation" aria-hidden={done ? undefined : true}>
            <span>{moved.length} read</span> = <span>{share.landed} landed</span> + <span>{share.held} held</span> +{" "}
            <span>{share.skipped} skipped</span>
          </p>
          <button
            type="button"
            className="mk-ledger2-replay"
            onClick={() => setRun((r) => r + 1)}
            disabled={!done}
          >
            Replay
          </button>
        </div>
      </div>
    </figure>
  );
}

/** `--i` for a staggered entrance, as a typed style object. */
function stagger(i: number): CSSProperties {
  return { ["--i" as string]: i } as CSSProperties;
}

/**
 * A panel frame that assembles once: its rows settle in a short stagger the
 * first time it scrolls into view. Under reduced motion it renders settled.
 */
function Panel({
  title,
  meta,
  badge,
  className = "",
  labelledBy,
  children,
}: {
  title: string;
  meta?: string;
  badge?: ReactNode;
  className?: string;
  labelledBy?: string;
  children: ReactNode;
}) {
  const { ref, inView } = useInViewOnce<HTMLElement>();
  const ownId = useId();
  return (
    <figure
      ref={ref}
      className={`mk-panel mk-assemble${inView ? " is-in" : ""} ${className}`.trim()}
      aria-labelledby={labelledBy ?? ownId}
    >
      <PanelHead title={title} meta={meta} badge={badge} titleId={labelledBy ? undefined : ownId} />
      {children}
    </figure>
  );
}

function PanelHead({ title, meta, badge, titleId }: { title: string; meta?: string; badge?: ReactNode; titleId?: string }) {
  return (
    <div className="mk-panel-head">
      <div>
        <p className="mk-panel-title" id={titleId}>{title}</p>
        {meta ? <p className="mk-panel-meta">{meta}</p> : null}
      </div>
      {badge}
    </div>
  );
}

/**
 * The Trust Report comparison, scale-free: what was compared and the result,
 * with the column census settling one column at a time once in view.
 */
export function VerifyPanel({ animate = true }: { animate?: boolean }) {
  const total = RECORDED_MAPPING.length;
  const { ref, inView } = useInViewOnce<HTMLElement>();
  const [checked, setChecked] = useState(animate ? 0 : total);
  const titleId = useId();

  useEffect(() => {
    if (!animate || !inView) return;
    if (prefersReducedMotion()) {
      setChecked(total);
      return;
    }
    let n = 0;
    const id = window.setInterval(() => {
      n += 1;
      setChecked(n);
      if (n >= total) window.clearInterval(id);
    }, 90);
    return () => window.clearInterval(id);
  }, [animate, inView, total]);

  const done = checked >= total;
  return (
    <figure ref={ref} className={`mk-panel mk-verify mk-assemble${inView ? " is-in" : ""}`} aria-labelledby={titleId}>
      <PanelHead
        title="Trust Report"
        titleId={titleId}
        meta={EXAMPLE_META}
        badge={
          <span className="mk-status mk-status--held">
            <StatusGlyph kind="held" />
            Completed with quarantine
          </span>
        }
      />
      <RouteLine />
      <div className="mk-verify-bars">
        <IdentityBars share={EXAMPLE_SHARE} />
      </div>
      <table className="mk-panel-table mk-verify-table">
        <caption>Comparison after the write</caption>
        <thead>
          <tr>
            <th scope="col">Check</th>
            <th scope="col">What was compared</th>
            <th scope="col">Result</th>
          </tr>
        </thead>
        <tbody>
          <tr style={stagger(0)}>
            <th scope="row">Row count</th>
            <td>The landed rows, counted by {RECORDED_ROUTE.destEngine}</td>
            <td><StatusGlyph kind="landed" /> Equal</td>
          </tr>
          <tr style={stagger(1)}>
            <th scope="row">Checksum</th>
            <td>Every landed row, order-independent</td>
            <td><StatusGlyph kind="landed" /> Match</td>
          </tr>
          <tr style={stagger(2)}>
            <th scope="row">Column census</th>
            <td>
              <span className="mk-sr-only">{done ? "Every column compared" : "Comparing columns"}</span>
              <span className="mk-census" aria-hidden>
                {RECORDED_MAPPING.map((m, i) => (
                  <span key={m.col} className={i < checked ? "is-checked" : undefined} title={m.col} />
                ))}
              </span>
            </td>
            <td>
              <StatusGlyph kind={done ? "landed" : "pending"} /> {done ? "Every column" : "Comparing"}
            </td>
          </tr>
          <tr style={stagger(3)}>
            <th scope="row">Not landed</th>
            <td>
              <code>{EXAMPLE_RUN.held[0].key}</code> held: {EXAMPLE_RUN.held[0].note}. <code>{EXAMPLE_RUN.skipped[0].key}</code>{" "}
              skipped by the run&apos;s filter.
            </td>
            <td><StatusGlyph kind="held" /> Accounted for</td>
          </tr>
        </tbody>
      </table>
      <dl className="mk-verify-verdict mk-verify-verdict--two">
        <div>
          <dt>Verdict</dt>
          <dd>
            <span className="mk-verdict mk-verdict--explained">Explained</span>
            <span>
              {RUN_TOTALS.held} rows differ and none is unexplained: each is held in quarantine, such as{" "}
              <code>{EXAMPLE_RUN.held[0].key}</code>. The landed rows match in full.
            </span>
          </dd>
        </div>
        <div>
          <dt>Accounting</dt>
          <dd>
            <span className="mk-status mk-status--ok"><StatusGlyph kind="landed" />Balanced</span>
            <span>{exampleIdentity()}</span>
          </dd>
        </div>
      </dl>
    </figure>
  );
}

/** Map step: the plan a person approves. */
export function PlanPanel() {
  return (
    <Panel
      title="Map"
      meta="Example, from a recorded run. Every column mapped and ready to approve"
      badge={<span className="mk-status">New table</span>}
    >
      <table className="mk-panel-table mk-plan-table">
        <caption>Column mapping</caption>
        <thead>
          <tr>
            <th scope="col">Source column</th>
            <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
            <th scope="col">{RECORDED_ROUTE.destEngine}</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {RECORDED_MAPPING.map((m, i) => (
            <tr key={m.col} style={stagger(i)}>
              <th scope="row"><code>{m.col}</code></th>
              <td><code>{m.from}</code></td>
              <td>
                <code>{m.to}</code>
                {m.transform ? <span className="mk-panel-note">{m.transform}</span> : null}
              </td>
              <td><span className="mk-dot mk-dot--ok" aria-hidden />Safe</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mk-panel-foot mk-plan-more">
        <span>
          <StatusGlyph kind="landed" /> {RECORDED_MAPPING.length} columns, all mapped and safe
        </span>
      </p>
    </Panel>
  );
}

/**
 * Migrations: a map between two schemas that do not match name for name.
 * Renamed pairs map by meaning; an ambiguous pair waits for a person; a
 * narrowing cannot be approved without a signed risk record.
 */
export function MigrationMapPanel() {
  const rows: { from: string; to: string; state: "safe" | "review" | "risk"; note: string }[] = [
    { from: "order_id bigint", to: "ORDER_ID NUMBER(19,0)", state: "safe", note: "Same name, wider type" },
    { from: "cust_ref integer", to: "CUSTOMER_ID NUMBER(10,0)", state: "safe", note: "Renamed, matched by role" },
    { from: "created timestamptz", to: "CREATED_AT TIMESTAMP_TZ", state: "safe", note: "Renamed, matched by role" },
    { from: "amt numeric(12,2)", to: "AMOUNT_USD or AMOUNT_LOCAL", state: "review", note: "Two candidates, waiting for a person" },
    { from: "note varchar(2000)", to: "NOTE VARCHAR(500)", state: "risk", note: "Narrowing: needs a signed risk record" },
  ];
  const label = { safe: "Safe", review: "Review", risk: "Risk record" } as const;
  const glyph = { safe: "landed", review: "pending", risk: "held" } as const;
  return (
    <Panel
      title="Map"
      meta="Example. Two schemas that do not match name for name"
      badge={
        <span className="mk-status mk-status--held">
          <StatusGlyph kind="pending" />2 need a decision
        </span>
      }
    >
      <table className="mk-panel-table mk-migrate-table">
        <caption>Column mapping between two schemas</caption>
        <thead>
          <tr>
            <th scope="col">Source</th>
            <th scope="col">Destination</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.from} style={stagger(i)} className={`is-${r.state}`}>
              <th scope="row">
                <code>{r.from}</code>
              </th>
              <td>
                <code>{r.to}</code>
                <span className="mk-panel-note">{r.note}</span>
              </td>
              <td>
                <span className={`mk-map-state is-${r.state}`}>
                  <StatusGlyph kind={glyph[r.state]} />
                  {label[r.state]}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mk-panel-foot">
        <span>Nothing is pinned until someone accepts it</span>
      </p>
    </Panel>
  );
}

/**
 * Validate, in plain language: four groups that resolve once in view, each
 * opening to the checks inside it.
 */
export function GatesPanel() {
  const { ref, inView } = useInViewOnce<HTMLElement>();
  const total = RECORDED_CHECK_GROUPS.length;
  const [resolved, setResolved] = useState(0);
  const [open, setOpen] = useState<number | null>(null);
  const titleId = useId();
  const baseId = useId();

  useEffect(() => {
    if (!inView) return;
    if (prefersReducedMotion()) {
      setResolved(total);
      return;
    }
    let n = 0;
    const id = window.setInterval(() => {
      n += 1;
      setResolved(n);
      if (n >= total) window.clearInterval(id);
    }, 240);
    return () => window.clearInterval(id);
  }, [inView, total]);

  const done = resolved >= total;
  const all = RECORDED_CHECK_GROUPS.flatMap((g) => g.checks);
  const passed = all.filter((c) => c.state === "pass").length;
  return (
    <figure ref={ref} className="mk-panel mk-checkgroups" aria-labelledby={titleId}>
      <PanelHead
        title="Validate"
        titleId={titleId}
        meta="Example, from a recorded run"
        badge={
          <span className={`mk-status${done ? " mk-status--ok" : ""}`} aria-live="polite">
            <StatusGlyph kind={done ? "landed" : "pending"} />
            {done ? "Approved" : "Validating"}
          </span>
        }
      />
      <ul className="mk-groups">
        {RECORDED_CHECK_GROUPS.map((g, i) => {
          const ready = i < resolved;
          const pass = g.checks.filter((c) => c.state === "pass").length;
          const na = g.checks.length - pass;
          const isOpen = open === i;
          const panelId = `${baseId}-g${i}`;
          return (
            <li key={g.name} className={ready ? "is-done" : "is-pending"}>
              <button type="button" aria-expanded={isOpen} aria-controls={panelId} onClick={() => setOpen(isOpen ? null : i)}>
                <StatusGlyph kind={ready ? "landed" : "pending"} />
                <span className="mk-groups-name">{g.name}</span>
                <span className="mk-groups-count">
                  {ready ? `${pass} passed${na ? `, ${na} not applicable` : ""}` : "Checking"}
                </span>
                <i aria-hidden />
              </button>
              <ul id={panelId} className="mk-groups-detail" hidden={!isOpen}>
                {g.checks.map((c) => (
                  <li key={c.name} className={`is-${c.state}`}>
                    <strong>{c.name}</strong>
                    <span>{c.does}</span>
                    <em>{c.state === "pass" ? "Passed" : "Not applicable"}</em>
                  </li>
                ))}
              </ul>
            </li>
          );
        })}
      </ul>
      <p className="mk-panel-foot">
        <span>
          {passed} passed, {all.length - passed} not applicable to this route
        </span>
        <span>{done ? "Execute unlocked" : "Execute locked"}</span>
      </p>
    </figure>
  );
}

export function RunPanel() {
  const held = EXAMPLE_RUN.held[0];
  const skipped = EXAMPLE_RUN.skipped[0];
  const rows: [string, ReactNode][] = [
    ["Mode", "Full append, batched, bound parameters"],
    ["Rows read", runFmt.format(RUN_TOTALS.read)],
    ["Landed", `${runFmt.format(RUN_TOTALS.landed)}, counted by ${RECORDED_ROUTE.destEngine} before and after the write`],
    ["Held in quarantine", <>{RUN_TOTALS.held} rows, such as <code>{held.key}</code>: {held.note}</>],
    ["Skipped by a rule", <>{RUN_TOTALS.skipped} rows, such as <code>{skipped.key}</code>: {skipped.note}</>],
  ];
  return (
    <Panel
      title="Run"
      meta={EXAMPLE_META}
      badge={
        <span className="mk-status mk-status--held">
          <StatusGlyph kind="held" />
          Completed with quarantine
        </span>
      }
    >
      <div className="mk-verify-bars">
        <IdentityBars share={EXAMPLE_SHARE} />
      </div>
      <dl className="mk-panel-dl">
        {rows.map(([k, v], i) => (
          <div key={k} style={stagger(i)}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <p className="mk-panel-foot">
        <span><StatusGlyph kind="landed" /> Balanced: {exampleIdentity()}</span>
      </p>
    </Panel>
  );
}

/** Evidence: what the job hands to the people who sign it. */
export function EvidencePanel() {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <Panel title="Sign-off" meta={`${EXAMPLE_META}, signed by role`}>
      <dl className="mk-panel-dl">
        {[
          ["Verdict", "Explained: the one row that differs is held in quarantine"],
          ["Accounting", `Balanced, ${exampleIdentity()}`],
          ["Comparison on record", "Full checksum of the landed rows"],
          ["Proof pack", "Signed, verifiable through the API"],
          ["Audit log", "Tamper-evident, and exportable"],
        ].map(([k, v], i) => (
          <div key={k} style={stagger(i)}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <ul className="mk-signoff">
        {roles.map((r, i) => (
          <li key={r} style={stagger(i + 3)}>
            <span>{r}</span>
            <em>Awaiting signature</em>
          </li>
        ))}
      </ul>
    </Panel>
  );
}

type Held = { key: string; col: string; value: string; reason: string };

const EXAMPLE_HELD: Held[] = [
  { key: "48204", col: "status", value: "NULL", reason: "status is NULL; NOT NULL in the destination" },
];

type ReplayState = "held" | "replaying" | "balanced";

/**
 * A run that finished with quarantine. Replay answers the visitor's click and
 * walks the job through its states: held, replaying, balanced.
 */
export function QuarantinePanel() {
  const [state, setState] = useState<ReplayState>("held");
  const titleId = useId();
  const { ref, inView } = useInViewOnce<HTMLElement>();

  const replay = () => {
    if (state !== "held") {
      setState("held");
      return;
    }
    if (prefersReducedMotion()) {
      setState("balanced");
      return;
    }
    setState("replaying");
    window.setTimeout(() => setState("balanced"), 900);
  };

  const badge =
    state === "held" ? (
      <span className="mk-status mk-status--held"><StatusGlyph kind="held" />Completed with quarantine</span>
    ) : state === "replaying" ? (
      <span className="mk-status">Replaying</span>
    ) : (
      <span className="mk-status mk-status--ok">Balanced</span>
    );

  return (
    <figure ref={ref} className={`mk-panel mk-quarantine mk-assemble${inView ? " is-in" : ""} is-${state}`} aria-labelledby={titleId}>
      <PanelHead title="Quarantine" titleId={titleId} meta={EXAMPLE_META} badge={<span aria-live="polite">{badge}</span>} />
      <div className="mk-verify-bars">
        <IdentityBars
          share={state === "balanced" ? { ...EXAMPLE_SHARE, landed: EXAMPLE_SHARE.landed + EXAMPLE_SHARE.held, held: 0 } : EXAMPLE_SHARE}
        />
      </div>
      <table className="mk-panel-table mk-q-table">
        <caption>Rows held on the job, with the reason for each</caption>
        <thead>
          <tr>
            <th scope="col">order_id</th>
            <th scope="col">Column</th>
            <th scope="col">Value</th>
            <th scope="col">Reason</th>
          </tr>
        </thead>
        <tbody>
          {EXAMPLE_HELD.map((h, i) => (
            <tr key={h.key} style={stagger(i)} className={state === "held" ? undefined : "is-replayed"}>
              <th scope="row"><code>{h.key}</code></th>
              <td><code>{h.col}</code></td>
              <td><code className="mk-q-value">{h.value}</code></td>
              <td>{state === "held" ? h.reason : state === "replaying" ? "Writing again" : "Replayed into the same table"}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="mk-panel-actions">
        <button
          type="button"
          className="mk-btn mk-btn--secondary mk-btn--sm"
          onClick={replay}
          disabled={state === "replaying"}
        >
          {state === "held" ? "Replay the held row" : state === "replaying" ? "Replaying" : "Show the held row again"}
        </button>
        <p>
          {state === "held"
            ? "Fix the cause at the source, then replay."
            : state === "replaying"
              ? "Same writer, same table."
              : `${runFmt.format(RUN_TOTALS.read)} read = ${runFmt.format(RUN_TOTALS.landed + RUN_TOTALS.held)} landed + ${RUN_TOTALS.skipped} skipped. The status was fixed at the source first.`}
        </p>
      </div>
    </figure>
  );
}

type Tick = { at: string; secs: string; state: "ok" | "paused" | "review" };

const EXAMPLE_TICKS: Tick[] = [
  { at: "08:20", secs: "53s", state: "ok" },
  { at: "08:22", secs: "53s", state: "ok" },
  { at: "08:24", secs: "54s", state: "ok" },
  { at: "08:26", secs: "", state: "paused" },
  { at: "12:05", secs: "", state: "review" },
  { at: "12:07", secs: "1m 3s", state: "ok" },
  { at: "12:09", secs: "52s", state: "ok" },
];

/** A schedule's run history, including a drift pause. */
export function ScheduleTimeline() {
  return (
    <Panel title="Orders feed, every 2 minutes" meta="Example data. Incremental, deduplicated on order_id">
      <ol className="mk-ticks" aria-label="Run history">
        {EXAMPLE_TICKS.map((t, i) => (
          <li key={t.at} className={`is-${t.state}`} style={stagger(i)}>
            <time>{t.at}</time>
            <span className="mk-tick-mark" aria-hidden />
            {t.state === "ok" ? (
              <>
                <span className="mk-tick-what">Run completed, destination read back equal</span>
                <span className="mk-tick-secs">{t.secs}</span>
              </>
            ) : t.state === "paused" ? (
              <span className="mk-tick-what">
                Paused: column <code>coupon_code</code> appeared at the source
              </span>
            ) : (
              <span className="mk-tick-what">New column reviewed in Map, schedule resumed</span>
            )}
          </li>
        ))}
      </ol>
    </Panel>
  );
}

/** Rows per region on each side of the example run, derived from its rows. */
function regionCounts() {
  const regions = [...new Set(EXAMPLE_RUN.rows.map((r) => r.region))].sort();
  return regions.map((region) => {
    const all = EXAMPLE_RUN.rows.filter((r) => r.region === region);
    const landed = all.filter((r) => r.to === "landed");
    return { region, source: all.length, dest: landed.length, missing: all.filter((r) => r.to !== "landed") };
  });
}

const QUERY_SQL = (table: string) => `SELECT region, COUNT(*) AS orders\nFROM ${table}\nGROUP BY region\nORDER BY region;`;

/**
 * Query: one read-only statement on the copy. "Try a write" shows the
 * refusal, before anything reaches the database.
 */
export function QueryPanel() {
  const [tryWrite, setTryWrite] = useState(false);
  const rows = regionCounts().filter((r) => r.dest > 0);
  return (
    <Panel
      title="Query"
      meta={`Example rows. Read-only, on Warehouse ${RECORDED_ROUTE.destEngine} (demo)`}
      badge={<span className="mk-status">Read-only</span>}
      className="mk-query"
    >
      <pre className="mk-code" aria-label="SQL statement" tabIndex={0}>
        <code>{tryWrite ? `DELETE FROM ${RECORDED_ROUTE.dest}\nWHERE region = 'APAC';` : QUERY_SQL(RECORDED_ROUTE.dest)}</code>
      </pre>
      {tryWrite ? (
        <p className="mk-query-refused" role="status">
          <span className="mk-status mk-status--held"><StatusGlyph kind="held" />Refused</span>
          Only read and metadata statements run here. The statement never reached the database.
        </p>
      ) : (
        <table className="mk-panel-table">
          <caption>Orders by region in the copy</caption>
          <thead>
            <tr>
              <th scope="col">region</th>
              <th scope="col">orders</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={r.region} style={stagger(i)}>
                <th scope="row"><code>{r.region}</code></th>
                <td className="mk-num">{r.dest}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div className="mk-panel-actions">
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => setTryWrite((w) => !w)} aria-pressed={tryWrite}>
          {tryWrite ? "Back to the read" : "Try a write"}
        </button>
        <p>{tryWrite ? "Writes are refused before they run." : "Under your role, on a saved connection."}</p>
      </div>
    </Panel>
  );
}

/**
 * Query, the page's own module: the same statement asked of the source and
 * the copy, side by side, with every difference traced to the run.
 */
export function QueryDiffPanel() {
  const rows = regionCounts();
  return (
    <Panel title="Same question, both sides" meta={EXAMPLE_META} className="mk-qdiff">
      <div className="mk-qdiff-sql">
        <pre className="mk-code" tabIndex={0} aria-label={`SQL on ${RECORDED_ROUTE.sourceEngine}`}>
          <code>{QUERY_SQL(RECORDED_ROUTE.source)}</code>
        </pre>
        <pre className="mk-code" tabIndex={0} aria-label={`SQL on ${RECORDED_ROUTE.destEngine}`}>
          <code>{QUERY_SQL(RECORDED_ROUTE.dest)}</code>
        </pre>
      </div>
      <table className="mk-panel-table mk-qdiff-table">
        <caption>Orders by region on each side</caption>
        <thead>
          <tr>
            <th scope="col">region</th>
            <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
            <th scope="col">{RECORDED_ROUTE.destEngine}</th>
            <th scope="col">Difference</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.region} style={stagger(i)} className={r.missing.length ? "is-diff" : undefined}>
              <th scope="row"><code>{r.region}</code></th>
              <td className="mk-num">{r.source}</td>
              <td className="mk-num">{r.dest}</td>
              <td>
                {r.missing.length ? (
                  r.missing.map((m) => (
                    <span key={m.key} className="mk-qdiff-why">
                      <StatusGlyph kind={m.to} />
                      <code>{m.key}</code> {m.to === "held" ? "held in quarantine" : "skipped by the run's filter"}
                    </span>
                  ))
                ) : (
                  <span className="mk-yes"><StatusGlyph kind="landed" />Equal</span>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mk-panel-foot">
        <span>Every difference is a row the Trust Report already names: {exampleIdentity()}</span>
      </p>
    </Panel>
  );
}

/**
 * Pilot: a question answered by running a tool, with the SQL under the
 * answer, and a change staged for the person to confirm.
 */
export function PilotPanel() {
  const [confirmed, setConfirmed] = useState(false);
  const rows = regionCounts().filter((r) => r.dest > 0);
  return (
    <Panel title="Pilot" meta="Example conversation, on the example run's tables" className="mk-pilot">
      <ol className="mk-chat" aria-label="Conversation">
        <li className="mk-chat-you" style={stagger(0)}>
          <span className="mk-sr-only">You: </span>How many orders per region are in the Snowflake copy?
        </li>
        <li className="mk-chat-pilot" style={stagger(1)}>
          <span className="mk-sr-only">Pilot: </span>
          <p>
            {rows.map((r, i) => (
              <span key={r.region}>
                {r.region} {r.dest}
                {i < rows.length - 1 ? ", " : "."}
              </span>
            ))}
          </p>
          <pre className="mk-code mk-chat-sql" tabIndex={0} aria-label="The SQL Pilot ran">
            <code>{QUERY_SQL(RECORDED_ROUTE.dest)}</code>
          </pre>
          <p className="mk-chat-meta">Exact aggregate over the whole table, on Warehouse {RECORDED_ROUTE.destEngine} (demo)</p>
        </li>
        <li className="mk-chat-you" style={stagger(2)}>
          <span className="mk-sr-only">You: </span>Keep it in sync every hour.
        </li>
        <li className="mk-chat-pilot" style={stagger(3)}>
          <span className="mk-sr-only">Pilot: </span>
          <div className="mk-confirm">
            <p>
              <strong>Create schedule</strong> Orders Postgres to Warehouse Snowflake, hourly, incremental on{" "}
              <code>ordered_at</code>
            </p>
            <p className="mk-chat-meta" aria-live="polite">
              {confirmed ? "Confirmed. The schedule runs through the same checks as Studio." : "Staged. Nothing happens until you confirm."}
            </p>
            <div className="mk-confirm-actions">
              <button
                type="button"
                className="mk-btn mk-btn--primary mk-btn--sm"
                onClick={() => setConfirmed(true)}
                disabled={confirmed}
              >
                {confirmed ? "Confirmed" : "Confirm"}
              </button>
              {confirmed ? (
                <button type="button" className="mk-btn mk-btn--quiet mk-btn--sm" onClick={() => setConfirmed(false)}>
                  Reset example
                </button>
              ) : null}
            </div>
          </div>
        </li>
      </ol>
    </Panel>
  );
}

/** Pilot, the page's own module: one answer, taken apart. */
export function PilotAnswerPanel() {
  const rows = regionCounts().filter((r) => r.dest > 0);
  const parts: [string, ReactNode][] = [
    ["Question", "How many orders per region are in the Snowflake copy?"],
    ["Answer", rows.map((r) => `${r.region} ${r.dest}`).join(", ")],
    [
      "Statement it ran",
      <pre key="sql" className="mk-code mk-anatomy-sql" tabIndex={0} aria-label="SQL statement">
        <code>{QUERY_SQL(RECORDED_ROUTE.dest)}</code>
      </pre>,
    ],
    ["Where it ran", `Warehouse ${RECORDED_ROUTE.destEngine} (demo), read-only, under your workspace role`],
    ["What kind of number", "An exact aggregate over the whole table, run on the server, not a sample or a recollection"],
    ["What happens next", "Nothing changes. A follow-up that would change data is staged for you to confirm"],
  ];
  return (
    <Panel title="An answer, taken apart" meta={EXAMPLE_META} className="mk-anatomy">
      <dl className="mk-panel-dl mk-anatomy-dl">
        {parts.map(([k, v], i) => (
          <div key={k} style={stagger(i)}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
    </Panel>
  );
}

/** MCP: an agent's tool calls under a role-scoped key, with a staged action. */
export function McpPanel() {
  const calls: { tool: string; perm: string; result: string; tone: "ok" | "staged" | "done" }[] = [
    { tool: "plan_transfer", perm: "job.plan", result: "Plan ready. Mapping attached, no blocking findings.", tone: "ok" },
    { tool: "start_transfer", perm: "job.run", result: "Staged for confirmation. ack_id returned; nothing has run.", tone: "staged" },
    { tool: "confirm_action", perm: "ack_id, used once", result: "Transfer queued. Same checks and read-back as Studio.", tone: "done" },
  ];
  return (
    <Panel title="MCP session" meta="Example. Agent in Cursor, API key with the Editor role" className="mk-mcp">
      <dl className="mk-mcp-key">
        <div>
          <dt>Credentials</dt>
          <dd>Secret reference <code>conn_orders_pg</code>; the agent never sees a password</dd>
        </div>
        <div>
          <dt>Scope</dt>
          <dd>Capped at the grants of the person who created the key</dd>
        </div>
      </dl>
      <ol className="mk-mcp-calls" aria-label="Tool calls">
        {calls.map((c, i) => (
          <li key={c.tool} className={`is-${c.tone}`} style={stagger(i)}>
            <code>{c.tool}</code>
            <span className="mk-mcp-perm">{c.perm}</span>
            <p>{c.result}</p>
          </li>
        ))}
      </ol>
    </Panel>
  );
}

/** Enterprise controls on one workspace. */
export function ControlsPanel() {
  const rows: [string, string][] = [
    ["Sign-in", "OIDC or SAML, routed by email domain"],
    ["Provisioning", "SCIM 2.0, identity-provider groups mapped to roles"],
    ["Roles", ROLES.map((r) => r.name).join(", ")],
    ["API keys", "Role and scopes capped at the creator's grants"],
    ["Connector secrets", "Encrypted at rest, wrapped with your AWS KMS key"],
    ["Data region", "Recorded on the workspace; other regions refused before a run"],
    ["Audit", "Tamper-evident: each entry signed and linked to the one before"],
  ];
  return (
    <Panel title="Workspace controls" meta="Example workspace settings" badge={<span className="mk-status mk-status--ok">Enforced on every run</span>}>
      <dl className="mk-panel-dl">
        {rows.map(([k, v], i) => (
          <div key={k} style={stagger(i)}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
    </Panel>
  );
}

/** Security: audit entries, each linked to the one before by its hash. */
export function AuditChainPanel() {
  const entries: { what: string; who: string; hash: string; prev: string }[] = [
    { what: "Plan approved", who: "data.owner@example.com", hash: "9c02e1", prev: "4b7a30" },
    { what: "Transfer started", who: "API key, Editor role", hash: "a41f8d", prev: "9c02e1" },
    { what: "Rows held in quarantine", who: "Transfer engine", hash: "0de76b", prev: "a41f8d" },
    { what: "Trust Report signed", who: "compliance@example.com", hash: "e5c913", prev: "0de76b" },
  ];
  return (
    <Panel title="Audit log" meta="Example entries" badge={<span className="mk-status mk-status--ok">Chain verified</span>}>
      <ol className="mk-chain" aria-label="Audit entries">
        {entries.map((e, i) => (
          <li key={e.hash} style={stagger(i)}>
            <span className="mk-chain-link" aria-hidden />
            <div>
              <p className="mk-chain-what">{e.what}</p>
              <p className="mk-chain-who">{e.who}</p>
            </div>
            <p className="mk-chain-hash">
              <code>{e.hash}</code>
              <span>links to <code>{e.prev}</code></span>
            </p>
          </li>
        ))}
      </ol>
      <p className="mk-panel-foot">
        <span>Every entry is signed with the chain's own key, named by a key id</span>
      </p>
    </Panel>
  );
}

/** The four workspace roles, as docs/operate/security.md defines them. */
export const ROLES: { name: string; does: string }[] = [
  { name: "Viewer", does: "Reads jobs, connectors, schedules, and the audit log. Can ask Pilot and run read-only queries." },
  { name: "Operator", does: "Runs and repairs pipelines but does not design them." },
  { name: "Editor", does: "Plans transfers, saves connectors, and manages schedules." },
  { name: "Admin", does: "Deletes connectors, authorizes schedules, manages the workspace, and invites members." },
];

/** Connectors: every family, each driver marked proven in CI or implemented. */
export function CoveragePanel() {
  return (
    <Panel title="Connector coverage" meta={`${PROVEN_DRIVERS} of ${CATALOG_TOTAL} drivers proven in CI`} className="mk-coverage">
      <ul className="mk-coverage-list">
        {MARKETING_STACK.map((f, i) => (
          <li key={f.family} style={stagger(i)}>
            <span className="mk-coverage-name">{f.family}</span>
            <span className="mk-coverage-dots" aria-hidden>
              {ALL_CONNECTORS.filter((c) => c.family === f.family).map((item) => (
                <i
                  key={item.driver}
                  className={item.proven ? (item.emulated ? "is-proven is-emulated" : "is-proven") : undefined}
                  title={`${item.name}: ${item.proven ? provenBadge(item.emulated) : "Implemented"}`}
                />
              ))}
            </span>
            <span className="mk-coverage-count">
              {f.provenCount} of {f.total}
              <span className="mk-sr-only">
                {" "}
                proven in CI
                {ALL_CONNECTORS.some((c) => c.family === f.family && c.emulated)
                  ? `, ${ALL_CONNECTORS.filter((c) => c.family === f.family && c.emulated).map((c) => c.name).join(" and ")} against an emulator`
                  : ""}
              </span>
            </span>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span><i className="mk-coverage-key is-proven" aria-hidden />Proven in CI</span>
        <span><i className="mk-coverage-key is-proven is-emulated" aria-hidden />{provenBadge(true)}</span>
        <span><i className="mk-coverage-key" aria-hidden />Implemented</span>
      </p>
    </Panel>
  );
}

/**
 * Migrations, the page's own module: an example cutover plan from first
 * profile to the morning after, each stage tied to what the product records.
 */
export function CutoverTimeline() {
  const stages: { when: string; what: string; record: string }[] = [
    { when: "Two weeks out", what: "Profile both schemas and review the map", record: "Approved map, with every renamed pair and its reason" },
    { when: "One week out", what: "Clear the checks and load a subset", record: "Validate results and the subset's Trust Report" },
    { when: "The day before", what: "Accept any known loss explicitly", record: "A signed risk record for each narrowing" },
    { when: "Cutover", what: "Freeze writes, run the full load, read the destination back", record: "The run's accounting: read = landed + held + skipped" },
    { when: "The morning after", what: "Sign the report, then point applications at the new system", record: "Sign-off by engineering, the data owner, and compliance" },
  ];
  return (
    <figure className="mk-cutover" aria-labelledby="mk-cutover-title">
      <figcaption id="mk-cutover-title" className="mk-cutover-head">
        <span>An example cutover plan</span>
      </figcaption>
      <ol className="mk-cutover-line">
        {stages.map((s) => (
          <li key={s.when}>
            <p className="mk-cutover-when">{s.when}</p>
            <p className="mk-cutover-what">{s.what}</p>
            <p className="mk-cutover-record">
              <span className="mk-sr-only">Recorded: </span>
              {s.record}
            </p>
          </li>
        ))}
      </ol>
    </figure>
  );
}

/**
 * MCP, the page's own module: the stage → confirm exchange between an agent
 * and Datawrap, including the refused second confirm.
 */
export function StageConfirmSequence() {
  const steps: { from: "agent" | "datawrap"; call: string; detail: string; tone?: "staged" | "done" | "refused" }[] = [
    { from: "agent", call: "start_transfer", detail: "With the workspace API key. The key's role is Editor." },
    { from: "datawrap", call: "Staged, ack_id returned", detail: "Permission job.run checked. Nothing has run.", tone: "staged" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "After a person has looked at the staged plan." },
    { from: "datawrap", call: "Transfer queued", detail: "Permission checked again, ack_id spent. Same checks and read-back as Studio.", tone: "done" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "The same ack_id, a second time." },
    { from: "datawrap", call: "Refused", detail: "An ack_id is used once.", tone: "refused" },
  ];
  return (
    <figure className="mk-seq" aria-labelledby="mk-seq-title">
      <figcaption id="mk-seq-title" className="mk-seq-head">
        <span>Agent</span>
        <span>Datawrap</span>
      </figcaption>
      <ol className="mk-seq-list">
        {steps.map((s, i) => (
          <li key={i} className={`is-${s.from}${s.tone ? ` is-${s.tone}` : ""}`}>
            <span className="mk-sr-only">{s.from === "agent" ? "Agent: " : "Datawrap: "}</span>
            <code>{s.call}</code>
            <span>{s.detail}</span>
          </li>
        ))}
      </ol>
    </figure>
  );
}

/** Schedules: one tick opened, as a coded panel. */
export function TickDetailPanel() {
  const rows: [string, ReactNode][] = [
    ["Started", "12:07, the run after the reviewed column"],
    ["Mode", "Incremental on ordered_at, deduplicated on order_id"],
    ["Cursor", <><code>ordered_at</code> after the last run&apos;s high-water mark</>],
    ["Map", "The version approved at 12:05, with coupon_code added"],
    ["Checks", "Same Validate checks as the first run, all passed"],
    ["Accounting", "Every row read landed; none held, none skipped"],
  ];
  return (
    <Panel
      title="Tick at 12:07"
      meta="Example data. Orders feed, every 2 minutes"
      badge={<span className="mk-status mk-status--ok"><StatusGlyph kind="landed" />Balanced</span>}
    >
      <div className="mk-verify-bars">
        <IdentityBars share={{ landed: 1, held: 0, skipped: 0 }} legend={["landed"]} />
      </div>
      <dl className="mk-panel-dl">
        {rows.map(([k, v], i) => (
          <div key={k} style={stagger(i)}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <p className="mk-panel-foot">
        <span>Opens the tick&apos;s own job and Trust Report</span>
      </p>
    </Panel>
  );
}

/** Recurring sync, the page's own module: a delete at the source, carried to a mirror. */
export function MirrorDeletePanel() {
  const rows: { key: string; region: string; deleted: boolean }[] = [
    { key: "48201", region: "EMEA", deleted: false },
    { key: "48202", region: "APAC", deleted: true },
    { key: "48203", region: "AMER", deleted: false },
  ];
  return (
    <Panel title="Mirror, after a delete" meta="Example rows. CDC from PostgreSQL, mirror mode">
      <p className="mk-mirror-event">
        <span className="mk-status"><StatusGlyph kind="pending" />Change read from the log</span>
        <code>DELETE order_id = 48202</code>
      </p>
      <table className="mk-panel-table">
        <caption>The destination after the change is applied</caption>
        <thead>
          <tr>
            <th scope="col">order_id</th>
            <th scope="col">region</th>
            <th scope="col">_deleted</th>
            <th scope="col">_deleted_at</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.key} style={stagger(i)} className={r.deleted ? "is-deleted" : undefined}>
              <th scope="row"><code>{r.key}</code></th>
              <td><code>{r.region}</code></td>
              <td><code>{r.deleted ? "true" : "false"}</code></td>
              <td><code>{r.deleted ? "2026-10-30 12:09:14" : "NULL"}</code></td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mk-panel-foot">
        <span>Soft delete by default: the row is kept and flagged, so nothing disappears without a record</span>
      </p>
    </Panel>
  );
}

/** Recurring sync: the run history, each run with the evidence it left. */
export function RunEvidencePanel() {
  const runs = EXAMPLE_TICKS.filter((t) => t.state !== "review").slice(-5);
  return (
    <Panel title="Run history, with evidence" meta="Example data. Orders feed, every 2 minutes">
      <table className="mk-panel-table mk-runs-table">
        <caption>Each run and the evidence it left</caption>
        <thead>
          <tr>
            <th scope="col">Run</th>
            <th scope="col">Verdict</th>
            <th scope="col">Evidence</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((t, i) => (
            <tr key={t.at} style={stagger(i)} className={t.state === "paused" ? "is-paused" : undefined}>
              <th scope="row"><time>{t.at}</time></th>
              <td>
                {t.state === "ok" ? (
                  <span className="mk-verdict mk-verdict--proven">Proven</span>
                ) : (
                  <span className="mk-status mk-status--held"><StatusGlyph kind="held" />Paused before writing</span>
                )}
              </td>
              <td>
                {t.state === "ok" ? "Trust Report and proof pack on the run's job" : "Waiting for a person to review the new column in Map"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Panel>
  );
}
