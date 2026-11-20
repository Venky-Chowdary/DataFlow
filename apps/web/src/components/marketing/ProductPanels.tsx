import { useEffect, useId, useState, type CSSProperties, type ReactNode } from "react";
import { useInViewOnce } from "../../hooks/useInViewOnce";
import { CONNECTOR_CLAIMS } from "../../lib/provenEvidence";

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

/** Validate on the recorded route, gate by gate. */
export const RECORDED_GATES: { id: string; name: string; state: "pass" | "na" }[] = [
  { id: "G1", name: "Source readable", state: "pass" },
  { id: "G2", name: "Destination write access", state: "pass" },
  { id: "G3", name: "Schema contract", state: "pass" },
  { id: "G3F", name: "Population fit", state: "pass" },
  { id: "G4", name: "Column mappings", state: "pass" },
  { id: "G5", name: "Sample dry-run", state: "pass" },
  { id: "G6", name: "Target DDL", state: "pass" },
  { id: "G6C", name: "Create destination table", state: "pass" },
  { id: "G7", name: "Staging capacity", state: "pass" },
  { id: "G8", name: "Sample reconciliation", state: "pass" },
  { id: "G9", name: "Data integrity", state: "pass" },
  { id: "G10", name: "Schema change policy", state: "pass" },
  { id: "G11", name: "Validation posture", state: "pass" },
  { id: "G12", name: "Sync contract", state: "pass" },
  { id: "G12S", name: "Staging policy", state: "pass" },
  { id: "G13", name: "Source column coverage", state: "pass" },
  { id: "G14", name: "Destination required columns", state: "na" },
  { id: "G15", name: "Dest-exists shape", state: "na" },
  { id: "G16", name: "Field reduction governance", state: "pass" },
  { id: "G19", name: "Destination schema replacement", state: "na" },
  { id: "G20", name: "Code crosswalk coverage", state: "na" },
  { id: "G21", name: "Control totals", state: "na" },
  { id: "G22", name: "Destination referential integrity", state: "na" },
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
            <p className="mk-ledger2-colhead"><StatusGlyph kind="landed" />Landed in {RECORDED_ROUTE.destEngine}, counted by its engine</p>
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
        meta="Example, from a recorded run"
        badge={<span className="mk-status mk-status--ok">Completed</span>}
      />
      <RouteLine />
      <div className="mk-verify-bars">
        <IdentityBars share={{ landed: 1, held: 0, skipped: 0 }} legend={["landed"]} />
      </div>
      <table className="mk-panel-table mk-verify-table">
        <caption>Comparison after the write</caption>
        <thead>
          <tr>
            <th scope="col">Check</th>
            <th scope="col">How</th>
            <th scope="col">Result</th>
          </tr>
        </thead>
        <tbody>
          <tr style={stagger(0)}>
            <th scope="row">Row count</th>
            <td>Source and {RECORDED_ROUTE.destEngine}, each by its own engine</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Equal</td>
          </tr>
          <tr style={stagger(1)}>
            <th scope="row">Checksum</th>
            <td>Every row, order-independent</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Match</td>
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
              <span className={`mk-dot ${done ? "mk-dot--ok" : "mk-dot--run"}`} aria-hidden />
              {done ? "Every column" : "Comparing"}
            </td>
          </tr>
        </tbody>
      </table>
      <div className="mk-verify-verdict">
        <span className="mk-verdict mk-verdict--proven">Proven</span>
        <p>Every row was compared and none differs.</p>
      </div>
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
 * Validate: the route's own gates. Once in view they resolve one by one from
 * pending to their result, the way Validate fills in on screen.
 */
export function GatesPanel() {
  const { ref, inView } = useInViewOnce<HTMLElement>();
  const total = RECORDED_GATES.length;
  const [resolved, setResolved] = useState(0);
  const titleId = useId();

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
    }, 55);
    return () => window.clearInterval(id);
  }, [inView, total]);

  const done = resolved >= total;
  return (
    <figure ref={ref} className="mk-panel" aria-labelledby={titleId}>
      <PanelHead
        title="Validate"
        titleId={titleId}
        meta="Example, from a recorded run. Core checks plus the route's policy checks"
        badge={
          <span className={`mk-status${done ? " mk-status--ok" : ""}`} aria-live="polite">
            {done ? "Approved" : "Validating"}
          </span>
        }
      />
      <ul className="mk-gates">
        {RECORDED_GATES.map((g, i) => {
          const state = i < resolved ? g.state : "pending";
          return (
            <li key={g.id} className={`is-${state}`}>
              <span
                className={`mk-dot ${state === "pass" ? "mk-dot--ok" : state === "na" ? "mk-dot--na" : "mk-dot--wait"}`}
                aria-hidden
              />
              <span>{g.name}</span>
              <em>{state === "pass" ? "Passed" : state === "na" ? "Not applicable" : "Pending"}</em>
            </li>
          );
        })}
      </ul>
      <p className="mk-panel-foot">
        <span><span className="mk-dot mk-dot--ok" aria-hidden />Passed</span>
        <span><span className="mk-dot mk-dot--na" aria-hidden />Not applicable</span>
        <span>{done ? "Execute unlocked" : "Execute locked"}</span>
      </p>
    </figure>
  );
}

/** Run step: the destination is counted before and after the write. */
export function RunPanel() {
  const rows: [string, string][] = [
    ["Mode", "Full append"],
    ["Writes", "Batched, bound parameters"],
    ["Rows written", "Equal to rows read"],
    ["Held in quarantine", "None"],
    [`${RECORDED_ROUTE.destEngine} count`, "Taken before and after the write"],
  ];
  return (
    <Panel title="Run" meta="Example, from a recorded run" badge={<span className="mk-status mk-status--ok">Completed</span>}>
      <div className="mk-run-bar" aria-hidden>
        <span />
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
        <span><span className="mk-dot mk-dot--ok" aria-hidden />Append delta balanced: count after, minus count before, equals rows written</span>
      </p>
    </Panel>
  );
}

/** Evidence: what the job hands to the people who sign it. */
export function EvidencePanel() {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <Panel title="Sign-off" meta="Example. The same report, signed by role">
      <dl className="mk-panel-dl">
        {[
          ["Comparison on record", "Full checksum, independent re-read of the source"],
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
  { key: "48977", col: "ordered_at", value: "0000-00-00 00:00:00", reason: "Not a valid timestamp" },
  { key: "50114", col: "attributes", value: "{\"gift\": true,}", reason: "Not valid JSON for VARIANT" },
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
      <span className="mk-status mk-status--held">Completed with quarantine</span>
    ) : state === "replaying" ? (
      <span className="mk-status">Replaying</span>
    ) : (
      <span className="mk-status mk-status--ok">Balanced</span>
    );

  return (
    <figure ref={ref} className={`mk-panel mk-quarantine mk-assemble${inView ? " is-in" : ""} is-${state}`} aria-labelledby={titleId}>
      <PanelHead title="Quarantine" titleId={titleId} meta="Example data" badge={<span aria-live="polite">{badge}</span>} />
      <div className="mk-verify-bars">
        <IdentityBars share={state === "balanced" ? { landed: 1, held: 0, skipped: 0 } : { landed: 14, held: 1, skipped: 0 }} legend={["landed", "held"]} />
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
          {state === "held" ? "Replay the held rows" : state === "replaying" ? "Replaying" : "Show the held rows again"}
        </button>
        <p>
          {state === "held"
            ? "Fix the cause at the source, then replay."
            : state === "replaying"
              ? "Same writer, same table."
              : "Every row read is accounted for again."}
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

const QUERY_ROWS: [string, string, string][] = [
  ["EMEA", "Equal", "Equal"],
  ["AMER", "Equal", "Equal"],
  ["APAC", "Equal", "Equal"],
  ["LATAM", "Equal", "Equal"],
];

/**
 * Query: one read-only statement, asked of the copy and the source. "Try a
 * write" shows what happens to a statement that would change data.
 */
export function QueryPanel() {
  const [tryWrite, setTryWrite] = useState(false);
  return (
    <Panel
      title="Query"
      meta="Example. Read-only, on Warehouse Snowflake (demo) and Orders Postgres (demo)"
      badge={<span className="mk-status">Read-only</span>}
      className="mk-query"
    >
      <pre className="mk-code" aria-label="SQL statement" tabIndex={0}>
        <code>
          {tryWrite
            ? "DELETE FROM DW_DEMO.SALES_ORDERS_REPORTING\nWHERE region = 'TEST';"
            : "SELECT region, COUNT(*) AS orders, SUM(order_total) AS revenue\nFROM DW_DEMO.SALES_ORDERS_REPORTING\nGROUP BY region;"}
        </code>
      </pre>
      {tryWrite ? (
        <p className="mk-query-refused" role="status">
          <span className="mk-status mk-status--held">Refused</span>
          Only read and metadata statements run here. The statement never reached the database.
        </p>
      ) : (
        <table className="mk-panel-table">
          <caption>The same aggregate on both sides</caption>
          <thead>
            <tr>
              <th scope="col">region</th>
              <th scope="col">orders, Snowflake vs Postgres</th>
              <th scope="col">revenue, Snowflake vs Postgres</th>
            </tr>
          </thead>
          <tbody>
            {QUERY_ROWS.map(([r, a, b], i) => (
              <tr key={r} style={stagger(i)}>
                <th scope="row"><code>{r}</code></th>
                <td><span className="mk-dot mk-dot--ok" aria-hidden />{a}</td>
                <td><span className="mk-dot mk-dot--ok" aria-hidden />{b}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      <div className="mk-panel-actions">
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => setTryWrite((w) => !w)} aria-pressed={tryWrite}>
          {tryWrite ? "Back to the read" : "Try a write"}
        </button>
        <p>{tryWrite ? "Writes are refused before they run." : "Each region compared on the copy and the source."}</p>
      </div>
    </Panel>
  );
}

/**
 * Pilot: a question answered by running a tool, with the SQL under the
 * answer, and a change staged for the person to confirm.
 */
export function PilotPanel() {
  const [confirmed, setConfirmed] = useState(false);
  return (
    <Panel title="Pilot" meta="Example conversation" className="mk-pilot">
      <ol className="mk-chat" aria-label="Conversation">
        <li className="mk-chat-you" style={stagger(0)}>
          <span className="mk-sr-only">You: </span>Does the Snowflake copy of the orders still match Postgres?
        </li>
        <li className="mk-chat-pilot" style={stagger(1)}>
          <span className="mk-sr-only">Pilot: </span>
          <p>
            Yes. The latest run on this route is <strong>Proven</strong>: row counts are equal on both sides and the full
            checksum matches. No difference has an open cause.
          </p>
          <p className="mk-chat-meta">From the job's Trust Report. Tool: explain_parity</p>
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

/** Integrations: every family, each member marked proven in CI or implemented. */
export function CoveragePanel() {
  return (
    <Panel title="Connector coverage" meta="From the CI evidence file" className="mk-coverage">
      <ul className="mk-coverage-list">
        {CONNECTOR_CLAIMS.families.map((f, i) => (
          <li key={f.family} style={stagger(i)}>
            <span className="mk-coverage-name">{f.family}</span>
            <span className="mk-coverage-dots" aria-hidden>
              {f.items.map((item) => (
                <i key={item.driver} className={item.proven ? "is-proven" : undefined} title={item.name} />
              ))}
            </span>
            <span className="mk-coverage-count">
              {f.items.filter((x) => x.proven).length} of {f.items.length}
              <span className="mk-sr-only"> proven in CI</span>
            </span>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span><i className="mk-coverage-key is-proven" aria-hidden />Proven in CI</span>
        <span><i className="mk-coverage-key" aria-hidden />Implemented</span>
      </p>
    </Panel>
  );
}
