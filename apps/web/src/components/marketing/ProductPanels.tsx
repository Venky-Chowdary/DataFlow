import { useEffect, useState, type ReactNode } from "react";

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

function PanelHead({ title, meta, badge }: { title: string; meta?: string; badge?: ReactNode }) {
  return (
    <div className="mk-panel-head">
      <div>
        <p className="mk-panel-title">{title}</p>
        {meta ? <p className="mk-panel-meta">{meta}</p> : null}
      </div>
      {badge}
    </div>
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
function IdentityBars({ share, settled = true }: { share: Share; settled?: boolean }) {
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
        <span><i className="is-landed" />Landed</span>
        {share.held ? <span><i className="is-held" />Held with a reason</span> : null}
        {share.skipped ? <span><i className="is-skipped" />Skipped by a rule you set</span> : null}
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
    <figure className="mk-ledger2" aria-labelledby="mk-ledger2-title">
      <div className="mk-ledger2-head">
        <div>
          <p id="mk-ledger2-title" className="mk-ledger2-title">Trust Report, reconciliation</p>
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
                <i aria-hidden />
              </li>
            ))}
          </ul>
        </div>

        <div className="mk-ledger2-flow" aria-hidden>
          <span />
        </div>

        <div className="mk-ledger2-col mk-ledger2-lanes">
          <div className="mk-ledger2-lane is-landed">
            <p className="mk-ledger2-colhead">Landed in {RECORDED_ROUTE.destEngine}, counted by its engine</p>
            <ul className="mk-ledger2-chips">
              {lane("landed").map((r) => (
                <li key={r.key} className="mk-ledger2-chip">
                  <code>{r.key}</code>
                </li>
              ))}
            </ul>
          </div>
          <div className="mk-ledger2-lane is-held">
            <p className="mk-ledger2-colhead">Held in quarantine</p>
            <ul>
              {lane("held").map((r) => (
                <li key={r.key} className="mk-ledger2-row">
                  <code>{r.key}</code>
                  <span>{r.note}</span>
                </li>
              ))}
            </ul>
          </div>
          <div className="mk-ledger2-lane is-skipped">
            <p className="mk-ledger2-colhead">Skipped by a rule you set</p>
            <ul>
              {lane("skipped").map((r) => (
                <li key={r.key} className="mk-ledger2-row">
                  <code>{r.key}</code>
                  <span>{r.note}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>

      <div className="mk-ledger2-foot">
        <IdentityBars share={share} settled={done} />
        <div className="mk-ledger2-verdict" aria-live="polite">
          {done ? (
            <>
              <span className="mk-status mk-status--ok">Balanced</span>
              <p>Every row read is landed, held with its reason, or skipped by a rule you set. Nothing is unaccounted for.</p>
            </>
          ) : (
            <p>Reconciling as rows are written</p>
          )}
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

/**
 * The Trust Report comparison, scale-free: what was compared and the result,
 * with the column census settling one column at a time on first paint.
 */
export function VerifyPanel({ animate = true }: { animate?: boolean }) {
  const total = RECORDED_MAPPING.length;
  const [checked, setChecked] = useState(animate ? 0 : total);

  useEffect(() => {
    if (!animate) return;
    if (prefersReducedMotion()) {
      setChecked(total);
      return;
    }
    let n = 0;
    const id = window.setInterval(() => {
      n += 1;
      setChecked(n);
      if (n >= total) window.clearInterval(id);
    }, 110);
    return () => window.clearInterval(id);
  }, [animate, total]);

  const done = checked >= total;
  return (
    <figure className="mk-panel mk-verify" aria-labelledby="mk-verify-title">
      <PanelHead
        title="Trust Report"
        meta="Example, from a recorded run"
        badge={<span className="mk-status mk-status--ok">Completed</span>}
      />
      <RouteLine />
      <div className="mk-verify-bars">
        <IdentityBars share={{ landed: 1, held: 0, skipped: 0 }} />
      </div>
      <table className="mk-panel-table mk-verify-table">
        <caption id="mk-verify-title">Comparison after the write</caption>
        <thead>
          <tr>
            <th scope="col">Check</th>
            <th scope="col">How</th>
            <th scope="col">Result</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <th scope="row">Row count</th>
            <td>Source and {RECORDED_ROUTE.destEngine}, each by its own engine</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Equal</td>
          </tr>
          <tr>
            <th scope="row">Checksum</th>
            <td>Every row, order-independent</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Match</td>
          </tr>
          <tr>
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
    <figure className="mk-panel" aria-labelledby="mk-plan-title">
      <PanelHead
        title="Map"
        meta="Example, from a recorded run. Every column mapped and ready to approve"
        badge={<span className="mk-status">New table</span>}
      />
      <table className="mk-panel-table mk-plan-table">
        <caption id="mk-plan-title">Column mapping</caption>
        <thead>
          <tr>
            <th scope="col">Source column</th>
            <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
            <th scope="col">{RECORDED_ROUTE.destEngine}</th>
            <th scope="col">Status</th>
          </tr>
        </thead>
        <tbody>
          {RECORDED_MAPPING.map((m) => (
            <tr key={m.col}>
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
    </figure>
  );
}

/** Validate: the route's own gates, before anything is written. */
export function GatesPanel() {
  return (
    <figure className="mk-panel" aria-labelledby="mk-gates-title">
      <PanelHead
        title="Validate"
        meta="Example, from a recorded run. Core checks plus the route's policy checks"
        badge={<span className="mk-status mk-status--ok">Approved</span>}
      />
      <p className="mk-sr-only" id="mk-gates-title">
        Checks before the write, each passed or not applicable on this route.
      </p>
      <ul className="mk-gates">
        {RECORDED_GATES.map((g) => (
          <li key={g.id} className={g.state === "pass" ? "is-pass" : "is-na"}>
            <span className={`mk-dot ${g.state === "pass" ? "mk-dot--ok" : "mk-dot--na"}`} aria-hidden />
            <code>{g.id}</code>
            <span>{g.name}</span>
            <em>{g.state === "pass" ? "Passed" : "Not applicable"}</em>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span><span className="mk-dot mk-dot--ok" aria-hidden />Passed</span>
        <span><span className="mk-dot mk-dot--na" aria-hidden />Not applicable</span>
        <span>Execute unlocked</span>
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
    <figure className="mk-panel" aria-labelledby="mk-run-title">
      <PanelHead title="Run" meta="Example, from a recorded run" badge={<span className="mk-status mk-status--ok">Completed</span>} />
      <div className="mk-run-bar" aria-hidden>
        <span style={{ width: "100%" }} />
      </div>
      <dl className="mk-panel-dl" id="mk-run-title">
        {rows.map(([k, v]) => (
          <div key={k}>
            <dt>{k}</dt>
            <dd>{v}</dd>
          </div>
        ))}
      </dl>
      <p className="mk-panel-foot">
        <span><span className="mk-dot mk-dot--ok" aria-hidden />Append delta balanced: count after, minus count before, equals rows written</span>
      </p>
    </figure>
  );
}

/** Evidence: what the job hands to the people who sign it. */
export function EvidencePanel() {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <figure className="mk-panel" aria-labelledby="mk-evidence-title">
      <PanelHead title="Sign-off" meta="Example. The same report, signed by role" />
      <dl className="mk-panel-dl" id="mk-evidence-title">
        <div>
          <dt>Comparison on record</dt>
          <dd>Full checksum, independent re-read of the source</dd>
        </div>
        <div>
          <dt>Proof pack</dt>
          <dd>Signed, verifiable through the API</dd>
        </div>
        <div>
          <dt>Audit log</dt>
          <dd>Hash-chained, exports as CSV, JSON, or CEF</dd>
        </div>
      </dl>
      <ul className="mk-signoff">
        {roles.map((r) => (
          <li key={r}>
            <span>{r}</span>
            <em>Awaiting signature</em>
          </li>
        ))}
      </ul>
    </figure>
  );
}

type Held = { key: string; col: string; value: string; reason: string };

const EXAMPLE_HELD: Held[] = [
  { key: "48213", col: "status", value: "NULL", reason: "STATUS is NOT NULL in the destination" },
  { key: "48977", col: "ordered_at", value: "0000-00-00 00:00:00", reason: "Not a valid timestamp" },
  { key: "50114", col: "attributes", value: "{\"gift\": tru", reason: "Not valid JSON for VARIANT" },
];

/**
 * A run that finished with quarantine. "Replay" answers the visitor's click:
 * the held rows go back through the writer and the accounting closes.
 */
export function QuarantinePanel() {
  const [replayed, setReplayed] = useState(false);
  return (
    <figure className="mk-panel mk-quarantine" aria-labelledby="mk-q-title">
      <PanelHead
        title="Quarantine"
        meta="Example data"
        badge={
          <span className={`mk-status ${replayed ? "mk-status--ok" : "mk-status--held"}`}>
            {replayed ? "Completed" : "Completed with quarantine"}
          </span>
        }
      />
      <div className="mk-verify-bars" aria-live="polite">
        <IdentityBars share={replayed ? { landed: 1, held: 0, skipped: 0 } : { landed: 14, held: 1, skipped: 0 }} />
      </div>
      <table className="mk-panel-table mk-q-table">
        <caption id="mk-q-title">Rows held on the job, with the reason for each</caption>
        <thead>
          <tr>
            <th scope="col">order_id</th>
            <th scope="col">Column</th>
            <th scope="col">Value</th>
            <th scope="col">Reason</th>
          </tr>
        </thead>
        <tbody>
          {EXAMPLE_HELD.map((h) => (
            <tr key={h.key} className={replayed ? "is-replayed" : undefined}>
              <th scope="row"><code>{h.key}</code></th>
              <td><code>{h.col}</code></td>
              <td><code className="mk-q-value">{h.value}</code></td>
              <td>{replayed ? "Replayed after the source was fixed" : h.reason}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <div className="mk-panel-actions">
        <button
          type="button"
          className="mk-btn mk-btn--secondary mk-btn--sm"
          onClick={() => setReplayed((r) => !r)}
          aria-pressed={replayed}
        >
          {replayed ? "Show the held rows again" : "Replay the held rows"}
        </button>
        <p>{replayed ? "Same writer, same table. The accounting closes." : "Fix the cause at the source, then replay."}</p>
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
    <figure className="mk-panel" aria-labelledby="mk-sched-title">
      <PanelHead title="Orders feed, every 2 minutes" meta="Example data. Incremental, deduplicated on order_id" />
      <ol className="mk-ticks" id="mk-sched-title" aria-label="Run history">
        {EXAMPLE_TICKS.map((t) => (
          <li key={t.at} className={`is-${t.state}`}>
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
    </figure>
  );
}
