import { useEffect, useState, type ReactNode } from "react";

/**
 * Datawrap product surfaces rendered in code for the public site.
 *
 * Two kinds of data appear here, and each panel says which it shows:
 *  - RECORDED: transcribed from the recorded run in the hosted workspace,
 *    Orders Postgres (demo) public.sales_orders into Warehouse Snowflake
 *    (demo) DW_DEMO.SALES_ORDERS_REPORTING. Columns, types, gate ids, gate
 *    results, and counts match the screenshots in WORKSPACE_SHOT.
 *  - EXAMPLE: shaped like the product's quarantine and schedule views, with
 *    illustrative values. These panels carry a visible "Example" label.
 */

export const RECORDED_RUN = {
  source: "public.sales_orders",
  sourceEngine: "PostgreSQL",
  dest: "DW_DEMO.SALES_ORDERS_REPORTING",
  destEngine: "Snowflake",
  read: 5100,
  written: 5100,
  held: 0,
  skipped: 0,
  destBefore: 0,
  destAfter: 5100,
};

const fmt = (n: number) => n.toLocaleString("en-US");

/** The 11 columns of the recorded run, as the Map step showed them. */
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

/** Validate on the recorded route: 23 checks, 17 passed, 6 not applicable. */
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
      <span>{RECORDED_RUN.sourceEngine}</span> <code>{RECORDED_RUN.source}</code>
      <span className="mk-panel-route-to">to</span>
      <span>{RECORDED_RUN.destEngine}</span> <code>{RECORDED_RUN.dest}</code>
    </p>
  );
}

/**
 * The Trust Report for the recorded run. The column census settles one
 * column at a time on first paint: the only ambient motion on the site, and
 * it shows what verification does. Reduced motion shows the settled state.
 */
export function VerifyPanel({ animate = true }: { animate?: boolean }) {
  const total = RECORDED_MAPPING.length;
  const [checked, setChecked] = useState(animate ? 0 : total);

  useEffect(() => {
    if (!animate) return;
    let reduce = false;
    try {
      reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch {
      reduce = true;
    }
    if (reduce) {
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
        meta="sales_orders, full append"
        badge={<span className="mk-status mk-status--ok">Completed</span>}
      />
      <RouteLine />
      <div className="mk-verify-identity" role="group" aria-label="Row accounting">
        <div>
          <span>Read</span>
          <b>{fmt(RECORDED_RUN.read)}</b>
        </div>
        <i aria-hidden>=</i>
        <div>
          <span>In destination</span>
          <b>{fmt(RECORDED_RUN.destAfter - RECORDED_RUN.destBefore)}</b>
        </div>
        <i aria-hidden>+</i>
        <div className="is-held">
          <span>Held</span>
          <b>{fmt(RECORDED_RUN.held)}</b>
        </div>
        <i aria-hidden>+</i>
        <div>
          <span>Skipped</span>
          <b>{fmt(RECORDED_RUN.skipped)}</b>
        </div>
      </div>
      <table className="mk-panel-table mk-verify-table">
        <caption id="mk-verify-title">Comparison after the write</caption>
        <thead>
          <tr>
            <th scope="col">Check</th>
            <th scope="col">Source</th>
            <th scope="col">{RECORDED_RUN.destEngine}</th>
            <th scope="col">Result</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <th scope="row">Row count</th>
            <td>{fmt(RECORDED_RUN.read)}</td>
            <td>{fmt(RECORDED_RUN.destAfter)}</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Equal</td>
          </tr>
          <tr>
            <th scope="row">Checksum, every row</th>
            <td>Computed</td>
            <td>Computed</td>
            <td><span className="mk-dot mk-dot--ok" aria-hidden />Match</td>
          </tr>
          <tr>
            <th scope="row">Columns compared</th>
            <td colSpan={2}>
              <span className="mk-sr-only">{checked} of {total} columns compared</span>
              <span className="mk-census" aria-hidden>
                {RECORDED_MAPPING.map((m, i) => (
                  <span key={m.col} className={i < checked ? "is-checked" : undefined} title={m.col} />
                ))}
              </span>
            </td>
            <td aria-live="polite">
              <span className={`mk-dot ${done ? "mk-dot--ok" : "mk-dot--run"}`} aria-hidden />
              {checked} of {total}
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

/** Map step of the recorded run: the plan a person approves. */
export function PlanPanel() {
  return (
    <figure className="mk-panel" aria-labelledby="mk-plan-title">
      <PanelHead
        title="Map"
        meta="11 of 11 mapped, 11 ready to approve"
        badge={<span className="mk-status">New table</span>}
      />
      <table className="mk-panel-table mk-plan-table">
        <caption id="mk-plan-title">Column mapping for the recorded run</caption>
        <thead>
          <tr>
            <th scope="col">Source column</th>
            <th scope="col">{RECORDED_RUN.sourceEngine}</th>
            <th scope="col">{RECORDED_RUN.destEngine}</th>
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

/** Validate on the recorded route. */
export function GatesPanel() {
  const passed = RECORDED_GATES.filter((g) => g.state === "pass").length;
  const na = RECORDED_GATES.length - passed;
  return (
    <figure className="mk-panel" aria-labelledby="mk-gates-title">
      <PanelHead
        title="Validate"
        meta={`${RECORDED_GATES.length} checks before the write, mapping threshold 85%`}
        badge={<span className="mk-status mk-status--ok">Approved</span>}
      />
      <p className="mk-sr-only" id="mk-gates-title">
        {passed} checks passed and {na} were not applicable on this route.
      </p>
      <ul className="mk-gates">
        {RECORDED_GATES.map((g) => (
          <li key={g.id} className={g.state === "pass" ? "is-pass" : "is-na"}>
            <code>{g.id}</code>
            <span>{g.name}</span>
            <em>{g.state === "pass" ? "Passed" : "Not applicable"}</em>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span><span className="mk-dot mk-dot--ok" aria-hidden />{passed} passed</span>
        <span><span className="mk-dot mk-dot--na" aria-hidden />{na} not applicable</span>
        <span>Execute unlocked</span>
      </p>
    </figure>
  );
}

/** Run step of the recorded run. */
export function RunPanel() {
  const rows: [string, string][] = [
    ["Mode", "Full append"],
    ["Rows read", fmt(RECORDED_RUN.read)],
    ["Rows written", fmt(RECORDED_RUN.written)],
    ["Held in quarantine", fmt(RECORDED_RUN.held)],
    [`${RECORDED_RUN.destEngine} count before`, fmt(RECORDED_RUN.destBefore)],
    [`${RECORDED_RUN.destEngine} count after`, fmt(RECORDED_RUN.destAfter)],
  ];
  return (
    <figure className="mk-panel" aria-labelledby="mk-run-title">
      <PanelHead title="Run" meta="Batched writes with bound parameters" badge={<span className="mk-status mk-status--ok">Completed</span>} />
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
        <span><span className="mk-dot mk-dot--ok" aria-hidden />Append delta balanced: {fmt(RECORDED_RUN.destAfter)} after, {fmt(RECORDED_RUN.destBefore)} before, {fmt(RECORDED_RUN.written)} written</span>
      </p>
    </figure>
  );
}

/** Evidence: what the job hands to the people who sign it. */
export function EvidencePanel() {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <figure className="mk-panel" aria-labelledby="mk-evidence-title">
      <PanelHead title="Sign-off" meta="The same report, signed by role" />
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
 * Example of a run that finished with quarantine. "Replay" answers the
 * visitor's click: the held rows go back through the writer and the
 * accounting closes.
 */
export function QuarantinePanel() {
  const [replayed, setReplayed] = useState(false);
  const read = 5100;
  const held = replayed ? 0 : EXAMPLE_HELD.length;
  const landed = read - held;
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
      <div className="mk-verify-identity mk-verify-identity--compact" aria-live="polite">
        <div>
          <span>Read</span>
          <b>{fmt(read)}</b>
        </div>
        <i aria-hidden>=</i>
        <div>
          <span>Landed</span>
          <b>{fmt(landed)}</b>
        </div>
        <i aria-hidden>+</i>
        <div className="is-held">
          <span>Held</span>
          <b>{held}</b>
        </div>
        <i aria-hidden>+</i>
        <div>
          <span>Skipped</span>
          <b>0</b>
        </div>
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
          {replayed ? "Show the held rows again" : "Replay 3 held rows"}
        </button>
        <p>{replayed ? "Same writer, same table. The accounting closes." : "Fix the cause at the source, then replay."}</p>
      </div>
    </figure>
  );
}

type Tick = { at: string; rows: number | null; secs: string; state: "ok" | "paused" | "review" };

const EXAMPLE_TICKS: Tick[] = [
  { at: "08:20", rows: 4, secs: "53s", state: "ok" },
  { at: "08:22", rows: 7, secs: "53s", state: "ok" },
  { at: "08:24", rows: 2, secs: "54s", state: "ok" },
  { at: "08:26", rows: null, secs: "", state: "paused" },
  { at: "12:05", rows: null, secs: "", state: "review" },
  { at: "12:07", rows: 31, secs: "1m 3s", state: "ok" },
  { at: "12:09", rows: 3, secs: "52s", state: "ok" },
];

/** Example of a schedule's run history, including a drift pause. */
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
                <span className="mk-tick-what">
                  {t.rows} {t.rows === 1 ? "row" : "rows"} written, read back equal
                </span>
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
