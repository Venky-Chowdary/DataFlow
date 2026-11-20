import { useId, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import { Ban, Check, CircleSlash, FileSignature, Fingerprint, Download, KeyRound, OctagonX } from "lucide-react";
import { PROCESS_STEPS } from "./ProcessStepper";
import { RECORDED_MAPPING, RECORDED_ROUTE, StatusGlyph } from "./ProductPanels";
import { PROVEN_CDC_SOURCES } from "../../lib/provenEvidence";
import { SYNC_MODES } from "../../lib/transferConstants";

/**
 * Page-specific panels for the product and solution pages (designer B).
 * Every panel is a light product frame and an example, and says so where a
 * visitor can see it; any figures shown add up; names, types and check
 * names come from the recorded run or the docs.
 *
 * The home page's run (48,213,906 read) belongs to Home and Job space. Every
 * other page carries its own labelled example, defined here.
 */

/** The home page's example run. Shown on Home and on Job space only. */
export const RUN = { read: 48_213_906, landed: 48_213_871, held: 33, skipped: 2 };

/** The keys of the home run's held rows shown in examples (10 of RUN.held). */
export const RUN_HELD_KEYS: readonly string[] = ["48204", "48977", "51310", "52046", "55813", "60129", "61402", "63318", "66751", "70264"];

/** Which comparison ran, weakest to strongest, as the docs define it. */
export const COVERAGE_RUNGS: { id: string; what: string; strength: string; proof?: boolean }[] = [
  { id: "none", what: "Nothing compared", strength: "No evidence" },
  { id: "count_only", what: "Only row counts", strength: "Says nothing about values" },
  { id: "sampled", what: "Values on a sample of rows", strength: "An estimate with a stated bound" },
  { id: "key_diff", what: "Every row, by key", strength: "Proof for the rows compared", proof: true },
  { id: "full_checksum", what: "Every row, by checksum", strength: "Proof for the rows compared", proof: true },
];

/** Orders per region on each side of the home run; sums to RUN. */
export const REGIONS: { region: string; source: number; held: number }[] = [
  { region: "APAC", source: 9_642_781, held: 0 },
  { region: "Europe", source: 9_640_920, held: 21 },
  { region: "LATAM", source: 9_644_102, held: 12 },
  { region: "Middle East", source: 9_641_553, held: 0 },
  { region: "North America", source: 9_644_548, held: 0 },
];

export const fmtN = (v: number) => v.toLocaleString("en-US");

export type RunCounts = { read: number; landed: number; held: number; skipped: number };

/** "read = landed + held + skipped", spelled out. */
export function identity(r: RunCounts) {
  return `${fmtN(r.read)} read = ${fmtN(r.landed)} landed + ${fmtN(r.held)} held + ${fmtN(r.skipped)} skipped`;
}

/** "48,213,906 read = 48,213,871 landed + 33 held + 2 skipped". */
export const RUN_IDENTITY = identity(RUN);

/** Migrations: the example cutover load. 6,804,101 + 12 + 0 = 6,804,113. */
export const MIGRATION_RUN: RunCounts = { read: 6_804_113, landed: 6_804_101, held: 12, skipped: 0 };

function Frame({
  title,
  meta,
  badge,
  className = "",
  children,
}: {
  title: string;
  meta?: string;
  badge?: ReactNode;
  className?: string;
  children: ReactNode;
}) {
  const id = useId();
  return (
    <figure className={`mk-panel mk-sb-panel ${className}`.trim()} aria-labelledby={id}>
      <div className="mk-panel-head">
        <div>
          <p className="mk-panel-title" id={id}>
            {title}
          </p>
          {meta ? <p className="mk-panel-meta">{meta}</p> : null}
        </div>
        {badge}
      </div>
      {children}
    </figure>
  );
}

function Ok({ children }: { children: ReactNode }) {
  return (
    <span className="mk-status mk-status--ok">
      <StatusGlyph kind="landed" />
      {children}
    </span>
  );
}

/** A neutral state chip: paused, staged, locked. Ink, never amber. */
function Neutral({ children }: { children: ReactNode }) {
  return <span className="mk-status">{children}</span>;
}

/** Roving focus for a row of tabs or radios: arrows, Home, End. */
function useRoving(count: number, active: number, set: (i: number) => void) {
  const refs = useRef<Array<HTMLButtonElement | null>>([]);
  const onKey = (e: KeyboardEvent<HTMLElement>) => {
    let next = active;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = (active + 1) % count;
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = (active - 1 + count) % count;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = count - 1;
    else return;
    e.preventDefault();
    set(next);
    refs.current[next]?.focus();
  };
  const bind = (i: number) => (el: HTMLButtonElement | null) => {
    refs.current[i] = el;
  };
  return { onKey, bind };
}

/** Pill tabs: the one segmented control on these pages. */
function PillTabs({
  label,
  options,
  active,
  onChange,
  role = "tab",
  controls,
  idBase,
}: {
  label: string;
  options: readonly string[];
  active: number;
  onChange: (i: number) => void;
  role?: "tab" | "radio";
  controls?: string;
  idBase?: string;
}) {
  const { onKey, bind } = useRoving(options.length, active, onChange);
  return (
    <div className="mk-sb-tabs" role={role === "tab" ? "tablist" : "radiogroup"} aria-label={label} onKeyDown={onKey}>
      {options.map((o, i) => (
        <button
          key={o}
          ref={bind(i)}
          type="button"
          role={role}
          id={idBase ? `${idBase}-t${i}` : undefined}
          aria-selected={role === "tab" ? i === active : undefined}
          aria-checked={role === "radio" ? i === active : undefined}
          aria-controls={controls}
          tabIndex={i === active ? 0 : -1}
          className={i === active ? "is-active" : undefined}
          onClick={() => onChange(i)}
        >
          {o}
        </button>
      ))}
    </div>
  );
}

/* ── Transfer Studio ─────────────────────────────────────────────── */

/** What Validate stops: two blocking findings on an example plan. */
export function ValidateFindings() {
  const findings: { check: string; col: string; detail: ReactNode }[] = [
    {
      check: "Population fit",
      col: "unit_price",
      detail: (
        <>
          <code>DECIMAL(12,4)</code> into <code>NUMBER(10,2)</code>: values carry four decimal places, the destination keeps two.
        </>
      ),
    },
    {
      check: "Source column coverage",
      col: "legacy_flag",
      detail: <>Neither mapped nor declared dropped, so it would disappear without a record.</>,
    },
  ];
  return (
    <Frame
      title="Validate"
      meta="Example findings on an unapproved plan"
      badge={
        <span className="mk-status mk-status--stop">
          <OctagonX size={13} aria-hidden />2 blocking
        </span>
      }
      className="mk-ts-findings"
    >
      <ul className="mk-sb-list">
        {findings.map((f) => (
          <li key={f.col}>
            <p className="mk-sb-list-title">
              {f.check}
              <code>{f.col}</code>
            </p>
            <p className="mk-sb-list-body">{f.detail}</p>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span>Execute locked</span>
        <span>Nothing written</span>
      </p>
    </Frame>
  );
}

/**
 * Plan, Validate, Execute and Verify, as one stepper: told once on the
 * site, here on Transfer Studio. The plan hash is recorded at Validate.
 */
export function TransferSteps({ points }: { points: { step: string; title: string; body: string }[] }) {
  return (
    <ol className="mk-sb-steps">
      {points.map((p) => (
        <li key={p.step}>
          <p className="mk-sb-step-head">
            <span className="mk-sb-step-num" aria-hidden>
              {PROCESS_STEPS.indexOf(p.step as (typeof PROCESS_STEPS)[number]) + 1}
            </span>
            <span>{p.step}</span>
          </p>
          <h3>{p.title}</h3>
          <p>{p.body}</p>
          {p.step === "Validate" ? (
            <p className="mk-sb-step-note">
              <Fingerprint size={16} aria-hidden />
              Plan hash recorded on approval
            </p>
          ) : null}
        </li>
      ))}
    </ol>
  );
}

/** One Map row opened: why the pair was made, in the product's words. */
export function MappingDetail() {
  return (
    <Frame title="Map, one row opened" meta="Example, from a recorded run" badge={<Ok>Safe</Ok>} className="mk-ts-why">
      <div className="mk-ts-why-pair">
        <span>
          <code>attributes</code>
          <small>JSONB</small>
        </span>
        <span aria-hidden>→</span>
        <span>
          <code>attributes</code>
          <small>VARIANT</small>
        </span>
        <span className="mk-ts-why-tf">Parse JSON</span>
      </div>
      <dl className="mk-panel-dl">
        <div>
          <dt>Why</dt>
          <dd>New destination table, a lossless equivalent. Created on first write as VARIANT, semi-structured.</dd>
        </div>
        <div>
          <dt>Confidence</dt>
          <dd>93%</dd>
        </div>
        <div>
          <dt>Sample</dt>
          <dd>
            <code>{`{"gift": false, "channel": "store"}`}</code> null 0%
          </dd>
        </div>
        <div>
          <dt>Checked</dt>
          <dd>Sample-validated: 8 of 8 rows</dd>
        </div>
      </dl>
    </Frame>
  );
}

/* ── Job space ───────────────────────────────────────────────────── */

/** "A green job is not proof": what the job says beside what the destination holds. */
export function GreenVsCounted() {
  return (
    <Frame title="sales_orders, after the write" meta="Example rows, the home page's run" className="mk-jobs-vs">
      <div className="mk-jobs-vs-row">
        <p className="mk-jobs-vs-who">The job says</p>
        <p className="mk-jobs-vs-big">Succeeded</p>
        <p className="mk-jobs-vs-note">No error. The writer&apos;s count says what it sent, not what the destination holds.</p>
      </div>
      <div className="mk-jobs-vs-row">
        <p className="mk-jobs-vs-who">{RECORDED_ROUTE.destEngine} counts</p>
        <dl className="mk-sb-eq" aria-label={RUN_IDENTITY}>
          <div>
            <dt>Read</dt>
            <dd>{fmtN(RUN.read)}</dd>
          </div>
          <div className="is-landed" data-op="=">
            <dt>Landed</dt>
            <dd>{fmtN(RUN.landed)}</dd>
          </div>
          <div className="is-held" data-op="+">
            <dt>Held</dt>
            <dd>{RUN.held}</dd>
          </div>
          <div data-op="+">
            <dt>Skipped</dt>
            <dd>{RUN.skipped}</dd>
          </div>
        </dl>
        <p className="mk-jobs-vs-note">
          The 33 held rows: <code>status</code> is NULL; NOT NULL in the destination.
        </p>
      </div>
    </Frame>
  );
}

/** Which comparison ran, weakest to strongest, as the docs define it. */
export function CoverageTable() {
  const rungs = COVERAGE_RUNGS;
  return (
    <table className="mk-sb-table mk-jobs-cov">
      <caption className="mk-sr-only">Which comparison ran, from weakest to strongest</caption>
      <thead>
        <tr>
          <th scope="col">Comparison</th>
          <th scope="col">What is compared</th>
          <th scope="col">What it shows</th>
        </tr>
      </thead>
      <tbody>
        {rungs.map((r) => (
          <tr key={r.id}>
            <th scope="row">
              <code>{r.id}</code>
            </th>
            <td>{r.what}</td>
            <td>
              {r.proof ? (
                <span className="mk-jobs-cov-proof">
                  <Check size={16} aria-hidden />
                  {r.strength}
                </span>
              ) : (
                r.strength
              )}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/** A Trust Report's sign-off: the verdict, the statement and who signs. */
export function SignoffPanel({ title, meta, counts, check = "Full checksum of the landed rows" }: { title: string; meta: string; counts: RunCounts; check?: string }) {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <Frame title={title} meta={meta} badge={<Ok>Proven equal</Ok>} className="mk-sb-signoff">
      <p className="mk-sb-signoff-eq">{identity(counts)}</p>
      <p className="mk-sb-signoff-check">Check: {check}</p>
      <ul className="mk-sb-signoff-roles">
        {roles.map((r) => (
          <li key={r}>
            <FileSignature size={16} aria-hidden />
            <span>{r}</span>
            <em>Awaiting signature</em>
          </li>
        ))}
      </ul>
      <p className="mk-panel-foot">
        <span>
          <Download size={14} aria-hidden />
          Report (.md) and proof pack
        </span>
      </p>
    </Frame>
  );
}

/** The two proof-pack endpoints, as a reader would call them. */
export function ProofPackApi() {
  return (
    <Frame title="Proof pack" meta="API, from the docs" className="mk-jobs-api">
      <dl className="mk-jobs-api-list">
        <div>
          <dt>
            <span className="mk-jobs-verb">GET</span>
            <code>/api/v1/transfer/{"{job_id}"}/proof-pack</code>
          </dt>
          <dd>A signed record of the comparison and the mapping evidence.</dd>
        </div>
        <div>
          <dt>
            <span className="mk-jobs-verb">POST</span>
            <code>/api/v1/transfer/proof-pack/verify</code>
          </dt>
          <dd>Check a pack yourself, without trusting the screen.</dd>
        </div>
      </dl>
      <p className="mk-panel-foot">
        <span>A pack claims &ldquo;migration proven&rdquo; only after a full checksum</span>
      </p>
    </Frame>
  );
}

/* ── Schedules ───────────────────────────────────────────────────── */

type Tick = { at: string; state: "ok" | "paused" | "review" };

/** Ticks up to the pause. Nothing runs after 08:26 until someone decides. */
const FEED_BEFORE: Tick[] = [
  { at: "08:16", state: "ok" },
  { at: "08:18", state: "ok" },
  { at: "08:20", state: "ok" },
  { at: "08:22", state: "ok" },
  { at: "08:24", state: "ok" },
  { at: "08:26", state: "paused" },
];

/** The review at 12:05, and the runs on the reviewed map. Shown once approved. */
const FEED_AFTER: Tick[] = [
  { at: "12:05", state: "review" },
  { at: "12:07", state: "ok" },
  { at: "12:09", state: "ok" },
];

/** A schedule's recent ticks, one paused on drift, and the review that answers it. */
export function DriftReview() {
  const [done, setDone] = useState(false);
  const tick = (t: Tick) => (
    <li key={t.at} className={`is-${t.state}`}>
      <span className="mk-pipe-tick" aria-hidden />
      <time>{t.at}</time>
      <span className="mk-sr-only">
        {t.state === "ok" ? "Run completed, destination read back equal" : t.state === "paused" ? "Paused before writing: a new column appeared at the source" : "New column reviewed in Map"}
      </span>
    </li>
  );
  return (
    <Frame
      title="Orders feed, every 2 minutes"
      meta="Example data. Incremental, deduplicated on order_id"
      badge={<span aria-live="polite">{done ? <Ok>Reviewed at 12:05</Ok> : <Neutral>Paused since 08:26</Neutral>}</span>}
      className={`mk-pipe-drift${done ? " is-done" : ""}`}
    >
      <ol className="mk-pipe-ticks" aria-label="Recent ticks">
        {FEED_BEFORE.map(tick)}
        {done ? (
          <li className="mk-pipe-gap">
            <time>3 h 39 min</time>
            <span className="mk-sr-only">No runs while the schedule waited for a decision</span>
          </li>
        ) : null}
        {done ? FEED_AFTER.map(tick) : null}
      </ol>
      <div className="mk-pipe-drift-row">
        <code>+ coupon_code TEXT</code>
        <span aria-hidden>→</span>
        <code>{done ? "COUPON_CODE VARCHAR" : "not mapped yet"}</code>
      </div>
      <div className="mk-panel-actions">
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => setDone((d) => !d)} aria-pressed={done}>
          {done ? "Show the pause again" : "Approve the mapping"}
        </button>
        <p>{done ? "Runs resumed at 12:07, on the reviewed map." : "No run writes until someone decides."}</p>
      </div>
    </Frame>
  );
}

const MODE_NOTE: Record<string, { does: string; needs: string }> = {
  full_refresh_overwrite: { does: "Replaces the destination with a full snapshot of the source.", needs: "Nothing extra" },
  full_refresh_append: { does: "Inserts the full snapshot again and keeps the rows already there.", needs: "Nothing extra" },
  incremental_append: { does: "Reads only new rows, past the last run's cursor, and never rewrites history.", needs: "A cursor column" },
  incremental_deduped: { does: "Reads new and changed rows by cursor and updates them in place by key.", needs: "A cursor and a key" },
  cdc: { does: "Reads changes from the database log, deletes included, and applies them by key.", needs: "A key and a supported source" },
  scd2: { does: "Keeps every version of a row, with valid-from and valid-to.", needs: "A key" },
  mirror: { does: "Flags rows gone from the source with _deleted and _deleted_at, and keeps them.", needs: "A key" },
};

/** The sync modes Transfer Studio offers, as pill tabs over one detail. */
export function ModeSelector() {
  const [active, setActive] = useState(SYNC_MODES.findIndex((m) => m.id === "incremental_deduped"));
  const base = useId();
  const mode = SYNC_MODES[active];
  const note = MODE_NOTE[mode.id] ?? { does: mode.detail, needs: "" };
  return (
    <Frame title="Sync mode" meta="From the Studio's Destination step" className="mk-pipe-modes">
      <div className="mk-pipe-modes-body">
        <PillTabs label="Sync modes" options={SYNC_MODES.map((m) => m.label)} active={active} onChange={setActive} controls={`${base}-p`} idBase={base} />
        <div className="mk-pipe-modes-panel" role="tabpanel" id={`${base}-p`} aria-labelledby={`${base}-t${active}`}>
          <p className="mk-pipe-modes-name">
            <code>{mode.id}</code>
          </p>
          <p className="mk-pipe-modes-does">{note.does}</p>
          {note.needs ? (
            <p className="mk-pipe-modes-needs">
              <span>Needs:</span> {note.needs}
            </p>
          ) : null}
        </div>
      </div>
    </Frame>
  );
}

/* ── Query ───────────────────────────────────────────────────────── */

/** The Query page's example: the customers copy, which matches its source. */
export const QUERY_SEGMENTS: { segment: string; n: number }[] = [
  { segment: "Enterprise", n: 182_404 },
  { segment: "Mid-market", n: 611_930 },
  { segment: "SMB", n: 1_624_216 },
];

/** Both sides, one question: the counts set side by side. */
export function QueryCompare() {
  const total = QUERY_SEGMENTS.reduce((a, s) => a + s.n, 0);
  return (
    <Frame title="Same SQL, both connections" meta="Example rows. Customers MySQL (demo) and Warehouse Snowflake (demo)" badge={<Ok>Counts agree</Ok>} className="mk-q-compare">
      <pre className="mk-sb-code" tabIndex={0} aria-label="SQL statement (example)">
        <code>{"SELECT segment, COUNT(*) AS customers\nFROM customers\nGROUP BY segment\nORDER BY segment;"}</code>
      </pre>
      <table className="mk-panel-table">
        <caption>Customers by segment on the source and the copy (example)</caption>
        <thead>
          <tr>
            <th scope="col">segment</th>
            <th scope="col" className="mk-num">
              MySQL
            </th>
            <th scope="col" className="mk-num">
              Snowflake
            </th>
            <th scope="col" className="mk-num">
              Difference
            </th>
          </tr>
        </thead>
        <tbody>
          {QUERY_SEGMENTS.map((s) => (
            <tr key={s.segment}>
              <th scope="row">
                <code>{s.segment}</code>
              </th>
              <td className="mk-num">{fmtN(s.n)}</td>
              <td className="mk-num">{fmtN(s.n)}</td>
              <td className="mk-num">0</td>
            </tr>
          ))}
          <tr className="is-total">
            <th scope="row">Total</th>
            <td className="mk-num">{fmtN(total)}</td>
            <td className="mk-num">{fmtN(total)}</td>
            <td className="mk-num">0</td>
          </tr>
        </tbody>
      </table>
      <p className="mk-panel-foot">
        <span>Two tabs, one statement each. Neither side is written to.</span>
      </p>
    </Frame>
  );
}

const GUARD_CASES: { label: string; sql: string; stop: number; result: string }[] = [
  {
    label: "A read",
    sql: "SELECT segment, COUNT(*)\nFROM customers\nGROUP BY segment;",
    stop: -1,
    result: "Runs, and returns rows up to the limit.",
  },
  {
    label: "A write",
    sql: "DELETE FROM customers\nWHERE segment = 'SMB';",
    stop: 0,
    result: "Refused in the editor: a write keyword. Nothing reached the database.",
  },
  {
    label: "Two statements",
    sql: "SELECT 1;\nDROP TABLE customers;",
    stop: 0,
    result: "Refused in the editor: only one statement runs. Nothing reached the database.",
  },
];

/**
 * Three guards between a statement and the database. Choosing a statement
 * shows where it stops: the stop is an ink rule and a label, not a colour.
 */
export function GuardLayers() {
  const [active, setActive] = useState(1);
  const c = GUARD_CASES[active];
  const guards = [
    { name: "In the editor", rule: "One statement, a read-only head, no write keyword" },
    { name: "On the server", rule: "A read-only transaction on PostgreSQL, MySQL, SQLite and Oracle" },
    { name: "On the result", rule: "A row limit, unless you raise it" },
  ];
  return (
    <Frame title="Try a statement" meta="Example statements" className="mk-q-guards">
      <div className="mk-q-guards-body">
        <PillTabs label="Try a statement" options={GUARD_CASES.map((g) => g.label)} active={active} onChange={setActive} role="radio" />
        <pre className="mk-sb-code" tabIndex={0} aria-label="Statement">
          <code>{c.sql}</code>
        </pre>
        <ol className="mk-q-guards-list">
          {guards.map((g, i) => {
            const state = c.stop === -1 ? "is-pass" : i < c.stop ? "is-pass" : i === c.stop ? "is-stop" : "is-idle";
            return (
              <li key={g.name} className={state}>
                <span className="mk-q-guards-gate" aria-hidden>
                  {state === "is-stop" ? <Ban size={14} /> : state === "is-pass" ? <Check size={14} /> : null}
                </span>
                <strong>{g.name}</strong>
                <span>{g.rule}</span>
                {state === "is-stop" ? <em>Stopped here</em> : null}
                <span className="mk-sr-only">{state === "is-stop" ? "" : state === "is-pass" ? ", passed" : ", not reached"}</span>
              </li>
            );
          })}
        </ol>
        <p className="mk-q-guards-result" aria-live="polite">
          {c.result}
        </p>
      </div>
    </Frame>
  );
}

/** The write that never reached the database. Red is one rule and one icon. */
export function RefusalPanel() {
  return (
    <Frame title="Query" meta="Example. Warehouse Snowflake (demo), read-only" className="mk-q-refusal">
      <pre className="mk-sb-code" tabIndex={0} aria-label="Refused statement (example)">
        <code>{"DELETE FROM DW_DEMO.CUSTOMERS\nWHERE segment = 'SMB';"}</code>
      </pre>
      <div className="mk-q-refusal-msg">
        <OctagonX size={16} aria-hidden />
        <div>
          <p className="mk-q-refusal-title">Query refused</p>
          <p>Only read and metadata queries run here (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, VALUES). Found DELETE.</p>
        </div>
      </div>
      <p className="mk-panel-foot">
        <span>The statement never reached Warehouse Snowflake (demo)</span>
      </p>
    </Frame>
  );
}

/* ── Pilot ───────────────────────────────────────────────────────── */

/** How Pilot answers: a short loop, and the fields every answer carries. */
export function PilotLoop() {
  const stops = ["Read the question", "Choose tools", "Run them under your role", "Answer from what they returned"];
  return (
    <Frame title="How an answer is made" meta="Up to 6 rounds of tool calls per turn" className="mk-pilot-loop">
      <ol className="mk-pilot-loop-steps">
        {stops.map((s, i) => (
          <li key={s}>
            <span className="mk-sb-step-num" aria-hidden>
              {i + 1}
            </span>
            <span>{s}</span>
          </li>
        ))}
      </ol>
      <dl className="mk-panel-dl">
        <div>
          <dt>
            <code>tools_used</code>
          </dt>
          <dd>
            <code>aggregate_data</code> succeeded
          </dd>
        </div>
        <div>
          <dt>
            <code>grounded</code>
          </dt>
          <dd>
            <code>true</code>, read from live state
          </dd>
        </div>
        <div>
          <dt>
            <code>pending_actions</code>
          </dt>
          <dd>None. Nothing changes.</dd>
        </div>
      </dl>
    </Frame>
  );
}

/** What Confirm does, in the order it happens. */
export function ConfirmChecks() {
  const steps: { name: string; detail: string }[] = [
    { name: "Staged", detail: "The tool stops with a preview. Nothing has changed." },
    { name: "Permission checked again", detail: "At Confirm, for that specific action, against your role." },
    { name: "Runs once", detail: "An approval is used once and expires 15 minutes after staging." },
  ];
  return (
    <Frame title="At Confirm" meta="What happens, in order" badge={<Neutral>Pilot and MCP</Neutral>} className="mk-pilot-confirm">
      <ol className="mk-pilot-confirm-list">
        {steps.map((s, i) => (
          <li key={s.name}>
            <span className="mk-sb-step-num" aria-hidden>
              {i + 1}
            </span>
            <strong>{s.name}</strong>
            <span>{s.detail}</span>
          </li>
        ))}
      </ol>
    </Frame>
  );
}

/** What Pilot has no tool for. A circle-slash, not an empty checkbox. */
export const PILOT_CANNOT = [
  "Change members, roles, or API keys",
  "Change workspace settings",
  "Sign a Trust Report",
  "Approve a Trust Report fix",
  "Run a query that writes",
  "Decide a mapping or a verdict",
] as const;

export function CannotList() {
  return (
    <ul className="mk-pilot-cannot">
      {PILOT_CANNOT.map((c) => (
        <li key={c}>
          <CircleSlash size={18} aria-hidden />
          {c}
        </li>
      ))}
    </ul>
  );
}

/* ── MCP server ──────────────────────────────────────────────────── */

/** Stage and confirm between an agent and Datawrap. One row style; the state is a pill. */
export function McpLanes() {
  const steps: { from: "agent" | "dw"; call: string; detail: string; state?: string }[] = [
    { from: "agent", call: "start_transfer", detail: "With a workspace API key whose role is Editor." },
    { from: "dw", call: "ack_id", detail: "Permission job.run checked. A preview comes back; nothing has run.", state: "Staged" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "After a person has looked at the preview." },
    { from: "dw", call: "Transfer queued", detail: "Permission checked again. Same map, checks, quarantine and read-back as Studio.", state: "Queued" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "The same ack_id, a second time." },
    { from: "dw", call: "Original result", detail: "An ack_id is used once. The transfer does not run twice.", state: "Same result" },
  ];
  return (
    <Frame title="Agent session" meta="Example. A client connected to the MCP server" className="mk-mcp-lanes">
      <ol className="mk-mcp-lanes-list">
        {steps.map((s, i) => (
          <li key={i}>
            <span className="mk-mcp-who">{s.from === "agent" ? "Agent" : "Datawrap"}</span>
            <span className="mk-mcp-call">
              <code>{s.call}</code>
              {s.state ? <span className={`mk-status${s.state === "Queued" ? " mk-status--ok" : ""}`}>{s.state}</span> : null}
            </span>
            <span className="mk-mcp-detail">{s.detail}</span>
          </li>
        ))}
      </ol>
    </Frame>
  );
}

/** What the agent holds: a URL and a key, never a database password. */
export function McpKeyPanel() {
  return (
    <Frame title="Workspace API key" meta="Example" badge={<Neutral>Role: Editor</Neutral>} className="mk-mcp-key">
      <p className="mk-mcp-key-line">
        <KeyRound size={16} aria-hidden />
        <code>Authorization: Bearer &lt;workspace-api-key&gt;</code>
      </p>
      <dl className="mk-panel-dl">
        <div>
          <dt>Acts as</dt>
          <dd>Never more than the person who created it holds today</dd>
        </div>
        <div>
          <dt>Connections</dt>
          <dd>
            By reference, such as <code>conn_orders_pg</code>. Credentials stay in the vault.
          </dd>
        </div>
        <div>
          <dt>Changes</dt>
          <dd>
            Staged with an <code>ack_id</code>; nothing runs until <code>confirm_action</code>
          </dd>
        </div>
        <div>
          <dt>Record</dt>
          <dd>Every call names the key that made it</dd>
        </div>
      </dl>
    </Frame>
  );
}

/* ── Migrations ──────────────────────────────────────────────────── */

const CUTOVER: { when: string; does: string; record: string }[] = [
  { when: "Two weeks out", does: "Profile both schemas, review the map", record: "Approved map, every renamed pair with its reason" },
  { when: "One week out", does: "Clear the checks, load a subset", record: "Validate results and the subset's Trust Report" },
  { when: "The day before", does: "Accept any known loss explicitly", record: "A signed risk record for each narrowing" },
  { when: "Cutover", does: "Freeze writes, run the full load, read it back", record: "The run's statement: read = landed + held + skipped" },
  { when: "The morning after", does: "Sign, then point applications at the new system", record: "Sign-off by engineering, data owner and compliance" },
];

/** The cutover plan, as the table it is. */
export function CutoverTable() {
  return (
    <table className="mk-sb-table mk-mig-plan">
      <caption className="mk-sr-only">An example cutover plan: what you do, and what is on record</caption>
      <thead>
        <tr>
          <th scope="col">When</th>
          <th scope="col">You</th>
          <th scope="col">On record</th>
        </tr>
      </thead>
      <tbody>
        {CUTOVER.map((c) => (
          <tr key={c.when}>
            <th scope="row">{c.when}</th>
            <td>{c.does}</td>
            <td>{c.record}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ── Warehouse loading ───────────────────────────────────────────── */

/** The type mapping the route carries. */
export function TypeMapping() {
  const rows = RECORDED_MAPPING.filter((m) => ["units", "order_total", "ordered_at", "attributes"].includes(m.col));
  return (
    <Frame title="Types, as they land" meta="Example, from a recorded run" className="mk-wh-types">
      <table className="mk-panel-table">
        <caption>Source and destination types</caption>
        <thead>
          <tr>
            <th scope="col">Column</th>
            <th scope="col">{RECORDED_ROUTE.sourceEngine}</th>
            <th scope="col">{RECORDED_ROUTE.destEngine}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((m) => (
            <tr key={m.col}>
              <th scope="row">
                <code>{m.col}</code>
              </th>
              <td>
                <code>{m.from}</code>
              </td>
              <td>
                <code>{m.to}</code>
                {m.transform ? <span className="mk-panel-note">{m.transform}</span> : null}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </Frame>
  );
}

/** Precision, kept and lost: one value through two destination types. */
export function PrecisionSpecimen() {
  return (
    <Frame title="One value, two destination types" meta="Example value" className="mk-wh-spec">
      <p className="mk-wh-spec-src">
        <code>order_total</code>
        <span className="mk-wh-spec-val">1234567.89</span>
        <span className="mk-wh-spec-type">
          <code>DECIMAL(12,2)</code> in {RECORDED_ROUTE.sourceEngine}
        </span>
      </p>
      <ul className="mk-wh-spec-list">
        <li>
          <span className="mk-wh-spec-val">1234567.89</span>
          <span className="mk-wh-spec-type">
            <code>NUMBER(12,2)</code> exact
          </span>
          <Ok>Lands</Ok>
        </li>
        <li>
          <span className="mk-wh-spec-val">1234568</span>
          <span className="mk-wh-spec-type">
            <code>NUMBER(12,0)</code> rounds
          </span>
          <span className="mk-status mk-status--stop">
            <OctagonX size={13} aria-hidden />
            Stopped at Validate
          </span>
        </li>
      </ul>
    </Frame>
  );
}

/** A narrowing accepted on the record: a signed risk record, never a flag. */
export function RiskRecordPanel() {
  return (
    <Frame title="Risk record" meta="Example. legacy.orders to CORE.ORDERS" badge={<Neutral>Awaiting signature</Neutral>} className="mk-mig-risk">
      <div className="mk-ts-why-pair">
        <span>
          <code>note</code>
          <small>varchar(2000)</small>
        </span>
        <span aria-hidden>→</span>
        <span>
          <code>NOTE</code>
          <small>VARCHAR(500)</small>
        </span>
        <span className="mk-ts-why-tf">Narrowing</span>
      </div>
      <dl className="mk-panel-dl">
        <div>
          <dt>Decision</dt>
          <dd>Accept a known loss on this column</dd>
        </div>
        <div>
          <dt>How</dt>
          <dd>A signed risk record, never a yes/no flag</dd>
        </div>
        <div>
          <dt>Until then</dt>
          <dd>Validate stays locked while the decision is open</dd>
        </div>
      </dl>
      <p className="mk-panel-foot">
        <span>Nothing is pinned until someone accepts it</span>
      </p>
    </Frame>
  );
}

/** An upsert blocked before the load window: duplicate keys in the destination. */
export function UpsertCheck() {
  return (
    <Frame
      title="Validate, upsert by key"
      meta="Example. Into DW_DEMO.ORDER_LINES"
      badge={
        <span className="mk-status mk-status--stop">
          <OctagonX size={13} aria-hidden />1 blocking
        </span>
      }
      className="mk-wh-upsert"
    >
      <ul className="mk-sb-list">
        <li>
          <p className="mk-sb-list-title">
            Data integrity
            <code>order_line_id</code>
          </p>
          <p className="mk-sb-list-body">Duplicate keys already in the destination. An upsert on this key is blocked until they are resolved.</p>
        </li>
        <li>
          <p className="mk-sb-list-title">
            Destination write access
            <Check size={15} aria-hidden className="mk-sb-ok" />
          </p>
          <p className="mk-sb-list-body">Write rights, privileges and capacity, checked before the load window.</p>
        </li>
      </ul>
      <p className="mk-panel-foot">
        <span>Writes as MERGE, or a keyed delete and insert. No index is created.</span>
      </p>
    </Frame>
  );
}

/* ── Recurring sync ──────────────────────────────────────────────── */

/** The CDC sources a green CI run proves, as a row of chips. */
export function CdcSources() {
  return (
    <ul className="mk-sync-cdc" aria-label="CDC sources proven in CI">
      {PROVEN_CDC_SOURCES.map((s) => (
        <li key={s}>
          <Check size={14} aria-hidden />
          {s}
        </li>
      ))}
    </ul>
  );
}

