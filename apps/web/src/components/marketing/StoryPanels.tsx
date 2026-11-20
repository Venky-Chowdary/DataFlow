import { useId, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import { PROCESS_STEPS } from "./ProcessStepper";
import { RECORDED_MAPPING, RECORDED_ROUTE, StatusGlyph } from "./ProductPanels";
import { PROVEN_CDC_SOURCES } from "../../lib/provenEvidence";
import { SYNC_MODES } from "../../lib/transferConstants";

/**
 * Page-specific panels for the product and solution pages (designer B).
 * Same rules as ProductPanels: every panel is an example and says so where a
 * visitor can see it; any figures shown add up; names, types and check names
 * come from the recorded run or the docs.
 */

/** The home page's example run. Every page that shows a run uses it. */
export const RUN = { read: 48_213_906, landed: 48_213_871, held: 33, skipped: 2 };

/** Orders per region on each side of the example run; sums to RUN. */
export const REGIONS: { region: string; source: number; held: number }[] = [
  { region: "APAC", source: 9_642_781, held: 0 },
  { region: "Europe", source: 9_640_920, held: 21 },
  { region: "LATAM", source: 9_644_102, held: 12 },
  { region: "Middle East", source: 9_641_553, held: 0 },
  { region: "North America", source: 9_644_548, held: 0 },
];

export const fmtN = (v: number) => v.toLocaleString("en-US");

/** "48,213,906 read = 48,213,871 landed + 33 held + 2 skipped". */
export const RUN_IDENTITY = `${fmtN(RUN.read)} read = ${fmtN(RUN.landed)} landed + ${RUN.held} held + ${RUN.skipped} skipped`;

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

function Held({ children }: { children: ReactNode }) {
  return (
    <span className="mk-status mk-status--held">
      <StatusGlyph kind="held" />
      {children}
    </span>
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

/* ── Transfer Studio ─────────────────────────────────────────────── */

const RAIL_STATE: Record<(typeof PROCESS_STEPS)[number], string> = {
  Plan: "Approved",
  Validate: "Checking",
  Execute: "Locked",
  Verify: "After the write",
  Evidence: "Sign-off",
};

/** The five steps as Transfer Studio's top rail, with one step lit. */
export function StepRail({ current }: { current: number }) {
  return (
    <ol className="mk-ts-rail" aria-label="The five steps of a transfer">
      {PROCESS_STEPS.map((s, i) => {
        const state = i < current ? "is-done" : i === current ? "is-current" : "is-ahead";
        return (
          <li key={s} className={state} aria-current={i === current ? "step" : undefined}>
            <span className="mk-ts-rail-num" aria-hidden>
              {i < current ? <StatusGlyph kind="landed" /> : i + 1}
            </span>
            <span className="mk-ts-rail-name">{s}</span>
            <span className="mk-ts-rail-state">{RAIL_STATE[s]}</span>
          </li>
        );
      })}
    </ol>
  );
}

/** A few rows of the recorded map, for the layer behind Validate. */
export function MapExcerpt() {
  const rows = RECORDED_MAPPING.filter((m) => ["order_id", "unit_price", "status", "ordered_at", "attributes"].includes(m.col));
  return (
    <Frame title="Map" meta="Example, from a recorded run" badge={<Ok>Approved</Ok>} className="mk-ts-map">
      <table className="mk-panel-table">
        <caption>Column mapping, excerpt</caption>
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

/** What Validate stops: two blocking findings on a different, example plan. */
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
    <Frame title="Validate" meta="Example findings on an unapproved plan" badge={<Held>2 blocking</Held>} className="mk-ts-findings">
      <ul className="mk-ts-findings-list">
        {findings.map((f) => (
          <li key={f.col}>
            <p className="mk-ts-findings-check">
              <StatusGlyph kind="held" />
              {f.check}
              <code>{f.col}</code>
            </p>
            <p className="mk-ts-findings-detail">{f.detail}</p>
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
 * Plan, Validate, Execute on one engraved line, with the plan hash sealed
 * between approval and the write.
 */
export function PlanHashLine({ points }: { points: { step: string; title: string; body: string }[] }) {
  return (
    <ol className="mk-ts-line">
      {points.map((p, i) => (
        <li key={p.step} className={p.step === "Validate" ? "has-seal" : undefined}>
          <p className="mk-ts-line-step">
            <span className="mk-ts-line-num" aria-hidden>
              {PROCESS_STEPS.indexOf(p.step as (typeof PROCESS_STEPS)[number]) + 1}
            </span>
            {p.step}
          </p>
          <h3>{p.title}</h3>
          <p>{p.body}</p>
          {p.step === "Validate" ? (
            <p className="mk-ts-seal">
              <svg viewBox="0 0 40 40" width="40" height="40" aria-hidden focusable="false">
                <circle cx="20" cy="20" r="18" />
                <circle cx="20" cy="20" r="13" />
                <path d="M13.5 20.5 18 25l8.5-9.5" />
              </svg>
              <span>Plan hash recorded on approval</span>
            </p>
          ) : null}
          {i === points.length - 1 ? null : <span className="mk-ts-line-rule" aria-hidden />}
        </li>
      ))}
    </ol>
  );
}

/* ── Job space ───────────────────────────────────────────────────── */

/** One run, start to verdict, along a timeline. */
export function RunTimeline() {
  const events: { name: string; detail: string; tone: "ok" | "held" | "end" }[] = [
    { name: "Start", detail: "Plan hash matches the approved plan", tone: "ok" },
    { name: "Read", detail: `${fmtN(RUN.read)} rows from ${RECORDED_ROUTE.sourceEngine}`, tone: "ok" },
    { name: "Write", detail: `${fmtN(RUN.landed)} landed, ${RUN.held} held, ${RUN.skipped} skipped`, tone: "held" },
    { name: "Read back", detail: `Landed rows counted by ${RECORDED_ROUTE.destEngine}`, tone: "ok" },
    { name: "Compare", detail: "Full checksum of the landed rows", tone: "end" },
  ];
  return (
    <Frame
      title="Run"
      meta={`Example rows. ${RECORDED_ROUTE.source} to ${RECORDED_ROUTE.dest}`}
      badge={<Held>Completed with quarantine</Held>}
      className="mk-jobs-tl"
    >
      <ol className="mk-jobs-tl-track" aria-label="What the run did, in order">
        {events.map((e) => (
          <li key={e.name} className={`is-${e.tone}`}>
            <span className="mk-jobs-tl-dot" aria-hidden>
              <StatusGlyph kind={e.tone === "held" ? "held" : "landed"} />
            </span>
            <strong>{e.name}</strong>
            <span>{e.detail}</span>
          </li>
        ))}
      </ol>
    </Frame>
  );
}

/** The Trust Report's verdict, set in the statement numerals. */
export function VerdictCard() {
  return (
    <Frame title="Trust Report" meta="Example rows" className="mk-jobs-verdict">
      <dl className="mk-jobs-eq" aria-label={RUN_IDENTITY}>
        <div className="is-read">
          <dd>{fmtN(RUN.read)}</dd>
          <dt>read</dt>
        </div>
        <span aria-hidden>=</span>
        <div className="is-landed">
          <dd>{fmtN(RUN.landed)}</dd>
          <dt>landed</dt>
        </div>
        <span aria-hidden>+</span>
        <div className="is-held">
          <dd>{RUN.held}</dd>
          <dt>held</dt>
        </div>
        <span aria-hidden>+</span>
        <div className="is-skipped">
          <dd>{RUN.skipped}</dd>
          <dt>skipped</dt>
        </div>
      </dl>
      <div className="mk-jobs-verdict-foot">
        <span className="mk-verdict mk-verdict--proven">Proven equal</span>
        <p>Every landed row compared by full checksum; none differs. The 33 held rows each carry a reason.</p>
      </div>
    </Frame>
  );
}

/** "A green job is not proof": what the job says beside what the destination holds. */
export function GreenVsCounted() {
  return (
    <div className="mk-jobs-vs">
      <div className="mk-jobs-vs-side is-job">
        <p className="mk-jobs-vs-who">The job says</p>
        <p className="mk-jobs-vs-big">
          <StatusGlyph kind="landed" />
          Succeeded
        </p>
        <p>No error. The writer&apos;s count says what it sent, not what the destination holds.</p>
      </div>
      <div className="mk-jobs-vs-side is-dest">
        <p className="mk-jobs-vs-who">
          {RECORDED_ROUTE.destEngine} counts, after the write
          <span className="mk-sb-example">Example rows</span>
        </p>
        <p className="mk-jobs-vs-big mk-jobs-vs-eq">
          <span>{fmtN(RUN.read)}</span>
          <i>=</i>
          <span className="is-landed">{fmtN(RUN.landed)}</span>
          <i>+</i>
          <span className="is-held">{RUN.held}</span>
          <i>+</i>
          <span className="is-skipped">{RUN.skipped}</span>
        </p>
        <p>
          Read, landed, held and skipped. The 33 held rows: <code>status</code> is NULL; NOT NULL in the destination.
        </p>
      </div>
    </div>
  );
}

/** Which comparison ran, weakest to strongest, as the docs define it. */
export function CoverageLadder() {
  const rungs: { id: string; what: string; strength: string; proof?: boolean }[] = [
    { id: "none", what: "Nothing compared", strength: "No evidence" },
    { id: "count_only", what: "Only row counts", strength: "Says nothing about values" },
    { id: "sampled", what: "Values on a sample of rows", strength: "An estimate with a stated bound" },
    { id: "key_diff", what: "Every row, by key", strength: "Proof for the rows compared", proof: true },
    { id: "full_checksum", what: "Every row, by checksum", strength: "Proof for the rows compared", proof: true },
  ];
  return (
    <ol className="mk-jobs-ladder" aria-label="Which comparison ran, from weakest to strongest">
      {rungs.map((r, i) => (
        <li key={r.id} className={r.proof ? "is-proof" : undefined} style={{ ["--rung" as string]: i }}>
          <span className="mk-jobs-ladder-bar" aria-hidden />
          <code>{r.id}</code>
          <strong>{r.what}</strong>
          <span>{r.strength}</span>
        </li>
      ))}
    </ol>
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

type Run = { at: string; state: "ok" | "paused" | "review" };

const FEED: Run[] = [
  { at: "08:12", state: "ok" },
  { at: "08:14", state: "ok" },
  { at: "08:16", state: "ok" },
  { at: "08:18", state: "ok" },
  { at: "08:20", state: "ok" },
  { at: "08:22", state: "ok" },
  { at: "08:24", state: "ok" },
  { at: "08:26", state: "paused" },
  { at: "12:05", state: "review" },
  { at: "12:07", state: "ok" },
  { at: "12:09", state: "ok" },
];

/** A schedule's run strip: every tick a job, one paused on drift. */
export function ScheduleStrip() {
  return (
    <Frame
      title="Orders feed, every 2 minutes"
      meta="Example data. Incremental, deduplicated on order_id"
      badge={<Held>Paused at 08:26</Held>}
      className="mk-pipe-strip"
    >
      <ol className="mk-pipe-runs" aria-label="Run history">
        {FEED.map((t) => (
          <li key={t.at} className={`is-${t.state}`}>
            <span className="mk-pipe-runs-bar" aria-hidden />
            <time>{t.at}</time>
            <span className="mk-sr-only">
              {t.state === "ok" ? "Run completed, destination read back equal" : t.state === "paused" ? "Paused: a new column appeared at the source" : "New column reviewed in Map, schedule resumed"}
            </span>
          </li>
        ))}
      </ol>
      <p className="mk-panel-foot">
        <span>
          <StatusGlyph kind="landed" /> Read back equal
        </span>
        <span>
          <StatusGlyph kind="held" /> Paused for review
        </span>
      </p>
    </Frame>
  );
}

/** The drift pause, answered by a person. The only motion answers the click. */
export function DriftReview() {
  const [done, setDone] = useState(false);
  return (
    <Frame
      title="Map review"
      meta="Example. A column appeared at the source"
      badge={<span aria-live="polite">{done ? <Ok>Reviewed</Ok> : <Held>Waiting</Held>}</span>}
      className={`mk-pipe-drift${done ? " is-done" : ""}`}
    >
      <div className="mk-pipe-drift-row">
        <code className="mk-pipe-drift-new">+ coupon_code TEXT</code>
        <span className="mk-pipe-drift-to" aria-hidden />
        <code>{done ? "COUPON_CODE VARCHAR" : "not mapped yet"}</code>
      </div>
      <div className="mk-panel-actions">
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--sm" onClick={() => setDone((d) => !d)} aria-pressed={done}>
          {done ? "Show the pause again" : "Approve the mapping"}
        </button>
        <p>{done ? "The next run uses the reviewed map." : "No run writes until someone decides."}</p>
      </div>
    </Frame>
  );
}

/**
 * Every tick is a full job. A long strip of runs, the first reviewed by a
 * person, one paused on drift, all held to the same checks.
 */
export function RunCensus() {
  const total = 72;
  const paused = 47;
  return (
    <figure className="mk-pipe-census">
      <ol className="mk-pipe-census-marks" aria-hidden>
        {Array.from({ length: total }, (_, i) => (
          <li key={i} className={i === 0 ? "is-first" : i === paused ? "is-paused" : undefined} />
        ))}
      </ol>
      <figcaption>
        <span className="mk-pipe-census-first">Run 1: reviewed and approved by a person</span>
        <span className="mk-pipe-census-pause">Paused: a new column waits in Map</span>
        <span className="mk-pipe-census-rest">Every other mark: the same checks and the same read-back as run 1</span>
        <span className="mk-sb-example">Example schedule</span>
      </figcaption>
    </figure>
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

/** The sync modes Transfer Studio offers, as a selector. */
export function ModeSelector() {
  const [active, setActive] = useState(SYNC_MODES.findIndex((m) => m.id === "incremental_deduped"));
  const { onKey, bind } = useRoving(SYNC_MODES.length, active, setActive);
  const base = useId();
  const mode = SYNC_MODES[active];
  const note = MODE_NOTE[mode.id] ?? { does: mode.detail, needs: "" };
  return (
    <div className="mk-pipe-modes">
      <div className="mk-pipe-modes-list" role="tablist" aria-label="Sync modes" aria-orientation="vertical" onKeyDown={onKey}>
        {SYNC_MODES.map((m, i) => (
          <button
            key={m.id}
            ref={bind(i)}
            type="button"
            role="tab"
            id={`${base}-t${i}`}
            aria-selected={i === active}
            aria-controls={`${base}-p`}
            tabIndex={i === active ? 0 : -1}
            className={i === active ? "is-active" : undefined}
            onClick={() => setActive(i)}
          >
            {m.label}
          </button>
        ))}
      </div>
      <div className="mk-pipe-modes-panel" role="tabpanel" id={`${base}-p`} aria-labelledby={`${base}-t${active}`}>
        <p className="mk-pipe-modes-name">{mode.label}</p>
        <p className="mk-pipe-modes-does">{note.does}</p>
        {note.needs ? (
          <p className="mk-pipe-modes-needs">
            <span>Needs</span> {note.needs}
          </p>
        ) : null}
      </div>
    </div>
  );
}

/* ── Query ───────────────────────────────────────────────────────── */

function regionCounts() {
  return [
    ...REGIONS.map((r) => ({ region: r.region, source: r.source, dest: r.source - r.held })),
    { region: "TEST", source: RUN.skipped, dest: 0 },
  ];
}

const SQL = (table: string) => `SELECT region, COUNT(*) AS orders\nFROM ${table}\nGROUP BY region\nORDER BY region;`;

/** Saved connections, the layer behind the editor. */
export function ConnectionList() {
  return (
    <Frame title="Saved connections" meta="Example workspace" className="mk-q-conns">
      <ul className="mk-q-conns-list">
        <li>
          <strong>Orders Postgres (demo)</strong>
          <span>{RECORDED_ROUTE.sourceEngine}, read-only transaction</span>
        </li>
        <li className="is-active">
          <strong>Warehouse Snowflake (demo)</strong>
          <span>{RECORDED_ROUTE.destEngine}, read-only in the editor</span>
        </li>
      </ul>
      <p className="mk-panel-foot">
        <span>Credentials stay in the vault</span>
      </p>
    </Frame>
  );
}

/** The editor: one statement and its result, switchable between the two sides. */
export function QueryEditor() {
  const [side, setSide] = useState<"dest" | "source">("dest");
  const counts = regionCounts().filter((r) => (side === "dest" ? r.dest > 0 : true));
  const table = side === "dest" ? RECORDED_ROUTE.dest : RECORDED_ROUTE.source;
  return (
    <Frame
      title="Query"
      meta="Example rows"
      badge={<span className="mk-status">Read-only</span>}
      className="mk-q-editor"
    >
      <div className="mk-q-switch" role="group" aria-label="Connection">
        <button type="button" aria-pressed={side === "source"} onClick={() => setSide("source")}>
          {RECORDED_ROUTE.sourceEngine}
        </button>
        <button type="button" aria-pressed={side === "dest"} onClick={() => setSide("dest")}>
          {RECORDED_ROUTE.destEngine}
        </button>
      </div>
      <pre className="mk-code" tabIndex={0} aria-label="SQL statement">
        <code>{SQL(table)}</code>
      </pre>
      <table className="mk-panel-table">
        <caption>Orders by region</caption>
        <thead>
          <tr>
            <th scope="col">region</th>
            <th scope="col">orders</th>
          </tr>
        </thead>
        <tbody>
          {counts.map((r) => (
            <tr key={r.region}>
              <th scope="row">
                <code>{r.region}</code>
              </th>
              <td className="mk-num">{fmtN(side === "dest" ? r.dest : r.source)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mk-panel-foot">
        <span aria-live="polite">
          {counts.length} rows, {side === "dest" ? "on the copy" : "on the source"}
        </span>
      </p>
    </Frame>
  );
}

/** The write that never reached the database. */
export function RefusalCard() {
  return (
    <Frame title="Refused" meta="Before it reached the database" className="mk-q-refusal">
      <pre className="mk-code" tabIndex={0} aria-label="Refused statement">
        <code>{`DELETE FROM ${RECORDED_ROUTE.dest}\nWHERE region = 'APAC';`}</code>
      </pre>
      <p className="mk-q-refusal-why">
        <StatusGlyph kind="held" />
        Only read and metadata statements run here.
      </p>
    </Frame>
  );
}

const GUARD_CASES: { label: string; sql: string; stop: number; result: string }[] = [
  {
    label: "A read",
    sql: `SELECT region, COUNT(*)\nFROM ${RECORDED_ROUTE.source}\nGROUP BY region;`,
    stop: -1,
    result: "Runs, and returns rows up to the limit.",
  },
  {
    label: "A write",
    sql: `DELETE FROM ${RECORDED_ROUTE.source}\nWHERE region = 'APAC';`,
    stop: 0,
    result: "Refused in the editor: a write keyword. Nothing reached the database.",
  },
  {
    label: "Two statements",
    sql: `SELECT 1;\nDROP TABLE ${RECORDED_ROUTE.source};`,
    stop: 0,
    result: "Refused in the editor: only one statement runs. Nothing reached the database.",
  },
];

/**
 * Three guards between a statement and the database. Choosing a statement
 * shows where it stops.
 */
export function GuardLayers() {
  const [active, setActive] = useState(1);
  const { onKey, bind } = useRoving(GUARD_CASES.length, active, setActive);
  const c = GUARD_CASES[active];
  const guards = [
    { name: "In the editor", rule: "One statement, a read-only head, no write keyword" },
    { name: "On the server", rule: "A read-only transaction on PostgreSQL, MySQL, SQLite and Oracle" },
    { name: "On the result", rule: "A row limit, unless you raise it" },
  ];
  return (
    <div className="mk-q-guards">
      <div className="mk-q-guards-pick" role="radiogroup" aria-label="Try a statement" onKeyDown={onKey}>
        {GUARD_CASES.map((g, i) => (
          <button
            key={g.label}
            ref={bind(i)}
            type="button"
            role="radio"
            aria-checked={i === active}
            tabIndex={i === active ? 0 : -1}
            onClick={() => setActive(i)}
          >
            {g.label}
          </button>
        ))}
      </div>
      <div className="mk-q-guards-flow">
        <pre className="mk-q-guards-sql" tabIndex={0} aria-label="Statement">
          <code>{c.sql}</code>
        </pre>
        <ol className="mk-q-guards-list">
          {guards.map((g, i) => {
            const state = c.stop === -1 ? "is-pass" : i < c.stop ? "is-pass" : i === c.stop ? "is-stop" : "is-idle";
            return (
              <li key={g.name} className={state}>
                <span className="mk-q-guards-gate" aria-hidden>
                  <StatusGlyph kind={state === "is-stop" ? "held" : state === "is-pass" ? "landed" : "pending"} />
                </span>
                <strong>{g.name}</strong>
                <span>{g.rule}</span>
                <span className="mk-sr-only">{state === "is-stop" ? ", stopped here" : state === "is-pass" ? ", passed" : ", not reached"}</span>
              </li>
            );
          })}
        </ol>
        <p className={`mk-q-guards-db${c.stop === -1 ? " is-reached" : ""}`}>
          <span>{c.stop === -1 ? "Database" : "Database, untouched"}</span>
        </p>
      </div>
      <p className="mk-q-guards-result" aria-live="polite">
        {c.result}
      </p>
    </div>
  );
}

/* ── Pilot ───────────────────────────────────────────────────────── */

const QUESTION = `How many orders per region are in the ${RECORDED_ROUTE.destEngine} copy?`;

export function PilotQuestion() {
  return (
    <p className="mk-pilot-ask">
      <span className="mk-sr-only">Example question: </span>
      {QUESTION}
    </p>
  );
}

/** The answer Pilot returns, with the fields every response carries. */
export function PilotStatement() {
  const rows = regionCounts().filter((r) => r.dest > 0);
  return (
    <Frame title="Pilot" meta="Example answer, on the example run's tables" badge={<Ok>Grounded</Ok>} className="mk-pilot-answer">
      <p className="mk-pilot-answer-text">
        {rows.map((r, i) => (
          <span key={r.region}>
            {r.region} <b>{fmtN(r.dest)}</b>
            {i < rows.length - 1 ? ", " : "."}
          </span>
        ))}
      </p>
      <dl className="mk-pilot-fields">
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
            <code>true</code> read from live state
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

/** The statement behind the answer. */
export function PilotSql() {
  return (
    <Frame title="The statement it ran" meta={`Warehouse ${RECORDED_ROUTE.destEngine} (demo), read-only`} className="mk-pilot-sql">
      <pre className="mk-code" tabIndex={0} aria-label="SQL statement">
        <code>{SQL(RECORDED_ROUTE.dest)}</code>
      </pre>
      <p className="mk-panel-foot">
        <span>An exact aggregate over the whole table</span>
      </p>
    </Frame>
  );
}

/** Pilot's loop, as an engraved ring: question, tools, results, answer. */
export function PilotLoop() {
  const stops = ["Read the question", "Choose tools", "Run them under your role", "Answer from what they returned"];
  return (
    <figure className="mk-pilot-loop">
      <svg viewBox="0 0 320 320" aria-hidden focusable="false">
        <circle cx="160" cy="160" r="150" />
        <circle cx="160" cy="160" r="138" />
        <circle cx="160" cy="160" r="96" className="is-faint" />
        <path d="M160 22 a138 138 0 0 1 138 138" className="is-arc" />
        <path d="M298 160 a138 138 0 0 1 -138 138" className="is-arc" />
        <path d="M160 298 a138 138 0 0 1 -138 -138" className="is-arc" />
        <path d="M22 160 a138 138 0 0 1 138 -138" className="is-arc" />
      </svg>
      <ol>
        {stops.map((s) => (
          <li key={s}>{s}</li>
        ))}
      </ol>
      <figcaption>
        <strong>6</strong>
        <span>rounds of tool calls, at most, per turn</span>
      </figcaption>
    </figure>
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
    <Frame title="At Confirm" meta="What happens, in order" badge={<span className="mk-status">Pilot and MCP</span>} className="mk-pilot-confirm">
      <ol className="mk-pilot-confirm-list">
        {steps.map((s) => (
          <li key={s.name}>
            <StatusGlyph kind="landed" />
            <strong>{s.name}</strong>
            <span>{s.detail}</span>
          </li>
        ))}
      </ol>
    </Frame>
  );
}

/* ── MCP server ──────────────────────────────────────────────────── */

/** An agent's tool calls, as the client shows them. */
export function McpTranscript() {
  const lines: { tool: string; perm: string; out: string; tone: "ok" | "staged" }[] = [
    { tool: "list_connectors", perm: "connector.read", out: "2 saved connections, by reference", tone: "ok" },
    { tool: "plan_transfer", perm: "job.plan", out: "Plan ready. Mapping attached, no blocking findings.", tone: "ok" },
    { tool: "start_transfer", perm: "job.run", out: "Staged. ack_id returned; nothing has run.", tone: "staged" },
  ];
  return (
    <Frame title="Agent session" meta="Example. A client connected to the MCP server" className="mk-mcp-log">
      <ol className="mk-mcp-log-lines" aria-label="Tool calls">
        {lines.map((l) => (
          <li key={l.tool} className={`is-${l.tone}`}>
            <p className="mk-mcp-log-call">
              <code>{l.tool}</code>
              <span>{l.perm}</span>
            </p>
            <p className="mk-mcp-log-out">
              <StatusGlyph kind={l.tone === "ok" ? "landed" : "pending"} />
              {l.out}
            </p>
          </li>
        ))}
      </ol>
      <p className="mk-mcp-log-prompt" aria-hidden>
        <code>awaiting confirm_action</code>
        <i />
      </p>
    </Frame>
  );
}

/** The guarded result: a staged action with its preview and its limits. */
export function McpStaged() {
  return (
    <Frame title="Staged for confirmation" meta="Example preview" badge={<span className="mk-status">Not run</span>} className="mk-mcp-staged">
      <dl className="mk-panel-dl">
        <div>
          <dt>Action</dt>
          <dd>
            <code>start_transfer</code>
          </dd>
        </div>
        <div>
          <dt>Route</dt>
          <dd>Orders Postgres (demo) to Warehouse Snowflake (demo)</dd>
        </div>
        <div>
          <dt>Expires</dt>
          <dd>15 minutes after staging</dd>
        </div>
        <div>
          <dt>Runs</dt>
          <dd>
            Once, when <code>confirm_action</code> is called with its <code>ack_id</code>
          </dd>
        </div>
      </dl>
    </Frame>
  );
}

/** The key the agent holds. */
export function McpKeyCard() {
  return (
    <Frame title="Workspace API key" meta="Example" badge={<span className="mk-status">Editor</span>} className="mk-mcp-keycard">
      <p className="mk-mcp-keycard-line">Never more than the person who created it holds today.</p>
      <p className="mk-mcp-keycard-line">
        Connections by reference: <code>conn_orders_pg</code>
      </p>
    </Frame>
  );
}

/** Stage and confirm between an agent and Datawrap, as two lanes. */
export function McpLanes() {
  const steps: { from: "agent" | "dw"; call: string; detail: string; tone?: "staged" | "done" | "same" }[] = [
    { from: "agent", call: "start_transfer", detail: "With a workspace API key whose role is Editor." },
    { from: "dw", call: "Staged, ack_id", detail: "Permission job.run checked. A preview comes back; nothing has run.", tone: "staged" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "After a person has looked at the preview." },
    { from: "dw", call: "Transfer queued", detail: "Permission checked again. Same map, checks, quarantine and read-back as Studio.", tone: "done" },
    { from: "agent", call: "confirm_action(ack_id)", detail: "The same ack_id, a second time." },
    { from: "dw", call: "Original result", detail: "An ack_id is used once. The transfer does not run twice.", tone: "same" },
  ];
  return (
    <figure className="mk-mcp-lanes">
      <div className="mk-mcp-lanes-head" aria-hidden>
        <span>Agent</span>
        <span>Datawrap</span>
      </div>
      <ol>
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

/* ── Migrations ──────────────────────────────────────────────────── */

const CUTOVER: { when: string; does: string; record: string }[] = [
  { when: "Two weeks out", does: "Profile both schemas, review the map", record: "Approved map, every renamed pair with its reason" },
  { when: "One week out", does: "Clear the checks, load a subset", record: "Validate results and the subset's Trust Report" },
  { when: "The day before", does: "Accept any known loss explicitly", record: "A signed risk record for each narrowing" },
  { when: "Cutover", does: "Freeze writes, run the full load, read it back", record: "The run's statement: read = landed + held + skipped" },
  { when: "The morning after", does: "Sign, then point applications at the new system", record: "Sign-off by engineering, data owner and compliance" },
];

/** The cutover plan as a horizontal ledger. */
export function CutoverLedger() {
  return (
    <figure className="mk-mig-ledger" aria-labelledby="mk-mig-ledger-title">
      <figcaption id="mk-mig-ledger-title" className="mk-mig-ledger-cap">
        <span>Cutover plan</span>
        <span className="mk-sb-example">Example</span>
      </figcaption>
      <ol className="mk-mig-ledger-grid">
        <li className="mk-mig-keys" aria-hidden>
          <span />
          <span>You</span>
          <span>On record</span>
        </li>
        {CUTOVER.map((c, i) => (
          <li key={c.when} className={i === CUTOVER.length - 2 ? "is-cut" : undefined}>
            <p className="mk-mig-when">
              <span className="mk-mig-tick" aria-hidden />
              {c.when}
            </p>
            <p className="mk-mig-does">
              <span className="mk-mig-lbl">You: </span>
              {c.does}
            </p>
            <p className="mk-mig-rec">
              <span className="mk-mig-lbl">On record: </span>
              {c.record}
            </p>
          </li>
        ))}
      </ol>
    </figure>
  );
}

/** The sheet the cutover meeting signs. */
export function SignoffSheet() {
  const roles = ["Engineering", "Data owner", "Compliance"];
  return (
    <figure className="mk-mig-sheet" aria-labelledby="mk-mig-sheet-title">
      <p id="mk-mig-sheet-title" className="mk-mig-sheet-title">
        Trust Report, sign-off
      </p>
      <p className="mk-mig-sheet-meta">Example rows</p>
      <p className="mk-mig-sheet-eq">{RUN_IDENTITY}</p>
      <p className="mk-mig-sheet-verdict">
        <span className="mk-verdict mk-verdict--proven">Proven equal</span>
        <span>Full checksum of the landed rows</span>
      </p>
      <ul className="mk-mig-sheet-lines">
        {roles.map((r) => (
          <li key={r}>
            <span className="mk-mig-sheet-rule" aria-hidden />
            <span>{r}</span>
            <em>Awaiting signature</em>
          </li>
        ))}
      </ul>
    </figure>
  );
}

/* ── Warehouse loading ───────────────────────────────────────────── */

/** An engraved route from the source, through the four stations, into the warehouse. */
export function WarehouseRoute() {
  const stations = ["Plan", "Validate", "Execute", "Verify"];
  const others = ["BigQuery", "Apache Iceberg", "S3, ADLS, GCS"];
  return (
    <figure className="mk-wh-route" aria-label={`Example route: ${RECORDED_ROUTE.sourceEngine} ${RECORDED_ROUTE.source} into ${RECORDED_ROUTE.destEngine} ${RECORDED_ROUTE.dest}, through Plan, Validate, Execute and Verify`}>
      <svg className="mk-wh-route-art" viewBox="0 0 800 300" preserveAspectRatio="none" aria-hidden focusable="false">
        <g className="is-guide">
          {Array.from({ length: 9 }, (_, i) => (
            <path key={i} d={`M120 ${150 + (i - 4) * 4} C 300 ${150 + (i - 4) * 4}, 300 ${150 + (i - 4) * 4}, 680 ${150 + (i - 4) * 4}`} />
          ))}
        </g>
        <g className="is-branch">
          <path d="M560 150 C 620 150, 640 232, 700 236" />
          <path d="M560 150 C 620 150, 640 262, 700 266" />
          <path d="M560 150 C 620 150, 640 292, 700 296" />
        </g>
      </svg>
      <div className="mk-wh-node is-source">
        <span>{RECORDED_ROUTE.sourceEngine}</span>
        <code>{RECORDED_ROUTE.source}</code>
      </div>
      <ol className="mk-wh-stations">
        {stations.map((s) => (
          <li key={s}>
            <i aria-hidden />
            {s}
          </li>
        ))}
      </ol>
      <div className="mk-wh-node is-dest">
        <span>{RECORDED_ROUTE.destEngine}</span>
        <code>{RECORDED_ROUTE.dest}</code>
      </div>
      <ul className="mk-wh-others" aria-label="Other warehouse destinations">
        {others.map((o) => (
          <li key={o}>{o}</li>
        ))}
      </ul>
    </figure>
  );
}

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
    <div className="mk-wh-spec">
      <p className="mk-wh-spec-src">
        <code>order_total</code>
        <span className="mk-wh-spec-val">1234567.89</span>
        <span className="mk-wh-spec-type">
          <code>DECIMAL(12,2)</code> in {RECORDED_ROUTE.sourceEngine}
        </span>
      </p>
      <ul>
        <li className="is-ok">
          <span className="mk-wh-spec-val">1234567.89</span>
          <span className="mk-wh-spec-type">
            <code>NUMBER(12,2)</code> exact
          </span>
          <Ok>Lands</Ok>
        </li>
        <li className="is-lost">
          <span className="mk-wh-spec-val">1234568</span>
          <span className="mk-wh-spec-type">
            <code>NUMBER(12,0)</code> rounds
          </span>
          <Held>Stopped at Validate</Held>
        </li>
      </ul>
      <p className="mk-sb-example">Example value</p>
    </div>
  );
}

/* ── Recurring sync ──────────────────────────────────────────────── */

type SyncRun = { at: string; read: number; landed: number; held: number; skipped: number; first?: boolean; paused?: boolean };

const SYNC_RUNS: SyncRun[] = [
  { at: "Mon 02:00", read: RUN.read, landed: RUN.landed, held: RUN.held, skipped: RUN.skipped, first: true },
  { at: "Mon 10:00", read: 38, landed: 38, held: 0, skipped: 0 },
  { at: "Mon 11:00", read: 41, landed: 40, held: 1, skipped: 0 },
  { at: "Mon 12:00", read: 27, landed: 27, held: 0, skipped: 0 },
  { at: "Mon 13:00", read: 0, landed: 0, held: 0, skipped: 0, paused: true },
  { at: "Mon 14:00", read: 64, landed: 62, held: 0, skipped: 2 },
  { at: "Mon 15:00", read: 33, landed: 33, held: 0, skipped: 0 },
  { at: "Mon 16:00", read: 29, landed: 29, held: 0, skipped: 0 },
];


/** A continuous rail of runs, each a statement held to the first run's standard. */
export function SyncRail() {
  return (
    <div className="mk-sync-rail" role="region" aria-label="Example runs of one schedule, each with its statement" tabIndex={0}>
      <ol>
        {SYNC_RUNS.map((r) => (
          <li key={r.at} className={r.first ? "is-first" : r.paused ? "is-paused" : r.held ? "is-held" : undefined}>
            <p className="mk-sync-at">
              <time>{r.at}</time>
              {r.first ? <span>First run, approved</span> : null}
            </p>
            {r.paused ? (
              <>
                <p className="mk-sync-pause">Paused before writing</p>
                <p className="mk-sync-note">
                  <code>coupon_code</code> appeared at the source. It waits in Map for review.
                </p>
              </>
            ) : (
              <>
                <p className="mk-sync-eq" aria-label={`${fmtN(r.read)} read = ${fmtN(r.landed)} landed + ${r.held} held + ${r.skipped} skipped`}>
                  <span>{fmtN(r.read)}</span>
                  <i>=</i>
                  <span className="is-landed">{fmtN(r.landed)}</span>
                  <i>+</i>
                  <span className="is-held">{r.held}</span>
                  <i>+</i>
                  <span className="is-skipped">{r.skipped}</span>
                </p>
                <p className="mk-sync-note">{r.first ? "The full load: map reviewed, every check passed, read back in full" : "Same checks, same read-back"}</p>
              </>
            )}
          </li>
        ))}
      </ol>
    </div>
  );
}

/** The CDC sources a green CI run proves, set large. */
export function CdcSources() {
  return (
    <ul className="mk-sync-cdc" aria-label="CDC sources proven in CI">
      {PROVEN_CDC_SOURCES.map((s) => (
        <li key={s}>{s}</li>
      ))}
    </ul>
  );
}
