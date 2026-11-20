import { createContext, useContext, useLayoutEffect, useState, type ReactNode } from "react";
import {
  ArrowLeft,
  ArrowRight,
  CalendarClock,
  Check,
  ChevronRight,
  Clock,
  Copy,
  Database,
  FileCode2,
  Filter,
  OctagonX,
  Play,
  Plus,
  RefreshCw,
  Search,
  Send,
  ShieldCheck,
  Sparkles,
  Table2,
  TriangleAlert,
  X,
} from "lucide-react";
import { ConnectorIcon } from "../../app/brand-icons";
import { AppWindow, PanelWindow } from "./app-frame/AppFrame";
import { RECORDED_CHECK_GROUPS, RECORDED_MAPPING } from "./ProductPanels";
import { MIGRATION_RUN, QUERY_SEGMENTS, REGIONS, RUN, type RunCounts } from "./StoryPanels";

/**
 * Full product screens for the product and solution pages (designer B),
 * built on the shared AppFrame kit. Every figure is example data and adds up.
 * With no props a scene shows the home page's run
 * (48,213,906 read = 48,213,871 landed + 33 held + 2 skipped); the product
 * and solution pages pass `example` to show their own labelled example.
 * On phones each scene renders one panel in window chrome, which reflows at
 * full size (UI text 12px or larger) instead of shrinking the whole screen.
 */

const fmt = new Intl.NumberFormat("en-US");

/* ── Numbered callouts ──────────────────────────────────────────── */

const MarksOn = createContext(false);

/** A numbered pin on the part of a scene that matters. Shown when the scene's `marks` is on. */
function Pin({ n, at = "corner" }: { n: number; at?: "corner" | "right" | "inline" }) {
  const on = useContext(MarksOn);
  if (!on) return null;
  return (
    <span className={`mk-bsc-pin is-${at}`} aria-hidden>
      {n}
    </span>
  );
}

/** What each scene's numbered pins point at, in order. Pair with `marks`. */
export const SCENE_NOTES = {
  transfer: [
    "The step rail: Map, Validate and Run, with the current step lit.",
    "What the step found, including anything that still needs a person.",
    "The next action stays locked until the step is approved.",
  ],
  schedules: [
    "A schedule paused on a schema change says what it is waiting for.",
    "The paused schedule, flagged in the list.",
    "Its run history: the paused tick, then runs on the reviewed map.",
  ],
  query: [
    "Two tabs: the copy and the source. Try a write to see it refused.",
    "Each result shows its rows and time; the total matches the run.",
    "Results are capped by a row limit unless you raise it.",
  ],
  pilot: [
    "The answer, with the SQL that produced it.",
    "The tool that ran, and whether the answer rests on live state.",
    "A change is staged; nothing happens until you confirm.",
  ],
  mcp: [
    "One server URL; the workspace API key is the Bearer header.",
    "start_transfer is staged and returns an ack_id; nothing has run.",
    "A second confirm with the same ack_id returns the original result.",
  ],
} as const;

/** Options every scene takes. All are optional, so a scene renders with no props. */
export type SceneOptions = {
  /** Caption under the window. Defaults to a line saying the data is an example. */
  caption?: string;
  /** False omits the caption (when the host page sets its own). */
  showCaption?: boolean;
  /** Show numbered pins; read them with SCENE_NOTES. */
  marks?: boolean;
};

function cap(caption: string | undefined, fallback: string, show: boolean | undefined) {
  return { text: caption ?? fallback, cls: show === false ? " mk-bsc-nocap" : "" };
}

type Side = { id: string; name: string; table: string };

const SRC: Side = { id: "postgresql", name: "Orders Postgres (demo)", table: "public.sales_orders" };
const DST: Side = { id: "snowflake", name: "Warehouse Snowflake (demo)", table: "DW_DEMO.SALES_ORDERS_REPORTING" };

/** Phone crops: designed at this width, so they reflow at full size on phones. */
const PHONE_DESIGN = 460;

/** True on phone-sized viewports. */
function useNarrow(query = "(max-width: 767px)") {
  const [narrow, setNarrow] = useState(() => {
    try {
      return window.matchMedia(query).matches;
    } catch {
      return false;
    }
  });
  useLayoutEffect(() => {
    let mq: MediaQueryList;
    try {
      mq = window.matchMedia(query);
    } catch {
      return;
    }
    const on = () => setNarrow(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return narrow;
}

function Conn({ id, name }: { id: string; name: string }) {
  return (
    <span className="mk-app-chip">
      <ConnectorIcon id={id} size={14} />
      {name}
    </span>
  );
}

function lastPart(table: string) {
  return table.split(".").pop() ?? table;
}

/** A small segmented picker above a phone crop. */
function Picker<T extends string>({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: readonly (readonly [T, string])[];
  value: T;
  onChange: (v: T) => void;
}) {
  return (
    <div className="mk-app-picker mk-bsc-picker" role="tablist" aria-label={label}>
      {options.map(([id, text]) => (
        <button key={id} type="button" role="tab" aria-selected={value === id} className={value === id ? "is-active" : undefined} onClick={() => onChange(id)}>
          {text}
        </button>
      ))}
    </div>
  );
}

/* ══ Transfer Studio ═══════════════════════════════════════════════ */

const STUDIO_STEPS = ["Source", "Destination", "Transform", "Map", "Validate", "Run"] as const;

/** The one mapped column a person still has to look at in the Map example. */
const REVIEW_COL = "ordered_at";
type StudioStep = "Map" | "Validate" | "Run";

type TransferExample = {
  src: Side;
  dst: Side;
  rows: string;
  run: RunCounts;
  duration: string;
  held: ReactNode;
  skipped: ReactNode;
  caption: string;
};

/** Each page's Transfer example. The default is the home page's run. */
const TRANSFER_EXAMPLES: Record<"home" | "transfer" | "migration", TransferExample> = {
  home: {
    src: SRC,
    dst: DST,
    rows: "48.2M rows",
    run: RUN,
    duration: "41m 37s",
    held: (
      <>
        <code>status</code> is NULL; NOT NULL in the destination.
      </>
    ),
    skipped: (
      <>
        {RUN.skipped} skipped: <code>region = &apos;TEST&apos;</code>.
      </>
    ),
    caption: "Example data: Orders Postgres (demo) into Warehouse Snowflake (demo). Checks from a recorded run.",
  },
  transfer: {
    src: { ...SRC, table: "public.store_orders" },
    dst: { ...DST, table: "DW_DEMO.STORE_ORDERS" },
    rows: "3.1M rows",
    run: { read: 3_126_540, landed: 3_126_512, held: 26, skipped: 2 },
    duration: "4m 12s",
    held: (
      <>
        <code>status</code> is NULL; NOT NULL in the destination.
      </>
    ),
    skipped: (
      <>
        2 skipped: <code>region = &apos;TEST&apos;</code>.
      </>
    ),
    caption: "Example data. Column types and checks follow a recorded run; table names and row counts are illustrative.",
  },
  migration: {
    src: { ...SRC, table: "legacy.orders" },
    dst: { ...DST, table: "CORE.ORDERS" },
    rows: "6.8M rows",
    run: MIGRATION_RUN,
    duration: "9m 48s",
    held: (
      <>
        <code>created</code> is NULL; <code>CREATED_AT</code> is NOT NULL in the destination.
      </>
    ),
    skipped: <>None skipped.</>,
    caption: "Example schemas and counts. Validate and Run show the example cutover load: 6,804,113 read.",
  },
};

const MIGRATION_ROWS: { from: string; ft: string; to: string; tt: string; state: "safe" | "review" | "risk"; why: string }[] = [
  { from: "order_id", ft: "bigint", to: "ORDER_ID", tt: "NUMBER(19,0)", state: "safe", why: "Same name, wider type" },
  { from: "cust_ref", ft: "integer", to: "CUSTOMER_ID", tt: "NUMBER(10,0)", state: "safe", why: "Renamed, matched by role" },
  { from: "created", ft: "timestamptz", to: "CREATED_AT", tt: "TIMESTAMP_TZ", state: "safe", why: "Renamed, matched by role" },
  { from: "amt", ft: "numeric(12,2)", to: "AMOUNT_USD or AMOUNT_LOCAL", tt: "NUMBER(12,2)", state: "review", why: "Two candidates, waiting for a person" },
  { from: "note", ft: "varchar(2000)", to: "NOTE", tt: "VARCHAR(500)", state: "risk", why: "Narrowing: needs a signed risk record" },
];

function StudioRail({ step, onStep, ex }: { step: StudioStep; onStep: (s: StudioStep) => void; ex: TransferExample }) {
  const at = STUDIO_STEPS.indexOf(step);
  return (
    <div className="mk-bsc-rail">
      <Pin n={1} />
      <ol>
        {STUDIO_STEPS.map((s, i) => {
          const choosable = s === "Map" || s === "Validate" || s === "Run";
          const state = i < at ? "is-done" : i === at ? "is-current" : "is-next";
          const inner = (
            <>
              <span className="mk-bsc-rail-box" aria-hidden>
                {i < at ? <Check size={13} /> : i === at && s === "Validate" ? <ShieldCheck size={13} /> : i + 1}
              </span>
              {s}
            </>
          );
          return (
            <li key={s} className={state}>
              {choosable ? (
                <button type="button" aria-current={i === at ? "step" : undefined} onClick={() => onStep(s as StudioStep)}>
                  {inner}
                </button>
              ) : (
                <span>{inner}</span>
              )}
            </li>
          );
        })}
      </ol>
      <p className="mk-bsc-rail-route">
        <Conn id={ex.src.id} name={ex.src.name} />
        <ArrowRight size={13} aria-hidden />
        <Conn id={ex.dst.id} name={ex.dst.name} />
        <code>{lastPart(ex.dst.table)}</code>
        <strong>{ex.rows}</strong>
      </p>
    </div>
  );
}

function StudioMap({ migration }: { migration?: boolean }) {
  if (migration) {
    return (
      <div className="mk-bsc-card">
        <Pin n={2} />
        <div className="mk-bsc-card-head">
          <strong>Column matches</strong>
          <span className="mk-app-pill mk-app-pill--v-not-run">2 need a decision</span>
          <span className="mk-bsc-grow" />
          <span className="mk-app-tbtn">Open mapping proof</span>
        </div>
        <div className="mk-bsc-grid mk-bsc-grid--mig" role="table" aria-label="Column matches between two schemas (example)">
          <div className="mk-bsc-gr mk-bsc-gh" role="row">
            <span role="columnheader">Source</span>
            <span role="columnheader">Destination</span>
            <span role="columnheader">Why</span>
            <span role="columnheader">Status</span>
          </div>
          {MIGRATION_ROWS.map((r) => (
            <div key={r.from} className={`mk-bsc-gr is-${r.state}`} role="row">
              <span role="cell">
                <code>{r.from}</code> <small>{r.ft}</small>
              </span>
              <span role="cell">
                <code>{r.to}</code> <small>{r.tt}</small>
              </span>
              <span role="cell" className="mk-bsc-muted">
                {r.why}
              </span>
              <span role="cell">
                <span className={`mk-app-pill ${r.state === "safe" ? "mk-app-pill--completed" : r.state === "review" ? "mk-app-pill--v-not-run" : "mk-app-pill--failed mk-bsc-risk"}`}>
                  {r.state === "safe" ? "Safe" : r.state === "review" ? "Review" : "Risk record"}
                </span>
              </span>
            </div>
          ))}
        </div>
        <p className="mk-bsc-card-foot">Nothing is pinned until someone accepts it. Validate stays locked while a decision is open.</p>
      </div>
    );
  }
  return (
    <div className="mk-bsc-card">
      <Pin n={2} />
      <div className="mk-bsc-card-head">
        <strong>Column matches</strong>
        <span className="mk-app-pill mk-app-pill--completed">
          {RECORDED_MAPPING.length} of {RECORDED_MAPPING.length} mapped
        </span>
        <span className="mk-app-pill mk-app-pill--v-not-run">1 needs review</span>
        <span className="mk-bsc-grow" />
        <span className="mk-app-tbtn">Open mapping proof</span>
      </div>
      <div className="mk-bsc-grid mk-bsc-grid--map" role="table" aria-label="Column mapping (example, from a recorded run)">
        <div className="mk-bsc-gr mk-bsc-gh" role="row">
          <span role="columnheader">Source column</span>
          <span role="columnheader">PostgreSQL</span>
          <span role="columnheader">Snowflake</span>
          <span role="columnheader">Transform</span>
          <span role="columnheader">Status</span>
        </div>
        {RECORDED_MAPPING.map((m) => {
          const review = m.col === REVIEW_COL;
          return (
            <div key={m.col} className={`mk-bsc-gr${review ? " is-review" : ""}`} role="row">
              <span role="cell">
                <code>{m.col}</code>
              </span>
              <span role="cell">
                <code>{m.from}</code>
              </span>
              <span role="cell">
                <code>{m.to}</code>
              </span>
              <span role="cell" className="mk-bsc-muted">
                {review ? "Time zone kept? Confirm" : (m.transform ?? "None")}
              </span>
              <span role="cell">
                {review ? (
                  <span className="mk-app-pill mk-app-pill--v-not-run">Needs review</span>
                ) : (
                  <span className="mk-app-pill mk-app-pill--completed">Safe</span>
                )}
              </span>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function StudioValidate() {
  const all = RECORDED_CHECK_GROUPS.flatMap((g) => g.checks);
  const passed = all.filter((c) => c.state === "pass").length;
  return (
    <div className="mk-bsc-stack">
      <div className="mk-bsc-verdict">
        <Pin n={2} />
        <span className="mk-bsc-verdict-ring" aria-hidden>
          100<small>%</small>
        </span>
        <div>
          <p className="mk-bsc-verdict-title">
            <span className="mk-app-pill mk-app-pill--completed">Approve</span>
            <strong>Execute-ready, not migration proven</strong>
          </p>
          <p className="mk-bsc-verdict-counts">
            <span>
              <b>{passed}</b> passed
            </span>
            <span>
              <b>0</b> blocked gates
            </span>
            <span>
              <b>{all.length - passed}</b> not applicable
            </span>
          </p>
          <p className="mk-bsc-muted">Proof comes after the write: the destination is read back and compared.</p>
        </div>
      </div>
      <div className="mk-bsc-groups">
        {RECORDED_CHECK_GROUPS.map((g) => (
          <div key={g.name} className="mk-bsc-group">
            <p className="mk-bsc-group-name">
              <Check size={13} aria-hidden />
              {g.name}
              <em>
                {g.checks.filter((c) => c.state === "pass").length}/{g.checks.length}
              </em>
            </p>
            <ul>
              {g.checks.map((c) => (
                <li key={c.name} className={`is-${c.state}`}>
                  <span>{c.name}</span>
                  <small>{c.state === "pass" ? "Passed" : "N/A"}</small>
                </li>
              ))}
            </ul>
          </div>
        ))}
      </div>
    </div>
  );
}

/** read = landed + held + skipped, in the product's own figures. */
function RunEquation({ run }: { run: RunCounts }) {
  return (
    <dl className="mk-bsc-eq">
      <div>
        <dt>Read</dt>
        <dd>{fmt.format(run.read)}</dd>
      </div>
      <span aria-hidden>=</span>
      <div className="is-landed">
        <dt>Landed</dt>
        <dd>{fmt.format(run.landed)}</dd>
      </div>
      <span aria-hidden>+</span>
      <div className="is-held">
        <dt>Held</dt>
        <dd>{fmt.format(run.held)}</dd>
      </div>
      <span aria-hidden>+</span>
      <div className="is-skipped">
        <dt>Skipped</dt>
        <dd>{fmt.format(run.skipped)}</dd>
      </div>
    </dl>
  );
}

function StudioRun({ ex }: { ex: TransferExample }) {
  return (
    <div className="mk-bsc-stack">
      <div className="mk-bsc-card mk-bsc-runcard">
        <Pin n={2} />
        <div className="mk-bsc-card-head">
          <strong>Run complete</strong>
          <span className="mk-app-pill mk-app-pill--completed">Completed with quarantine</span>
          <span className="mk-bsc-grow" />
          <span className="mk-bsc-muted">{ex.duration}</span>
        </div>
        <RunEquation run={ex.run} />
        <div className="mk-bsc-bar" aria-hidden>
          <i style={{ flexGrow: 98 }} />
          <i className="is-held" style={{ flexGrow: 1 }} />
          {ex.run.skipped ? <i className="is-skipped" style={{ flexGrow: 1 }} /> : null}
        </div>
      </div>
      <div className="mk-bsc-two">
        <div className="mk-bsc-card">
          <div className="mk-bsc-card-head">
            <strong>Read back</strong>
            <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
          </div>
          <p className="mk-bsc-card-body">Landed rows counted by Snowflake and compared by full checksum. None differs.</p>
        </div>
        <div className="mk-bsc-card">
          <div className="mk-bsc-card-head">
            <strong>Quarantine</strong>
            <span className="mk-app-pill mk-app-pill--v-explained">{fmt.format(ex.run.held)} held</span>
          </div>
          <p className="mk-bsc-card-body">
            {ex.held} {ex.skipped}
          </p>
        </div>
      </div>
    </div>
  );
}

function StudioFoot({ step }: { step: StudioStep }) {
  return (
    <div className="mk-bsc-foot" aria-hidden>
      <Pin n={3} />
      <span className="mk-app-tbtn">
        <ArrowLeft size={13} />
        Back
      </span>
      <span className="mk-bsc-foot-status">
        <strong>{step}</strong> {step === "Map" ? "Waiting for approval" : step === "Validate" ? "Ready. Execute unlocked" : "Finished. Trust Report ready"}
      </span>
      <span className="mk-bsc-grow" />
      {step === "Validate" ? (
        <>
          <span className="mk-app-tbtn">
            <RefreshCw size={13} />
            Re-run Validate
          </span>
          <span className="mk-app-tbtn mk-app-tbtn--primary">
            <ArrowRight size={13} />
            Execute
          </span>
        </>
      ) : step === "Map" ? (
        <span className="mk-app-tbtn mk-app-tbtn--primary">
          <Check size={13} />
          Approve map
        </span>
      ) : (
        <span className="mk-app-tbtn mk-app-tbtn--primary">
          <ShieldCheck size={13} />
          Open Trust Report
        </span>
      )}
    </div>
  );
}

function TransferInner({
  caption,
  showCaption,
  initial = "Validate",
  migration,
  example,
}: SceneOptions & { initial?: StudioStep; migration?: boolean; example?: "transfer" | "migration" } = {}) {
  const narrow = useNarrow();
  const ex = TRANSFER_EXAMPLES[example ?? "home"];
  const c = cap(caption, ex.caption, showCaption);
  const [step, setStep] = useState<StudioStep>(initial);
  const body = step === "Map" ? <StudioMap migration={migration} /> : step === "Validate" ? <StudioValidate /> : <StudioRun ex={ex} />;
  if (narrow) {
    return (
      <div className="mk-bsc-phone">
        <Picker
          label="Choose a step (example)"
          options={[
            ["Map", "Map"],
            ["Validate", "Validate"],
            ["Run", "Run"],
          ]}
          value={step}
          onChange={setStep}
        />
        <PanelWindow path={`transfer/${step.toLowerCase()}`} caption={c.text} designWidth={PHONE_DESIGN}>
          <div className={`mk-bsc-panel${c.cls}`}>{body}</div>
        </PanelWindow>
      </div>
    );
  }
  return (
    <AppWindow path={`transfer/${step.toLowerCase()}`} active="transfer" caption={c.text} className={`mk-bsc-studio${c.cls}`}>
      <div className="mk-bsc-studio-body">
        <StudioRail step={step} onStep={setStep} ex={ex} />
        <div className="mk-bsc-studio-view" key={step}>
          {body}
        </div>
        <StudioFoot step={step} />
      </div>
    </AppWindow>
  );
}

/* ══ Schedules ═════════════════════════════════════════════════════ */

type Sched = { id: string; name: string; route: string; from: string; to: string; cadence: string; mode: string; last: "completed" | "paused" | "running"; status: "Active" | "Paused" };

const SCHEDULES: Sched[] = [
  { id: "orders-feed", name: "Orders feed, every 2 min", route: "Orders Postgres (demo) → Warehouse Snowflake (demo)", from: "postgresql", to: "snowflake", cadence: "Every 2 minutes", mode: "Incremental deduped", last: "paused", status: "Paused" },
  { id: "sales-nightly", name: "sales_orders → SALES_ORDERS_REPORTING", route: "Orders Postgres (demo) → Warehouse Snowflake (demo)", from: "postgresql", to: "snowflake", cadence: "Daily 02:00 UTC", mode: "Full append", last: "completed", status: "Active" },
  { id: "customers", name: "customers → DW_DEMO.CUSTOMERS", route: "Customers MySQL (demo) → Warehouse Snowflake (demo)", from: "mysql", to: "snowflake", cadence: "Hourly", mode: "Incremental deduped", last: "completed", status: "Active" },
  { id: "orders-live", name: "orders → ANALYTICS.ORDERS_LIVE", route: "Orders Postgres (demo) → Warehouse Snowflake (demo)", from: "postgresql", to: "snowflake", cadence: "Continuous", mode: "CDC", last: "running", status: "Active" },
];

type HistoryRun = { at: string; state: "completed" | "paused"; delta: number; held: number; secs: string; job: string };

const HISTORY: HistoryRun[] = [
  { at: "Today 12:09", state: "completed", delta: 41, held: 0, secs: "52s", job: "6ae4f2c1" },
  { at: "Today 12:07", state: "completed", delta: 3, held: 0, secs: "1m 3s", job: "6ae4e88b" },
  { at: "Today 08:26", state: "paused", delta: 0, held: 0, secs: "", job: "" },
  { at: "Today 08:24", state: "completed", delta: 0, held: 0, secs: "54s", job: "6ae45425" },
  { at: "Today 08:22", state: "completed", delta: 7, held: 0, secs: "53s", job: "6ae453ad" },
];

function HistoryList() {
  return (
    <ol className="mk-bsc-history" aria-label="Run history (example)">
      {HISTORY.map((r) => (
        <li key={r.at} className={`is-${r.state}`}>
          {r.state === "paused" ? <Pin n={3} /> : null}
          <p className="mk-bsc-history-top">
            <span className={`mk-app-pill ${r.state === "completed" ? "mk-app-pill--completed" : "mk-app-pill--v-not-run"}`}>
              {r.state === "completed" ? "Completed" : "Paused before writing"}
            </span>
            <span>{r.at}</span>
            <span className="mk-bsc-grow" />
            <span className="mk-bsc-muted">{r.secs}</span>
          </p>
          {r.state === "paused" ? (
            <p className="mk-bsc-history-note">
              <TriangleAlert size={13} aria-hidden />
              <span>
                Column <code>coupon_code</code> appeared at the source. Waiting for review in Map; reviewed at 12:05.
              </span>
            </p>
          ) : (
            <dl className="mk-bsc-history-dl">
              <div>
                <dt>Dest Δ this run</dt>
                <dd>{r.delta}</dd>
              </div>
              <div>
                <dt>Held</dt>
                <dd>{r.held}</dd>
              </div>
              <div>
                <dt>Job</dt>
                <dd>
                  <code>{r.job}</code>
                </dd>
              </div>
            </dl>
          )}
        </li>
      ))}
    </ol>
  );
}

function ScheduleDrawer({ s, onClose }: { s: Sched; onClose: () => void }) {
  return (
    <aside className="mk-app-drawer mk-bsc-drawer" aria-label={`Run history for ${s.name} (example)`}>
      <div className="mk-app-drawer-head">
        <div>
          <strong>{s.name}</strong>
          <span className={`mk-app-pill ${s.status === "Paused" ? "mk-app-pill--v-not-run" : "mk-app-pill--completed"}`}>{s.status}</span>
          <p>{s.route}</p>
        </div>
        <button type="button" className="mk-app-icon-btn" onClick={onClose} aria-label="Close run history">
          <X size={16} />
        </button>
      </div>
      <div className="mk-app-drawer-tabs" aria-hidden>
        <span>Overview</span>
        <span>
          Schema <em>12</em>
        </span>
        <span className="is-active">
          History <em>{HISTORY.length}</em>
        </span>
        <span>Config</span>
      </div>
      <div className="mk-app-drawer-body">
        <HistoryList />
      </div>
    </aside>
  );
}

function SchedulesInner({ caption, showCaption, initial = "orders-feed" }: SceneOptions & { initial?: string | null } = {}) {
  const narrow = useNarrow();
  const c = cap(caption, "Example workspace. Run times and counts are illustrative.", showCaption);
  const [open, setOpen] = useState<string | null>(initial);
  const [tab, setTab] = useState<"all" | "active" | "paused">("all");
  const shown = SCHEDULES.filter((s) => (tab === "all" ? true : tab === "active" ? s.status === "Active" : s.status === "Paused"));
  const sched = SCHEDULES.find((s) => s.id === open) ?? null;

  if (narrow) {
    return (
      <PanelWindow path="schedules/orders-feed" caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>
          <p className="mk-bsc-panel-title">
            <strong>Orders feed, every 2 min</strong>
            <span className="mk-app-pill mk-app-pill--v-not-run">Paused</span>
          </p>
          <HistoryList />
        </div>
      </PanelWindow>
    );
  }

  return (
    <AppWindow
      path="schedules"
      active="schedules"
      caption={c.text}
      overlay={sched ? <ScheduleDrawer s={sched} onClose={() => setOpen(null)} /> : null}
      className={`${sched ? "has-drawer" : ""}${c.cls}`}
    >
      <div className="mk-bsc-stack">
        <div className="mk-app-toolbar">
          <span className="mk-bsc-search" aria-hidden>
            <Search size={13} />
            Orders
          </span>
          <div className="mk-app-tabs" role="tablist" aria-label="Filter schedules (example)">
            {(
              [
                ["all", "All"],
                ["active", "Active"],
                ["paused", "Paused"],
              ] as const
            ).map(([id, label]) => (
              <button key={id} type="button" role="tab" aria-selected={tab === id} className={tab === id ? "is-active" : undefined} onClick={() => setTab(id)}>
                {label} <em>{SCHEDULES.filter((s) => (id === "all" ? true : id === "active" ? s.status === "Active" : s.status === "Paused")).length}</em>
              </button>
            ))}
          </div>
          <span className="mk-bsc-grow" />
          <div className="mk-app-toolbar-actions" aria-hidden>
            <span className="mk-app-tbtn">Export YAML</span>
            <span className="mk-app-tbtn mk-app-tbtn--primary">
              <Plus size={13} />
              New schedule
            </span>
          </div>
        </div>

        <div className="mk-bsc-decision">
          <Pin n={1} />
          <p className="mk-bsc-decision-head">
            <strong>Waiting on a decision</strong>
            <span className="mk-bsc-muted">1 schedule paused on a schema change, not retrying</span>
          </p>
          <div className="mk-bsc-decision-card">
            <p>
              <TriangleAlert size={14} aria-hidden />
              <code>SCHEMA_DRIFT</code>
              <strong>Orders feed, every 2 min</strong>
            </p>
            <p className="mk-bsc-muted">
              A new column, <code>coupon_code</code>, appeared at the source. The next run waits until it is mapped or declared dropped.
            </p>
            <span className="mk-app-tbtn mk-app-tbtn--primary" aria-hidden>
              Review in Map
            </span>
          </div>
        </div>

        <div className="mk-app-table mk-bsc-sched" role="table" aria-label="Schedules (example workspace)">
          <div className="mk-app-tr mk-app-th" role="row">
            <span role="columnheader">Schedule</span>
            <span role="columnheader">Cadence</span>
            <span role="columnheader">Mode</span>
            <span role="columnheader">Last run</span>
            <span role="columnheader">Status</span>
            <span role="columnheader">
              <span className="mk-sr-only">Open</span>
            </span>
          </div>
          {shown.map((s) => (
            <button
              key={s.id}
              type="button"
              role="row"
              className={`mk-app-tr${open === s.id ? " is-open" : ""}`}
              onClick={() => setOpen(s.id)}
              aria-label={`Open the run history for ${s.name}`}
            >
              {s.status === "Paused" ? <Pin n={2} at="right" /> : null}
              <span role="cell" className="mk-app-job">
                <strong>{s.name}</strong>
                <span className="mk-bsc-muted">{s.route}</span>
              </span>
              <span role="cell" className="mk-app-mode">
                {s.cadence}
              </span>
              <span role="cell" className="mk-app-mode">
                {s.mode}
              </span>
              <span role="cell">
                <span className={`mk-app-pill ${s.last === "completed" ? "mk-app-pill--completed" : s.last === "running" ? "mk-app-pill--running" : "mk-app-pill--v-not-run"}`}>
                  {s.last === "completed" ? "Completed" : s.last === "running" ? "Running" : "Needs review"}
                </span>
              </span>
              <span role="cell" className="mk-app-mode">
                {s.status}
              </span>
              <span role="cell" aria-hidden className="mk-bsc-chev">
                <ChevronRight size={15} />
              </span>
            </button>
          ))}
        </div>
      </div>
    </AppWindow>
  );
}

/* ══ Query ═════════════════════════════════════════════════════════ */

type QueryExample = {
  src: Side & { engine: string; tables: string[] };
  dst: Side & { engine: string; tables: string[] };
  col: string;
  noun: string;
  where: string;
  numType: string;
  rows: (side: "dest" | "source") => { name: string; n: number }[];
  times: { dest: string; source: string };
  caption: string;
};

const QUERY_EXAMPLES: Record<"home" | "customers", QueryExample> = {
  home: {
    src: { ...SRC, engine: "PostgreSQL", tables: ["sales_orders", "customers", "order_items"] },
    dst: { ...DST, engine: "Snowflake", tables: ["SALES_ORDERS", "SALES_ORDERS_REPORTING", "CUSTOMERS"] },
    col: "region",
    noun: "orders",
    where: "region = 'APAC'",
    numType: "NUMBER(18,0)",
    rows: (side) => {
      const rows = REGIONS.map((r) => ({ name: r.region, n: side === "dest" ? r.source - r.held : r.source }));
      if (side === "source") rows.push({ name: "TEST", n: RUN.skipped });
      return rows;
    },
    times: { dest: "3.10 s", source: "2.42 s" },
    caption: "Example data: the copy holds 48,213,871 rows; the source 48,213,906, with 33 held and 2 skipped by the run.",
  },
  customers: {
    src: { id: "mysql", name: "Customers MySQL (demo)", table: "crm.customers", engine: "MySQL", tables: ["accounts", "customers", "contacts"] },
    dst: { ...DST, table: "DW_DEMO.CUSTOMERS", engine: "Snowflake", tables: ["CUSTOMERS", "SALES_ORDERS", "SALES_ORDERS_REPORTING"] },
    col: "segment",
    noun: "customers",
    where: "segment = 'SMB'",
    numType: "NUMBER(18,0)",
    rows: () => QUERY_SEGMENTS.map((s) => ({ name: s.segment, n: s.n })),
    times: { dest: "0.84 s", source: "0.61 s" },
    caption: "Example data: the copy and the source each hold 2,418,550 customers, as the run's Trust Report says.",
  },
};

function sqlFor(q: QueryExample, side: "dest" | "source") {
  const t = side === "dest" ? q.dst.table : q.src.table;
  return `SELECT ${q.col},\n       COUNT(*) AS ${q.noun}\nFROM ${t}\nGROUP BY ${q.col}\nORDER BY ${q.col};`;
}

function QueryEditor({ q, side, write }: { q: QueryExample; side: "dest" | "source"; write: boolean }) {
  const t = side === "dest" ? q.dst.table : q.src.table;
  const sql = write ? `DELETE FROM ${t}\nWHERE ${q.where};` : sqlFor(q, side);
  const lines = sql.split("\n");
  return (
    <div className="mk-bsc-editor">
      <div className="mk-bsc-editor-bar">
        <span className="mk-bsc-muted">Syntax</span>
        <span className="mk-app-chip">{side === "dest" ? q.dst.engine : q.src.engine}</span>
        <span className="mk-bsc-muted">SELECT, WITH, SHOW</span>
        <span className="mk-bsc-grow" />
        <span className="mk-app-tbtn" aria-hidden>
          <X size={12} />
          Clear
        </span>
      </div>
      <pre className="mk-bsc-code" tabIndex={0} aria-label="SQL statement (example)">
        {lines.map((l, i) => (
          <span key={i} className="mk-bsc-code-line">
            <i aria-hidden>{i + 1}</i>
            <code>{l}</code>
          </span>
        ))}
      </pre>
    </div>
  );
}

function QueryResult({ q, side, write }: { q: QueryExample; side: "dest" | "source"; write: boolean }) {
  if (write) {
    return (
      <div className="mk-bsc-refused" role="status">
        <p>
          <OctagonX size={15} aria-hidden />
          <strong>Query refused</strong>
        </p>
        <p>Only read and metadata queries run here (SELECT, WITH, EXPLAIN, SHOW, DESCRIBE, ANALYZE, PRAGMA, VALUES). Found DELETE.</p>
        <p className="mk-bsc-muted">The statement never reached {side === "dest" ? q.dst.name : q.src.name}.</p>
      </div>
    );
  }
  const rows = q.rows(side);
  const total = rows.reduce((a, r) => a + r.n, 0);
  return (
    <div className="mk-bsc-result">
      <div className="mk-bsc-result-bar">
        <Pin n={2} />
        <strong>{rows.length} rows, 2 cols</strong>
        <span className="mk-bsc-muted">
          <Clock size={12} aria-hidden /> {side === "dest" ? q.times.dest : q.times.source}
        </span>
        <span className="mk-bsc-grow" />
        <span className="mk-app-tbtn" aria-hidden>
          <Filter size={12} />
          Filter
        </span>
        <span className="mk-app-tbtn" aria-hidden>
          Copy CSV
        </span>
      </div>
      <table className="mk-bsc-table">
        <caption className="mk-sr-only">Rows by {q.col} (example)</caption>
        <thead>
          <tr>
            <th scope="col">#</th>
            <th scope="col">
              {q.col.toUpperCase()} <small>VARCHAR</small>
            </th>
            <th scope="col" className="is-num">
              {q.noun.toUpperCase()} <small>{q.numType}</small>
            </th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={r.name}>
              <td className="mk-bsc-muted">{i + 1}</td>
              <td>{r.name}</td>
              <td className="is-num">{fmt.format(r.n)}</td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr>
            <td />
            <td>Total</td>
            <td className="is-num">{fmt.format(total)}</td>
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

function QueryInner({ caption, showCaption, example }: SceneOptions & { example?: "customers" } = {}) {
  const narrow = useNarrow();
  const q = QUERY_EXAMPLES[example ?? "home"];
  const c = cap(caption, q.caption, showCaption);
  const [side, setSide] = useState<"dest" | "source">("dest");
  const [write, setWrite] = useState(false);
  const controls = (
    <div className="mk-bsc-qtabs">
      <Pin n={1} />
      <div className="mk-app-tabs" role="tablist" aria-label="Query tabs (example)">
        <button type="button" role="tab" aria-selected={side === "dest"} className={side === "dest" ? "is-active" : undefined} onClick={() => setSide("dest")}>
          {lastPart(q.dst.table)}
        </button>
        <button type="button" role="tab" aria-selected={side === "source"} className={side === "source" ? "is-active" : undefined} onClick={() => setSide("source")}>
          {lastPart(q.src.table)}
        </button>
      </div>
      <span className="mk-bsc-grow" />
      <button type="button" className="mk-bsc-trywrite" aria-pressed={write} onClick={() => setWrite((w) => !w)}>
        {write ? "Back to the read" : "Try a write"}
      </button>
    </div>
  );

  if (narrow) {
    return (
      <PanelWindow path="query" caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>
          {controls}
          <QueryEditor q={q} side={side} write={write} />
          <QueryResult q={q} side={side} write={write} />
        </div>
      </PanelWindow>
    );
  }

  const tables = side === "dest" ? q.dst.tables : q.src.tables;
  const activeTable = lastPart(side === "dest" ? q.dst.table : q.src.table);
  return (
    <AppWindow path="query" active="query" caption={c.text} className={`mk-bsc-query${c.cls}`}>
      <div className="mk-bsc-qwrap">
        {controls}
        <div className="mk-bsc-qconn">
          <Pin n={3} />
          <label>
            <small>Connector</small>
            <span className="mk-bsc-field">
              <ConnectorIcon id={side === "dest" ? q.dst.id : q.src.id} size={15} />
              {side === "dest" ? q.dst.name : q.src.name}
            </span>
          </label>
          <label>
            <small>Row limit</small>
            <span className="mk-bsc-field">1,000</span>
          </label>
          <span className="mk-bsc-grow" />
          <span className="mk-app-tbtn mk-app-tbtn--primary" aria-hidden>
            <Play size={13} />
            Run
          </span>
        </div>
        <div className="mk-bsc-qbody">
          <aside className="mk-bsc-schema" aria-label="Schema (example)">
            <p className="mk-bsc-schema-head">
              <Database size={13} aria-hidden />
              Schema
            </p>
            <ul>
              {tables.map((t) => (
                <li key={t} className={t === activeTable ? "is-active" : undefined}>
                  <Table2 size={13} aria-hidden />
                  {t}
                </li>
              ))}
            </ul>
            <p className="mk-bsc-muted mk-bsc-schema-foot">Types from connector introspection</p>
          </aside>
          <div className="mk-bsc-qmain">
            <QueryEditor q={q} side={side} write={write} />
            <QueryResult q={q} side={side} write={write} />
          </div>
        </div>
      </div>
    </AppWindow>
  );
}

/* ══ Pilot ═════════════════════════════════════════════════════════ */

type PilotExample = {
  question: string;
  table: string;
  where: string;
  noun: string;
  col: string;
  rows: { name: string; n: number }[];
  sql: string;
  route: string;
  cadence: string;
  job: string;
  bar: string;
  chats: string[];
  caption: string;
};

const PILOT_EXAMPLES: Record<"home" | "users", PilotExample> = {
  home: {
    question: "How many orders per region are in the Snowflake copy?",
    table: DST.table,
    where: DST.name,
    noun: "orders",
    col: "REGION",
    rows: REGIONS.map((r) => ({ name: r.region, n: r.source - r.held })),
    sql: `SELECT "region", COUNT(*) AS "orders"\nFROM ${DST.table}\nGROUP BY "region"`,
    route: `${SRC.name} → ${DST.name}`,
    cadence: "Hourly, incremental on ordered_at",
    job: "6af912d7",
    bar: `${SRC.name} → ${DST.table}`,
    chats: ["Orders per region in the copy", "Why did invoices.csv fail?", "Rows held on sales_orders"],
    caption: "Example conversation on the example run's tables. Counts add up to the 48,213,871 landed rows.",
  },
  users: {
    question: "How many users are on each plan in analytics.users?",
    table: "analytics.users",
    where: "Analytics Postgres (demo)",
    noun: "users",
    col: "PLAN",
    rows: [
      { name: "Enterprise", n: 24_242 },
      { name: "Free", n: 701_352 },
      { name: "Team", n: 186_410 },
    ],
    sql: `SELECT "plan", COUNT(*) AS "users"\nFROM analytics.users\nGROUP BY "plan"`,
    route: "App MongoDB (demo) → Analytics Postgres (demo)",
    cadence: "Hourly, incremental on updated_at",
    job: "2c09d6e3",
    bar: "App MongoDB (demo) → analytics.users",
    chats: ["Users on each plan", "Why did invoices.csv fail?", "Which runs were sampled?"],
    caption: "Example conversation. Counts add up to the 912,004 rows of the example users run.",
  },
};

function PilotChat({ p, confirmed, onConfirm }: { p: PilotExample; confirmed: boolean; onConfirm: (v: boolean) => void }) {
  const total = p.rows.reduce((a, r) => a + r.n, 0);
  return (
    <ol className="mk-bsc-chat" aria-label="Pilot conversation (example)">
      <li className="is-you">
        <span className="mk-sr-only">You: </span>
        {p.question}
      </li>
      <li className="is-pilot">
        <Pin n={1} />
        <span className="mk-sr-only">Pilot: </span>
        <p>
          <code>{p.table}</code> on <strong>{p.where}</strong> holds <strong>{fmt.format(total)}</strong> {p.noun} in {p.rows.length} {p.col === "REGION" ? "regions" : "plans"}.
        </p>
        <table className="mk-bsc-table mk-bsc-table--chat">
          <caption className="mk-sr-only">{p.noun} per {p.col.toLowerCase()} (example)</caption>
          <thead>
            <tr>
              <th scope="col">{p.col}</th>
              <th scope="col" className="is-num">
                {p.noun.toUpperCase()}
              </th>
            </tr>
          </thead>
          <tbody>
            {p.rows.map((r) => (
              <tr key={r.name}>
                <td>{r.name}</td>
                <td className="is-num">{fmt.format(r.n)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        <p className="mk-bsc-muted">Exact server-side aggregate over the whole table, not a sample.</p>
        <pre className="mk-bsc-sql" tabIndex={0} aria-label="The SQL Pilot ran (example)">
          <small>SQL</small>
          <code>{p.sql}</code>
        </pre>
        <p className="mk-bsc-pilot-meta">
          <Pin n={2} at="inline" />
          <span className="mk-app-chip">
            <Sparkles size={12} aria-hidden />
            aggregate_data
          </span>
          <span className="mk-app-pill mk-app-pill--completed">Grounded</span>
        </p>
      </li>
      <li className="is-you">
        <span className="mk-sr-only">You: </span>Keep it in sync every hour.
      </li>
      <li className="is-pilot">
        <span className="mk-sr-only">Pilot: </span>
        <div className={`mk-bsc-staged${confirmed ? " is-done" : ""}`}>
          <Pin n={3} />
          <p className="mk-bsc-staged-head">
            <CalendarClock size={14} aria-hidden />
            <strong>Create schedule</strong>
            <span className={`mk-app-pill ${confirmed ? "mk-app-pill--completed" : "mk-app-pill--v-not-run"}`} aria-live="polite">
              {confirmed ? "Confirmed" : "Staged, not run"}
            </span>
          </p>
          <dl>
            <div>
              <dt>Route</dt>
              <dd>{p.route}</dd>
            </div>
            <div>
              <dt>Cadence</dt>
              <dd>{p.cadence}</dd>
            </div>
            <div>
              <dt>Approval</dt>
              <dd>Expires 15 minutes after staging. Used once.</dd>
            </div>
          </dl>
          <div className="mk-bsc-staged-actions">
            {confirmed ? (
              <>
                <span className="mk-bsc-muted">Created. It runs through the same checks as Studio.</span>
                <button type="button" className="mk-bsc-btn" onClick={() => onConfirm(false)}>
                  Reset example
                </button>
              </>
            ) : (
              <>
                <button type="button" className="mk-bsc-btn mk-bsc-btn--primary" onClick={() => onConfirm(true)}>
                  <Check size={13} aria-hidden />
                  Confirm
                </button>
                <span className="mk-bsc-muted">Nothing happens until you confirm.</span>
              </>
            )}
          </div>
        </div>
      </li>
    </ol>
  );
}

function PilotInner({ caption, showCaption, example }: SceneOptions & { example?: "users" } = {}) {
  const narrow = useNarrow();
  const p = PILOT_EXAMPLES[example ?? "home"];
  const c = cap(caption, p.caption, showCaption);
  const [confirmed, setConfirmed] = useState(false);
  const chat = <PilotChat p={p} confirmed={confirmed} onConfirm={setConfirmed} />;
  if (narrow) {
    return (
      <PanelWindow path="pilot" caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>{chat}</div>
      </PanelWindow>
    );
  }
  return (
    <AppWindow path="pilot" active="pilot" caption={c.text} className={`mk-bsc-pilot${c.cls}`}>
      <div className="mk-bsc-pwrap">
        <aside className="mk-bsc-chats" aria-label="Recent chats (example)">
          <span className="mk-app-tbtn mk-app-tbtn--primary mk-bsc-newchat" aria-hidden>
            <Plus size={13} />
            New chat
          </span>
          <p className="mk-bsc-chats-label">Recent chats</p>
          <ul>
            {p.chats.map((t, i) => (
              <li key={t} className={i === 0 ? "is-active" : undefined}>
                {t}
              </li>
            ))}
          </ul>
        </aside>
        <div className="mk-bsc-pmain">
          <div className="mk-bsc-pbar">
            <span className="mk-app-chip">
              JOB <code>{p.job}</code>
              <Copy size={11} aria-hidden />
            </span>
            <span className="mk-bsc-muted">{p.bar}</span>
            <span className="mk-app-pill mk-app-pill--completed">Completed</span>
          </div>
          <div className="mk-bsc-pscroll">{chat}</div>
          <div className="mk-bsc-composer" aria-hidden>
            <span>Follow up: summarize that, or ask what to do next</span>
            <Send size={15} />
          </div>
        </div>
      </div>
    </AppWindow>
  );
}

/* ══ MCP ═══════════════════════════════════════════════════════════ */

const CLIENTS: { name: string; how: string; config: string }[] = [
  { name: "Cursor", how: "Settings → MCP. The URL is absolute; the workspace API key is the Bearer header.", config: `{"mcpServers": {"datawrap": {\n  "url": "https://www.datawrap.io/api/v1/mcp",\n  "headers": {"Authorization": "Bearer <workspace-api-key>"}\n}}}` },
  { name: "Claude Desktop", how: "Remote HTTP MCP. Same endpoint and key as Cursor.", config: `Remote MCP server URL:\nhttps://www.datawrap.io/api/v1/mcp\nHeader: Authorization: Bearer <workspace-api-key>` },
  { name: "VS Code", how: "MCP extension, HTTP transport, same endpoint.", config: `"mcp": {"servers": {"datawrap": {\n  "type": "http",\n  "url": "https://www.datawrap.io/api/v1/mcp"\n}}}` },
  { name: "Other remote clients", how: "Any client that speaks MCP Streamable HTTP. Same endpoint and key.", config: `POST https://www.datawrap.io/api/v1/mcp\nAuthorization: Bearer <workspace-api-key>` },
];

const CALLS: { t: string; tool: string; who: string; status: "ok" | "staged" | "queued" | "same"; ms: string }[] = [
  { t: "09:41:02", tool: "list_connectors", who: "Cursor, Editor key", status: "ok", ms: "84 ms" },
  { t: "09:41:09", tool: "plan_transfer", who: "Cursor, Editor key", status: "ok", ms: "2.1 s" },
  { t: "09:41:31", tool: "start_transfer", who: "Cursor, Editor key", status: "staged", ms: "140 ms" },
  { t: "09:43:55", tool: "confirm_action", who: "Cursor, Editor key", status: "queued", ms: "96 ms" },
  { t: "09:44:02", tool: "confirm_action", who: "Cursor, Editor key", status: "same", ms: "12 ms" },
];

const CALL_LABEL = { ok: "OK", staged: "Staged, ack_id", queued: "Transfer queued", same: "Original result" } as const;

function McpActivity() {
  return (
    <div className="mk-bsc-card">
      <div className="mk-bsc-card-head">
        <strong>Recent activity</strong>
        <span className="mk-bsc-grow" />
        <span className="mk-bsc-muted">Every call names the key</span>
      </div>
      <table className="mk-bsc-table mk-bsc-table--calls">
        <caption className="mk-sr-only">Recent MCP tool calls (example)</caption>
        <thead>
          <tr>
            <th scope="col">Time</th>
            <th scope="col">Tool</th>
            <th scope="col">Client / caller</th>
            <th scope="col">Status</th>
            <th scope="col" className="is-num">
              Latency
            </th>
          </tr>
        </thead>
        <tbody>
          {CALLS.map((c, i) => (
            <tr key={i}>
              <td className="mk-bsc-muted">{c.t}</td>
              <td>
                <code>{c.tool}</code>
              </td>
              <td className="mk-bsc-muted">{c.who}</td>
              <td>
                {c.status === "staged" ? <Pin n={2} at="inline" /> : c.status === "same" ? <Pin n={3} at="inline" /> : null}
                <span className={`mk-app-pill ${c.status === "staged" || c.status === "same" ? "mk-app-pill--v-not-run" : "mk-app-pill--completed"}`}>{CALL_LABEL[c.status]}</span>
              </td>
              <td className="is-num mk-bsc-muted">{c.ms}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function McpInner({ caption, showCaption }: SceneOptions = {}) {
  const narrow = useNarrow();
  const c = cap(caption, "Example workspace. Times and latencies are illustrative.", showCaption);
  const [open, setOpen] = useState<string | null>("Cursor");
  if (narrow) {
    return (
      <PanelWindow path="mcp" caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>
          <McpActivity />
        </div>
      </PanelWindow>
    );
  }
  return (
    <AppWindow path="mcp" active="mcp" caption={c.text} className={`mk-bsc-mcp${c.cls}`}>
      <div className="mk-bsc-stack">
        <div className="mk-bsc-card mk-bsc-url">
          <Pin n={1} />
          <div>
            <p className="mk-bsc-url-label">
              <small>MCP server URL</small>
              <span className="mk-app-pill mk-app-pill--completed">Online</span>
            </p>
            <code className="mk-bsc-url-value">https://www.datawrap.io/api/v1/mcp</code>
            <p className="mk-bsc-muted">
              Add the workspace API key as Authorization: Bearer. start_transfer, create_connector and create_schedule return an ack_id; confirm_action finishes the change.
            </p>
          </div>
          <span className="mk-app-tbtn mk-app-tbtn--primary" aria-hidden>
            <Check size={13} />
            Copy MCP URL
          </span>
        </div>
        <div className="mk-bsc-mcpgrid">
          <div className="mk-bsc-card">
            <div className="mk-bsc-card-head">
              <strong>Client setup</strong>
            </div>
            <ul className="mk-bsc-clients">
              {CLIENTS.map((cl) => {
                const isOpen = open === cl.name;
                return (
                  <li key={cl.name}>
                    <div className="mk-bsc-client">
                      <span className="mk-bsc-client-icon" aria-hidden>
                        <FileCode2 size={15} />
                      </span>
                      <span>
                        <strong>{cl.name}</strong>
                        <small>{cl.how}</small>
                      </span>
                      <button type="button" className="mk-bsc-btn" aria-expanded={isOpen} onClick={() => setOpen(isOpen ? null : cl.name)}>
                        {isOpen ? "Hide" : "Setup"}
                      </button>
                    </div>
                    {isOpen ? (
                      <pre className="mk-bsc-sql" tabIndex={0} aria-label={`${cl.name} configuration (example)`}>
                        <code>{cl.config}</code>
                      </pre>
                    ) : null}
                  </li>
                );
              })}
            </ul>
          </div>
          <McpActivity />
        </div>
      </div>
    </AppWindow>
  );
}

/* ══ Warehouse loading: one load, read back by the warehouse ═══════ */

const WH = {
  name: "order_lines → DW_DEMO.ORDER_LINES",
  job: "8d3c27b5",
  run: { read: 18_402_267, landed: 18_402_250, held: 17, skipped: 0 } as RunCounts,
  duration: "11m 05s",
};

const WH_CHECKS = ["Destination write access", "Staging capacity", "Staging policy", "Population fit", "Target table definition"];

function WarehouseCards() {
  return (
    <div className="mk-bsc-two">
      <div className="mk-bsc-card">
        <div className="mk-bsc-card-head">
          <strong>Trust Report</strong>
          <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
          <span className="mk-bsc-grow" />
          <span className="mk-bsc-muted">Check: Full checksum</span>
        </div>
        <dl className="mk-bsc-kv">
          <div>
            <dt>Rows read at the source</dt>
            <dd>{fmt.format(WH.run.read)}</dd>
          </div>
          <div>
            <dt>Rows in the destination, counted by Snowflake</dt>
            <dd>{fmt.format(WH.run.landed)}</dd>
          </div>
          <div>
            <dt>Rows that differ</dt>
            <dd>0</dd>
          </div>
        </dl>
        <p className="mk-bsc-card-foot">
          Every landed row was compared and none differs. {WH.run.held} rows are held with a reason: <code>units</code> is NULL; NOT NULL in the destination.
        </p>
      </div>
      <div className="mk-bsc-card">
        <div className="mk-bsc-card-head">
          <strong>Checked before the write</strong>
          <span className="mk-app-pill mk-app-pill--completed">Passed</span>
        </div>
        <ul className="mk-bsc-checks">
          {WH_CHECKS.map((c) => (
            <li key={c}>
              <Check size={13} aria-hidden />
              <span>{c}</span>
              <small>Passed</small>
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
}

function WarehouseInner({ caption, showCaption }: SceneOptions = {}) {
  const narrow = useNarrow();
  const c = cap(caption, "Example load. Column types follow a recorded run; table names and row counts are illustrative.", showCaption);
  const head = (
    <div className="mk-bsc-card mk-bsc-runcard">
      <div className="mk-bsc-card-head">
        <strong>{WH.name}</strong>
        <span className="mk-app-pill mk-app-pill--completed">Completed</span>
        <span className="mk-bsc-grow" />
        <span className="mk-bsc-muted">
          Full append, {WH.duration}, job <code>{WH.job}</code>
        </span>
      </div>
      <p className="mk-bsc-route">
        <Conn id={SRC.id} name={SRC.name} />
        <ArrowRight size={13} aria-hidden />
        <Conn id={DST.id} name={DST.name} />
      </p>
      <RunEquation run={WH.run} />
      <div className="mk-bsc-bar" aria-hidden>
        <i style={{ flexGrow: 99 }} />
        <i className="is-held" style={{ flexGrow: 1 }} />
      </div>
    </div>
  );
  if (narrow) {
    return (
      <PanelWindow path={`jobs/${WH.job}`} caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>
          {head}
          <WarehouseCards />
        </div>
      </PanelWindow>
    );
  }
  return (
    <AppWindow path={`jobs/${WH.job}`} active="jobs" caption={c.text} className={`mk-bsc-wh${c.cls}`}>
      <div className="mk-bsc-stack">
        {head}
        <WarehouseCards />
      </div>
    </AppWindow>
  );
}

/* ══ Recurring sync: one schedule's runs, each with its statement ══ */

type SyncRun = { at: string; run?: RunCounts; note: ReactNode; job?: string; first?: boolean };

const SYNC_RUNS: SyncRun[] = [
  { at: "Today 01:40", run: { read: 2_418_550, landed: 2_418_550, held: 0, skipped: 0 }, note: "First run, full load: map reviewed, every check passed", job: "5d20b7c4", first: true },
  { at: "Today 02:40", run: { read: 38, landed: 38, held: 0, skipped: 0 }, note: "Same checks, same read-back", job: "5d2a1e07" },
  {
    at: "Today 03:40",
    run: { read: 41, landed: 40, held: 1, skipped: 0 },
    note: (
      <>
        1 held: <code>email</code> is NULL; NOT NULL in the destination
      </>
    ),
    job: "5d33c9a4",
  },
  { at: "Today 04:40", run: { read: 27, landed: 27, held: 0, skipped: 0 }, note: "Same checks, same read-back", job: "5d3d70f2" },
  {
    at: "Today 05:40",
    note: (
      <>
        Paused before writing: <code>loyalty_tier</code> appeared at the source and waits in Map
      </>
    ),
  },
  {
    at: "Today 06:40",
    run: { read: 64, landed: 62, held: 0, skipped: 2 },
    note: (
      <>
        Map reviewed. 2 skipped by the filter <code>is_test = true</code>
      </>
    ),
    job: "5d50be61",
  },
  { at: "Today 07:40", run: { read: 33, landed: 33, held: 0, skipped: 0 }, note: "Same checks, same read-back", job: "5d5a6593" },
];

function SyncTable({ compact }: { compact?: boolean }) {
  return (
    <table className={`mk-bsc-table mk-bsc-table--sync${compact ? " is-compact" : ""}`}>
      <caption className="mk-sr-only">Runs of the customers schedule, each with its statement (example)</caption>
      <thead>
        <tr>
          <th scope="col">Run</th>
          <th scope="col" className="is-num">
            Read
          </th>
          <th scope="col" className="is-num">
            Landed
          </th>
          <th scope="col" className="is-num">
            Held
          </th>
          <th scope="col" className="is-num">
            Skipped
          </th>
          {compact ? null : <th scope="col">What happened</th>}
        </tr>
      </thead>
      <tbody>
        {SYNC_RUNS.map((r) =>
          r.run ? (
            <tr key={r.at} className={r.first ? "is-first" : undefined}>
              <th scope="row">
                {r.at.replace("Today ", "")}
                {compact ? null : (
                  <small>
                    <code>{r.job}</code>
                  </small>
                )}
              </th>
              <td className="is-num">{fmt.format(r.run.read)}</td>
              <td className="is-num">{fmt.format(r.run.landed)}</td>
              <td className={`is-num${r.run.held ? " is-held" : ""}`}>{r.run.held}</td>
              <td className="is-num">{r.run.skipped}</td>
              {compact ? null : <td className="mk-bsc-muted">{r.note}</td>}
            </tr>
          ) : (
            <tr key={r.at} className="is-paused">
              <th scope="row">{r.at.replace("Today ", "")}</th>
              <td colSpan={compact ? 4 : 5}>
                <span className="mk-app-pill mk-app-pill--v-not-run">Paused</span> {compact ? <code>loyalty_tier</code> : r.note}
              </td>
            </tr>
          ),
        )}
      </tbody>
    </table>
  );
}

function SyncInner({ caption, showCaption }: SceneOptions = {}) {
  const narrow = useNarrow();
  const c = cap(caption, "Example schedule. Each run is a job with its own statement: read = landed + held + skipped.", showCaption);
  const head = (
    <div className="mk-bsc-synchead">
      <p className="mk-bsc-panel-title">
        <strong>customers → DW_DEMO.CUSTOMERS</strong>
        <span className="mk-app-pill mk-app-pill--completed">Active</span>
      </p>
      <p className="mk-bsc-route">
        <Conn id="mysql" name="Customers MySQL (demo)" />
        <ArrowRight size={13} aria-hidden />
        <Conn id={DST.id} name={DST.name} />
        <span className="mk-bsc-muted">Hourly, incremental deduped</span>
      </p>
    </div>
  );
  if (narrow) {
    return (
      <PanelWindow path="schedules/customers" caption={c.text} designWidth={PHONE_DESIGN}>
        <div className={`mk-bsc-panel${c.cls}`}>
          {head}
          <SyncTable compact />
        </div>
      </PanelWindow>
    );
  }
  return (
    <AppWindow path="schedules/customers" active="schedules" caption={c.text} className={`mk-bsc-sync${c.cls}`}>
      <div className="mk-bsc-stack">
        <div className="mk-bsc-card">
          {head}
          <div className="mk-app-drawer-tabs" aria-hidden>
            <span>Overview</span>
            <span>
              Schema <em>9</em>
            </span>
            <span className="is-active">
              History <em>{SYNC_RUNS.length}</em>
            </span>
            <span>Config</span>
          </div>
          <SyncTable />
        </div>
      </div>
    </AppWindow>
  );
}

/* ══ Exports: each scene renders a whole AppWindow, with no required props ══ */

/** Transfer Studio: the step rail with Map, Validate and Run you can switch between. */
export function TransferScene(props: SceneOptions & { initial?: StudioStep; migration?: boolean; example?: "transfer" | "migration" } = {}) {
  return (
    <MarksOn.Provider value={props.marks === true}>
      <TransferInner {...props} />
    </MarksOn.Provider>
  );
}

/** Schedules: the list, a schedule paused on drift, and its run history. */
export function SchedulesScene(props: SceneOptions & { initial?: string | null } = {}) {
  return (
    <MarksOn.Provider value={props.marks === true}>
      <SchedulesInner {...props} />
    </MarksOn.Provider>
  );
}

/** Query: the editor and its result grid; switch sides, or try a write. */
export function QueryScene(props: SceneOptions & { example?: "customers" } = {}) {
  return (
    <MarksOn.Provider value={props.marks === true}>
      <QueryInner {...props} />
    </MarksOn.Provider>
  );
}

/** Pilot: a question, the answer with its SQL, and a change staged for Confirm. */
export function PilotScene(props: SceneOptions & { example?: "users" } = {}) {
  return (
    <MarksOn.Provider value={props.marks === true}>
      <PilotInner {...props} />
    </MarksOn.Provider>
  );
}

/** MCP: the server URL, client setup you can open, and the tool-call log. */
export function McpScene(props: SceneOptions = {}) {
  return (
    <MarksOn.Provider value={props.marks === true}>
      <McpInner {...props} />
    </MarksOn.Provider>
  );
}

/** Warehouse loading: one load, its statement, and the warehouse's read-back. */
export function WarehouseScene(props: SceneOptions = {}) {
  return <WarehouseInner {...props} />;
}

/** Recurring sync: one schedule's runs, each with its own statement. */
export function SyncScene(props: SceneOptions = {}) {
  return <SyncInner {...props} />;
}

/* ══ Jobs: the kit's grid and Trust Report, re-exported for the pages ══ */

export { JobsWithTrust } from "./app-frame/AppFrame";
