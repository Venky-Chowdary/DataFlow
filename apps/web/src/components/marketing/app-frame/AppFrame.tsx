import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import {
  ArrowLeftRight,
  BookOpen,
  Boxes,
  CalendarClock,
  CircleHelp,
  Cpu,
  Download,
  FileCheck2,
  FileSignature,
  Layers,
  LayoutGrid,
  LogOut,
  Play,
  Plug,
  Plus,
  RefreshCw,
  Search,
  Settings,
  ShieldCheck,
  Sparkles,
  TriangleAlert,
  Undo2,
  X,
} from "lucide-react";
import { ConnectorIcon } from "../../../app/brand-icons";
import { jobStatusLabel } from "../../../lib/uiUtils";
import { DtLogo } from "../../DtLogo";
import { Breakable } from "../StatementHero";

/**
 * AppFrame: the Datawrap workspace, rendered in code for the public site.
 *
 * It mirrors the real app (sidebar groups, ⌘K bar, Jobs grid, Trust Report
 * drawer) in the product's own theme, so the site shows the product rather
 * than a drawing of it. Every figure is example data from one workspace and
 * adds up; the caption under each frame says so.
 *
 * Product UI is never shrunk to fit. A desktop window renders at 1:1 and
 * reflows to its container; below the narrowest width its layout holds, it
 * crops at the right edge instead of scaling its text down. Phones get one
 * panel at full size (PanelWindow). Nothing inside renders below 12px.
 *
 * Statuses and verdicts use the product's own words: job states from
 * `jobStatusLabel` (lib/uiUtils), verdicts by their API names (proven,
 * explained, no_difference_found, unexplained, unverifiable).
 */

export type AppNavId =
  | "overview"
  | "transfer"
  | "connectors"
  | "contracts"
  | "exit"
  | "jobs"
  | "schedules"
  | "transforms"
  | "library"
  | "query"
  | "pilot"
  | "settings"
  | "mcp"
  | "help"
  | "proofs";

const NAV: { group: string; items: { id: AppNavId; label: string; icon: ReactNode; count?: string }[] }[] = [
  {
    group: "Platform",
    items: [
      { id: "overview", label: "Overview", icon: <LayoutGrid size={15} /> },
      { id: "transfer", label: "Transfer", icon: <ArrowLeftRight size={15} /> },
      { id: "connectors", label: "Connectors", icon: <Plug size={15} />, count: "12" },
      { id: "contracts", label: "Contracts", icon: <FileCheck2 size={15} /> },
      { id: "exit", label: "Exit readiness", icon: <Undo2 size={15} /> },
    ],
  },
  {
    group: "Operations",
    items: [
      { id: "jobs", label: "Jobs", icon: <Boxes size={15} />, count: "1,284" },
      { id: "schedules", label: "Schedules", icon: <CalendarClock size={15} /> },
      { id: "transforms", label: "Transforms", icon: <Layers size={15} /> },
      { id: "library", label: "Library", icon: <BookOpen size={15} /> },
      { id: "query", label: "Query", icon: <Search size={15} /> },
      { id: "pilot", label: "Pilot", icon: <Sparkles size={15} /> },
    ],
  },
  {
    group: "System",
    items: [
      { id: "settings", label: "Settings", icon: <Settings size={15} /> },
      { id: "mcp", label: "MCP", icon: <Cpu size={15} /> },
      { id: "help", label: "Help", icon: <CircleHelp size={15} /> },
      { id: "proofs", label: "Proofs", icon: <ShieldCheck size={15} /> },
    ],
  },
];

const NAV_TITLE: Record<AppNavId, string> = Object.fromEntries(
  NAV.flatMap((g) => g.items.map((i) => [i.id, i.label] as const)),
) as Record<AppNavId, string>;

/** Scale a fixed-width layout to its container's width. */
function useFitScale(designWidth: number, maxScale = 1) {
  const outer = useRef<HTMLDivElement | null>(null);
  const inner = useRef<HTMLDivElement | null>(null);
  const [fit, setFit] = useState({ scale: 1, height: 0 });

  useLayoutEffect(() => {
    const measure = () => {
      const o = outer.current;
      const i = inner.current;
      if (!o || !i) return;
      const scale = Math.min(maxScale, o.clientWidth / designWidth);
      const height = Math.round(i.offsetHeight * scale);
      setFit((prev) => (prev.scale === scale && prev.height === height ? prev : { scale, height }));
    };
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(measure);
    if (outer.current) ro.observe(outer.current);
    if (inner.current) ro.observe(inner.current);
    return () => ro.disconnect();
  }, [designWidth, maxScale]);

  return { outer, inner, ...fit };
}

function WindowChrome({ path }: { path: string }) {
  return (
    <div className="mk-app-chrome" aria-hidden>
      <span className="mk-app-dots">
        <i />
        <i />
        <i />
      </span>
      <span className="mk-app-url">
        <ShieldCheck size={12} />
        app.datawrap.io/{path}
      </span>
      <span className="mk-app-chrome-end" />
    </div>
  );
}

function Sidebar({ active, collapsed }: { active: AppNavId; collapsed?: boolean }) {
  return (
    <nav className={`mk-app-side${collapsed ? " is-collapsed" : ""}`} aria-label="Workspace navigation (example)">
      <div className="mk-app-brand">
        <DtLogo size={24} title="" />
        {!collapsed ? <span>Datawrap</span> : null}
      </div>
      {NAV.map((g) => (
        <div key={g.group} className="mk-app-side-group">
          {!collapsed ? <p className="mk-app-side-label">{g.group}</p> : null}
          <ul>
            {g.items.map((item) => (
              <li key={item.id} className={item.id === active ? "is-active" : undefined}>
                {item.icon}
                {!collapsed ? <span>{item.label}</span> : null}
                {!collapsed && item.count ? <em>{item.count}</em> : null}
              </li>
            ))}
          </ul>
        </div>
      ))}
      {!collapsed ? (
        <div className="mk-app-side-foot">
          <span className="mk-app-avatar" aria-hidden>
            D
          </span>
          <span className="mk-app-side-who">
            Data platform
            <small>Example workspace</small>
          </span>
          <LogOut size={14} aria-hidden />
        </div>
      ) : null}
    </nav>
  );
}

function Topbar({ active }: { active: AppNavId }) {
  return (
    <header className="mk-app-top">
      <div className="mk-app-crumb">
        <small>Workspace</small>
        <strong>{NAV_TITLE[active]}</strong>
      </div>
      <div className="mk-app-search">
        <Search size={14} aria-hidden />
        <span>Search pages, connections, schedules, jobs</span>
        <kbd>⌘K</kbd>
      </div>
      <div className="mk-app-top-actions">
        <span className="mk-app-tbtn">
          <TriangleAlert size={14} aria-hidden />
          Status
        </span>
        <span className="mk-app-tbtn">
          <Sparkles size={14} aria-hidden />
          Pilot
        </span>
        <span className="mk-app-tbtn mk-app-tbtn--primary">
          <Plus size={14} aria-hidden />
          New transfer
        </span>
      </div>
    </header>
  );
}

/** The widest container a marketing page gives a frame (--mk-measure). */
const WIDEST_FRAME = 1200;

/** The narrowest width the desktop scenes reflow to before they crop. */
export const APP_MIN_LAYOUT = 960;

/** True while the container is narrower than the layout's minimum width. */
function useCropped(minWidth: number) {
  const outer = useRef<HTMLDivElement | null>(null);
  const [cropped, setCropped] = useState(false);
  useLayoutEffect(() => {
    const measure = () => {
      const o = outer.current;
      if (!o) return;
      setCropped(o.clientWidth > 0 && o.clientWidth < minWidth);
    };
    measure();
    if (typeof ResizeObserver === "undefined") return;
    const ro = new ResizeObserver(measure);
    if (outer.current) ro.observe(outer.current);
    return () => ro.disconnect();
  }, [minWidth]);
  return { outer, cropped };
}

export interface AppWindowProps {
  /** URL path shown in the address bar, after app.datawrap.io/ */
  path: string;
  active: AppNavId;
  children: ReactNode;
  /** Caption under the frame; say that the data is an example. */
  caption: string;
  /**
   * The narrowest width the layout reflows to. The window fills its container
   * at 1:1; in a narrower container it keeps this width and crops at the right
   * edge. Never scales text down. Capped at the widest page container (1200).
   */
  designWidth?: number;
  /** Narrow icon sidebar for denser scenes. */
  collapsedNav?: boolean;
  /** Overlay rendered above the page content, inside the window (a drawer). */
  overlay?: ReactNode;
  className?: string;
}

export function AppWindow({
  path,
  active,
  children,
  caption,
  designWidth = APP_MIN_LAYOUT,
  collapsedNav,
  overlay,
  className = "",
}: AppWindowProps) {
  const minWidth = Math.min(designWidth, WIDEST_FRAME);
  const { outer, cropped } = useCropped(minWidth);
  return (
    <figure className={`mk-app-figure${cropped ? " is-cropped" : ""} ${className}`.trim()}>
      <div className="mk-app-fit" ref={outer}>
        <div className="mk-app-window mk-app-window--desk" style={{ minWidth }}>
          <WindowChrome path={path} />
          <div className={`mk-app-shell${collapsedNav ? " is-collapsed" : ""}`}>
            <Sidebar active={active} collapsed={collapsedNav} />
            <div className="mk-app-main">
              <Topbar active={active} />
              <div className="mk-app-content">{children}</div>
            </div>
            {overlay}
          </div>
        </div>
      </div>
      <figcaption className="mk-app-caption">{caption}</figcaption>
    </figure>
  );
}

/* ── Example data: one workspace, figures that add up ───────────── */

/** Job lifecycle states, as the API names them (lib/types JobStatus). */
export type ExampleJobState = "running" | "completed" | "completed_with_quarantine" | "failed";

/** Parity verdicts, as the API names them. */
export type ExampleVerdict = "proven" | "explained" | "no_difference_found" | "unexplained" | "unverifiable";

/** What the site calls each verdict (the home page's verdict scale). */
export const VERDICT_NAME: Record<ExampleVerdict, string> = {
  proven: "Proven equal",
  explained: "Explained",
  no_difference_found: "No difference found",
  unexplained: "Unexplained",
  unverifiable: "Unverifiable",
};

type Tone = "proven" | "explained" | "open" | "none";

const VERDICT_TONE: Record<ExampleVerdict, Tone> = {
  proven: "proven",
  explained: "explained",
  no_difference_found: "explained",
  unexplained: "open",
  unverifiable: "none",
};

export interface ExampleJob {
  id: string;
  name: string;
  dest: string;
  from: { id: string; label: string };
  to: { id: string; label: string };
  mode: string;
  rows: number;
  held?: number;
  state: ExampleJobState;
  verdict: ExampleVerdict;
  duration: string;
  started: string;
  /**
   * Trust Report numbers for the drawer. `compared` is how many landed rows
   * the check compared; `verdict` and `tone` are derived from the job's verdict.
   */
  trust: { source: number; destination: number; compared: number; differ: number; check: string; verdict: string; tone: Tone; note: string };
}

type JobInput = Omit<ExampleJob, "trust"> & { trust: Omit<ExampleJob["trust"], "verdict" | "tone"> };

const withVerdict = (j: JobInput): ExampleJob => ({
  ...j,
  trust: { ...j.trust, verdict: VERDICT_NAME[j.verdict], tone: VERDICT_TONE[j.verdict] },
});

export const EXAMPLE_JOBS: ExampleJob[] = ([
  {
    id: "7c41e9a2",
    name: "orders",
    dest: "ANALYTICS.ORDERS_LIVE",
    from: { id: "postgresql", label: "PostgreSQL" },
    to: { id: "snowflake", label: "Snowflake" },
    mode: "CDC",
    rows: 1_204_880,
    state: "running",
    verdict: "unverifiable",
    duration: "12m 08s",
    started: "Today 09:12",
    trust: { source: 1_204_880, destination: 1_204_880, compared: 0, differ: 0, check: "Running total", note: "The run is still applying changes. Nothing is compared until it settles, so there is no verdict to sign yet." },
  },
  {
    id: "6af912d7",
    name: "sales_orders",
    dest: "DW_DEMO.SALES_ORDERS_REPORTING",
    from: { id: "postgresql", label: "PostgreSQL" },
    to: { id: "snowflake", label: "Snowflake" },
    mode: "Full append",
    rows: 48_213_871,
    held: 33,
    state: "completed_with_quarantine",
    verdict: "proven",
    duration: "41m 37s",
    started: "Today 02:03",
    trust: { source: 48_213_906, destination: 48_213_871, compared: 48_213_871, differ: 0, check: "Full checksum", note: "Every landed row was compared and none differs. 33 rows are held in quarantine with a reason and 2 were skipped by a filter: 48,213,906 read = 48,213,871 landed + 33 held + 2 skipped." },
  },
  {
    id: "5d20b7c4",
    name: "customers",
    dest: "DW_DEMO.CUSTOMERS",
    from: { id: "mysql", label: "MySQL" },
    to: { id: "snowflake", label: "Snowflake" },
    mode: "Incremental deduped",
    rows: 2_418_550,
    state: "completed",
    verdict: "explained",
    duration: "6m 52s",
    started: "Today 01:40",
    trust: { source: 2_418_550, destination: 2_418_550, compared: 2_418_550, differ: 1_912, check: "Key diff", note: "1,912 rows differ only in updated_at precision: microseconds in MySQL, milliseconds after the cast you approved." },
  },
  {
    id: "4b88c1f0",
    name: "events",
    dest: "raw.events",
    from: { id: "mongodb", label: "MongoDB" },
    to: { id: "bigquery", label: "BigQuery" },
    mode: "Full append",
    rows: 9_046_118,
    state: "completed",
    verdict: "no_difference_found",
    duration: "18m 03s",
    started: "Yesterday 23:10",
    trust: { source: 9_046_118, destination: 9_046_118, compared: 40_000, differ: 0, check: "Random sample", note: "Values were compared on a random sample of 40,000 rows. The report gives an upper bound on unexplained differences; a sample is not proof." },
  },
  {
    id: "3e71aa58",
    name: "invoices.csv",
    dest: "finance.invoices",
    from: { id: "csv", label: "CSV" },
    to: { id: "postgresql", label: "PostgreSQL" },
    mode: "Full overwrite",
    rows: 0,
    state: "failed",
    verdict: "unverifiable",
    duration: "4s",
    started: "Yesterday 18:22",
    trust: { source: 0, destination: 0, compared: 0, differ: 0, check: "None", note: "Validate refused the plan: amount is DECIMAL(12,2) at the source and INTEGER in finance.invoices. Nothing was written." },
  },
  {
    id: "2c09d6e3",
    name: "users",
    dest: "analytics.users",
    from: { id: "mongodb", label: "MongoDB" },
    to: { id: "postgresql", label: "PostgreSQL" },
    mode: "Incremental append",
    rows: 912_004,
    state: "completed",
    verdict: "proven",
    duration: "3m 11s",
    started: "Yesterday 14:05",
    trust: { source: 912_004, destination: 912_004, compared: 912_004, differ: 0, check: "Full checksum", note: "Every row was compared and none differs." },
  },
] satisfies JobInput[]).map(withVerdict);

const fmt = new Intl.NumberFormat("en-US");

/** The product's own status label (lib/uiUtils), e.g. "Completed with quarantine". */
export function exampleStateLabel(state: ExampleJobState): string {
  return jobStatusLabel(state);
}

/** The grid's one-word status, as the product's Jobs grid shortens it. */
function stateShort(state: ExampleJobState): string {
  return state === "completed_with_quarantine" ? "Quarantined" : jobStatusLabel(state);
}

/** Pill colour per state: amber for a run that finished with rows held. */
const STATE_PILL: Record<ExampleJobState, string> = {
  running: "mk-app-pill--running",
  completed: "mk-app-pill--completed",
  completed_with_quarantine: "mk-app-pill--quarantine",
  failed: "mk-app-pill--failed",
};

/** "Needs attention", by the product's rule: a failed run or a failed check. */
const needsAttention = (j: ExampleJob) => j.state === "failed" || j.verdict === "unexplained";

/** Finished runs, by the product's rule (completed with quarantine is a success). */
const isFinished = (j: ExampleJob) => j.state === "completed" || j.state === "completed_with_quarantine";

/** "1 explained, 1 sampled, 1 refused, 1 running": every run that is not proven. */
function notProvenSummary(jobs: ExampleJob[]): string {
  const n = (f: (j: ExampleJob) => boolean) => jobs.filter(f).length;
  const parts: [number, string][] = [
    [n((j) => j.verdict === "explained"), "explained"],
    [n((j) => j.verdict === "no_difference_found"), "sampled"],
    [n((j) => j.verdict === "unexplained"), "unexplained"],
    [n((j) => j.verdict === "unverifiable" && j.state === "failed"), "refused"],
    [n((j) => j.verdict === "unverifiable" && j.state === "running"), "running"],
  ];
  return parts
    .filter(([c]) => c > 0)
    .map(([c, w]) => `${c} ${w}`)
    .join(", ");
}

/** The Jobs KPIs, computed from the example runs (never typed in). */
export function exampleJobKpis(jobs: ExampleJob[] = EXAMPLE_JOBS) {
  const running = jobs.filter((j) => j.state === "running");
  const today = jobs.filter((j) => j.started.startsWith("Today") && isFinished(j));
  const attention = jobs.filter(needsAttention);
  return {
    running,
    proven: jobs.filter((j) => j.verdict === "proven").length,
    total: jobs.length,
    notProven: notProvenSummary(jobs),
    writtenToday: today.reduce((s, j) => s + j.rows, 0),
    finishedToday: today.length,
    attention,
  };
}

function Route({ job }: { job: ExampleJob }) {
  return (
    <span className="mk-app-route">
      <span className="mk-app-chip" title={job.from.label}>
        <ConnectorIcon id={job.from.id} size={14} />
        <span className="mk-app-route-name">{job.from.label}</span>
      </span>
      <span aria-hidden className="mk-app-route-arrow">
        →
      </span>
      <span className="mk-app-chip" title={job.to.label}>
        <ConnectorIcon id={job.to.id} size={14} />
        <span className="mk-app-route-name">{job.to.label}</span>
      </span>
    </span>
  );
}

type Tab = "all" | "running" | "attention" | "completed";

/**
 * The Jobs grid. The filter tabs work, and choosing a row opens its Trust
 * Report drawer, the same way the product does.
 */
export function JobsScene({
  onOpen,
  openId,
}: {
  onOpen?: (id: string) => void;
  openId?: string | null;
}) {
  const [tab, setTab] = useState<Tab>("all");
  const inTab = (t: Tab) => (j: ExampleJob) =>
    t === "all" ? true : t === "running" ? j.state === "running" : t === "attention" ? needsAttention(j) : isFinished(j);
  const rows = EXAMPLE_JOBS.filter(inTab(tab));
  const count = (t: Tab) => EXAMPLE_JOBS.filter(inTab(t)).length;
  const k = exampleJobKpis();
  const live = k.running[0];
  const stuck = k.attention[0];

  return (
    <div className="mk-app-jobs">
      <div className="mk-app-kpis">
        <div className="mk-app-kpi">
          <small>Running now</small>
          <strong>{k.running.length}</strong>
          <span>{live ? `${live.name}, ${live.mode} into ${live.to.label}` : "Nothing running"}</span>
        </div>
        <div className="mk-app-kpi">
          <small>Proven equal, last 7 days</small>
          <strong className="is-good">
            {k.proven} of {k.total}
          </strong>
          <span>{k.notProven}</span>
        </div>
        <div className="mk-app-kpi">
          <small>Rows written today</small>
          <strong>{fmt.format(k.writtenToday)}</strong>
          <span>across {k.finishedToday} finished runs</span>
        </div>
        <div className={`mk-app-kpi${k.attention.length ? " is-attention" : ""}`}>
          <small>Needs attention</small>
          <strong className={k.attention.length ? "is-bad" : undefined}>{k.attention.length}</strong>
          <span>
            {stuck ? `${stuck.name}, ${stuck.state === "failed" ? "refused before writing" : "a difference has no known cause"}` : "Nothing failed"}
          </span>
        </div>
      </div>

      <div className="mk-app-toolbar">
        <div className="mk-app-tabs" role="tablist" aria-label="Filter jobs (example)">
          {(
            [
              ["all", "All"],
              ["running", "Running"],
              ["attention", "Needs attention"],
              ["completed", "Completed"],
            ] as const
          ).map(([id, label]) => (
            <button
              key={id}
              type="button"
              role="tab"
              aria-selected={tab === id}
              className={tab === id ? "is-active" : undefined}
              onClick={() => setTab(id)}
            >
              {label} <em>{count(id)}</em>
            </button>
          ))}
        </div>
        <div className="mk-app-toolbar-actions" aria-hidden>
          <span className="mk-app-tbtn">
            <RefreshCw size={13} />
            Refresh
          </span>
          <span className="mk-app-tbtn">
            <Play size={13} />
            Re-run
          </span>
          <span className="mk-app-tbtn">
            <Download size={13} />
            Export CSV
          </span>
        </div>
      </div>

      <div className="mk-app-table" role="table" aria-label="Jobs (example workspace)">
        <div className="mk-app-tr mk-app-th" role="row">
          <span role="columnheader">Status</span>
          <span role="columnheader">Job</span>
          <span role="columnheader">Route</span>
          <span role="columnheader" className="is-num">
            Rows landed
          </span>
          <span role="columnheader">Verdict</span>
        </div>
        {rows.map((j) => (
          <button
            key={j.id}
            type="button"
            role="row"
            className={`mk-app-tr${openId === j.id ? " is-open" : ""}`}
            onClick={() => onOpen?.(j.id)}
            aria-label={`Open the Trust Report for ${j.name} to ${j.dest}`}
          >
            <span role="cell">
              <span className={`mk-app-pill ${STATE_PILL[j.state]}`} title={exampleStateLabel(j.state)}>
                {stateShort(j.state)}
              </span>
            </span>
            <span role="cell" className="mk-app-job">
              <strong>
                {j.name} → {j.dest}
              </strong>
              <span className="mk-app-job-meta">
                <code>{j.id}</code>
                <span>
                  {j.mode}, {j.started.toLowerCase()}
                </span>
              </span>
            </span>
            <span role="cell">
              <Route job={j} />
            </span>
            <span role="cell" className="is-num">
              {fmt.format(j.rows)}
              {j.held ? <small className="mk-app-held">{j.held} held</small> : null}
            </span>
            <span role="cell">
              <span className={`mk-app-pill mk-app-pill--t-${j.trust.tone}`}>{j.trust.verdict}</span>
            </span>
          </button>
        ))}
      </div>
    </div>
  );
}

/** The verdict ring: the share of compared rows that are proven equal. */
function Ring({ value, tone }: { value: number; tone: ExampleJob["trust"]["tone"] }) {
  const r = 52;
  const c = 2 * Math.PI * r;
  return (
    <svg className={`mk-app-ring mk-app-ring--${tone}`} viewBox="0 0 128 128" aria-hidden>
      <circle cx="64" cy="64" r={r} className="mk-app-ring-track" />
      <circle
        cx="64"
        cy="64"
        r={r}
        className="mk-app-ring-value"
        strokeDasharray={`${(c * value) / 100} ${c}`}
        transform="rotate(-90 64 64)"
      />
    </svg>
  );
}

/** The Trust Report drawer for one job, over the Jobs grid. */
export function TrustDrawer({ job, onClose }: { job: ExampleJob; onClose?: () => void }) {
  const t = job.trust;
  // The ring is the share of landed rows the check compared and found equal
  // (differences with a known cause excluded). A sample shows how little it
  // covered; nothing compared shows no figure at all.
  const equal = Math.max(0, t.compared - t.differ);
  const pct = t.destination === 0 ? 0 : (equal / t.destination) * 100;
  const shown = t.compared === 0 ? "—" : `${equal === t.destination ? "100" : pct.toFixed(1)}%`;
  const ringLabel =
    t.compared === 0 ? "not compared" : job.verdict === "no_difference_found" ? "rows sampled" : "rows equal";
  return (
    <aside className="mk-app-drawer" aria-label={`Trust Report for ${job.name} (example)`}>
      <div className="mk-app-drawer-head">
        <div>
          <strong>
            <Breakable id={job.name} /> → <Breakable id={job.dest} />
          </strong>
          <span className={`mk-app-pill ${STATE_PILL[job.state]}`}>{exampleStateLabel(job.state)}</span>
          <p>
            {job.from.label} to {job.to.label}, {job.mode.toLowerCase()}
          </p>
        </div>
        {onClose ? (
          <button type="button" className="mk-app-icon-btn" onClick={onClose} aria-label="Close the Trust Report">
            <X size={16} />
          </button>
        ) : null}
      </div>
      <div className="mk-app-drawer-tabs" aria-hidden>
        <span>Detail</span>
        <span>
          Mapping <em>11</em>
        </span>
        <span className="is-active">
          <ShieldCheck size={13} />
          Trust Report
        </span>
        <span>Reversibility</span>
        <span>
          Log <em>30</em>
        </span>
      </div>
      <div className="mk-app-drawer-body">
        <p className="mk-app-verdict">
          <span className={`mk-app-pill mk-app-pill--t-${t.tone}`}>{t.verdict}</span>
          <span>Check: {t.check}</span>
        </p>
        <div className="mk-app-trust-grid">
          <div className="mk-app-ring-box">
            <Ring value={t.compared === 0 ? 0 : pct} tone={t.tone} />
            <span className="mk-app-ring-label">
              <strong>{shown}</strong>
              {ringLabel}
            </span>
          </div>
          <dl className="mk-app-counts">
            <div>
              <dt>Rows read at the source</dt>
              <dd>{fmt.format(t.source)}</dd>
            </div>
            <div>
              <dt>Rows landed in the destination</dt>
              <dd>{fmt.format(t.destination)}</dd>
            </div>
            <div>
              <dt>Rows compared</dt>
              <dd>{fmt.format(t.compared)}</dd>
            </div>
            <div>
              <dt>Rows that differ</dt>
              <dd className={t.differ ? "is-warn" : undefined}>{fmt.format(t.differ)}</dd>
            </div>
          </dl>
        </div>
        <p className="mk-app-note">{t.note}</p>
        <div className="mk-app-drawer-actions" aria-hidden>
          <span className="mk-app-tbtn">
            <Download size={13} />
            Report (.md)
          </span>
          <span className="mk-app-tbtn mk-app-tbtn--primary">
            <FileSignature size={13} />
            Go to sign-off
          </span>
        </div>
      </div>
    </aside>
  );
}

/** Single-panel crops are laid out at this width and reflow below it. */
const PANEL_DESIGN = 460;

/** True when the viewport matches `query` (phone-sized by default). */
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

/**
 * A single product panel in window chrome, for phones: a crop of the
 * workspace at a legible scale rather than the whole workspace shrunk.
 */
export function PanelWindow({
  path,
  caption,
  children,
  designWidth = PANEL_DESIGN,
  maxScale = 1,
  fill,
}: {
  path: string;
  /** Empty when a neighbouring element already labels the example. */
  caption: string;
  children: ReactNode;
  designWidth?: number;
  /** Allow a small panel to grow to fill a wider slot (vector, so it stays crisp). */
  maxScale?: number;
  /** Always reflow at 1:1 to the slot's width (phones and tablets); the slot caps it. */
  fill?: boolean;
}) {
  const { outer, inner, scale, height } = useFitScale(designWidth, maxScale);
  // Narrower than the design: reflow at full size instead of shrinking the
  // text below legibility (phones). Wider: scale to fill, as before.
  const fluid = fill || scale < 1;
  return (
    <figure
      className={`mk-app-figure mk-app-figure--panel${fluid ? " is-fluid" : ""}${fill ? " is-fill" : ""}`}
      aria-label={caption || "Example workspace panel"}
    >
      <div className="mk-app-fit" ref={outer} style={{ height: fluid ? undefined : height || undefined }}>
        <div
          className="mk-app-window"
          ref={inner}
          style={fluid ? undefined : { width: designWidth, transform: `scale(${scale})` }}
        >
          <WindowChrome path={path} />
          {children}
        </div>
      </div>
      {caption ? <figcaption className="mk-app-caption">{caption}</figcaption> : null}
    </figure>
  );
}

/** The width below which scenes show one panel instead of the whole workspace. */
export const APP_NARROW_QUERY = "(max-width: 1023px)";

/**
 * Jobs grid with a live Trust Report drawer. On desktop the grid opens whole,
 * with no drawer over it; choosing a row opens that run's Trust Report. On
 * phones and tablets the frame shows one run's Trust Report at full size,
 * with a run picker above it; `initial` is the run it starts on.
 */
export function JobsWithTrust({ caption, initial = "6af912d7" }: { caption: string; initial?: string | null }) {
  const narrow = useNarrow(APP_NARROW_QUERY);
  const [openId, setOpenId] = useState<string | null>(null);
  const [pickedId, setPickedId] = useState<string | null>(initial);
  const job = EXAMPLE_JOBS.find((j) => j.id === openId) ?? null;

  if (narrow) {
    const shown = EXAMPLE_JOBS.find((j) => j.id === pickedId) ?? EXAMPLE_JOBS[1];
    return (
      <div className="mk-app-phone">
        <div className="mk-app-picker" role="tablist" aria-label="Choose an example run">
          {EXAMPLE_JOBS.filter((j) => j.state !== "running").map((j) => (
            <button
              key={j.id}
              type="button"
              role="tab"
              aria-selected={shown.id === j.id}
              className={shown.id === j.id ? "is-active" : undefined}
              onClick={() => setPickedId(j.id)}
            >
              {j.name}
            </button>
          ))}
        </div>
        <PanelWindow path={`jobs/${shown.id}`} caption={caption} designWidth={PANEL_DESIGN} fill>
          <TrustDrawer job={shown} />
        </PanelWindow>
      </div>
    );
  }

  return (
    <AppWindow
      path={job ? `jobs/${job.id}` : "jobs"}
      active="jobs"
      caption={caption}
      collapsedNav
      overlay={job ? <TrustDrawer job={job} onClose={() => setOpenId(null)} /> : null}
      className={job ? "has-drawer" : undefined}
    >
      <JobsScene onOpen={setOpenId} openId={openId} />
    </AppWindow>
  );
}

/* Small pieces other scenes reuse. */
export { Route as AppRoute, Ring as AppRing };
