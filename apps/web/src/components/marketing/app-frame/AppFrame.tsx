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

/**
 * AppFrame: the Datawrap workspace, rendered in code for the public site.
 *
 * It mirrors the real app (sidebar groups, ⌘K bar, Jobs grid, Trust Report
 * drawer) in the product's own theme, so the site shows the product rather
 * than a drawing of it. Every figure is example data from one workspace and
 * adds up; the caption under each frame says so.
 *
 * The window is laid out at a fixed design width and scaled to fit its
 * container, so it stays crisp and proportionate from phone to desktop.
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
        <span className="mk-app-logo" aria-hidden />
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

export interface AppWindowProps {
  /** URL path shown in the address bar, after app.datawrap.io/ */
  path: string;
  active: AppNavId;
  children: ReactNode;
  /** Caption under the frame; say that the data is an example. */
  caption: string;
  /** Layout width before scaling. */
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
  designWidth = 1360,
  collapsedNav,
  overlay,
  className = "",
}: AppWindowProps) {
  const { outer, inner, scale, height } = useFitScale(designWidth);
  return (
    <figure className={`mk-app-figure ${className}`.trim()}>
      <div className="mk-app-fit" ref={outer} style={{ height: height || undefined }}>
        <div
          className="mk-app-window"
          ref={inner}
          style={{ width: designWidth, transform: `scale(${scale})` }}
        >
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

type JobState = "running" | "completed" | "failed";
type Verify = "verified" | "explained" | "sampled" | "verifying" | "not-run";

export interface ExampleJob {
  id: string;
  name: string;
  dest: string;
  from: { id: string; label: string };
  to: { id: string; label: string };
  mode: string;
  rows: number;
  held?: number;
  state: JobState;
  verify: Verify;
  duration: string;
  started: string;
  /** Trust Report numbers for the drawer. */
  trust: { source: number; destination: number; differ: number; check: string; verdict: string; tone: "proven" | "explained" | "open" | "none"; note: string };
}

export const EXAMPLE_JOBS: ExampleJob[] = [
  {
    id: "7c41e9a2",
    name: "orders",
    dest: "ANALYTICS.ORDERS_LIVE",
    from: { id: "postgresql", label: "PostgreSQL" },
    to: { id: "snowflake", label: "Snowflake" },
    mode: "CDC",
    rows: 1_204_880,
    state: "running",
    verify: "verifying",
    duration: "12m 08s",
    started: "Today 09:12",
    trust: { source: 1_204_880, destination: 1_204_880, differ: 0, check: "Running total", verdict: "Unverifiable yet", tone: "none", note: "The run is still applying changes. A verdict is given once it settles." },
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
    state: "completed",
    verify: "verified",
    duration: "41m 37s",
    started: "Today 02:03",
    trust: { source: 48_213_906, destination: 48_213_871, differ: 0, check: "Full checksum", verdict: "Proven equal", tone: "proven", note: "Every landed row was compared and none differs. 33 rows are held with a reason and 2 were skipped by a filter." },
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
    verify: "explained",
    duration: "6m 52s",
    started: "Today 01:40",
    trust: { source: 2_418_550, destination: 2_418_550, differ: 1_912, check: "Key diff", verdict: "Explained", tone: "explained", note: "1,912 rows differ only in updated_at precision: microseconds in MySQL, milliseconds after the cast you approved." },
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
    verify: "sampled",
    duration: "18m 03s",
    started: "Yesterday 23:10",
    trust: { source: 9_046_118, destination: 9_046_118, differ: 0, check: "Random sample", verdict: "No difference found", tone: "explained", note: "Values were compared on a random sample of 40,000 rows. The report gives an upper bound on unexplained differences." },
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
    verify: "not-run",
    duration: "4s",
    started: "Yesterday 18:22",
    trust: { source: 0, destination: 0, differ: 0, check: "None", verdict: "Unverifiable", tone: "none", note: "Validate refused the plan: amount is DECIMAL(12,2) at the source and INTEGER in finance.invoices. Nothing was written." },
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
    verify: "verified",
    duration: "3m 11s",
    started: "Yesterday 14:05",
    trust: { source: 912_004, destination: 912_004, differ: 0, check: "Full checksum", verdict: "Proven equal", tone: "proven", note: "Every row was compared and none differs." },
  },
];

const fmt = new Intl.NumberFormat("en-US");

const STATE_LABEL: Record<JobState, string> = { running: "Running", completed: "Completed", failed: "Failed" };
const VERIFY_LABEL: Record<Verify, string> = {
  verified: "Verified",
  explained: "Explained",
  sampled: "Sampled",
  verifying: "Verifying",
  "not-run": "Not run",
};

function Route({ job }: { job: ExampleJob }) {
  return (
    <span className="mk-app-route">
      <span className="mk-app-chip">
        <ConnectorIcon id={job.from.id} size={14} />
        {job.from.label}
      </span>
      <span aria-hidden className="mk-app-route-arrow">
        →
      </span>
      <span className="mk-app-chip">
        <ConnectorIcon id={job.to.id} size={14} />
        {job.to.label}
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
  const rows = EXAMPLE_JOBS.filter((j) =>
    tab === "all"
      ? true
      : tab === "running"
        ? j.state === "running"
        : tab === "attention"
          ? j.state === "failed" || (j.held ?? 0) > 0
          : j.state === "completed",
  );
  const count = (t: Tab) =>
    EXAMPLE_JOBS.filter((j) =>
      t === "all" ? true : t === "running" ? j.state === "running" : t === "attention" ? j.state === "failed" || (j.held ?? 0) > 0 : j.state === "completed",
    ).length;
  const written = EXAMPLE_JOBS.filter((j) => j.started.startsWith("Today") && j.state !== "running").reduce(
    (n, j) => n + j.rows,
    0,
  );

  return (
    <div className="mk-app-jobs">
      <div className="mk-app-kpis">
        <div className="mk-app-kpi">
          <small>Running now</small>
          <strong>1</strong>
          <span>orders, CDC into Snowflake</span>
        </div>
        <div className="mk-app-kpi">
          <small>Verified, last 7 days</small>
          <strong className="is-good">5 of 6</strong>
          <span>1 refused before writing</span>
        </div>
        <div className="mk-app-kpi">
          <small>Rows written today</small>
          <strong>{fmt.format(written)}</strong>
          <span>across 2 completed runs</span>
        </div>
        <div className="mk-app-kpi is-attention">
          <small>Need attention</small>
          <strong className="is-bad">2</strong>
          <span>1 refused plan, 33 held rows</span>
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
          <span role="columnheader">Mode</span>
          <span role="columnheader" className="is-num">
            Rows
          </span>
          <span role="columnheader">Verification</span>
          <span role="columnheader">Started</span>
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
              <span className={`mk-app-pill mk-app-pill--${j.state}`}>{STATE_LABEL[j.state]}</span>
            </span>
            <span role="cell" className="mk-app-job">
              <strong>
                {j.name} → {j.dest}
              </strong>
              <code>{j.id}</code>
            </span>
            <span role="cell">
              <Route job={j} />
            </span>
            <span role="cell" className="mk-app-mode">
              {j.mode}
            </span>
            <span role="cell" className="is-num">
              {fmt.format(j.rows)}
              {j.held ? <small className="mk-app-held">{j.held} held</small> : null}
            </span>
            <span role="cell">
              <span className={`mk-app-pill mk-app-pill--v-${j.verify}`}>{VERIFY_LABEL[j.verify]}</span>
            </span>
            <span role="cell" className="mk-app-started">
              {j.started}
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
  const pct = t.source === 0 ? 0 : Math.round(((t.destination - t.differ) / t.source) * 1000) / 10;
  const shown = t.tone === "none" ? "—" : `${pct}%`;
  return (
    <aside className="mk-app-drawer" aria-label={`Trust Report for ${job.name} (example)`}>
      <div className="mk-app-drawer-head">
        <div>
          <strong>
            {job.name} → {job.dest}
          </strong>
          <span className={`mk-app-pill mk-app-pill--${job.state}`}>{STATE_LABEL[job.state]}</span>
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
            <Ring value={t.tone === "none" ? 0 : pct} tone={t.tone} />
            <span className="mk-app-ring-label">
              <strong>{shown}</strong>
              rows proven equal
            </span>
          </div>
          <dl className="mk-app-counts">
            <div>
              <dt>Rows read at the source</dt>
              <dd>{fmt.format(t.source)}</dd>
            </div>
            <div>
              <dt>Rows in the destination</dt>
              <dd>{fmt.format(t.destination)}</dd>
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

/** True when the viewport is phone-sized. */
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
  designWidth = 460,
  maxScale = 1,
}: {
  path: string;
  /** Empty when a neighbouring element already labels the example. */
  caption: string;
  children: ReactNode;
  designWidth?: number;
  /** Allow a small panel to grow to fill a wider slot (vector, so it stays crisp). */
  maxScale?: number;
}) {
  const { outer, inner, scale, height } = useFitScale(designWidth, maxScale);
  // Narrower than the design: reflow at full size instead of shrinking the
  // text below legibility (phones). Wider: scale to fill, as before.
  const fluid = scale < 1;
  return (
    <figure
      className={`mk-app-figure mk-app-figure--panel${fluid ? " is-fluid" : ""}`}
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

/**
 * Jobs grid with a live Trust Report drawer: choose any row. On phones the
 * frame crops to the Trust Report, with a run picker above it.
 */
export function JobsWithTrust({ caption, initial = "6af912d7" }: { caption: string; initial?: string | null }) {
  const narrow = useNarrow();
  const [openId, setOpenId] = useState<string | null>(initial);
  const job = EXAMPLE_JOBS.find((j) => j.id === openId) ?? null;

  if (narrow) {
    const shown = job ?? EXAMPLE_JOBS[1];
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
              onClick={() => setOpenId(j.id)}
            >
              {j.name}
            </button>
          ))}
        </div>
        <PanelWindow path={`jobs/${shown.id}`} caption={caption}>
          <TrustDrawer job={shown} />
        </PanelWindow>
      </div>
    );
  }

  return (
    <AppWindow
      path="jobs"
      active="jobs"
      caption={caption}
      overlay={job ? <TrustDrawer job={job} onClose={() => setOpenId(null)} /> : null}
      className={job ? "has-drawer" : undefined}
    >
      <JobsScene onOpen={setOpenId} openId={openId} />
    </AppWindow>
  );
}

/* Small pieces other scenes reuse. */
export { Route as AppRoute, Ring as AppRing };
