import { useLayoutEffect, useMemo, useRef, useState } from "react";
import { Check, Database, FileUp, Layers, TriangleAlert } from "lucide-react";
import {
  AppRoute,
  AppWindow,
  EXAMPLE_JOBS,
  PanelWindow,
  TrustDrawer,
} from "../../components/marketing/app-frame/AppFrame";
import { VideoPlayer } from "../../components/marketing/VideoPlayer";
import {
  FEATURED_VIDEO_ID,
  MARKETING_VIDEOS,
  YOUTUBE_CHANNEL_URL,
  type MarketingVideo,
} from "../../lib/videoCatalog";
import type { PublicRoute } from "../../lib/publicNavigation";

const TAGS = ["All", "Overview", "Sync & CDC", "Verification", "Sources & files", "Transforms & AI"] as const;

/** A small coded mark per topic, drawn with the current colour. */
function TopicGlyph({ tag }: { tag: MarketingVideo["tag"] }) {
  const paths: Record<MarketingVideo["tag"], string> = {
    Overview: "M3 8h10M3 4h10M3 12h6",
    "Sync & CDC": "M3 5h8l-2-2M13 11H5l2 2",
    Verification: "M3 8.5l3 3 7-7",
    "Sources & files": "M4 2h5l3 3v9H4zM9 2v3h3",
    "Transforms & AI": "M3 4h4M9 4h4M3 12h7M12 12h1M7 2v4M10 10v4",
  };
  return (
    <svg className="mk-topic-glyph" viewBox="0 0 16 16" width="16" height="16" aria-hidden focusable="false">
      <path d={paths[tag]} fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

function scrollTo(el: HTMLElement | null, block: ScrollLogicalPosition) {
  let reduce = false;
  try {
    reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch {
    reduce = true;
  }
  el?.scrollIntoView({ behavior: reduce ? "auto" : "smooth", block });
}

/* ── Product frames behind the featured player ────────────────────
   Each topic shows the part of the workspace the video is about, as
   example data rendered in code (the same example workspace as the home
   page). The frame is decorative and sits behind the play button. */

const FRAME_NOTE = "Example workspace, rendered in code. Not a frame from the video.";

type StepState = "done" | "active" | "todo";
const TRANSFER_STEPS = ["Source", "Destination", "Transform", "Map", "Validate", "Run"] as const;

function TransferStepper({
  at,
  job = EXAMPLE_JOBS[1],
}: {
  at: (typeof TRANSFER_STEPS)[number];
  job?: (typeof EXAMPLE_JOBS)[number];
}) {
  const idx = TRANSFER_STEPS.indexOf(at);
  return (
    <div className="mk-demo-scene-stepper">
      <ol>
        {TRANSFER_STEPS.map((name, i) => {
          const state: StepState = i < idx ? "done" : i === idx ? "active" : "todo";
          return (
            <li key={name} className={`is-${state}`}>
              <span className="mk-demo-scene-step-mark">{state === "done" ? <Check size={13} /> : i + 1}</span>
              {name}
            </li>
          );
        })}
      </ol>
      {idx === 0 ? (
        <p>
          <span className="mk-app-chip">{job.from.label}</span>
          <span>Choose a destination next</span>
        </p>
      ) : (
        <p>
          <AppRoute job={job} />
          <code>{job.dest}</code>
        </p>
      )}
    </div>
  );
}

const MAP_ROWS: { col: string; from: string; to: string; transform?: string }[] = [
  { col: "order_id", from: "BIGINT", to: "BIGINT" },
  { col: "region", from: "TEXT", to: "VARCHAR" },
  { col: "store_code", from: "TEXT", to: "VARCHAR" },
  { col: "product_sku", from: "TEXT", to: "VARCHAR" },
  { col: "units", from: "INT4", to: "BIGINT" },
  { col: "unit_price", from: "DECIMAL(10,2)", to: "NUMBER(10,2)" },
  { col: "order_total", from: "DECIMAL(12,2)", to: "NUMBER(12,2)" },
  { col: "status", from: "TEXT", to: "VARCHAR" },
];

const TRANSFORM_ROWS: { col: string; from: string; to: string; transform?: string }[] = [
  { col: "customer_id", from: "BIGINT", to: "NUMBER(19,0)" },
  { col: "email", from: "VARCHAR(255)", to: "VARCHAR", transform: "Mask" },
  { col: "full_name", from: "VARCHAR(120)", to: "VARCHAR" },
  { col: "country", from: "CHAR(2)", to: "VARCHAR(2)" },
  { col: "phone", from: "VARCHAR(32)", to: "VARCHAR", transform: "Mask" },
  { col: "created_at", from: "DATETIME(6)", to: "TIMESTAMP_NTZ(6)" },
  { col: "updated_at", from: "DATETIME(6)", to: "TIMESTAMP_NTZ(3)", transform: "Cast, approved" },
  { col: "status", from: "ENUM", to: "VARCHAR" },
];

function MapTable({ rows, title, meta }: { rows: typeof MAP_ROWS; title: string; meta: string }) {
  return (
    <div className="mk-demo-scene-map">
      <div className="mk-demo-scene-map-head">
        <div>
          <strong>{title}</strong>
          <span>{meta}</span>
        </div>
        <span className="mk-app-tbtn">
          <Check size={13} />
          Approve eligible
        </span>
      </div>
      <div className="mk-demo-scene-grid">
        <div className="mk-demo-scene-tr is-head">
          <span>Source</span>
          <span>Destination</span>
          <span>Transform</span>
          <span>Status</span>
        </div>
        {rows.map((r) => (
          <div key={r.col} className="mk-demo-scene-tr">
            <span>
              <code>{r.col}</code>
              <small>{r.from}</small>
            </span>
            <span>
              <code>{r.col}</code>
              <small>{r.to}</small>
            </span>
            <span className={r.transform ? "is-set" : undefined}>{r.transform ?? "None"}</span>
            <span>
              <span className="mk-app-pill mk-app-pill--completed">Safe</span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

function SourceScene() {
  const rows = [
    ["1", "Europe", "ST-001", "SKU-00001"],
    ["2", "APAC", "ST-002", "SKU-00002"],
    ["3", "LATAM", "ST-003", "SKU-00003"],
    ["4", "Middle East", "ST-004", "SKU-00004"],
    ["5", "North America", "ST-005", "SKU-00005"],
    ["6", "Europe", "ST-006", "SKU-00006"],
  ];
  return (
    <div className="mk-demo-scene-source">
      <div className="mk-demo-scene-form">
        <small>Where is your data?</small>
        <div className="mk-demo-scene-choice">
          <span>
            <FileUp size={14} />
            File
          </span>
          <span className="is-on">
            <Database size={14} />
            Database
          </span>
          <span>
            <Layers size={14} />
            Cloud storage
          </span>
        </div>
        <small>Source extract</small>
        <div className="mk-demo-scene-seg">
          <span className="is-on">Table</span>
          <span>SQL query</span>
          <span>Stored procedure</span>
        </div>
        <small>Table</small>
        <div className="mk-demo-scene-input">sales_orders</div>
      </div>
      <div className="mk-demo-scene-schema">
        <strong>Source schema</strong>
        <span>11 fields, 100 sample rows from sales_orders</span>
        <div className="mk-demo-scene-sample">
          {["ORDER_ID", "REGION", "STORE_CODE", "PRODUCT_SKU"].map((h) => (
            <code key={h} className="is-head">{h}</code>
          ))}
          {rows.flat().map((v, i) => (
            <span key={i}>{v}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

type ScheduleRow = { name: string; job: (typeof EXAMPLE_JOBS)[number]; cadence: string; last: string; tone: string; state: string };

function SchedulesScene() {
  const [orders, , customers, , invoices, users] = EXAMPLE_JOBS;
  const rows: ScheduleRow[] = [
    { name: "orders", job: orders, cadence: "Continuous, from the log", last: "Running", tone: "running", state: "Active" },
    { name: "customers", job: customers, cadence: "Every hour", last: "Completed", tone: "completed", state: "Active" },
    { name: "users", job: users, cadence: "Daily 14:00 UTC", last: "Completed", tone: "completed", state: "Active" },
    { name: "invoices.csv", job: invoices, cadence: "Daily 18:20 UTC", last: "Refused", tone: "failed", state: "Parked" },
  ];
  return (
    <div className="mk-demo-scene-sched">
      <div className="mk-demo-scene-park">
        <TriangleAlert size={15} />
        <div>
          <strong>
            Waiting on a decision: {invoices.name} → {invoices.dest}
          </strong>
          <p>{invoices.trust.note}</p>
        </div>
      </div>
      <div className="mk-demo-scene-grid mk-demo-scene-grid--sched">
        <div className="mk-demo-scene-tr is-head">
          <span>Schedule</span>
          <span>Route</span>
          <span>Cadence</span>
          <span>Mode</span>
          <span>Last run</span>
          <span>State</span>
        </div>
        {rows.map((r) => (
          <div key={r.name} className="mk-demo-scene-tr">
            <span>
              <strong>
                {r.name} → {r.job.dest}
              </strong>
            </span>
            <span>
              <AppRoute job={r.job} />
            </span>
            <span>{r.cadence}</span>
            <span>{r.job.mode}</span>
            <span>
              <span className={`mk-app-pill mk-app-pill--${r.tone}`}>{r.last}</span>
            </span>
            <span className="mk-demo-scene-state">{r.state}</span>
          </div>
        ))}
      </div>
    </div>
  );
}

function useNarrowViewport(query = "(max-width: 767px)") {
  const [narrow, setNarrow] = useState(false);
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

/** The product frame for a topic: a full window on desktop, a panel crop on phones. */
function TopicFrame({ tag }: { tag: MarketingVideo["tag"] }) {
  const narrow = useNarrowViewport();
  const sales = EXAMPLE_JOBS[1];

  if (tag === "Verification") {
    return (
      <PanelWindow path={`jobs/${sales.id}`} caption={FRAME_NOTE} designWidth={narrow ? 460 : 560}>
        <TrustDrawer job={sales} />
      </PanelWindow>
    );
  }

  if (tag === "Sync & CDC") {
    return narrow ? (
      <PanelWindow path="schedules" caption={FRAME_NOTE} designWidth={620}>
        <SchedulesScene />
      </PanelWindow>
    ) : (
      <AppWindow path="schedules" active="schedules" caption={FRAME_NOTE} designWidth={1200}>
        <SchedulesScene />
      </AppWindow>
    );
  }

  const body =
    tag === "Sources & files" ? (
      <>
        <TransferStepper at="Source" />
        <SourceScene />
      </>
    ) : tag === "Transforms & AI" ? (
      <>
        <TransferStepper at="Map" job={EXAMPLE_JOBS[2]} />
        <MapTable rows={TRANSFORM_ROWS} title="Map columns" meta="customers, MySQL to Snowflake. 3 transforms set" />
      </>
    ) : (
      <>
        <TransferStepper at="Map" />
        <MapTable rows={MAP_ROWS} title="Map columns" meta="New table, full append. 11 of 11 mapped" />
      </>
    );

  return narrow ? (
    <PanelWindow path="transfer" caption={FRAME_NOTE} designWidth={620}>
      <div className="mk-demo-scene-panel">{body}</div>
    </PanelWindow>
  ) : (
    <AppWindow path="transfer" active="transfer" caption={FRAME_NOTE} designWidth={1200}>
      {body}
    </AppWindow>
  );
}

/** The next few videos after the current one, wrapping round the catalogue. */
function upNext(current: MarketingVideo, count: number): MarketingVideo[] {
  const at = MARKETING_VIDEOS.findIndex((v) => v.id === current.id);
  return Array.from({ length: count }, (_, i) => MARKETING_VIDEOS[(at + 1 + i) % MARKETING_VIDEOS.length]);
}

export function DemosPage({ onNavigate }: { onNavigate: (route: PublicRoute) => void }) {
  const [current, setCurrent] = useState<MarketingVideo>(
    MARKETING_VIDEOS.find((v) => v.id === FEATURED_VIDEO_ID) ?? MARKETING_VIDEOS[0],
  );
  const [tag, setTag] = useState<(typeof TAGS)[number]>("All");
  const playerRef = useRef<HTMLElement>(null);

  const groups = useMemo(() => {
    const topics = TAGS.filter((t): t is MarketingVideo["tag"] => t !== "All" && (tag === "All" || t === tag));
    return topics.map((t) => ({ tag: t, videos: MARKETING_VIDEOS.filter((v) => v.tag === t) }));
  }, [tag]);
  const shown = groups.reduce((n, g) => n + g.videos.length, 0);

  const choose = (video: MarketingVideo) => {
    setCurrent(video);
    scrollTo(playerRef.current, "start");
  };

  return (
    <div className="mk-page mk-demos mk-demo-page">
      <section className="mk-demo-stage" aria-label="Video demos" ref={playerRef}>
        <div className="mk-wrap">
          <header className="mk-demo-stage-head">
            <h1>Watch a transfer prove itself.</h1>
            <p>Recorded runs on the production engine, from mapping to the read-back at the end.</p>
          </header>
          <div className="mk-demo-stage-grid">
            <div className="mk-demo-screen">
              <VideoPlayer
                key={current.id}
                videoId={current.id}
                title={current.title}
                tag={current.tag}
                glyph={<TopicGlyph tag={current.tag} />}
                showTitle={false}
                backdrop={<TopicFrame tag={current.tag} />}
              />
            </div>
            <aside className="mk-demo-now" aria-labelledby="mk-demo-current">
              <p className="mk-demo-now-tag">
                <TopicGlyph tag={current.tag} />
                {current.tag}
              </p>
              <h2 id="mk-demo-current">{current.title}</h2>
              <p className="mk-demo-now-blurb">{current.blurb}</p>
              <h3 className="mk-demo-next-title">Up next</h3>
              <ul className="mk-demo-next">
                {upNext(current, 3).map((video) => (
                  <li key={video.id}>
                    <button type="button" onClick={() => setCurrent(video)}>
                      <TopicGlyph tag={video.tag} />
                      <span>{video.title}</span>
                    </button>
                  </li>
                ))}
              </ul>
              <a className="mk-btn mk-btn--secondary mk-demo-subscribe" href={YOUTUBE_CHANNEL_URL} target="_blank" rel="noopener noreferrer">
                Subscribe on YouTube
              </a>
            </aside>
          </div>
        </div>
      </section>

      <section className="mk-section mk-demo-catalogue" aria-labelledby="mk-demos-all">
        <div className="mk-wrap">
          <header className="mk-demo-catalogue-head">
            <h2 id="mk-demos-all">All demos</h2>
            <div className="mk-chips mk-demo-filter" role="group" aria-label="Filter demos by topic">
              {TAGS.map((t) => (
                <button key={t} type="button" className="mk-chip" aria-pressed={tag === t} onClick={() => setTag(t)}>
                  {t}
                </button>
              ))}
            </div>
            <p className="mk-demo-count" aria-live="polite">
              {shown} {shown === 1 ? "video" : "videos"}
            </p>
          </header>
          <div className="mk-demo-index">
            {groups.map((group) => (
              <section key={group.tag} className="mk-demo-group" aria-label={group.tag}>
                <h3 className="mk-demo-group-title">
                  <TopicGlyph tag={group.tag} />
                  {group.tag}
                  <span>{group.videos.length}</span>
                </h3>
                <ul>
                  {group.videos.map((video) => {
                    const isCurrent = video.id === current.id;
                    return (
                      <li key={video.id}>
                        <button
                          type="button"
                          className="mk-demo-row"
                          aria-current={isCurrent ? "true" : undefined}
                          onClick={() => choose(video)}
                        >
                          <span className="mk-demo-row-title">{video.title}</span>
                          <span className="mk-demo-row-blurb">{video.blurb}</span>
                          <span className="mk-demo-row-act">
                            <svg viewBox="0 0 24 24" width="14" height="14" fill="currentColor" aria-hidden="true" focusable="false">
                              <path d="M8 5v14l11-7z" />
                            </svg>
                            {isCurrent ? "On the stage" : "Watch"}
                          </span>
                        </button>
                      </li>
                    );
                  })}
                </ul>
              </section>
            ))}
          </div>
          <p className="mk-demo-foot">
            <button type="button" className="mk-text-btn" onClick={() => onNavigate("help")}>
              Prefer reading? Open the docs.
            </button>
          </p>
        </div>
      </section>
    </div>
  );
}
