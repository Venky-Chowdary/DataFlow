import { useRef, useState, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";
import { StatementHero } from "../components/marketing/StatementHero";
import { ConnectorStrip } from "../components/marketing/ConnectorStrip";
import {
  McpScene,
  PilotScene,
  QueryScene,
  SCENE_NOTES,
  SchedulesScene,
  TransferScene,
} from "../components/marketing/StoryScenes";
import {
  EXAMPLE_JOBS,
  JobsWithTrust,
} from "../components/marketing/app-frame/AppFrame";
import {
  ALL_CONNECTORS,
  PROVEN_CDC_SOURCES,
  PROVEN_DRIVERS,
} from "../lib/provenEvidence";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";
import { SYNC_MODE_COUNT_WORD } from "../lib/syncModesPhrase";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}



const DIFFERENT: { title: string; without: string; with: string; where: string }[] = [
  {
    title: "A green job is taken as proof",
    without: "The job reports success, and nobody counts the destination until a report looks wrong.",
    with: "The run that writes the rows also counts the destination with its own engine and compares it with the source.",
    where: "Job space, Trust Report",
  },
  {
    title: "Failures show up mid-write",
    without: "A value the destination cannot hold fails a batch halfway through the night.",
    with: "Validate pushes sample rows through the real write path first. The approved plan is hashed, and a run that drifts from it is refused.",
    where: "Transfer Studio, Validate",
  },
  {
    title: "Rejected rows go quiet",
    without: "Rows the destination refuses end up in a log file, or nowhere.",
    with: "A row that cannot land is held with its column, value, and reason, and replayed once fixed. A row not landed, held, or skipped is reported as potential loss.",
    where: "Job space, Quarantine",
  },
  {
    title: "Sign-off lives in a spreadsheet",
    without: "Approval is a tick in a sheet that nobody can re-check later.",
    with: "The report names the comparison that ran, proof packs are signed and verifiable, and every action lands in a tamper-evident audit log.",
    where: "Job space, Sign-off",
  },
];

const VERDICTS: { name: string; tone: "proven" | "explained" | "bounded" | "open" | "none"; meaning: string }[] = [
  { name: "Proven", tone: "proven", meaning: "Every row was compared and none differs." },
  { name: "Explained", tone: "explained", meaning: "Every row was compared. Some differ, and each difference has a known cause, such as timestamp precision or a transform you applied." },
  { name: "No difference found", tone: "bounded", meaning: "Values were compared on a random sample. The report gives an upper bound on the rate of unexplained differences." },
  { name: "Unexplained", tone: "open", meaning: "At least one difference has no known cause. The report points to the column and the row." },
  { name: "Unverifiable", tone: "none", meaning: "There is nothing to sign yet: the job is still running, or only row counts exist." },
];


function RouteLink({
  route,
  onNavigate,
  children,
  className = "mk-link",
}: {
  route: PublicRoute;
  onNavigate: (route: PublicRoute) => void;
  children: string;
  className?: string;
}) {
  return (
    <a
      className={className}
      href={hashForPublicRoute(route)}
      onClick={(e: MouseEvent<HTMLAnchorElement>) => {
        e.preventDefault();
        onNavigate(route);
      }}
    >
      {children}
    </a>
  );
}

type SwitchTab = { id: string; label: string; notes: readonly string[]; render: () => ReactNode };

const SWITCH_TABS: SwitchTab[] = [
  {
    id: "jobs",
    label: "Jobs and Trust Report",
    notes: [
      "Every run with its route, rows, held rows, and the comparison that ran.",
      "Choose a run: its Trust Report opens beside the grid.",
      "Only a full comparison with no difference is called proven.",
    ],
    render: () => (
      <JobsWithTrust initial="5d20b7c4" caption="An example workspace, rendered in code. Choose any row." />
    ),
  },
  { id: "transfer", label: "Transfer Studio", notes: SCENE_NOTES.transfer, render: () => <TransferScene marks /> },
  { id: "schedules", label: "Schedules", notes: SCENE_NOTES.schedules, render: () => <SchedulesScene marks /> },
  { id: "query", label: "Query", notes: SCENE_NOTES.query, render: () => <QueryScene marks /> },
  { id: "pilot", label: "Pilot", notes: SCENE_NOTES.pilot, render: () => <PilotScene marks /> },
  { id: "mcp", label: "MCP server", notes: SCENE_NOTES.mcp, render: () => <McpScene marks /> },
];

/**
 * The product, one part at a time: a tab per surface, each a full app
 * window with numbered pins and the notes that read them. Never advances on
 * its own; arrow keys move between tabs.
 */
function ProductSwitcher() {
  const [active, setActive] = useState(0);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const tab = SWITCH_TABS[active];

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const last = SWITCH_TABS.length - 1;
    let next = active;
    if (e.key === "ArrowRight") next = active === last ? 0 : active + 1;
    else if (e.key === "ArrowLeft") next = active === 0 ? last : active - 1;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = last;
    else return;
    e.preventDefault();
    setActive(next);
    tabs.current[next]?.focus();
  };

  return (
    <div className="mk-switch">
      <div className="mk-switch-tabs" role="tablist" aria-label="Parts of the product" onKeyDown={onKey}>
        {SWITCH_TABS.map((t, i) => (
          <button
            key={t.id}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`mk-switch-tab-${t.id}`}
            aria-selected={i === active}
            aria-controls={`mk-switch-panel-${t.id}`}
            tabIndex={i === active ? 0 : -1}
            className={i === active ? "is-active" : undefined}
            onClick={() => setActive(i)}
          >
            {t.label}
          </button>
        ))}
      </div>
      <div
        className="mk-switch-panel"
        role="tabpanel"
        id={`mk-switch-panel-${tab.id}`}
        aria-labelledby={`mk-switch-tab-${tab.id}`}
        key={tab.id}
      >
        {tab.render()}
        <ol className="mk-switch-notes">
          {tab.notes.map((n) => (
            <li key={n}>{n}</li>
          ))}
        </ol>
      </div>
    </div>
  );
}

const HERO_JOB = EXAMPLE_JOBS.find((j) => j.id === "6af912d7") ?? EXAMPLE_JOBS[1];

const SIGNERS: { role: string; state: string; signed: boolean }[] = [
  { role: "Engineering", state: "Signed today at 02:51", signed: true },
  { role: "Data owner", state: "Waiting to sign", signed: false },
  { role: "Compliance", state: "Waiting to sign", signed: false },
];

const HELD_ROWS: { key: string; value: string }[] = [
  { key: "48204", value: "NULL" },
  { key: "48977", value: "NULL" },
  { key: "51310", value: "NULL" },
  { key: "52046", value: "NULL" },
  { key: "55813", value: "NULL" },
  { key: "60129", value: "NULL" },
  { key: "61402", value: "NULL" },
  { key: "63318", value: "NULL" },
  { key: "66751", value: "NULL" },
  { key: "70264", value: "NULL" },
];

/** The rows the example's `region = 'TEST'` filter excluded. */
const SKIPPED_ROWS: { key: string }[] = [{ key: "50017" }, { key: "57240" }];

const fmtN = new Intl.NumberFormat("en-US");

/**
 * Where every row went, as product UI: the three places a row can land,
 * and the check that none is unaccounted. Every figure is the hero's run.
 */
function RowBento() {
  const [replayed, setReplayed] = useState(false);
  const t = HERO_JOB.trust;
  const held = HERO_JOB.held ?? 0;
  const skipped = t.source - HERO_JOB.rows - held;
  const unaccounted = t.source - HERO_JOB.rows - held - skipped;
  return (
    <div className="mk-bento">
      <article className="mk-bento-tile mk-bento-tile--landed">
        <h3>Landed</h3>
        <p>Written, then counted by the destination's own engine and compared with what was read.</p>
        <div className="mk-app-surface mk-bento-ui">
          <p className="mk-app-verdict">
            <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
            <span>Check: {t.check}</span>
          </p>
          <dl className="mk-app-counts mk-bento-counts">
            <div>
              <dt>Read at the source</dt>
              <dd>{fmtN.format(t.source)}</dd>
            </div>
            <div>
              <dt>In the destination</dt>
              <dd>{fmtN.format(t.destination)}</dd>
            </div>
            <div>
              <dt>Differ</dt>
              <dd>{fmtN.format(t.differ)}</dd>
            </div>
          </dl>
        </div>
      </article>

      <article className="mk-bento-tile mk-bento-tile--held">
        <h3>Held</h3>
        <p>Refused by the destination and kept with its value and reason, instead of stopping the load.</p>
        <div className="mk-app-surface mk-bento-ui mk-bento-ui--fill">
          <div className="mk-bento-held-head">
            <span className="mk-app-pill mk-app-pill--v-explained">{held} held</span>
            <code>status</code>
            <span>is NOT NULL in the destination</span>
          </div>
          <ul className="mk-bento-rows">
            {HELD_ROWS.map((r) => (
              <li key={r.key} className={replayed ? "is-landed" : undefined}>
                <code>order_id {r.key}</code>
                <code>status = {replayed ? "'pending'" : r.value}</code>
                <span>{replayed ? "Landed on replay" : "Held"}</span>
              </li>
            ))}
          </ul>
          <div className="mk-bento-actions">
            <button
              type="button"
              className="mk-app-tbtn mk-app-tbtn--primary"
              aria-pressed={replayed}
              onClick={() => setReplayed((v) => !v)}
            >
              {replayed ? "Undo the example replay" : `Replay ${held} rows after the fix`}
            </button>
            <span className="mk-bento-note" aria-live="polite">
              {replayed ? `${held} rows landed into the same table` : `Showing ${HELD_ROWS.length} of ${held}`}
            </span>
          </div>
        </div>
      </article>

      <article className="mk-bento-tile mk-bento-tile--skipped">
        <h3>Skipped</h3>
        <p>Excluded by a rule you set, and named in the statement.</p>
        <div className="mk-app-surface mk-bento-ui">
          <span className="mk-bento-filter">
            Filter <code>region = 'TEST'</code>
          </span>
          <p className="mk-bento-big">
            <strong>{skipped}</strong> rows skipped by this filter
          </p>
          <ul className="mk-bento-rows mk-bento-rows--skipped">
            {SKIPPED_ROWS.slice(0, skipped).map((r) => (
              <li key={r.key}>
                <code>
                  <span className="mk-bento-key-label">order_id </span>
                  {r.key}
                </code>
                <span>Skipped</span>
              </li>
            ))}
          </ul>
        </div>
      </article>

      <article className="mk-bento-tile mk-bento-tile--balance">
        <h3>Unaccounted</h3>
        <p>A row that is none of the three is reported as potential loss, and the statement does not balance.</p>
        <div className="mk-app-surface mk-bento-ui mk-bento-sum">
          <span>{fmtN.format(t.source)} read</span>
          <span>− {fmtN.format(HERO_JOB.rows)} landed</span>
          <span>− {held} held</span>
          <span>− {skipped} skipped</span>
          <strong>= {unaccounted} unaccounted</strong>
        </div>
      </article>
    </div>
  );
}

export function LandingHome({ onGetStarted, onNavigate }: LandingHomeProps) {
  return (
    <div className="mk-page mk-home">
      <section className="mk-hero mk-hero--statement" aria-labelledby="mk-hero-title">
        <div className="mk-wrap mk-hero-stage">
          <div className="mk-hero-copy">
            <h1 id="mk-hero-title">Every row, accounted for.</h1>
            <p className="mk-lead">
              Datawrap moves data between databases, warehouses, files, and streams, and gives you a
              statement for every run: what was read, what landed, what was held and why, and what a
              rule skipped.
            </p>
            <div className="mk-actions">
              <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
                Start free
              </button>
              <RouteLink route="contact" onNavigate={onNavigate} className="mk-btn mk-btn--secondary mk-btn--lg">
                Talk to an engineer
              </RouteLink>
            </div>
          </div>
          <div className="mk-hero-statement">
            <StatementHero />
          </div>
        </div>
      </section>

      <ConnectorStrip />

      <section className="mk-evidence" aria-label="What CI and the product prove">
        <div className="mk-wrap">
          <ul className="mk-evidence-list">
            <li>
              <RouteLink route="integrations" onNavigate={onNavigate} className="mk-evidence-item">
                {`${PROVEN_DRIVERS} of ${ALL_CONNECTORS.length} drivers proven in CI`}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="solution-sync" onNavigate={onNavigate} className="mk-evidence-item">
                {`${SYNC_MODE_COUNT_WORD[0].toUpperCase() + SYNC_MODE_COUNT_WORD.slice(1)} sync modes, including CDC`}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="product-pipelines" onNavigate={onNavigate} className="mk-evidence-item">
                {PROVEN_CDC_SOURCES.length
                  ? `CDC proven on ${PROVEN_CDC_SOURCES.length} databases`
                  : "Change data capture from the log"}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="customers" onNavigate={onNavigate} className="mk-evidence-item">
                Signed statements you can verify
              </RouteLink>
            </li>
          </ul>
        </div>
      </section>

      <section className="mk-showcase" aria-labelledby="mk-showcase-title">
        <div className="mk-wrap">
          <h2 id="mk-showcase-title">One workspace, from the plan to the proof</h2>
          <p className="mk-showcase-lead">
            Every screen below is the product, rendered in code with one example workspace. Choose a
            part of the product to open it.
          </p>
          <ProductSwitcher />
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-bins-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-bins-title">Where every row went</h2>
            <p>Each run sorts every row it read into one of three places. The statement only balances when they add up.</p>
          </header>
          <RowBento />
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-different-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-different-title">What a migration usually leaves to chance</h2>
            <p>Four gaps that scripts and spreadsheets leave open, and what Datawrap does on every run instead.</p>
          </header>
          <dl className="mk-contrast">
            <div className="mk-contrast-head" aria-hidden>
              <span />
              <span>Usually</span>
              <span>With Datawrap</span>
            </div>
            {DIFFERENT.map((d) => (
              <div key={d.title} className="mk-contrast-row">
                <dt>{d.title}</dt>
                <dd className="mk-contrast-without">
                  <span className="mk-contrast-label">Usually: </span>
                  {d.without}
                </dd>
                <dd className="mk-contrast-with">
                  <span className="mk-contrast-label">With Datawrap: </span>
                  {d.with}
                  <span className="mk-different-where">You see it in {d.where}</span>
                </dd>
              </div>
            ))}
          </dl>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-verdict-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-verdict-title">Five verdicts, and only one is called proven</h2>
            <p>
              Each table carries the comparison that actually ran: full checksum, key diff, sample, or
              count. The Trust Report turns it into a verdict a reviewer can sign, or says there is
              nothing to sign yet.
            </p>
          </header>
          <ol className="mk-scale" aria-label="Trust Report verdicts, strongest first">
            {VERDICTS.map((v) => (
              <li key={v.name} className={`mk-scale-step mk-scale-step--${v.tone}`}>
                <span className="mk-scale-name">{v.name}</span>
                <span className="mk-scale-meaning">{v.meaning}</span>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="mk-closer" aria-labelledby="mk-closing-title">
        <div className="mk-wrap mk-closer-grid">
          <div className="mk-closer-copy">
            <h2 id="mk-closing-title">Run your first statement on your own data</h2>
            <p className="mk-closer-lead">
              Start with a free workspace and a table you know well, or ask for a scoped pilot on your
              own sources with a solutions engineer.
            </p>
            <div className="mk-actions">
              <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
                Start free
              </button>
              <RouteLink route="contact" onNavigate={onNavigate} className="mk-btn mk-btn--secondary mk-btn--lg">
                Request a pilot
              </RouteLink>
            </div>
            <p className="mk-caption">
              Or write to <a href="mailto:sales@datawrap.io">sales@datawrap.io</a>.{" "}
              <RouteLink route="demos" onNavigate={onNavigate}>
                Watch recorded runs
              </RouteLink>
            </p>
          </div>
          <figure className="mk-closer-sheet" aria-labelledby="mk-closer-sheet-title">
            <figcaption id="mk-closer-sheet-title">
              Sign-off for <code>{HERO_JOB.name}</code>, an example
            </figcaption>
            <ol className="mk-closer-sign">
              {SIGNERS.map((r) => (
                <li key={r.role} className={r.signed ? "is-signed" : undefined}>
                  <span className="mk-closer-role">{r.role}</span>
                  <span className="mk-closer-state">{r.state}</span>
                </li>
              ))}
            </ol>
          </figure>
        </div>
      </section>
    </div>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
