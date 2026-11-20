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
  EVIDENCE_AS_OF,
  PROVEN_CDC_SOURCES,
  PROVEN_DRIVERS,
} from "../lib/provenEvidence";
import { MarketingClosing } from "../components/marketing/MarketingClosing";
import { RUN_HELD_KEYS } from "../components/marketing/StoryPanels";
import { Bento, BentoTile } from "../components/marketing/kit/Bento";
import { SplitHead } from "../components/marketing/kit/SplitHead";
import { StatRow } from "../components/marketing/kit/StatRow";
import { SYNC_MODES } from "../lib/transferConstants";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";

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

const HELD_ROWS: { key: string; value: string }[] = RUN_HELD_KEYS.map((key) => ({ key, value: "NULL" }));

/** The rows the example's `region = 'TEST'` filter excluded. */
const SKIPPED_ROWS: { key: string }[] = [{ key: "50017" }, { key: "57240" }];

const fmtN = new Intl.NumberFormat("en-US");

/** How many held rows the Held tile lists before "Showing n of 33". */
const HELD_SHOWN = 3;

/**
 * Where every row went, built as the statement itself: landed + held +
 * skipped, then the line that proves nothing is unaccounted. Each tile is a
 * fragment of the product UI; every figure is the hero's run.
 */
function RowBento() {
  const [replayed, setReplayed] = useState(false);
  const t = HERO_JOB.trust;
  const held = HERO_JOB.held ?? 0;
  const skipped = t.source - HERO_JOB.rows - held;
  const unaccounted = t.source - HERO_JOB.rows - held - skipped;
  const shown = HELD_ROWS.slice(0, HELD_SHOWN);
  return (
    <Bento className="mk-kit-bento--eq mk-rowbento" label="Where every row of the example run went">
      <BentoTile
        span={5}
        tone="landed"
        title="Landed"
        figure={fmtN.format(HERO_JOB.rows)}
        body="Written, then counted by the destination's own engine and compared with what was read."
        bleed
      >
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <span className="mk-app-pill mk-app-pill--t-proven">Proven equal</span>
            <span className="mk-tui-muted">Check: {t.check}</span>
          </div>
          <dl className="mk-tui-counts mk-tui-counts--rows">
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
      </BentoTile>

      <BentoTile
        span={4}
        op="+"
        tone="held"
        title="Held"
        figure={fmtN.format(held)}
        body="Refused by the destination and kept with its value and reason, instead of stopping the load."
        bleed
      >
        <div className="mk-app-surface mk-tui">
          <div className="mk-tui-head">
            <code>status</code>
            <span className="mk-tui-muted">is NOT NULL in the destination</span>
          </div>
          <ul className="mk-tui-rows">
            {shown.map((r) => (
              <li key={r.key} className={replayed ? "is-landed" : undefined}>
                <code>order_id {r.key}</code>
                <code>status = {replayed ? "'pending'" : r.value}</code>
                <span>{replayed ? "Landed on replay" : "Held"}</span>
              </li>
            ))}
          </ul>
          <div className="mk-tui-actions">
            <button
              type="button"
              className="mk-app-tbtn mk-app-tbtn--primary"
              aria-pressed={replayed}
              onClick={() => setReplayed((v) => !v)}
            >
              {replayed ? "Undo the example replay" : `Replay ${held} rows after the fix`}
            </button>
            <span className="mk-tui-muted" aria-live="polite">
              {replayed ? `${held} rows landed into the same table` : `Showing ${shown.length} of ${held}`}
            </span>
          </div>
        </div>
      </BentoTile>

      <BentoTile
        span={3}
        op="+"
        tone="skipped"
        title="Skipped"
        figure={fmtN.format(skipped)}
        body="Excluded by a rule you set, and named in the statement."
        bleed
      >
        <div className="mk-app-surface mk-tui">
          <p className="mk-tui-filter">
            Filter <code>region = 'TEST'</code>
          </p>
          <ul className="mk-tui-rows mk-tui-rows--skipped">
            {SKIPPED_ROWS.slice(0, skipped).map((r) => (
              <li key={r.key}>
                <code>order_id {r.key}</code>
                <span>Skipped</span>
              </li>
            ))}
          </ul>
        </div>
      </BentoTile>

      <BentoTile
        span={12}
        tone="balance"
        className="mk-tile--sum"
        title="Unaccounted"
        body="A row that is none of the three is reported as potential loss, and the statement does not balance."
      >
        <dl
          className="mk-sumline"
          aria-label={`${fmtN.format(t.source)} read, minus ${fmtN.format(HERO_JOB.rows)} landed, ${held} held and ${skipped} skipped, leaves ${unaccounted} unaccounted`}
        >
          <div>
            <dt>Read</dt>
            <dd>{fmtN.format(t.source)}</dd>
          </div>
          <div data-op="−">
            <dt>Landed</dt>
            <dd>{fmtN.format(HERO_JOB.rows)}</dd>
          </div>
          <div className="is-held" data-op="−">
            <dt>Held</dt>
            <dd>{fmtN.format(held)}</dd>
          </div>
          <div data-op="−">
            <dt>Skipped</dt>
            <dd>{fmtN.format(skipped)}</dd>
          </div>
          <div className="is-result" data-op="=">
            <dt>Unaccounted</dt>
            <dd>{fmtN.format(unaccounted)}</dd>
          </div>
        </dl>
      </BentoTile>
    </Bento>
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

      <section className="mk-showcase mk-showcase--light" aria-labelledby="mk-showcase-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-showcase-title"
            title="One workspace, from the plan to the proof"
            lead="Every screen below is the product, rendered in code with one example workspace. Choose a part of the product to open it."
          />
          <ProductSwitcher />
        </div>
      </section>

      <section className="mk-section mk-kit-sec" aria-labelledby="mk-bins-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-bins-title"
            title="Where every row went"
            lead="Each run sorts every row it read into one of three places. The statement only balances when they add up."
          />
          <RowBento />
        </div>
      </section>

      <section className="mk-section mk-kit-sec mk-proven" aria-labelledby="mk-proven-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-proven-title"
            title="What CI has proven"
            lead={EVIDENCE_AS_OF ? `Counted from the CI evidence behind the connectors page, ${EVIDENCE_AS_OF}.` : "Counted from the CI evidence behind the connectors page."}
          />
          <StatRow
            label="What CI and the product prove"
            onNavigate={onNavigate}
            items={[
              {
                figure: (
                  <>
                    {PROVEN_DRIVERS} <span className="mk-statrow-rel">of</span> {ALL_CONNECTORS.length}
                  </>
                ),
                text: "drivers proven in CI",
                route: "integrations",
              },
              ...(PROVEN_CDC_SOURCES.length
                ? [{ figure: PROVEN_CDC_SOURCES.length, text: "databases with change data capture proven", route: "product-pipelines" as const }]
                : []),
              { figure: SYNC_MODES.length, text: "sync modes, CDC included", route: "solution-sync" },
            ]}
          />
        </div>
      </section>

      <section className="mk-section mk-kit-sec" aria-labelledby="mk-different-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-different-title"
            title="What a migration usually leaves to chance"
            lead="Four gaps that scripts and spreadsheets leave open, and what Datawrap does on every run instead."
          />
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

      <section className="mk-section mk-section--band mk-kit-sec" aria-labelledby="mk-verdict-title">
        <div className="mk-wrap">
          <SplitHead
            id="mk-verdict-title"
            title="Five verdicts, and only one is called proven"
            lead="Each table carries the comparison that actually ran: full checksum, key diff, sample, or count. The Trust Report turns it into a verdict a reviewer can sign, or says there is nothing to sign yet."
          />
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

      <MarketingClosing
        title="Run your first statement on your own data"
        body="Start with a free workspace and a table you know well, or ask for a scoped pilot on your own sources with a solutions engineer."
        note={
          <>
            Or write to <a href="mailto:sales@datawrap.io">sales@datawrap.io</a>.{" "}
            <RouteLink route="demos" onNavigate={onNavigate}>
              Watch recorded runs
            </RouteLink>
          </>
        }
      >
        <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
          Start free
        </button>
        <RouteLink route="contact" onNavigate={onNavigate} className="mk-btn mk-btn--secondary mk-btn--lg">
          Request a pilot
        </RouteLink>
      </MarketingClosing>
    </div>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
