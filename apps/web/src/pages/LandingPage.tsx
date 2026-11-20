import { useRef, useState, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";
import {
  CoveragePanel,
  EvidencePanel,
  GatesPanel,
  PlanPanel,
  QuarantinePanel,
  VerifyPanel,
} from "../components/marketing/ProductPanels";
import { StatementHero } from "../components/marketing/StatementHero";
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
  PanelWindow,
  TrustDrawer,
} from "../components/marketing/app-frame/AppFrame";
import {
  ALL_CONNECTORS,
  PROVEN_CDC_SOURCES,
  PROVEN_DRIVERS,
  catalogHonestyLead,
  cdcSourcesPhrase,
} from "../lib/provenEvidence";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";
import { SYNC_MODE_COUNT_WORD, syncModesList } from "../lib/syncModesPhrase";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

type Step = { id: string; name: string; title: string; body: string; facts: string[]; panel: ReactNode };

/* Every panel on this page is product UI rendered in code, and each is
   labelled as an example inside the panel. */
const STEPS: Step[] = [
  {
    id: "plan",
    name: "Plan",
    title: "Pair columns by meaning, and approve the plan",
    body: "Pick a table, a SQL query, or a stored procedure as the source. Each pairing is scored on the column's role, qualifier, and type fit. Confident pairs map themselves; ambiguous ones wait for a person, with the reason on the row.",
    facts: ["Every pairing scored on role, qualifier, and type", "JSONB lands in Snowflake as VARIANT", "Pairs below the confidence floor wait for review"],
    panel: <PlanPanel />,
  },
  {
    id: "validate",
    name: "Validate",
    title: "Check the plan before the first write",
    body: "The checks answer four questions: can we read and write, will every value fit, do the columns line up, and is it safe to write. Sample rows go through the real write path, so a value the destination cannot hold is a finding now, not a failed batch later.",
    facts: ["Approving the plan records a hash of it", "Execute stays locked until the plan is approved"],
    panel: <GatesPanel />,
  },
  {
    id: "execute",
    name: "Execute",
    title: "Run the approved plan, and hold what fails",
    body: "Rows are written in batches with bound parameters, by insert, merge, or a stored procedure. A row the destination rejects is held with its column, value, and reason instead of stopping the load. Fix the cause, then replay it into the same table.",
    facts: ["A run that drifts from the approved plan is refused", "Accepting a known loss takes a signed risk record"],
    panel: <QuarantinePanel />,
  },
  {
    id: "verify",
    name: "Verify",
    title: "Read the destination back and compare",
    body: "After the write, Datawrap queries the destination itself and compares it with what was read, so verification is part of the run rather than a job someone schedules later.",
    facts: ["Row count, checksum, and a value census by key", "Each table records which comparison ran", "Only a full comparison is called proven"],
    panel: <VerifyPanel />,
  },
  {
    id: "evidence",
    name: "Evidence",
    title: "Hand the record to the people who sign it",
    body: "The job keeps the mapping decisions, every check result, the held rows, and the comparison. Engineering, the data owner, and compliance sign the same report.",
    facts: ["Signed proof pack, verifiable through the API", "Sign-off by role", "Tamper-evident audit log you can export"],
    panel: <EvidencePanel />,
  },
];

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

const CONTROLS: { term: string; detail: string }[] = [
  { term: "Single sign-on", detail: "OIDC and SAML, routed by email domain, with an idle timeout you set." },
  { term: "Provisioning", detail: "SCIM 2.0 maps identity-provider groups to workspace roles." },
  { term: "Roles, keys, and isolation", detail: "Viewer, operator, editor, and admin, each scoped to its own workspace. An API key never holds more than the person who made it." },
  { term: "Your own keys", detail: "Connector secrets are encrypted at rest and can be wrapped with your key in AWS KMS." },
  { term: "Audit log", detail: "Tamper-evident: every entry is signed and linked to the one before, and the chain can be verified and exported." },
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

/**
 * The path every load takes, as a selector: choosing a step swaps in that
 * step's product panel. Motion here only answers the click.
 */
function PathSelector() {
  const [active, setActive] = useState(0);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);
  const step = STEPS[active];

  const onKey = (e: KeyboardEvent<HTMLDivElement>) => {
    const last = STEPS.length - 1;
    let next = active;
    if (e.key === "ArrowRight" || e.key === "ArrowDown") next = active === last ? 0 : active + 1;
    else if (e.key === "ArrowLeft" || e.key === "ArrowUp") next = active === 0 ? last : active - 1;
    else if (e.key === "Home") next = 0;
    else if (e.key === "End") next = last;
    else return;
    e.preventDefault();
    setActive(next);
    tabs.current[next]?.focus();
  };

  return (
    <div className="mk-path">
      <div className="mk-path-track" role="tablist" aria-label="The five steps of a load" onKeyDown={onKey}>
        {STEPS.map((s, i) => (
          <button
            key={s.id}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`mk-path-tab-${s.id}`}
            aria-selected={i === active}
            aria-controls={`mk-path-panel-${s.id}`}
            tabIndex={i === active ? 0 : -1}
            className={`mk-path-stop${i === active ? " is-active" : ""}${i < active ? " is-past" : ""}`}
            onClick={() => setActive(i)}
          >
            <span className="mk-path-num" aria-hidden>
              {i + 1}
            </span>
            <span className="mk-path-name">{s.name}</span>
          </button>
        ))}
      </div>
      <div
        className="mk-path-body"
        role="tabpanel"
        id={`mk-path-panel-${step.id}`}
        aria-labelledby={`mk-path-tab-${step.id}`}
        key={step.id}
      >
        <div className="mk-path-copy">
          <h3>{step.title}</h3>
          <p>{step.body}</p>
          <ul className="mk-facts">
            {step.facts.map((fact) => (
              <li key={fact}>{fact}</li>
            ))}
          </ul>
        </div>
        <div className="mk-path-panel">{step.panel}</div>
      </div>
    </div>
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
];

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
        <div className="mk-app-surface mk-bento-ui">
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
          <div className="mk-hero-window">
            <PanelWindow path={`jobs/${HERO_JOB.id}`} caption="" designWidth={540} maxScale={1.3}>
              <TrustDrawer job={HERO_JOB} />
            </PanelWindow>
          </div>
          <div className="mk-hero-statement">
            <StatementHero />
          </div>
        </div>
      </section>

      <section className="mk-evidence" aria-label="What CI and the product prove">
        <div className="mk-wrap">
          <ul className="mk-evidence-list">
            <li>
              <RouteLink route="integrations" onNavigate={onNavigate} className="mk-evidence-item">
                {`${PROVEN_DRIVERS} of ${ALL_CONNECTORS.length} drivers proven in CI, each with a green run and no skipped test`}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="solution-sync" onNavigate={onNavigate} className="mk-evidence-item">
                {`${SYNC_MODE_COUNT_WORD[0].toUpperCase() + SYNC_MODE_COUNT_WORD.slice(1)} sync modes, from full overwrite to SCD Type 2`}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="product-pipelines" onNavigate={onNavigate} className="mk-evidence-item">
                {PROVEN_CDC_SOURCES.length
                  ? `Change data capture proven on ${cdcSourcesPhrase()}`
                  : "Change data capture from the database log"}
              </RouteLink>
            </li>
            <li>
              <RouteLink route="customers" onNavigate={onNavigate} className="mk-evidence-item">
                Every statement is signed, and you can verify it yourself
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

      <section className="mk-section mk-section--band" aria-labelledby="mk-steps-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-steps-title">The path every load takes</h2>
            <p>
              The same five steps run whether a person starts the load in Transfer Studio, a schedule
              fires, or an agent calls it through MCP. Each panel is an example built from a recorded run.
            </p>
          </header>
          <PathSelector />
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

      <section className="mk-section" aria-labelledby="mk-conn-title">
        <div className="mk-wrap mk-split">
          <header className="mk-section-head mk-section-head--stack">
            <h2 id="mk-conn-title">Coverage, stated by what CI proves</h2>
            <p>
              {catalogHonestyLead()} Every family goes through the same map, checks, and quarantine.
            </p>
            <dl className="mk-terms">
              <div>
                <dt>Sync modes</dt>
                <dd>
                  {SYNC_MODE_COUNT_WORD[0].toUpperCase() + SYNC_MODE_COUNT_WORD.slice(1)}: {syncModesList()}.
                  Each scheduled run is held to the standard of the first.
                </dd>
              </div>
              <div>
                <dt>Change data capture</dt>
                <dd>
                  {PROVEN_CDC_SOURCES.length
                    ? `Proven in CI on ${cdcSourcesPhrase()}. Inserts, updates, and deletes apply by key.`
                    : "Reads the database log. Inserts, updates, and deletes apply by key."}
                </dd>
              </div>
            </dl>
            <div className="mk-actions">
              <RouteLink route="integrations" onNavigate={onNavigate} className="mk-btn mk-btn--secondary">
                See every driver
              </RouteLink>
            </div>
          </header>
          <div className="mk-split-visual">
            <CoveragePanel />
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-controls-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-controls-title">Security and deployment</h2>
            <p>The same controls apply in Transfer Studio, on schedules, in Pilot, and over MCP.</p>
          </header>
          <dl className="mk-terms mk-terms--grid">
            {CONTROLS.map((c) => (
              <div key={c.term}>
                <dt>{c.term}</dt>
                <dd>{c.detail}</dd>
              </div>
            ))}
          </dl>
          <div className="mk-deploy">
            <div>
              <h3>Hosted</h3>
              <p>The default on every plan. Sign up and run transfers; connector credentials stay encrypted in your workspace.</p>
            </div>
            <div>
              <h3>Self-managed</h3>
              <p>
                An Enterprise plan option: your cloud or VPC, with Docker Compose or Helm. Jobs,
                schedules, and audit events live in your own MongoDB.
              </p>
            </div>
          </div>
          <p className="mk-inline-links">
            <RouteLink route="security" onNavigate={onNavigate}>
              Security overview
            </RouteLink>
            <RouteLink route="enterprise" onNavigate={onNavigate}>
              Enterprise controls
            </RouteLink>
          </p>
        </div>
      </section>

      <section className="mk-closer" aria-labelledby="mk-closing-title">
        <div className="mk-wrap">
          <h2 id="mk-closing-title">Run your first statement on your own data</h2>
          <p className="mk-closer-lead">
            Start with a free workspace and a table you know well, or ask for a scoped pilot on your
            own sources with a solutions engineer.
          </p>
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
      </section>
    </div>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
