import { useRef, useState, type KeyboardEvent, type MouseEvent } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { ProductShot } from "../components/marketing/ProductShot";
import { RunLedger } from "../components/marketing/RunLedger";
import {
  CONNECTOR_CLAIMS,
  MARKETING_STACK,
  PROVEN_CDC_SOURCES,
  catalogHonestyLead,
} from "../lib/provenEvidence";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";
import { DEMO_ROUTE, WORKSPACE_SHOT } from "../lib/workspaceShots";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

type Step = {
  id: string;
  name: string;
  title: string;
  body: string;
  facts: string[];
  shot: string;
  alt: string;
  surface: string;
  route?: { source: string; dest: string };
};

const STEPS: Step[] = [
  {
    id: "connect",
    name: "Connect",
    title: "Choose a source and a destination",
    body: "Pick saved connections, then a table, a SQL query, or a stored procedure as the source. Datawrap profiles a sample first, so the next steps work from real types and values.",
    facts: [
      "Table, SQL query, or stored procedure as the source",
      "A profiled sample before anything is written",
      "Credentials stay in the workspace vault",
    ],
    shot: WORKSPACE_SHOT.transferSource,
    alt: "Transfer Studio source step: Orders Postgres (demo), table sales_orders, with a profiled sample of 100 rows",
    surface: "Transfer Studio, source step",
  },
  {
    id: "map",
    name: "Map",
    title: "Pair columns by meaning, and see why",
    body: "Each pairing is scored on the column's role, its qualifier, and type fit. Confident pairs map themselves; ambiguous ones wait for a person, with the reason written on the row.",
    facts: [
      "11 of 11 columns mapped on this run",
      "JSONB lands in Snowflake as VARIANT",
      "Pairs below the confidence floor wait for review",
    ],
    shot: WORKSPACE_SHOT.transferMapStage,
    alt: "Transfer Studio map step: 11 of 11 columns from public.sales_orders mapped to SALES_ORDERS_REPORTING",
    surface: "Transfer Studio, map step",
    route: { source: DEMO_ROUTE.source, dest: DEMO_ROUTE.dest },
  },
  {
    id: "validate",
    name: "Validate",
    title: "Check the plan before the first write",
    body: "Nine core checks run on every route, from source access to a dry run of the real write path on sample rows, plus the policy checks the route needs. Execute stays locked until the plan is approved.",
    facts: [
      "23 checks on this run: 17 passed, 6 not applicable",
      "A value the destination cannot hold is caught here",
      "New tables wait for DDL sign-off",
    ],
    shot: WORKSPACE_SHOT.transferValidateGates,
    alt: "Validation rules grid for the Postgres to Snowflake route: 23 checks, 17 passed and 6 not applicable",
    surface: "Transfer Studio, validate step",
  },
  {
    id: "write",
    name: "Write",
    title: "Write in batches, and hold what fails",
    body: "Rows are written in batches with bound parameters, by insert, merge, or a stored procedure. A row the destination refuses is held in quarantine with its column, value, and reason instead of stopping the load.",
    facts: [
      "5,100 rows read and 5,100 written",
      "Held rows replay into the same table once fixed",
      "A live log for every batch",
    ],
    shot: WORKSPACE_SHOT.transferRunComplete,
    alt: "Transfer Studio run complete: 5,100 rows appended to Snowflake, append delta balanced",
    surface: "Transfer Studio, run step",
  },
  {
    id: "verify",
    name: "Verify",
    title: "Read the destination back and compare",
    body: "After the write, Datawrap queries the destination itself and compares it with what was read: row counts, an order-independent checksum, and a value census by key. The Trust Report records which comparison ran.",
    facts: [
      "5,100 in the source and 5,100 in Snowflake",
      "Full checksum, every row compared",
      "Export the report and route it for sign-off",
    ],
    shot: WORKSPACE_SHOT.jobTrust,
    alt: "Trust Report for the Snowflake run: 5,100 rows in source and destination, full checksum match",
    surface: "Job space, Trust Report",
  },
];

const VERDICTS: { name: string; tone: "proven" | "explained" | "bounded" | "open" | "none"; meaning: string }[] = [
  { name: "Proven", tone: "proven", meaning: "Every row was compared and none differs." },
  {
    name: "Explained",
    tone: "explained",
    meaning: "Every row was compared. Some differ, and each difference has a known cause, such as time zone precision or a transform you applied.",
  },
  {
    name: "No difference found",
    tone: "bounded",
    meaning: "Values were compared on a random sample. The report states an upper bound on the rate of unexplained differences.",
  },
  { name: "Unexplained", tone: "open", meaning: "At least one difference has no known cause. The report points to the column and the row." },
  { name: "Unverifiable", tone: "none", meaning: "There is nothing to sign yet: the job is still running, or only row counts exist." },
];

const SCHEDULE_FACTS: { term: string; detail: string }[] = [
  {
    term: "Sync modes",
    detail: "Full refresh, append, upsert, mirror, incremental by a typed cursor, SCD Type 2, and CDC.",
  },
  {
    term: "Change data capture",
    detail: PROVEN_CDC_SOURCES.length
      ? `Reads the database log on ${listPhrase(PROVEN_CDC_SOURCES)}, and applies inserts, updates, and deletes by key.`
      : "Reads the database log and applies inserts, updates, and deletes by key.",
  },
  {
    term: "Every run is a job",
    detail: "Each tick runs the same checks and the same read-back as a transfer started by hand, and links to its own report.",
  },
  {
    term: "Schema drift",
    detail: "A change at the source is held for review instead of being applied to the destination on its own.",
  },
];

const CONTROLS: { term: string; detail: string }[] = [
  { term: "Single sign-on", detail: "OIDC and SAML, routed by email domain, with an idle timeout you set." },
  { term: "Provisioning", detail: "SCIM 2.0 maps identity-provider groups to workspace roles." },
  { term: "Roles and keys", detail: "Viewer, Editor, and Admin. An API key can never hold more than the person who made it." },
  { term: "Your own keys", detail: "Connector secrets are encrypted at rest and can be wrapped with your key in AWS KMS." },
  { term: "Audit chain", detail: "Every action is hash-chained and signed, and exports as CSV, JSON, or CEF." },
  { term: "Isolation", detail: "Jobs, connectors, schedules, and agent tools are scoped to the workspace that owns them." },
];

const SAME_ENGINE: { route: PublicRoute; name: string; body: string }[] = [
  { route: "product-query", name: "Query", body: "Read-only SQL against any saved connection. Statements that write are refused." },
  { route: "product-pilot", name: "Pilot", body: "Ask how many rows a table holds and get an exact server-side answer, with the SQL under it." },
  { route: "product-mcp", name: "MCP server", body: "Cursor, Claude, and VS Code call the same governed tools, under the role of their API key." },
  { route: "product-jobs", name: "Reversibility", body: "Column by column: can the destination give the source value back? Hashes and narrowing casts are named." },
];

function listPhrase(items: string[]): string {
  if (items.length < 2) return items.join("");
  return `${items.slice(0, -1).join(", ")}, and ${items[items.length - 1]}`;
}

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

function StepTabs() {
  const [active, setActive] = useState(0);
  const tabs = useRef<Array<HTMLButtonElement | null>>([]);

  const onKey = (e: KeyboardEvent<HTMLButtonElement>, index: number) => {
    const last = STEPS.length - 1;
    let next = -1;
    if (e.key === "ArrowDown" || e.key === "ArrowRight") next = index === last ? 0 : index + 1;
    if (e.key === "ArrowUp" || e.key === "ArrowLeft") next = index === 0 ? last : index - 1;
    if (e.key === "Home") next = 0;
    if (e.key === "End") next = last;
    if (next < 0) return;
    e.preventDefault();
    setActive(next);
    tabs.current[next]?.focus();
  };

  const step = STEPS[active];
  return (
    <div className="mk-steps">
      <div className="mk-steps-list" role="tablist" aria-orientation="vertical" aria-label="Steps of a transfer">
        {STEPS.map((s, i) => (
          <button
            key={s.id}
            ref={(el) => {
              tabs.current[i] = el;
            }}
            type="button"
            role="tab"
            id={`mk-step-tab-${s.id}`}
            aria-selected={i === active}
            aria-controls="mk-step-panel"
            tabIndex={i === active ? 0 : -1}
            className="mk-step-tab"
            onClick={() => setActive(i)}
            onKeyDown={(e) => onKey(e, i)}
          >
            <span className="mk-step-num">{i + 1}</span>
            <span className="mk-step-name">{s.name}</span>
            <span className="mk-step-title">{s.title}</span>
          </button>
        ))}
      </div>
      <div className="mk-steps-panel" role="tabpanel" id="mk-step-panel" aria-labelledby={`mk-step-tab-${step.id}`}>
        <div className="mk-steps-copy">
          <h3>{step.title}</h3>
          <p>{step.body}</p>
          <ul className="mk-facts">
            {step.facts.map((fact) => (
              <li key={fact}>{fact}</li>
            ))}
          </ul>
        </div>
        <ProductShot key={step.id} src={step.shot} alt={step.alt} surface={step.surface} route={step.route} />
      </div>
    </div>
  );
}

export function LandingHome({ onGetStarted, onNavigate }: LandingHomeProps) {
  return (
    <div className="mk-page">
      <section className="mk-hero" aria-labelledby="mk-hero-title">
        <div className="mk-wrap mk-hero-grid">
          <div className="mk-hero-copy">
            <h1 id="mk-hero-title">Move data between systems, then count both sides.</h1>
            <p className="mk-lead">
              Datawrap maps columns by meaning, checks the plan before the first write, and
              afterwards reads the destination back to compare it with the source. Rows that
              cannot be written wait in quarantine with a reason, ready to replay.
            </p>
            <div className="mk-actions">
              <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
                Start free
              </button>
              <RouteLink route="contact" onNavigate={onNavigate} className="mk-btn mk-btn--secondary mk-btn--lg">
                Contact sales
              </RouteLink>
            </div>
            <p className="mk-hero-note">
              Databases, warehouses, object storage, files, streams, and vector stores.{" "}
              <RouteLink route="integrations" onNavigate={onNavigate}>
                See connectors
              </RouteLink>
            </p>
          </div>
          <div className="mk-hero-ledger">
            <RunLedger />
            <p className="mk-caption">
              Figures from a recorded run in the hosted workspace, the same run shown in every
              screenshot on this page.
            </p>
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-steps-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-steps-title">One path for every transfer</h2>
            <p>
              The same five steps run whether a person starts the load in Transfer Studio, a
              schedule fires, or an agent calls it through MCP. None of them can skip the checks
              or the read-back.
            </p>
          </header>
          <StepTabs />
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-write-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-write-title">Before, during, and after the write</h2>
            <p>The parts of a migration that usually live in a spreadsheet are part of the job.</p>
          </header>
          <div className="mk-spec">
            <article className="mk-spec-row">
              <h3>The approved plan is the plan that runs</h3>
              <div>
                <p>
                  Validate pushes sample rows through the real write path and predicts how each
                  value will land, so a decimal the destination cannot hold is a finding before
                  the load, not a failed batch during it.
                </p>
                <ul className="mk-facts">
                  <li>Approving a plan records a hash of it</li>
                  <li>A run whose plan no longer matches is refused</li>
                  <li>Accepting a known loss takes a signed risk record, not a checkbox</li>
                </ul>
              </div>
            </article>
            <article className="mk-spec-row">
              <h3>Rows that fail are held, then replayed</h3>
              <div>
                <p>
                  A row the destination refuses is quarantined on the job with its column, its
                  value, and the reason. The job finishes as completed with quarantine, so a
                  partial load never reads as a clean one.
                </p>
                <ul className="mk-facts">
                  <li>Fix the cause, then replay into the same table</li>
                  <li>Replay writes through the same writer as the screen</li>
                  <li>Read equals landed plus held plus skipped, on every counted load</li>
                </ul>
              </div>
            </article>
            <article className="mk-spec-row">
              <h3>The verdict names its evidence</h3>
              <div>
                <p>
                  Each table in a result carries the comparison that actually ran: full checksum,
                  key diff, sample, or count. The Trust Report turns that into one of five
                  verdicts, and only a full comparison is called proven.
                </p>
                <table className="mk-verdicts">
                  <caption>Trust Report verdicts</caption>
                  <thead>
                    <tr>
                      <th scope="col">Verdict</th>
                      <th scope="col">What it means</th>
                    </tr>
                  </thead>
                  <tbody>
                    {VERDICTS.map((v) => (
                      <tr key={v.name}>
                        <th scope="row">
                          <span className={`mk-verdict mk-verdict--${v.tone}`}>{v.name}</span>
                        </th>
                        <td>{v.meaning}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </article>
            <article className="mk-spec-row">
              <h3>Evidence you can hand over</h3>
              <div>
                <p>
                  The cutover meeting gets the job itself: the mapping decisions, every check
                  result, the held rows, and the comparison, with the sign-offs on record.
                </p>
                <ul className="mk-facts">
                  <li>Signed proof pack, verifiable through the API</li>
                  <li>Sign-off by role: engineering, data owner, compliance</li>
                  <li>A hash-chained audit log you can export</li>
                </ul>
              </div>
            </article>
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-sync-title">
        <div className="mk-wrap mk-split">
          <header className="mk-section-head mk-section-head--stack">
            <h2 id="mk-sync-title">Keep two systems in step</h2>
            <p>
              Put an approved transfer on a schedule. Each run is held to the standard of the
              first one.
            </p>
            <dl className="mk-terms">
              {SCHEDULE_FACTS.map((f) => (
                <div key={f.term}>
                  <dt>{f.term}</dt>
                  <dd>{f.detail}</dd>
                </div>
              ))}
            </dl>
            <RouteLink route="product-pipelines" onNavigate={onNavigate}>
              How schedules run
            </RouteLink>
          </header>
          <ProductShot
            src={WORKSPACE_SHOT.schedulesHistory}
            alt="Run history for the Orders feed schedule, each run linked to its job"
            surface="Schedules, run history"
          />
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-conn-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-conn-title">Connectors, counted by what CI proves</h2>
            <p>
              {catalogHonestyLead()} Every family goes through the same map, checks, and
              quarantine.
            </p>
          </header>
          <table className="mk-families">
            <caption>Connector families</caption>
            <thead>
              <tr>
                <th scope="col">Family</th>
                <th scope="col">Members</th>
                <th scope="col">Status</th>
              </tr>
            </thead>
            <tbody>
              {MARKETING_STACK.map((row) => {
                const family = CONNECTOR_CLAIMS.families.find((f) => f.family === row.family);
                return (
                  <tr key={row.family}>
                    <th scope="row">{row.family}</th>
                    <td>
                      <ul className="mk-family-members">
                        {(family?.items ?? []).map((item) => (
                          <li key={item.driver} className={item.proven ? "is-proven" : undefined}>
                            <ConnectorIcon id={item.driver} size={16} />
                            {item.name}
                          </li>
                        ))}
                      </ul>
                    </td>
                    <td>{row.badge}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
          <p className="mk-caption">Names in bold have a green CI run behind them.</p>
          <div className="mk-actions">
            <RouteLink route="integrations" onNavigate={onNavigate} className="mk-btn mk-btn--secondary">
              Browse the connector catalog
            </RouteLink>
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-controls-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-controls-title">Controls your security review will ask about</h2>
            <p>
              The same controls apply in Transfer Studio, on schedules, in Pilot, and over MCP.
            </p>
          </header>
          <dl className="mk-terms mk-terms--grid">
            {CONTROLS.map((c) => (
              <div key={c.term}>
                <dt>{c.term}</dt>
                <dd>{c.detail}</dd>
              </div>
            ))}
          </dl>
          <p className="mk-inline-links">
            <RouteLink route="security" onNavigate={onNavigate}>
              Security overview
            </RouteLink>
            <RouteLink route="enterprise" onNavigate={onNavigate}>
              Enterprise controls
            </RouteLink>
            <RouteLink route="subprocessors" onNavigate={onNavigate}>
              Subprocessors
            </RouteLink>
          </p>
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-engine-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-engine-title">On the same engine</h2>
            <p>Looking at the data, asking about it, and handing it to an agent use the same connections and roles.</p>
          </header>
          <ul className="mk-engine">
            {SAME_ENGINE.map((item) => (
              <li key={item.name}>
                <h3>
                  <RouteLink route={item.route} onNavigate={onNavigate}>
                    {item.name}
                  </RouteLink>
                </h3>
                <p>{item.body}</p>
              </li>
            ))}
          </ul>
        </div>
      </section>

      <section className="mk-closing" aria-labelledby="mk-closing-title">
        <div className="mk-wrap mk-closing-grid">
          <div>
            <h2 id="mk-closing-title">Run a transfer on your own data</h2>
            <p>
              Start with a free workspace and a table you know well, or ask for a scoped pilot on
              your own sources with a solutions engineer.
            </p>
          </div>
          <div className="mk-closing-actions">
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
        </div>
      </section>
    </div>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
