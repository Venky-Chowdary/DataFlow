import { useRef, useState, type KeyboardEvent, type MouseEvent, type ReactNode } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { ProductShot } from "../components/marketing/ProductShot";
import {
  EvidencePanel,
  GatesPanel,
  PlanPanel,
  QuarantinePanel,
  ReconciliationLedger,
  RunPanel,
  ScheduleTimeline,
  VerifyPanel,
} from "../components/marketing/ProductPanels";
import {
  CONNECTOR_CLAIMS,
  MARKETING_STACK,
  PROVEN_CDC_SOURCES,
  catalogHonestyLead,
  cdcSourcesPhrase,
} from "../lib/provenEvidence";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";
import { DEMO_ROUTE, WORKSPACE_SHOT } from "../lib/workspaceShots";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

type Step = { id: string; name: string; title: string; body: string; facts: string[]; panel: ReactNode };

/* Every panel on this page is product UI rendered in code. The recorded-run
   panels carry the figures of the run in WORKSPACE_SHOT; the quarantine and
   schedule panels are labelled as example data. */
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
    body: "Nine core checks run on every route, plus the policy checks the route needs. Sample rows go through the real write path, so a value the destination cannot hold is a finding now, not a failed batch later.",
    facts: ["Nine core checks on every route, plus the route's own", "Approving the plan records a hash of it", "Execute stays locked until the plan is approved"],
    panel: <GatesPanel />,
  },
  {
    id: "execute",
    name: "Execute",
    title: "Run the approved plan, and hold what fails",
    body: "Rows are written in batches with bound parameters, by insert, merge, or a stored procedure. A row the destination rejects is held with its reason instead of stopping the load.",
    facts: ["A run that drifts from the approved plan is refused", "The destination is counted before and after", "Held rows replay into the same table"],
    panel: <RunPanel />,
  },
  {
    id: "verify",
    name: "Verify",
    title: "Read the destination back and compare",
    body: "After the write, Datawrap queries the destination itself and compares it with what was read, so verification is part of the run rather than a job someone schedules later.",
    facts: ["Row count, checksum, and a value census by key", "Each table records which comparison ran", "Only a full comparison is called proven"],
    panel: <VerifyPanel animate={false} />,
  },
  {
    id: "evidence",
    name: "Evidence",
    title: "Hand the record to the people who sign it",
    body: "The job keeps the mapping decisions, every check result, the held rows, and the comparison. Engineering, the data owner, and compliance sign the same report.",
    facts: ["Signed proof pack, verifiable through the API", "Sign-off by role", "Hash-chained audit log you can export"],
    panel: <EvidencePanel />,
  },
];

const DIFFERENT: { title: string; body: string; where: string }[] = [
  {
    title: "Verification is part of the move",
    body: "The run that writes the rows also counts the destination with the destination's own engine and compares it with the source. There is no separate reconciliation job to schedule, or to forget.",
    where: "Job space, Trust Report",
  },
  {
    title: "The plan predicts the run",
    body: "Validate pushes sample rows through the real write path before anything is written. The approved plan is hashed, and a run that no longer matches it is refused.",
    where: "Transfer Studio, Validate",
  },
  {
    title: "Nothing fails silently",
    body: "A row that cannot land is held with its column, value, and reason, then replayed into the same table once fixed. A row that is not landed, held, or skipped by a rule you set is reported as potential loss.",
    where: "Job space, Quarantine",
  },
  {
    title: "Evidence you can sign",
    body: "The report names the comparison that ran, and only a full comparison is called proven. Proof packs are signed and verifiable through the API, and every action lands in a hash-chained audit log.",
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
  { term: "Roles and keys", detail: "Viewer, Editor, and Admin. An API key never holds more than the person who made it." },
  { term: "Your own keys", detail: "Connector secrets are encrypted at rest and can be wrapped with your key in AWS KMS." },
  { term: "Audit chain", detail: "Every action is hash-chained and signed, and exports as CSV, JSON, or CEF." },
  { term: "Isolation", detail: "Jobs, connectors, schedules, and agent tools are scoped to the workspace that owns them." },
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
        <div className="mk-steps-visual" key={step.id}>
          {step.panel}
        </div>
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
            <h1 id="mk-hero-title">
              Every row,
              <br />
              accounted for.
            </h1>
            <p className="mk-lead">
              Datawrap moves data between databases, warehouses, files, and streams, and shows
              where every row went: landed and counted by the destination, held with a reason,
              or skipped by a rule you set.
            </p>
            <div className="mk-actions">
              <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
                Start free
              </button>
              <RouteLink route="contact" onNavigate={onNavigate} className="mk-btn mk-btn--ghost-on-dark mk-btn--lg">
                Contact sales
              </RouteLink>
            </div>
            <p className="mk-hero-note">Hosted, or in your own cloud.</p>
          </div>
          <div className="mk-hero-visual">
            <ReconciliationLedger />
          </div>
        </div>
      </section>

      <section className="mk-section mk-different" aria-labelledby="mk-different-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-different-title">What makes Datawrap different</h2>
            <p>Four things a migration usually leaves to scripts and spreadsheets, built into every run.</p>
          </header>
          <div className="mk-different-grid">
            {DIFFERENT.map((d) => (
              <article key={d.title}>
                <h3>{d.title}</h3>
                <p>{d.body}</p>
                <p className="mk-different-where">Where you see it: {d.where}</p>
              </article>
            ))}
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-steps-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-steps-title">Plan, validate, execute, verify, keep the evidence</h2>
            <p>
              The same path runs whether a person starts the load in Transfer Studio, a schedule
              fires, or an agent calls it through MCP. The panels are examples built from a
              recorded run.
            </p>
          </header>
          <StepTabs />
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-q-section-title">
        <div className="mk-wrap mk-split">
          <header className="mk-section-head mk-section-head--stack">
            <h2 id="mk-q-section-title">Rows that fail are held, then replayed</h2>
            <p>
              A row the destination refuses is quarantined on the job with its column, its value,
              and the reason. The job finishes as completed with quarantine, so a partial load
              never reads as a clean one.
            </p>
            <ul className="mk-facts">
              <li>Fix the cause, then replay into the same table</li>
              <li>Replay writes through the same writer as the original run</li>
              <li>Accepting a known loss takes a signed risk record, not a checkbox</li>
            </ul>
          </header>
          <div className="mk-split-visual">
            <QuarantinePanel />
          </div>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-sync-title">
        <div className="mk-wrap mk-split">
          <header className="mk-section-head mk-section-head--stack">
            <h2 id="mk-sync-title">Keep two systems in step</h2>
            <p>Put an approved transfer on a schedule. Each run is held to the standard of the first.</p>
            <dl className="mk-terms">
              <div>
                <dt>Sync modes</dt>
                <dd>Full refresh, append, upsert, mirror, incremental by a typed cursor, SCD Type 2, and CDC.</dd>
              </div>
              <div>
                <dt>Change data capture</dt>
                <dd>
                  {PROVEN_CDC_SOURCES.length
                    ? `Proven in CI on ${cdcSourcesPhrase()}. Inserts, updates, and deletes apply by key.`
                    : "Reads the database log. Inserts, updates, and deletes apply by key."}
                </dd>
              </div>
              <div>
                <dt>Every run is a job</dt>
                <dd>Each tick runs the same checks and the same read-back as a transfer started by hand.</dd>
              </div>
              <div>
                <dt>Schema drift</dt>
                <dd>A new or renamed source column waits in Map for review before the next run uses it.</dd>
              </div>
            </dl>
          </header>
          <div className="mk-split-visual">
            <ScheduleTimeline />
          </div>
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-verdict-title">
        <div className="mk-wrap">
          <div className="mk-split">
            <header className="mk-section-head mk-section-head--stack">
              <h2 id="mk-verdict-title">Five verdicts, and only one is called proven</h2>
              <p>
                Each table carries the comparison that actually ran: full checksum, key diff,
                sample, or count. The Trust Report turns it into a verdict a reviewer can sign, or
                says there is nothing to sign yet.
              </p>
            </header>
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
          <figure className="mk-shots">
            <ProductShot
              src={WORKSPACE_SHOT.transferMapStage}
              alt="Transfer Studio map step: every column from public.sales_orders mapped to SALES_ORDERS_REPORTING, each marked safe"
              surface="Transfer Studio, map step"
              route={{ source: DEMO_ROUTE.source, dest: DEMO_ROUTE.dest }}
            />
            <figcaption className="mk-caption">
              Transfer Studio as it ships: the map a reviewer approves before Validate runs.
            </figcaption>
          </figure>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-conn-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-conn-title">Connector coverage, stated by what CI proves</h2>
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
                            {item.proven ? <span className="mk-sr-only"> (proven in CI)</span> : null}
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
          <p className="mk-caption">Names in bold have a green CI run behind them; the others are implemented.</p>
          <div className="mk-actions">
            <RouteLink route="integrations" onNavigate={onNavigate} className="mk-btn mk-btn--secondary">
              Browse the connector catalog
            </RouteLink>
          </div>
        </div>
      </section>

      <section className="mk-section" aria-labelledby="mk-controls-title">
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
              <h3>Hosted workspace</h3>
              <p>Sign up and run transfers in the hosted service. Connector credentials stay encrypted in your workspace.</p>
            </div>
            <div>
              <h3>In your own cloud</h3>
              <p>
                Two container images, an API and a web app, run with Docker Compose or the Helm
                chart. Jobs, schedules, and audit events live in your own MongoDB.
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
            <RouteLink route="subprocessors" onNavigate={onNavigate}>
              Subprocessors
            </RouteLink>
          </p>
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
