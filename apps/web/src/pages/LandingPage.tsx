import { useEffect, useRef, useState, type MouseEvent, type ReactNode } from "react";
import {
  CoveragePanel,
  EvidencePanel,
  GatesPanel,
  PlanPanel,
  QuarantinePanel,
  ReconciliationLedger,
  ScheduleTimeline,
  VerifyPanel,
} from "../components/marketing/ProductPanels";
import {
  PROVEN_CDC_SOURCES,
  catalogHonestyLead,
  cdcSourcesPhrase,
} from "../lib/provenEvidence";
import { hashForPublicRoute, type PublicRoute } from "../lib/publicNavigation";

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

function useWide(query = "(min-width: 1024px)") {
  const [wide, setWide] = useState(() => {
    try {
      return window.matchMedia(query).matches;
    } catch {
      return true;
    }
  });
  useEffect(() => {
    let mq: MediaQueryList;
    try {
      mq = window.matchMedia(query);
    } catch {
      return;
    }
    const on = () => setWide(mq.matches);
    on();
    mq.addEventListener("change", on);
    return () => mq.removeEventListener("change", on);
  }, [query]);
  return wide;
}

/**
 * The five steps as scrollytelling: the text scrolls on the left while one
 * sticky panel on the right changes to the step crossing the middle of the
 * viewport. A panel mounts the first time its step is reached, so its
 * one-time motion plays where it can be seen. On phones each step is
 * followed by its own panel, and nothing is pinned.
 */
function StepsScrolly() {
  const wide = useWide();
  const [active, setActive] = useState(0);
  const [seen, setSeen] = useState<Set<number>>(() => new Set([0]));
  const items = useRef<Array<HTMLLIElement | null>>([]);

  useEffect(() => {
    if (!wide || typeof IntersectionObserver === "undefined") return;
    const io = new IntersectionObserver(
      (entries) => {
        for (const entry of entries) {
          if (!entry.isIntersecting) continue;
          const i = Number((entry.target as HTMLElement).dataset.step);
          setActive(i);
          setSeen((prev) => (prev.has(i) ? prev : new Set(prev).add(i)));
        }
      },
      { rootMargin: "-45% 0px -45% 0px" },
    );
    for (const el of items.current) if (el) io.observe(el);
    return () => io.disconnect();
  }, [wide]);

  return (
    <div className={`mk-scrolly${wide ? " is-wide" : ""}`}>
      <ol className="mk-scrolly-steps">
        {STEPS.map((s, i) => (
          <li
            key={s.id}
            ref={(el) => {
              items.current[i] = el;
            }}
            data-step={i}
            className={wide && i === active ? "is-active" : undefined}
            aria-current={wide && i === active ? "step" : undefined}
          >
            <p className="mk-scrolly-name">
              <span className="mk-step-num" aria-hidden>
                {i + 1}
              </span>
              {s.name}
            </p>
            <h3>{s.title}</h3>
            <p>{s.body}</p>
            <ul className="mk-facts">
              {s.facts.map((fact) => (
                <li key={fact}>{fact}</li>
              ))}
            </ul>
            {!wide ? <div className="mk-scrolly-inline">{s.panel}</div> : null}
          </li>
        ))}
      </ol>
      {wide ? (
        <div className="mk-scrolly-stage">
          <p className="mk-scrolly-progress" aria-hidden>
            Step {active + 1} of {STEPS.length}: {STEPS[active].name}
          </p>
          <div className="mk-scrolly-panels">
            {STEPS.map((s, i) => (
              <div
                key={s.id}
                className={`mk-scrolly-panel${i === active ? " is-active" : ""}`}
                aria-hidden={i === active ? undefined : true}
                inert={i === active ? undefined : true}
              >
                {seen.has(i) ? s.panel : null}
              </div>
            ))}
          </div>
        </div>
      ) : null}
    </div>
  );
}

export function LandingHome({ onGetStarted, onNavigate }: LandingHomeProps) {
  return (
    <div className="mk-page">
      <section className="mk-hero" aria-labelledby="mk-hero-title">
        <div className="mk-wrap mk-hero-grid">
          <div className="mk-hero-copy">
            <p className="mk-hero-kicker">Data migration and sync</p>
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
            <p className="mk-hero-note">Hosted by default. Self-managed in your own cloud on the Enterprise plan.</p>
          </div>
          <div className="mk-hero-visual">
            <ReconciliationLedger />
          </div>
        </div>
      </section>

      <section className="mk-section mk-different" aria-labelledby="mk-different-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-different-title">What goes wrong without it</h2>
            <p>Four things a migration usually leaves to scripts and spreadsheets, and what Datawrap does instead on every run.</p>
          </header>
          <dl className="mk-contrast">
            <div className="mk-contrast-head" aria-hidden>
              <span />
              <span>Without it</span>
              <span>With Datawrap</span>
            </div>
            {DIFFERENT.map((d) => (
              <div key={d.title} className="mk-contrast-row">
                <dt>{d.title}</dt>
                <dd className="mk-contrast-without">
                  <span className="mk-contrast-label">Without it: </span>
                  {d.without}
                </dd>
                <dd className="mk-contrast-with">
                  <span className="mk-contrast-label">With Datawrap: </span>
                  {d.with}
                  <span className="mk-different-where">Where you see it: {d.where}</span>
                </dd>
              </div>
            ))}
          </dl>
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
          <StepsScrolly />
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
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-conn-title">
        <div className="mk-wrap mk-split">
          <header className="mk-section-head mk-section-head--stack">
            <h2 id="mk-conn-title">Connector coverage, stated by what CI proves</h2>
            <p>
              {catalogHonestyLead()} Every family goes through the same map, checks, and
              quarantine.
            </p>
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
