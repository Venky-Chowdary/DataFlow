import { useEffect, useRef, useState, type CSSProperties, type ReactNode } from "react";
import { DtLogo } from "../components/DtLogo";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import { ComparisonSection } from "../components/landing/ComparisonSection";
import { TrustSection } from "../components/landing/TrustSection";
import { TestimonialSection } from "../components/landing/TestimonialSection";
import { fetchCatalogStats } from "../lib/api";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import { MarketingSectionFooter } from "../components/marketing/MarketingSectionFooter";
import type { PublicRoute } from "../lib/publicNavigation";

export interface LandingHomeProps {
  onLogin: () => void;
  onGetStarted: () => void;
  onNavigate: (route: PublicRoute) => void;
}

const MARQUEE_IDS = [
  "postgresql", "snowflake", "mysql", "mongodb", "bigquery", "redshift",
  "s3", "json", "csv", "dynamodb", "elasticsearch", "redis",
  "salesforce", "kafka",
];

function Reveal({ children, className = "" }: { children: ReactNode; className?: string }) {
  const reveal = useRevealOnScroll();
  return (
    <div ref={reveal.ref} className={`${reveal.className} ${className}`.trim()}>
      {children}
    </div>
  );
}

function KnowledgeField() {
  const [step, setStep] = useState(0);
  const items = [
    {
      label: "Use when",
      rule: "Source amount fields map to payment_amount",
      status: "pending" as const,
    },
    {
      label: "Approved mapping",
      rule: "order_amt → payment_amount (96% confidence)",
      status: "ok" as const,
    },
    {
      label: "Rejected alias",
      rule: "order_id should never map to email",
      status: "no" as const,
    },
  ];

  useEffect(() => {
    const id = window.setInterval(() => setStep((s) => (s + 1) % 3), 2800);
    return () => window.clearInterval(id);
  }, []);

  const current = items[step];

  return (
    <div className="lp-knowledge-field" key={step}>
      <span className="lp-knowledge-field-label">{current.label}</span>
      <p className="lp-knowledge-field-rule">{current.rule}</p>
      <div className="lp-knowledge-field-actions">
        {current.status === "pending" ? (
          <>
            <button type="button" className="lp-knowledge-btn lp-knowledge-btn--ok">Accept</button>
            <button type="button" className="lp-knowledge-btn lp-knowledge-btn--no">Reject</button>
          </>
        ) : current.status === "ok" ? (
          <span className="lp-knowledge-tag is-ok">Accepted into synonym dictionary</span>
        ) : (
          <span className="lp-knowledge-tag is-no">Blocked from future auto-maps</span>
        )}
      </div>
    </div>
  );
}

function ConnectorMarqueeBand() {
  const track = [...MARQUEE_IDS, ...MARQUEE_IDS];
  return (
    <div className="lp-marquee" aria-hidden>
      <div className="lp-marquee-track">
        {track.map((id, i) => (
          <span key={`${id}-${i}`} className="lp-marquee-item">
            <ConnectorIcon id={id} size={28} />
          </span>
        ))}
      </div>
      <div className="lp-marquee-track lp-marquee-track--reverse">
        {track.map((id, i) => (
          <span key={`b-${id}-${i}`} className="lp-marquee-item">
            <ConnectorIcon id={id} size={28} />
          </span>
        ))}
      </div>
    </div>
  );
}

/**
 * Count-up metric that animates once when scrolled into view. Honors
 * prefers-reduced-motion by rendering the final value immediately.
 */
function CountUpStat({
  target,
  prefix = "",
  suffix = "",
  label,
  detail,
  index,
}: {
  target: number;
  prefix?: string;
  suffix?: string;
  label: string;
  detail: string;
  index: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [value, setValue] = useState(0);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced || target === 0) {
      setValue(target);
      return;
    }

    let raf = 0;
    let started = false;
    const run = () => {
      if (started) return;
      started = true;
      const duration = 1100;
      const start = performance.now();
      const tick = (now: number) => {
        const t = Math.min(1, (now - start) / duration);
        const eased = 1 - Math.pow(1 - t, 3);
        setValue(Math.round(eased * target));
        if (t < 1) raf = window.requestAnimationFrame(tick);
      };
      raf = window.requestAnimationFrame(tick);
    };

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          run();
          observer.disconnect();
        }
      },
      { threshold: 0.4 },
    );
    observer.observe(el);
    return () => {
      observer.disconnect();
      window.cancelAnimationFrame(raf);
    };
  }, [target]);

  return (
    <div ref={ref} className="lp-outcome" style={{ "--reveal-i": index } as CSSProperties}>
      <strong className="lp-outcome-value">
        {prefix}
        {value.toLocaleString()}
        {suffix}
      </strong>
      <span className="lp-outcome-label">{label}</span>
      <span className="lp-outcome-detail">{detail}</span>
    </div>
  );
}

function OutcomesBand() {
  return (
    <section className="lp-section lp-outcomes" id="outcomes" aria-label="Outcomes">
      <Reveal>
        <div className="lp-section-head">
          <p className="lp-section-kicker">Outcomes</p>
          <h2>Built to prove every transfer</h2>
          <p>Governance isn&rsquo;t a dashboard afterthought — it&rsquo;s enforced on every run, before and after write.</p>
        </div>
      </Reveal>
      <Reveal className="lp-outcomes-grid">
        <CountUpStat index={0} target={8} label="Preflight gates" detail="Block bad writes before production" />
        <CountUpStat index={1} target={600} suffix="+" label="Transfer drivers" detail="Any source to any destination" />
        <CountUpStat index={2} target={100} suffix="%" label="Row & checksum proof" detail="Reconciled end-to-end after write" />
        <CountUpStat index={3} target={0} label="Silently dropped rows" detail="Bad rows are quarantined, never lost" />
      </Reveal>
    </section>
  );
}

/** Home marketing body — chrome (nav/footer) lives in MarketingChrome. */
function HeroStudioMock({ gateStep }: { gateStep: number }) {
  const gates = [
    "Schema contract",
    "Type coercion",
    "Nullability",
    "Destination probe",
    "Capacity",
    "Write plan",
    "Quarantine policy",
    "Reconcile plan",
  ];

  return (
    <div className="lp-hero-mock" aria-label="DataFlow Transfer Studio preview">
      <div className="lp-mock-shell">
        <aside className="lp-mock-side">
          <div className="lp-mock-side-title">
            <span className="lp-mock-org">
              <DtLogo size={16} />
              DataFlow
            </span>
            <DtIcon name="chevron-down" size={12} />
          </div>
          {[
            { id: "transfer", label: "Transfer Studio", icon: "transfer" as const, active: true },
            { id: "pilot", label: "Data Pilot", icon: "sparkle" as const, active: false },
            { id: "jobs", label: "Job Theater", icon: "jobs" as const, active: false },
            { id: "connectors", label: "Connectors", icon: "connectors" as const, active: false },
          ].map((item) => (
            <div key={item.id} className={`lp-mock-nav-item ${item.active ? "is-active" : ""}`}>
              <DtIcon name={item.icon} size={14} />
              {item.label}
            </div>
          ))}
          <div className="lp-mock-section-label">
            <span>Recent</span>
            <span className="lp-mock-section-actions">
              <DtIcon name="search" size={12} />
              <DtIcon name="plus" size={12} />
            </span>
          </div>
          {[
            { title: "Orders CSV → PostgreSQL", meta: "Completed · 12k rows", active: true, badge: "proof" },
            { title: "Mongo customers → BigQuery", meta: "Running · 64%", active: false, badge: "live" },
            { title: "S3 events → Snowflake", meta: "Queued", active: false, badge: "queued" },
          ].map((job) => (
            <div key={job.title} className={`lp-mock-job ${job.active ? "is-active" : ""}`}>
              <strong>{job.title}</strong>
              <span>{job.meta}</span>
              <em>{job.badge}</em>
            </div>
          ))}
        </aside>

        <div className="lp-mock-main">
          <div className="lp-mock-main-head">
            <span>Transfer Studio / Orders migration</span>
            <span className="lp-mock-live-chip">
              <span className="lp-mock-live-dot" aria-hidden />
              Live
            </span>
          </div>
          <div className="lp-mock-prompt">
            <div className="lp-mock-prompt-text">
              Map source <code>order_amt</code> to destination <code>payment_amount</code>, then run preflight before load.
            </div>
            <div className="lp-mock-avatar" aria-hidden>DF</div>
          </div>
          <div className="lp-mock-reply">
            Mapping <code>order_amt → payment_amount</code> at 96% confidence. Running eight preflight gates, then writing with reconciliation.
          </div>
          <div className="lp-mock-stat">
            <span>Preflight</span>
            <strong>{Math.min(gateStep + 1, 8)} / 8 gates passed</strong>
            <div className="lp-mock-progress" aria-hidden>
              <i style={{ width: `${((Math.min(gateStep + 1, 8)) / 8) * 100}%` }} />
            </div>
          </div>
          <div className="lp-mock-pr">
            <div className="lp-mock-pr-top">
              <strong>Proof report: Orders migration</strong>
              <span className="lp-mock-open">matched</span>
            </div>
            <div className="lp-mock-pr-meta">
              <span>Source 12,480</span>
              <span>·</span>
              <span>Target 12,480</span>
              <span>·</span>
              <span>Checksum OK</span>
            </div>
          </div>
        </div>

        <aside className="lp-mock-detail">
          <h3>Proof report: Orders migration</h3>
          <p>Independent reconciliation verifies row counts and content checksums after write.</p>
          <ul>
            <li>Source rows: 12,480</li>
            <li>Target rows: 12,480</li>
            <li>Checksum: matched</li>
          </ul>
          <div className="lp-mock-gates">
            {gates.map((g, i) => (
              <div key={g} className={`lp-mock-gate ${i <= gateStep ? "is-pass" : "is-pending"}`}>
                <span>{g}</span>
                <em>{i <= gateStep ? "pass" : "…"}</em>
              </div>
            ))}
          </div>
        </aside>
      </div>
    </div>
  );
}

export function LandingHome({ onLogin, onGetStarted, onNavigate }: LandingHomeProps) {
  const [liveDrivers, setLiveDrivers] = useState<number | null>(null);
  const [gateStep, setGateStep] = useState(0);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setLiveDrivers(s.transfer_live ?? s.live))
      .catch(() => setLiveDrivers(null));
  }, []);

  useEffect(() => {
    const prefersReduced = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReduced) {
      setGateStep(7);
      return;
    }
    const id = window.setInterval(() => setGateStep((s) => (s + 1) % 8), 900);
    return () => window.clearInterval(id);
  }, []);

  return (
    <>
      <section className="lp-hero">
        <span className="lp-hero-eyebrow">
          <span className="lp-hero-eyebrow-dot" aria-hidden />
          Universal data movement, proven end-to-end
        </span>
        <h1>Move any schema anywhere</h1>
        <p className="lp-hero-sub">
          Semantic mapping, eight preflight gates, and checksum proof — from Transfer Studio to MCP agents.
          {liveDrivers != null ? ` ${liveDrivers} transfer-ready drivers today.` : ""}
        </p>

        <div className="lp-hero-cta">
          <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
            Try DataFlow
          </button>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("product-transfer")}>
            See Transfer Studio
          </button>
        </div>

        <div className="lp-hero-value-strip" aria-label="Platform highlights">
          <article>
            <span className="lp-hero-value-icon" aria-hidden><DtIcon name="gate" size={22} /></span>
            <div>
              <strong>8 preflight gates</strong>
              <span>Block bad writes before production</span>
            </div>
          </article>
          <article>
            <span className="lp-hero-value-icon" aria-hidden><DtIcon name="check" size={22} /></span>
            <div>
              <strong>Checksum proof</strong>
              <span>Reconcile every load end-to-end</span>
            </div>
          </article>
          <article>
            <span className="lp-hero-value-icon" aria-hidden><DtIcon name="connectors" size={22} /></span>
            <div>
              <strong>{liveDrivers != null ? `${liveDrivers} transfer-ready` : "Transfer-ready"} drivers</strong>
              <span>Honest labels — catalog stubs stay planned</span>
            </div>
          </article>
        </div>

        <HeroStudioMock gateStep={gateStep} />
      </section>

      <section className="lp-logos" aria-label="Trusted stacks">
        <h5>Industry leaders move data with</h5>
        <div className="lp-logos-row">
          {["postgresql", "snowflake", "bigquery", "mongodb", "sqlserver", "s3"].map((id) => (
            <span key={id} className="lp-logo-item">
              <ConnectorIcon id={id} size={22} />
              {id}
            </span>
          ))}
        </div>
      </section>

      <OutcomesBand />

      <section className="lp-section lp-section-platform" id="platform">
        <Reveal>
          <div className="lp-section-head">
            <p className="lp-section-kicker">Platform</p>
            <h2>From source to proof in four steps</h2>
            <p>The same governed path in Transfer Studio, Data Pilot, MCP, and scheduled pipelines.</p>
          </div>
        </Reveal>
        <Reveal className="lp-platform-steps">
          {[
            { step: "01", title: "Connect", body: "Pick a transfer-ready driver or upload CSV, JSONL, and Parquet. Honest capability labels — no inflated marketplace counts.", icon: "connectors" as const },
            { step: "02", title: "Map", body: "Semantic column mapping with confidence scores and human review for ambiguous fields.", icon: "sparkle" as const },
            { step: "03", title: "Preflight", body: "Eight fail-fast gates — schema, types, capacity, and destination probes before write.", icon: "gate" as const },
            { step: "04", title: "Proof", body: "Job Theater shows batch progress, reconciliation, checksums, and quarantine for bad rows.", icon: "check" as const },
          ].map((item, i) => (
            <article key={item.step} className="lp-platform-step" style={{ "--reveal-i": i } as CSSProperties}>
              <span className="lp-platform-step-num">{item.step}</span>
              <span className="lp-platform-step-icon" aria-hidden>
                <DtIcon name={item.icon} size={22} />
              </span>
              <h3>{item.title}</h3>
              <p>{item.body}</p>
            </article>
          ))}
        </Reveal>
      </section>

      <section className="lp-section lp-section-band" id="product">
        <Reveal>
          <div className="lp-section-head">
            <p className="lp-section-kicker">Product</p>
            <h2>Every surface. One governed engine.</h2>
            <p>Transfer Studio plans the load. Job Theater proves it. Pipelines, Query, Pilot, and MCP reuse the same path — never a silent shortcut.</p>
          </div>
        </Reveal>
        <Reveal className="lp-product-cards lp-product-cards--six">
          {[
            { title: "Transfer Studio", body: "Map → preflight → write → reconcile in one wizard.", route: "product-transfer" as PublicRoute, icon: "transfer" as const },
            { title: "Job Theater", body: "Live phases, quarantine samples, and checksum proof.", route: "product-jobs" as PublicRoute, icon: "jobs" as const },
            { title: "Pipelines", body: "Hourly to weekly sync with watermarks and gates.", route: "product-pipelines" as PublicRoute, icon: "activity" as const },
            { title: "Query Playground", body: "Ad-hoc SQL and document queries with Studio handoff.", route: "product-query" as PublicRoute, icon: "database" as const },
            { title: "Data Pilot", body: "Natural-language triage on failed gates and maps.", route: "product-pilot" as PublicRoute, icon: "sparkle" as const },
            { title: "MCP Server", body: "Agent tools under RBAC — never raw passwords.", route: "product-mcp" as PublicRoute, icon: "zap" as const },
          ].map((card, i) => (
            <button
              key={card.title}
              type="button"
              className="lp-product-card"
              style={{ "--reveal-i": i } as CSSProperties}
              onClick={() => onNavigate(card.route)}
            >
              <span className="lp-product-card-icon" aria-hidden>
                <DtIcon name={card.icon} size={22} />
              </span>
              <h3>{card.title}</h3>
              <p>{card.body}</p>
              <span className="lp-section-link">Learn more →</span>
            </button>
          ))}
        </Reveal>
        <Reveal>
          <MarketingSectionFooter>
            <p className="lp-section-cta-text">See how teams ship governed migrations without brittle scripts.</p>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("customers")}>
              Hear from our customers
            </button>
          </MarketingSectionFooter>
        </Reveal>
      </section>

      <section className="lp-section" id="usecases" style={{ paddingTop: 0 }}>
        <Reveal>
          <div className="lp-section-head lp-section-head--left">
            <h2>Use cases</h2>
            <p>Use DataFlow to plan and execute complex data movement — from one-shot migrations to recurring syncs.</p>
          </div>
        </Reveal>
        <Reveal className="lp-usecases">
          {[
            {
              title: "Cross-schema migrations",
              items: [
                "Semantic column mapping across SQL, NoSQL, and files",
                "Type coercion with fail-fast preflight gates",
                "Checksum-proven loads into warehouses",
              ],
              cta: "Learn about migrations →",
              route: "solution-migrations" as PublicRoute,
            },
            {
              title: "Schema intelligence",
              items: [
                "Auto-detect roles like amount, email, and identifiers",
                "Review ambiguous maps before production write",
                "Backfill new fields when schemas drift",
              ],
              cta: "Learn about Data Pilot →",
              route: "product-pilot" as PublicRoute,
            },
            {
              title: "Governed recurring sync",
              items: [
                "Hourly, daily, and weekly pipelines",
                "Upsert, append, overwrite, and watermark incremental",
                "Quarantine bad rows without silent failure",
              ],
              cta: "Learn about sync →",
              route: "solution-sync" as PublicRoute,
            },
            {
              title: "Preflight & proof",
              items: [
                "Eight gates before any production write",
                "Destination probes and capacity checks",
                "Post-load reconciliation reports",
              ],
              cta: "Learn about Transfer Studio →",
              route: "product-transfer" as PublicRoute,
            },
            {
              title: "Agent-native ops",
              items: [
                "MCP server for Cursor, Claude, and VS Code",
                "Natural-language Data Pilot for triage",
                "Same governed engine under every surface",
              ],
              cta: "Learn about MCP →",
              route: "product-mcp" as PublicRoute,
            },
            {
              title: "Warehouse loading",
              items: [
                "Snowflake, BigQuery, and Redshift bulk paths",
                "Finance-ready row counts and checksums",
                "Scheduled refreshes from Pipelines",
              ],
              cta: "Learn about warehouses →",
              route: "solution-warehouse" as PublicRoute,
            },
          ].map((card, i) => (
            <article key={card.title} className="lp-usecase" style={{ "--reveal-i": i } as CSSProperties}>
              <h3>{card.title}</h3>
              <ul>
                {card.items.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
              {card.cta && card.route ? (
                <button type="button" className="lp-section-link lp-usecase-link" onClick={() => onNavigate(card.route)}>
                  {card.cta}
                </button>
              ) : null}
            </article>
          ))}
        </Reveal>
      </section>

      <ComparisonSection />
      <TestimonialSection onNavigate={onNavigate} />

      <section className="lp-section" id="customers">
        <Reveal>
          <div className="lp-section-head">
            <h2>Learn and work together</h2>
            <p>DataFlow is built for data teams with complex, multi-system stacks.</p>
          </div>
        </Reveal>
        <Reveal className="lp-together">
          <article className="lp-together-card">
            <div className="lp-together-visual">
              <KnowledgeField />
            </div>
            <h3>Learns your schemas &amp; mapping corrections</h3>
            <p>Semantic patterns, synonym dictionaries, and optional LLM fallback improve with every reviewed map.</p>
          </article>
          <article className="lp-together-card">
            <div className="lp-together-visual">
              <div className="lp-collab">
                <span>Transfer Studio</span>
                <span>Data Pilot</span>
                <span>MCP</span>
                <span>API</span>
              </div>
            </div>
            <h3>Works where your team works</h3>
            <p>Run Transfer Studio in the UI, ask Data Pilot in chat, or trigger governed transfers from MCP-connected agents.</p>
          </article>
          <article className="lp-together-card">
            <div className="lp-together-visual">
              <div className="lp-fleet">
                <div className="lp-fleet-card">
                  <ConnectorIcon id="csv" size={18} />
                  <span>CSV → Postgres</span>
                </div>
                <div className="lp-fleet-card">
                  <ConnectorIcon id="mongodb" size={18} />
                  <span>Mongo → BigQuery</span>
                </div>
                <div className="lp-fleet-card">
                  <ConnectorIcon id="s3" size={18} />
                  <span>S3 → Snowflake</span>
                </div>
                <strong>3 routes · 1 proof plan</strong>
              </div>
            </div>
            <h3>Multi-route, multi-destination projects</h3>
            <p>Move files, databases, and warehouses in one platform — with Job Theater visibility from queue to reconcile.</p>
          </article>
        </Reveal>
      </section>

      <section className="lp-section" id="tools">
        <Reveal>
          <div className="lp-section-head">
            <h2>Able to work with hundreds of systems</h2>
            <p>Native transfer drivers plus SQLAlchemy generics and file formats — with honest transfer-ready labels.</p>
          </div>
        </Reveal>
        <Reveal>
          <ConnectorMarqueeBand />
        </Reveal>
        <Reveal className="lp-tools">
          <div className="lp-tools-grid">
            {[
              ["postgresql", "PostgreSQL", "Read, write, upsert, incremental"],
              ["snowflake", "Snowflake", "Warehouse bulk loads with proof"],
              ["bigquery", "BigQuery", "Analytics destination routes"],
              ["mongodb", "MongoDB", "Document → relational mappings"],
              ["mysql", "MySQL", "Operational database sync"],
              ["s3", "Amazon S3", "Object store ingest and export"],
              ["redis", "Redis", "Key-value transfer paths"],
              ["elasticsearch", "Elasticsearch", "Search index destinations"],
            ].map(([id, name, blurb]) => (
              <div key={id} className="lp-tool">
                <ConnectorIcon id={id} size={28} />
                <strong>{name}</strong>
                <span>{blurb}</span>
              </div>
            ))}
          </div>
          <div className="lp-tools-featured">
            <article>
              <h4>PostgreSQL</h4>
              <p>DataFlow ships upserts and watermark incremental the way your warehouse team expects — with preflight before every write.</p>
            </article>
            <article>
              <h4>Snowflake &amp; BigQuery</h4>
              <p>Bulk-load destinations with reconciliation reports so finance and analytics can trust the row counts.</p>
            </article>
            <article>
              <h4>MCP &amp; Data Pilot</h4>
              <p>Tag DataFlow from Cursor or Claude to launch the same governed engine your UI already uses.</p>
            </article>
          </div>
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("integrations")}>
              Browse the connector catalog
            </button>
          </MarketingSectionFooter>
        </Reveal>
      </section>

      <TrustSection />

      <section className="lp-section" id="enterprise">
        <div className="lp-enterprise">
          <div>
            <h3>Need DataFlow for your enterprise?</h3>
            <p>
              DataFlow Enterprise adds workspace RBAC, SSO, audit trails, tenant controls, and the
              same governed transfer engine your team already trusts.
            </p>
          </div>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("enterprise")}>
            Learn about DataFlow Enterprise
          </button>
        </div>
      </section>

    </>
  );
}

/** @deprecated Prefer MarketingSite — kept for any direct imports. */
export function LandingPage(props: LandingHomeProps) {
  return <LandingHome {...props} />;
}
