import { useEffect, useState, type CSSProperties, type ReactNode } from "react";
import { DtLogo } from "../components/DtLogo";
import { DtIcon } from "../components/DtIcon";
import { ConnectorIcon } from "../app/brand-icons";
import { fetchCatalogStats } from "../lib/api";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";

interface LandingPageProps {
  onEnterApp: () => void;
  onStartTransfer: () => void;
  onOpenPilot?: () => void;
  onOpenMcp?: () => void;
}

type NavMenu = "product" | "solutions" | "resources" | null;

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

/** Devin.ai layout ditto — DataFlow product copy and surfaces. */
export function LandingPage({ onEnterApp, onStartTransfer, onOpenPilot, onOpenMcp }: LandingPageProps) {
  const [navOpen, setNavOpen] = useState(false);
  const [menu, setMenu] = useState<NavMenu>(null);
  const [scrolled, setScrolled] = useState(false);
  const [liveDrivers, setLiveDrivers] = useState<number | null>(null);

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 4);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setLiveDrivers(s.transfer_live ?? s.live))
      .catch(() => setLiveDrivers(null));
  }, []);

  const closeMenus = () => {
    setMenu(null);
    setNavOpen(false);
  };

  return (
    <div className="lp" onMouseLeave={() => setMenu(null)}>
      <header className={`lp-nav ${scrolled ? "is-scrolled" : ""}`}>
        <a className="lp-nav-brand" href="/" onClick={(e) => e.preventDefault()}>
          <DtLogo size={26} />
          <span>DataFlow</span>
        </a>

        <button
          type="button"
          className="lp-nav-toggle"
          aria-label="Toggle menu"
          aria-expanded={navOpen}
          onClick={() => setNavOpen((o) => !o)}
        >
          <DtIcon name="menu" size={18} />
        </button>

        <nav className={`lp-nav-links ${navOpen ? "is-open" : ""}`}>
          <div
            className={`lp-nav-item ${menu === "product" ? "is-open" : ""}`}
            onMouseEnter={() => setMenu("product")}
          >
            <button type="button" className="lp-nav-link" aria-expanded={menu === "product"}>
              Product <DtIcon name="chevron-down" size={12} />
            </button>
            <div className="lp-nav-dropdown">
              <button type="button" onClick={() => { closeMenus(); onStartTransfer(); }}>
                <strong>Transfer Studio</strong>
                <span>Map, preflight, and prove any→any loads</span>
              </button>
              <button type="button" onClick={() => { closeMenus(); (onOpenPilot ?? onEnterApp)(); }}>
                <strong>Data Pilot</strong>
                <span>Natural-language triage for transfers</span>
              </button>
              <button type="button" onClick={() => { closeMenus(); (onOpenMcp ?? onEnterApp)(); }}>
                <strong>MCP Server</strong>
                <span>Governed transfers from Cursor &amp; Claude</span>
              </button>
              <a href="#tools" onClick={closeMenus}>
                <strong>Connectors</strong>
                <span>Native drivers + SQLAlchemy generics</span>
              </a>
            </div>
          </div>

          <div
            className={`lp-nav-item ${menu === "solutions" ? "is-open" : ""}`}
            onMouseEnter={() => setMenu("solutions")}
          >
            <button type="button" className="lp-nav-link" aria-expanded={menu === "solutions"}>
              Solutions <DtIcon name="chevron-down" size={12} />
            </button>
            <div className="lp-nav-dropdown">
              <a href="#usecases" onClick={closeMenus}>
                <strong>Migrations</strong>
                <span>Cross-schema moves with proof</span>
              </a>
              <a href="#usecases" onClick={closeMenus}>
                <strong>Warehouse loading</strong>
                <span>Snowflake, BigQuery, Redshift routes</span>
              </a>
              <a href="#usecases" onClick={closeMenus}>
                <strong>Recurring sync</strong>
                <span>Incremental pipelines with quarantine</span>
              </a>
            </div>
          </div>

          <a href="#customers" className="lp-nav-link" onClick={closeMenus}>Customers</a>

          <div
            className={`lp-nav-item ${menu === "resources" ? "is-open" : ""}`}
            onMouseEnter={() => setMenu("resources")}
          >
            <button type="button" className="lp-nav-link" aria-expanded={menu === "resources"}>
              Resources <DtIcon name="chevron-down" size={12} />
            </button>
            <div className="lp-nav-dropdown">
              <button type="button" onClick={() => { closeMenus(); onEnterApp(); }}>
                <strong>Docs</strong>
                <span>Guides for Transfer Studio &amp; drivers</span>
              </button>
              <a href="#enterprise" onClick={closeMenus}>
                <strong>Enterprise</strong>
                <span>SSO, RBAC, audit, tenants</span>
              </a>
              <a href="#tools" onClick={closeMenus}>
                <strong>Connector catalog</strong>
                <span>Honest transfer-ready labels</span>
              </a>
            </div>
          </div>

          <a href="#enterprise" className="lp-nav-link" onClick={closeMenus}>Pricing</a>
        </nav>

        <div className="lp-nav-actions">
          <button type="button" className="lp-btn lp-btn--ghost" onClick={onEnterApp}>Contact sales</button>
          <button type="button" className="lp-btn lp-btn--outline" onClick={onStartTransfer}>Get started</button>
          <button type="button" className="lp-btn lp-btn--black" onClick={onEnterApp}>Log in</button>
        </div>
      </header>

      <section className="lp-hero">
        <a className="lp-pill" href="#product" onClick={(e) => e.preventDefault()}>
          <span className="lp-pill-new">NEW</span>
          Introducing Transfer Studio proof dashboard
          <DtIcon name="arrow-up-right" size={14} />
        </a>

        <h1>DataFlow, the universal data platform</h1>

        <div className="lp-hero-cta">
          <button type="button" className="lp-btn lp-btn--black lp-btn--lg" onClick={onStartTransfer}>
            Try DataFlow
          </button>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={onEnterApp}>
            Contact sales
          </button>
        </div>

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
                { title: "Orders CSV → PostgreSQL", meta: "Completed · 12k rows", active: true, badge: "1 open" },
                { title: "Mongo customers → BigQuery", meta: "Running · 64%", active: false, badge: "2 open" },
                { title: "S3 events → Snowflake", meta: "Queued", active: false, badge: "1 merged" },
              ].map((job) => (
                <div key={job.title} className={`lp-mock-job ${job.active ? "is-active" : ""}`}>
                  <strong>{job.title}</strong>
                  <span>{job.meta}</span>
                  <em>{job.badge}</em>
                </div>
              ))}
            </aside>

            <div className="lp-mock-main">
              <div className="lp-mock-main-head">Transfer Studio / Orders migration</div>
              <div className="lp-mock-prompt">
                <div className="lp-mock-prompt-text">
                  Map source <code>order_amt</code> to destination <code>payment_amount</code>, then run preflight before load.
                </div>
                <div className="lp-mock-avatar" aria-hidden>DF</div>
              </div>
              <div className="lp-mock-reply">
                Mapping <code>order_amt → payment_amount</code> at 96% confidence. Running eight preflight gates, then writing with reconciliation. Starting now.
              </div>
              <div className="lp-mock-stat">
                <span>Worked for 4m 13s</span>
                <span className="lp-mock-diff"><em>+12,480</em> <i>−0</i></span>
              </div>
              <div className="lp-mock-pr">
                <div className="lp-mock-pr-top">
                  <span className="lp-mock-open">Open</span>
                  <strong>Orders CSV → PostgreSQL</strong>
                </div>
                <div className="lp-mock-pr-meta">
                  <ConnectorIcon id="csv" size={14} />
                  orders_export.csv
                  <DtIcon name="transfer" size={12} />
                  <ConnectorIcon id="postgresql" size={14} />
                  public.payments
                </div>
                <div className="lp-mock-pr-stats">+12,480 rows · 8/8 gates</div>
              </div>
              <div className="lp-mock-pr">
                <div className="lp-mock-pr-top">
                  <span className="lp-mock-open">Open</span>
                  <strong>Semantic map review</strong>
                </div>
                <div className="lp-mock-pr-meta">order_amt · customer_email · created_at</div>
                <div className="lp-mock-pr-stats">96% avg confidence · 1 needs review</div>
              </div>
            </div>

            <div className="lp-mock-detail">
              <div className="lp-mock-filetab">proof_orders_migration.md</div>
              <h3>Proof report: Orders migration</h3>
              <p>
                Independent reconciliation verifies row counts and content checksums after write.
                {liveDrivers != null ? ` ${liveDrivers} transfer-ready drivers online.` : ""}
              </p>
              <div className="lp-mock-compare">
                <div>
                  <span className="lp-mock-compare-label">Before mapping</span>
                  <strong className="lp-mock-before">order_amt</strong>
                </div>
                <div>
                  <span className="lp-mock-compare-label">After mapping</span>
                  <strong className="lp-mock-after">payment_amount</strong>
                </div>
              </div>
              <div className="lp-mock-gates">
                {[
                  ["G1 Schema contract", "Pass"],
                  ["G4 Mapping confidence", "Pass"],
                  ["G5 Dry-run transform", "Pass"],
                  ["G8 Reconciliation", "Pass"],
                ].map(([name, status]) => (
                  <div key={name} className="lp-mock-gate">
                    <span>{name}</span>
                    <em>{status}</em>
                  </div>
                ))}
              </div>
              <ul>
                <li>Source rows: 12,480</li>
                <li>Target rows: 12,480</li>
                <li>Checksum: matched</li>
              </ul>
            </div>
          </div>
        </div>
      </section>

      <section className="lp-logos" aria-label="Trusted stacks">
        <Reveal>
          <h5>Industry leaders move data with</h5>
          <div className="lp-logos-row">
            {[
              ["postgresql", "PostgreSQL"],
              ["snowflake", "Snowflake"],
              ["bigquery", "BigQuery"],
              ["mongodb", "MongoDB"],
              ["mysql", "MySQL"],
              ["s3", "Amazon S3"],
              ["redshift", "Redshift"],
              ["elasticsearch", "Elastic"],
            ].map(([id, label]) => (
              <span key={id} className="lp-logo-item">
                <ConnectorIcon id={id} size={22} />
                {label}
              </span>
            ))}
          </div>
        </Reveal>
      </section>

      <section className="lp-section" id="product">
        <Reveal>
          <div className="lp-section-head">
            <h2>Build with DataFlow</h2>
            <p>Governed any-schema transfers for migration, sync, and warehouse loading — with proof before and after every run.</p>
            <button type="button" className="lp-section-link" onClick={onEnterApp}>
              Hear from our customers →
            </button>
          </div>
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
              cta: "Learn about Transfer Studio →",
              action: onStartTransfer,
            },
            {
              title: "Schema intelligence",
              items: [
                "Auto-detect roles like amount, email, and identifiers",
                "Review ambiguous maps before production write",
                "Backfill new fields when schemas drift",
              ],
              cta: "Learn about Data Pilot →",
              action: onOpenPilot ?? onEnterApp,
            },
            {
              title: "Governed recurring sync",
              items: [
                "Hourly, daily, and weekly pipelines",
                "Upsert, append, overwrite, and watermark incremental",
                "Quarantine bad rows without silent failure",
              ],
              cta: "Learn about Pipelines →",
              action: onEnterApp,
            },
            {
              title: "Preflight & proof",
              items: [
                "Eight gates before any production write",
                "Destination probes and capacity checks",
                "Post-load reconciliation reports",
              ],
            },
            {
              title: "Agent-native ops",
              items: [
                "MCP server for Cursor, Claude, and VS Code",
                "Natural-language Data Pilot for triage",
                "Same governed engine under every surface",
              ],
              cta: "Learn about MCP →",
              action: onOpenMcp ?? onEnterApp,
            },
            {
              title: "And many others",
              items: [
                "File → database and database → file dumps",
                "Locale and currency integrity transfers",
                "Object store routes across S3, GCS, and ADLS",
                "Query playground across live connectors",
                "Job Theater from queue to reconcile",
              ],
            },
          ].map((card, i) => (
            <article key={card.title} className="lp-usecase" style={{ "--reveal-i": i } as CSSProperties}>
              <h3>{card.title}</h3>
              <ul>
                {card.items.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
              {card.cta && card.action ? (
                <button type="button" className="lp-section-link lp-usecase-link" onClick={card.action}>
                  {card.cta}
                </button>
              ) : null}
            </article>
          ))}
        </Reveal>
      </section>

      <section className="lp-section" id="customers">
        <Reveal>
          <div className="lp-section-head">
            <h2>
              Learn &amp; work
              <br />
              together
            </h2>
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
        </Reveal>
      </section>

      <section className="lp-section" id="enterprise">
        <div className="lp-enterprise">
          <div>
            <h3>Need DataFlow for your enterprise?</h3>
            <p>
              DataFlow Enterprise adds workspace RBAC, SSO, audit trails, tenant controls, and the
              same governed transfer engine your team already trusts.
            </p>
          </div>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={onEnterApp}>
            Learn about DataFlow Enterprise
          </button>
        </div>
      </section>

      <section className="lp-cta-band">
        <h3>Build more with DataFlow</h3>
        <div className="lp-hero-cta">
          <button type="button" className="lp-btn lp-btn--black lp-btn--lg" onClick={onStartTransfer}>
            Get started
          </button>
          <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={onEnterApp}>
            Contact sales
          </button>
        </div>
      </section>

      <footer className="lp-footer">
        <div className="lp-footer-grid">
          <div className="lp-footer-brand">
            <strong>DataFlow</strong>
            <p>Universal data freedom — move any data, anywhere, with proof.</p>
          </div>
          <div>
            <h4>Product</h4>
            <button type="button" className="lp-footer-link" onClick={onStartTransfer}>Transfer Studio</button>
            <button type="button" className="lp-footer-link" onClick={onOpenPilot ?? onEnterApp}>Data Pilot</button>
            <button type="button" className="lp-footer-link" onClick={onOpenMcp ?? onEnterApp}>MCP Server</button>
            <a href="#tools">Connectors</a>
          </div>
          <div>
            <h4>Solutions</h4>
            <a href="#usecases">Migrations</a>
            <a href="#usecases">Recurring sync</a>
            <a href="#usecases">Warehouse loading</a>
          </div>
          <div>
            <h4>Resources</h4>
            <button type="button" className="lp-footer-link" onClick={onEnterApp}>Docs</button>
            <a href="#enterprise">Enterprise</a>
            <a href="#customers">Customers</a>
          </div>
          <div>
            <h4>Company</h4>
            <button type="button" className="lp-footer-link" onClick={onEnterApp}>Contact sales</button>
            <button type="button" className="lp-footer-link" onClick={onEnterApp}>Log in</button>
            <a href="#enterprise">Pricing</a>
          </div>
        </div>
        <div className="lp-footer-bottom">
          <span>© {new Date().getFullYear()} DataFlow</span>
          <span>Privacy · Terms</span>
        </div>
      </footer>
    </div>
  );
}
