import { useEffect, useMemo, useState, type CSSProperties } from "react";
import { ConnectorIcon } from "../app/brand-icons";
import { AnimatedCounter } from "../components/landing/AnimatedCounter";
import { ConnectorMarquee } from "../components/landing/ConnectorMarquee";
import { LandingHeroVisual } from "../components/landing/LandingHeroVisual";
import { DtLogo } from "../components/DtLogo";
import { DtIcon } from "../components/DtIcon";
import { useRevealOnScroll } from "../hooks/useRevealOnScroll";
import { fetchCatalogConnectors, fetchCatalogStats } from "../lib/api";

interface LandingPageProps {
  onEnterApp: () => void;
  onStartTransfer: () => void;
  onOpenPilot?: () => void;
  onOpenMcp?: () => void;
}

const CATEGORY_META: Record<string, { label: string; icon: string; blurb: string; count?: string }> = {
  database: { label: "Databases", icon: "database", blurb: "PostgreSQL, MySQL, MongoDB, Oracle, SQL Server, Cassandra…" },
  warehouse: { label: "Warehouses", icon: "snowflake", blurb: "Snowflake, BigQuery, Redshift, Databricks, ClickHouse…" },
  file: { label: "Files", icon: "file", blurb: "CSV, JSON, Parquet, Excel, Avro, ORC, XML…" },
  saas: { label: "SaaS", icon: "cloud", blurb: "Salesforce, HubSpot, Stripe, Shopify, Workday…" },
  lake: { label: "Data lakes", icon: "activity", blurb: "S3, GCS, Azure Data Lake, Delta, Iceberg…" },
  streaming: { label: "Streaming", icon: "zap", blurb: "Kafka, Kinesis, Pub/Sub, Event Hubs, Pulsar…" },
};

const PRODUCT_TABS = [
  {
    id: "transfer",
    label: "Transfer Studio",
    icon: "transfer",
    headline: "Governed migrations in minutes",
    body: "Upload or connect any source, get semantic column maps, run eight preflight gates, and load with checksum proof.",
    bullets: ["Hybrid LLM + BM25 mapping", "Dry-run transforms", "Destination probes"],
  },
  {
    id: "pilot",
    label: "Data Pilot",
    icon: "sparkle",
    headline: "Natural language data ops",
    body: "Ask questions about your schema, mappings, and jobs. Same intelligence as the UI — in chat.",
    bullets: ["Schema introspection", "Mapping explanations", "Job triage"],
  },
  {
    id: "mcp",
    label: "MCP Server",
    icon: "zap",
    headline: "Agent-native integrations",
    body: "Expose transfers, connectors, and preflight to Cursor, Claude Desktop, and VS Code via MCP.",
    bullets: ["Tool parity with UI", "Secure credentials", "Catalog-aware"],
  },
] as const;

export function LandingPage({ onEnterApp, onStartTransfer, onOpenPilot, onOpenMcp }: LandingPageProps) {
  const [stats, setStats] = useState<{
    total: number;
    transfer_live: number;
    categories: number;
    roadmap: number;
  } | null>(null);
  const [featured, setFeatured] = useState<{ id: string; name: string }[]>([]);
  const [navOpen, setNavOpen] = useState(false);
  const [productTab, setProductTab] = useState<(typeof PRODUCT_TABS)[number]["id"]>("transfer");
  const [scrolled, setScrolled] = useState(false);

  const connectorsReveal = useRevealOnScroll();
  const howReveal = useRevealOnScroll();
  const platformReveal = useRevealOnScroll();
  const securityReveal = useRevealOnScroll();

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  useEffect(() => {
    Promise.all([
      fetchCatalogStats().catch(() => null),
      fetchCatalogConnectors({ limit: 12, transferOnly: true }).catch(() => null),
    ]).then(([catalog, connectors]) => {
      if (catalog) {
        setStats({
          total: catalog.total,
          transfer_live: catalog.transfer_live ?? catalog.live,
          categories: catalog.categories,
          roadmap: catalog.roadmap ?? catalog.planned,
        });
      }
      const list = connectors?.connectors?.slice(0, 10) ?? [];
      setFeatured(list.map((c: { id: string; name: string }) => ({ id: c.id, name: c.name })));
    });
  }, []);

  const categoryCards = useMemo(() => {
    const keys = ["database", "warehouse", "file", "saas", "lake", "streaming"];
    return keys.map((key) => ({ key, ...CATEGORY_META[key] }));
  }, []);

  const activeProduct = PRODUCT_TABS.find((t) => t.id === productTab) ?? PRODUCT_TABS[0];

  const openActiveProduct = () => {
    if (activeProduct.id === "pilot") {
      (onOpenPilot ?? onEnterApp)();
      return;
    }
    if (activeProduct.id === "mcp") {
      (onOpenMcp ?? onEnterApp)();
      return;
    }
    onStartTransfer();
  };

  return (
    <div className="lp">
      <div className="lp-ambient" aria-hidden>
        <span className="lp-orb lp-orb--1" />
        <span className="lp-orb lp-orb--2" />
        <span className="lp-orb lp-orb--3" />
      </div>

      <header className={`lp-nav ${scrolled ? "lp-nav--scrolled" : ""}`}>
        <a className="lp-nav-brand" href="/" onClick={(e) => e.preventDefault()}>
          <DtLogo size={32} />
          <span>DataFlow</span>
        </a>
        <button
          type="button"
          className="lp-nav-toggle"
          onClick={() => setNavOpen((o) => !o)}
          aria-label="Menu"
          aria-expanded={navOpen}
        >
          <DtIcon name="menu" size={20} />
        </button>
        <nav className={`lp-nav-links ${navOpen ? "open" : ""}`}>
          <a href="#how" onClick={() => setNavOpen(false)}>How it works</a>
          <a href="#platform" onClick={() => setNavOpen(false)}>Platform</a>
          <a href="#connectors" onClick={() => setNavOpen(false)}>Connectors</a>
          <a href="#security" onClick={() => setNavOpen(false)}>Security</a>
        </nav>
        <div className="lp-nav-actions">
          <button type="button" className="df2-btn df2-btn-ghost" onClick={onEnterApp}>Sign in</button>
          <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>Get started</button>
        </div>
      </header>

      <section className="lp-hero">
        <div className="lp-hero-bg" aria-hidden />
        <div className="lp-hero-inner">
          <div className="lp-hero-copy lp-reveal lp-reveal--in">
            <p className="lp-eyebrow">
              <span className="lp-eyebrow-pulse" />
              <DtIcon name="sparkle" size={14} />
              Universal data transfer platform
            </p>
            <h1>
              Move any data,
              <span className="lp-gradient-text"> anywhere</span>
            </h1>
            <p className="lp-lead">
              Database to warehouse. Warehouse to files. Files to database. Any type to any type —
              with schema intelligence, eight preflight gates, and checksum-proven loads.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="df2-btn df2-btn-primary df2-btn-lg lp-btn-glow" onClick={onStartTransfer}>
                Start a transfer
                <DtIcon name="transfer" size={16} />
              </button>
              <button type="button" className="df2-btn df2-btn-lg" onClick={onEnterApp}>
                Sign in to workspace
              </button>
            </div>
            <dl className="lp-stats">
              <div>
                <dt>Catalog</dt>
                <dd>{stats ? <AnimatedCounter value={stats.total} suffix="+" /> : "…"}</dd>
              </div>
              <div>
                <dt>Transfer ready</dt>
                <dd>{stats ? <AnimatedCounter value={stats.transfer_live} /> : "…"}</dd>
              </div>
              <div>
                <dt>Categories</dt>
                <dd>{stats ? <AnimatedCounter value={stats.categories} /> : "…"}</dd>
              </div>
              <div>
                <dt>Preflight gates</dt>
                <dd>8</dd>
              </div>
            </dl>
          </div>

          <div className="lp-hero-visual lp-reveal lp-reveal--in lp-reveal--delay">
            <LandingHeroVisual />
          </div>
        </div>
      </section>

      <div className="lp-value-strip" aria-label="Product pillars">
        {[
          { icon: "transfer", title: "Anywhere → anywhere", body: "Sources and destinations across databases, warehouses, lakes, files, and SaaS." },
          { icon: "sparkle", title: "Schema intelligence", body: "Semantic column maps that understand roles — not just string similarity." },
          { icon: "gate", title: "Governed by default", body: "Eight preflight gates and post-load reconciliation before and after every move." },
        ].map((item) => (
          <div key={item.title} className="lp-value-item">
            <span className="lp-value-icon" aria-hidden>
              <DtIcon name={item.icon} size={18} />
            </span>
            <div>
              <strong>{item.title}</strong>
              <span>{item.body}</span>
            </div>
          </div>
        ))}
      </div>

      <ConnectorMarquee />

      <section className={`lp-section lp-section-how ${howReveal.className}`} id="how" ref={howReveal.ref}>
        <div className="lp-section-head">
          <p className="lp-section-kicker">Workflow</p>
          <h2>How governed transfers work</h2>
          <p>Four steps from raw files to verified loads — no silent failures.</p>
        </div>
        <ol className="lp-steps">
          {[
            { n: "01", title: "Connect", desc: "Authenticate sources and destinations from a catalog of 650+ systems with honest readiness labels.", icon: "connectors" },
            { n: "02", title: "Map", desc: "Semantic column matching understands roles — AMT maps to payment_amount, not just string similarity.", icon: "sparkle" },
            { n: "03", title: "Preflight", desc: "Eight gates validate transforms, probe destinations, and block bad loads before data moves.", icon: "gate" },
            { n: "04", title: "Reconcile", desc: "Independent row counts and checksums prove the migration completed correctly.", icon: "check" },
          ].map((step, i) => (
            <li key={step.n} className="lp-step" style={{ "--reveal-i": i } as CSSProperties}>
              <span className="lp-step-icon"><DtIcon name={step.icon} size={18} /></span>
              <span className="lp-step-num">{step.n}</span>
              <div>
                <h3>{step.title}</h3>
                <p>{step.desc}</p>
              </div>
            </li>
          ))}
        </ol>
      </section>

      <section className={`lp-section lp-section-alt ${platformReveal.className}`} id="platform" ref={platformReveal.ref}>
        <div className="lp-section-head">
          <p className="lp-section-kicker">Product</p>
          <h2>One platform. Three ways to operate.</h2>
          <p>UI, AI chat, and MCP — same governed engine underneath.</p>
        </div>

        <div className="lp-product-showcase">
          <div className="lp-product-tabs" role="tablist">
            {PRODUCT_TABS.map((tab) => (
              <button
                key={tab.id}
                type="button"
                role="tab"
                aria-selected={productTab === tab.id}
                className={`lp-product-tab ${productTab === tab.id ? "active" : ""}`}
                onClick={() => setProductTab(tab.id)}
              >
                <DtIcon name={tab.icon} size={16} />
                {tab.label}
              </button>
            ))}
          </div>
          <article className="lp-product-panel" key={activeProduct.id}>
            <div className="lp-product-copy">
              <h3>{activeProduct.headline}</h3>
              <p>{activeProduct.body}</p>
              <ul>
                {activeProduct.bullets.map((b) => (
                  <li key={b}><DtIcon name="check" size={14} />{b}</li>
                ))}
              </ul>
              <button type="button" className="df2-btn df2-btn-primary" onClick={openActiveProduct}>
                {activeProduct.id === "transfer" && "Start a transfer"}
                {activeProduct.id === "pilot" && "Open Data Pilot"}
                {activeProduct.id === "mcp" && "View MCP Server"}
              </button>
            </div>
            <div className="lp-product-visual" aria-hidden>
              <div className="lp-product-mock">
                <div className="lp-product-mock-bar">
                  <span /><span /><span />
                </div>
                <div className="lp-product-mock-body">
                  {activeProduct.bullets.map((b, i) => (
                    <div key={b} className="lp-product-mock-row" style={{ "--i": i } as CSSProperties}>
                      <span className="lp-product-mock-dot" />
                      <span>{b}</span>
                      <span className="lp-product-mock-check">✓</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </article>
        </div>

        <div className="lp-bento">
          {[
            { icon: "sparkle", title: "Semantic mapping", desc: "BM25 + role matching maps AMT to payment_amount — not just string similarity." },
            { icon: "gate", title: "8 preflight gates", desc: "Dry-run transforms, destination probes, and capacity checks before rows move." },
            { icon: "check", title: "Reconciliation", desc: "Independent row counts and checksums against Postgres, Snowflake, MySQL, BigQuery." },
            { icon: "activity", title: "Job Theater", desc: "Live batch progress, phase tracking, and failure triage in one view." },
            { icon: "clock", title: "Pipelines", desc: "Hourly, daily, or weekly syncs with schedule monitoring." },
            { icon: "zap", title: "Agent native", desc: "MCP server and Data Pilot — same tools in Cursor, Claude, and VS Code." },
          ].map((f, i) => (
            <article key={f.title} className="lp-bento-card" style={{ "--reveal-i": i } as CSSProperties}>
              <DtIcon name={f.icon} size={20} />
              <h3>{f.title}</h3>
              <p>{f.desc}</p>
            </article>
          ))}
        </div>
      </section>

      <section className={`lp-section ${connectorsReveal.className}`} id="connectors" ref={connectorsReveal.ref}>
        <div className="lp-section-head">
          <p className="lp-section-kicker">Integrations</p>
          <h2>One catalog. Every system your stack uses.</h2>
          <p>Databases, warehouses, lakes, files, and SaaS — with honest readiness labels.</p>
        </div>
        <div className="lp-category-grid">
          {categoryCards.map((cat, i) => (
            <article key={cat.key} className="lp-category-card" style={{ "--reveal-i": i } as CSSProperties}>
              <span className="lp-category-icon" aria-hidden>
                <DtIcon name={cat.icon} size={22} />
              </span>
              <h3>{cat.label}</h3>
              <p>{cat.blurb}</p>
            </article>
          ))}
        </div>
        {featured.length > 0 && (
          <div className="lp-featured">
            <span className="lp-featured-label">Transfer-ready today</span>
            <div className="lp-featured-chips">
              {featured.map((c) => (
                <span key={c.id} className="lp-featured-chip">
                  <ConnectorIcon id={c.id} size={16} />
                  {c.name}
                </span>
              ))}
            </div>
          </div>
        )}
      </section>

      <section className={`lp-section lp-section-alt ${securityReveal.className}`} id="security" ref={securityReveal.ref}>
        <div className="lp-security">
          <div className="lp-security-copy">
            <p className="lp-section-kicker">Security</p>
            <h2>Enterprise security from sign-in to reconciliation</h2>
            <p>Server-verified sessions, masked connector secrets, fail-closed drivers, and audit-ready job history.</p>
            <ul>
              <li><DtIcon name="shield" size={16} /> Server-side authentication</li>
              <li><DtIcon name="key" size={16} /> Credentials never echoed to the client</li>
              <li><DtIcon name="gate" size={16} /> Preflight blocks bad transfers</li>
              <li><DtIcon name="check" size={16} /> Post-load checksum proof</li>
            </ul>
          </div>
          <div className="lp-security-visual">
            <div className="lp-security-stat">
              <strong>{stats ? <AnimatedCounter value={stats.transfer_live} suffix="+" /> : "…"}</strong>
              <span>production transfer routes</span>
            </div>
            <div className="lp-security-stat">
              <strong>{stats ? <AnimatedCounter value={stats.roadmap} suffix="+" /> : "…"}</strong>
              <span>catalog roadmap entries</span>
            </div>
          </div>
        </div>
      </section>

      <section className="lp-cta">
        <div className="lp-cta-inner">
          <h2>Ready to move data with confidence?</h2>
          <p>Join teams using governed pipelines instead of fragile scripts.</p>
          <div className="lp-hero-cta">
            <button type="button" className="df2-btn df2-btn-primary df2-btn-lg lp-btn-glow" onClick={onStartTransfer}>
              Start free transfer
            </button>
            <button type="button" className="df2-btn df2-btn-lg" onClick={onEnterApp}>
              Sign in
            </button>
            {onOpenPilot && (
              <button type="button" className="df2-btn df2-btn-ghost df2-btn-lg" onClick={onOpenPilot}>
                Try Data Pilot
              </button>
            )}
          </div>
        </div>
      </section>

      <footer className="lp-footer">
        <div className="lp-footer-inner">
          <div className="lp-footer-brand">
            <DtLogo size={24} />
            <span>DataFlow</span>
          </div>
          <p>Universal data platform · Semantic intelligence · Enterprise UI</p>
          <nav className="lp-footer-links">
            <a href="#how">How it works</a>
            <a href="#platform">Platform</a>
            <a href="#connectors">Connectors</a>
            <a href="#security">Security</a>
          </nav>
        </div>
      </footer>
    </div>
  );
}
