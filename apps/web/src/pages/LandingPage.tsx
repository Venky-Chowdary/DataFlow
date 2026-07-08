import { useEffect, useState } from "react";
import { DtLogo } from "../components/DtLogo";
import { DtIcon } from "../components/DtIcon";
import { fetchCatalogStats } from "../lib/api";

interface LandingPageProps {
  onEnterApp: () => void;
  onStartTransfer: () => void;
  onOpenPilot?: () => void;
}

export function LandingPage({ onEnterApp, onStartTransfer, onOpenPilot }: LandingPageProps) {
  const [stats, setStats] = useState<{ live: number; total: number } | null>(null);
  const [navOpen, setNavOpen] = useState(false);

  useEffect(() => {
    fetchCatalogStats()
      .then((s) => setStats({ live: s.live, total: s.total }))
      .catch(() => setStats(null));
  }, []);

  return (
    <div className="landing-page">
      <header className="landing-nav">
        <div className="landing-nav-brand">
          <DtLogo size={32} />
          DataFlow
        </div>
        <button
          type="button"
          className="df2-btn df2-btn-ghost landing-nav-toggle"
          onClick={() => setNavOpen((o) => !o)}
          aria-label="Toggle menu"
        >
          <DtIcon name="menu" />
        </button>
        <nav className={`landing-nav-links ${navOpen ? "open" : ""}`} aria-label="Product">
          <a href="#features" onClick={() => setNavOpen(false)}>Features</a>
          <a href="#compare" onClick={() => setNavOpen(false)}>Why DataFlow</a>
        </nav>
        <div className="landing-nav-actions">
          <button type="button" className="df2-btn" onClick={onEnterApp}>Sign in</button>
          <button type="button" className="df2-btn df2-btn-primary" onClick={onStartTransfer}>Start free</button>
        </div>
      </header>

      <section className="landing-hero">
        <div className="landing-eyebrow">
          <DtIcon name="sparkle" size={14} />
          Universal data platform
        </div>
        <h1>Any data. Anywhere. With intelligence built in.</h1>
        <p className="landing-hero-lead">
          Move data between databases, warehouses, and files with semantic AI mapping,
          eight preflight validation gates, and post-transfer reconciliation — not just pipes.
        </p>
        <div className="landing-hero-actions">
          <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={onStartTransfer}>
            Start a transfer
          </button>
          <button type="button" className="df2-btn df2-btn-lg" onClick={onEnterApp}>
            Open platform
          </button>
          {onOpenPilot && (
            <button type="button" className="df2-btn df2-btn-lg" onClick={onOpenPilot}>
              Try Data Pilot
            </button>
          )}
        </div>
        <div className="landing-metrics">
          <div className="landing-metric">
            <span className="landing-metric-value">
              {stats === null ? "…" : stats.live}
            </span>
            <span className="landing-metric-label">Live connectors</span>
          </div>
          <div className="landing-metric">
            <span className="landing-metric-value">
              {stats === null ? "…" : stats.total}
            </span>
            <span className="landing-metric-label">Catalog entries</span>
          </div>
          <div className="landing-metric">
            <span className="landing-metric-value">8</span>
            <span className="landing-metric-label">Preflight gates</span>
          </div>
          <div className="landing-metric">
            <span className="landing-metric-value">100%</span>
            <span className="landing-metric-label">Reconciliation</span>
          </div>
        </div>
      </section>

      <section className="landing-section" id="features">
        <h2 className="landing-section-title">Built for data teams who can&apos;t afford silent failures</h2>
        <p className="landing-section-sub">Every transfer runs through real algorithms — not demo stubs.</p>
        <div className="landing-features">
          {[
            {
              title: "Semantic mapping",
              desc: "BM25 + role matching understands AMT as payment_amount — not just string similarity.",
            },
            {
              title: "Preflight gates",
              desc: "Dry-run transforms, destination probes, and capacity checks before a single row moves.",
            },
            {
              title: "Gate 8 reconciliation",
              desc: "Independent row count and checksum verification against PostgreSQL, Snowflake, MySQL, and BigQuery.",
            },
            {
              title: "Scheduled pipelines",
              desc: "Hourly, daily, or weekly DB→DB syncs with live progress in Job Theater.",
            },
            {
              title: "Agent native",
              desc: "MCP server for Cursor, Claude, and VS Code — same tools as Data Pilot.",
            },
            {
              title: "Fail-closed drivers",
              desc: "Missing database drivers fail loudly in production; no fake success responses.",
            },
          ].map((f) => (
            <article key={f.title} className="landing-feature">
              <h3>{f.title}</h3>
              <p>{f.desc}</p>
            </article>
          ))}
        </div>
      </section>

      <section className="landing-section" id="compare">
        <h2 className="landing-section-title">DataFlow vs traditional ELT</h2>
        <p className="landing-section-sub">Airbyte moves bytes. DataFlow moves bytes with understanding.</p>
        <div className="landing-compare">
          <div className="landing-compare-col">
            <h3>Traditional ELT (Airbyte-style)</h3>
            <ul>
              <li>Connector count as primary metric</li>
              <li>Schema mapping mostly manual</li>
              <li>Validation optional or post-hoc</li>
              <li>Success = rows accepted by destination</li>
            </ul>
          </div>
          <div className="landing-compare-col highlight">
            <h3>DataFlow</h3>
            <ul>
              <li>Semantic AI + expanding connector catalog</li>
              <li>Auto-mapping with confidence scores</li>
              <li>8 preflight gates block bad transfers</li>
              <li>Success = reconciled checksum fidelity</li>
            </ul>
          </div>
        </div>
      </section>

      <section className="landing-cta">
        <h2 className="landing-section-title">Ready to move data with confidence?</h2>
        <p className="landing-section-sub">Open the platform or start a transfer in under a minute.</p>
        <div className="landing-hero-actions">
          <button type="button" className="df2-btn df2-btn-primary df2-btn-lg" onClick={onStartTransfer}>
            Start transfer
          </button>
          <button type="button" className="df2-btn df2-btn-lg" onClick={onEnterApp}>
            Enter platform
          </button>
        </div>
      </section>

      <footer className="landing-footer">
        DataFlow — Universal Data Platform · Inter · Enterprise UI
      </footer>
    </div>
  );
}
