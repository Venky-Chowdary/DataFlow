import { useState, type CSSProperties, type FormEvent } from "react";
import { DtIcon } from "../../components/DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import {
  AlgorithmCinemaBand,
  ProofCinema,
} from "../../components/landing/AlgorithmCinema";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingIllustration } from "../../components/marketing/MarketingIllustration";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  EVIDENCE_AS_OF,
  MARKETING_PROOF_HIGHLIGHTS,
  MARKETING_STACK,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import {
  DataPilotPage,
  JobTheaterPage,
  McpServerPage,
  MigrationsSolutionPage,
  PipelinesPage,
  QueryPlaygroundPage,
  SyncSolutionPage,
  TransferStudioPage,
  WarehouseSolutionPage,
} from "./ProductSurfaces";

interface PageActions {
  onGetStarted: () => void;
  onLogin: () => void;
  onNavigate: (route: PublicRoute) => void;
}

function StatsStrip({ items }: { items: { value: string; label: string }[] }) {
  return (
    <div className="lp-mkt-stats-strip" role="list">
      {items.map((item) => (
        <div key={item.label} className="lp-mkt-stats-item" role="listitem">
          <strong>{item.value}</strong>
          <span>{item.label}</span>
        </div>
      ))}
    </div>
  );
}

/**
 * Control posture, not certificates. No third-party audit has been completed,
 * so the footnote ships with the badges and cannot be dropped by a caller.
 */
function ComplianceBadges({ items }: { items: string[] }) {
  return (
    <div className="lp-mkt-compliance">
      <div className="lp-mkt-compliance-badges" aria-label="Security controls">
        {items.map((item) => (
          <span key={item} className="lp-mkt-compliance-badge">
            <DtIcon name="shield" size={14} />
            {item}
          </span>
        ))}
      </div>
      <p className="lp-mkt-compliance-note">
        Encryption, SSO, customer-managed keys, and audit logging ship with the product.
        Request the security questionnaire and DPA for procurement.
      </p>
    </div>
  );
}

export function MarketingSubpage({ route, onGetStarted, onLogin, onNavigate }: { route: PublicRoute } & PageActions) {
  if (route === "home") return null;
  if (isHelpDocRoute(route)) {
    return <DocArticlePage docId={route} onNavigate={onNavigate} onGetStarted={onGetStarted} />;
  }

  switch (route) {
    case "pricing":
      return <PricingPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "enterprise":
      return <EnterprisePage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "customers":
      return <CustomersPage onNavigate={onNavigate} />;
    case "contact":
      return <ContactPage onNavigate={onNavigate} />;
    case "privacy":
      return <LegalPage kind="privacy" />;
    case "terms":
      return <LegalPage kind="terms" />;
    case "security":
      return <SecurityPage onNavigate={onNavigate} />;
    case "help":
      return <DocsPortal onNavigate={onNavigate} onGetStarted={onGetStarted} />;
    case "product-transfer":
      return <TransferStudioPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-jobs":
      return <JobTheaterPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-pipelines":
      return <PipelinesPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-query":
      return <QueryPlaygroundPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-pilot":
      return <DataPilotPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "product-mcp":
      return <McpServerPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "integrations":
      return <IntegrationsPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-migrations":
      return <MigrationsSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-warehouse":
      return <WarehouseSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-sync":
      return <SyncSolutionPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    default:
      return null;
  }
}

function PricingPage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const plans = [
    {
      name: "Starter",
      price: "Free",
      period: "forever",
      blurb: "Ship a governed transfer today — same engine, real preflight, no teaser demo.",
      cta: "Start free",
      action: onGetStarted,
      tone: "starter" as const,
      features: ["Transfer Studio", "9 preflight gates", "Quarantine + checksum", "Community support"],
    },
    {
      name: "Team",
      price: "Custom",
      period: "usage-aligned",
      blurb: "Pipelines, Pilot, and shared connectors — priced to cadence, never seats-first.",
      cta: "Talk to sales",
      action: () => onNavigate("contact"),
      tone: "team" as const,
      featured: true,
      features: ["Everything in Starter", "Scheduled pipelines", "Datawrap Pilot", "Email support"],
    },
    {
      name: "Enterprise",
      price: "Custom",
      period: "security & scale",
      blurb: "SSO, BYOK, tenant isolation, MCP under policy, and a dedicated success engineer.",
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      tone: "enterprise" as const,
      features: ["Everything in Team", "SSO / SAML + BYOK", "MCP for agents", "Dedicated success"],
    },
  ];

  const compareRows = [
    { feature: "Transfer Studio", starter: true, team: true, enterprise: true },
    { feature: "Preflight gates & quarantine", starter: true, team: true, enterprise: true },
    { feature: "Checksum proof", starter: true, team: true, enterprise: true },
    { feature: "Pipelines & schedules", starter: false, team: true, enterprise: true },
    { feature: "Datawrap Pilot", starter: false, team: true, enterprise: true },
    { feature: "MCP for agents", starter: false, team: false, enterprise: true },
    { feature: "SSO / SAML", starter: false, team: false, enterprise: true },
    { feature: "BYOK & dedicated tenant", starter: false, team: false, enterprise: true },
  ];

  return (
    <div className="lp-mkt-page lp-page-pricing lp-pricing-v2">
      <section className="lp-pricing-hero" aria-label="Pricing">
        <div className="lp-pricing-hero-waves" aria-hidden>
          <span className="lp-wave lp-wave--a" />
          <span className="lp-wave lp-wave--b" />
          <span className="lp-wave lp-wave--c" />
          <span className="lp-wave-grid" />
          <span className="lp-wave-glow lp-wave-glow--1" />
          <span className="lp-wave-glow lp-wave-glow--2" />
          <svg className="lp-wave-svg" viewBox="0 0 1440 320" preserveAspectRatio="none">
            <path
              className="lp-wave-path lp-wave-path--1"
              d="M0,192 C240,128 360,256 600,192 C840,128 960,64 1200,128 C1320,160 1380,176 1440,160 L1440,320 L0,320 Z"
            />
            <path
              className="lp-wave-path lp-wave-path--2"
              d="M0,224 C180,160 420,288 720,224 C1020,160 1200,96 1440,176 L1440,320 L0,320 Z"
            />
            <path
              className="lp-wave-path lp-wave-path--3"
              d="M0,256 C300,200 540,300 780,240 C1020,180 1260,220 1440,200 L1440,320 L0,320 Z"
            />
          </svg>
          <svg className="lp-wave-flow" viewBox="0 0 400 200" preserveAspectRatio="xMidYMid meet">
            <path d="M20 100 C80 40, 140 160, 200 100 S320 40, 380 100" fill="none" stroke="rgba(94,234,212,0.55)" strokeWidth="2.5" strokeLinecap="round" />
            <path d="M20 130 C90 70, 150 180, 210 120 S320 70, 380 120" fill="none" stroke="rgba(245,158,11,0.4)" strokeWidth="2" strokeLinecap="round" />
            <path d="M20 70 C100 20, 160 120, 220 70 S310 20, 380 70" fill="none" stroke="rgba(45,212,191,0.35)" strokeWidth="1.75" strokeLinecap="round" />
            <circle cx="200" cy="100" r="6" fill="#2dd4bf" />
            <circle cx="200" cy="100" r="14" fill="none" stroke="rgba(45,212,191,0.35)" strokeWidth="2" />
          </svg>
        </div>
        <div className="lp-pricing-hero-inner">
          <p className="lp-pricing-hero-kicker">
            <span className="lp-pricing-hero-dot" aria-hidden />
            Pricing
          </p>
          <h1>
            Plans that scale with
            <span className="lp-pricing-hero-em"> proof</span>
          </h1>
          <p className="lp-pricing-hero-lead">
            Semantic mapping, nine core gates, quarantine, and checksum reconcile — included from first
            pilot to regulated pipelines. Pay for cadence and security, not seats.
          </p>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Start for free
            </button>
            <button
              type="button"
              className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink"
              onClick={() => onNavigate("contact")}
            >
              Talk to sales
            </button>
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-pricing-v2-plans" aria-label="Plans">
          <div className="lp-pricing-v2-grid">
            {plans.map((plan, i) => (
              <article
                key={plan.name}
                className={`lp-pricing-card lp-pricing-card--${plan.tone}${plan.featured ? " is-featured" : ""}`}
                style={{ "--reveal-i": i } as CSSProperties}
              >
                {plan.featured ? <span className="lp-pricing-card-flag">Recommended</span> : null}
                <header className="lp-pricing-card-head">
                  <h2>{plan.name}</h2>
                  <p className="lp-pricing-card-period">{plan.period}</p>
                </header>
                <p className="lp-pricing-card-price">
                  <strong>{plan.price}</strong>
                </p>
                <p className="lp-pricing-card-blurb">{plan.blurb}</p>
                <ul className="lp-pricing-card-features">
                  {plan.features.map((f) => (
                    <li key={f}>
                      <DtIcon name="check" size={14} />
                      {f}
                    </li>
                  ))}
                </ul>
                <button
                  type="button"
                  className={plan.featured ? "lp-btn lp-btn--brand lp-btn--block" : "lp-btn lp-btn--outline lp-btn--block"}
                  onClick={plan.action}
                >
                  {plan.cta}
                </button>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-pricing-v2-compare" aria-label="Plan comparison">
          <div className="lp-pricing-v2-compare-head">
            <p className="lp-mkt-kicker">Compare</p>
            <h2>One engine. Three unlock levels.</h2>
            <p>
              Silent-data-loss prevention ships in Starter. Collaboration unlocks in Team.
              Identity, tenancy, and agents unlock in Enterprise.
            </p>
          </div>
          <div className="lp-pricing-v2-table-wrap">
            <table className="lp-pricing-v2-table">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">Starter</th>
                  <th scope="col" className="is-hot">Team</th>
                  <th scope="col">Enterprise</th>
                </tr>
              </thead>
              <tbody>
                {compareRows.map((row) => (
                  <tr key={row.feature}>
                    <th scope="row">{row.feature}</th>
                    <td>{row.starter ? <span className="lp-pricing-yes" aria-label="Included">✓</span> : <span className="lp-pricing-no">—</span>}</td>
                    <td className="is-hot">{row.team ? <span className="lp-pricing-yes" aria-label="Included">✓</span> : <span className="lp-pricing-no">—</span>}</td>
                    <td>{row.enterprise ? <span className="lp-pricing-yes" aria-label="Included">✓</span> : <span className="lp-pricing-no">—</span>}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-pricing-v2-principles" aria-label="Pricing principles">
          <div className="lp-pricing-principle">
            <strong>Honest free tier</strong>
            <span>Full Transfer Studio path — no teaser demo.</span>
          </div>
          <div className="lp-pricing-principle">
            <strong>Quote when you scale</strong>
            <span>Priced to connectors and cadence, never seats-first.</span>
          </div>
          <div className="lp-pricing-principle">
            <strong>One engine everywhere</strong>
            <span>UI, Pipelines, Pilot, and MCP share the same gates.</span>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-pricing-v2-cta">
          <div className="lp-pricing-v2-cta-inner">
            <div>
              <h3>Procurement, MSA, or a security pack?</h3>
              <p>
                Enterprise deals ship with negotiated MSA, DPA, SOC&nbsp;2 posture pack, and a
                pre-populated security questionnaire — reviewed by a solutions engineer.
              </p>
            </div>
            <div className="lp-pricing-v2-cta-actions">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
                Contact sales
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("security")}>
                Security overview
              </button>
            </div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

function EnterprisePage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const capabilities = [
    {
      id: "identity",
      kicker: "01 · Identity",
      title: "SSO that actually gates runs",
      body: "SAML/OIDC and SCIM-ready roles. Every Transfer Studio job, Pilot session, and MCP call inherits who is allowed to map, approve, and write.",
    },
    {
      id: "tenancy",
      kicker: "02 · Tenancy",
      title: "Isolation without a fork",
      body: "Dedicated tenants, custom domains, and region pinning. Same governed engine — no shared control-plane bleed between customers.",
    },
    {
      id: "keys",
      kicker: "03 · Keys",
      title: "BYOK on connector secrets",
      body: "Customer-managed keys wrap credentials. Purpose keys stay scoped to the job that needs them — never a global vault shortcut.",
    },
    {
      id: "audit",
      kicker: "04 · Audit",
      title: "Proof ready for SOC review",
      body: "Immutable logs for jobs, mapping decisions, quarantine samples, and agent MCP calls — checksum reconcile included.",
    },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-enterprise-v2">
      <section className="lp-ent-hero" aria-label="Enterprise">
        <div className="lp-ent-hero-waves" aria-hidden>
          <span className="lp-wave-grid" />
          <span className="lp-wave-glow lp-wave-glow--1" />
          <span className="lp-wave-glow lp-wave-glow--2" />
          <svg className="lp-wave-svg" viewBox="0 0 1440 320" preserveAspectRatio="none">
            <path
              className="lp-wave-path lp-wave-path--1"
              d="M0,192 C240,128 360,256 600,192 C840,128 960,64 1200,128 C1320,160 1380,176 1440,160 L1440,320 L0,320 Z"
            />
            <path
              className="lp-wave-path lp-wave-path--2"
              d="M0,224 C180,160 420,288 720,224 C1020,160 1200,96 1440,176 L1440,320 L0,320 Z"
            />
          </svg>
        </div>
        <div className="lp-ent-hero-inner">
          <div className="lp-ent-hero-copy">
            <p className="lp-pricing-hero-kicker">
              <span className="lp-pricing-hero-dot" aria-hidden />
              Enterprise
            </p>
            <h1>
              Controls that travel with
              <span className="lp-pricing-hero-em"> every load</span>
            </h1>
            <p className="lp-ent-hero-lead">
              SSO, RBAC, BYOK, and tenant isolation on the same Transfer Studio engine your operators
              already trust. No parallel “enterprise-only” path that skips gates.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
                Contact sales
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={onGetStarted}>
                Start a pilot
              </button>
            </div>
            <ul className="lp-ent-hero-proof">
              <li>SSO enforced</li>
              <li>9 / 9 preflight</li>
              <li>Checksum MATCH</li>
            </ul>
          </div>
          <aside className="lp-ent-hero-stage" aria-label="Enterprise control plane preview">
            <div className="lp-ent-stage-chrome">
              <span className="lp-ent-stage-dots" aria-hidden>
                <i /><i /><i />
              </span>
              <em>datawrap.company.com · SSO enforced</em>
            </div>
            <div className="lp-ent-stage-body">
              <div className="lp-ent-stage-rail">
                <span className="is-active">Workspaces</span>
                <span>Identity</span>
                <span>Keys</span>
                <span>Audit</span>
              </div>
              <div className="lp-ent-stage-main">
                <header className="lp-ent-stage-head">
                  <strong>Tenant posture</strong>
                  <span className="lp-ent-stage-pill">Live</span>
                </header>
                <div className="lp-ent-stage-cards">
                  <article>
                    <span>Workspace A</span>
                    <strong>Analytics ops</strong>
                    <p>Preflight 9/9 · checksum match</p>
                    <div className="lp-ent-stage-bar"><i style={{ width: "92%" }} /></div>
                  </article>
                  <article>
                    <span>Workspace B</span>
                    <strong>Regulated loads</strong>
                    <p>Region pinned · tenant isolated</p>
                    <div className="lp-ent-stage-bar"><i style={{ width: "78%" }} /></div>
                  </article>
                </div>
                <div className="lp-ent-stage-rows">
                  <div><span>Identity</span><em>SAML · Okta</em></div>
                  <div><span>Secrets</span><em>BYOK · AWS KMS</em></div>
                  <div><span>Agents</span><em>MCP under RBAC</em></div>
                  <div className="is-ok"><span>Reconcile</span><em>MATCH</em></div>
                </div>
              </div>
            </div>
          </aside>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-ent-metrics" aria-label="Enterprise capabilities at a glance">
          {[
            { value: "SSO", label: "SAML & OIDC" },
            { value: "BYOK", label: "Customer keys" },
            { value: "Full", label: "Job audit trail" },
            { value: "Multi", label: "Tenant isolation" },
          ].map((item) => (
            <div key={item.label} className="lp-ent-metric">
              <strong>{item.value}</strong>
              <span>{item.label}</span>
            </div>
          ))}
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-ent-capabilities">
          <div className="lp-ent-section-head">
            <p className="lp-mkt-kicker">Control plane</p>
            <h2>Enterprise controls on every run</h2>
            <p>
              Identity, tenancy, keys, and audit wrap the same map → preflight → prove path — not a
              wall of feature cards bolted on after the fact.
            </p>
          </div>
          <div className="lp-ent-capability-grid">
            {capabilities.map((cap) => (
              <article key={cap.id} className="lp-ent-capability">
                <span>{cap.kicker}</span>
                <h3>{cap.title}</h3>
                <p>{cap.body}</p>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-ent-engine">
          <div className="lp-ent-engine-split">
            <div>
              <p className="lp-mkt-kicker">How it lands</p>
              <h2>One engine. Enterprise controls on top.</h2>
              <p>
                Procurement does not get a different transfer algorithm. Your teams map, preflight,
                quarantine, and reconcile the same way — with identity, tenancy, and keys wrapping
                every run.
              </p>
              <ul className="lp-ent-checklist">
                <li>Workspace RBAC for who can map, approve drift, and run production loads</li>
                <li>Region pinning for jobs and artifacts when policy requires residency</li>
                <li>MCP and Datawrap Pilot inherit the same gates — agents never get a silent shortcut</li>
                <li>Security questionnaire and control-mapping pack for procurement kickoff</li>
              </ul>
            </div>
            <aside className="lp-ent-panel" aria-label="Enterprise control snapshot">
              <h3>Control snapshot</h3>
              <div className="lp-ent-panel-row"><span>Identity</span><em>SSO enforced</em></div>
              <div className="lp-ent-panel-row"><span>Secrets</span><em>BYOK wrapped</em></div>
              <div className="lp-ent-panel-row"><span>Preflight</span><em>9 / 9 required</em></div>
              <div className="lp-ent-panel-row"><span>Quarantine</span><em>surfaced, never dropped</em></div>
              <div className="lp-ent-panel-row"><span>Reconcile</span><em>checksum + counts</em></div>
              <div className="lp-ent-panel-row"><span>Audit</span><em>jobs · maps · agents</em></div>
            </aside>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-ent-cta-band">
          <div className="lp-ent-cta-inner">
            <div>
              <p className="lp-mkt-kicker lp-mkt-kicker--on-ink">Ready when procurement is</p>
              <h2>Ship governed transfers under enterprise policy</h2>
              <p>
                Start a pilot on the same engine. Add SSO, BYOK, and audit when security is ready —
                without re-platforming operators.
              </p>
              <ComplianceBadges items={["Encryption at rest", "SSO / SAML", "Customer-managed keys", "GDPR processing", "Regional residency"]} />
            </div>
            <div className="lp-ent-cta-actions">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
                Talk to sales
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("security")}>
                Security overview
              </button>
            </div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

function CustomersPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  return (
    <div className="lp-mkt-page lp-cust-v3">
      <section className="lp-cust3-hero" aria-label="Customers">
        <div className="lp-shell lp-cust3-hero-inner">
          <p className="lp-mkt-kicker">Customers</p>
          <h1>
            Every load leaves
            <em> proof</em>
          </h1>
          <p className="lp-cust3-lead">
            Load Snowflake, BigQuery, Redshift, and your lake the way operators actually work:
            map once, validate before write, quarantine bad rows, and keep a checksum finance
            can archive.
          </p>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Book a pilot
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("enterprise")}>
              Enterprise overview
            </button>
          </div>
        </div>
      </section>

      <section className="lp-cust3-metrics" aria-label="Product scale">
        <div className="lp-shell lp-cust3-metrics-row">
          <div>
            <strong>8</strong>
            <span>Preflight gates before every write</span>
          </div>
          <div>
            <strong>Warehouses</strong>
            <span>Snowflake, BigQuery, Redshift, Databricks</span>
          </div>
          <div>
            <strong>Lakes</strong>
            <span>Amazon S3, ADLS, Google Cloud Storage</span>
          </div>
          <div>
            <strong>0</strong>
            <span>Silent drops by design</span>
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-cust3-stories" aria-label="Why teams choose Datawrap">
          <div className="lp-shell">
            <div className="lp-cust3-section-head">
              <p className="lp-mkt-kicker">Why Datawrap</p>
              <h2>Built for operators who cannot afford a quiet miss</h2>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_PROOF_HIGHLIGHTS.map((row) => (
                <article key={row.title} className="lp-mkt-evidence-card">
                  <span className="lp-cust-industry">{row.stat}</span>
                  <p><strong>{row.title}</strong> {row.body}</p>
                  <footer>
                    <strong>Measured {EVIDENCE_AS_OF}</strong>
                    <span>Live engines · destination re-read</span>
                  </footer>
                </article>
              ))}
            </div>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-cust3-sectors" aria-label="Connect your stack">
          <div className="lp-shell">
            <div className="lp-cust3-section-head">
              <p className="lp-mkt-kicker">Your stack</p>
              <h2>Warehouses, lakes, databases, and apps</h2>
              <p>
                Connect Snowflake, BigQuery, S3, ADLS, GCS, and the applications your revenue
                team already runs. Same map, same gates, same reconcile report — every destination.
              </p>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_STACK.map((row) => (
                <article key={row.family} className="lp-mkt-evidence-card">
                  <span className="lp-cust-industry">{row.family}</span>
                  <p><strong>{row.items}</strong> {row.note}</p>
                </article>
              ))}
            </div>
          </div>
        </section>
      </MarketingReveal>

      <section className="lp-cust3-cta">
        <div className="lp-shell lp-cust3-cta-inner">
          <div>
            <h2>Run a pilot on your data</h2>
            <p>
              Design partners get a scoped load on their own sources and destinations — mapping,
              preflight, quarantine, and a reconcile artifact you keep.
            </p>
          </div>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Talk to sales
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("pricing")}>
              See pricing
            </button>
          </div>
        </div>
      </section>
    </div>
  );
}

const CONTACT_SOURCES = ["PostgreSQL", "MySQL", "MongoDB", "Salesforce", "S3", "Kafka", "Other"] as const;
const CONTACT_DESTINATIONS = ["Snowflake", "BigQuery", "Redshift", "PostgreSQL", "Other"] as const;

const CONTACT_STEPS = [
  { id: 1 as const, label: "Stack", hint: "Sources & scale" },
  { id: 2 as const, label: "You", hint: "How we reach you" },
];

function ContactPickGrid({
  label,
  options,
  value,
  onChange,
}: {
  label: string;
  options: readonly string[];
  value: string[];
  onChange: (next: string[]) => void;
}) {
  const toggle = (opt: string) => {
    onChange(value.includes(opt) ? value.filter((v) => v !== opt) : [...value, opt]);
  };
  return (
    <fieldset className="lp-ct6-pick">
      <legend>{label}</legend>
      <div className="lp-ct6-pick-grid">
        {options.map((opt) => (
          <button
            key={opt}
            type="button"
            className={`lp-ct6-pick-item${value.includes(opt) ? " is-on" : ""}`}
            onClick={() => toggle(opt)}
            aria-pressed={value.includes(opt)}
          >
            {opt}
          </button>
        ))}
      </div>
    </fieldset>
  );
}

function ContactPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  const [step, setStep] = useState<1 | 2>(1);
  const [sent, setSent] = useState(false);
  const [sources, setSources] = useState<string[]>([]);
  const [destinations, setDestinations] = useState<string[]>([]);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");
  const [role, setRole] = useState("");
  const [volume, setVolume] = useState("");
  const [region, setRegion] = useState("");
  const [timeframe, setTimeframe] = useState("");
  const [message, setMessage] = useState("");
  const [honeypot, setHoneypot] = useState("");

  const canAdvance =
    step === 1
      ? sources.length > 0 && destinations.length > 0 && Boolean(volume && region && timeframe)
      : Boolean(name.trim() && email.trim() && company.trim());

  const goNext = () => {
    if (!canAdvance || step !== 1) return;
    setStep(2);
  };

  const goBack = () => {
    if (step === 2) setStep(1);
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    if (honeypot.trim()) {
      setSent(true);
      return;
    }
    if (step !== 2 || !canAdvance) return;
    // eslint-disable-next-line no-console
    console.info("[marketing/contact] pilot request", {
      sources,
      destinations,
      volume,
      region,
      timeframe,
      name: name.trim(),
      email: email.trim(),
      company: company.trim(),
      role: role.trim(),
      message: message.trim().slice(0, 1200),
      submittedAt: new Date().toISOString(),
    });
    setSent(true);
  };

  return (
    <div className="lp-mkt-page lp-contact-v6">
      <div className="lp-ct6">
        <header className="lp-ct6-top">
          <div className="lp-ct6-top-inner">
            <div className="lp-ct6-top-copy">
              <p className="lp-ct6-kicker">Contact sales</p>
              <h1>Request a pilot</h1>
              <p className="lp-ct6-lead">
                Scoped on your stack — Map → Preflight → Prove. A solutions engineer replies within one
                business day.
              </p>
            </div>
            <aside className="lp-ct6-top-aside" aria-label="What you get">
              <div>
                <strong>&lt;1 day</strong>
                <span>Engineer reply</span>
              </div>
              <div>
                <strong>9 gates</strong>
                <span>Same as production</span>
              </div>
              <div>
                <strong>Σ MATCH</strong>
                <span>Checksum proof</span>
              </div>
            </aside>
          </div>
        </header>

        <div className="lp-ct6-shell">
          {!sent ? (
            <form className="lp-ct6-form" onSubmit={submit} noValidate>
              <nav className="lp-ct6-steps" aria-label="Form progress">
                {CONTACT_STEPS.map((s) => {
                  const state = s.id < step ? "done" : s.id === step ? "active" : "pending";
                  return (
                    <div key={s.id} className={`lp-ct6-step is-${state}`}>
                      <span className="lp-ct6-step-index" aria-hidden>
                        {state === "done" ? "✓" : s.id}
                      </span>
                      <div className="lp-ct6-step-copy">
                        <strong>{s.label}</strong>
                        <em>{s.hint}</em>
                      </div>
                    </div>
                  );
                })}
              </nav>

                {step === 1 ? (
                  <div className="lp-ct6-body">
                    <div className="lp-ct6-body-head">
                      <h2>Your stack</h2>
                      <p>Pick sources, destinations, and scale. You can refine on the call.</p>
                    </div>
                    <div className="lp-ct6-picks">
                      <ContactPickGrid
                        label="Sources"
                        options={CONTACT_SOURCES}
                        value={sources}
                        onChange={setSources}
                      />
                      <ContactPickGrid
                        label="Destinations"
                        options={CONTACT_DESTINATIONS}
                        value={destinations}
                        onChange={setDestinations}
                      />
                    </div>
                    <div className="lp-ct6-fields lp-ct6-fields--3">
                      <label>
                        Daily volume
                        <select
                          className="lp-ct6-input"
                          value={volume}
                          onChange={(e) => setVolume(e.target.value)}
                          required
                        >
                          <option value="">Select…</option>
                          <option value="lt-1m">&lt; 1M rows/day</option>
                          <option value="1m-100m">1M – 100M rows/day</option>
                          <option value="100m-1b">100M – 1B rows/day</option>
                          <option value="gt-1b">&gt; 1B rows/day</option>
                        </select>
                      </label>
                      <label>
                        Region
                        <select
                          className="lp-ct6-input"
                          value={region}
                          onChange={(e) => setRegion(e.target.value)}
                          required
                        >
                          <option value="">Select…</option>
                          <option value="us">US</option>
                          <option value="eu">EU</option>
                          <option value="apac">APAC</option>
                          <option value="other">Other</option>
                        </select>
                      </label>
                      <label>
                        Timeframe
                        <select
                          className="lp-ct6-input"
                          value={timeframe}
                          onChange={(e) => setTimeframe(e.target.value)}
                          required
                        >
                          <option value="">Select…</option>
                          <option value="pilot">Pilot now</option>
                          <option value="prod-30d">Production in 30 days</option>
                          <option value="evaluating">Still evaluating</option>
                        </select>
                      </label>
                    </div>
                  </div>
                ) : (
                  <div className="lp-ct6-body">
                    <div className="lp-ct6-body-head">
                      <h2>How we reach you</h2>
                      <p>One engineer reply with a scoped plan — never a drip campaign.</p>
                    </div>
                    <div className="lp-ct6-fields lp-ct6-fields--2">
                      <label>
                        First name
                        <input
                          className="lp-ct6-input"
                          value={name}
                          onChange={(e) => setName(e.target.value)}
                          required
                          autoComplete="given-name"
                        />
                      </label>
                      <label>
                        Work email
                        <input
                          className="lp-ct6-input"
                          type="email"
                          value={email}
                          onChange={(e) => setEmail(e.target.value)}
                          required
                          autoComplete="email"
                          placeholder="name@company.com"
                        />
                      </label>
                      <label>
                        Company
                        <input
                          className="lp-ct6-input"
                          value={company}
                          onChange={(e) => setCompany(e.target.value)}
                          required
                          autoComplete="organization"
                        />
                      </label>
                      <label>
                        Role
                        <input
                          className="lp-ct6-input"
                          value={role}
                          onChange={(e) => setRole(e.target.value)}
                          placeholder="Data platform lead"
                          autoComplete="organization-title"
                        />
                      </label>
                      <label className="lp-ct6-span-all">
                        Anything else? <em>Optional</em>
                        <textarea
                          className="lp-ct6-input lp-ct6-textarea"
                          value={message}
                          onChange={(e) => setMessage(e.target.value)}
                          rows={2}
                          placeholder="Compliance constraints, cutover windows, deadlines…"
                        />
                      </label>
                      <label className="lp-ct6-hp" aria-hidden="true">
                        Do not fill
                        <input
                          className="lp-ct6-input"
                          tabIndex={-1}
                          autoComplete="off"
                          value={honeypot}
                          onChange={(e) => setHoneypot(e.target.value)}
                        />
                      </label>
                    </div>
                  </div>
                )}

                <div className="lp-ct6-nav">
                  <div className="lp-ct6-nav-left">
                    {step === 2 ? (
                      <button type="button" className="lp-ct6-back" onClick={goBack}>
                        ← Back
                      </button>
                    ) : (
                      <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
                    )}
                  </div>
                  {step === 1 ? (
                    <button
                      type="button"
                      className="lp-btn lp-btn--brand lp-btn--lg"
                      onClick={goNext}
                      disabled={!canAdvance}
                    >
                      Continue
                    </button>
                  ) : (
                    <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg" disabled={!canAdvance}>
                      Request pilot
                    </button>
                  )}
                </div>
              </form>
          ) : (
            <div className="lp-ct6-success" role="status">
              <div className="lp-ct6-success-mark" aria-hidden>
                <DtIcon name="check" size={28} />
              </div>
              <h2>Request received</h2>
              <p>
                A solutions engineer will reply within one business day
                {sources.length > 0 ? ` about ${sources.slice(0, 2).join(", ")}` : ""}
                {destinations.length > 0 ? ` → ${destinations.slice(0, 2).join(", ")}` : ""}.
              </p>
              <p className="lp-ct6-success-mail">
                Or write us now at{" "}
                <a href="mailto:sales@datawrap.io?subject=Datawrap%20pilot%20request">sales@datawrap.io</a>
              </p>
              <div className="lp-ct6-success-actions">
                <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("help")}>
                  Browse docs
                </button>
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("pricing")}>
                  See pricing
                </button>
                <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("enterprise")}>
                  Enterprise
                </button>
              </div>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

function LegalPage({ kind }: { kind: "privacy" | "terms" }) {
  const privacy = [
    {
      h: "Who we are",
      p: "Datawrap, Inc. (“Datawrap”, “we”, “us”) provides a governed data-movement platform. This policy explains how we handle personal data when you visit datawrap.com, create a workspace, or use Transfer Studio, pipelines, Pilot, or MCP. For source and destination datasets you connect, you are the controller; Datawrap is the processor.",
    },
    {
      h: "What we process",
      p: "Account identity (name, email, role), workspace metadata, encrypted connector configurations, job logs, mapping decisions, quarantine summaries, audit events, and support correspondence. We do not sell personal data. We do not use connected source or destination records for advertising.",
    },
    {
      h: "Credentials and secrets",
      p: "Connector passwords, tokens, and keys are encrypted at rest and scoped to the workspace that stored them. Enterprise plans support customer-managed keys (BYOK) so your KMS wraps those secrets. Purpose keys stay bound to the job that needs them. Agents and MCP tools never receive raw destination passwords.",
    },
    {
      h: "Job artifacts",
      p: "Sample rows, mapping proofs, quarantine extracts, and reconcile reports stay inside your tenant boundary. Retention follows the workspace setting (default 90 days unless your agreement says otherwise). You can export or delete artifacts from Settings or by written request.",
    },
    {
      h: "How we use data",
      p: "We use workspace data to operate the service, authenticate users, enforce RBAC, run transfers you start, surface quarantine and reconcile proof, provide support, and improve reliability. We do not train public models on your connected datasets.",
    },
    {
      h: "Legal bases",
      p: "Where GDPR or UK GDPR applies, we process account data to perform the contract, to meet legal obligations, and — for product analytics on the marketing site — on legitimate interests or consent where required. Processing of records inside your sources and destinations is on your instructions as controller.",
    },
    {
      h: "Sharing and subprocessors",
      p: "We share data with infrastructure providers that host the control plane, send transactional email, and (on Enterprise) your identity provider and KMS. We do not share connected datasets with other customers. A current subprocessor list is available on request from privacy@datawrap.ai.",
    },
    {
      h: "International transfers",
      p: "If we transfer personal data out of the EEA, UK, or Switzerland, we use an approved mechanism such as Standard Contractual Clauses and apply the same encryption and access controls described here.",
    },
    {
      h: "Retention",
      p: "Account records last for the life of the workspace plus a short wind-down after deletion. Job logs and samples follow your retention setting. Backup copies expire on the backup cycle. SSO-managed accounts follow your identity-provider lifecycle.",
    },
    {
      h: "Your rights",
      p: "You may request access, correction, export, or deletion of workspace personal data through your admin or privacy@datawrap.ai. You may object to or restrict certain processing where the law allows. You may lodge a complaint with your supervisory authority.",
    },
    {
      h: "Cookies",
      p: "The marketing site uses essential cookies to keep a session and remember consent. Optional analytics cookies load only after you accept. The signed-in product uses session cookies required to stay authenticated.",
    },
    {
      h: "Security",
      p: "We encrypt data in transit (TLS) and credentials at rest, isolate tenants, and record admin, mapping, and job actions in an audit trail. Enterprise customers receive a security questionnaire and a data-processing addendum with their order form.",
    },
    {
      h: "Children",
      p: "The service is for organizations. We do not knowingly collect personal data from children under 16.",
    },
    {
      h: "Changes and contact",
      p: "We will post material changes on this page and update the date below. Questions: privacy@datawrap.ai. Enterprise customers may attach a DPA to their order form.",
    },
  ];
  const terms = [
    {
      h: "Agreement",
      p: "These Terms of Service (“Terms”) govern access to the Datawrap platform, including Transfer Studio, connectors, pipelines, Job Theater, Query, Pilot, and MCP. By creating a workspace or signing an order form you agree to these Terms. If your organization has a signed master agreement, that agreement controls where it conflicts.",
    },
    {
      h: "The service",
      p: "Datawrap moves data you authorize from sources to destinations with mapping, preflight, quarantine, and reconcile proof. Features vary by plan (Starter, Team, Enterprise). We may improve the product with notice when a change affects production behavior. Preview or beta features are labelled and are not warranted as generally available.",
    },
    {
      h: "Accounts and access",
      p: "You are responsible for users you invite, for SSO configuration, and for keeping credentials confidential. Workspace admins control roles for who can map, approve drift, and run production loads. You will promptly revoke access when a person leaves your organization.",
    },
    {
      h: "Acceptable use",
      p: "Use Datawrap only to move data you are authorized to access. Do not probe other tenants, attempt to bypass preflight or quarantine in production, mine the service for vulnerabilities outside a coordinated disclosure, or use the platform to violate export, privacy, or industry law.",
    },
    {
      h: "Customer data",
      p: "You retain all rights to source and destination data. You grant Datawrap a limited license to process that data solely to provide the service you request. You are the controller; Datawrap is the processor. You represent that you have a lawful basis to transfer the data you connect.",
    },
    {
      h: "Connector credentials",
      p: "You supply and control source and destination credentials. We store them encrypted and, on Enterprise, wrapped with your KMS key. You must use least-privilege accounts. Revoking a credential in the source system immediately stops new reads or writes that depend on it.",
    },
    {
      h: "Proof and cutover",
      p: "The platform provides preflight, quarantine, and checksum reports so you can accept or refuse a load. You decide when a route is ready for production. Recurring change delivery uses idempotent upserts so a retried batch cannot create duplicate business keys. Service levels, if any, are set in the order form.",
    },
    {
      h: "Intellectual property",
      p: "Datawrap and its licensors own the platform, documentation, and trademarks. You own your mappings, job artifacts, and connected data. Feedback you send may be used to improve the product without obligation.",
    },
    {
      h: "Confidentiality",
      p: "Each party will protect the other’s confidential information with reasonable care and use it only to perform under these Terms. Job samples and connector secrets are your confidential information.",
    },
    {
      h: "Warranties",
      p: "We warrant that we will provide the service with reasonable skill and care. Except as stated in an order form, THE SERVICE IS OTHERWISE PROVIDED AS IS. Preview and beta features are labelled and are not warranted as generally available.",
    },
    {
      h: "Limitation of liability",
      p: "Neither party is liable for indirect, incidental, special, or consequential damages. Except for confidentiality breaches, IP infringement, or payment obligations, each party’s aggregate liability in a twelve-month period is limited to the fees you paid for the service in that period. Use preflight and reconciliation before production cutovers. Signed enterprise agreements may set different caps.",
    },
    {
      h: "Indemnity",
      p: "You will defend Datawrap against claims arising from data you connect or from use that violates these Terms. We will defend you against claims that the unmodified platform infringes a third-party IP right, and we may replace or refund the affected service.",
    },
    {
      h: "Term and termination",
      p: "These Terms last while you have an active workspace or order. Either party may terminate for material breach not cured within 30 days. On termination we will disable access and, on request, export or delete workspace data within the retention window.",
    },
    {
      h: "Governing law",
      p: "Unless an order form says otherwise, these Terms are governed by the laws of the State of Delaware, excluding conflict-of-law rules. The courts of Delaware have exclusive jurisdiction, except that either party may seek injunctive relief in any court of competent jurisdiction.",
    },
    {
      h: "Changes and contact",
      p: "We will post material changes on this page and update the date below. Continued use after the effective date constitutes acceptance, except that enterprise customers are governed by the notice terms in their order form. Questions: legal@datawrap.ai.",
    },
  ];
  const blocks = kind === "privacy" ? privacy : terms;

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Legal"
        title={kind === "privacy" ? "Privacy" : "Terms of service"}
        lead={
          kind === "privacy"
            ? "How Datawrap handles workspace data, credentials, and audit logs — written for security and legal review."
            : "The agreement that governs use of the Datawrap platform, including Transfer Studio, pipelines, and MCP."
        }
        visual={<MarketingIllustration kind="legal" />}
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-legal-layout">
          <nav className="lp-mkt-legal-nav" aria-label="On this page">
            <h2>On this page</h2>
            <ul>
              {blocks.map((b) => (
                <li key={b.h}>
                  <a href={`#${b.h.toLowerCase().replace(/\s+/g, "-")}`}>{b.h}</a>
                </li>
              ))}
            </ul>
          </nav>
          <div className="lp-mkt-legal-content">
            {blocks.map((b) => (
              <article key={b.h} id={b.h.toLowerCase().replace(/\s+/g, "-")} className="lp-mkt-legal-block">
                <h2>{b.h}</h2>
                <p>{b.p}</p>
              </article>
            ))}
            <p className="lp-mkt-footnote">Last updated August 2026. Enterprise customers receive a DPA and negotiated addenda with their order form.</p>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

function SecurityPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  const layers = [
    {
      phase: "01",
      t: "Isolate",
      tag: "Tenancy",
      d: "Dedicated tenants, workspace scoping, and per-tenant security posture — no shared control-plane bleed.",
      pill: "Tenant isolated",
    },
    {
      phase: "02",
      t: "Encrypt",
      tag: "Keys",
      d: "Customer-managed keys wrap connector secrets. Purpose keys stay scoped to the job that needs them.",
      pill: "BYOK wrapped",
    },
    {
      phase: "03",
      t: "Reside",
      tag: "Regions",
      d: "Pin jobs and artifacts to the regions your policy requires. Audit trails stay where you choose.",
      pill: "Region pinned",
    },
    {
      phase: "04",
      t: "Prove",
      tag: "Reconcile",
      d: "Post-load reconciliation verifies counts and content hashes. Quarantine never silently drops rows.",
      pill: "Checksum MATCH",
    },
  ];

  return (
    <div className="lp-mkt-page lp-sec-v2">
      <section className="lp-sec-hero" aria-label="Security">
        <div className="lp-sec-hero-waves" aria-hidden>
          <span className="lp-wave-grid" />
          <span className="lp-wave-glow lp-wave-glow--1" />
          <span className="lp-wave-glow lp-wave-glow--2" />
        </div>
        <div className="lp-sec-hero-inner">
          <div className="lp-sec-hero-copy">
            <p className="lp-pricing-hero-kicker">
              <span className="lp-pricing-hero-dot" aria-hidden />
              Security
            </p>
            <h1>
              Security that moves with
              <span className="lp-pricing-hero-em"> the data</span>
            </h1>
            <p className="lp-sec-hero-lead">
              Isolation, encryption, residency, and checksum proof — the same governed path your
              transfers already use. Agents inherit it too.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
                Request security pack
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
                Enterprise overview
              </button>
            </div>
          </div>
          <aside className="lp-sec-hero-panel" aria-label="Runtime security posture">
            <header>
              <strong>Runtime posture</strong>
              <span>Live</span>
            </header>
            <div className="lp-sec-hero-row is-ok"><span>Preflight</span><em>9 / 9</em></div>
            <div className="lp-sec-hero-row is-ok"><span>Write</span><em>quarantine surfaced</em></div>
            <div className="lp-sec-hero-row is-ok"><span>Reconcile</span><em>checksum MATCH</em></div>
            <div className="lp-sec-hero-row is-ok"><span>Agents</span><em>MCP under RBAC</em></div>
            <div className="lp-sec-hero-row"><span>Audit</span><em>jobs · maps · keys</em></div>
          </aside>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-sec-badges" aria-label="Compliance posture">
          <ComplianceBadges
            items={[
              "Encryption at rest",
              "SSO / SAML",
              "Customer-managed keys",
              "GDPR processing",
              "Audit logging",
              "Regional residency",
            ]}
          />
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sec-section">
          <div className="lp-sec-section-head">
            <p className="lp-mkt-kicker">Control plane</p>
            <h2>Four layers between source and destination</h2>
            <p>A continuous security path that activates on every transfer — not a wall of checkboxes.</p>
          </div>
          <div className="lp-sec-layer-grid">
            {layers.map((layer) => (
              <article key={layer.phase} className="lp-sec-layer-card">
                <div className="lp-sec-layer-top">
                  <span className="lp-sec-layer-num">{layer.phase}</span>
                  <span className="lp-sec-layer-tag">{layer.tag}</span>
                </div>
                <h3>{layer.t}</h3>
                <p>{layer.d}</p>
                <em>{layer.pill}</em>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sec-agent">
          <div className="lp-sec-agent-copy">
            <p className="lp-mkt-kicker">Runtime proof</p>
            <h2>Agents inherit the same gates</h2>
            <p>
              MCP tools and Datawrap Pilot never receive raw destination passwords. Every agent
              action rides the same RBAC, quarantine, and reconciliation path as Transfer Studio.
            </p>
            <div className="lp-sec-agent-actions">
              <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
                Request security pack
              </button>
              <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("enterprise")}>
                Enterprise capabilities
              </button>
            </div>
          </div>
          <div className="lp-sec-agent-visual" aria-hidden>
            <div className="lp-sec-agent-chip">MCP call</div>
            <div className="lp-sec-agent-path">
              <span>Auth</span><i /><span>Map</span><i /><span>G1–G9</span><i /><span>Prove</span>
            </div>
            <div className="lp-sec-agent-chip is-ok">No raw passwords</div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

function IntegrationsPage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const families = [
    {
      title: "Warehouses",
      body: "Load Snowflake, BigQuery, Redshift, and Databricks with native MERGE, capacity checks, and a reconcile report finance can archive.",
      ids: ["snowflake", "bigquery", "redshift"],
      badge: "Native",
    },
    {
      title: "Object storage",
      body: "Land files and open-table paths on Amazon S3, Azure Data Lake, and Google Cloud Storage with write accounting.",
      ids: ["s3"],
      badge: "Native",
    },
    {
      title: "Databases",
      body: "PostgreSQL, MySQL, SQL Server, Oracle, and MongoDB — upsert, watermark incremental, and checksum proof.",
      ids: ["postgresql", "mysql", "mongodb"],
      badge: "Native",
    },
    {
      title: "Applications",
      body: "Salesforce, Stripe, Shopify, and HubSpot — connect with your integration user.",
      ids: ["salesforce"],
      badge: "Native",
    },
  ];

  return (
    <div className="lp-mkt-page lp-int-v2">
      <section className="lp-int-hero" aria-label="Connectors">
        <div className="lp-int-hero-bg" aria-hidden>
          <span className="lp-wave-grid" />
          <span className="lp-wave-glow lp-wave-glow--1" />
          <span className="lp-wave-glow lp-wave-glow--2" />
        </div>
        <div className="lp-int-hero-inner">
          <div className="lp-int-hero-copy">
            <p className="lp-pricing-hero-kicker">
              <span className="lp-pricing-hero-dot" aria-hidden />
              Connection catalog
            </p>
            <h1>
              Connect the systems
              <span className="lp-pricing-hero-em"> you already run.</span>
            </h1>
            <p className="lp-int-hero-lead">
              Load Snowflake, BigQuery, Redshift, and Databricks. Land files on S3, ADLS, and
              GCS. Connect PostgreSQL, MySQL, SQL Server, Oracle, MongoDB, and the apps your
              revenue team already runs — one catalog, one governed path. Every production load
              still maps, validates, quarantines bad rows, and returns a checksum.
            </p>
            <div className="lp-hero-cta">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
                Open connector catalog
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("help")}>
                Driver docs
              </button>
            </div>
          </div>
          <div className="lp-int-hero-stack" aria-hidden>
            {["postgresql", "snowflake", "bigquery", "mongodb", "kafka", "s3", "mysql", "salesforce"].map((id) => (
              <span key={id} className="lp-int-hero-tile">
                <ConnectorIcon id={id} size={28} />
                <em>{id}</em>
              </span>
            ))}
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-int-strip" aria-label="Capability labels">
          <div><strong>DLQ</strong><span>Quarantine replay</span></div>
          <div><strong>Native</strong><span>Warehouse paths</span></div>
          <div><strong>SQLA</strong><span>Generic drivers</span></div>
          <div><strong>Files</strong><span>CSV · JSON · Parquet</span></div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-int-section">
          <div className="lp-int-section-head">
            <p className="lp-mkt-kicker">Catalog families</p>
            <h2>What you can connect</h2>
            <p>Grouped the way teams plan loads — warehouses first, then lakes, databases, and apps.</p>
          </div>
          <div className="lp-int-family-grid">
            {families.map((f) => (
              <article key={f.title} className="lp-int-family-card">
                <header>
                  <h3>{f.title}</h3>
                  <span>{f.badge}</span>
                </header>
                <p>{f.body}</p>
                <div className="lp-int-family-icons">
                  {f.ids.map((id) => (
                    <span key={id} title={id}>
                      <ConnectorIcon id={id} size={24} />
                    </span>
                  ))}
                </div>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-int-honesty">
          <div>
            <p className="lp-mkt-kicker">The bar</p>
            <h2>Every production load still proves itself</h2>
            <p>
              Every destination still runs the same path: introspect, map, preflight, write, and
              reconcile. Warehouses, lakes, databases, and apps share one engine and one report.
            </p>
            <ul>
              <li>Upserts only where the destination truly supports them</li>
              <li>Incremental sync uses a real watermark, not a guessed cursor</li>
              <li>Bad rows quarantine with a reason — they are never dropped quietly</li>
            </ul>
          </div>
          <aside className="lp-int-honesty-panel">
            <h3>Every route still runs</h3>
            <div><span>01</span><em>Semantic map</em></div>
            <div><span>02</span><em>G1–G9 preflight</em></div>
            <div><span>03</span><em>Quarantine write</em></div>
            <div><span>04</span><em>Checksum prove</em></div>
          </aside>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-int-cta">
          <div>
            <h2>Browse the live catalog</h2>
            <p>Open Transfer Studio and pick a source and destination — same engine as production.</p>
          </div>
          <div className="lp-int-cta-actions">
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Open connector catalog
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
              See Transfer Studio →
            </button>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}
