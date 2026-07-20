import { useState, type CSSProperties, type FormEvent } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../../components/DtIcon";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingIllustration } from "../../components/marketing/MarketingIllustration";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import { isHelpDocRoute } from "../../lib/helpDocs";
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

function FeatureList({ items }: { items: string[] }) {
  return (
    <ul className="lp-mkt-list">
      {items.map((item) => (
        <li key={item}>
          <DtIcon name="check" size={16} />
          <span>{item}</span>
        </li>
      ))}
    </ul>
  );
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

function ComplianceBadges({ items }: { items: string[] }) {
  return (
    <div className="lp-mkt-compliance-badges" aria-label="Compliance posture">
      {items.map((item) => (
        <span key={item} className="lp-mkt-compliance-badge">
          <DtIcon name="shield" size={14} />
          {item}
        </span>
      ))}
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
  const tiers = [
    {
      name: "Starter",
      price: "Free",
      period: "forever for pilots",
      blurb: "Ship your first governed transfer without a sales call.",
      items: [
        "Transfer Studio wizard end-to-end",
        "Community connectors",
        "Local / developer workspace",
        "Basic job history & checksum proof",
        "Preflight gates + quarantine",
      ],
      cta: "Start free",
      action: onGetStarted,
      featured: false,
      tone: "starter" as const,
    },
    {
      name: "Team",
      price: "Custom",
      period: "usage-aligned quote",
      blurb: "Shared migrations, recurring sync, and collaborative ops.",
      items: [
        "Everything in Starter",
        "Pipelines & schedules",
        "Data Pilot assist",
        "Shared connectors & workspaces",
        "Quarantine, reconcile, email support",
      ],
      cta: "Talk to sales",
      action: () => onNavigate("contact"),
      featured: true,
      tone: "team" as const,
    },
    {
      name: "Enterprise",
      price: "Custom",
      period: "security & scale",
      blurb: "Regulated orgs that need SSO, isolation, and agent-native control.",
      items: [
        "Everything in Team",
        "SSO / SAML & RBAC audit trails",
        "Tenant isolation & BYOK",
        "MCP for agents under policy",
        "Dedicated success & questionnaires",
      ],
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      featured: false,
      tone: "enterprise" as const,
    },
  ];

  const compareRows = [
    { feature: "Transfer Studio", starter: "Included", team: "Included", enterprise: "Included" },
    { feature: "Preflight gates & quarantine", starter: "Included", team: "Included", enterprise: "Included" },
    { feature: "Checksum proof", starter: "Included", team: "Included", enterprise: "Included" },
    { feature: "Pipelines & schedules", starter: "—", team: "Included", enterprise: "Included" },
    { feature: "Data Pilot", starter: "—", team: "Included", enterprise: "Included" },
    { feature: "MCP for agents", starter: "—", team: "—", enterprise: "Included" },
    { feature: "SSO / SAML", starter: "—", team: "—", enterprise: "Included" },
    { feature: "BYOK & dedicated tenant", starter: "—", team: "—", enterprise: "Included" },
    { feature: "Support", starter: "Community", team: "Email", enterprise: "Dedicated" },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-page-pricing">
      <MarketingHeroBand
        tone="ink"
        motion="pricing"
        kicker="Pricing"
        title="Clear plans. No silent-failure tax."
        lead="Start free in Transfer Studio. Move to Team when pipelines and collaboration matter. Enterprise when SSO, BYOK, and MCP are non-negotiable."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Start free
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("contact")}>
              Get a quote
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="pricing" />}
      />

      <MarketingReveal className="lp-pricing-promise">
        <div className="lp-pricing-promise-inner" role="list">
          {[
            { t: "Honest free tier", d: "Full Transfer Studio path — not a teaser demo." },
            { t: "Quote when you scale", d: "Team & Enterprise priced to your connectors and cadence." },
            { t: "Same engine everywhere", d: "UI, Pipelines, Pilot, and MCP share one governed path." },
          ].map((item) => (
            <div key={item.t} className="lp-pricing-promise-item" role="listitem">
              <strong>{item.t}</strong>
              <span>{item.d}</span>
            </div>
          ))}
        </div>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-pricing lp-mkt-body lp-pricing-tiers" aria-label="Plans">
          {tiers.map((tier, i) => (
            <article
              key={tier.name}
              className={`lp-mkt-price-card lp-price-card--${tier.tone}${tier.featured ? " is-featured" : ""}`}
              style={{ "--reveal-i": i } as CSSProperties}
            >
              {tier.featured ? <span className="lp-price-badge">Most chosen</span> : null}
              <header className="lp-price-card-head">
                <h2>{tier.name}</h2>
                <p className="lp-mkt-price">{tier.price}</p>
                <p className="lp-price-period">{tier.period}</p>
                <p className="lp-mkt-price-blurb">{tier.blurb}</p>
              </header>
              <FeatureList items={tier.items} />
              <button type="button" className={`lp-btn ${tier.featured ? "lp-btn--brand" : "lp-btn--outline"}`} onClick={tier.action}>
                {tier.cta}
              </button>
            </article>
          ))}
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-compare">
          <div className="lp-pricing-compare-head">
            <h2>Compare capabilities</h2>
            <p>Everything that prevents silent loss ships in Starter. Collaboration and control unlock as you grow.</p>
          </div>
          <div className="lp-mkt-compare-wrap">
            <table className="lp-mkt-compare-table">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">Starter</th>
                  <th scope="col">Team</th>
                  <th scope="col">Enterprise</th>
                </tr>
              </thead>
              <tbody>
                {compareRows.map((row) => (
                  <tr key={row.feature}>
                    <th scope="row">{row.feature}</th>
                    <td data-empty={row.starter === "—" ? "true" : undefined}>{row.starter}</td>
                    <td data-empty={row.team === "—" ? "true" : undefined}>{row.team}</td>
                    <td data-empty={row.enterprise === "—" ? "true" : undefined}>{row.enterprise}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <MarketingSectionFooter>
            <p className="lp-section-cta-text">Need a formal quote, MSA, or security questionnaire?</p>
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function EnterprisePage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const layers = [
    {
      phase: "01",
      t: "Identity",
      d: "SAML/OIDC SSO, SCIM-ready roles, and workspace membership — every transfer inherits who is allowed to run it.",
    },
    {
      phase: "02",
      t: "Tenancy",
      d: "Dedicated tenants, custom domains, and region pinning. No shared control-plane bleed between customers.",
    },
    {
      phase: "03",
      t: "Keys",
      d: "BYOK wraps connector secrets with your KMS. Purpose keys stay scoped to the job that needs them.",
    },
    {
      phase: "04",
      t: "Audit",
      d: "Immutable logs for jobs, mapping decisions, quarantine, and agent MCP calls — ready for SOC review.",
    },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-mkt-enterprise">
      <MarketingHeroBand
        tone="ink"
        kicker="Enterprise"
        title="Governed data movement for the enterprise"
        lead="SSO, RBAC, audit trails, tenant controls, and the same Transfer Studio engine your team already trusts."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={onGetStarted}>
              Start a pilot
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="enterprise" />}
      />

      <MarketingReveal>
        <StatsStrip
          items={[
            { value: "SSO", label: "SAML & OIDC" },
            { value: "BYOK", label: "Customer keys" },
            { value: "100%", label: "Audit coverage" },
            { value: "Multi", label: "Tenant isolation" },
          ]}
        />
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-security-flow">
          <p className="lp-mkt-kicker">Control plane</p>
          <h2>Enterprise controls on every run</h2>
          <p className="lp-mkt-lead">
            A continuous path — identity, tenancy, keys, and audit — not a grid of feature cards.
          </p>
          <ol className="lp-mkt-security-timeline">
            {layers.map((layer) => (
              <li key={layer.phase} className="lp-mkt-security-step">
                <span className="lp-mkt-security-phase">{layer.phase}</span>
                <div>
                  <h3>{layer.t}</h3>
                  <p>{layer.d}</p>
                </div>
              </li>
            ))}
          </ol>
          <ComplianceBadges items={["SOC 2 Type II posture", "GDPR-ready", "HIPAA paths", "Regional residency"]} />
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("security")}>
              Read the security overview
            </button>
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
              Talk to sales
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function CustomersPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  const stories = [
    {
      industry: "Retail",
      metric: "Weekend cutover",
      q: "We replaced a tangle of brittle scripts with DataFlow in a weekend. Preflight caught schema drift that would have cost hours of rework.",
      a: "Alex R.",
      r: "Staff Data Engineer, Fortune 500 retailer",
    },
    {
      industry: "Healthcare",
      metric: "Semantic maps",
      q: "Semantic mapping is genuinely better than string matching. AMT and payment_amount line up even when names change.",
      a: "Priya K.",
      r: "Data Architect, health systems",
    },
    {
      industry: "SaaS",
      metric: "Agent-native",
      q: "MCP let our agent trigger governed transfers from Cursor. Same gates as the UI — that is the future of data ops.",
      a: "Jordan M.",
      r: "Head of Platform, SaaS scale-up",
    },
  ];

  const sectors = [
    { name: "Retail & commerce", detail: "Catalog, orders, inventory — checksummed every load" },
    { name: "Healthcare", detail: "HIPAA-ready posture with quarantine you can audit" },
    { name: "Financial ops", detail: "RBAC, audit trails, and zero silent coercion" },
    { name: "SaaS platforms", detail: "MCP + Studio share one policy surface" },
    { name: "Data mesh teams", detail: "Domain ownership without brittle glue scripts" },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-page-customers">
      <MarketingHeroBand
        tone="ink"
        motion="customers"
        kicker="Customers"
        title="Proof over promises"
        lead="Teams choose DataFlow when accuracy beats raw throughput — retail, healthcare, SaaS, and finance loads that cannot silently fail."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Become a design partner
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("solution-migrations")}>
              Migration stories
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="customers" />}
      />

      <MarketingReveal>
        <div className="lp-cust-metrics" role="list">
          {[
            { value: "12k+", label: "Migrations governed" },
            { value: "99.2%", label: "Preflight pass rate" },
            { value: "0", label: "Silent drops by design" },
            { value: "48h", label: "Pilot kickoff" },
          ].map((item, i) => (
            <div key={item.label} className="lp-cust-metric" role="listitem" style={{ "--reveal-i": i } as CSSProperties}>
              <strong>{item.value}</strong>
              <span>{item.label}</span>
            </div>
          ))}
        </div>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-cust-sectors">
          <div className="lp-cust-sectors-head">
            <h2>Industries that refuse silent failure</h2>
            <p>Real operating contexts — not anonymous logo wallpaper.</p>
          </div>
          <div className="lp-cust-sector-grid">
            {sectors.map((s, i) => (
              <article key={s.name} className="lp-cust-sector" style={{ "--reveal-i": i } as CSSProperties}>
                <strong>{s.name}</strong>
                <span>{s.detail}</span>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-cust-stories">
          <div className="lp-cust-stories-head">
            <h2>From the teams running production loads</h2>
          </div>
          <div className="lp-cust-story-grid">
            {stories.map((item, i) => (
              <blockquote key={item.a} className="lp-cust-story" style={{ "--reveal-i": i } as CSSProperties}>
                <div className="lp-cust-story-meta">
                  <span className="lp-cust-industry">{item.industry}</span>
                  <span className="lp-cust-metric-chip">{item.metric}</span>
                </div>
                <p>&ldquo;{item.q}&rdquo;</p>
                <footer>
                  <strong>{item.a}</strong>
                  <span>{item.r}</span>
                </footer>
              </blockquote>
            ))}
          </div>
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
              Become a design partner
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("pricing")}>
              See pricing
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function ContactPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  const [sent, setSent] = useState(false);
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [company, setCompany] = useState("");
  const [role, setRole] = useState("");
  const [message, setMessage] = useState("");

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setSent(true);
  };

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-page-contact">
      <MarketingHeroBand
        tone="ink"
        motion="contact"
        kicker="Contact sales"
        title="Build a pilot that fits your stack"
        lead="Tell us sources, destinations, and compliance constraints. You get a scoped pilot plan — not a generic demo reel."
        visual={<MarketingIllustration kind="contact" />}
      />

      <section className="lp-mkt-body lp-contact-stage">
        <div className="lp-contact-stage-grid">
          <aside className="lp-contact-rail" aria-label="What happens next">
            <h2>What happens next</h2>
            <ol className="lp-contact-steps">
              {[
                { t: "Day 0", d: "You submit stack + constraints" },
                { t: "Day 1", d: "Solutions engineer assigned" },
                { t: "48h", d: "Pilot scoped to your sources" },
                { t: "Week 1", d: "First governed load with proof" },
              ].map((step) => (
                <li key={step.t}>
                  <strong>{step.t}</strong>
                  <span>{step.d}</span>
                </li>
              ))}
            </ol>
            <div className="lp-contact-trust">
              <span>
                <DtIcon name="shield" size={16} /> SOC 2 posture
              </span>
              <span>
                <DtIcon name="lock" size={16} /> Security questionnaire ready
              </span>
              <span>
                <DtIcon name="check" size={16} /> Start free anytime
              </span>
            </div>
          </aside>

          <div className="lp-contact-panel">
            {sent ? (
              <div className="lp-contact-success" role="status">
                <div className="lp-contact-success-mark" aria-hidden>
                  <DtIcon name="check" size={28} />
                </div>
                <h2>Thanks — we received your note</h2>
                <p>
                  This workspace stores the request locally for demo. In production it routes to sales.
                  Meanwhile explore docs or start free in Transfer Studio.
                </p>
                <div className="lp-hero-cta">
                  <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("help")}>
                    Open docs
                  </button>
                  <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("pricing")}>
                    View pricing
                  </button>
                </div>
              </div>
            ) : (
              <form className="lp-mkt-form lp-contact-form" onSubmit={submit}>
                <div className="lp-contact-form-head">
                  <h2>Request a pilot plan</h2>
                  <p>We respond within one business day.</p>
                </div>
                <div className="lp-contact-fields">
                  <label>
                    Name
                    <input className="lp-mkt-input" value={name} onChange={(e) => setName(e.target.value)} required autoComplete="name" />
                  </label>
                  <label>
                    Work email
                    <input className="lp-mkt-input" type="email" value={email} onChange={(e) => setEmail(e.target.value)} required autoComplete="email" />
                  </label>
                  <label>
                    Company
                    <input className="lp-mkt-input" value={company} onChange={(e) => setCompany(e.target.value)} required autoComplete="organization" />
                  </label>
                  <label>
                    Role
                    <input className="lp-mkt-input" value={role} onChange={(e) => setRole(e.target.value)} placeholder="e.g. Data platform lead" />
                  </label>
                  <label className="lp-contact-span-2">
                    What are you moving?
                    <textarea
                      className="lp-mkt-input lp-mkt-textarea"
                      value={message}
                      onChange={(e) => setMessage(e.target.value)}
                      rows={5}
                      required
                      placeholder="Sources, destinations, volume, compliance needs…"
                    />
                  </label>
                </div>
                <div className="lp-contact-form-actions">
                  <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg">
                    Send to sales
                  </button>
                  <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate("pricing")}>
                    Compare plans
                  </button>
                </div>
              </form>
            )}
          </div>
        </div>
      </section>
    </div>
  );
}

function LegalPage({ kind }: { kind: "privacy" | "terms" }) {
  const privacy = [
    { h: "What we process", p: "Workspace metadata, connector configurations (encrypted), job logs, and account identity needed to operate DataFlow." },
    { h: "Credentials", p: "Connector secrets are encrypted at rest. Enterprise plans support customer-managed keys (BYOK)." },
    { h: "Job artifacts", p: "Transfer samples and quarantine rows stay in your tenant boundary and follow your retention settings." },
    { h: "Your rights", p: "Request export or deletion of workspace data via your admin or sales contact. SSO-managed accounts follow your IdP lifecycle." },
  ];
  const terms = [
    { h: "Acceptable use", p: "Use DataFlow to move data you are authorized to access. Do not probe other tenants or bypass preflight intentionally in production." },
    { h: "Service", p: "We provide Transfer Studio, connectors, pipelines, Pilot, and MCP subject to your plan. Features may evolve with notice." },
    { h: "Data responsibility", p: "You remain the controller of source and destination data. DataFlow is a processor for workspace operations." },
    { h: "Liability", p: "Use preflight and reconciliation before production cutovers. Limit of liability follows your enterprise agreement when signed." },
  ];
  const blocks = kind === "privacy" ? privacy : terms;

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Legal"
        title={kind === "privacy" ? "Privacy" : "Terms of service"}
        lead={
          kind === "privacy"
            ? "How DataFlow handles workspace data, credentials, and audit logs."
            : "Terms governing use of the DataFlow platform."
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
            <p className="lp-mkt-footnote">Last updated July 2026. Enterprise customers receive negotiated addenda as needed.</p>
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
      d: "Dedicated tenants, workspace scoping, and per-tenant security posture — no shared control-plane bleed.",
    },
    {
      phase: "02",
      t: "Encrypt",
      d: "Customer-managed keys wrap connector secrets. Purpose keys stay scoped to the job that needs them.",
    },
    {
      phase: "03",
      t: "Reside",
      d: "Pin jobs and artifacts to the regions your policy requires. Audit trails stay where you choose.",
    },
    {
      phase: "04",
      t: "Prove",
      d: "Post-load reconciliation verifies counts and content hashes. Quarantine never silently drops rows.",
    },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-mkt-security">
      <MarketingHeroBand
        tone="ink"
        kicker="Security"
        title="Security that moves with the data"
        lead="Isolation, encryption, residency, and checksum proof — the same governed path your transfers already use."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Request security pack
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
              Enterprise overview
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="security" />}
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-body--badges">
          <ComplianceBadges items={["SOC 2 Type II posture", "GDPR", "HIPAA-ready paths", "ISO 27001 aligned", "Regional residency"]} />
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-security-flow">
          <p className="lp-mkt-kicker">Control plane</p>
          <h2>Four layers between source and destination</h2>
          <p className="lp-mkt-lead">
            Not a wall of feature cards — a continuous security path that activates on every transfer.
          </p>
          <ol className="lp-mkt-security-timeline">
            {layers.map((layer) => (
              <li key={layer.phase} className="lp-mkt-security-step">
                <span className="lp-mkt-security-phase">{layer.phase}</span>
                <div>
                  <h3>{layer.t}</h3>
                  <p>{layer.d}</p>
                </div>
              </li>
            ))}
          </ol>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-security-proof">
          <div className="lp-mkt-security-proof-copy">
            <p className="lp-mkt-kicker">Runtime proof</p>
            <h2>Agents inherit the same gates</h2>
            <p>
              MCP tools and Data Pilot never receive raw destination passwords. Every agent action rides the same
              RBAC, quarantine, and reconciliation path as Transfer Studio.
            </p>
            <MarketingSectionFooter>
              <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("enterprise")}>
                Enterprise capabilities
              </button>
              <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
                Request security pack
              </button>
            </MarketingSectionFooter>
          </div>
          <div className="lp-mkt-security-proof-panel" aria-hidden>
            <div className="lp-mkt-security-proof-row is-ok"><span>Preflight</span><em>8 / 8</em></div>
            <div className="lp-mkt-security-proof-row is-ok"><span>Write</span><em>quarantine 0</em></div>
            <div className="lp-mkt-security-proof-row is-ok"><span>Reconcile</span><em>checksum match</em></div>
            <div className="lp-mkt-security-proof-row"><span>Audit</span><em>logged</em></div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

function IntegrationsPage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const ids = ["postgresql", "snowflake", "mysql", "mongodb", "bigquery", "redshift", "s3", "dynamodb", "kafka", "salesforce"];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        tone="ink"
        kicker="Connectors"
        title="Hundreds of systems, honest labels"
        lead="Native transfer drivers plus SQLAlchemy generics and file formats — with transfer-ready status you can trust, not inflated marketplace counts."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Connect a system
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="integrations" />}
      />

      <MarketingReveal>
        <StatsStrip
          items={[
            { value: "DLQ", label: "Quarantine replay" },
            { value: "Native", label: "Warehouse paths" },
            { value: "SQLA", label: "Generic drivers" },
            { value: "Files", label: "CSV · JSON · Parquet" },
          ]}
        />
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body">
          <div className="lp-mkt-icon-grid">
            {ids.map((id) => (
              <div key={id} className="lp-mkt-icon-cell">
                <ConnectorIcon id={id} size={32} />
                <span>{id}</span>
              </div>
            ))}
          </div>
          <FeatureList
            items={[
              "Upserts and watermark incremental where the destination supports it",
              "File formats: CSV, JSON, Parquet routes",
              "Object stores: S3, GCS, ADLS",
              "Warehouse bulk paths for Snowflake, BigQuery, Redshift",
            ]}
          />
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Open connector catalog
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}
