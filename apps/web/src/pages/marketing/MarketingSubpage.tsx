import { useEffect, useState, type CSSProperties, type FormEvent } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../../components/DtIcon";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingIllustration } from "../../components/marketing/MarketingIllustration";
import { MarketingFigure } from "../../components/marketing/MarketingFigure";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";
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
      return <HelpPage onNavigate={onNavigate} onGetStarted={onGetStarted} />;
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
      blurb: "For pilots and personal projects.",
      items: ["Transfer Studio wizard", "Community connectors", "Local/dev workspace", "Basic job history"],
      cta: "Get started",
      action: onGetStarted,
      featured: false,
    },
    {
      name: "Team",
      price: "Custom",
      blurb: "For shared migrations and recurring sync.",
      items: ["Pipelines & schedules", "Data Pilot", "Shared connectors", "Quarantine & reconcile", "Email support"],
      cta: "Talk to sales",
      action: () => onNavigate("contact"),
      featured: true,
    },
    {
      name: "Enterprise",
      price: "Custom",
      blurb: "For regulated and multi-tenant orgs.",
      items: ["SSO / SAML", "RBAC & audit trails", "Tenant isolation & BYOK", "MCP for agents", "Dedicated success"],
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      featured: false,
    },
  ];

  const compareRows = [
    { feature: "Transfer Studio", starter: true, team: true, enterprise: true },
    { feature: "Preflight gates & quarantine", starter: true, team: true, enterprise: true },
    { feature: "Pipelines & schedules", starter: false, team: true, enterprise: true },
    { feature: "Data Pilot", starter: false, team: true, enterprise: true },
    { feature: "MCP for agents", starter: false, team: false, enterprise: true },
    { feature: "SSO / SAML", starter: false, team: false, enterprise: true },
    { feature: "BYOK & dedicated tenant", starter: false, team: false, enterprise: true },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        tone="ink"
        kicker="Pricing"
        title="Plans that match how you move data"
        lead="Start free in Transfer Studio. Scale to Team or Enterprise when you need pipelines, SSO, and agent-native ops."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Start free
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("contact")}>
              Talk to sales
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="pricing" />}
      />

      <MarketingReveal>
        <StatsStrip
          items={[
            { value: "Free", label: "Starter tier" },
            { value: "8", label: "Preflight gates" },
            { value: "600+", label: "Connectors" },
            { value: "48h", label: "Pilot kickoff" },
          ]}
        />
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-pricing lp-mkt-body">
          {tiers.map((tier) => (
            <article key={tier.name} className={`lp-mkt-price-card ${tier.featured ? "is-featured" : ""}`}>
              <h2>{tier.name}</h2>
              <p className="lp-mkt-price">{tier.price}</p>
              <p className="lp-mkt-price-blurb">{tier.blurb}</p>
              <FeatureList items={tier.items} />
              <button type="button" className={`lp-btn ${tier.featured ? "lp-btn--brand" : "lp-btn--outline"}`} onClick={tier.action}>
                {tier.cta}
              </button>
            </article>
          ))}
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body">
          <h2>Compare plans</h2>
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
                    <td>{row.starter ? "✓" : "—"}</td>
                    <td>{row.team ? "✓" : "—"}</td>
                    <td>{row.enterprise ? "✓" : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <MarketingSectionFooter>
            <p className="lp-section-cta-text">Need a formal quote or security questionnaire?</p>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function EnterprisePage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const pillars = [
    { t: "Identity", d: "SAML/OIDC SSO, SCIM-ready roles, and workspace membership controls.", icon: "users" as const },
    { t: "Audit", d: "Immutable logs for jobs, mapping decisions, quarantine, and agent MCP calls.", icon: "book" as const },
    { t: "Tenancy", d: "Dedicated tenants, custom domains, and region pinning for residency.", icon: "server" as const },
    { t: "Keys", d: "BYOK wraps connector secrets with your KMS — credentials never sit in cleartext.", icon: "lock" as const },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
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
        <section className="lp-mkt-body">
          <h2>Enterprise pillars</h2>
          <div className="lp-mkt-feature-grid">
            {pillars.map((c) => (
              <article key={c.t} className="lp-mkt-feature-card">
                <span className="lp-mkt-feature-icon" aria-hidden>
                  <DtIcon name={c.icon} size={18} />
                </span>
                <div>
                  <h3>{c.t}</h3>
                  <p>{c.d}</p>
                </div>
              </article>
            ))}
          </div>
          <ComplianceBadges items={["SOC 2 Type II posture", "GDPR-ready", "HIPAA paths", "Regional residency"]} />
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("security")}>
              Read the security overview
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

function CustomersPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  const quotes = [
    {
      q: "We replaced a tangle of brittle scripts with DataFlow in a weekend. Preflight caught schema drift that would have cost hours of rework.",
      a: "Alex R.",
      r: "Staff Data Engineer, Fortune 500 retailer",
    },
    {
      q: "Semantic mapping is genuinely better than string matching. AMT and payment_amount line up even when names change.",
      a: "Priya K.",
      r: "Data Architect, health systems",
    },
    {
      q: "MCP let our agent trigger governed transfers from Cursor. Same gates as the UI — that is the future of data ops.",
      a: "Jordan M.",
      r: "Head of Platform, SaaS scale-up",
    },
  ];

  const logos = ["RetailCo", "HealthSys", "CloudScale", "FinOps", "DataMesh"];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        tone="ink"
        kicker="Customers"
        title="Built for teams who cannot afford silent failure"
        lead="Retail, healthcare, SaaS, and finance teams use DataFlow when accuracy matters more than raw throughput alone."
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
        <StatsStrip
          items={[
            { value: "12k+", label: "Migrations run" },
            { value: "99.2%", label: "Preflight pass rate" },
            { value: "4.8", label: "Avg. NPS" },
            { value: "48h", label: "Time to first load" },
          ]}
        />
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body">
          <div className="lp-mkt-logo-row" aria-label="Customer industries">
            {logos.map((name) => (
              <span key={name} className="lp-mkt-logo-pill">
                {name}
              </span>
            ))}
          </div>
          <div className="lp-mkt-quote-grid">
            {quotes.map((item) => (
              <blockquote key={item.a} className="lp-mkt-quote">
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
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("solution-migrations")}>
              See migration stories
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
  const [message, setMessage] = useState("");

  const submit = (e: FormEvent) => {
    e.preventDefault();
    setSent(true);
  };

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        tone="ink"
        kicker="Contact"
        title="Talk to DataFlow sales"
        lead="Tell us about your sources, destinations, and compliance needs. We will follow up with a pilot plan — not a generic demo reel."
        visual={<MarketingIllustration kind="contact" />}
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-contact">
          <div className="lp-mkt-contact-grid">
            <div className="lp-mkt-contact-aside">
              <h2>What to expect</h2>
              <FeatureList
                items={[
                  "Solutions engineer assigned within one business day",
                  "Pilot scoped to your sources and compliance needs",
                  "Security questionnaire support for enterprise",
                  "No obligation — start with Transfer Studio free",
                ]}
              />
              <div className="lp-mkt-trust-strip">
                <span>
                  <DtIcon name="clock" size={16} /> 48h pilot kickoff
                </span>
                <span>
                  <DtIcon name="shield" size={16} /> SOC 2 posture
                </span>
              </div>
            </div>
            <div className="lp-mkt-contact-main">
              {sent ? (
                <div className="lp-mkt-card">
                  <h2>Thanks — we received your note</h2>
                  <p>
                    This demo workspace stores the request locally. In production, it routes to sales. Meanwhile, explore{" "}
                    <button type="button" className="lp-section-link" onClick={() => onNavigate("help")}>
                      docs
                    </button>{" "}
                    or{" "}
                    <button type="button" className="lp-section-link" onClick={() => onNavigate("pricing")}>
                      pricing
                    </button>
                    .
                  </p>
                </div>
              ) : (
                <form className="lp-mkt-form" onSubmit={submit}>
                  <label>
                    Name
                    <input className="lp-mkt-input" value={name} onChange={(e) => setName(e.target.value)} required />
                  </label>
                  <label>
                    Work email
                    <input className="lp-mkt-input" type="email" value={email} onChange={(e) => setEmail(e.target.value)} required />
                  </label>
                  <label>
                    Company
                    <input className="lp-mkt-input" value={company} onChange={(e) => setCompany(e.target.value)} required />
                  </label>
                  <label>
                    What are you moving?
                    <textarea className="lp-mkt-input lp-mkt-textarea" value={message} onChange={(e) => setMessage(e.target.value)} rows={5} required />
                  </label>
                  <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg">
                    Send message
                  </button>
                </form>
              )}
            </div>
          </div>
        </section>
      </MarketingReveal>
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
  const items = [
    { t: "Tenant isolation", d: "Dedicated tenants with workspace scoping and per-tenant security posture." },
    { t: "BYOK encryption", d: "Customer-managed keys wrap connector secrets and purpose keys." },
    { t: "Data residency", d: "Pin jobs and artifacts to regions your policy requires." },
    { t: "Checksum proof", d: "Post-load reconciliation verifies counts and content hashes." },
    { t: "Audit trails", d: "Every run, quarantine row, and schema decision is logged." },
    { t: "Agent controls", d: "MCP tools inherit RBAC — agents never get raw destination passwords." },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        tone="ink"
        kicker="Security"
        title="Security and governance built in"
        lead="Tenant isolation, encryption, residency, and audit-ready jobs — designed for regulated environments from day one."
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
        <section className="lp-mkt-body">
          <h2>Security controls</h2>
          <div className="lp-mkt-feature-grid">
            {items.map((c) => (
              <article key={c.t} className="lp-mkt-feature-card">
                <span className="lp-mkt-feature-icon" aria-hidden>
                  <DtIcon name="shield" size={18} />
                </span>
                <div>
                  <h3>{c.t}</h3>
                  <p>{c.d}</p>
                </div>
              </article>
            ))}
          </div>
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("enterprise")}>
              Enterprise capabilities
            </button>
            <button type="button" className="lp-btn lp-btn--brand" onClick={() => onNavigate("contact")}>
              Request security pack
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}

const HELP_HUB_TOPICS = [
  { id: "concepts", label: "Core concepts", icon: "book" as const },
  { id: "quick-start", label: "Quick start", icon: "zap" as const },
  { id: "architecture", label: "Architecture", icon: "layers" as const },
  { id: "walkthrough", label: "Walkthrough", icon: "transfer" as const },
  { id: "guides", label: "Product guides", icon: "connectors" as const },
  { id: "faq", label: "FAQ", icon: "alert" as const },
] as const;

function HelpPage({ onNavigate, onGetStarted }: Pick<PageActions, "onNavigate" | "onGetStarted">) {
  const [activeHub, setActiveHub] = useState<string>(HELP_HUB_TOPICS[0].id);

  const jump = (id: string) => {
    setActiveHub(id);
    document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  useEffect(() => {
    const nodes = HELP_HUB_TOPICS.map((t) => document.getElementById(t.id)).filter(
      (el): el is HTMLElement => Boolean(el),
    );
    if (!nodes.length) return;
    const observer = new IntersectionObserver(
      (entries) => {
        const visible = entries
          .filter((e) => e.isIntersecting)
          .sort((a, b) => b.intersectionRatio - a.intersectionRatio)[0];
        if (visible?.target?.id) setActiveHub(visible.target.id);
      },
      { rootMargin: "-20% 0px -55% 0px", threshold: [0.15, 0.35, 0.55] },
    );
    nodes.forEach((n) => observer.observe(n));
    return () => observer.disconnect();
  }, []);

  const concepts = [
    {
      icon: "transfer" as const,
      title: "Governed transfers",
      body: "Every load follows one path: connect → map → preflight → write → reconcile. No silent drops.",
      r: "product-transfer" as PublicRoute,
    },
    {
      icon: "sparkle" as const,
      title: "Semantic mapping",
      body: "Columns match by meaning and type, not just names — with confidence scores you can review.",
      r: "product-transfer" as PublicRoute,
    },
    {
      icon: "gate" as const,
      title: "Preflight gates",
      body: "Eight fail-fast checks block dangerous writes before production. Evidence stays with the job.",
      r: "product-transfer" as PublicRoute,
    },
    {
      icon: "check" as const,
      title: "Checksum proof",
      body: "Post-load reconciliation verifies row counts and content hashes before a job is complete.",
      r: "product-transfer" as PublicRoute,
    },
    {
      icon: "activity" as const,
      title: "Quarantine",
      body: "Bad rows are isolated with column, value, and reason — never discarded without a trail.",
      r: "solution-sync" as PublicRoute,
    },
    {
      icon: "zap" as const,
      title: "Agent-native MCP",
      body: "Cursor and Claude call the same governed engine your UI uses — one proof plan everywhere.",
      r: "product-mcp" as PublicRoute,
    },
  ];

  const guides = [
    { t: "Transfer Studio", d: "Semantic maps, eight gates, quarantine, and checksum proof in one wizard.", r: "product-transfer" as PublicRoute, icon: "transfer" as const },
    { t: "Job Theater", d: "Live phases, batch counters, quarantine samples, and proof reports.", r: "product-jobs" as PublicRoute, icon: "jobs" as const },
    { t: "Pipelines", d: "Hourly to weekly sync with watermarks — same gates every tick.", r: "product-pipelines" as PublicRoute, icon: "activity" as const },
    { t: "Query Playground", d: "Ad-hoc SQL and document queries with safe preview and Studio handoff.", r: "product-query" as PublicRoute, icon: "database" as const },
    { t: "Data Pilot", d: "Natural-language triage on failed gates and mapping questions.", r: "product-pilot" as PublicRoute, icon: "sparkle" as const },
    { t: "MCP Server", d: "Agent tools under RBAC — never raw destination passwords.", r: "product-mcp" as PublicRoute, icon: "zap" as const },
    { t: "Connector catalog", d: "Native drivers and generics with honest transfer-ready labels.", r: "integrations" as PublicRoute, icon: "connectors" as const },
    { t: "Enterprise & security", d: "SSO, RBAC, audit, residency, and BYOK posture for regulated teams.", r: "security" as PublicRoute, icon: "shield" as const },
  ];

  const quickStart = [
    { step: "01", title: "Connect", body: "Add source and destination — or upload CSV, JSONL, Parquet.", icon: "connectors" as const },
    { step: "02", title: "Map", body: "Review semantic mappings; pin or reject ambiguous fields.", icon: "sparkle" as const },
    { step: "03", title: "Preflight", body: "Eight gates validate schema, types, capacity, and destination.", icon: "gate" as const },
    { step: "04", title: "Prove", body: "Write, reconcile checksums, inspect quarantine in Job Theater.", icon: "check" as const },
  ];

  const walkthrough = [
    {
      step: 1,
      title: "Connect your systems",
      body: "Pick from 600+ drivers or upload files. Every connector shows an honest transfer-ready label — no inflated marketplace counts.",
      label: "Transfer Studio · Connectors",
      caption: "Native drivers, warehouse paths, and file formats with transfer-ready status.",
      kind: "integrations" as const,
    },
    {
      step: 2,
      title: "Review the semantic map",
      body: "DataFlow proposes column mappings with confidence scores. Accept high-confidence matches; review anything ambiguous before write.",
      label: "Transfer Studio · Mapping",
      caption: "Source columns matched to destination fields — you approve the edge cases.",
      kind: "mapping" as const,
    },
    {
      step: 3,
      title: "Run preflight & prove the load",
      body: "Eight gates validate readiness. After write, checksums reconcile every row and quarantine captures failures with evidence.",
      label: "Job Theater · Proof report",
      caption: "Gates, quarantine, and reconciliation in one governed engine.",
      kind: "security" as const,
    },
  ];

  const archNodes = [
    { label: "Sources", sub: "Files · DBs · SaaS" },
    { label: "Ingest", sub: "Parse · Profile" },
    { label: "Canonical", sub: "Types · Keys" },
    { label: "Mapper", sub: "AI · Rules" },
    { label: "Preflight", sub: "8 gates", gate: true },
    { label: "Execute", sub: "Write · Quarantine" },
    { label: "Targets", sub: "Prove · Reconcile" },
  ];

  const faqs = [
    {
      q: "What is quarantine?",
      a: "Rows that fail validation during load are isolated with the column, value, and reason — never silently dropped.",
    },
    {
      q: "Do I need the API online?",
      a: "File-to-file demo transfers work locally. Live connectors and Job Theater need the control plane API.",
    },
    {
      q: "How is this different from ETL scripts?",
      a: "Preflight gates and post-load reconciliation prove every transfer before and after write — with audit-ready evidence.",
    },
    {
      q: "Can agents run transfers?",
      a: "Yes. The MCP server exposes the same governed engine as Transfer Studio and Data Pilot.",
    },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-mkt-help">
      <MarketingHeroBand
        tone="ink"
        breadcrumb={
          <>
            <button type="button" className="lp-mkt-breadcrumb-link" onClick={() => onNavigate("home")}>
              Home
            </button>
            <span aria-hidden> › </span>
            <span>Knowledge hub</span>
          </>
        }
        kicker="Knowledge hub"
        title="Master governed data movement"
        lead="Concepts, architecture, and product guides — built for teams that need proof on every transfer, not another brittle script."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Open the app
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("contact")}>
              Talk to sales
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="help" />}
      />

      <div className="lp-mkt-help-stats" aria-label="Platform highlights">
        {[
          { v: "8", l: "Preflight gates" },
          { v: "600+", l: "Transfer drivers" },
          { v: "100%", l: "Row proof" },
          { v: "0", l: "Silent drops" },
        ].map((s) => (
          <div key={s.l} className="lp-mkt-help-stat">
            <strong>{s.v}</strong>
            <span>{s.l}</span>
          </div>
        ))}
      </div>

      <section className="lp-mkt-body lp-mkt-docs-layout">
        <nav className="lp-mkt-docs-nav lp-mkt-docs-nav--hub" aria-label="Help sections">
          <h2>Explore</h2>
          <ul>
            {HELP_HUB_TOPICS.map((item) => (
              <li key={item.id}>
                <button
                  type="button"
                  className={`lp-mkt-docs-nav-link${activeHub === item.id ? " is-active" : ""}`}
                  aria-current={activeHub === item.id ? "true" : undefined}
                  onClick={() => jump(item.id)}
                >
                  <DtIcon name={item.icon} size={15} />
                  <span>{item.label}</span>
                </button>
              </li>
            ))}
          </ul>
          <div className="lp-mkt-docs-nav-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--block" onClick={onGetStarted}>
              Open the app
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--block" onClick={() => onNavigate("integrations")}>
              Browse connectors
            </button>
          </div>
        </nav>

        <div className="lp-mkt-docs-content">
          <MarketingReveal>
            <section id="concepts" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">Core concepts</p>
              <h2>The ideas behind every transfer</h2>
              <p className="lp-mkt-lead">
                Same vocabulary your team will see in Transfer Studio, Data Pilot, and MCP — so ops and agents stay aligned.
              </p>
              <div className="lp-mkt-concept-grid">
                {concepts.map((c, i) => (
                  <button
                    key={c.title}
                    type="button"
                    className="lp-mkt-concept-card"
                    style={{ "--i": i } as CSSProperties}
                    onClick={() => onNavigate(c.r)}
                  >
                    <span className="lp-mkt-concept-icon" aria-hidden>
                      <DtIcon name={c.icon} size={22} />
                    </span>
                    <h3>{c.title}</h3>
                    <p>{c.body}</p>
                    <span className="lp-mkt-concept-link">Learn more →</span>
                  </button>
                ))}
              </div>
            </section>
          </MarketingReveal>

          <MarketingReveal>
            <section id="quick-start" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">Quick start</p>
              <h2>From first connector to proof</h2>
              <p className="lp-mkt-lead">Four steps. One governed engine. No silent data loss.</p>
              <div className="lp-mkt-workflow lp-mkt-workflow--help">
                {quickStart.map((item, i) => (
                  <article key={item.step} className="lp-mkt-workflow-step" style={{ "--i": i } as CSSProperties}>
                    <span className="lp-mkt-workflow-num">{item.step}</span>
                    <span className="lp-mkt-workflow-icon" aria-hidden>
                      <DtIcon name={item.icon} size={18} />
                    </span>
                    <h3>{item.title}</h3>
                    <p>{item.body}</p>
                  </article>
                ))}
              </div>
            </section>
          </MarketingReveal>

          <MarketingReveal>
            <section id="architecture" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">Architecture</p>
              <h2>One plane under every surface</h2>
              <p className="lp-mkt-lead">
                Studio, Pilot, MCP, and Pipelines share the same path — so proof is never a separate product.
              </p>
              <div className="lp-mkt-arch-panel lp-mkt-arch-panel--live">
                <div className="lp-mkt-arch-planes">
                  <span>Control · Studio · Pilot · MCP · API</span>
                  <span className="is-data">Canonical data plane</span>
                </div>
                <div className="lp-mkt-arch-flow" aria-label="DataFlow architecture flow">
                  {archNodes.map((n, i) => (
                    <div key={n.label} className="lp-mkt-arch-step" style={{ "--i": i } as CSSProperties}>
                      <div className={`lp-mkt-arch-node ${n.gate ? "is-gate" : ""}`}>
                        <strong>{n.label}</strong>
                        <span>{n.sub}</span>
                      </div>
                      {i < archNodes.length - 1 ? <span className="lp-mkt-arch-connector" aria-hidden /> : null}
                    </div>
                  ))}
                </div>
                <p className="lp-mkt-arch-footnote">
                  Profile → map → validate → execute → reconcile
                </p>
              </div>
            </section>
          </MarketingReveal>

          <MarketingReveal>
            <section id="walkthrough" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">Guided walkthrough</p>
              <h2>See the product before you sign in</h2>
              <p className="lp-mkt-lead">The same surfaces you&rsquo;ll use once credentials are live.</p>
              <div className="lp-mkt-walkthrough">
                {walkthrough.map((w) => (
                  <article key={w.step} className="lp-mkt-walkthrough-row">
                    <div className="lp-mkt-walkthrough-copy">
                      <span className="lp-mkt-workflow-num">{w.step}</span>
                      <h3>{w.title}</h3>
                      <p>{w.body}</p>
                    </div>
                    <MarketingFigure step={w.step} label={w.label} caption={w.caption}>
                      <MarketingIllustration kind={w.kind} />
                    </MarketingFigure>
                  </article>
                ))}
              </div>
            </section>
          </MarketingReveal>

          <MarketingReveal>
            <section id="guides" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">Product guides</p>
              <h2>Go deeper by surface</h2>
              <div className="lp-mkt-guide-grid">
                {guides.map((g, i) => (
                  <button
                    key={g.t}
                    type="button"
                    className="lp-mkt-guide-card"
                    style={{ "--i": i } as CSSProperties}
                    onClick={() => onNavigate(g.r)}
                  >
                    <span className="lp-mkt-guide-icon" aria-hidden>
                      <DtIcon name={g.icon} size={20} />
                    </span>
                    <div>
                      <h3>{g.t}</h3>
                      <p>{g.d}</p>
                    </div>
                    <span className="lp-mkt-guide-arrow" aria-hidden>→</span>
                  </button>
                ))}
              </div>
            </section>
          </MarketingReveal>

          <MarketingReveal>
            <section id="faq" className="lp-mkt-doc-section">
              <p className="lp-mkt-kicker">FAQ</p>
              <h2>Common questions</h2>
              <div className="lp-mkt-faq-grid">
                {faqs.map((f) => (
                  <article key={f.q} className="lp-mkt-faq-card">
                    <h3>{f.q}</h3>
                    <p>{f.a}</p>
                  </article>
                ))}
              </div>
              <MarketingSectionFooter>
                <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("contact")}>
                  Contact sales
                </button>
                <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
                  Start a transfer
                </button>
              </MarketingSectionFooter>
            </section>
          </MarketingReveal>
        </div>
      </section>
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
            { value: "600+", label: "Connectors" },
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
