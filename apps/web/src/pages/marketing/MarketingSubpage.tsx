import { useState, type FormEvent } from "react";
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
            { value: "CDC", label: "Native log capture" },
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
