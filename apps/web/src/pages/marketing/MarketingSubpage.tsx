import { useState, type FormEvent } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { DtIcon } from "../../components/DtIcon";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import type { PublicRoute } from "../../lib/publicNavigation";

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
      return (
        <ProductPage
          kicker="Product"
          title="Transfer Studio"
          lead="Plan any-to-any loads with semantic mapping, eight preflight gates, and post-load reconciliation — before you trust production writes."
          points={[
            "Semantic column mapping across SQL, NoSQL, files, and warehouses",
            "Eight fail-fast preflight gates including type, nullability, and capacity",
            "Quarantine bad rows without silent failure",
            "Checksum and row-count proof after every write",
            "Same engine behind UI, Data Pilot, and MCP",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="product-pilot"
          nextLabel="Data Pilot"
        />
      );
    case "product-pilot":
      return (
        <ProductPage
          kicker="Product"
          title="Data Pilot"
          lead="Ask natural-language questions about connectors, jobs, and failed gates. Pilot uses the same governed transfer engine as Transfer Studio."
          points={[
            "Triage failed preflight gates in plain language",
            "Propose mapping corrections into your synonym dictionary",
            "Inspect Job Theater runs without leaving chat",
            "Hand off to Transfer Studio when you need the full wizard",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="product-mcp"
          nextLabel="MCP Server"
        />
      );
    case "product-mcp":
      return (
        <ProductPage
          kicker="Product"
          title="MCP Server"
          lead="Trigger governed transfers from Cursor, Claude, and VS Code. Agents get tools — not raw credentials — with the same preflight and proof path as the UI."
          points={[
            "MCP tools for connectors, transfers, and job status",
            "Workspace auth and RBAC respected on every call",
            "Audit trail for agent-initiated runs",
            "Identical mapping and quarantine behavior as Transfer Studio",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="integrations"
          nextLabel="Connectors"
        />
      );
    case "integrations":
      return <IntegrationsPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
    case "solution-migrations":
      return (
        <ProductPage
          kicker="Solutions"
          title="Cross-schema migrations"
          lead="Move databases and files across schemas that never matched 1:1 — with reviewable maps and proof before cutover."
          points={[
            "Auto-detect amount, email, and identifier roles",
            "Review ambiguous maps before production write",
            "Type coercion with fail-fast gates",
            "Checksum-proven loads into warehouses",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="solution-warehouse"
          nextLabel="Warehouse loading"
        />
      );
    case "solution-warehouse":
      return (
        <ProductPage
          kicker="Solutions"
          title="Warehouse loading"
          lead="Bulk-load Snowflake, BigQuery, and Redshift with destination probes, capacity checks, and reconciliation reports finance can trust."
          points={[
            "Native warehouse destinations with upsert and overwrite",
            "Capacity and permission probes before write",
            "Post-load row count and content checksum reports",
            "Schedule recurring warehouse refreshes from Pipelines",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="solution-sync"
          nextLabel="Recurring sync"
        />
      );
    case "solution-sync":
      return (
        <ProductPage
          kicker="Solutions"
          title="Recurring sync"
          lead="Hourly, daily, and weekly pipelines with watermark incremental, upsert modes, and quarantine for bad rows."
          points={[
            "Cadence schedules with workspace visibility",
            "Upsert, append, overwrite, and watermark incremental",
            "Schema-drift blocking with reviewable diffs",
            "Job Theater from queue to reconcile",
          ]}
          onGetStarted={onGetStarted}
          onNavigate={onNavigate}
          next="pricing"
          nextLabel="Pricing"
        />
      );
    default:
      return null;
  }
}

function ProductPage({
  kicker,
  title,
  lead,
  points,
  onGetStarted,
  onNavigate,
  next,
  nextLabel,
}: {
  kicker: string;
  title: string;
  lead: string;
  points: string[];
  onGetStarted: () => void;
  onNavigate: (r: PublicRoute) => void;
  next: PublicRoute;
  nextLabel: string;
}) {
  const workflow = [
    { step: "01", label: "Connect", detail: "Sources, files, and warehouses" },
    { step: "02", label: "Map", detail: "Semantic columns with confidence" },
    { step: "03", label: "Preflight", detail: "Eight fail-fast gates" },
    { step: "04", label: "Proof", detail: "Checksums and reconciliation" },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker={kicker}
        title={title}
        lead={lead}
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Try DataFlow
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
          </div>
        }
        visual={
          <div className="lp-mkt-mock">
            <div className="lp-mkt-mock-bar">
              <span className="lp-mkt-mock-dot" />
              <span className="lp-mkt-mock-dot" />
              <span className="lp-mkt-mock-dot" />
              <span>{title}</span>
            </div>
            <div className="lp-mkt-mock-body">
              {workflow.map((item, i) => (
                <div key={item.step} className={`lp-mkt-mock-row ${i === 1 ? "is-active" : ""}`}>
                  <span>{item.step}</span>
                  <div>
                    <strong>{item.label}</strong>
                    <small>{item.detail}</small>
                  </div>
                  {i < 3 && <em className="lp-mkt-mock-pass">pass</em>}
                </div>
              ))}
            </div>
          </div>
        }
      />

      <section className="lp-mkt-workflow" aria-label="Workflow">
        {workflow.map((item) => (
          <article key={item.step} className="lp-mkt-workflow-step">
            <span className="lp-mkt-workflow-num">{item.step}</span>
            <h3>{item.label}</h3>
            <p>{item.detail}</p>
          </article>
        ))}
      </section>

      <section className="lp-mkt-body">
        <h2>What you get</h2>
        <div className="lp-mkt-feature-grid">
          {points.map((point) => (
            <article key={point} className="lp-mkt-feature-card">
              <span className="lp-mkt-feature-icon" aria-hidden>
                <DtIcon name="check" size={18} />
              </span>
              <p>{point}</p>
            </article>
          ))}
        </div>
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--ghost" onClick={() => onNavigate(next)}>
            Next: {nextLabel} →
          </button>
        </MarketingSectionFooter>
      </section>
    </div>
  );
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

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Pricing"
        title="Plans that match how you move data"
        lead="Start free in Transfer Studio. Scale to Team or Enterprise when you need pipelines, SSO, and agent-native ops."
      />
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
      <MarketingSectionFooter>
        <p className="lp-section-cta-text">Need a formal quote or security questionnaire?</p>
        <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("contact")}>
          Contact sales
        </button>
      </MarketingSectionFooter>
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
        kicker="Enterprise"
        title="Governed data movement for the enterprise"
        lead="SSO, RBAC, audit trails, tenant controls, and the same Transfer Studio engine your team already trusts."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={onGetStarted}>
              Start a pilot
            </button>
          </div>
        }
      />
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
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("security")}>
            Read the security overview
          </button>
        </MarketingSectionFooter>
      </section>
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

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Customers"
        title="Built for teams who cannot afford silent failure"
        lead="Retail, healthcare, SaaS, and finance teams use DataFlow when accuracy matters more than raw throughput alone."
      />
      <section className="lp-mkt-body">
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
        kicker="Contact"
        title="Talk to DataFlow sales"
        lead="Tell us about your sources, destinations, and compliance needs. We will follow up with a pilot plan — not a generic demo reel."
      />
      <section className="lp-mkt-body lp-mkt-contact">
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
      />
      <section className="lp-mkt-body">
        {blocks.map((b) => (
          <article key={b.h} className="lp-mkt-legal-block">
            <h2>{b.h}</h2>
            <p>{b.p}</p>
          </article>
        ))}
        <p className="lp-mkt-footnote">Last updated July 2026. Enterprise customers receive negotiated addenda as needed.</p>
      </section>
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
        kicker="Security"
        title="Security and governance built in"
        lead="Tenant isolation, encryption, residency, and audit-ready jobs — designed for regulated environments from day one."
      />
      <section className="lp-mkt-body">
        <div className="lp-mkt-feature-grid">
          {items.map((c) => (
            <article key={c.t} className="lp-mkt-feature-card">
              <span className="lp-mkt-feature-icon" aria-hidden>
                <DtIcon name="check" size={18} />
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
        </MarketingSectionFooter>
      </section>
    </div>
  );
}

function HelpPage({ onNavigate, onGetStarted }: Pick<PageActions, "onNavigate" | "onGetStarted">) {
  const guides = [
    { t: "Transfer Studio basics", d: "Connect source and destination, review maps, run preflight, write with proof.", r: "product-transfer" as PublicRoute },
    { t: "Connector catalog", d: "Honest transfer-ready labels for native drivers and SQLAlchemy generics.", r: "integrations" as PublicRoute },
    { t: "Preflight gates", d: "Eight gates that block dangerous writes before production.", r: "product-transfer" as PublicRoute },
    { t: "Pipelines & sync", d: "Schedule recurring loads with quarantine and watermark incremental.", r: "solution-sync" as PublicRoute },
    { t: "Data Pilot", d: "Natural-language triage for failed jobs and mapping questions.", r: "product-pilot" as PublicRoute },
    { t: "MCP for agents", d: "Call the same governed engine from Cursor and Claude.", r: "product-mcp" as PublicRoute },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Docs & help"
        title="Learn DataFlow without signing in"
        lead="Public guides for product surfaces. Sign in when you are ready to run a live transfer in your workspace."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Open the app
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("contact")}>
              Ask sales
            </button>
          </div>
        }
      />
      <section className="lp-mkt-body">
        <h2>Guides</h2>
        <div className="lp-mkt-feature-grid">
          {guides.map((g) => (
            <button key={g.t} type="button" className="lp-mkt-feature-card lp-mkt-card--button" onClick={() => onNavigate(g.r)}>
              <span className="lp-mkt-feature-icon" aria-hidden>
                <DtIcon name="sparkle" size={18} />
              </span>
              <div>
                <h3>{g.t}</h3>
                <p>{g.d}</p>
              </div>
            </button>
          ))}
        </div>
        <MarketingSectionFooter>
          <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("contact")}>
            Contact sales
          </button>
        </MarketingSectionFooter>
      </section>
    </div>
  );
}

function IntegrationsPage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const ids = ["postgresql", "snowflake", "mysql", "mongodb", "bigquery", "redshift", "s3", "dynamodb", "kafka", "salesforce"];

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingHeroBand
        kicker="Connectors"
        title="Hundreds of systems, honest labels"
        lead="Native transfer drivers plus SQLAlchemy generics and file formats — with transfer-ready status you can trust, not inflated marketplace counts."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Connect a system
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        }
      />
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
      </section>
    </div>
  );
}
