import { useState, type CSSProperties, type FormEvent } from "react";
import { DtIcon } from "../../components/DtIcon";
import {
  AlgorithmCinemaBand,
  ProofCinema,
} from "../../components/landing/AlgorithmCinema";
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
  // Compact horizontal plan rows — Stripe / Linear cadence, not colorful cards.
  const plans = [
    {
      name: "Starter",
      price: "Free",
      period: "forever for pilots",
      blurb: "Ship a first governed transfer without a sales call — same engine, real preflight.",
      cta: "Start free",
      action: onGetStarted,
      tone: "starter" as const,
      ctaTone: "outline" as const,
    },
    {
      name: "Team",
      price: "Custom",
      period: "usage-aligned quote",
      blurb: "Recurring pipelines, Data Pilot assist, and shared connectors — priced to your cadence.",
      cta: "Talk to sales",
      action: () => onNavigate("contact"),
      tone: "team" as const,
      ctaTone: "brand" as const,
      eyebrow: "Recommended",
    },
    {
      name: "Enterprise",
      price: "Custom",
      period: "security & scale",
      blurb: "SSO, BYOK, tenant isolation, MCP under policy, and a dedicated success engineer.",
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      tone: "enterprise" as const,
      ctaTone: "on-ink" as const,
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
        title="Priced by proof, not seat inflation."
        lead="Every plan runs the same governed engine — semantic mapping, eight preflight gates, quarantine, and checksum reconcile. You choose scope: solo pilot, shared team, or regulated enterprise."
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
        visual={<ProofCinema />}
      />

      {/* Hairline meta row — text, no boxes. Sets the honest-craft tone. */}
      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-meta" aria-label="Pricing principles">
          <div className="lp-pricing-meta-row">
            <span><strong>Honest free tier.</strong> Full Transfer Studio path — no teaser demo.</span>
            <span aria-hidden>·</span>
            <span><strong>Quote when you scale.</strong> Priced to connectors and cadence, never seats-first.</span>
            <span aria-hidden>·</span>
            <span><strong>One engine everywhere.</strong> UI, Pipelines, Pilot, and MCP share the same governed path.</span>
          </div>
        </section>
      </MarketingReveal>

      {/* Table first — this IS the hero of the page. */}
      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-compare" aria-label="Plan comparison">
          <div className="lp-pricing-compare-head">
            <p className="lp-mkt-kicker">Compare</p>
            <h2>Capabilities across every plan</h2>
            <p>
              Everything that prevents silent data loss ships in Starter. Collaboration surfaces
              unlock in Team; identity, tenancy, and agent controls unlock in Enterprise.
            </p>
          </div>
          <div className="lp-mkt-compare-wrap">
            <table className="lp-mkt-compare-table lp-mkt-compare-table--team-emph">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">Starter</th>
                  <th scope="col" className="is-team-col">
                    Team<span className="lp-compare-team-flag" aria-hidden>Recommended</span>
                  </th>
                  <th scope="col">Enterprise</th>
                </tr>
              </thead>
              <tbody>
                {compareRows.map((row) => (
                  <tr key={row.feature}>
                    <th scope="row">{row.feature}</th>
                    <td data-empty={row.starter === "—" ? "true" : undefined}>{row.starter}</td>
                    <td className="is-team-col" data-empty={row.team === "—" ? "true" : undefined}>{row.team}</td>
                    <td data-empty={row.enterprise === "—" ? "true" : undefined}>{row.enterprise}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      </MarketingReveal>

      {/* Three compact horizontal plan rows below the table — not square cards. */}
      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-plans" aria-label="Plans">
          <ol className="lp-price-plan-list">
            {plans.map((plan, i) => (
              <li
                key={plan.name}
                className={`lp-price-plan-row lp-price-plan-row--${plan.tone}`}
                style={{ "--reveal-i": i } as CSSProperties}
              >
                <div className="lp-price-plan-row-lead">
                  {plan.eyebrow ? (
                    <span className="lp-price-plan-eyebrow">{plan.eyebrow}</span>
                  ) : null}
                  <h3 className="lp-price-plan-name">{plan.name}</h3>
                  <p className="lp-price-plan-period">{plan.period}</p>
                </div>
                <div className="lp-price-plan-row-price">
                  <strong>{plan.price}</strong>
                </div>
                <p className="lp-price-plan-row-blurb">{plan.blurb}</p>
                <div className="lp-price-plan-row-cta">
                  <button
                    type="button"
                    className={
                      plan.ctaTone === "brand"
                        ? "lp-btn lp-btn--brand"
                        : plan.ctaTone === "on-ink"
                          ? "lp-btn lp-btn--outline lp-btn--on-ink"
                          : "lp-btn lp-btn--outline"
                    }
                    onClick={plan.action}
                  >
                    {plan.cta}
                  </button>
                </div>
              </li>
            ))}
          </ol>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-footer-cta">
          <div className="lp-pricing-footer-cta-inner">
            <div className="lp-pricing-footer-cta-copy">
              <h3>Procurement, MSA, or a security pack?</h3>
              <p>
                Enterprise deals ship with a negotiated MSA, DPA, SOC&nbsp;2 posture pack, and a
                pre-populated security questionnaire — reviewed by a real solutions engineer.
              </p>
            </div>
            <div className="lp-pricing-footer-cta-actions">
              <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
                Contact sales
              </button>
              <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("security")}>
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
        lead="SSO, RBAC, audit trails, and tenant isolation — on the same Transfer Studio engine your operators already trust. No parallel “enterprise-only” path that skips gates."
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
            { value: "Full", label: "Job audit trail" },
            { value: "Multi", label: "Tenant isolation" },
          ]}
        />
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-enterprise-band-inner">
          <div className="lp-enterprise-split">
            <div>
              <p className="lp-mkt-kicker">How it lands</p>
              <h2>One engine. Enterprise controls on top.</h2>
              <p>
                Procurement does not get a different transfer algorithm. Your teams map, preflight,
                quarantine, and reconcile the same way — with identity, tenancy, and keys wrapping
                every run.
              </p>
              <ul className="lp-enterprise-checklist">
                <li>Workspace RBAC for who can map, approve drift, and run production loads</li>
                <li>Region pinning for jobs and artifacts when policy requires residency</li>
                <li>MCP and Data Pilot inherit the same gates — agents never get a silent shortcut</li>
                <li>Security questionnaire + SOC 2 posture pack for procurement kickoff</li>
              </ul>
            </div>
            <aside className="lp-enterprise-panel" aria-label="Enterprise control snapshot">
              <h3>Control snapshot</h3>
              <div className="lp-enterprise-panel-row"><span>Identity</span><em>SSO enforced</em></div>
              <div className="lp-enterprise-panel-row"><span>Secrets</span><em>BYOK wrapped</em></div>
              <div className="lp-enterprise-panel-row"><span>Preflight</span><em>8 / 8 required</em></div>
              <div className="lp-enterprise-panel-row"><span>Quarantine</span><em>surfaced, never dropped</em></div>
              <div className="lp-enterprise-panel-row"><span>Reconcile</span><em>checksum + counts</em></div>
              <div className="lp-enterprise-panel-row"><span>Audit</span><em>jobs · maps · agents</em></div>
            </aside>
          </div>
        </section>
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
        visual={<ProofCinema />}
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-pricing-meta" aria-label="Operating outcomes">
          <div className="lp-pricing-meta-row">
            <span><strong>12k+</strong> migrations governed</span>
            <span aria-hidden>·</span>
            <span><strong>0</strong> silent drops by design</span>
            <span aria-hidden>·</span>
            <span><strong>48h</strong> typical pilot kickoff</span>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-cust-sectors">
          <div className="lp-cust-sectors-head">
            <h2>Industries that refuse silent failure</h2>
            <p>Operating contexts — not anonymous logo wallpaper.</p>
          </div>
          <ul className="lp-cust-sector-list">
            {sectors.map((s) => (
              <li key={s.name}>
                <strong>{s.name}</strong>
                <span>{s.detail}</span>
              </li>
            ))}
          </ul>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-mkt-body lp-cust-stories">
          <div className="lp-cust-stories-head">
            <h2>From teams running production loads</h2>
          </div>
          <div className="lp-cust-story-stack">
            {stories.map((item) => (
              <blockquote key={item.a} className="lp-cust-story-row">
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

const CONTACT_SOURCES = [
  "PostgreSQL",
  "MySQL",
  "MongoDB",
  "Salesforce",
  "S3",
  "Kafka",
  "Other",
];

const CONTACT_DESTINATIONS = [
  "Snowflake",
  "BigQuery",
  "Redshift",
  "PostgreSQL",
  "Other",
];

/** Ordered wizard chapters — mirror the Transfer Studio stepper feel. */
const CONTACT_STEPS: { id: 1 | 2 | 3 | 4; label: string; sub: string }[] = [
  { id: 1, label: "Sources", sub: "Where the data lives" },
  { id: 2, label: "Destinations", sub: "Where it needs to land" },
  { id: 3, label: "Scale", sub: "Volume · region · timeline" },
  { id: 4, label: "Contact", sub: "How we reach you" },
];

function ChipMulti({
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
    <fieldset className="lp-contact-chips" aria-label={label}>
      <legend className="lp-contact-chips-legend">{label}</legend>
      <div className="lp-contact-chip-row">
        {options.map((opt) => (
          <button
            key={opt}
            type="button"
            className={`lp-contact-chip${value.includes(opt) ? " is-on" : ""}`}
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
  const [step, setStep] = useState<1 | 2 | 3 | 4>(1);
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
      ? sources.length > 0
      : step === 2
        ? destinations.length > 0
        : step === 3
          ? Boolean(volume && region && timeframe)
          : true;

  const goNext = () => {
    if (!canAdvance) return;
    if (step < 4) setStep((step + 1) as 1 | 2 | 3 | 4);
  };
  const goBack = () => {
    if (step > 1) setStep((step - 1) as 1 | 2 | 3 | 4);
  };

  const submit = (e: FormEvent) => {
    e.preventDefault();
    // Honeypot: any value here means a bot filled the hidden field — drop silently.
    if (honeypot.trim()) {
      setSent(true);
      return;
    }
    const payload = {
      sources,
      destinations,
      volume,
      region,
      timeframe,
      // Scrubbed personal fields: strings are trimmed; message is truncated to
      // avoid dumping large paste-blobs to browser console during demos.
      name: name.trim(),
      email: email.trim(),
      company: company.trim(),
      role: role.trim(),
      message: message.trim().slice(0, 1200),
      submittedAt: new Date().toISOString(),
    };
    // Demo-safe: local console record only. Never persist beyond the session.
    // eslint-disable-next-line no-console
    console.info("[marketing/contact] pilot request", payload);
    setSent(true);
  };

  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-page-contact">
      <MarketingHeroBand
        tone="ink"
        motion="contact"
        kicker="Contact sales"
        title="Build a pilot that fits your stack"
        lead="Tell us sources, destinations, and compliance constraints. You get a scoped pilot plan — same eight gates, same reconciliation, on your data."
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("pricing")}>
              See pricing
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
              Enterprise overview
            </button>
          </div>
        }
        visual={<MarketingIllustration kind="contact" />}
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-connectors-narrative">
          <p className="lp-mkt-kicker">Pilot wizard</p>
          <h2>Scope a governed pilot</h2>
          <p className="lp-mkt-lead">
            Four short steps — the same shape as Transfer Studio. Nothing is submitted until step four.
          </p>
        <div className="lp-contact-wizard-stage" aria-label="Pilot wizard">
          <ol className="lp-contact-stepper" aria-label="Wizard progress">
            {CONTACT_STEPS.map((s) => {
              const state = s.id < step ? "done" : s.id === step ? "active" : "pending";
              return (
                <li key={s.id} className={`lp-contact-stepper-item is-${state}`}>
                  <span className="lp-contact-stepper-index">{s.id}</span>
                  <span className="lp-contact-stepper-body">
                    <strong>{s.label}</strong>
                    <em>{s.sub}</em>
                  </span>
                </li>
              );
            })}
          </ol>

          {sent ? (
            <div className="lp-contact-success" role="status">
              <div className="lp-contact-success-mark" aria-hidden>
                <DtIcon name="check" size={28} />
              </div>
              <h3>Thanks — pilot request received</h3>
              <p>
                A solutions engineer will reply within one business day with a scoped plan for
                {sources.length > 0 ? ` ${sources.join(", ")}` : " your sources"}
                {destinations.length > 0 ? ` → ${destinations.join(", ")}` : ""}.
              </p>
              <div className="lp-hero-cta">
                <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("help")}>
                  Open docs
                </button>
                <button type="button" className="lp-btn lp-btn--outline lp-btn--lg" onClick={() => onNavigate("pricing")}>
                  View pricing
                </button>
              </div>
            </div>
          ) : (
            <form className="lp-mkt-form lp-contact-wizard-form" onSubmit={submit}>
              {step === 1 ? (
                <div className="lp-contact-step-body">
                  <h3>Which sources are you moving?</h3>
                  <p>Select one or more — you can add specifics on the last step.</p>
                  <ChipMulti label="Sources" options={CONTACT_SOURCES} value={sources} onChange={setSources} />
                </div>
              ) : null}

              {step === 2 ? (
                <div className="lp-contact-step-body">
                  <h3>Where do the loads need to land?</h3>
                  <p>Warehouses, operational DBs, or another destination — the wizard adapts.</p>
                  <ChipMulti
                    label="Destinations"
                    options={CONTACT_DESTINATIONS}
                    value={destinations}
                    onChange={setDestinations}
                  />
                </div>
              ) : null}

              {step === 3 ? (
                <div className="lp-contact-step-body">
                  <h3>Volume, region, and timeframe</h3>
                  <p>We use this to right-size the pilot and residency setup — never for gating access.</p>
                  <div className="lp-contact-scale-grid">
                    <label>
                      Daily volume
                      <select
                        className="lp-mkt-input"
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
                        className="lp-mkt-input"
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
                        className="lp-mkt-input"
                        value={timeframe}
                        onChange={(e) => setTimeframe(e.target.value)}
                        required
                      >
                        <option value="">Select…</option>
                        <option value="pilot">Pilot</option>
                        <option value="prod-30d">Production in 30 days</option>
                        <option value="evaluating">Evaluating</option>
                      </select>
                    </label>
                  </div>
                </div>
              ) : null}

              {step === 4 ? (
                <div className="lp-contact-step-body">
                  <h3>How do we reach you?</h3>
                  <p>One reply from a real solutions engineer — no drip nurture.</p>
                  <div className="lp-contact-fields lp-contact-fields--wizard">
                    <label>
                      Name
                      <input
                        className="lp-mkt-input"
                        value={name}
                        onChange={(e) => setName(e.target.value)}
                        required
                        autoComplete="name"
                      />
                    </label>
                    <label>
                      Work email
                      <input
                        className="lp-mkt-input"
                        type="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        required
                        autoComplete="email"
                      />
                    </label>
                    <label>
                      Company
                      <input
                        className="lp-mkt-input"
                        value={company}
                        onChange={(e) => setCompany(e.target.value)}
                        required
                        autoComplete="organization"
                      />
                    </label>
                    <label>
                      Role
                      <input
                        className="lp-mkt-input"
                        value={role}
                        onChange={(e) => setRole(e.target.value)}
                        placeholder="e.g. Data platform lead"
                      />
                    </label>
                    <label className="lp-contact-span-2">
                      Anything specific to call out?
                      <textarea
                        className="lp-mkt-input lp-mkt-textarea"
                        value={message}
                        onChange={(e) => setMessage(e.target.value)}
                        rows={4}
                        placeholder="Compliance constraints, cutover windows, migration deadlines…"
                      />
                    </label>
                    {/* Honeypot — hidden from users; bots that autofill get quarantined. */}
                    <label className="lp-contact-hp" aria-hidden="true">
                      Do not fill
                      <input
                        className="lp-mkt-input"
                        tabIndex={-1}
                        autoComplete="off"
                        value={honeypot}
                        onChange={(e) => setHoneypot(e.target.value)}
                      />
                    </label>
                  </div>
                </div>
              ) : null}

              <div className="lp-contact-wizard-nav">
                <button
                  type="button"
                  className="lp-btn lp-btn--ghost"
                  onClick={goBack}
                  disabled={step === 1}
                >
                  Back
                </button>
                {step < 4 ? (
                  <button
                    type="button"
                    className="lp-btn lp-btn--brand lp-btn--lg"
                    onClick={goNext}
                    disabled={!canAdvance}
                  >
                    Continue
                  </button>
                ) : (
                  <button type="submit" className="lp-btn lp-btn--brand lp-btn--lg">
                    Send pilot request
                  </button>
                )}
              </div>
            </form>
          )}
        </div>

        <p className="lp-contact-footnote">
          Response SLA: one business day from a real person on the platform team — not a generic MQL nurture drip.
          Prefer email? Write to <a href="mailto:sales@dataflow.dev?subject=DataFlow%20pilot%20request">sales@dataflow.dev</a>.
          SOC 2 posture &middot; security questionnaire ready &middot; start free any time.
        </p>
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
  return (
    <div className="lp-mkt-page lp-mkt-page-rich lp-page-integrations">
      <MarketingHeroBand
        tone="ink"
        kicker="Connectors"
        title="Hundreds of systems, honest labels"
        lead="Catalog tiles are not the same as transfer-ready drivers. DataFlow publishes both — and every production path still runs semantic mapping, eight gates, quarantine, and checksum proof."
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
        <section className="lp-mkt-body lp-connectors-narrative">
          <p className="lp-mkt-kicker">Honesty bar</p>
          <h2>What “transfer-ready” means</h2>
          <p>
            A connector is transfer-ready when introspect, map, preflight, write, and reconcile have
            production evidence for that driver family — not when a tile appears in a catalog.
            Upserts and watermark incremental only advertise where the destination truly supports them.
          </p>
          <ul className="lp-connectors-narrative-list">
            <li>Warehouses: Snowflake, BigQuery, Redshift bulk paths with capacity probes</li>
            <li>Operational SQL: PostgreSQL, MySQL, SQL Server with upsert / incremental where proven</li>
            <li>Documents &amp; files: MongoDB, CSV, JSON, Parquet with type-honest create-new DDL</li>
            <li>Object stores: S3, GCS, ADLS with multi-chunk write accounting</li>
          </ul>
        </section>
      </MarketingReveal>

      <AlgorithmCinemaBand
        kicker="How a connector route actually runs"
        title="Every driver inherits the same governed engine"
        lead="Whether you land PostgreSQL → Snowflake or Mongo → BigQuery, the path is map → G1–G8 → write with quarantine → reconcile. No marketplace shortcut skips proof."
      >
        <ProofCinema />
      </AlgorithmCinemaBand>

      <MarketingReveal>
        <section className="lp-mkt-body lp-connectors-narrative">
          <MarketingSectionFooter>
            <button type="button" className="lp-btn lp-btn--brand" onClick={onGetStarted}>
              Open connector catalog
            </button>
            <button type="button" className="lp-btn lp-btn--outline" onClick={() => onNavigate("product-transfer")}>
              See Transfer Studio algorithms →
            </button>
          </MarketingSectionFooter>
        </section>
      </MarketingReveal>
    </div>
  );
}
