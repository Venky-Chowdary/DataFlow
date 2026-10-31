import { type CSSProperties } from "react";
import { DtIcon } from "../../components/DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import {
  AlgorithmCinemaBand,
  ProofCinema,
} from "../../components/landing/AlgorithmCinema";
import { MarketingInkHero } from "../../components/marketing/MarketingInkHero";
import {
  EvidencePlatesArt,
  PlanLadderArt,
} from "../../components/marketing/hero-art/companyArt";
import {
  LatticeArt,
  PerimeterArt,
  VaultArt,
} from "../../components/marketing/hero-art/enterpriseArt";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { MarketingSectionFooter } from "../../components/marketing/MarketingSectionFooter";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  EVIDENCE_AS_OF,
  MARKETING_PROOF_HIGHLIGHTS,
  MARKETING_STACK,
  TRANSFER_READY_DRIVERS,
  catalogHonestyLead,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import { ContactSalesPage } from "./ContactSalesPage";
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
 * Control posture badges. Each label names a shipped control
 * (docs/MARKETING_AUDIT_2026-10.md §2); certifications are not listed.
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
        Encryption, SSO, SCIM, customer-managed keys, and a tamper-evident audit log ship with
        the product. Request the security questionnaire and DPA for procurement.
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
      return <ContactSalesPage onNavigate={onNavigate} />;
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
      blurb: "Ship a governed, verified transfer today on the full production engine.",
      cta: "Start free",
      action: onGetStarted,
      tone: "starter" as const,
      features: [
        "Transfer Studio — table, destination query, stored procedure",
        "G1–G9 preflight + quarantine with replay",
        "Gate-8 read-back with value & key census",
        `${TRANSFER_READY_DRIVERS} transfer-ready drivers`,
      ],
    },
    {
      name: "Team",
      price: "Custom",
      period: "usage-aligned",
      blurb: "Pipelines, native CDC, Pilot, and shared connectors — priced to cadence and connectors.",
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      tone: "team" as const,
      featured: true,
      features: [
        "Everything in Starter",
        "Schedules, typed watermarks & native CDC",
        "Datawrap Pilot on its own local engine",
        "Shared connectors + email support",
      ],
    },
    {
      name: "Enterprise",
      price: "Custom",
      period: "security & scale",
      blurb: "SSO, SCIM, BYOK, workspace isolation, MCP under policy, and a dedicated success engineer.",
      cta: "Contact sales",
      action: () => onNavigate("contact"),
      tone: "enterprise" as const,
      features: [
        "Everything in Team",
        "SSO (OIDC / SAML) + SCIM + BYOK",
        "MCP for agents + tamper-evident audit",
        "Dedicated success",
      ],
    },
  ];

  const compareRows = [
    { feature: "Transfer Studio (table / destination query / stored procedure)", starter: true, team: true, enterprise: true },
    { feature: "G1–G9 preflight & quarantine with replay", starter: true, team: true, enterprise: true },
    { feature: "Gate-8 read-back: value & key census", starter: true, team: true, enterprise: true },
    { feature: "Source query / stored procedure read", starter: true, team: true, enterprise: true },
    { feature: "Signed risk contracts & proof packs", starter: true, team: true, enterprise: true },
    { feature: "Pipelines, schedules & typed watermarks", starter: false, team: true, enterprise: true },
    { feature: "Native CDC (SQL Server, PostgreSQL, MySQL, Oracle)", starter: false, team: true, enterprise: true },
    { feature: "Datawrap Pilot", starter: false, team: true, enterprise: true },
    { feature: "MCP for agents", starter: false, team: false, enterprise: true },
    { feature: "SSO (OIDC / SAML) & SCIM", starter: false, team: false, enterprise: true },
    { feature: "BYOK & custom domain", starter: false, team: false, enterprise: true },
    { feature: "Tamper-evident audit chain & signed webhooks", starter: false, team: false, enterprise: true },
  ];

  return (
    <div className="lp-mkt-page lp-page-pricing lp-pricing-v2">
      <MarketingInkHero
        kicker="Pricing"
        title={<>Plans that scale with proof.</>}
        lead={
          <>
            Datawrap prices cadence and security. Semantic mapping, G1–G9 preflight, quarantine
            with replay, destination query and stored-procedure writes, and value-by-value
            verification all ship in Starter.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Start for free
            </button>
            <button
              type="button"
              className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink"
              onClick={() => onNavigate("contact")}
            >
              Contact sales
            </button>
          </div>
        }
        slas={[
          { value: "Free", label: "Starter forever" },
          { value: "Verified", label: "Every load, every plan" },
          { value: String(TRANSFER_READY_DRIVERS), label: "Transfer-ready drivers" },
          { value: "Custom", label: "Team & Enterprise" },
        ]}
        aside={
          <div className="lp-sales-hero-art">
            <PlanLadderArt />
          </div>
        }
      />

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
              Verification ships in Starter. Schedules, CDC, and collaboration unlock in Team.
              Identity, tenancy, audit, and agents unlock in Enterprise.
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
            <strong>A real free tier</strong>
            <span>The full Transfer Studio path, verification included.</span>
          </div>
          <div className="lp-pricing-principle">
            <strong>Quote when you scale</strong>
            <span>Priced to connectors and cadence.</span>
          </div>
          <div className="lp-pricing-principle">
            <strong>One engine everywhere</strong>
            <span>UI, Pipelines, Pilot, and MCP share the same gates.</span>
          </div>
        </section>
      </MarketingReveal>

      <section className="lp-sales-compare" aria-label="What every plan includes">
        <div className="lp-mkt-wrap">
          <header>
            <p className="lp-sales-kicker lp-sales-kicker--ink">In every plan</p>
            <h2>Pay for cadence. Verification is always included.</h2>
            <p>
              We quote connectors and schedule. The proof engine is the same in every plan — it is
              how Datawrap moves data.
            </p>
          </header>
          <div className="lp-sales-table-wrap">
            <table className="lp-sales-table">
              <thead>
                <tr>
                  <th scope="col">Dimension</th>
                  <th scope="col">What you get</th>
                  <th scope="col">How it works</th>
                  <th scope="col">Plans</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <th scope="row">Meter</th>
                  <td>Cadence + security tier</td>
                  <td>Quoted when you scale</td>
                  <td>Team · Enterprise</td>
                </tr>
                <tr>
                  <th scope="row">Proof</th>
                  <td>Gate-8 read-back</td>
                  <td>Row counts plus a value and key census from the destination</td>
                  <td>All plans</td>
                </tr>
                <tr>
                  <th scope="row">Held-back rows</th>
                  <td>Quarantine + replay</td>
                  <td>Column, value, and reason in a durable workspace queue</td>
                  <td>All plans</td>
                </tr>
                <tr>
                  <th scope="row">Destination SQL</th>
                  <td>Query and stored procedure</td>
                  <td>One INSERT/MERGE/UPDATE or CALL per row, binds only</td>
                  <td>All plans</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-pricing-v2-cta">
          <div className="lp-pricing-v2-cta-inner">
            <div>
              <h3>Procurement, MSA, or a security pack?</h3>
              <p>
                Enterprise deals ship with a negotiated MSA, DPA, a pre-populated security
                questionnaire, and a workspace audit export your reviewers can verify.
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
      title: "SSO and SCIM that gate every run",
      body: "OIDC and SAML sign-in with email-domain routing to the right identity provider. SCIM 2.0 provisions users and maps groups to workspace roles; deactivating a user revokes their sessions and keys.",
    },
    {
      id: "access",
      kicker: "02 · Access",
      title: "RBAC with API keys that can't outrank you",
      body: "Viewer, Editor, and Admin roles on every Studio job, Pilot session, and MCP call. An API key carries a role and scopes no higher than the person who created it, and the audit log names the key.",
    },
    {
      id: "tenancy",
      kicker: "03 · Tenancy",
      title: "Workspace isolation at the store layer",
      body: "Every read of jobs, connectors, schedules, notifications, and audit is scoped to the caller's workspace — Pilot and MCP tools included. Custom domains and a per-workspace data region on top.",
    },
    {
      id: "keys",
      kicker: "04 · Keys",
      title: "BYOK on connector secrets",
      body: "Connector secrets are encrypted at rest and can be wrapped with your own key: local, wrapped, or AWS KMS, with rotation. PII hashing uses a per-workspace key generated on first use.",
    },
    {
      id: "audit",
      kicker: "05 · Audit",
      title: "A tamper-evident audit chain",
      body: "Every record is hash-chained and signed with the chain's own key, named by a key id. Verify the chain per workspace, filter by action, level, and time, and export CSV, JSON, NDJSON, or CEF.",
    },
    {
      id: "operations",
      kicker: "06 · Operations",
      title: "Policies you can turn on",
      body: "Idle session timeout, signed webhooks (HMAC-SHA256 with per-channel secrets), job-history retention purge with a dry run first, and deployment settings reserved for platform admins.",
    },
  ];

  return (
    <div className="lp-mkt-page lp-mkt-enterprise-v2">
      <MarketingInkHero
        kicker="Enterprise"
        title={<>Controls that travel with every load.</>}
        lead={
          <>
            SSO, SCIM, RBAC, BYOK, workspace isolation, and a tamper-evident audit chain — on the
            same Transfer Studio engine, with every G1–G9 gate and every post-write verification
            in place.
          </>
        }
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
        slas={[
          { value: "SSO", label: "OIDC / SAML" },
          { value: "SCIM", label: "Group → role" },
          { value: "BYOK", label: "Customer keys" },
          { value: "Chained", label: "Signed audit log" },
        ]}
        aside={
          <div className="lp-sales-hero-art">
            <PerimeterArt />
          </div>
        }
      />

      <MarketingReveal>
        <section className="lp-ent-capabilities">
          <div className="lp-ent-section-head">
            <p className="lp-mkt-kicker">Control plane</p>
            <h2>Enterprise controls on every run</h2>
            <p>
              Identity, access, tenancy, keys, audit, and operations policies wrap the same
              map → preflight → verify path that every load runs.
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
                Your teams map, preflight, quarantine, and verify the same way on every plan — with
                identity, tenancy, and keys wrapping every run.
              </p>
              <ul className="lp-ent-checklist">
                <li>Workspace RBAC for who can map, approve drift, and run production loads</li>
                <li>A workspace data region, with residency enforcement that refuses out-of-region destinations before the run</li>
                <li>MCP and Datawrap Pilot run the same gates, with secret references instead of passwords</li>
                <li>Signed proof packs and a verifiable audit export for every review</li>
                <li>Security questionnaire and control-mapping pack for procurement kickoff</li>
              </ul>
            </div>
            <aside className="lp-ent-panel" aria-label="Enterprise control snapshot">
              <h3>Control snapshot</h3>
              <div className="lp-ent-panel-row"><span>Identity</span><em>OIDC / SAML · SCIM</em></div>
              <div className="lp-ent-panel-row"><span>Secrets</span><em>BYOK wrapped</em></div>
              <div className="lp-ent-panel-row"><span>Preflight</span><em>9 / 9 required</em></div>
              <div className="lp-ent-panel-row"><span>Quarantine</span><em>replayable</em></div>
              <div className="lp-ent-panel-row"><span>Verify</span><em>value &amp; key census</em></div>
              <div className="lp-ent-panel-row"><span>Audit</span><em>hash-chained · key id</em></div>
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
                Start a pilot on the same engine. Add SSO, SCIM, BYOK, and audit policies when
                security is ready — your operators keep working the same way.
              </p>
              <ComplianceBadges items={["Credentials encrypted at rest", "SSO / SAML / OIDC", "SCIM 2.0", "Customer-managed keys", "GDPR processing", "Workspace data region"]} />
            </div>
            <div className="lp-ent-cta-actions">
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

function CustomersPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  return (
    <div className="lp-mkt-page lp-cust-v3">
      <MarketingInkHero
        kicker="Customers"
        title={<>Every load leaves proof you can keep.</>}
        lead={
          <>
            What we publish is measured: schema drift, identity carry, retry safety, and
            destination read-back on named engines. Book a pilot and get the same evidence on{" "}
            <em>your</em> sources and destinations.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Book a pilot
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("enterprise")}>
              Enterprise overview
            </button>
          </div>
        }
        slas={[
          { value: "9", label: "Core preflight gates" },
          { value: "48", label: "Live schema-drift cases" },
          { value: String(TRANSFER_READY_DRIVERS), label: "Transfer-ready drivers" },
          { value: "4", label: "Native CDC engines" },
        ]}
        aside={
          <div className="lp-sales-hero-art">
            <EvidencePlatesArt />
          </div>
        }
      />

      <section className="lp-cust3-metrics" aria-label="Who this is for">
        <div className="lp-shell lp-cust3-metrics-row">
          <div>
            <strong>Migrations</strong>
            <span>Oracle and SQL Server cutover with identity, FK, and Unicode carry</span>
          </div>
          <div>
            <strong>Warehouses</strong>
            <span>Snowflake, BigQuery, Apache Iceberg — MERGE + verified</span>
          </div>
          <div>
            <strong>Lakes</strong>
            <span>Amazon S3, ADLS, Google Cloud Storage — exact decimals</span>
          </div>
          <div>
            <strong>CDC</strong>
            <span>SQL Server, PostgreSQL, MySQL, Oracle — snapshot, stream, resume</span>
          </div>
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-cust3-stories" aria-label="Why teams choose Datawrap">
          <div className="lp-shell">
            <div className="lp-cust3-section-head">
              <p className="lp-mkt-kicker">Why Datawrap</p>
              <h2>Built for operators who answer for every row</h2>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_PROOF_HIGHLIGHTS.map((row) => (
                <article key={row.title} className="lp-mkt-evidence-card">
                  <span className="lp-cust-industry">{row.stat}</span>
                  <p><strong>{row.title}.</strong> {row.body}</p>
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
              <h2>Warehouses, lakes, databases, streams, vectors, and apps</h2>
              <p>
                {catalogHonestyLead()} Same map, same gates, same verification report on every
                destination.
              </p>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_STACK.map((row) => (
                <article key={row.family} className="lp-mkt-evidence-card">
                  <span className="lp-cust-industry">{row.family}</span>
                  <p><strong>{row.items}.</strong> {row.note}</p>
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
              preflight, quarantine, and a signed proof pack you keep.
            </p>
          </div>
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={() => onNavigate("contact")}>
              Contact sales
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

function LegalPage({ kind }: { kind: "privacy" | "terms" }) {
  const privacy = [
    {
      h: "Who we are",
      p: "Datawrap, Inc. (“Datawrap”, “we”, “us”) provides a governed data-movement platform. This policy explains how we handle personal data when you visit datawrap.io, create a workspace, or use Transfer Studio, pipelines, Pilot, or MCP. For source and destination datasets you connect, you are the controller; Datawrap is the processor.",
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
      p: "We share data with infrastructure providers that host the control plane, send transactional email, and (on Enterprise) your identity provider and KMS. We do not share connected datasets with other customers. A current subprocessor list is available on request from contact@datawrap.io.",
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
      p: "You may request access, correction, export, or deletion of workspace personal data through your admin or contact@datawrap.io. You may object to or restrict certain processing where the law allows. You may lodge a complaint with your supervisory authority.",
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
      p: "We will post material changes on this page and update the date below. Questions: contact@datawrap.io. Enterprise customers may attach a DPA to their order form.",
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
      p: "We will post material changes on this page and update the date below. Continued use after the effective date constitutes acceptance, except that enterprise customers are governed by the notice terms in their order form. Questions: contact@datawrap.io.",
    },
  ];
  const blocks = kind === "privacy" ? privacy : terms;

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingInkHero
        tone="doc"
        kicker={kind === "privacy" ? "Privacy" : "Terms"}
        title={kind === "privacy" ? "Privacy policy" : "Terms of service"}
        meta={
          kind === "privacy"
            ? "Last updated August 2026 · contact@datawrap.io · You are the controller of connected data"
            : "Last updated August 2026 · contact@datawrap.io · A signed MSA supersedes where it conflicts"
        }
        lead={
          kind === "privacy"
            ? "How Datawrap handles workspace data, credentials, and audit logs — written for security and legal review."
            : "The agreement that governs use of Transfer Studio, pipelines, Pilot, and MCP."
        }
      />

      <MarketingReveal>
        <section className="lp-mkt-body lp-mkt-legal-layout">
          <nav className="lp-mkt-legal-nav" aria-label="On this page">
            <h2>On this page</h2>
            <ul>
              {blocks.map((b) => {
                const id = b.h.toLowerCase().replace(/\s+/g, "-");
                return (
                  <li key={b.h}>
                    <button
                      type="button"
                      onClick={() => {
                        document.getElementById(id)?.scrollIntoView({ behavior: "smooth", block: "start" });
                      }}
                    >
                      {b.h}
                    </button>
                  </li>
                );
              })}
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
      d: "Every store read is scoped to the caller's workspace — jobs, connectors, schedules, notifications, audit, and agent tools.",
      pill: "Workspace isolated",
    },
    {
      phase: "02",
      t: "Encrypt",
      tag: "Keys",
      d: "TLS in transit. Connector secrets encrypted at rest and wrappable with your own key: local, wrapped, or AWS KMS.",
      pill: "BYOK wrapped",
    },
    {
      phase: "03",
      t: "Authenticate",
      tag: "Identity",
      d: "OIDC and SAML SSO with email-domain routing, SCIM 2.0 group-to-role provisioning, and an idle session timeout.",
      pill: "SSO · SCIM",
    },
    {
      phase: "04",
      t: "Prove",
      tag: "Verify",
      d: "Every load is read back and verified value by value. Every action lands in a hash-chained, signed audit log.",
      pill: "Gate-8 MATCH",
    },
  ];
  const controls = [
    {
      phase: "05",
      t: "Least privilege",
      tag: "RBAC",
      d: "Viewer, Editor, and Admin roles. API keys carry a role and scopes capped at their creator's own grants.",
      pill: "Key role capped",
    },
    {
      phase: "06",
      t: "Signed delivery",
      tag: "Webhooks",
      d: "Each webhook channel has its own secret; every delivery carries a timestamped HMAC-SHA256 signature.",
      pill: "HMAC-SHA256",
    },
    {
      phase: "07",
      t: "Residency",
      tag: "Regions",
      d: "Each workspace records its data region; with residency enforcement on, a destination in another region is refused before the run starts.",
      pill: "Region checked",
    },
    {
      phase: "08",
      t: "Retention",
      tag: "Lifecycle",
      d: "Purge finished jobs older than your retention window, with a dry run and a confirm token first.",
      pill: "Dry run first",
    },
  ];
  return (
    <div className="lp-mkt-page lp-sec-v2">
      <MarketingInkHero
        kicker="Security"
        title={<>Security that moves with the data.</>}
        lead={
          <>
            Isolation, encryption, identity, and verified loads — on the same path Transfer Studio
            uses, inherited by Pilot and every MCP agent.
          </>
        }
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
        slas={[
          { value: "TLS", label: "In transit" },
          { value: "BYOK", label: "Connector secrets" },
          { value: "SCIM", label: "Group → role" },
          { value: "RBAC", label: "Studio · Pilot · MCP" },
        ]}
        aside={
          <div className="lp-sales-hero-art">
            <VaultArt />
          </div>
        }
      />

      <MarketingReveal>
        <section className="lp-sec-badges" aria-label="Compliance posture">
          <ComplianceBadges
            items={[
              "Credentials encrypted at rest",
              "SSO / SAML / OIDC",
              "SCIM 2.0",
              "Customer-managed keys",
              "GDPR processing",
              "Tamper-evident audit log",
              "Signed webhooks",
              "Workspace data region",
            ]}
          />
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sec-section">
          <div className="lp-sec-section-head">
            <p className="lp-mkt-kicker">Control plane</p>
            <h2>Four layers between source and destination</h2>
            <p>A continuous security path that activates on every transfer.</p>
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
        <section className="lp-sec-section">
          <div className="lp-sec-section-head">
            <p className="lp-mkt-kicker">Workspace controls</p>
            <h2>Controls your admins switch on</h2>
            <p>
              Least-privilege keys, signed webhooks, residency, and retention — set per workspace,
              with deployment-wide settings reserved for platform admins.
            </p>
          </div>
          <div className="lp-sec-layer-grid">
            {controls.map((layer) => (
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
              MCP tools and Datawrap Pilot work through secret references, so agents act without
              ever handling a destination password. Every agent action rides the same RBAC,
              quarantine, and verification path as Transfer Studio.
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
            <div className="lp-sec-agent-chip is-ok">Secret references only</div>
          </div>
        </section>
      </MarketingReveal>
    </div>
  );
}

const INTEGRATION_FAMILY_ICONS: Record<string, string[]> = {
  "Warehouses & lakehouse": ["snowflake", "bigquery", "iceberg"],
  "Object storage": ["s3", "adls", "gcs"],
  Databases: ["postgresql", "mysql", "sqlserver", "oracle", "mongodb"],
  Streaming: ["kafka"],
  "Vector databases": ["pgvector", "pinecone", "qdrant", "weaviate", "milvus"],
  "Applications & APIs": ["salesforce", "hubspot", "stripe"],
};

function IntegrationsPage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {

  return (
    <div className="lp-mkt-page lp-int-v2">
      <MarketingInkHero
        kicker="Connection catalog"
        title={<>Connect the systems you already run — {TRANSFER_READY_DRIVERS} transfer-ready drivers.</>}
        lead={
          <>
            Databases, warehouses, object storage, files, Kafka, vector stores, and apps. Every
            load maps, clears preflight, quarantines held-back rows, and is verified against the
            destination engine.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="lp-btn lp-btn--brand lp-btn--lg" onClick={onGetStarted}>
              Open connector catalog
            </button>
            <button type="button" className="lp-btn lp-btn--outline lp-btn--lg lp-btn--on-ink" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        }
        slas={[
          { value: String(TRANSFER_READY_DRIVERS), label: "Transfer-ready" },
          { value: "4", label: "Native CDC engines" },
          { value: "5", label: "Vector destinations" },
          { value: "DLQ", label: "Quarantine replay" },
        ]}
        aside={
          <div className="lp-sales-hero-art">
            <LatticeArt readyCount={TRANSFER_READY_DRIVERS} />
          </div>
        }
      />

      <MarketingReveal>
        <section className="lp-int-strip" aria-label="Capability labels">
          <div><strong>DLQ</strong><span>Quarantine replay</span></div>
          <div><strong>CDC</strong><span>SQL Server · PostgreSQL · MySQL · Oracle</span></div>
          <div><strong>SQLA</strong><span>Generic SQL drivers</span></div>
          <div><strong>Files</strong><span>CSV · JSON · Parquet · Avro · ORC · Excel</span></div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-int-section">
          <div className="lp-int-section-head">
            <p className="lp-mkt-kicker">Catalog families</p>
            <h2>What you can connect</h2>
            <p>
              Grouped the way teams plan loads — warehouses first, then lakes, databases, streams,
              vector stores, and apps. Every name below is a transfer-ready driver.
            </p>
          </div>
          <div className="lp-int-family-grid">
            {MARKETING_STACK.map((f) => (
              <article key={f.family} className="lp-int-family-card">
                <header>
                  <h3>{f.family}</h3>
                  <span>{f.badge}</span>
                </header>
                <p>{f.note}</p>
                <div className="lp-int-family-icons">
                  {(INTEGRATION_FAMILY_ICONS[f.family] || []).map((id) => (
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
            <h2>Every load proves itself</h2>
            <p>
              Every destination runs the same path: introspect, map, preflight, write, and verify.
              Warehouses, lakes, databases, streams, vectors, and apps share one engine and one
              report.
            </p>
            <ul>
              <li>Upsert and MERGE wherever the destination&apos;s keys support them</li>
              <li>Incremental sync binds a typed watermark on the cursor column</li>
              <li>Held-back rows quarantine with a reason and replay once fixed</li>
            </ul>
          </div>
          <aside className="lp-int-honesty-panel">
            <h3>Every route runs</h3>
            <div><span>01</span><em>Semantic map</em></div>
            <div><span>02</span><em>G1–G9 preflight</em></div>
            <div><span>03</span><em>Quarantine write</em></div>
            <div><span>04</span><em>Gate-8 verify</em></div>
          </aside>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-int-cta">
          <div>
            <h2>Browse the connector catalog</h2>
            <p>
              Open Transfer Studio and pick a source and destination. The transfer-ready filter
              shows every driver you can load today, on the same engine as production.
            </p>
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
