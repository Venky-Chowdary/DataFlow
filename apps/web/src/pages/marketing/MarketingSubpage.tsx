import { type CSSProperties } from "react";
import { DtIcon } from "../../components/DtIcon";
import { ConnectorIcon } from "../../app/brand-icons";
import { MarketingInkHero } from "../../components/marketing/MarketingInkHero";
import { ProductShot } from "../../components/marketing/ProductShot";
import { VerifyPanel } from "../../components/marketing/ProductPanels";
import { WORKSPACE_SHOT } from "../../lib/workspaceShots";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingReveal } from "../../components/marketing/MarketingReveal";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  MARKETING_PROOF_HIGHLIGHTS,
  MARKETING_STACK,
  PENDING_DRIVERS,
  PROVEN_CDC_SOURCES,
  PROVEN_DRIVERS,
  provenDriversPhrase,
  catalogHonestyLead,
  cdcSourcesPhrase,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import { ContactSalesPage } from "./ContactSalesPage";
import { DemosPage } from "./DemosPage";
import { StatusPage, SubprocessorsPage } from "./TrustPages";
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
 * (see docs/operate/security.md); certifications are not listed.
 */
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
    case "dpa":
      return <LegalPage kind="dpa" />;
    case "subprocessors":
      return <SubprocessorsPage />;
    case "status":
      return <StatusPage />;
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
    case "demos":
      return <DemosPage onNavigate={onNavigate} />;
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
        "Read-back with a value and key census",
        provenDriversPhrase(),
      ],
    },
    {
      name: "Team",
      price: "Custom",
      period: "usage-aligned",
      blurb: "Schedules, native CDC, Pilot, and shared connectors — priced to cadence and connectors.",
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
    { feature: "Read-back: value and key census", starter: true, team: true, enterprise: true },
    { feature: "Source query / stored procedure read", starter: true, team: true, enterprise: true },
    { feature: "Signed risk contracts & proof packs", starter: true, team: true, enterprise: true },
    { feature: "Schedules & typed watermarks", starter: false, team: true, enterprise: true },
    { feature: `Native CDC (${cdcSourcesPhrase()})`, starter: false, team: true, enterprise: true },
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
        title={<>Every plan includes the checks and the read-back.</>}
        lead={
          <>
            Starter includes the whole proof path: a map you review, checks that stop a bad
            write, quarantine with replay, and a value-by-value read of the destination. You pay
            for cadence and security, not for knowing the rows arrived.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
              Start for free
            </button>
            <button
              type="button"
              className="mk-btn mk-btn--secondary mk-btn--lg"
              onClick={() => onNavigate("contact")}
            >
              Contact sales
            </button>
          </div>
        }
        slas={[
          { value: "Free", label: "Starter forever" },
          { value: "Verified", label: "Every load, every plan" },
          { value: String(PROVEN_DRIVERS), label: "Drivers proven in CI" },
          { value: "Custom", label: "Team & Enterprise" },
        ]}
      />

      <MarketingReveal>
        <section className="lp-pricing-v2-plans" aria-label="Plans">
          <div className="lp-pricing-v2-grid">
            {plans.map((plan, i) => (
              <article
                key={plan.name}
                className={`lp-pricing-card lp-scene-card lp-pricing-card--${plan.tone}${plan.featured ? " is-featured" : ""}`}
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
                  className={plan.featured ? "mk-btn mk-btn--primary mk-btn--block" : "mk-btn mk-btn--secondary mk-btn--block"}
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
            <p className="mk-kicker">Compare</p>
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
          <div className="lp-pricing-principle lp-scene-card">
            <strong>A real free tier</strong>
            <span>The full Transfer Studio path, verification included.</span>
          </div>
          <div className="lp-pricing-principle lp-scene-card">
            <strong>Quote when you scale</strong>
            <span>Priced to connectors and cadence.</span>
          </div>
          <div className="lp-pricing-principle lp-scene-card">
            <strong>One engine everywhere</strong>
            <span>UI, Schedules, Pilot, and MCP share the same gates.</span>
          </div>
        </section>
      </MarketingReveal>

      <section className="lp-sales-compare" aria-label="What every plan includes">
        <div className="lp-mkt-wrap">
          <header>
            <p className="mk-kicker">In every plan</p>
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
                  <td>Read-back after the write</td>
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
        <MarketingClosing
          title="Procurement, MSA, or a security pack?"
          body="Enterprise agreements come with a negotiated MSA and DPA, a completed security questionnaire, and a workspace audit export your reviewers can verify."
        >
          <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
            Contact sales
          </button>
          <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("security")}>
            Security overview
          </button>
        </MarketingClosing>
      </MarketingReveal>
    </div>
  );
}

function EnterprisePage({ onGetStarted, onNavigate }: Pick<PageActions, "onGetStarted" | "onNavigate">) {
  const capabilities = [
    {
      id: "identity",
      kicker: "Identity",
      title: "SSO and SCIM that gate every run",
      body: "OIDC and SAML sign-in with email-domain routing to the right identity provider. SCIM 2.0 provisions users and maps groups to workspace roles; deactivating a user revokes their sessions and keys.",
    },
    {
      id: "access",
      kicker: "Access",
      title: "RBAC with API keys that can't outrank you",
      body: "Viewer, Editor, and Admin roles on every Studio job, Pilot session, and MCP call. An API key carries a role and scopes no higher than the person who created it, and the audit log names the key.",
    },
    {
      id: "tenancy",
      kicker: "Tenancy",
      title: "Workspace isolation at the store layer",
      body: "Every read of jobs, connectors, schedules, notifications, and audit is scoped to the caller's workspace — Pilot and MCP tools included. Custom domains and a per-workspace data region on top.",
    },
    {
      id: "keys",
      kicker: "Keys",
      title: "BYOK on connector secrets",
      body: "Connector secrets are encrypted at rest and can be wrapped with your own key: local, wrapped, or AWS KMS, with rotation. PII hashing uses a per-workspace key generated on first use.",
    },
    {
      id: "audit",
      kicker: "Audit",
      title: "A tamper-evident audit chain",
      body: "Every record is hash-chained and signed with the chain's own key, named by a key id. Verify the chain per workspace, filter by action, level, and time, and export CSV, JSON, NDJSON, or CEF.",
    },
    {
      id: "operations",
      kicker: "Operations",
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
            SSO, SCIM, role-capped keys, your own encryption key, and a signed audit chain ride
            the same engine as every load. The checks before a write, and the read-back after it,
            stay in place when security policy is added.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
              Contact sales
            </button>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={onGetStarted}>
              Request a pilot
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
          <ProductShot
            src={WORKSPACE_SHOT.settingsSso}
            alt="Settings SSO tab with SAML 2.0, OpenID Connect, and Azure AD, SCIM provisioning, and group mappings"
            surface="Settings, single sign-on"
          />
        }
      />

      <MarketingReveal>
        <section className="lp-ent-capabilities">
          <div className="lp-ent-section-head">
            <p className="mk-kicker">Control plane</p>
            <h2>Enterprise controls on every run</h2>
            <p>
              Identity, access, tenancy, keys, audit, and operations policies wrap the same
              map → preflight → verify path that every load runs.
            </p>
          </div>
          <div className="lp-ent-capability-grid">
            {capabilities.map((cap) => (
              <article key={cap.id} className="lp-ent-capability lp-scene-card">
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
              <p className="mk-kicker">How it lands</p>
              <h2>One engine. Enterprise controls on top.</h2>
              <p>
                Your teams map, preflight, quarantine, and verify the same way on every plan — with
                identity, tenancy, and keys wrapping every run.
              </p>
              <ul className="lp-ent-checklist">
                <li>Workspace RBAC for who can map, approve drift, and run production loads</li>
                <li>A workspace data region: once set, a run is refused before it starts when the destination&apos;s endpoint address or declared region is elsewhere, or shows no region</li>
                <li>MCP and Datawrap Pilot run the same gates, with secret references instead of passwords</li>
                <li>Signed proof packs and a verifiable audit export for every review</li>
                <li>Security questionnaire and control-mapping pack for procurement kickoff</li>
              </ul>
            </div>
            <aside className="lp-ent-panel" aria-label="Enterprise control snapshot">
              <h3>Control snapshot</h3>
              <div className="lp-ent-panel-row"><span>Identity</span><em>OIDC / SAML · SCIM</em></div>
              <div className="lp-ent-panel-row"><span>Secrets</span><em>BYOK wrapped</em></div>
              <div className="lp-ent-panel-row"><span>Preflight</span><em>G1–G9 + route policy</em></div>
              <div className="lp-ent-panel-row"><span>Quarantine</span><em>replayable</em></div>
              <div className="lp-ent-panel-row"><span>Verify</span><em>value &amp; key census</em></div>
              <div className="lp-ent-panel-row"><span>Audit</span><em>hash-chained · key id</em></div>
            </aside>
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <MarketingClosing
          title="Run a pilot under your own policy"
          body="Start on the same engine your operators will use. SSO, SCIM, your own keys, and audit policies can be switched on when security is ready, and the way people work does not change."
        >
          <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
            Contact sales
          </button>
          <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("security")}>
            Security overview
          </button>
        </MarketingClosing>
      </MarketingReveal>
    </div>
  );
}

function CustomersPage({ onNavigate }: Pick<PageActions, "onNavigate">) {
  return (
    <div className="lp-mkt-page lp-cust-v3">
      <MarketingInkHero
        kicker="Evidence"
        title={<>One run, counted on both sides.</>}
        lead={
          <>
            The Trust Report beside this text is from a recorded load: 5,100 orders from Postgres into
            Snowflake, every column mapped, full checksum match. A pilot produces the same record
            on your own sources and destinations.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
              Request a pilot
            </button>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("enterprise")}>
              Enterprise overview
            </button>
          </div>
        }
        slas={[
          { value: "9", label: "Core preflight gates" },
          { value: String(PROVEN_DRIVERS), label: "Drivers proven in CI" },
          { value: String(PENDING_DRIVERS), label: "More implemented, in CI proof" },
          { value: String(PROVEN_CDC_SOURCES.length), label: "CDC sources proven in CI" },
        ]}
        aside={<VerifyPanel />}
      />

      <section className="lp-cust3-metrics" aria-label="Who this is for">
        <div className="lp-shell lp-cust3-metrics-row">
          {[
            ["Migrations", "Database cutover with identity, FK, and Unicode carry"],
            ["Warehouses", "Snowflake, BigQuery, Apache Iceberg — MERGE + verified"],
            ["Lakes", "Amazon S3, ADLS, Google Cloud Storage — exact decimals"],
            ["CDC", "Snapshot, stream from the database log, resume"],
          ].map(([title, body], i) => (
            <div key={title} style={{ ["--i" as string]: i }}>
              <strong>{title}</strong>
              <span>{body}</span>
            </div>
          ))}
        </div>
      </section>

      <MarketingReveal>
        <section className="lp-cust3-stories" aria-label="Why teams choose Datawrap">
          <div className="lp-shell">
            <div className="lp-cust3-section-head">
              <p className="mk-kicker">Why Datawrap</p>
              <h2>Built for operators who answer for every row</h2>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_PROOF_HIGHLIGHTS.map((row, i) => (
                <article key={row.title} className="lp-mkt-evidence-card lp-scene-card" style={{ ["--reveal-i" as string]: i }}>
                  <span className="lp-cust-industry">{row.stat}</span>
                  <p><strong>{row.title}.</strong> {row.body}</p>
                  <footer>
                    <strong>{row.proof}</strong>
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
              <p className="mk-kicker">Your stack</p>
              <h2>Warehouses, lakes, databases, streams, vectors, and apps</h2>
              <p>
                {catalogHonestyLead()} Same map, same gates, same verification report on every
                destination.
              </p>
            </div>
            <div className="lp-mkt-evidence-grid">
              {MARKETING_STACK.map((row) => (
                <article key={row.family} className="lp-mkt-evidence-card lp-scene-card">
                  <span className="lp-cust-industry">{row.family}</span>
                  <p><strong>{row.items}.</strong> {row.note}</p>
                </article>
              ))}
            </div>
          </div>
        </section>
      </MarketingReveal>

      <MarketingClosing
        title="Get the same evidence on your data"
        body="A pilot is a scoped load on your own sources and destinations: mapping, preflight, quarantine, and a signed proof pack you keep."
      >
        <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
          Request a pilot
        </button>
        <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("pricing")}>
          See pricing
        </button>
      </MarketingClosing>
    </div>
  );
}

function LegalPage({ kind }: { kind: "privacy" | "terms" | "dpa" }) {
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
      p: "We share data with infrastructure providers that host the control plane, send transactional email, and (on Enterprise) your identity provider and KMS. We do not share connected datasets with other customers. The current list is published on our Subprocessors page (datawrap.io/#/subprocessors).",
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
      p: "These Terms of Service (“Terms”) govern access to the Datawrap platform, including Transfer Studio, connectors, schedules, Job space, Query, Pilot, and MCP. By creating a workspace or signing an order form you agree to these Terms. If your organization has a signed master agreement, that agreement controls where it conflicts.",
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
  const dpa = [
    { h: "Status of this document", p: "This is Datawrap's standard data processing addendum, published as a draft pending counsel review. The executed version is attached to your order form or master agreement, and the executed version controls." },
    { h: "Roles and scope", p: "You are the controller of personal data contained in the sources, destinations, and workspace you connect. Datawrap is your processor and processes that data only to provide the service. Where you act as a processor for your own customers, Datawrap is your subprocessor on the same terms." },
    { h: "Processing details", p: "Subject matter: data movement, mapping, validation, quarantine, and verification you configure. Duration: the term of your agreement plus the deletion period below. Data subjects and categories: whatever your connected datasets contain, plus workspace users' name, email, role, and audit events. Special-category data is processed only if you choose to connect it." },
    { h: "Your instructions", p: "Datawrap processes personal data only on your documented instructions — your agreement, your workspace configuration, and the transfers, schedules, and exports you run. We will tell you if we believe an instruction breaks data-protection law." },
    { h: "Confidentiality", p: "Everyone at Datawrap with access to customer personal data is bound by written confidentiality obligations and has access only as needed to operate and support the service." },
    { h: "Security measures", p: "TLS in transit; connector credentials encrypted at rest and optionally wrapped with your own KMS key; workspace isolation on every store read; role-based access with capped API keys; SSO and SCIM; a hash-chained, signed audit log; quarantine instead of silent drops; and post-write checksum verification. These measures are reviewed and may improve, but will not materially weaken during your term." },
    { h: "Subprocessors", p: "You authorize the subprocessors on our Subprocessors page. We bind each to data-protection terms at least as protective as this addendum and remain responsible for them. We give 30 days' notice of a new subprocessor; you may object on reasonable data-protection grounds, and if we cannot address the objection you may terminate the affected service." },
    { h: "Data subject requests", p: "We will promptly forward requests we receive about your data to you and, taking into account the nature of processing, help you respond through the workspace's export and deletion controls or by reasonable assistance." },
    { h: "Personal data breach", p: "We will notify you without undue delay, and in any case within 72 hours, after becoming aware of a personal data breach affecting your data, with the information you need to meet your own notification duties, and we will keep you updated as we learn more." },
    { h: "Assessments and audits", p: "We will provide information reasonably needed for your data-protection impact assessments and answer your security questionnaire. On reasonable written notice, no more than once a year unless required by a regulator or after a breach, we will support an audit of our compliance with this addendum." },
    { h: "International transfers", p: "Where personal data leaves the EEA, UK, or Switzerland to a country without an adequacy decision, the Standard Contractual Clauses (and UK Addendum where relevant) apply and are incorporated by reference." },
    { h: "Return and deletion", p: "At the end of the service you may export workspace data. We then delete customer personal data within 30 days, and from backups on their normal expiry cycle, unless the law requires us to keep it." },
    { h: "Liability and order of precedence", p: "Liability under this addendum follows the limits in your agreement. If this addendum conflicts with the agreement on data protection, this addendum controls; the Standard Contractual Clauses control over both." },
  ];
  const blocks = kind === "privacy" ? privacy : kind === "dpa" ? dpa : terms;
  const legalTitle = kind === "privacy" ? "Privacy policy" : kind === "dpa" ? "Data processing addendum" : "Terms of service";

  return (
    <div className="lp-mkt-page lp-mkt-page-rich">
      <MarketingInkHero
        tone="doc"
        kicker={kind === "privacy" ? "Privacy" : kind === "dpa" ? "Legal" : "Terms"}
        title={legalTitle}
        meta={
          kind === "privacy"
            ? "Last updated August 2026 · contact@datawrap.io · You are the controller of connected data"
            : kind === "dpa"
              ? "Draft pending legal review · Last updated November 2026 · contact@datawrap.io"
              : "Last updated August 2026 · contact@datawrap.io · A signed MSA supersedes where it conflicts"
        }
        lead={
          kind === "privacy"
            ? "How Datawrap handles workspace data, credentials, and audit logs — written for security and legal review."
            : kind === "dpa"
              ? "The processor terms that govern how Datawrap handles personal data in the workspaces and datasets you connect."
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
            <p className="lp-mkt-footnote">
              {kind === "dpa"
                ? "Draft pending legal review. The DPA executed with your order form controls."
                : "Last updated August 2026. Enterprise customers receive a DPA and negotiated addenda with their order form."}
            </p>
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
      pill: "Read back and compared",
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
      d: "A workspace can record its data region. Once it has, a destination whose endpoint address or declared region is elsewhere, or shows no region, is refused before the run starts.",
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
        title={<>How Datawrap protects the data it moves</>}
        lead={
          <>
            Workspace isolation, encrypted connector secrets, single sign-on, and a signed audit
            chain. The same controls apply in Transfer Studio, on schedules, in Pilot, and over
            MCP.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={() => onNavigate("contact")}>
              Request security pack
            </button>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("enterprise")}>
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
      />


      <MarketingReveal>
        <section className="lp-sec-section">
          <div className="lp-sec-section-head">
            <p className="mk-kicker">Control plane</p>
            <h2>Four layers between source and destination</h2>
            <p>Every transfer passes through the same four controls, whoever or whatever starts it.</p>
          </div>
          <div className="lp-sec-layer-grid">
            {layers.map((layer) => (
              <article key={layer.phase} className="lp-sec-layer-card lp-scene-card">
                <h3>{layer.t}</h3>
                <p>{layer.d}</p>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>
      <MarketingReveal>
        <section className="lp-sec-section">
          <div className="lp-sec-section-head">
            <p className="mk-kicker">Workspace controls</p>
            <h2>Controls your admins switch on</h2>
            <p>
              Least-privilege keys, signed webhooks, residency, and retention — set per workspace,
              with deployment-wide settings reserved for platform admins.
            </p>
          </div>
          <div className="lp-sec-layer-grid">
            {controls.map((layer) => (
              <article key={layer.phase} className="lp-sec-layer-card lp-scene-card">
                <h3>{layer.t}</h3>
                <p>{layer.d}</p>
              </article>
            ))}
          </div>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <section className="lp-sec-agent">
          <div className="lp-sec-agent-copy">
            <p className="mk-kicker">Runtime proof</p>
            <h2>Agents inherit the same gates</h2>
            <p>
              MCP tools and Datawrap Pilot work through secret references, so agents act without
              ever handling a destination password. Every agent action rides the same RBAC,
              quarantine, and verification path as Transfer Studio.
            </p>
            <div className="lp-sec-agent-actions">
              <button type="button" className="mk-btn mk-btn--primary" onClick={() => onNavigate("contact")}>
                Request security pack
              </button>
              <button type="button" className="mk-btn mk-btn--secondary" onClick={() => onNavigate("enterprise")}>
                Enterprise capabilities
              </button>
            </div>
          </div>
        </section>
      </MarketingReveal>
      <MarketingReveal>
        <section className="lp-sec-section" id="vulnerability-disclosure" aria-labelledby="lp-sec-disclosure">
          <div className="lp-sec-section-head">
            <p className="mk-kicker">Vulnerability disclosure</p>
            <h2 id="lp-sec-disclosure">Report a vulnerability</h2>
            <p>
              Email <a href="mailto:contact@datawrap.io?subject=Security%20report">contact@datawrap.io</a> with
              the subject “Security report” — please don’t open a public issue. We acknowledge within 3 business
              days, assess within 10, and update you at least every 14 days until the fix ships.
            </p>
            <p>
              Safe harbor: we won’t pursue good-faith research that tests only accounts you own, stops at the first
              sign of another customer’s data, avoids denial-of-service and social engineering, and gives us
              reasonable time to fix before disclosure. Machine-readable contact:{" "}
              <a href="/.well-known/security.txt">/.well-known/security.txt</a>.
            </p>
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
        title={<>Connect the systems you already run — {provenDriversPhrase()}.</>}
        lead={
          <>
            Databases, warehouses, object storage, files, Kafka, vector stores, and apps. Every
            load maps, clears preflight, quarantines held-back rows, and ends with a verification
            report that reads the destination back wherever its engine allows.
          </>
        }
        actions={
          <div className="lp-hero-cta">
            <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
              Open connector catalog
            </button>
            <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("help")}>
              Driver docs
            </button>
          </div>
        }
        slas={[
          { value: String(PROVEN_DRIVERS), label: "Proven in CI" },
          { value: String(PROVEN_CDC_SOURCES.length), label: "CDC sources proven in CI" },
          { value: "5", label: "Vector destinations" },
          { value: "DLQ", label: "Quarantine replay" },
        ]}
        aside={
          <ProductShot
            src={WORKSPACE_SHOT.connectorsWizard}
            alt="Choose a connector dialog with category chips, readiness filters, and connector cards"
            surface="Connectors, choose a connector"
          />
        }
      />


      <MarketingReveal>
        <section className="lp-int-section">
          <div className="lp-int-section-head">
            <p className="mk-kicker">Catalog families</p>
            <h2>What you can connect</h2>
            <p>
              Grouped the way teams plan loads — warehouses first, then lakes, databases, streams,
              vector stores, and apps. Each family's badge counts the members its CI run proves.
            </p>
          </div>
          <div className="lp-int-family-grid">
            {MARKETING_STACK.map((f, i) => (
              <article key={f.family} className="lp-int-family-card lp-scene-card" style={{ ["--reveal-i" as string]: i }}>
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
            <p className="mk-kicker">The bar</p>
            <h2>One path for every family</h2>
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
            <div><span>1</span><em>Semantic map</em></div>
            <div><span>2</span><em>G1–G9 preflight</em></div>
            <div><span>3</span><em>Quarantine write</em></div>
            <div><span>4</span><em>Read the destination back</em></div>
          </aside>
        </section>
      </MarketingReveal>

      <MarketingReveal>
        <MarketingClosing
          title="Pick a source and a destination"
          body="Open Transfer Studio and choose from the connectors your workspace can load today, on the same engine as production."
        >
          <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={onGetStarted}>
            Start free
          </button>
          <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={() => onNavigate("product-transfer")}>
            See Transfer Studio
          </button>
        </MarketingClosing>
      </MarketingReveal>
    </div>
  );
}
