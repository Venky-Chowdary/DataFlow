import { useId, useMemo, useState, type ChangeEvent, type ReactNode } from "react";
import { ConnectorIcon } from "../../app/brand-icons";
import { MarketingClosing } from "../../components/marketing/MarketingClosing";
import { MarketingFaq } from "../../components/marketing/MarketingFaq";
import { MarketingHeroBand } from "../../components/marketing/MarketingHeroBand";
import { DesignPartners } from "../../components/marketing/DesignPartners";
import {
  AuditChainPanel,
  ControlsPanel,
  CoveragePanel,
  ROLES,
  StatusGlyph,
  VerifyPanel,
} from "../../components/marketing/ProductPanels";
import { isHelpDocRoute } from "../../lib/helpDocs";
import {
  CONNECTOR_CLAIMS,
  EVIDENCE_AS_OF,
  EVIDENCE_RUN_ID,
  PROVEN_EVIDENCE,
  catalogHonestyLead,
  cdcSourcesPhrase,
  provenDriversPhrase,
} from "../../lib/provenEvidence";
import type { PublicRoute } from "../../lib/publicNavigation";
import { DocArticlePage, DocsPortal } from "./DocsPortal";
import { ContactSalesPage } from "./ContactSalesPage";
import { DemosPage } from "./DemosPage";
import { LegalPage } from "./LegalPages";
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
} from "./StoryPages";

interface PageActions {
  onGetStarted: () => void;
  onLogin: () => void;
  onNavigate: (route: PublicRoute) => void;
}

type Nav = Pick<PageActions, "onGetStarted" | "onNavigate">;

export function MarketingSubpage({ route, onGetStarted, onNavigate }: { route: PublicRoute } & PageActions) {
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
      return <ProofPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
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
      return <SecurityPage onGetStarted={onGetStarted} onNavigate={onNavigate} />;
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

type Action = { label: string; onClick: () => void };

/** The page's two actions: one primary, one secondary. Used in the hero and the close. */
function Actions({ primary, secondary }: { primary: Action; secondary: Action }) {
  return (
    <div className="lp-hero-cta">
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={primary.onClick}>
        {primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={secondary.onClick}>
        {secondary.label}
      </button>
    </div>
  );
}

function Closing({ title, body, primary, secondary }: { title: string; body: string; primary: Action; secondary: Action }) {
  return (
    <MarketingClosing title={title} body={body}>
      <button type="button" className="mk-btn mk-btn--primary mk-btn--lg" onClick={primary.onClick}>
        {primary.label}
      </button>
      <button type="button" className="mk-btn mk-btn--secondary mk-btn--lg" onClick={secondary.onClick}>
        {secondary.label}
      </button>
    </MarketingClosing>
  );
}

/** A two-column section: a short head on the left, the content on the right. */
function Block({
  id,
  title,
  lead,
  band,
  children,
}: {
  id: string;
  title: string;
  lead?: string;
  band?: boolean;
  children: ReactNode;
}) {
  return (
    <section className={`mk-section${band ? " mk-section--band" : ""}`} aria-labelledby={id}>
      <div className="mk-wrap mk-story-grid">
        <header className="mk-story-head">
          <h2 id={id}>{title}</h2>
          {lead ? <p className="mk-story-lede">{lead}</p> : null}
        </header>
        <div className="mk-story-body">{children}</div>
      </div>
    </section>
  );
}

function Facts({ items }: { items: { title: string; body: ReactNode }[] }) {
  return (
    <dl className="mk-checks">
      {items.map((c) => (
        <div key={c.title}>
          <dt>{c.title}</dt>
          <dd>{c.body}</dd>
        </div>
      ))}
    </dl>
  );
}

function Included() {
  return (
    <span className="mk-yes">
      <StatusGlyph kind="landed" />
      Included
    </span>
  );
}

function NotIncluded({ label = "Not included" }: { label?: string }) {
  return (
    <span className="mk-no">
      <span aria-hidden>–</span>
      <span className="mk-sr-only">{label}</span>
    </span>
  );
}

/* ------------------------------------------------------------------ Pricing */

type Tier = "starter" | "team" | "enterprise";
const TIERS: Tier[] = ["starter", "team", "enterprise"];

const PLAN_ROWS: { feature: string; in: Tier[] }[] = [
  { feature: "Transfer Studio: tables, destination queries, stored procedures", in: TIERS },
  { feature: "A plan you review before anything is written", in: TIERS },
  { feature: "Checks on the real write path, before the write", in: TIERS },
  { feature: "Quarantine with the reason, and replay", in: TIERS },
  { feature: "Read-back of the destination, counted by its own engine", in: TIERS },
  { feature: "Signed proof pack for every run", in: TIERS },
  { feature: "Schedules with typed watermarks", in: ["team", "enterprise"] },
  { feature: `Change data capture from ${cdcSourcesPhrase()}`, in: ["team", "enterprise"] },
  { feature: "Datawrap Pilot", in: ["team", "enterprise"] },
  { feature: "Shared workspace with four roles", in: ["team", "enterprise"] },
  { feature: "Single sign-on (OIDC or SAML) and SCIM", in: ["enterprise"] },
  { feature: "Connector secrets wrapped with your own key", in: ["enterprise"] },
  { feature: "Data-region fence on the workspace", in: ["enterprise"] },
  { feature: "Tamper-evident audit log and export", in: ["enterprise"] },
  { feature: "MCP server for agents", in: ["enterprise"] },
  { feature: "Self-managed deployment in your cloud or VPC", in: ["enterprise"] },
];

const PLANS: { tier: Tier; name: string; price: string; who: string; adds: string; highlights: string[] }[] = [
  {
    tier: "starter",
    name: "Starter",
    price: "Free while in early access",
    who: "For one person or a small team moving data they need to trust.",
    adds: "The whole load path",
    highlights: [
      "Plan, checks, quarantine, and read-back on every run",
      "A signed proof pack you can verify",
      `${provenDriversPhrase()}, more implemented`,
    ],
  },
  {
    tier: "team",
    name: "Team",
    price: "Talk to sales",
    who: "For a data team that runs loads on a schedule.",
    adds: "Everything in Starter, plus",
    highlights: [
      "Schedules and change data capture",
      "Datawrap Pilot",
      "A shared workspace with viewer, operator, editor, and admin roles",
    ],
  },
  {
    tier: "enterprise",
    name: "Enterprise",
    price: "Talk to sales",
    who: "For organisations with identity, key, and residency requirements.",
    adds: "Everything in Team, plus",
    highlights: [
      "SSO, SCIM, and your own key on connector secrets",
      "A data-region fence and a tamper-evident audit log",
      "MCP server for agents",
      "Self-managed deployment: your cloud or VPC, with Docker Compose or Helm",
    ],
  },
];

function PricingPage({ onGetStarted, onNavigate }: Nav) {
  const start = { label: "Start free", onClick: onGetStarted };
  const sales = { label: "Talk to sales", onClick: () => onNavigate("contact") };
  return (
    <div className="mk-page mk-pricing">
      <MarketingHeroBand
        kicker="Pricing"
        title="Start free. Talk to us when a team depends on it."
        lead="Every plan runs the same engine: the plan you review, the checks before the write, quarantine with replay, and the read-back after it. Verification is never an add-on."
        actions={<Actions primary={start} secondary={sales} />}
      />

      <section className="mk-section mk-section--tight" aria-labelledby="mk-plans-title">
        <div className="mk-wrap">
          <h2 id="mk-plans-title" className="mk-sr-only">
            Plans
          </h2>
          <ol className="mk-plans">
            {PLANS.map((plan) => (
              <li key={plan.tier} className={`mk-plan mk-plan--${plan.tier}`}>
                <h3>{plan.name}</h3>
                <p className="mk-plan-price">{plan.price}</p>
                <p className="mk-plan-who">{plan.who}</p>
                <p className="mk-plan-adds">{plan.adds}</p>
                <ul className="mk-facts">
                  {plan.highlights.map((h) => (
                    <li key={h}>{h}</li>
                  ))}
                </ul>
                <button
                  type="button"
                  className={`mk-btn mk-btn--block ${plan.tier === "starter" ? "mk-btn--primary" : "mk-btn--secondary"}`}
                  onClick={plan.tier === "starter" ? start.onClick : sales.onClick}
                >
                  {plan.tier === "starter" ? start.label : sales.label}
                  <span className="mk-sr-only"> for {plan.name}</span>
                </button>
              </li>
            ))}
          </ol>
        </div>
      </section>

      <section className="mk-section mk-section--band" aria-labelledby="mk-compare-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-compare-title">What each plan includes</h2>
            <p>The first six rows are the load path itself. They are in every plan.</p>
          </header>
          <div className="mk-table-scroll" role="region" aria-labelledby="mk-compare-title" tabIndex={0}>
            <table className="mk-compare">
              <thead>
                <tr>
                  <th scope="col">Capability</th>
                  <th scope="col">Starter</th>
                  <th scope="col">Team</th>
                  <th scope="col">Enterprise</th>
                </tr>
              </thead>
              <tbody>
                {PLAN_ROWS.map((row) => (
                  <tr key={row.feature}>
                    <th scope="row">{row.feature}</th>
                    {TIERS.map((t) => (
                      <td key={t}>{row.in.includes(t) ? <Included /> : <NotIncluded />}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <Block id="mk-pricing-deploy" title="Where it runs" lead="Hosted by Datawrap is the default on every plan.">
        <DeploymentOptions />
      </Block>

      <section className="mk-section mk-section--band" aria-labelledby="mk-pricing-faq">
        <div className="mk-wrap mk-story-grid">
          <header className="mk-story-head">
            <h2 id="mk-pricing-faq">Questions about pricing</h2>
          </header>
          <MarketingFaq
            items={[
              {
                q: "Why is there no price for Team or Enterprise?",
                a: "Datawrap is in early access, and team plans are agreed with each customer. Tell us what you move and how often, and we will quote it.",
              },
              {
                q: "Does Starter have usage limits?",
                a: "No usage limits are enforced while Starter is free in early access.",
              },
              {
                q: "Is verification part of every plan?",
                a: "Yes. The checks before the write, quarantine with replay, and the read-back after it are how Datawrap moves data, so every plan has them.",
              },
              {
                q: "Can we run Datawrap in our own cloud?",
                a: "Yes, on the Enterprise plan: self-managed deployment in your cloud or VPC, with Docker Compose or Helm. Hosted is the default.",
              },
            ]}
          />
        </div>
      </section>

      <Closing
        title="Run your first load today"
        body="Starter is free while Datawrap is in early access. Team plans start with a conversation."
        primary={start}
        secondary={sales}
      />
    </div>
  );
}

/** Hosted (default) and self-managed (Enterprise), side by side. */
function DeploymentOptions() {
  return (
    <div className="mk-deploy-options">
      <div>
        <h3>Hosted</h3>
        <p className="mk-plan-adds">Every plan. The default.</p>
        <p>We run Datawrap for you. You connect your sources and destinations; there is nothing to install.</p>
      </div>
      <div>
        <h3>Self-managed</h3>
        <p className="mk-plan-adds">An Enterprise plan option</p>
        <p>
          Run Datawrap in your own cloud or VPC: two images, the API and the web app, deployed with Docker Compose or
          Helm. Connector credentials, evidence, and the audit log stay in your account.
        </p>
      </div>
    </div>
  );
}

/* --------------------------------------------------------------- Enterprise */

function EnterprisePage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const security = { label: "Security overview", onClick: () => onNavigate("security") };
  return (
    <div className="mk-page mk-enterprise">
      <MarketingHeroBand
        kicker="Enterprise"
        title="Your identity provider, your keys, your region."
        lead="Single sign-on, SCIM, four workspace roles, your own key on connector secrets, a data-region fence, and a tamper-evident audit log. They sit on the same engine as every load, so the plan, the checks, and the read-back do not change."
        actions={<Actions primary={pilot} secondary={security} />}
        visual={<ControlsPanel />}
      />

      <Block
        id="mk-ent-controls"
        title="Controls for the whole workspace"
        lead="Switched on per workspace by an admin. Deployment-wide settings are reserved for platform admins."
      >
        <Facts
          items={[
            {
              title: "Sign-in and provisioning",
              body: "OIDC or SAML single sign-on, routed to the right identity provider by email domain. SCIM 2.0 provisions people and maps identity-provider groups to workspace roles.",
            },
            {
              title: "Keys you own",
              body: "Connector secrets are encrypted at rest. A workspace can bring its own key: secrets are then wrapped with it, and the platform key wraps the workspace key.",
            },
            {
              title: "Data region",
              body: "Record a region on the workspace, and a run is refused before it starts when the destination is elsewhere or shows no region.",
            },
            {
              title: "Audit",
              body: "Every action lands in a tamper-evident log: each entry is signed and linked to the one before, so a removed or altered entry is detected. Verify the chain and export it.",
            },
            {
              title: "Workspace policy",
              body: "Idle session timeout, signed webhooks with a secret per channel, and a job-history retention purge that runs a dry run first.",
            },
          ]}
        />
      </Block>

      <Block
        id="mk-ent-roles"
        band
        title="Four roles, checked on every request"
        lead="Every API route is checked against one permission, in Transfer Studio, on schedules, in Pilot, and over MCP. An API key never carries more than the person who created it."
      >
        <table className="mk-roles">
          <caption className="mk-sr-only">Workspace roles</caption>
          <thead>
            <tr>
              <th scope="col">Role</th>
              <th scope="col">What it can do</th>
            </tr>
          </thead>
          <tbody>
            {ROLES.map((r) => (
              <tr key={r.name}>
                <th scope="row">{r.name}</th>
                <td>{r.does}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Block>

      <Block id="mk-ent-deploy" title="Hosted, or in your own cloud" lead="Self-managed deployment is an Enterprise plan option.">
        <DeploymentOptions />
      </Block>

      <Block id="mk-ent-buy" band title="Buying Datawrap" lead="What procurement usually asks for, and how a pilot works.">
        <Facts
          items={[
            {
              title: "A pilot on your own data",
              body: "One scoped load from your source to your destination, with the plan, the checks, quarantine, and a signed proof pack you keep.",
            },
            {
              title: "Data processing agreement",
              body: (
                <>
                  Available on request: <a href="mailto:contact@datawrap.io?subject=DPA%20request">contact@datawrap.io</a>.
                </>
              ),
            },
            {
              title: "Subprocessor list",
              body: (
                <>
                  Available on request:{" "}
                  <a href="mailto:contact@datawrap.io?subject=Subprocessor%20list">contact@datawrap.io</a>.
                </>
              ),
            },
            {
              title: "Commercial terms",
              body: (
                <>
                  Talk to sales: <a href="mailto:sales@datawrap.io">sales@datawrap.io</a>.
                </>
              ),
            },
          ]}
        />
      </Block>

      <Closing
        title="Run a pilot under your own policy"
        body="Start on the engine your operators will use, and switch on SSO, SCIM, and your own key when security is ready."
        primary={pilot}
        secondary={security}
      />
    </div>
  );
}

/* -------------------------------------------------------------------- Proof */

function engineId(name: string): string {
  for (const family of CONNECTOR_CLAIMS.families) {
    const hit = family.items.find((i) => i.name === name);
    if (hit) return hit.driver;
  }
  return name.toLowerCase().replace(/[^a-z0-9]/g, "");
}

function ProofPage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const connectors = { label: "See connectors", onClick: () => onNavigate("integrations") };
  return (
    <div className="mk-page mk-proof">
      <MarketingHeroBand
        kicker="Proof"
        title="Evidence you can check yourself."
        lead="Connector claims come from CI runs against the engines. Every load ends with a report counted by the destination's own engine and a signed proof pack. The audit log detects edits. You can re-run or verify each of these."
        actions={<Actions primary={pilot} secondary={connectors} />}
        visual={<VerifyPanel />}
      />

      <section className="mk-section" aria-labelledby="mk-proof-ci">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-proof-ci">Connectors proven in CI</h2>
            <p>
              An engine counts as proven only when its CI jobs ran green with no test skipped. This table is generated from
              the evidence file, not typed by hand.
            </p>
          </header>
          <div className="mk-table-scroll" role="region" aria-labelledby="mk-proof-ci" tabIndex={0}>
            <table className="mk-evidence">
              <thead>
                <tr>
                  <th scope="col">Engine</th>
                  <th scope="col">Tests executed</th>
                  <th scope="col">Result</th>
                  <th scope="col">CI jobs</th>
                </tr>
              </thead>
              <tbody>
                {PROVEN_EVIDENCE.map((row) => (
                  <tr key={row.engines}>
                    <th scope="row">
                      <span className="mk-conn-icon">
                        <ConnectorIcon id={engineId(row.engines)} size={16} />
                      </span>
                      {row.engines}
                    </th>
                    <td className="mk-num">{row.cases}</td>
                    <td>
                      <span className="mk-yes">
                        <StatusGlyph kind="landed" />
                        Green, none skipped
                      </span>
                    </td>
                    <td>
                      <code>{row.artifact}</code>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <p className="mk-caption">
            Recorded {EVIDENCE_AS_OF}, CI run {EVIDENCE_RUN_ID}.
          </p>
        </div>
      </section>

      <Block
        id="mk-proof-pack"
        band
        title="Verify a proof pack"
        lead="Every load can export a signed record of its checks and mapping. Anyone with access to the workspace can check it."
      >
        <ol className="mk-api-steps">
          <li>
            <p>Download the pack for a finished job.</p>
            <code>GET /api/v1/transfer/{"{job_id}"}/proof-pack</code>
          </li>
          <li>
            <p>Check its signature and content hash.</p>
            <code>POST /api/v1/transfer/proof-pack/verify</code>
          </li>
          <li>
            <p>Check that the audit log still holds the record it was filed with.</p>
            <code>POST /api/v1/audit/verify-pack</code>
          </li>
        </ol>
        <p className="mk-story-note">
          A pack says “migration proven” only when the check after the write was a full checksum. A pack built from a sample
          can be exported, and it refuses that claim.
        </p>
      </Block>

      <DesignPartners headingId="mk-proof-partners" />

      <Closing
        title="Get the same evidence on your data"
        body="A pilot is one scoped load on your own source and destination, ending with a proof pack you keep."
        primary={pilot}
        secondary={connectors}
      />
    </div>
  );
}

/* ----------------------------------------------------------------- Security */

function SecurityPage({ onNavigate }: Nav) {
  const pilot = { label: "Request a pilot", onClick: () => onNavigate("contact") };
  const enterprise = { label: "Enterprise", onClick: () => onNavigate("enterprise") };
  return (
    <div className="mk-page mk-security">
      <MarketingHeroBand
        kicker="Security"
        title="What protects your data while it moves."
        lead="Workspace isolation, encrypted connector secrets, checked outbound connections, four roles, and a tamper-evident audit log. The same controls apply in Transfer Studio, on schedules, in Pilot, and over MCP."
        actions={<Actions primary={pilot} secondary={enterprise} />}
        visual={<AuditChainPanel />}
      />

      <Block
        id="mk-sec-every"
        title="On every transfer"
        lead="Whoever or whatever starts the load: a person, a schedule, Pilot, or an agent over MCP."
      >
        <Facts
          items={[
            {
              title: "Workspace isolation",
              body: "Every read of jobs, connectors, schedules, notifications, and audit is scoped to the caller's workspace, Pilot and MCP tools included. Another workspace's id returns not found.",
            },
            {
              title: "Secrets",
              body: "Connector credentials are encrypted at rest, can be wrapped with your own key, and can be stored in AWS Secrets Manager. Error responses do not carry raw driver text.",
            },
            {
              title: "Outbound connections",
              body: "Every connection is checked where it is made. Cloud metadata addresses are always refused; private networks are refused unless the workspace lists them.",
            },
            {
              title: "Roles",
              body: "Viewer, operator, editor, and admin. Every API route is checked against one permission, and a route with no rule is refused.",
            },
            {
              title: "Audit log",
              body: "Tamper-evident: each entry is signed and linked to the one before. The chain head can be anchored in S3 with Object Lock, outside the application's reach.",
            },
          ]}
        />
      </Block>

      <section className="mk-section mk-section--band" aria-labelledby="mk-sec-flow">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-sec-flow">Where your data goes</h2>
            <p>Rows travel from your source through the engine to your destination. What the run keeps is what it needs to prove itself.</p>
          </header>
          <DataFlowDiagram />
        </div>
      </section>

      <Block id="mk-sec-admin" title="Controls your admins switch on" lead="Per workspace, with deployment-wide settings reserved for platform admins.">
        <Facts
          items={[
            {
              title: "Data region",
              body: "Once a workspace records its region, a destination elsewhere, or with no region, is refused before the run starts.",
            },
            {
              title: "Single sign-on and SCIM",
              body: "OIDC or SAML, routed by email domain. Groups from your identity provider map to workspace roles.",
            },
            {
              title: "Signed webhooks",
              body: "Each channel has its own secret, and every delivery carries a timestamped HMAC-SHA256 signature.",
            },
            {
              title: "Retention",
              body: "Purge finished jobs older than your window, after a dry run and a confirm token.",
            },
          ]}
        />
      </Block>

      <section className="mk-section mk-section--band" id="vulnerability-disclosure" aria-labelledby="mk-sec-disclosure">
        <div className="mk-wrap mk-story-grid">
          <header className="mk-story-head">
            <h2 id="mk-sec-disclosure">Report a vulnerability</h2>
          </header>
          <div className="mk-story-body mk-prose">
            <p>
              Email <a href="mailto:contact@datawrap.io?subject=Security%20report">contact@datawrap.io</a> with the subject
              “Security report”, and please do not open a public issue. We acknowledge vulnerability reports within three
              business days and keep you updated until resolved.
            </p>
            <p>
              We will not pursue good-faith research that tests only accounts you own, stops at the first sign of another
              customer’s data, avoids denial of service and social engineering, and gives us reasonable time to fix before
              disclosure. Machine-readable contact: <a href="/.well-known/security.txt">/.well-known/security.txt</a>.
            </p>
          </div>
        </div>
      </section>

      <Closing
        title="Review it on your own data"
        body="A pilot runs under your workspace policy, and its audit log and proof pack are yours to inspect."
        primary={pilot}
        secondary={enterprise}
      />
    </div>
  );
}

/** Source, engine, destination, and what is kept along the way. */
function DataFlowDiagram() {
  const kept: { what: string; where: string }[] = [
    { what: "Connector credentials", where: "Encrypted at rest, and wrapped with your key if you bring one." },
    { what: "Held rows", where: "Kept in the job's quarantine with the reason, until you fix the cause and replay them." },
    { what: "Evidence", where: "The plan, the checks, the report, and the proof pack, stored on the job." },
    { what: "Audit log", where: "Signed entries, each linked to the one before, per workspace." },
  ];
  return (
    <figure className="mk-flow">
      <ol className="mk-flow-line" aria-label="The path of a row">
        <li>
          <strong>Your source</strong>
          <span>Read with the credentials you saved</span>
        </li>
        <li className="is-engine">
          <strong>Datawrap engine</strong>
          <span>Checks, writes, holds, and reads back</span>
        </li>
        <li>
          <strong>Your destination</strong>
          <span>Counted by its own engine after the write</span>
        </li>
      </ol>
      <dl className="mk-flow-kept">
        {kept.map((k) => (
          <div key={k.what}>
            <dt>{k.what}</dt>
            <dd>{k.where}</dd>
          </div>
        ))}
      </dl>
      <figcaption className="mk-caption">
        Hosted, this runs in Datawrap's account. Self-managed, on the Enterprise plan, it runs in your cloud or VPC.
      </figcaption>
    </figure>
  );
}

/* ------------------------------------------------------------- Connectors */

type ConnectorRow = {
  name: string;
  driver: string;
  family: string;
  proven: boolean;
  cdc: "proven" | "implemented" | null;
};

const CONNECTOR_ROWS: ConnectorRow[] = CONNECTOR_CLAIMS.families.flatMap((f) =>
  f.items.map((i): ConnectorRow => {
    const cdc = CONNECTOR_CLAIMS.cdc_sources.find((c) => c.driver === i.driver);
    return {
      name: i.name,
      driver: i.driver,
      family: f.family,
      proven: i.proven,
      cdc: cdc ? (cdc.proven ? "proven" : "implemented") : null,
    };
  }),
);

function IntegrationsPage({ onGetStarted, onNavigate }: Nav) {
  const families = useMemo(() => ["All", ...CONNECTOR_CLAIMS.families.map((f) => f.family)], []);
  const [family, setFamily] = useState("All");
  const [query, setQuery] = useState("");
  const [provenOnly, setProvenOnly] = useState(false);
  const searchId = useId();
  const q = query.trim().toLowerCase();
  const rows = CONNECTOR_ROWS.filter(
    (r) => (family === "All" || r.family === family) && (!provenOnly || r.proven) && (!q || r.name.toLowerCase().includes(q)),
  );
  const start = { label: "Start free", onClick: onGetStarted };
  const docs = { label: "Read the docs", onClick: () => onNavigate("help") };
  return (
    <div className="mk-page mk-connectors">
      <MarketingHeroBand
        kicker="Connectors"
        title="Connect what you run, and see what CI has proven."
        lead={`${catalogHonestyLead()} Every one runs the same path: plan, checks, quarantine, and a read-back wherever the destination allows it.`}
        actions={<Actions primary={start} secondary={docs} />}
        visual={<CoveragePanel />}
      />

      <section className="mk-section" aria-labelledby="mk-matrix-title">
        <div className="mk-wrap">
          <header className="mk-section-head">
            <h2 id="mk-matrix-title">Every connector</h2>
            <p>
              Filter by family, search by name, or show only connectors with a green CI run behind them. Evidence recorded{" "}
              {EVIDENCE_AS_OF}.
            </p>
          </header>
          <div className="mk-filter">
            <div className="mk-chips" role="group" aria-label="Filter by family">
              {families.map((f) => (
                <button key={f} type="button" className="mk-chip" aria-pressed={family === f} onClick={() => setFamily(f)}>
                  {f}
                </button>
              ))}
            </div>
            <div className="mk-filter-row">
              <div className="mk-field">
                <label htmlFor={searchId}>Search connectors</label>
                <input
                  id={searchId}
                  type="search"
                  value={query}
                  autoComplete="off"
                  onChange={(e: ChangeEvent<HTMLInputElement>) => setQuery(e.target.value)}
                />
              </div>
              <label className="mk-check">
                <input type="checkbox" checked={provenOnly} onChange={(e) => setProvenOnly(e.target.checked)} />
                <span>Proven in CI only</span>
              </label>
              <p className="mk-filter-count" role="status" aria-live="polite">
                {rows.length} {rows.length === 1 ? "connector" : "connectors"}
              </p>
            </div>
          </div>
          <div className="mk-table-scroll" role="region" aria-labelledby="mk-matrix-title" tabIndex={0}>
            <table className="mk-matrix">
              <thead>
                <tr>
                  <th scope="col">Connector</th>
                  <th scope="col">Family</th>
                  <th scope="col">Status</th>
                  <th scope="col">Change data capture</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={`${r.family}-${r.driver}`}>
                    <th scope="row">
                      <span className="mk-conn-icon">
                        <ConnectorIcon id={r.driver} size={18} />
                      </span>
                      {r.name}
                    </th>
                    <td>{r.family}</td>
                    <td>
                      {r.proven ? (
                        <span className="mk-badge mk-badge--ci">
                          <StatusGlyph kind="landed" />
                          Proven in CI
                        </span>
                      ) : (
                        <span className="mk-badge">Implemented</span>
                      )}
                    </td>
                    <td>
                      {r.cdc === "proven" ? (
                        <span className="mk-yes">
                          <StatusGlyph kind="landed" />
                          Source, proven in CI
                        </span>
                      ) : r.cdc === "implemented" ? (
                        "Source, implemented"
                      ) : (
                        <NotIncluded label="No" />
                      )}
                    </td>
                  </tr>
                ))}
                {rows.length === 0 ? (
                  <tr>
                    <td colSpan={4} className="mk-empty">
                      No connector matches. Clear the search or choose All.
                    </td>
                  </tr>
                ) : null}
              </tbody>
            </table>
          </div>
        </div>
      </section>

      <Block
        id="mk-conn-path"
        band
        title="One path for every family"
        lead="A warehouse, a lake, a database, a stream, a vector store, or an app: the load runs the same way."
      >
        <Facts
          items={[
            { title: "Upsert and MERGE", body: "Wherever the destination's keys support them." },
            {
              title: "Incremental sync",
              body: "Binds a typed watermark on the cursor column, so the next run starts where the last one ended.",
            },
            {
              title: "Held rows",
              body: "Quarantined with the column, the value, and the reason, and replayed into the same table once fixed.",
            },
          ]}
        />
      </Block>

      <Closing
        title="Pick a source and a destination"
        body="Choose from the connectors your workspace can load today, on the same engine as production."
        primary={start}
        secondary={docs}
      />
    </div>
  );
}
